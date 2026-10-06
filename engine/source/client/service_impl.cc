#include "engine/source/client/service_impl.h"

#include "engine/source/client/redaction.h"

#include "source/common/common/cleanup.h"
#include "source/common/protobuf/utility.h"

#include <grpc++/grpc++.h>

#include <system_error>
#include <thread>

#include "envoy/config/core/v3/base.pb.h"

#include "engine/source/client/client.h"
#include "engine/source/client/options_impl.h"
#include "engine/source/client/output_collector_impl.h"
#include "engine/source/common/request_source_impl.h"

#include "absl/strings/str_cat.h"

namespace Nighthawk {
namespace Client {

void ServiceImpl::handleExecutionRequest(const nighthawk::client::ExecutionRequest& request,
                                         Stream* stream, std::shared_ptr<Execution> execution) {
  // The slot was taken by the stream thread when it accepted the start. It is given back on
  // every way out of here -- and, on every path that answers, before the final response is
  // written (see write_final): a client that starts its next run the moment it reads that
  // response must never be refused as busy by the run it just watched end.
  bool released = false;
  auto release = [this, &execution, &released]() {
    if (released) {
      return;
    }
    released = true;
    {
      Envoy::Thread::LockGuard guard(active_lock_);
      ASSERT(active_executions_ > 0);
      active_executions_--;
    }
    Envoy::Thread::LockGuard guard(execution->lock);
    execution->done = true;
  };
  // For the exception paths out of Process::run().
  Envoy::Cleanup release_on_exit(release);
  // The one way a final response leaves this function, early errors included.
  auto write_final = [this, stream, &release](const nighthawk::client::ExecutionResponse& r) {
    release();
    writeResponse(stream, r);
  };

  nighthawk::client::ExecutionResponse response;
  OptionsPtr options;
  try {
    nighthawk::client::CommandLineOptions requested = request.start_request().options();
    if (max_concurrent_executions_ > 1) {
      // The log level is one process-wide setting (Envoy's logger registry), which every
      // Process sets from its own options when it is built. With one execution at a time
      // that honours each request; with several at once the last one built would set the
      // level for all of them. A service that runs executions concurrently therefore logs at
      // its own level throughout. Set unconditionally: a request that names no verbosity
      // would otherwise get the options' default, which is a different level again.
      if (requested.has_verbosity() &&
          requested.verbosity().value() != nighthawk::client::Verbosity::INFO) {
        ENVOY_LOG(info, "Ignoring the request's verbosity: this service runs executions "
                        "concurrently, and the log level is process-wide.");
      }
      requested.mutable_verbosity()->set_value(nighthawk::client::Verbosity::INFO);
    }
    options = std::make_unique<OptionsImpl>(requested);
  } catch (const MalformedArgvException& e) {
    response.mutable_error_detail()->set_code(grpc::StatusCode::INTERNAL);
    response.mutable_error_detail()->set_message(e.what());
    write_final(response);
    return;
  }
  // A set interval asks for progress; one that cannot be honoured -- not positive, or below
  // the millisecond the timer runs at -- is an error rather than silently no progress. Checked
  // before anything is created, so there is nothing to tear down on the way out.
  std::chrono::milliseconds progress_interval(0);
  if (request.start_request().has_progress_interval()) {
    const auto& interval = request.start_request().progress_interval();
    const int64_t nanos = interval.seconds() * 1000000000LL + interval.nanos();
    if (nanos < 1000000) {
      response.mutable_error_detail()->set_code(grpc::StatusCode::INVALID_ARGUMENT);
      response.mutable_error_detail()->set_message(
          "progress_interval must be at least 1ms (it is the period of the progress timer)");
      write_final(response);
      return;
    }
    // Rounded up: a snapshot never comes more often than asked for.
    progress_interval = std::chrono::milliseconds((nanos + 999999) / 1000000);
  }
  envoy::config::core::v3::TypedExtensionConfig typed_dns_resolver_config;
  Envoy::Network::DnsResolverFactory& dns_resolver_factory =
      Envoy::Network::createDefaultDnsResolverFactory(typed_dns_resolver_config);

  absl::StatusOr<ProcessPtr> process_or_status = ProcessImpl::CreateProcessImpl(
      *options, dns_resolver_factory, std::move(typed_dns_resolver_config), time_system_,
      process_wide_);
  if (!process_or_status.ok()) {
    response.mutable_error_detail()->set_code(grpc::StatusCode::INTERNAL);
    response.mutable_error_detail()->set_message(
        fmt::format("Unable to create ProcessImpl: {}", process_or_status.status().ToString()));
    write_final(response);
    return;
  }
  ProcessPtr process = std::move(*process_or_status);
  {
    Envoy::Thread::LockGuard guard(execution->lock);
    execution->process = process.get();
  }
  // Unpublished on every way out of this scope -- a normal return, or one of
  // the exceptions Process::run() rethrows -- and before `process` itself is
  // destroyed, since this guard was declared after it. A late cancellation
  // then finds nothing rather than a Process mid-teardown or already freed.
  // A run that was cancelled returns early with what it collected, and that
  // is the response the client gets.
  Envoy::Cleanup unpublish([&execution]() {
    Envoy::Thread::LockGuard guard(execution->lock);
    execution->process = nullptr;
  });

  // Progress, when the request asks for it: a thread that snapshots the run every interval
  // and writes the snapshot as an interim response. Writes on a gRPC stream must not overlap,
  // so this thread is stopped and joined before the final response is written below (the
  // stream's own thread only ever reads).
  Envoy::Thread::MutexBasicLockable progress_lock;
  Envoy::Thread::CondVar progress_stop;
  bool stop_progress = false;
  std::thread progress_thread;
  if (progress_interval.count() > 0) {
    progress_thread = std::thread([&]() {
      Envoy::Thread::LockGuard guard(progress_lock);
      while (!stop_progress) {
        progress_stop.waitFor(progress_lock, progress_interval); // NO_CHECK_FORMAT(real_time)
        if (stop_progress) {
          break;
        }
        std::optional<nighthawk::client::Output> snapshot = process->snapshot();
        if (!snapshot.has_value()) {
          continue;
        }
        nighthawk::client::ExecutionResponse interim;
        if (!snapshot->results().empty()) {
          *interim.mutable_progress()->mutable_elapsed() =
              snapshot->results(0).execution_duration();
        } else {
          interim.mutable_progress();
        }
        *interim.mutable_output() = std::move(*snapshot);
        writeResponse(stream, interim);
      }
    });
  }
  auto stop_progress_thread = [&]() {
    if (progress_thread.joinable()) {
      {
        Envoy::Thread::LockGuard guard(progress_lock);
        stop_progress = true;
        progress_stop.notifyAll();
      }
      progress_thread.join();
    }
  };
  // Also on the exception paths out of run(), before the lambda's captures go out of scope.
  Envoy::Cleanup stop_progress_on_exit(stop_progress_thread);

  OutputCollectorImpl output_collector(time_system_, *options);
  const bool ok = process->run(output_collector);
  stop_progress_thread();
  if (!ok) {
    response.mutable_error_detail()->set_code(grpc::StatusCode::INTERNAL);
    // TODO(https://github.com/envoyproxy/nighthawk/issues/181): wire through error descriptions, so
    // we can do better here.
    response.mutable_error_detail()->set_message(
        "Unknown failure. See Nighthawk Service logs. Make sure the URI is well formed and the DNS "
        "name resolves (if applicable). Check the output for problematic counter values. The "
        "default Nighthawk failure predicates report failure if (1) Nighthawk could not connect to "
        "the target (see 'benchmark.pool_connection_failure' counter; check the address and port "
        "number, and try explicitly setting --address-family v4 or v6, especially when using DNS; "
        "instead of localhost try 127.0.0.1 or ::1 explicitly), (2) the protocol was not supported "
        "by the target (see 'benchmark.stream_resets' counter; check http/https in the URI, --h2), "
        "(3) the target returned a 4xx or 5xx HTTP response code (see 'benchmark.http_4xx' and "
        "'benchmark.http_5xx' counters; check the URI path and the server config), or (4) a custom "
        "gRPC RequestSource failed. To relax expectations, set explicit failure predicates in the "
        "benchmark request.");
  }
  *(response.mutable_output()) = output_collector.toProto();
  process->shutdown();
  // Unpublished first, so a cancellation racing the end finds no Process; then the slot, then
  // the response (see write_final).
  unpublish.cancel();
  {
    Envoy::Thread::LockGuard guard(execution->lock);
    execution->process = nullptr;
  }
  write_final(response);
}

void ServiceImpl::writeResponse(Stream* stream,
                                const nighthawk::client::ExecutionResponse& response) {
  ENVOY_LOG(debug, "Write response: {}", absl::StrCat(redactedForLog(response)));
  if (!stream->Write(response)) {
    ENVOY_LOG(warn, "Failed to write response to the stream");
  }
}

grpc::Status ServiceImpl::finishGrpcStream(Execution* execution, const bool success,
                                           absl::string_view description) {
  // The stream that started an execution may get here while it is still in
  // flight, in the error paths: let it wrap up and put its response on the
  // stream before finishing the stream. A stream that started nothing has
  // nothing to wait for -- least of all another client's run.
  if (execution != nullptr && execution->future.valid()) {
    execution->future.wait();
  }
  return success ? grpc::Status::OK
                 : grpc::Status(grpc::StatusCode::INTERNAL, std::string(description));
}

// TODO(oschaaf): implement a way to update rps config on the fly.
// TODO(oschaaf): unit-test Process, create MockProcess & use in service_test.cc / client_test.cc
// TODO(oschaaf): should we merge incoming request options with defaults?
// TODO(oschaaf): aggregate the client's logs and forward them in the grpc response.
grpc::Status ServiceImpl::ExecutionStream(
    grpc::ServerContext* /*context*/,
    grpc::ServerReaderWriter<nighthawk::client::ExecutionResponse,
                             nighthawk::client::ExecutionRequest>* stream) {
  nighthawk::client::ExecutionRequest request;
  // The execution this stream most recently started, if any. A stream runs
  // one at a time: it may start the next once the previous one has answered.
  std::shared_ptr<Execution> execution;

  while (stream->Read(&request)) {
    ENVOY_LOG(debug, "Read ExecutionRequest data {}", absl::StrCat(redactedForLog(request)));
    if (request.has_start_request()) {
      if (execution != nullptr) {
        bool done;
        {
          Envoy::Thread::LockGuard guard(execution->lock);
          done = execution->done;
        }
        if (!done) {
          return finishGrpcStream(execution.get(), false,
                                  "This stream already has an execution running.");
        }
        // Finished, or about to write its response: let the thread wind down
        // before this stream reuses the variable.
        execution->future.wait();
      }
      {
        // Counted here, on the stream thread, so two streams starting at once
        // cannot both be accepted into the last slot.
        Envoy::Thread::LockGuard guard(active_lock_);
        if (active_executions_ >= max_concurrent_executions_) {
          return finishGrpcStream(
              nullptr, false,
              max_concurrent_executions_ == 1
                  ? "Only a single benchmark session is allowed at a time."
                  : fmt::format("Busy: {} executions are running, the maximum this service "
                                "allows (--max-concurrent-executions).",
                                active_executions_));
        }
        active_executions_++;
      }
      execution = std::make_shared<Execution>();
      // std::launch::async: the run starts now, on its own thread, not when
      // the future is first waited on.
      try {
        execution->future = std::async(std::launch::async, &ServiceImpl::handleExecutionRequest,
                                       this, request, stream, execution);
      } catch (const std::system_error& e) {
        // No thread to run it on. Nothing was started, so nothing will ever release the slot
        // taken above: give it back here, or each such failure would leave the service one
        // execution closer to reporting busy forever.
        {
          Envoy::Thread::LockGuard guard(active_lock_);
          ASSERT(active_executions_ > 0);
          active_executions_--;
        }
        execution.reset();
        return finishGrpcStream(
            nullptr, false,
            fmt::format("Unable to start a thread for the execution: {}", e.what()));
      }
    } else if (request.has_cancellation_request()) {
      // Stops this stream's execution early; its response, with whatever it
      // collected, follows on this stream as usual. The stream stays open:
      // the client half-closes when it has read that response. Without an
      // execution of its own there is nothing to do, and that is not an error
      // -- a cancellation racing the end of a run is the expected case. Another
      // stream's execution is out of reach by construction: one client cannot
      // stop another's run.
      if (execution == nullptr) {
        ENVOY_LOG(info, "Cancellation requested by a stream that started no execution; ignored.");
      } else {
        Envoy::Thread::LockGuard guard(execution->lock);
        if (execution->process == nullptr) {
          ENVOY_LOG(info, "Cancellation requested with no active execution; nothing to cancel.");
        } else {
          ENVOY_LOG(info, "Cancelling the active execution on the client's request.");
          execution->process->requestExecutionCancellation();
        }
      }
    } else if (request.has_update_request()) {
      return finishGrpcStream(execution.get(), false, "Request is not supported yet.");
    } else {
      PANIC("not reached");
    }
  }
  return finishGrpcStream(execution.get(), true);
}

namespace {
void addHeader(envoy::config::core::v3::HeaderMap* map, absl::string_view key,
               absl::string_view value) {
  auto* request_header = map->add_headers();
  request_header->set_key(std::string(key));
  request_header->set_value(std::string(value));
}
} // namespace

RequestSourcePtr RequestSourceServiceImpl::createStaticEmptyRequestSource(const uint32_t amount) {
  Envoy::Http::RequestHeaderMapPtr header = Envoy::Http::RequestHeaderMapImpl::create();
  header->addCopy(Envoy::Http::LowerCaseString("x-from-remote-request-source"), "1");
  return std::make_unique<StaticRequestSourceImpl>(std::move(header), amount);
}

grpc::Status RequestSourceServiceImpl::RequestStream(
    grpc::ServerContext* /*context*/,
    grpc::ServerReaderWriter<nighthawk::request_source::RequestStreamResponse,
                             nighthawk::request_source::RequestStreamRequest>* stream) {
  nighthawk::request_source::RequestStreamRequest request;
  bool ok = true;
  while (stream->Read(&request)) {
    ENVOY_LOG(trace, "Inbound RequestStreamRequest {}", absl::StrCat(request));

    // TODO(oschaaf): this is useful for integration testing purposes, but sending
    // these nearly empty headers will basically be a near no-op (note that the client will merge
    // headers we send here into into own header configuration). The client can be configured to
    // connect to a custom grpc service as a remote data source instead of this one, and its workers
    // will comply. That in itself may be useful. But we could offer the following features here:
    // 1. Yet another remote request source, so we balance to-be-replayed headers over workers
    //    and only have a single stream to a remote service here.
    // 2. Read a and dispatch a header stream from disk.
    RequestSourcePtr request_source = createStaticEmptyRequestSource(request.quantity());
    RequestGenerator request_generator = request_source->get();
    RequestPtr request;
    while (ok && (request = request_generator()) != nullptr) {
      HeaderMapPtr headers = request->header();
      nighthawk::request_source::RequestStreamResponse response;
      auto* request_specifier = response.mutable_request_specifier();
      auto* request_headers = request_specifier->mutable_v3_headers();
      headers->iterate([&request_headers](const Envoy::Http::HeaderEntry& header)
                           -> Envoy::Http::HeaderMap::Iterate {
        addHeader(request_headers, header.key().getStringView(), header.value().getStringView());
        return Envoy::Http::RequestHeaderMap::Iterate::Continue;
      });
      // TODO(oschaaf): add static configuration for other fields plus expectations
      ok = ok && stream->Write(response);
    }
    if (!ok) {
      ENVOY_LOG(error, "Failed to send the complete set of replay data.");
      break;
    }
  }
  ENVOY_LOG(trace, "Finishing stream");
  return ok ? grpc::Status::OK : grpc::Status(grpc::StatusCode::INTERNAL, std::string("error"));
}

} // namespace Client
} // namespace Nighthawk
