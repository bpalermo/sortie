#pragma once
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic warning "-Wunused-parameter"
#endif
#include "engine/api/client/service.grpc.pb.h"
#include "engine/api/request_source/service.grpc.pb.h"

#ifdef __clang__
#pragma clang diagnostic pop
#endif

#include <future>
#include <memory>

#include "absl/base/thread_annotations.h"

#include "source/common/common/logger.h"
#include "source/common/common/thread.h"
#include "source/common/event/real_time_system.h"
#include "source/exe/process_wide.h"

#include "nighthawk/client/process.h"

#include "engine/source/client/process_impl.h"
#include "nighthawk/common/request_source.h"

namespace Nighthawk {
namespace Client {

/**
 * Implements Nighthawk's gRPC service. This service allows load generation to be
 * controlled by gRPC clients.
 */
class ServiceImpl final : public nighthawk::client::NighthawkService::Service,
                          public Envoy::Logger::Loggable<Envoy::Logger::Id::main> {

public:
  /**
   * Constructs a new ServiceImpl instance.
   *
   * @param max_concurrent_executions how many executions may run at once, each
   * started by its own stream. 1 is the historical behaviour (a second start is
   * refused as busy). Every execution is a Process of its own -- its own Envoy
   * cluster manager, worker threads and stats store -- so N concurrent
   * executions cost N times the threads a single one asks for.
   */
  explicit ServiceImpl(uint32_t max_concurrent_executions = 1)
      : process_wide_(std::make_shared<Envoy::ProcessWide>()),
        max_concurrent_executions_(max_concurrent_executions) {
    logging_context_ = std::make_unique<Envoy::Logger::Context>(
        spdlog::level::from_str("info"), "[%T.%f][%t][%L] %v", log_lock_, false);
    service_verbosity_ = currentVerbosity();
    // Before any Process exists, so no dispatcher is ever created while the environment is
    // being written (see ProcessImpl::setupForHRTimers).
    ProcessImpl::setupForHRTimers();
  }
  ServiceImpl(std::unique_ptr<Envoy::Logger::Context>&& logging_context,
              uint32_t max_concurrent_executions = 1)
      : process_wide_(std::make_shared<Envoy::ProcessWide>()),
        max_concurrent_executions_(max_concurrent_executions) {
    logging_context_ = std::move(logging_context);
    service_verbosity_ = currentVerbosity();
    // Before any Process exists, so no dispatcher is ever created while the environment is
    // being written (see ProcessImpl::setupForHRTimers).
    ProcessImpl::setupForHRTimers();
  }

  grpc::Status
  ExecutionStream(grpc::ServerContext* context,
                  grpc::ServerReaderWriter<nighthawk::client::ExecutionResponse,
                                           nighthawk::client::ExecutionRequest>* stream) override;

private:
  using Stream = grpc::ServerReaderWriter<nighthawk::client::ExecutionResponse,
                                          nighthawk::client::ExecutionRequest>;
  /**
   * The execution one stream started: the Process while it runs, for a
   * cancellation from that same stream, and the thread running it, which the
   * stream joins before it finishes. One per stream, so a cancellation can
   * only ever reach the run its own stream started.
   */
  struct Execution {
    Envoy::Thread::MutexBasicLockable lock;
    // Set by the running thread for as long as the Process runs, cleared --
    // under lock -- before the Process is destroyed, so a late cancellation
    // finds nothing rather than a Process mid-teardown.
    Process* process ABSL_GUARDED_BY(lock){nullptr};
    // Set once the run has released its slot and is about to write its final
    // response: from then on the stream may start its next execution.
    bool done ABSL_GUARDED_BY(lock){false};
    std::future<void> future;
  };

  void handleExecutionRequest(const nighthawk::client::ExecutionRequest& request, Stream* stream,
                              std::shared_ptr<Execution> execution);
  void writeResponse(Stream* stream, const nighthawk::client::ExecutionResponse& response);
  grpc::Status finishGrpcStream(Execution* execution, const bool success,
                                absl::string_view description = "");

  Envoy::Thread::MutexBasicLockable log_lock_;
  std::unique_ptr<Envoy::Logger::Context> logging_context_;
  std::shared_ptr<Envoy::ProcessWide> process_wide_;
  Envoy::Event::RealTimeSystem time_system_; // NO_CHECK_FORMAT(real_time)
  const uint32_t max_concurrent_executions_;
  // The level this service logs at: read back from the logging context it was constructed
  // with, and what every execution of a concurrent service runs at (the log level is
  // process-wide, so a request's own verbosity cannot be honoured there).
  nighthawk::client::Verbosity::VerbosityOptions service_verbosity_{
      nighthawk::client::Verbosity::INFO};
  static nighthawk::client::Verbosity::VerbosityOptions currentVerbosity();
  // Hands the allocator's free memory back to the OS. Called where this service has just
  // freed a lot of it: after a progress snapshot that copied histograms, and when an
  // execution's Process is gone.
  static void releaseFreeMemory();
  // How many executions are running right now, across all streams. Taken when
  // a start is accepted -- on the stream's thread, before the run's thread
  // exists, so a second start racing the first is counted correctly -- and
  // given back by the running thread before it writes its final response.
  Envoy::Thread::MutexBasicLockable active_lock_;
  uint32_t active_executions_ ABSL_GUARDED_BY(active_lock_){0};
};

/**
 * Dummy implementation of our request-source gRPC service definition, for testing and experimental
 * purposes.
 */
class RequestSourceServiceImpl final
    : public nighthawk::request_source::NighthawkRequestSourceService::Service,
      public Envoy::Logger::Loggable<Envoy::Logger::Id::main> {

public:
  /**
   * Constructs a new RequestSourceServiceImpl instance.
   */
  RequestSourceServiceImpl() {
    logging_context_ = std::make_unique<Envoy::Logger::Context>(
        spdlog::level::from_str("info"), "[%T.%f][%t][%L] %v", log_lock_, false);
  }

  grpc::Status RequestStream(
      grpc::ServerContext* context,
      grpc::ServerReaderWriter<nighthawk::request_source::RequestStreamResponse,
                               nighthawk::request_source::RequestStreamRequest>* stream) override;

private:
  Envoy::Thread::MutexBasicLockable log_lock_;
  std::unique_ptr<Envoy::Logger::Context> logging_context_;
  RequestSourcePtr createStaticEmptyRequestSource(const uint32_t amount);
};

} // namespace Client
} // namespace Nighthawk
