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

#include "source/common/common/logger.h"
#include "source/common/common/thread.h"
#include "source/common/event/real_time_system.h"
#include "source/exe/process_wide.h"

#include "nighthawk/client/process.h"
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
   * Constructs a new ServiceImpl instance
   */
  ServiceImpl() : process_wide_(std::make_shared<Envoy::ProcessWide>()) {
    logging_context_ = std::make_unique<Envoy::Logger::Context>(
        spdlog::level::from_str("info"), "[%T.%f][%t][%L] %v", log_lock_, false);
  }

  ServiceImpl(std::unique_ptr<Envoy::Logger::Context>&& logging_context)
      : process_wide_(std::make_shared<Envoy::ProcessWide>()) {
    logging_context_ = std::move(logging_context);
  }

  grpc::Status
  ExecutionStream(grpc::ServerContext* context,
                  grpc::ServerReaderWriter<nighthawk::client::ExecutionResponse,
                                           nighthawk::client::ExecutionRequest>* stream) override;

private:
  using Stream = grpc::ServerReaderWriter<nighthawk::client::ExecutionResponse,
                                          nighthawk::client::ExecutionRequest>;
  void handleExecutionRequest(const nighthawk::client::ExecutionRequest& request, Stream* stream);
  void writeResponse(Stream* stream, const nighthawk::client::ExecutionResponse& response);
  grpc::Status finishGrpcStream(const bool owner, const bool success,
                                absl::string_view description = "");

  Envoy::Thread::MutexBasicLockable log_lock_;
  std::unique_ptr<Envoy::Logger::Context> logging_context_;
  std::shared_ptr<Envoy::ProcessWide> process_wide_;
  Envoy::Event::RealTimeSystem time_system_; // NO_CHECK_FORMAT(real_time)
  // Written only by the stream that starts an execution, and waited on only by
  // that stream; a stream the service turns away never touches it.
  std::future<void> future_;
  // accepted_lock_ and accepted_event_ are used to synchronize the threads
  // when starting up a future to service a test, and ensure the code servicing it
  // in the other thread has acquired busy_lock_.
  Envoy::Thread::MutexBasicLockable accepted_lock_;
  Envoy::Thread::CondVar accepted_event_;
  // busy_lock_ is used to test from the service thread to query if there's
  // an active test being run.
  Envoy::Thread::MutexBasicLockable busy_lock_;
  // The execution a CancellationRequest applies to: set by the thread running
  // it for as long as it runs, read by the stream thread. Guarded by
  // process_lock_, which the running thread also holds while clearing it, so a
  // cancellation never reaches a Process that is being shut down.
  // active_stream_ is the stream that started it: a cancellation from any
  // other stream is ignored, so one client cannot stop another's run.
  Envoy::Thread::MutexBasicLockable process_lock_;
  Process* active_process_{nullptr};
  Stream* active_stream_{nullptr};
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
