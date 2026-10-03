#include <grpc++/grpc++.h>

#include <chrono>
#include <thread>

#include "nighthawk/common/exception.h"

#include "engine/test/test_common/environment.h"
#include "test/test_common/network_utility.h"
#include "test/test_common/utility.h"

#include "engine/api/client/service.pb.h"

#include "engine/source/client/service_impl.h"

#include "gtest/gtest.h"

using namespace std::chrono_literals;
using namespace testing;

namespace Nighthawk {
namespace Client {
namespace {

using nighthawk::client::ExecutionRequest;
using nighthawk::client::ExecutionResponse;

class ServiceTest : public TestWithParam<Envoy::Network::Address::IpVersion> {
public:
  void SetUp() override {
    service_ = std::make_unique<ServiceImpl>();
    grpc::ServerBuilder builder;
    loopback_address_ = Envoy::Network::Test::getLoopbackAddressUrlString(GetParam());

    builder.AddListeningPort(fmt::format("{}:0", loopback_address_),
                             grpc::InsecureServerCredentials(), &grpc_server_port_);
    builder.RegisterService(service_.get());
    server_ = builder.BuildAndStart();
    setupGrpcClient();
    setBasicRequestOptions();
  }

  void TearDown() override { server_->Shutdown(); }

  void setupGrpcClient() {
    channel_ = grpc::CreateChannel(fmt::format("{}:{}", loopback_address_, grpc_server_port_),
                                   grpc::InsecureChannelCredentials());
    stub_ = std::make_unique<nighthawk::client::NighthawkService::Stub>(channel_);
  }

  void singleStreamBackToBackExecution(grpc::ClientContext& context,
                                       nighthawk::client::NighthawkService::Stub&) {
    auto r = stub_->ExecutionStream(&context);
    EXPECT_TRUE(r->Write(request_, {}));
    EXPECT_TRUE(r->Read(&response_));
    ASSERT_TRUE(response_.has_error_detail());
    EXPECT_TRUE(response_.has_output());
    EXPECT_TRUE(r->Write(request_, {}));
    EXPECT_TRUE(r->Read(&response_));
    EXPECT_TRUE(response_.has_error_detail());
    EXPECT_TRUE(response_.has_output());
    EXPECT_TRUE(r->WritesDone());
    auto status = r->Finish();
    EXPECT_TRUE(status.ok());
  }

  std::thread testThreadedClientRun(bool expect_success) const {
    std::thread thread([this, expect_success]() {
      auto channel = grpc::CreateChannel(fmt::format("{}:{}", loopback_address_, grpc_server_port_),
                                         grpc::InsecureChannelCredentials());
      auto stub = std::make_unique<nighthawk::client::NighthawkService::Stub>(channel);
      grpc::ClientContext context;
      auto stream = stub->ExecutionStream(&context);
      EXPECT_TRUE(stream->Write(request_, {}));
      EXPECT_TRUE(stream->WritesDone());
      nighthawk::client::ExecutionResponse response;
      EXPECT_EQ(stream->Read(&response), expect_success);
      auto status = stream->Finish();
      EXPECT_EQ(status.ok(), expect_success);
    });
    return thread;
  }

  void setBasicRequestOptions() {
    auto options = request_.mutable_start_request()->mutable_options();
    // TODO(oschaaf): this sends actual traffic, which isn't relevant for the tests
    // we are about to perform. However, it would be nice to be able to mock out things
    // to clean this up.
    options->mutable_uri()->set_value("http://127.0.0.1:10001/");
    options->mutable_duration()->set_seconds(2);
    options->mutable_requests_per_second()->set_value(3);
  }

  void runWithFailingValidationExpectations(bool expect_output,
                                            absl::string_view match_error = "") {
    auto r = stub_->ExecutionStream(&context_);
    r->Write(request_, {});
    r->WritesDone();
    EXPECT_TRUE(r->Read(&response_));
    auto status = r->Finish();
    ASSERT_FALSE(match_error.empty());
    EXPECT_TRUE(response_.has_error_detail());
    EXPECT_EQ(response_.has_output(), expect_output);
    EXPECT_EQ(grpc::StatusCode::INTERNAL, response_.error_detail().code());
    EXPECT_THAT(response_.error_detail().message(), HasSubstr(std::string(match_error)));
    EXPECT_TRUE(status.ok());
  }

  std::unique_ptr<ServiceImpl> service_;
  std::unique_ptr<grpc::Server> server_;
  std::shared_ptr<grpc::Channel> channel_;
  grpc::ClientContext context_;
  nighthawk::client::ExecutionRequest request_;
  nighthawk::client::ExecutionResponse response_;
  std::unique_ptr<nighthawk::client::NighthawkService::Stub> stub_;
  std::string loopback_address_;
  int grpc_server_port_{0};
};

class ServiceTestWithParameterizedConstructor : public ServiceTest {
public:
  void SetUp() override {
    std::unique_ptr<Envoy::Logger::Context> logging_context =
        std::make_unique<Envoy::Logger::Context>(spdlog::level::info, "%L %n [%g:%#] %v", log_lock_,
                                                 false);
    service_ = std::make_unique<ServiceImpl>(std::move(logging_context));
    grpc::ServerBuilder builder;
    loopback_address_ = Envoy::Network::Test::getLoopbackAddressUrlString(GetParam());

    builder.AddListeningPort(fmt::format("{}:0", loopback_address_),
                             grpc::InsecureServerCredentials(), &grpc_server_port_);
    builder.RegisterService(service_.get());
    server_ = builder.BuildAndStart();
    setupGrpcClient();
    setBasicRequestOptions();
  }

private:
  Envoy::Thread::MutexBasicLockable log_lock_;
};

INSTANTIATE_TEST_SUITE_P(IpVersions, ServiceTestWithParameterizedConstructor,
                         ValuesIn(Envoy::TestEnvironment::getIpVersionsForTest()),
                         Envoy::TestUtility::ipTestParamsToString);

TEST_P(ServiceTestWithParameterizedConstructor,
       ConstructorWithLoggingContextParameterCanRespondToRequests) {
  std::unique_ptr<grpc::ClientReaderWriter<ExecutionRequest, ExecutionResponse>> stream =
      stub_->ExecutionStream(&context_);
  stream->Write(request_, {});
  stream->WritesDone();
  EXPECT_TRUE(stream->Read(&response_));
  ASSERT_TRUE(response_.has_error_detail());
  EXPECT_FALSE(response_.error_detail().message().empty());
  EXPECT_TRUE(response_.has_output());
  EXPECT_GE(response_.output().results(0).counters().size(), 8);
  grpc::Status status = stream->Finish();
  EXPECT_TRUE(status.ok());
}

INSTANTIATE_TEST_SUITE_P(IpVersions, ServiceTest,
                         ValuesIn(Envoy::TestEnvironment::getIpVersionsForTest()),
                         Envoy::TestUtility::ipTestParamsToString);

// Test single NH run
TEST_P(ServiceTest, Basic) {
  auto r = stub_->ExecutionStream(&context_);
  r->Write(request_, {});
  r->WritesDone();
  EXPECT_TRUE(r->Read(&response_));
  ASSERT_TRUE(response_.has_error_detail());
  EXPECT_THAT(response_.error_detail().message(), HasSubstr(std::string("Unknown failure")));
  EXPECT_TRUE(response_.has_output());
  EXPECT_GE(response_.output().results(0).counters().size(), 8);
  auto status = r->Finish();
  EXPECT_TRUE(status.ok());
}

// Test that attempts to perform concurrent executions result in a
// failure being returned.
TEST_P(ServiceTest, NoConcurrentStart) {
  auto r = stub_->ExecutionStream(&context_);
  EXPECT_TRUE(r->Write(request_, {}));
  EXPECT_TRUE(r->Write(request_, {}));
  EXPECT_TRUE(r->WritesDone());
  EXPECT_TRUE(r->Read(&response_));
  ASSERT_TRUE(response_.has_error_detail());
  EXPECT_THAT(response_.error_detail().message(), HasSubstr(std::string("Unknown failure")));
  EXPECT_TRUE(response_.has_output());
  EXPECT_FALSE(r->Read(&response_));
  auto status = r->Finish();
  EXPECT_FALSE(status.ok());
}

// Test we are able to perform serialized executions.
TEST_P(ServiceTest, BackToBackExecution) {
  grpc::ClientContext context1;
  singleStreamBackToBackExecution(context1, *stub_);
  // create a new client to connect to the same server, and do it one more time.
  setupGrpcClient();
  grpc::ClientContext context2;
  singleStreamBackToBackExecution(context2, *stub_);
}

// Test that proto validation is wired up and works.
// TODO(oschaaf): functional coverage of all the options / validations.
TEST_P(ServiceTest, InvalidRps) {
  auto options = request_.mutable_start_request()->mutable_options();
  options->mutable_requests_per_second()->set_value(0);
  // We do not expect output, because the options proto is not valid, and can't be echoed back.
  runWithFailingValidationExpectations(false, "value must be inside range");
}

// We didn't implement updates yet, ensure we indicate so.
TEST_P(ServiceTest, UpdatesNotSupported) {
  request_ = nighthawk::client::ExecutionRequest();
  request_.mutable_update_request();
  auto r = stub_->ExecutionStream(&context_);
  r->Write(request_, {});
  r->WritesDone();
  EXPECT_FALSE(r->Read(&response_));
  auto status = r->Finish();
  EXPECT_THAT(status.error_message(), HasSubstr("Request is not supported yet"));
  EXPECT_FALSE(status.ok());
}

// A cancellation with nothing running is not an error: it races the end of a
// run in practice, and the stream simply continues.
TEST_P(ServiceTest, CancelWithoutExecutionIsIgnored) {
  request_ = nighthawk::client::ExecutionRequest();
  request_.mutable_cancellation_request();
  auto r = stub_->ExecutionStream(&context_);
  EXPECT_TRUE(r->Write(request_, {}));
  EXPECT_TRUE(r->WritesDone());
  EXPECT_FALSE(r->Read(&response_));
  auto status = r->Finish();
  EXPECT_TRUE(status.ok());
}

// A cancellation ends the active execution early, and its response -- with
// whatever was collected -- arrives on the same stream.
TEST_P(ServiceTest, CancelStopsARunningExecution) {
  auto options = request_.mutable_start_request()->mutable_options();
  // Long enough that only a cancellation can end it in time. The target does
  // not exist, so the default failure predicates would end the run at the
  // first connection failure; a custom one keeps it going.
  options->mutable_duration()->set_seconds(60);
  (*options->mutable_failure_predicates())["benchmark.nonexistent"] = 0;
  auto r = stub_->ExecutionStream(&context_);
  EXPECT_TRUE(r->Write(request_, {}));
  // Give the execution a moment to actually be running.
  std::this_thread::sleep_for(std::chrono::seconds(2)); // NO_CHECK_FORMAT(real_time)
  nighthawk::client::ExecutionRequest cancel;
  cancel.mutable_cancellation_request();
  EXPECT_TRUE(r->Write(cancel, {}));
  EXPECT_TRUE(r->WritesDone());
  const auto started = std::chrono::steady_clock::now(); // NO_CHECK_FORMAT(real_time)
  EXPECT_TRUE(r->Read(&response_));
  const auto waited = std::chrono::steady_clock::now() - started; // NO_CHECK_FORMAT(real_time)
  EXPECT_LT(waited, std::chrono::seconds(30)) << "the response did not arrive promptly";
  EXPECT_TRUE(response_.has_output());
  EXPECT_FALSE(r->Read(&response_));
  auto status = r->Finish();
  EXPECT_TRUE(status.ok());
}

TEST_P(ServiceTest, Unresolvable) {
  auto options = request_.mutable_start_request()->mutable_options();
  options->mutable_uri()->set_value("http://unresolvable-host/");
  // We expect output, because the options proto is valid.
  runWithFailingValidationExpectations(false, "Unable to create ProcessImpl");
}

} // namespace
} // namespace Client
} // namespace Nighthawk
