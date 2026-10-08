#include <grpc++/grpc++.h>

#include <chrono>
#include <thread>
#include <tuple>

#include "nighthawk/common/exception.h"

#include "engine/test/test_common/environment.h"
#include "test/test_common/network_utility.h"
#include "test/test_common/utility.h"

#include "envoy/config/metrics/v3/stats.pb.h"

#include "engine/api/client/service.pb.h"
#include "engine/api/stats_sink/envoy_stats_sink_adapter.pb.h"

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
  // How many executions the service under test runs at once; 1 is the default.
  virtual uint32_t maxConcurrentExecutions() { return 1; }

  void SetUp() override {
    service_ = std::make_unique<ServiceImpl>(maxConcurrentExecutions());
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

  // The cap a refused stream names in its trailing metadata, or an empty string when it names
  // none. Valid once the stream has finished.
  static std::string maxConcurrentExecutionsTrailer(const grpc::ClientContext& context) {
    const auto& trailers = context.GetServerTrailingMetadata();
    const auto it = trailers.find(ServiceImpl::MaxConcurrentExecutionsTrailer);
    return it == trailers.end() ? std::string() : std::string(it->second.data(), it->second.size());
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

// A cancellation sent right behind the start -- before the service has built the run -- is
// not lost: the run ends at once instead of going on for its whole duration. That is what a
// client does when a sibling execution it started alongside this one was refused.
TEST_P(ServiceTest, CancelRightBehindTheStartIsNotLost) {
  auto options = request_.mutable_start_request()->mutable_options();
  options->mutable_duration()->set_seconds(60);
  (*options->mutable_failure_predicates())["benchmark.nonexistent"] = 0;
  auto r = stub_->ExecutionStream(&context_);
  nighthawk::client::ExecutionRequest cancel;
  cancel.mutable_cancellation_request();
  EXPECT_TRUE(r->Write(request_, {}));
  EXPECT_TRUE(r->Write(cancel, {}));
  EXPECT_TRUE(r->WritesDone());
  const auto started = std::chrono::steady_clock::now(); // NO_CHECK_FORMAT(real_time)
  EXPECT_TRUE(r->Read(&response_));
  const auto waited = std::chrono::steady_clock::now() - started; // NO_CHECK_FORMAT(real_time)
  EXPECT_LT(waited, std::chrono::seconds(30)) << "the cancellation was dropped";
  EXPECT_FALSE(r->Read(&response_));
  EXPECT_TRUE(r->Finish().ok());
}

// Only the stream that started an execution can cancel it. Another client's
// stream sending a cancellation is answered normally and changes nothing: a
// start request on it afterwards is still refused as busy, and the owner's
// own cancellation then ends the run.
TEST_P(ServiceTest, CancelFromAnotherStreamIsIgnored) {
  auto options = request_.mutable_start_request()->mutable_options();
  options->mutable_duration()->set_seconds(60);
  (*options->mutable_failure_predicates())["benchmark.nonexistent"] = 0;
  auto owner = stub_->ExecutionStream(&context_);
  EXPECT_TRUE(owner->Write(request_, {}));
  std::this_thread::sleep_for(std::chrono::seconds(2)); // NO_CHECK_FORMAT(real_time)

  nighthawk::client::ExecutionRequest cancel;
  cancel.mutable_cancellation_request();
  {
    grpc::ClientContext other_context;
    auto other = stub_->ExecutionStream(&other_context);
    EXPECT_TRUE(other->Write(cancel, {}));
    EXPECT_TRUE(other->WritesDone());
    nighthawk::client::ExecutionResponse response;
    EXPECT_FALSE(other->Read(&response));
    EXPECT_TRUE(other->Finish().ok());
  }
  {
    // Still running, so a new start is refused -- and refused promptly, not
    // after the owner's run ends.
    grpc::ClientContext other_context;
    auto other = stub_->ExecutionStream(&other_context);
    EXPECT_TRUE(other->Write(request_, {}));
    EXPECT_TRUE(other->WritesDone());
    nighthawk::client::ExecutionResponse response;
    const auto started = std::chrono::steady_clock::now(); // NO_CHECK_FORMAT(real_time)
    EXPECT_FALSE(other->Read(&response));
    const auto status = other->Finish();
    const auto waited = std::chrono::steady_clock::now() - started; // NO_CHECK_FORMAT(real_time)
    EXPECT_FALSE(status.ok());
    // The request was fine and the service is full: RESOURCE_EXHAUSTED, with the cap in a
    // trailer so that a client can tell this from any other exhaustion.
    EXPECT_EQ(grpc::StatusCode::RESOURCE_EXHAUSTED, status.error_code());
    EXPECT_EQ("1", maxConcurrentExecutionsTrailer(other_context));
    EXPECT_THAT(status.error_message(), HasSubstr("Only a single benchmark session"));
    EXPECT_LT(waited, std::chrono::seconds(10)) << "busy was reported only after the run";
  }

  EXPECT_TRUE(owner->Write(cancel, {}));
  EXPECT_TRUE(owner->WritesDone());
  const auto started = std::chrono::steady_clock::now(); // NO_CHECK_FORMAT(real_time)
  EXPECT_TRUE(owner->Read(&response_));
  const auto waited = std::chrono::steady_clock::now() - started; // NO_CHECK_FORMAT(real_time)
  EXPECT_LT(waited, std::chrono::seconds(30)) << "the response did not arrive promptly";
  EXPECT_TRUE(response_.has_output());
  EXPECT_FALSE(owner->Read(&response_));
  EXPECT_TRUE(owner->Finish().ok());
}

// A stream runs one execution at a time (back to back is fine, see above): a
// second start while the first still runs is refused, whatever the service's
// concurrency allows.
TEST_P(ServiceTest, SecondStartOnTheSameStreamIsRefused) {
  auto options = request_.mutable_start_request()->mutable_options();
  options->mutable_duration()->set_seconds(3);
  (*options->mutable_failure_predicates())["benchmark.nonexistent"] = 0;
  auto r = stub_->ExecutionStream(&context_);
  EXPECT_TRUE(r->Write(request_, {}));
  std::this_thread::sleep_for(std::chrono::seconds(1)); // NO_CHECK_FORMAT(real_time)
  EXPECT_TRUE(r->Write(request_, {}));
  EXPECT_TRUE(r->WritesDone());
  // As on every error path, the stream ends only after the run it started has
  // put its response on the stream; then comes the error status.
  EXPECT_TRUE(r->Read(&response_));
  EXPECT_TRUE(response_.has_output());
  EXPECT_FALSE(r->Read(&response_));
  const auto status = r->Finish();
  EXPECT_FALSE(status.ok());
  EXPECT_THAT(status.error_message(), HasSubstr("already has an execution running"));
  // A client misusing its own stream, not a full service: not the code, or the trailer, of a
  // service at its cap.
  EXPECT_EQ(grpc::StatusCode::INTERNAL, status.error_code());
  EXPECT_EQ("", maxConcurrentExecutionsTrailer(context_));
}

class ConcurrentServiceTest : public ServiceTest {
public:
  uint32_t maxConcurrentExecutions() override { return 2; }
};

INSTANTIATE_TEST_SUITE_P(IpVersions, ConcurrentServiceTest,
                         ValuesIn(Envoy::TestEnvironment::getIpVersionsForTest()),
                         Envoy::TestUtility::ipTestParamsToString);

// With --max-concurrent-executions 2, two streams run at once, each cancels
// only its own execution, and a third start is refused as busy while both run.
TEST_P(ConcurrentServiceTest, TwoExecutionsRunAtOnceAndEachStreamCancelsItsOwn) {
  auto options = request_.mutable_start_request()->mutable_options();
  options->mutable_duration()->set_seconds(60);
  (*options->mutable_failure_predicates())["benchmark.nonexistent"] = 0;
  grpc::ClientContext context_a;
  grpc::ClientContext context_b;
  auto a = stub_->ExecutionStream(&context_a);
  auto b = stub_->ExecutionStream(&context_b);
  EXPECT_TRUE(a->Write(request_, {}));
  EXPECT_TRUE(b->Write(request_, {}));
  std::this_thread::sleep_for(std::chrono::seconds(2)); // NO_CHECK_FORMAT(real_time)
  {
    grpc::ClientContext context_c;
    auto c = stub_->ExecutionStream(&context_c);
    EXPECT_TRUE(c->Write(request_, {}));
    EXPECT_TRUE(c->WritesDone());
    nighthawk::client::ExecutionResponse response;
    EXPECT_FALSE(c->Read(&response));
    const auto status = c->Finish();
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(grpc::StatusCode::RESOURCE_EXHAUSTED, status.error_code());
    EXPECT_EQ("2", maxConcurrentExecutionsTrailer(context_c));
    EXPECT_THAT(status.error_message(), HasSubstr("Busy: 2 executions are running"));
  }
  nighthawk::client::ExecutionRequest cancel;
  cancel.mutable_cancellation_request();
  // Cancelling a ends a only: b is still running afterwards, so a new start is
  // accepted into the slot a released, and refused once that one runs too.
  EXPECT_TRUE(a->Write(cancel, {}));
  EXPECT_TRUE(a->WritesDone());
  nighthawk::client::ExecutionResponse response_a;
  EXPECT_TRUE(a->Read(&response_a));
  EXPECT_TRUE(response_a.has_output());
  EXPECT_FALSE(a->Read(&response_a));
  EXPECT_TRUE(a->Finish().ok());
  {
    grpc::ClientContext context_d;
    auto d = stub_->ExecutionStream(&context_d);
    EXPECT_TRUE(d->Write(request_, {}));
    std::this_thread::sleep_for(std::chrono::seconds(1)); // NO_CHECK_FORMAT(real_time)
    {
      // b survived a's cancellation: with d running too both slots are taken, so one more
      // start is refused. Had cancelling a stopped b as well, this would be accepted.
      grpc::ClientContext context_e;
      auto e = stub_->ExecutionStream(&context_e);
      EXPECT_TRUE(e->Write(request_, {}));
      EXPECT_TRUE(e->WritesDone());
      nighthawk::client::ExecutionResponse response_e;
      EXPECT_FALSE(e->Read(&response_e));
      const auto status = e->Finish();
      EXPECT_FALSE(status.ok());
      EXPECT_EQ(grpc::StatusCode::RESOURCE_EXHAUSTED, status.error_code());
      EXPECT_THAT(status.error_message(), HasSubstr("Busy: 2 executions are running"));
    }
    EXPECT_TRUE(d->Write(cancel, {}));
    EXPECT_TRUE(d->WritesDone());
    nighthawk::client::ExecutionResponse response_d;
    EXPECT_TRUE(d->Read(&response_d));
    EXPECT_TRUE(response_d.has_output());
    EXPECT_FALSE(d->Read(&response_d));
    EXPECT_TRUE(d->Finish().ok());
  }
  EXPECT_TRUE(b->Write(cancel, {}));
  EXPECT_TRUE(b->WritesDone());
  nighthawk::client::ExecutionResponse response_b;
  EXPECT_TRUE(b->Read(&response_b));
  EXPECT_TRUE(response_b.has_output());
  EXPECT_FALSE(b->Read(&response_b));
  EXPECT_TRUE(b->Finish().ok());
}

// The log level is process-wide, so a service that runs executions concurrently does not
// let one request change it for the others: the options the run reports back carry the
// service's level, not the one asked for.
TEST_P(ConcurrentServiceTest, RequestVerbosityIsNotApplied) {
  auto options = request_.mutable_start_request()->mutable_options();
  options->mutable_duration()->set_seconds(1);
  options->mutable_verbosity()->set_value(nighthawk::client::Verbosity::TRACE);
  (*options->mutable_failure_predicates())["benchmark.nonexistent"] = 0;
  auto r = stub_->ExecutionStream(&context_);
  EXPECT_TRUE(r->Write(request_, {}));
  EXPECT_TRUE(r->WritesDone());
  EXPECT_TRUE(r->Read(&response_));
  ASSERT_TRUE(response_.has_output());
  EXPECT_EQ(nighthawk::client::Verbosity::INFO, response_.output().options().verbosity().value());
  EXPECT_FALSE(r->Read(&response_));
  EXPECT_TRUE(r->Finish().ok());
}

// Nor does a request that names no verbosity get the options' own default: every execution
// of a concurrent service runs at the one level.
TEST_P(ConcurrentServiceTest, OmittedVerbosityIsTheServiceLevelToo) {
  auto options = request_.mutable_start_request()->mutable_options();
  options->mutable_duration()->set_seconds(1);
  options->clear_verbosity();
  (*options->mutable_failure_predicates())["benchmark.nonexistent"] = 0;
  auto r = stub_->ExecutionStream(&context_);
  EXPECT_TRUE(r->Write(request_, {}));
  EXPECT_TRUE(r->WritesDone());
  EXPECT_TRUE(r->Read(&response_));
  ASSERT_TRUE(response_.has_output());
  EXPECT_EQ(nighthawk::client::Verbosity::INFO, response_.output().options().verbosity().value());
  EXPECT_FALSE(r->Read(&response_));
  EXPECT_TRUE(r->Finish().ok());
}

// With progress_interval set, interim responses carrying `progress` and a snapshot of the
// run arrive while it is in flight; the final response has no `progress` and ends the stream.
TEST_P(ServiceTest, ProgressIsStreamedWhenRequested) {
  auto options = request_.mutable_start_request()->mutable_options();
  options->mutable_duration()->set_seconds(3);
  (*options->mutable_failure_predicates())["benchmark.nonexistent"] = 0;
  request_.mutable_start_request()->mutable_progress_interval()->set_nanos(500000000);
  auto r = stub_->ExecutionStream(&context_);
  EXPECT_TRUE(r->Write(request_, {}));
  EXPECT_TRUE(r->WritesDone());
  int interim = 0;
  int interim_with_statistics = 0;
  bool final_seen = false;
  nighthawk::client::ExecutionResponse response;
  while (r->Read(&response)) {
    EXPECT_FALSE(final_seen) << "a response followed the final one";
    EXPECT_TRUE(response.has_output());
    if (response.has_progress()) {
      interim++;
      EXPECT_GT(response.progress().elapsed().seconds() * 1000000000LL +
                    response.progress().elapsed().nanos(),
                0);
      ASSERT_FALSE(response.output().results().empty());
      EXPECT_EQ(response.output().results(0).name(), "global");
      // Summaries by default: the statistics are there, without percentiles, since those
      // would take a copy of every worker's histograms per snapshot. Not in every snapshot,
      // though: a worker that has not answered within a second is left out, and the snapshot
      // goes with the counters alone (ProcessImpl::snapshot()). A sanitizer build on a busy
      // machine has snapshots like that, so what is required is that some carry statistics.
      interim_with_statistics += response.output().results(0).statistics_size() > 0 ? 1 : 0;
      for (const auto& statistic : response.output().results(0).statistics()) {
        EXPECT_EQ(statistic.percentiles_size(), 0) << statistic.id();
      }
    } else {
      final_seen = true;
      // The final response is unaffected: statistics in full.
      bool percentiles = false;
      for (const auto& result : response.output().results()) {
        for (const auto& statistic : result.statistics()) {
          percentiles = percentiles || statistic.percentiles_size() > 0;
        }
      }
      EXPECT_TRUE(percentiles);
    }
  }
  EXPECT_GE(interim, 2) << "expected about six interim responses in a 3 s run";
  EXPECT_GE(interim_with_statistics, 1) << "no interim response carried any statistics";
  EXPECT_TRUE(final_seen);
  EXPECT_TRUE(r->Finish().ok());
}

// progress_statistics opts in to snapshots that carry the statistics in full.
TEST_P(ServiceTest, ProgressCarriesFullStatisticsWhenAskedFor) {
  auto options = request_.mutable_start_request()->mutable_options();
  options->mutable_duration()->set_seconds(3);
  options->mutable_requests_per_second()->set_value(20);
  (*options->mutable_failure_predicates())["benchmark.nonexistent"] = 0;
  request_.mutable_start_request()->mutable_progress_interval()->set_nanos(500000000);
  request_.mutable_start_request()->set_progress_statistics(true);
  auto r = stub_->ExecutionStream(&context_);
  EXPECT_TRUE(r->Write(request_, {}));
  EXPECT_TRUE(r->WritesDone());
  int interim = 0;
  bool percentiles = false;
  nighthawk::client::ExecutionResponse response;
  while (r->Read(&response)) {
    if (!response.has_progress()) {
      continue;
    }
    interim++;
    ASSERT_FALSE(response.output().results().empty());
    for (const auto& statistic : response.output().results(0).statistics()) {
      if (statistic.count() > 0) {
        EXPECT_GT(statistic.percentiles_size(), 0) << statistic.id();
        percentiles = true;
      }
    }
  }
  EXPECT_GE(interim, 2);
  EXPECT_TRUE(percentiles) << "no snapshot carried a statistic with samples";
  EXPECT_TRUE(r->Finish().ok());
}

// progress_statistics alone asks for nothing: there is no progress to carry them.
TEST_P(ServiceTest, ProgressStatisticsWithoutAnIntervalIsIgnored) {
  auto options = request_.mutable_start_request()->mutable_options();
  options->mutable_duration()->set_seconds(1);
  (*options->mutable_failure_predicates())["benchmark.nonexistent"] = 0;
  request_.mutable_start_request()->set_progress_statistics(true);
  auto r = stub_->ExecutionStream(&context_);
  EXPECT_TRUE(r->Write(request_, {}));
  EXPECT_TRUE(r->WritesDone());
  EXPECT_TRUE(r->Read(&response_));
  EXPECT_FALSE(response_.has_progress());
  EXPECT_FALSE(r->Read(&response_));
  EXPECT_TRUE(r->Finish().ok());
}

// An interval that cannot be honoured is refused rather than silently ignored.
TEST_P(ServiceTest, ProgressIntervalBelowAMillisecondIsAnError) {
  request_.mutable_start_request()->mutable_progress_interval()->set_nanos(-1);
  auto r = stub_->ExecutionStream(&context_);
  EXPECT_TRUE(r->Write(request_, {}));
  EXPECT_TRUE(r->WritesDone());
  EXPECT_TRUE(r->Read(&response_));
  EXPECT_TRUE(response_.has_error_detail());
  EXPECT_EQ(grpc::StatusCode::INVALID_ARGUMENT, response_.error_detail().code());
  EXPECT_THAT(response_.error_detail().message(), HasSubstr("at least 1ms"));
  EXPECT_FALSE(r->Read(&response_));
  EXPECT_TRUE(r->Finish().ok());
}

// A sink prefix that asks for the backend's name, sent to a service that has none, ends the
// execution before it starts: the alternative is the placeholder itself in every metric name.
TEST_P(ServiceTest, BackendNameTokenWithoutANameIsAnError) {
  envoy::config::metrics::v3::StatsdSink statsd;
  statsd.set_prefix("sortie.soak.%BACKEND%");
  statsd.mutable_address()->mutable_socket_address()->set_address("127.0.0.1");
  statsd.mutable_address()->mutable_socket_address()->set_port_value(8125);
  nighthawk::EnvoyStatsSinkAdapterConfig adapter;
  adapter.mutable_sink()->set_name("envoy.stat_sinks.statsd");
  std::ignore = adapter.mutable_sink()->mutable_typed_config()->PackFrom(statsd);
  auto* sink = request_.mutable_start_request()->mutable_options()->add_stats_sinks();
  sink->set_name("nighthawk.envoy_stats_sink_adapter");
  std::ignore = sink->mutable_typed_config()->PackFrom(adapter);

  auto r = stub_->ExecutionStream(&context_);
  EXPECT_TRUE(r->Write(request_, {}));
  EXPECT_TRUE(r->WritesDone());
  EXPECT_TRUE(r->Read(&response_));
  EXPECT_TRUE(response_.has_error_detail());
  EXPECT_FALSE(response_.has_output());
  EXPECT_EQ(grpc::StatusCode::INVALID_ARGUMENT, response_.error_detail().code());
  EXPECT_THAT(response_.error_detail().message(), HasSubstr("--backend-name"));
  EXPECT_FALSE(r->Read(&response_));
  EXPECT_TRUE(r->Finish().ok());
}

// Without progress_interval nothing precedes the final response, as before.
TEST_P(ServiceTest, NoProgressUnlessRequested) {
  auto options = request_.mutable_start_request()->mutable_options();
  options->mutable_duration()->set_seconds(2);
  (*options->mutable_failure_predicates())["benchmark.nonexistent"] = 0;
  auto r = stub_->ExecutionStream(&context_);
  EXPECT_TRUE(r->Write(request_, {}));
  EXPECT_TRUE(r->WritesDone());
  EXPECT_TRUE(r->Read(&response_));
  EXPECT_FALSE(response_.has_progress());
  EXPECT_FALSE(r->Read(&response_));
  EXPECT_TRUE(r->Finish().ok());
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
