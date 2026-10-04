#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "envoy/network/io_handle.h"

#include "source/common/buffer/buffer_impl.h"
#include "source/common/http/header_map_impl.h"
#include "source/common/network/socket_impl.h"
#include "source/common/network/utility.h"
#include "source/common/stats/isolated_store_impl.h"
#include "source/exe/process_wide.h"

#include "test/mocks/upstream/mocks.h"
#include "test/test_common/utility.h"

#include "engine/source/client/udp_benchmark_client_impl.h"
#include "engine/source/common/request_impl.h"
#include "engine/source/common/statistic_impl.h"

#include "gtest/gtest.h"

using namespace testing;

namespace Nighthawk {
namespace Client {
namespace {

using namespace std::chrono_literals;

// The peer: a real UDP socket on the loopback that the test's dispatcher reads, echoing what it
// gets -- or dropping or duplicating it, as the test decides.
class UdpBenchmarkClientTest : public Test {
public:
  UdpBenchmarkClientTest()
      : api_(Envoy::Api::createApiForTest(time_system_)),
        dispatcher_(api_->allocateDispatcher("test_thread")),
        cluster_manager_(std::make_unique<Envoy::Upstream::MockClusterManager>()),
        host_(std::make_shared<NiceMock<Envoy::Upstream::MockHost>>()) {
    header_map_ = std::make_shared<Envoy::Http::TestRequestHeaderMapImpl>();
    server_ = std::make_unique<Envoy::Network::SocketImpl>(
        Envoy::Network::Socket::Type::Datagram,
        Envoy::Network::Utility::parseInternetAddressAndPortNoThrow("127.0.0.1:0"), nullptr,
        Envoy::Network::SocketCreationOptions{});
    const Envoy::Api::SysCallIntResult bound =
        server_->bind(Envoy::Network::Utility::parseInternetAddressAndPortNoThrow("127.0.0.1:0"));
    RELEASE_ASSERT(bound.return_value_ == 0, "bind");
    server_address_ = server_->connectionInfoProvider().localAddress();
    server_->ioHandle().initializeFileEvent(
        *dispatcher_,
        [this](uint32_t) {
          serve();
          return absl::OkStatus();
        },
        Envoy::Event::FileTriggerType::Edge, Envoy::Event::FileReadyType::Read);

    EXPECT_CALL(cluster_manager(), getThreadLocalCluster(_))
        .WillRepeatedly(Return(&thread_local_cluster_));
    ON_CALL(*host_, address()).WillByDefault(Return(server_address_));
    EXPECT_CALL(thread_local_cluster_, chooseHost(_))
        .WillRepeatedly(Invoke([this](Envoy::Upstream::LoadBalancerContext*) {
          return Envoy::Upstream::HostSelectionResponse(host_);
        }));
  }

  void serve() {
    std::string buffer(65536, '\0');
    while (true) {
      Envoy::Buffer::RawSlice slice{buffer.data(), buffer.size()};
      Envoy::Network::IoHandle::RecvMsgOutput output(1, nullptr);
      const Envoy::Api::IoCallUint64Result result =
          server_->ioHandle().recvmsg(&slice, 1, server_address_->ip()->port(),
                                      Envoy::Network::IoHandle::UdpSaveCmsgConfig(), output);
      if (!result.ok() || result.return_value_ == 0) {
        return;
      }
      const std::string datagram = buffer.substr(0, result.return_value_);
      received_.push_back(datagram);
      if (drop_next_ > 0) {
        drop_next_--;
        continue;
      }
      for (int i = 0; i < (duplicate_ ? 2 : 1); i++) {
        Envoy::Buffer::RawSlice out{const_cast<char*>(datagram.data()), datagram.size()};
        std::ignore =
            server_->ioHandle().sendmsg(&out, 1, 0, nullptr, *output.msg_[0].peer_address_);
      }
    }
  }

  void createClient(uint32_t max_inflight = 256, std::chrono::nanoseconds timeout = 200ms,
                    std::chrono::nanoseconds drain = 300ms) {
    RequestGenerator request_generator = [this]() {
      return std::make_unique<RequestImpl>(header_map_, message_);
    };
    client_ = std::make_unique<UdpBenchmarkClientImpl>(
        *api_, *dispatcher_, *store_.rootScope(), std::make_unique<StreamingStatistic>(),
        cluster_manager_, "benchmark", request_generator, max_inflight, timeout, drain);
    client_->setShouldMeasureLatencies(true);
  }

  // Runs the dispatcher for a while, so datagrams flow both ways.
  void pump(std::chrono::milliseconds for_how_long) {
    Envoy::Event::TimerPtr timer = dispatcher_->createTimer([this]() { dispatcher_->exit(); });
    timer->enableTimer(for_how_long);
    dispatcher_->run(Envoy::Event::Dispatcher::RunType::RunUntilExit);
  }

  uint64_t getCounter(absl::string_view name) {
    return client_->scope().counterFromString(std::string(name)).value();
  }

  Envoy::Upstream::MockClusterManager& cluster_manager() {
    return dynamic_cast<Envoy::Upstream::MockClusterManager&>(*cluster_manager_);
  }

  Envoy::Event::TestRealTimeSystem time_system_;
  Envoy::Stats::IsolatedStoreImpl store_;
  Envoy::Api::ApiPtr api_;
  Envoy::Event::DispatcherPtr dispatcher_;
  Envoy::ProcessWide process_wide_;
  Envoy::Upstream::ClusterManagerPtr cluster_manager_;
  NiceMock<Envoy::Upstream::MockThreadLocalCluster> thread_local_cluster_;
  std::shared_ptr<NiceMock<Envoy::Upstream::MockHost>> host_;
  std::shared_ptr<Envoy::Http::RequestHeaderMap> header_map_;
  std::string message_{"ping"};
  std::unique_ptr<Envoy::Network::SocketImpl> server_;
  Envoy::Network::Address::InstanceConstSharedPtr server_address_;
  std::unique_ptr<UdpBenchmarkClientImpl> client_;
  std::vector<std::string> received_;
  int drop_next_{0};
  bool duplicate_{false};
};

TEST_F(UdpBenchmarkClientTest, DatagramsAreEchoedAndMatchedBySequence) {
  createClient();
  client_->prepare();
  ASSERT_TRUE(client_->ready());
  int completions = 0;
  int successes = 0;
  CompletionCallback callback = [&](bool complete, bool success) {
    completions += complete ? 1 : 0;
    successes += success ? 1 : 0;
  };
  for (int i = 0; i < 3; i++) {
    EXPECT_TRUE(client_->tryStartRequest(callback));
  }
  EXPECT_EQ(3, getCounter("udp_datagrams_sent"));
  pump(100ms);
  ASSERT_EQ(3, received_.size());
  EXPECT_EQ(UdpBenchmarkClientImpl::sequencePrefix(0) + "ping", received_[0]);
  EXPECT_EQ(UdpBenchmarkClientImpl::sequencePrefix(2) + "ping", received_[2]);
  EXPECT_EQ(3, completions);
  EXPECT_EQ(3, successes);
  EXPECT_EQ(3, getCounter("udp_datagrams_received"));
  EXPECT_EQ(0, getCounter("udp_lost"));
  client_->terminate();
}

TEST_F(UdpBenchmarkClientTest, ADroppedDatagramIsLostAfterTheTimeout) {
  createClient(256, /*timeout=*/100ms);
  client_->prepare();
  drop_next_ = 1;
  int successes = 0;
  int failures = 0;
  CompletionCallback callback = [&](bool, bool success) {
    successes += success ? 1 : 0;
    failures += success ? 0 : 1;
  };
  EXPECT_TRUE(client_->tryStartRequest(callback)); // dropped by the peer
  EXPECT_TRUE(client_->tryStartRequest(callback)); // echoed
  pump(50ms);
  EXPECT_EQ(1, successes);
  EXPECT_EQ(0, failures);
  pump(200ms);
  EXPECT_EQ(1, failures);
  EXPECT_EQ(1, getCounter("udp_lost"));
  client_->terminate();
}

TEST_F(UdpBenchmarkClientTest, ADuplicateEchoIsUnexpected) {
  createClient();
  client_->prepare();
  duplicate_ = true;
  int successes = 0;
  EXPECT_TRUE(client_->tryStartRequest([&](bool, bool success) { successes += success ? 1 : 0; }));
  pump(100ms);
  EXPECT_EQ(1, successes);
  EXPECT_EQ(1, getCounter("udp_datagrams_received"));
  EXPECT_EQ(1, getCounter("udp_unexpected"));
  client_->terminate();
}

TEST_F(UdpBenchmarkClientTest, DeferredSendsAreDroppedNotQueued) {
  createClient(/*max_inflight=*/1);
  client_->prepare();
  int completions = 0;
  int successes = 0;
  CompletionCallback callback = [&](bool complete, bool success) {
    completions += complete ? 1 : 0;
    successes += success ? 1 : 0;
  };
  EXPECT_TRUE(client_->tryStartRequest(callback));
  EXPECT_TRUE(client_->tryStartRequest(callback));
  EXPECT_EQ(1, getCounter("udp_datagrams_sent"));
  EXPECT_EQ(1, getCounter("udp_deferred"));
  pump(100ms);
  EXPECT_EQ(2, completions);
  EXPECT_EQ(1, successes);
  client_->terminate();
}

TEST_F(UdpBenchmarkClientTest, FinishWaitsForOutstandingEchoesThenLosesTheRest) {
  createClient(256, /*timeout=*/150ms, /*drain=*/1s);
  client_->prepare();
  drop_next_ = 1;
  int successes = 0;
  int failures = 0;
  CompletionCallback callback = [&](bool, bool success) {
    successes += success ? 1 : 0;
    failures += success ? 0 : 1;
  };
  EXPECT_TRUE(client_->tryStartRequest(callback)); // dropped
  EXPECT_TRUE(client_->tryStartRequest(callback)); // echoed during finish()'s wait
  client_->finish();
  EXPECT_EQ(1, successes);
  EXPECT_EQ(1, failures);
  EXPECT_EQ(1, getCounter("udp_lost"));
  // After finish nothing can be sent.
  int late = 0;
  EXPECT_TRUE(client_->tryStartRequest([&](bool, bool) { late++; }));
  dispatcher_->run(Envoy::Event::Dispatcher::RunType::NonBlock);
  EXPECT_EQ(1, late);
  EXPECT_EQ(1, getCounter("udp_unavailable"));
  client_->terminate();
}

} // namespace
} // namespace Client
} // namespace Nighthawk
