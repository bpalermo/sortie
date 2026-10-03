#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "source/common/buffer/buffer_impl.h"
#include "source/common/http/header_map_impl.h"
#include "source/common/stats/isolated_store_impl.h"
#include "source/exe/process_wide.h"

#include "test/mocks/network/connection.h"
#include "test/mocks/upstream/mocks.h"
#include "test/test_common/utility.h"

#include "engine/source/client/tcp_benchmark_client_impl.h"
#include "engine/source/common/request_impl.h"
#include "engine/source/common/statistic_impl.h"

#include "gtest/gtest.h"

using namespace testing;

namespace Nighthawk {
namespace Client {
namespace {

using namespace std::chrono_literals;

class TcpBenchmarkClientTest : public Test {
public:
  TcpBenchmarkClientTest()
      : api_(Envoy::Api::createApiForTest(time_system_)),
        dispatcher_(api_->allocateDispatcher("test_thread")),
        cluster_manager_(std::make_unique<Envoy::Upstream::MockClusterManager>()) {
    header_map_ = std::make_shared<Envoy::Http::TestRequestHeaderMapImpl>();
    EXPECT_CALL(cluster_manager(), getThreadLocalCluster(_))
        .WillRepeatedly(Return(&thread_local_cluster_));
    // Every tcpConn() hands out a fresh mock connection; the test keeps a raw pointer and the
    // callbacks and read filter the client installs, to play the peer.
    // The mock cluster wraps the raw pointer tcpConn_ returns into the ClientConnectionPtr the
    // client owns.
    ON_CALL(thread_local_cluster_, tcpConn_(_))
        .WillByDefault([this](Envoy::Upstream::LoadBalancerContext*) {
          auto connection = std::make_unique<NiceMock<Envoy::Network::MockClientConnection>>();
          const size_t index = connections_.size();
          ON_CALL(*connection, addReadFilter(_))
              .WillByDefault([this, index](Envoy::Network::ReadFilterSharedPtr filter) {
                filters_[index] = filter;
              });
          ON_CALL(*connection, write(_, _))
              .WillByDefault(
                  [this, index](Envoy::Buffer::Instance& data, bool) { sent_[index].add(data); });
          ON_CALL(*connection, connect()).WillByDefault([this, index]() {
            if (!defer_connect_) {
              connections_[index]->raiseEvent(Envoy::Network::ConnectionEvent::Connected);
            }
          });
          connections_.push_back(connection.get());
          return Envoy::Upstream::MockHost::MockCreateConnectionData{connection.release(), nullptr};
        });
  }

  void createClient(uint32_t connections, uint32_t max_inflight = 256, bool expect_echo = true,
                    std::chrono::nanoseconds drain = 50ms,
                    std::chrono::seconds open_timeout = 1s) {
    RequestGenerator request_generator = [this]() {
      return std::make_unique<RequestImpl>(header_map_, message_);
    };
    client_ = std::make_unique<TcpBenchmarkClientImpl>(
        *api_, *dispatcher_, *store_.rootScope(), std::make_unique<StreamingStatistic>(),
        cluster_manager_, "benchmark", request_generator, connections, max_inflight, expect_echo,
        drain, open_timeout);
    client_->setShouldMeasureLatencies(true);
  }

  uint64_t getCounter(absl::string_view name) {
    return client_->scope().counterFromString(std::string(name)).value();
  }

  Envoy::Upstream::MockClusterManager& cluster_manager() {
    return dynamic_cast<Envoy::Upstream::MockClusterManager&>(*cluster_manager_);
  }

  // Plays the peer: echoes bytes of what the client sent on a connection, count messages' worth.
  void echo(size_t index, size_t count) {
    const size_t length = message_.size();
    Envoy::Buffer::OwnedImpl buffer;
    for (size_t i = 0; i < count && sent_[index].length() >= length; i++) {
      std::string bytes(length, '\0');
      sent_[index].copyOut(0, length, bytes.data());
      sent_[index].drain(length);
      buffer.add(bytes);
    }
    filters_[index]->onData(buffer, false);
  }

  void peerSends(size_t index, const std::string& bytes) {
    Envoy::Buffer::OwnedImpl buffer(bytes);
    filters_[index]->onData(buffer, false);
  }

  Envoy::Event::TestRealTimeSystem time_system_;
  Envoy::Stats::IsolatedStoreImpl store_;
  Envoy::Api::ApiPtr api_;
  Envoy::Event::DispatcherPtr dispatcher_;
  Envoy::ProcessWide process_wide_;
  Envoy::Upstream::ClusterManagerPtr cluster_manager_;
  NiceMock<Envoy::Upstream::MockThreadLocalCluster> thread_local_cluster_;
  std::shared_ptr<Envoy::Http::RequestHeaderMap> header_map_;
  std::string message_{"ping"};
  std::unique_ptr<TcpBenchmarkClientImpl> client_;
  std::vector<NiceMock<Envoy::Network::MockClientConnection>*> connections_;
  std::map<size_t, Envoy::Network::ReadFilterSharedPtr> filters_;
  std::map<size_t, Envoy::Buffer::OwnedImpl> sent_;
  bool defer_connect_{false};
};

TEST_F(TcpBenchmarkClientTest, PrepareOpensTheConnectionsAndWaitsForThem) {
  defer_connect_ = true;
  createClient(2);
  Envoy::Event::TimerPtr timer = dispatcher_->createTimer([this]() {
    for (auto* connection : connections_) {
      connection->raiseEvent(Envoy::Network::ConnectionEvent::Connected);
    }
  });
  timer->enableTimer(1ms);
  client_->prepare();
  EXPECT_EQ(2, client_->openConnections());
  EXPECT_EQ(2, getCounter("tcp_connections_opened"));
  EXPECT_EQ(0, getCounter("tcp_connect_failures"));
}

TEST_F(TcpBenchmarkClientTest, AConnectionThatClosesWhileConnectingIsAConnectFailure) {
  defer_connect_ = true;
  createClient(2);
  Envoy::Event::TimerPtr timer = dispatcher_->createTimer([this]() {
    connections_[0]->raiseEvent(Envoy::Network::ConnectionEvent::Connected);
    connections_[1]->raiseEvent(Envoy::Network::ConnectionEvent::RemoteClose);
  });
  timer->enableTimer(1ms);
  client_->prepare();
  EXPECT_EQ(1, client_->openConnections());
  EXPECT_EQ(1, getCounter("tcp_connect_failures"));
  // Sends scheduled for the closed one find it unavailable.
  int completions = 0;
  EXPECT_TRUE(client_->tryStartRequest([&](bool, bool) { completions++; }));
  EXPECT_TRUE(client_->tryStartRequest([&](bool, bool) { completions++; }));
  dispatcher_->run(Envoy::Event::Dispatcher::RunType::NonBlock);
  EXPECT_EQ(1, getCounter("tcp_messages_sent"));
  EXPECT_EQ(1, getCounter("tcp_unavailable"));
  EXPECT_EQ(1, completions);
}

// A connection that has not connected when the wait expires is a connect failure, closed, and
// never joins the run.
TEST_F(TcpBenchmarkClientTest, ConnectionsStillConnectingAtTheTimeoutAreFailures) {
  defer_connect_ = true;
  createClient(2, 256, true, 50ms, /*open_timeout=*/1s);
  Envoy::Event::TimerPtr timer = dispatcher_->createTimer(
      [this]() { connections_[0]->raiseEvent(Envoy::Network::ConnectionEvent::Connected); });
  timer->enableTimer(1ms);
  // The connections only exist once prepare() asked the cluster for them, so the close is
  // observed afterwards rather than expected up front.
  client_->prepare();
  EXPECT_EQ(1, client_->openConnections());
  EXPECT_EQ(1, getCounter("tcp_connect_failures"));
  // Connecting late changes nothing: the connection was closed and counted.
  connections_[1]->raiseEvent(Envoy::Network::ConnectionEvent::Connected);
  EXPECT_EQ(1, client_->openConnections());
}

TEST_F(TcpBenchmarkClientTest, MessagesRoundRobinAndEchoesCompleteThem) {
  createClient(2);
  client_->prepare();
  int completions = 0;
  int successes = 0;
  CompletionCallback callback = [&](bool complete, bool success) {
    completions += complete ? 1 : 0;
    successes += success ? 1 : 0;
  };
  for (int i = 0; i < 4; i++) {
    EXPECT_TRUE(client_->tryStartRequest(callback));
  }
  EXPECT_EQ(4, getCounter("tcp_messages_sent"));
  // Exactly the message, twice, on each: no framing of the client's own.
  EXPECT_EQ("pingping", sent_[0].toString());
  EXPECT_EQ("pingping", sent_[1].toString());
  EXPECT_EQ(0, completions);
  time_system_.advanceTimeWait(2ms);
  echo(0, 2);
  echo(1, 1);
  EXPECT_EQ(3, completions);
  EXPECT_EQ(3, successes);
  EXPECT_EQ(3, getCounter("tcp_messages_received"));
  // Bytes that are not the message, and an echo with nothing outstanding.
  peerSends(0, "pong");
  echo(1, 1);
  EXPECT_EQ(4, completions);
  peerSends(1, "ping");
  EXPECT_EQ(2, getCounter("tcp_echo_mismatch"));
  EXPECT_EQ(4, completions);
}

TEST_F(TcpBenchmarkClientTest, EchoesSplitAcrossReadsAreReassembled) {
  createClient(1);
  client_->prepare();
  int successes = 0;
  EXPECT_TRUE(client_->tryStartRequest([&](bool, bool success) { successes += success ? 1 : 0; }));
  peerSends(0, "pi");
  EXPECT_EQ(0, successes);
  peerSends(0, "ng");
  EXPECT_EQ(1, successes);
}

TEST_F(TcpBenchmarkClientTest, WithoutEchoesWritesCompleteAtOnceAndReadsAreIgnored) {
  createClient(1, 256, /*expect_echo=*/false);
  client_->prepare();
  int successes = 0;
  EXPECT_TRUE(client_->tryStartRequest([&](bool, bool success) { successes += success ? 1 : 0; }));
  dispatcher_->run(Envoy::Event::Dispatcher::RunType::NonBlock);
  EXPECT_EQ(1, successes);
  EXPECT_EQ(1, getCounter("tcp_messages_sent"));
  peerSends(0, "anything at all");
  EXPECT_EQ(0, getCounter("tcp_echo_mismatch"));
  EXPECT_EQ(0, getCounter("tcp_messages_received"));
}

TEST_F(TcpBenchmarkClientTest, DeferredSendsAreDroppedNotQueued) {
  createClient(1, /*max_inflight=*/1);
  client_->prepare();
  int completions = 0;
  int successes = 0;
  CompletionCallback callback = [&](bool complete, bool success) {
    completions += complete ? 1 : 0;
    successes += success ? 1 : 0;
  };
  EXPECT_TRUE(client_->tryStartRequest(callback));
  EXPECT_TRUE(client_->tryStartRequest(callback));
  dispatcher_->run(Envoy::Event::Dispatcher::RunType::NonBlock);
  EXPECT_EQ(1, getCounter("tcp_messages_sent"));
  EXPECT_EQ(1, getCounter("tcp_deferred"));
  EXPECT_EQ(1, completions);
  EXPECT_EQ(0, successes);
}

TEST_F(TcpBenchmarkClientTest, FinishWaitsForEchoesThenCloses) {
  createClient(1);
  client_->prepare();
  int successes = 0;
  EXPECT_TRUE(client_->tryStartRequest([&](bool, bool success) { successes += success ? 1 : 0; }));
  Envoy::Event::TimerPtr timer = dispatcher_->createTimer([this]() { echo(0, 1); });
  timer->enableTimer(1ms);
  EXPECT_CALL(*connections_[0], close(Envoy::Network::ConnectionCloseType::FlushWrite));
  client_->finish();
  EXPECT_EQ(1, successes);
  EXPECT_EQ(0, getCounter("tcp_drain_incomplete"));
  EXPECT_EQ(0, getCounter("tcp_inflight_lost"));
  client_->terminate();
}

TEST_F(TcpBenchmarkClientTest, DrainWindowExpiringLosesTheInflight) {
  createClient(1, 256, true, /*drain=*/5ms);
  client_->prepare();
  int successes = 0;
  EXPECT_TRUE(client_->tryStartRequest([&](bool, bool success) { successes += success ? 1 : 0; }));
  client_->finish();
  EXPECT_EQ(0, successes);
  EXPECT_EQ(1, getCounter("tcp_drain_incomplete"));
  EXPECT_EQ(1, getCounter("tcp_inflight_lost"));
  client_->terminate();
}

TEST_F(TcpBenchmarkClientTest, APeerCloseInUseLosesTheInflight) {
  createClient(1);
  client_->prepare();
  int successes = 0;
  EXPECT_TRUE(client_->tryStartRequest([&](bool, bool success) { successes += success ? 1 : 0; }));
  connections_[0]->raiseEvent(Envoy::Network::ConnectionEvent::RemoteClose);
  EXPECT_EQ(0, client_->openConnections());
  EXPECT_EQ(1, getCounter("tcp_connection_closed"));
  EXPECT_EQ(1, getCounter("tcp_inflight_lost"));
  EXPECT_EQ(0, successes);
}

} // namespace
} // namespace Client
} // namespace Nighthawk
