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
            if (refuse_connect_) {
              connections_[index]->raiseEvent(Envoy::Network::ConnectionEvent::RemoteClose);
            } else if (!defer_connect_) {
              connections_[index]->raiseEvent(Envoy::Network::ConnectionEvent::Connected);
            }
          });
          connections_.push_back(connection.get());
          return Envoy::Upstream::MockHost::MockCreateConnectionData{connection.release(), nullptr};
        });
  }

  void createClient(uint32_t connections, uint32_t max_inflight = 256, bool expect_echo = true,
                    std::chrono::nanoseconds drain = 50ms, std::chrono::seconds timeout = 1s,
                    uint32_t max_messages_per_connection = 0) {
    RequestGenerator request_generator = [this]() {
      return std::make_unique<RequestImpl>(header_map_, message_);
    };
    client_ = std::make_unique<TcpBenchmarkClientImpl>(
        *api_, *dispatcher_, *store_.rootScope(), std::make_unique<StreamingStatistic>(),
        std::make_unique<StreamingStatistic>(), cluster_manager_, "benchmark", request_generator,
        connections, max_inflight, expect_echo, drain, timeout, max_messages_per_connection);
    client_->setShouldMeasureLatencies(true);
  }

  uint64_t getCounter(absl::string_view name) {
    return client_->scope().counterFromString(std::string(name)).value();
  }

  uint64_t statisticCount(const std::string& id) { return client_->statistics()[id]->count(); }

  // Runs the dispatcher for a while: timers that come due fire, and connections the client let
  // go of are deleted -- their pointers in connections_ dangle afterwards.
  void runFor(std::chrono::milliseconds duration) {
    Envoy::Event::TimerPtr timer = dispatcher_->createTimer([this]() { dispatcher_->exit(); });
    timer->enableTimer(duration);
    dispatcher_->run(Envoy::Event::Dispatcher::RunType::RunUntilExit);
  }

  // Sends count messages, tallying how they complete.
  void send(int count) {
    for (int i = 0; i < count; i++) {
      EXPECT_TRUE(client_->tryStartRequest([this](bool, bool success) {
        completions_++;
        successes_ += success ? 1 : 0;
      }));
    }
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
  bool refuse_connect_{false};
  int completions_{0};
  int successes_{0};
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

// A connection that has not connected within the timeout is a connect failure and closed; the
// run starts without it, and it is tried again.
TEST_F(TcpBenchmarkClientTest, AConnectStillPendingAtTheTimeoutIsAFailureAndRetried) {
  defer_connect_ = true;
  createClient(2, 256, true, 50ms, /*timeout=*/1s);
  Envoy::Event::TimerPtr timer = dispatcher_->createTimer(
      [this]() { connections_[0]->raiseEvent(Envoy::Network::ConnectionEvent::Connected); });
  timer->enableTimer(1ms);
  client_->prepare();
  EXPECT_EQ(1, client_->openConnections());
  EXPECT_EQ(1, getCounter("tcp_connect_failures"));
  EXPECT_EQ(2, connections_.size());
  defer_connect_ = false;
  runFor(15ms);
  EXPECT_EQ(3, connections_.size());
  EXPECT_EQ(2, client_->openConnections());
  EXPECT_EQ(2, getCounter("tcp_connections_opened"));
  EXPECT_EQ(1, getCounter("tcp_reconnects"));
  EXPECT_EQ(1, getCounter("tcp_connect_failures"));
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
  // A flush that does not complete at once: no close event yet.
  EXPECT_CALL(*connections_[0], close(Envoy::Network::ConnectionCloseType::FlushWrite))
      .WillOnce(Return());
  client_->finish();
  EXPECT_EQ(1, successes);
  EXPECT_EQ(0, getCounter("tcp_drain_incomplete"));
  EXPECT_EQ(0, getCounter("tcp_inflight_lost"));
  // terminate() cuts the flush short, whatever finish() already did.
  EXPECT_CALL(*connections_[0], close(Envoy::Network::ConnectionCloseType::NoFlush));
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

// The peer closes the connection mid-run: what was sent before is delivered, what was in flight
// is lost, what comes due until the connection is back is unavailable, and what is sent after is
// delivered again.
TEST_F(TcpBenchmarkClientTest, AConnectionThePeerClosesIsReopened) {
  createClient(1);
  client_->prepare();
  send(2);
  echo(0, 2);
  send(1);
  connections_[0]->raiseEvent(Envoy::Network::ConnectionEvent::RemoteClose);
  EXPECT_EQ(0, client_->openConnections());
  send(2);
  // The first retry comes 10 ms after the close.
  runFor(15ms);
  ASSERT_EQ(2, connections_.size());
  EXPECT_EQ(1, client_->openConnections());
  send(2);
  EXPECT_EQ("pingping", sent_[1].toString());
  echo(1, 2);
  EXPECT_EQ(7, completions_);
  EXPECT_EQ(4, successes_);
  EXPECT_EQ(5, getCounter("tcp_messages_sent"));
  EXPECT_EQ(4, getCounter("tcp_messages_received"));
  EXPECT_EQ(1, getCounter("tcp_inflight_lost"));
  EXPECT_EQ(2, getCounter("tcp_unavailable"));
  EXPECT_EQ(1, getCounter("tcp_connection_closed"));
  EXPECT_EQ(1, getCounter("tcp_reconnects"));
  EXPECT_EQ(2, getCounter("tcp_connections_opened"));
  EXPECT_EQ(0, getCounter("tcp_connect_failures"));
  // Every connect is timed, the reconnect too; the latencies of the four echoes with it.
  EXPECT_EQ(2, statisticCount("benchmark_tcp.connect_latency"));
  EXPECT_EQ(4, statisticCount("benchmark_tcp.message_latency"));
}

// Against a port nothing listens on the attempts back off -- 10 ms, doubling -- rather than
// spin, messages are unavailable meanwhile, and the connection joins once the port answers.
TEST_F(TcpBenchmarkClientTest, ConnectAttemptsAgainstADeadPortBackOff) {
  refuse_connect_ = true;
  createClient(1);
  client_->prepare();
  EXPECT_EQ(1, getCounter("tcp_connect_failures"));
  send(3);
  // Attempts at 0, 10, 30, 70 and 150 ms; the next at 310. A timer is never early, so never
  // more than those five; a busy machine may be late with the last ones.
  runFor(200ms);
  EXPECT_LE(getCounter("tcp_connect_failures"), 5);
  EXPECT_GE(getCounter("tcp_connect_failures"), 3);
  EXPECT_EQ(getCounter("tcp_connect_failures"), connections_.size());
  EXPECT_EQ(3, getCounter("tcp_unavailable"));
  EXPECT_EQ(0, getCounter("tcp_connections_opened"));
  EXPECT_EQ(0, client_->openConnections());
  const uint64_t failures = getCounter("tcp_connect_failures");
  refuse_connect_ = false;
  runFor(700ms);
  EXPECT_EQ(1, client_->openConnections());
  EXPECT_EQ(1, getCounter("tcp_reconnects"));
  EXPECT_EQ(1, getCounter("tcp_connections_opened"));
  EXPECT_EQ(failures, getCounter("tcp_connect_failures"));
  send(1);
  EXPECT_EQ(1, getCounter("tcp_messages_sent"));
  EXPECT_EQ(0, getCounter("tcp_inflight_lost"));
}

// A peer that accepts and closes at once -- a proxy with no upstream -- does not start the
// backoff over with every connect; two seconds without a retry does.
TEST_F(TcpBenchmarkClientTest, TheBackoffStartsOverOnlyAfterAQuietSpell) {
  createClient(1);
  client_->prepare();
  connections_[0]->raiseEvent(Envoy::Network::ConnectionEvent::RemoteClose);
  runFor(15ms);
  ASSERT_EQ(2, connections_.size());
  // Closed again straight after connecting: the second retry waits 20 ms, not 10.
  connections_[1]->raiseEvent(Envoy::Network::ConnectionEvent::RemoteClose);
  runFor(15ms);
  EXPECT_EQ(2, connections_.size());
  runFor(15ms);
  ASSERT_EQ(3, connections_.size());
  EXPECT_EQ(2, getCounter("tcp_reconnects"));
  // After a quiet spell the next close is retried at 10 ms again, not at 40.
  runFor(2100ms);
  connections_[2]->raiseEvent(Envoy::Network::ConnectionEvent::RemoteClose);
  runFor(15ms);
  EXPECT_EQ(4, connections_.size());
  EXPECT_EQ(3, getCounter("tcp_reconnects"));
  EXPECT_EQ(3, getCounter("tcp_connection_closed"));
}

// With a replacement that connects at once, a connection carries exactly its limit: ten
// messages on a limit of three go 3, 3, 3 and 1, none lost, none unavailable.
TEST_F(TcpBenchmarkClientTest, AConnectionIsRotatedAfterItsMessages) {
  createClient(1, 256, true, 50ms, 1s, /*max_messages_per_connection=*/3);
  client_->prepare();
  send(3);
  ASSERT_EQ(2, connections_.size());
  EXPECT_EQ(1, getCounter("tcp_connections_rotated"));
  // The rotated connection stays for its echoes, and is closed with the last of them.
  EXPECT_EQ(0, successes_);
  echo(0, 2);
  EXPECT_EQ(2, successes_);
  EXPECT_CALL(*connections_[0], close(Envoy::Network::ConnectionCloseType::FlushWrite));
  echo(0, 1);
  EXPECT_EQ(3, successes_);
  send(7);
  ASSERT_EQ(4, connections_.size());
  EXPECT_EQ("pingpingping", sent_[1].toString());
  EXPECT_EQ("pingpingping", sent_[2].toString());
  EXPECT_EQ("ping", sent_[3].toString());
  echo(1, 3);
  echo(2, 3);
  echo(3, 1);
  EXPECT_EQ(10, successes_);
  EXPECT_EQ(10, getCounter("tcp_messages_sent"));
  EXPECT_EQ(10, getCounter("tcp_messages_received"));
  EXPECT_EQ(3, getCounter("tcp_connections_rotated"));
  EXPECT_EQ(4, getCounter("tcp_connections_opened"));
  EXPECT_EQ(0, getCounter("tcp_reconnects"));
  EXPECT_EQ(0, getCounter("tcp_connect_failures"));
  EXPECT_EQ(0, getCounter("tcp_unavailable"));
  EXPECT_EQ(0, getCounter("tcp_inflight_lost"));
  EXPECT_EQ(0, getCounter("tcp_connection_closed"));
  EXPECT_EQ(1, client_->openConnections());
  EXPECT_EQ(4, statisticCount("benchmark_tcp.connect_latency"));
}

// The connection goes on sending while its replacement connects, so rotating costs no
// message; a replacement that fails is tried again; and echoes a rotated connection never
// gets are lost when the timeout is up.
TEST_F(TcpBenchmarkClientTest, TheOldConnectionServesUntilItsReplacementHasConnected) {
  createClient(1, 256, true, 50ms, /*timeout=*/1s, /*max_messages_per_connection=*/2);
  client_->prepare();
  defer_connect_ = true;
  send(4);
  // One replacement on its way, and everything so far on the old connection.
  ASSERT_EQ(2, connections_.size());
  EXPECT_EQ("pingpingpingping", sent_[0].toString());
  EXPECT_EQ(0, getCounter("tcp_unavailable"));
  EXPECT_EQ(0, getCounter("tcp_connections_rotated"));
  // The replacement fails: a connect failure, retried after the backoff, the old one serving.
  connections_[1]->raiseEvent(Envoy::Network::ConnectionEvent::RemoteClose);
  EXPECT_EQ(1, getCounter("tcp_connect_failures"));
  send(1);
  EXPECT_EQ(5 * message_.size(), sent_[0].length());
  runFor(15ms);
  ASSERT_EQ(3, connections_.size());
  EXPECT_EQ(0, getCounter("tcp_connections_rotated"));
  connections_[2]->raiseEvent(Envoy::Network::ConnectionEvent::Connected);
  EXPECT_EQ(1, getCounter("tcp_connections_rotated"));
  EXPECT_EQ(0, getCounter("tcp_reconnects"));
  send(1);
  EXPECT_EQ("ping", sent_[2].toString());
  EXPECT_EQ(5 * message_.size(), sent_[0].length());
  // Three of the old connection's five echoes arrive; the other two never do.
  echo(0, 3);
  EXPECT_EQ(3, successes_);
  EXPECT_CALL(*connections_[0], close(Envoy::Network::ConnectionCloseType::NoFlush));
  runFor(1100ms);
  EXPECT_EQ(2, getCounter("tcp_inflight_lost"));
  EXPECT_EQ(5, completions_);
  EXPECT_EQ(0, getCounter("tcp_unavailable"));
  EXPECT_EQ(0, getCounter("tcp_connection_closed"));
  EXPECT_EQ(1, client_->openConnections());
}

// Without echoes there is nothing to wait for: the rotated connection is closed, flushed, as
// soon as its replacement has taken over.
TEST_F(TcpBenchmarkClientTest, WithoutEchoesARotatedConnectionIsClosedAtOnce) {
  createClient(1, 256, /*expect_echo=*/false, 50ms, 1s, /*max_messages_per_connection=*/2);
  client_->prepare();
  send(1);
  EXPECT_CALL(*connections_[0], close(Envoy::Network::ConnectionCloseType::FlushWrite));
  send(1);
  send(1);
  ASSERT_EQ(2, connections_.size());
  EXPECT_EQ("ping", sent_[1].toString());
  EXPECT_EQ(1, getCounter("tcp_connections_rotated"));
  EXPECT_EQ(3, getCounter("tcp_messages_sent"));
}

// finish() stops the reopening: a slot waiting to retry stays closed.
TEST_F(TcpBenchmarkClientTest, NothingIsReopenedAfterFinish) {
  createClient(1);
  client_->prepare();
  connections_[0]->raiseEvent(Envoy::Network::ConnectionEvent::RemoteClose);
  client_->finish();
  runFor(30ms);
  EXPECT_EQ(1, connections_.size());
  EXPECT_EQ(0, getCounter("tcp_reconnects"));
  client_->terminate();
}

} // namespace
} // namespace Client
} // namespace Nighthawk
