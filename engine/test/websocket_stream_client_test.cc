#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "source/common/buffer/buffer_impl.h"
#include "source/common/http/header_map_impl.h"
#include "source/common/stats/isolated_store_impl.h"
#include "source/exe/process_wide.h"

#include "test/mocks/http/mocks.h"
#include "test/mocks/stream_info/mocks.h"
#include "test/mocks/upstream/mocks.h"
#include "test/test_common/utility.h"

#include "engine/source/client/websocket_stream_client_impl.h"
#include "engine/source/common/request_impl.h"
#include "engine/source/common/statistic_impl.h"
#include "engine/source/common/websocket.h"

#include "gtest/gtest.h"

using namespace testing;

namespace Nighthawk {
namespace Client {
namespace {

using namespace std::chrono_literals;

ACTION(ReturnNewHostSelectionResponse) { return Envoy::Upstream::HostSelectionResponse(nullptr); }

class WebSocketStreamClientTest : public Test {
public:
  WebSocketStreamClientTest()
      : api_(Envoy::Api::createApiForTest(time_system_)),
        dispatcher_(api_->allocateDispatcher("test_thread")),
        cluster_manager_(std::make_unique<Envoy::Upstream::MockClusterManager>()),
        cluster_info_(std::make_unique<Envoy::Upstream::MockClusterInfo>()) {
    header_map_ = std::make_shared<Envoy::Http::TestRequestHeaderMapImpl>(
        std::initializer_list<std::pair<std::string, std::string>>{{":scheme", "http"},
                                                                   {":method", "GET"},
                                                                   {":path", "/echo"},
                                                                   {":authority", "localhost"}});
    EXPECT_CALL(cluster_manager(), getThreadLocalCluster(_))
        .WillRepeatedly(Return(&thread_local_cluster_));
    EXPECT_CALL(thread_local_cluster_, info()).WillRepeatedly(Return(cluster_info_));
    EXPECT_CALL(thread_local_cluster_, chooseHost(_))
        .WillRepeatedly(ReturnNewHostSelectionResponse());
    EXPECT_CALL(thread_local_cluster_, httpConnPool(_, _, _, _))
        .WillRepeatedly(Return(Envoy::Upstream::HttpPoolData([]() {}, &pool_)));
    // Every newStream() hands out a fresh encoder and remembers the decoder so the test can play
    // the server side: the upgrade request's key is captured to answer it.
    ON_CALL(pool_, newStream(_, _, _))
        .WillByDefault([this](Envoy::Http::ResponseDecoder& decoder,
                              Envoy::Http::ConnectionPool::Callbacks& callbacks,
                              const Envoy::Http::ConnectionPool::Instance::StreamOptions&)
                           -> Envoy::Http::ConnectionPool::Cancellable* {
          decoders_.push_back(&decoder);
          const size_t index = encoders_.size();
          auto encoder = std::make_unique<NiceMock<Envoy::Http::MockRequestEncoder>>();
          ON_CALL(*encoder, encodeHeaders(_, _))
              .WillByDefault([this, index](const Envoy::Http::RequestHeaderMap& headers, bool) {
                const auto key = headers.get(Envoy::Http::LowerCaseString("sec-websocket-key"));
                keys_[index] = key.empty() ? "" : std::string(key[0]->value().getStringView());
                const auto upgrade = headers.get(Envoy::Http::LowerCaseString("upgrade"));
                upgrades_[index] =
                    upgrade.empty() ? "" : std::string(upgrade[0]->value().getStringView());
                return Envoy::Http::Status();
              });
          ON_CALL(*encoder, encodeData(_, _))
              .WillByDefault(
                  [this, index](Envoy::Buffer::Instance& data, bool) { sent_[index].add(data); });
          encoders_.push_back(std::move(encoder));
          NiceMock<Envoy::StreamInfo::MockStreamInfo> stream_info;
          callbacks.onPoolReady(*encoders_.back(), Envoy::Upstream::HostDescriptionConstSharedPtr{},
                                stream_info, Envoy::Http::Protocol::Http11);
          return nullptr;
        });
  }

  void createClient(uint32_t streams, uint32_t max_inflight = 256,
                    std::chrono::nanoseconds drain = 50ms, bool binary = false,
                    std::chrono::seconds open_timeout = 1s) {
    RequestGenerator request_generator = [this]() {
      return std::make_unique<RequestImpl>(header_map_, message_);
    };
    client_ = std::make_unique<WebSocketStreamBenchmarkClientImpl>(
        *api_, *dispatcher_, *store_.rootScope(), std::make_unique<StreamingStatistic>(),
        cluster_manager_, "benchmark", request_generator, streams, max_inflight, drain,
        open_timeout, binary);
    client_->setShouldMeasureLatencies(true);
  }

  uint64_t getCounter(absl::string_view name) {
    return client_->scope().counterFromString(std::string(name)).value();
  }

  Envoy::Upstream::MockClusterManager& cluster_manager() {
    return dynamic_cast<Envoy::Upstream::MockClusterManager&>(*cluster_manager_);
  }

  // Plays the server's side of the handshake: a 101 with the accept key for the request's key,
  // or whatever status and accept the test wants.
  void upgrade(size_t index, const std::string& status = "101", std::string accept = "",
               const std::string& upgrade_header = "websocket",
               const std::string& connection_header = "Upgrade") {
    if (accept.empty()) {
      accept = WebSocket::acceptKey(keys_[index]);
    }
    Envoy::Http::ResponseHeaderMapPtr headers{
        new Envoy::Http::TestResponseHeaderMapImpl{{":status", status},
                                                   {"upgrade", upgrade_header},
                                                   {"connection", connection_header},
                                                   {"sec-websocket-accept", accept}}};
    decoders_[index]->decodeHeaders(std::move(headers), false);
  }

  // Decodes what the client sent on a connection so far, unmasked.
  std::vector<WebSocket::Frame> sentFrames(size_t index) {
    std::vector<WebSocket::Frame> frames;
    EXPECT_TRUE(decoders_sent_[index].feed(sent_[index], frames));
    return frames;
  }

  // Plays the server: echoes count of the client's unanswered data frames on a connection.
  void echo(size_t index, size_t count) {
    std::vector<WebSocket::Frame> frames = sentFrames(index);
    Envoy::Buffer::OwnedImpl buffer;
    for (WebSocket::Frame& frame : frames) {
      if (frame.opcode == WebSocket::Opcode::Text || frame.opcode == WebSocket::Opcode::Binary) {
        pending_echo_[index].push_back(std::move(frame));
      }
    }
    for (size_t i = 0; i < count && !pending_echo_[index].empty(); i++) {
      buffer.add(WebSocket::encodeFrame(pending_echo_[index].front(), /*mask=*/false));
      pending_echo_[index].pop_front();
    }
    decoders_[index]->decodeData(buffer, false);
  }

  void serverFrame(size_t index, const WebSocket::Frame& frame, bool end_stream = false) {
    Envoy::Buffer::OwnedImpl buffer(WebSocket::encodeFrame(frame, /*mask=*/false));
    decoders_[index]->decodeData(buffer, end_stream);
  }

  Envoy::Event::TestRealTimeSystem time_system_;
  Envoy::Stats::IsolatedStoreImpl store_;
  Envoy::Api::ApiPtr api_;
  Envoy::Event::DispatcherPtr dispatcher_;
  Envoy::ProcessWide process_wide_;
  Envoy::Upstream::ClusterManagerPtr cluster_manager_;
  Envoy::Upstream::ClusterInfoConstSharedPtr cluster_info_;
  Envoy::Upstream::MockThreadLocalCluster thread_local_cluster_;
  NiceMock<Envoy::Http::ConnectionPool::MockInstance> pool_;
  std::shared_ptr<Envoy::Http::RequestHeaderMap> header_map_;
  std::string message_{"ping"};
  std::unique_ptr<WebSocketStreamBenchmarkClientImpl> client_;
  std::vector<Envoy::Http::ResponseDecoder*> decoders_;
  std::vector<std::unique_ptr<NiceMock<Envoy::Http::MockRequestEncoder>>> encoders_;
  std::map<size_t, std::string> keys_;
  std::map<size_t, std::string> upgrades_;
  std::map<size_t, Envoy::Buffer::OwnedImpl> sent_;
  std::map<size_t, WebSocket::Decoder> decoders_sent_;
  std::map<size_t, std::deque<WebSocket::Frame>> pending_echo_;
};

TEST_F(WebSocketStreamClientTest, PrepareSendsUpgradesAndWaitsForThe101s) {
  createClient(2);
  // Answer the upgrades from a timer: prepare() has to run the dispatcher until both are in.
  Envoy::Event::TimerPtr timer = dispatcher_->createTimer([this]() {
    upgrade(0);
    upgrade(1);
  });
  timer->enableTimer(1ms);
  client_->prepare();
  EXPECT_EQ(2, client_->openStreams());
  EXPECT_EQ(2, getCounter("streams_opened"));
  EXPECT_EQ("websocket", upgrades_[0]);
  EXPECT_EQ(24, keys_[0].size());
  EXPECT_NE(keys_[0], keys_[1]);
  EXPECT_EQ(0, sent_[0].length());
}

TEST_F(WebSocketStreamClientTest, AnUpgradeAnsweredWithoutA101OrWithABadAcceptIsRejected) {
  createClient(2);
  Envoy::Event::TimerPtr timer = dispatcher_->createTimer([this]() {
    upgrade(0, "200");
    upgrade(1, "101", "bm90IHRoZSByaWdodCBhY2NlcHQ=");
  });
  timer->enableTimer(1ms);
  client_->prepare();
  EXPECT_EQ(0, client_->openStreams());
  EXPECT_EQ(2, getCounter("stream_upgrade_rejected"));
  EXPECT_EQ(0, getCounter("streams_opened"));
  // Nothing can be sent: every scheduled message finds no open connection.
  int completions = 0;
  EXPECT_TRUE(client_->tryStartRequest([&](bool, bool) { completions++; }));
  dispatcher_->run(Envoy::Event::Dispatcher::RunType::NonBlock);
  EXPECT_EQ(1, completions);
  EXPECT_EQ(1, getCounter("stream_unavailable"));
}

// A 101 without the Upgrade and Connection headers the RFC requires is not a WebSocket.
TEST_F(WebSocketStreamClientTest, A101WithoutTheUpgradeHeadersIsRejected) {
  createClient(3);
  Envoy::Event::TimerPtr timer = dispatcher_->createTimer([this]() {
    upgrade(0, "101", "", "h2c", "Upgrade");
    upgrade(1, "101", "", "websocket", "keep-alive");
    upgrade(2, "101", "", "WebSocket", "keep-alive, Upgrade");
  });
  timer->enableTimer(1ms);
  client_->prepare();
  EXPECT_EQ(1, client_->openStreams());
  EXPECT_EQ(2, getCounter("stream_upgrade_rejected"));
}

// An upgrade still unanswered when the wait expires is an open failure, and the stream is
// reset: it does not join the run later.
TEST_F(WebSocketStreamClientTest, UpgradesPendingAtTheTimeoutAreOpenFailures) {
  createClient(2, 256, 50ms, false, /*open_timeout=*/1s);
  Envoy::Event::TimerPtr timer = dispatcher_->createTimer([this]() { upgrade(0); });
  timer->enableTimer(1ms);
  client_->prepare();
  EXPECT_EQ(1, client_->openStreams());
  EXPECT_EQ(1, getCounter("stream_open_failures"));
  // A late 101 changes nothing.
  upgrade(1);
  EXPECT_EQ(1, client_->openStreams());
  EXPECT_EQ(1, getCounter("streams_opened"));
}

TEST_F(WebSocketStreamClientTest, MessagesRoundRobinAndEchoesCompleteThemBySequence) {
  createClient(2);
  Envoy::Event::TimerPtr timer = dispatcher_->createTimer([this]() {
    upgrade(0);
    upgrade(1);
  });
  timer->enableTimer(1ms);
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
  EXPECT_EQ(4, getCounter("stream_messages_sent"));
  std::vector<WebSocket::Frame> frames = sentFrames(0);
  ASSERT_EQ(2, frames.size());
  EXPECT_EQ(WebSocket::Opcode::Text, frames[0].opcode);
  EXPECT_EQ(WebSocketStreamBenchmarkClientImpl::sequencePrefix(0) + "ping", frames[0].payload);
  EXPECT_EQ(WebSocketStreamBenchmarkClientImpl::sequencePrefix(2) + "ping", frames[1].payload);
  for (WebSocket::Frame& frame : frames) {
    pending_echo_[0].push_back(std::move(frame));
  }
  EXPECT_EQ(0, completions);

  time_system_.advanceTimeWait(2ms);
  echo(0, 2);
  echo(1, 1);
  EXPECT_EQ(3, completions);
  EXPECT_EQ(3, successes);
  EXPECT_EQ(3, getCounter("stream_messages_received"));
  EXPECT_EQ(0, getCounter("stream_unexpected_message"));

  // An echo for a message never sent, or one already answered, is unexpected.
  WebSocket::Frame stray;
  stray.opcode = WebSocket::Opcode::Text;
  stray.payload = WebSocketStreamBenchmarkClientImpl::sequencePrefix(99) + "ping";
  serverFrame(0, stray);
  EXPECT_EQ(1, getCounter("stream_unexpected_message"));
  EXPECT_EQ(3, completions);
}

TEST_F(WebSocketStreamClientTest, PingsAreAnsweredWithPongs) {
  createClient(1);
  Envoy::Event::TimerPtr timer = dispatcher_->createTimer([this]() { upgrade(0); });
  timer->enableTimer(1ms);
  client_->prepare();
  WebSocket::Frame ping;
  ping.opcode = WebSocket::Opcode::Ping;
  ping.payload = "hb";
  serverFrame(0, ping);
  std::vector<WebSocket::Frame> frames = sentFrames(0);
  ASSERT_EQ(1, frames.size());
  EXPECT_EQ(WebSocket::Opcode::Pong, frames[0].opcode);
  EXPECT_EQ("hb", frames[0].payload);
}

TEST_F(WebSocketStreamClientTest, BinaryModeSendsBinaryFrames) {
  createClient(1, 256, 50ms, /*binary=*/true);
  Envoy::Event::TimerPtr timer = dispatcher_->createTimer([this]() { upgrade(0); });
  timer->enableTimer(1ms);
  client_->prepare();
  EXPECT_TRUE(client_->tryStartRequest([](bool, bool) {}));
  std::vector<WebSocket::Frame> frames = sentFrames(0);
  ASSERT_EQ(1, frames.size());
  EXPECT_EQ(WebSocket::Opcode::Binary, frames[0].opcode);
}

TEST_F(WebSocketStreamClientTest, DeferredSendsAreDroppedNotQueued) {
  createClient(1, /*max_inflight=*/1);
  Envoy::Event::TimerPtr timer = dispatcher_->createTimer([this]() { upgrade(0); });
  timer->enableTimer(1ms);
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
  EXPECT_EQ(1, getCounter("stream_messages_sent"));
  EXPECT_EQ(1, getCounter("stream_deferred"));
  EXPECT_EQ(1, completions);
  EXPECT_EQ(0, successes);
}

TEST_F(WebSocketStreamClientTest, FinishSendsCloseAndTheServersCloseEndsTheConnection) {
  createClient(1);
  Envoy::Event::TimerPtr timer = dispatcher_->createTimer([this]() { upgrade(0); });
  timer->enableTimer(1ms);
  client_->prepare();
  int completions = 0;
  int successes = 0;
  EXPECT_TRUE(client_->tryStartRequest([&](bool complete, bool success) {
    completions += complete ? 1 : 0;
    successes += success ? 1 : 0;
  }));
  // The server echoes the message and answers the Close from a timer, inside finish()'s wait.
  Envoy::Event::TimerPtr close_timer = dispatcher_->createTimer([this]() {
    // By now the client sent the message and, from finish(), its Close.
    std::vector<WebSocket::Frame> frames = sentFrames(0);
    ASSERT_EQ(2, frames.size());
    EXPECT_EQ(WebSocket::Opcode::Text, frames[0].opcode);
    EXPECT_EQ(WebSocket::Opcode::Close, frames[1].opcode);
    serverFrame(0, frames[0]);                      // the echo
    serverFrame(0, frames[1], /*end_stream=*/true); // the server's Close ends the response
  });
  close_timer->enableTimer(1ms);
  client_->finish();
  EXPECT_EQ(0, client_->openStreams());
  EXPECT_EQ(1, completions);
  EXPECT_EQ(1, successes);
  EXPECT_EQ(0, getCounter("stream_early_close"));
  EXPECT_EQ(0, getCounter("stream_drain_incomplete"));
  EXPECT_EQ(0, getCounter("stream_inflight_lost"));
  client_->terminate();
}

TEST_F(WebSocketStreamClientTest, AServerCloseWhileOpenIsAnEarlyCloseAndLosesInflight) {
  createClient(1);
  Envoy::Event::TimerPtr timer = dispatcher_->createTimer([this]() { upgrade(0); });
  timer->enableTimer(1ms);
  client_->prepare();
  int successes = 0;
  EXPECT_TRUE(client_->tryStartRequest([&](bool, bool success) { successes += success ? 1 : 0; }));
  serverFrame(0, WebSocket::closeFrame(1001));
  EXPECT_EQ(0, client_->openStreams());
  EXPECT_EQ(1, getCounter("stream_early_close"));
  EXPECT_EQ(1, getCounter("stream_inflight_lost"));
  EXPECT_EQ(0, successes);
  // The client answered the Close.
  std::vector<WebSocket::Frame> frames = sentFrames(0);
  ASSERT_EQ(2, frames.size());
  EXPECT_EQ(WebSocket::Opcode::Close, frames[1].opcode);
}

TEST_F(WebSocketStreamClientTest, DrainWindowExpiringCountsStillOpenConnections) {
  createClient(1, 256, /*drain=*/5ms);
  Envoy::Event::TimerPtr timer = dispatcher_->createTimer([this]() { upgrade(0); });
  timer->enableTimer(1ms);
  client_->prepare();
  EXPECT_TRUE(client_->tryStartRequest([](bool, bool) {}));
  client_->finish();
  EXPECT_EQ(1, getCounter("stream_drain_incomplete"));
  EXPECT_EQ(1, getCounter("stream_inflight_lost"));
  client_->terminate();
}

} // namespace
} // namespace Client
} // namespace Nighthawk
