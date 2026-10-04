#pragma once

#include <chrono>
#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "envoy/api/api.h"
#include "envoy/event/dispatcher.h"
#include "envoy/network/connection.h"
#include "envoy/network/filter.h"
#include "envoy/stats/scope.h"
#include "envoy/stats/stats_macros.h"
#include "envoy/upstream/cluster_manager.h"

#include "nighthawk/client/benchmark_client.h"
#include "nighthawk/common/request_source.h"
#include "nighthawk/common/statistic.h"

#include "source/common/buffer/buffer_impl.h"
#include "source/common/common/logger.h"

namespace Nighthawk {
namespace Client {

#define ALL_TCP_COUNTERS(COUNTER)                                                                  \
  COUNTER(tcp_connections_opened)                                                                  \
  COUNTER(tcp_connect_failures)                                                                    \
  COUNTER(tcp_messages_sent)                                                                       \
  COUNTER(tcp_messages_received)                                                                   \
  COUNTER(tcp_deferred)                                                                            \
  COUNTER(tcp_unavailable)                                                                         \
  COUNTER(tcp_connection_closed)                                                                   \
  COUNTER(tcp_echo_mismatch)                                                                       \
  COUNTER(tcp_inflight_lost)                                                                       \
  COUNTER(tcp_drain_incomplete)                                                                    \
  COUNTER(tcp_write_blocked)

struct TcpCounters {
  ALL_TCP_COUNTERS(GENERATE_COUNTER_STRUCT)
};

/**
 * BenchmarkClient for raw TCP: what Envoy's tcp_proxy and TLS-terminating listeners front. It
 * keeps a fixed pool of connections per worker, opened in prepare() through the cluster -- so the
 * transport socket the cluster carries (TLS for tcps://) applies -- and on every
 * tryStartRequest() writes the message -- exactly the message, no framing of its own -- on the
 * next connection, round-robin. With expect_echo the write completes when those bytes come back
 * on the same connection, and the round trip is the latency; a TCP connection delivers in order,
 * so echoes are matched FIFO against the messages outstanding on it. Without expect_echo the
 * write completes at once, nothing is read and nothing is timed: a write only queues bytes
 * locally. A send scheduled for a connection that is not open, has
 * max_inflight_per_connection unanswered messages, or is above its write high watermark, is
 * dropped and counted (tcp_unavailable, tcp_deferred) -- never queued or retried, like the stream
 * clients. finish() waits up to the drain duration for outstanding echoes, then closes.
 *
 * Counters live under the "benchmark." scope: tcp_connections_opened, tcp_connect_failures,
 * tcp_messages_sent, tcp_messages_received, tcp_deferred, tcp_unavailable, tcp_connection_closed
 * (the peer closed a connection in use), tcp_echo_mismatch (a message's worth of bytes that is
 * not the message), tcp_inflight_lost, tcp_drain_incomplete, tcp_write_blocked. Statistic:
 * benchmark_tcp.message_latency.
 */
class TcpBenchmarkClientImpl : public BenchmarkClient,
                               public Envoy::Logger::Loggable<Envoy::Logger::Id::main> {
public:
  /**
   * @param api Envoy api.
   * @param dispatcher the worker's dispatcher.
   * @param scope the worker's stats scope; counters are created under "benchmark." in it.
   * @param message_latency_statistic statistic that records per-message send-to-echo latencies.
   * @param cluster_manager cluster manager holding the worker's cluster.
   * @param cluster_name name of the worker's cluster.
   * @param request_generator yields the request whose body is the message; it must not be
   * empty when expect_echo is set, since the echo is matched by its bytes.
   * @param connections connections this worker keeps open.
   * @param max_inflight_per_connection unanswered messages a connection may hold before sends
   * on it are deferred.
   * @param expect_echo whether the peer echoes every message, which is what gets timed.
   * @param drain_duration how long finish() waits for outstanding echoes.
   * @param open_timeout how long prepare() waits for the connections.
   */
  TcpBenchmarkClientImpl(Envoy::Api::Api& api, Envoy::Event::Dispatcher& dispatcher,
                         Envoy::Stats::Scope& scope, StatisticPtr&& message_latency_statistic,
                         Envoy::Upstream::ClusterManagerPtr& cluster_manager,
                         absl::string_view cluster_name, RequestGenerator request_generator,
                         uint32_t connections, uint32_t max_inflight_per_connection,
                         bool expect_echo, std::chrono::nanoseconds drain_duration,
                         std::chrono::seconds open_timeout);
  ~TcpBenchmarkClientImpl() override;

  // BenchmarkClient
  void prepare() override;
  void finish() override;
  void terminate() override;
  void setShouldMeasureLatencies(bool measure_latencies) override {
    measure_latencies_ = measure_latencies;
  }
  bool shouldMeasureLatencies() const override { return measure_latencies_; }
  StatisticPtrMap statistics() const override;
  bool tryStartRequest(CompletionCallback caller_completion_callback) override;
  Envoy::Stats::Scope& scope() const override { return *scope_; }
  std::vector<nighthawk::client::UserDefinedOutput> getUserDefinedOutputResults() const override {
    return {};
  }

  /**
   * @return uint32_t the number of connections currently open.
   */
  uint32_t openConnections() const;

private:
  enum class State { Connecting, Open, Closed };

  // Envoy's callbacks for one connection, forwarded with the index.
  class Handler : public Envoy::Network::ConnectionCallbacks, public Envoy::Network::ReadFilter {
  public:
    Handler(TcpBenchmarkClientImpl& client, uint32_t index) : client_(client), index_(index) {}

    // Envoy::Network::ConnectionCallbacks
    void onEvent(Envoy::Network::ConnectionEvent event) override {
      client_.onEvent(index_, event);
    }
    void onAboveWriteBufferHighWatermark() override { client_.onWriteBlocked(index_, true); }
    void onBelowWriteBufferLowWatermark() override { client_.onWriteBlocked(index_, false); }

    // Envoy::Network::ReadFilter
    Envoy::Network::FilterStatus onData(Envoy::Buffer::Instance& data, bool) override {
      client_.onData(index_, data);
      return Envoy::Network::FilterStatus::StopIteration;
    }
    Envoy::Network::FilterStatus onNewConnection() override {
      return Envoy::Network::FilterStatus::Continue;
    }
    void initializeReadFilterCallbacks(Envoy::Network::ReadFilterCallbacks&) override {}

  private:
    TcpBenchmarkClientImpl& client_;
    const uint32_t index_;
  };

  struct InflightMessage {
    Envoy::MonotonicTime sent_at;
    CompletionCallback completion_callback;
  };

  struct Connection {
    State state{State::Connecting};
    Envoy::Network::ClientConnectionPtr connection;
    // Shared because the connection takes a shared_ptr read filter; the same object is the
    // connection callbacks.
    std::shared_ptr<Handler> handler;
    std::deque<InflightMessage> inflight;
    Envoy::Buffer::OwnedImpl received;
    bool write_blocked{false};
  };

  void open(uint32_t index);
  void onEvent(uint32_t index, Envoy::Network::ConnectionEvent event);
  void onData(uint32_t index, Envoy::Buffer::Instance& data);
  void onWriteBlocked(uint32_t index, bool blocked);
  void onEcho(Connection& connection, const std::string& bytes);
  void closeConnection(uint32_t index, Envoy::Network::ConnectionCloseType type);
  void completeInflight(Connection& connection, bool success);
  // Exits the dispatcher run loop started by prepare()/finish() when its condition is met.
  void maybeExitWaitLoop();
  bool anyInflight() const;

  Envoy::Api::Api& api_;
  Envoy::Event::Dispatcher& dispatcher_;
  Envoy::Stats::ScopeSharedPtr scope_;
  StatisticPtr message_latency_statistic_;
  Envoy::Upstream::ClusterManagerPtr& cluster_manager_;
  const std::string cluster_name_;
  const RequestGenerator request_generator_;
  const uint32_t connection_count_;
  const uint32_t max_inflight_per_connection_;
  const bool expect_echo_;
  const std::chrono::nanoseconds drain_duration_;
  const std::chrono::seconds open_timeout_;

  TcpCounters counters_;
  std::vector<Connection> connections_;
  std::string message_;
  uint32_t next_connection_{0};
  uint32_t pending_opens_{0};
  bool measure_latencies_{false};
  enum class WaitingFor { Nothing, Opens, Echoes };
  WaitingFor waiting_for_{WaitingFor::Nothing};
  Envoy::Event::TimerPtr wait_timer_;
  bool finished_{false};
};

} // namespace Client
} // namespace Nighthawk
