#pragma once

#include <chrono>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "envoy/api/api.h"
#include "envoy/event/deferred_deletable.h"
#include "envoy/event/dispatcher.h"
#include "envoy/event/timer.h"
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

#include "absl/types/optional.h"

namespace Nighthawk {
namespace Client {

#define ALL_TCP_COUNTERS(COUNTER)                                                                  \
  COUNTER(tcp_connections_opened)                                                                  \
  COUNTER(tcp_connect_failures)                                                                    \
  COUNTER(tcp_reconnects)                                                                          \
  COUNTER(tcp_connections_rotated)                                                                 \
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
 * keeps a fixed pool of connection slots per worker, opened in prepare() through the cluster --
 * so the transport socket the cluster carries (TLS for tcps://) applies -- and on every
 * tryStartRequest() writes the message -- exactly the message, no framing of its own -- on the
 * next slot's connection, round-robin. With expect_echo the write completes when those bytes
 * come back on the same connection, and the round trip is the latency; a TCP connection delivers
 * in order, so echoes are matched FIFO against the messages outstanding on it. Without
 * expect_echo the write completes at once, nothing is read and nothing is timed: a write only
 * queues bytes locally. A send scheduled for a slot whose connection is not open, has
 * max_inflight_per_connection unanswered messages, or is above its write high watermark, is
 * dropped and counted (tcp_unavailable, tcp_deferred) -- never queued or retried, like the stream
 * clients. finish() waits up to the drain duration for outstanding echoes, then closes.
 *
 * A connection that closes is reopened. Whatever ends it -- the peer closing or resetting it, a
 * connect that fails or does not complete within the timeout -- the slot connects again after a
 * backoff: 10 ms, doubling per consecutive retry to 1 s, and starting over once the slot has
 * gone 2 s without needing a retry. The first retry is quick so that one reset costs a
 * connection ~10 ms of its messages; the cap bounds what is missed after the target comes back
 * to 1 s per connection while keeping the connect rate against a dead target at one per second
 * per connection. The backoff does not start over on a mere successful connect: a proxy with no
 * upstream accepts and closes at once, and that would be a connect every 10 ms. Messages that
 * come due on a slot with no open connection are tcp_unavailable -- the schedule is open-loop,
 * so they are not held back and sent in a burst -- and messages unanswered on a connection when
 * it closes are tcp_inflight_lost.
 *
 * With expect_echo, a message's worth of bytes that is not the message -- or that comes with
 * nothing outstanding -- shows the connection's byte stream is not a stream of echoes, and
 * after that no boundary in it can be trusted. So the first mismatch on a connection ends the
 * matching on it: tcp_echo_mismatch is counted, an error is logged (once per worker), and the
 * connection is closed, its outstanding messages tcp_inflight_lost and untimed, then reopened
 * with the backoff above. A latency is therefore only ever recorded for an echo before which
 * every byte the connection returned was exactly what was sent: against a target that prefixes
 * or reframes its replies the statistic stays empty instead of filling with chance alignments.
 *
 * With max_messages_per_connection a connection is rotated: once it has sent that many
 * messages, its replacement is opened while it goes on carrying the slot's messages, and takes
 * over when it has connected -- so rotating neither drops nor delays a message, and a
 * connection carries the configured number plus whatever came due while the replacement
 * connected. The retired connection is sent nothing more, is given up to the timeout for its
 * outstanding echoes (those still missing then are tcp_inflight_lost), and is closed. A
 * replacement that fails to connect is retried with the slot's backoff, the old connection
 * serving meanwhile.
 *
 * Counters live under the "benchmark." scope: tcp_connections_opened (every successful connect:
 * the initial ones, tcp_reconnects and tcp_connections_rotated), tcp_connect_failures (every
 * failed attempt, so connection-setup success is opened / (opened + failures)), tcp_reconnects
 * (a slot connected again after its connection closed or failed to open),
 * tcp_connections_rotated (a replacement took over from a connection that reached its message
 * limit), tcp_messages_sent, tcp_messages_received, tcp_deferred, tcp_unavailable,
 * tcp_connection_closed (the peer closed a connection in use), tcp_echo_mismatch (connections
 * closed because what came back was not the message), tcp_inflight_lost, tcp_drain_incomplete,
 * tcp_write_blocked. Statistics: benchmark_tcp.message_latency, and
 * benchmark_tcp.connect_latency -- connect() to connected, the TLS handshake included, for
 * every successful connect whether initial, reconnect or rotation.
 */
class TcpBenchmarkClientImpl : public BenchmarkClient,
                               public Envoy::Logger::Loggable<Envoy::Logger::Id::main> {
public:
  /**
   * @param api Envoy api.
   * @param dispatcher the worker's dispatcher.
   * @param scope the worker's stats scope; counters are created under "benchmark." in it.
   * @param message_latency_statistic statistic that records per-message send-to-echo latencies.
   * @param connect_latency_statistic statistic that records how long each successful connect
   * took.
   * @param cluster_manager cluster manager holding the worker's cluster.
   * @param cluster_name name of the worker's cluster.
   * @param request_generator yields the request whose body is the message; it must not be
   * empty when expect_echo is set, since the echo is matched by its bytes.
   * @param connections connections (slots) this worker keeps open.
   * @param max_inflight_per_connection unanswered messages a connection may hold before sends
   * on it are deferred.
   * @param expect_echo whether the peer echoes every message, which is what gets timed.
   * @param drain_duration how long finish() waits for outstanding echoes.
   * @param timeout how long a connect may take, which bounds prepare()'s wait for the first
   * connections, and how long a rotated connection is given for its outstanding echoes.
   * @param max_messages_per_connection messages after which a connection is rotated; 0 never
   * rotates.
   */
  TcpBenchmarkClientImpl(Envoy::Api::Api& api, Envoy::Event::Dispatcher& dispatcher,
                         Envoy::Stats::Scope& scope, StatisticPtr&& message_latency_statistic,
                         StatisticPtr&& connect_latency_statistic,
                         Envoy::Upstream::ClusterManagerPtr& cluster_manager,
                         absl::string_view cluster_name, RequestGenerator request_generator,
                         uint32_t connections, uint32_t max_inflight_per_connection,
                         bool expect_echo, std::chrono::nanoseconds drain_duration,
                         std::chrono::seconds timeout, uint32_t max_messages_per_connection = 0);
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
   * @return uint32_t the number of slots whose connection is open for sending.
   */
  uint32_t openConnections() const;

private:
  // Retiring: rotated out; sent nothing more, open only for its outstanding echoes.
  enum class State { Connecting, Open, Retiring, Closed };
  enum class Role { Active, Standby, Retiring, None };
  struct Link;

  // Envoy's callbacks for one connection, forwarded with the link they belong to.
  class Handler : public Envoy::Network::ConnectionCallbacks, public Envoy::Network::ReadFilter {
  public:
    Handler(TcpBenchmarkClientImpl& client, Link& link) : client_(client), link_(&link) {}
    // The link is going away; whatever its connection still raises is not for it.
    void detach() { link_ = nullptr; }

    // Envoy::Network::ConnectionCallbacks
    void onEvent(Envoy::Network::ConnectionEvent event) override {
      if (link_ != nullptr) {
        client_.onEvent(*link_, event);
      }
    }
    void onAboveWriteBufferHighWatermark() override {
      if (link_ != nullptr) {
        client_.onWriteBlocked(*link_, true);
      }
    }
    void onBelowWriteBufferLowWatermark() override {
      if (link_ != nullptr) {
        client_.onWriteBlocked(*link_, false);
      }
    }

    // Envoy::Network::ReadFilter
    Envoy::Network::FilterStatus onData(Envoy::Buffer::Instance& data, bool) override {
      if (link_ != nullptr) {
        client_.onData(*link_, data);
      } else {
        data.drain(data.length());
      }
      return Envoy::Network::FilterStatus::StopIteration;
    }
    Envoy::Network::FilterStatus onNewConnection() override {
      return Envoy::Network::FilterStatus::Continue;
    }
    void initializeReadFilterCallbacks(Envoy::Network::ReadFilterCallbacks&) override {}

  private:
    TcpBenchmarkClientImpl& client_;
    Link* link_;
  };

  struct InflightMessage {
    Envoy::MonotonicTime sent_at;
    CompletionCallback completion_callback;
  };

  // One TCP connection. Deferred-deletable because it is let go of from inside its own
  // connection's callbacks.
  struct Link : public Envoy::Event::DeferredDeletable {
    ~Link() override {
      if (handler != nullptr) {
        handler->detach();
      }
    }
    uint32_t slot{0};
    State state{State::Connecting};
    // Whether this is its slot's first connect, the one prepare() waits for.
    bool initial{false};
    bool initial_pending{false};
    Envoy::Network::ClientConnectionPtr connection;
    // Shared because the connection takes a shared_ptr read filter; the same object is the
    // connection callbacks.
    std::shared_ptr<Handler> handler;
    std::deque<InflightMessage> inflight;
    Envoy::Buffer::OwnedImpl received;
    bool write_blocked{false};
    uint64_t sent{0};
    Envoy::MonotonicTime connect_started;
    // Bounds the connect while Connecting, and the wait for echoes while Retiring.
    Envoy::Event::TimerPtr timer;
  };

  // A place in the pool: the connection messages go to, the one being opened to replace it, and
  // the ones it replaced that still wait for echoes.
  struct Slot {
    std::unique_ptr<Link> active;
    std::unique_ptr<Link> standby;
    std::vector<std::unique_ptr<Link>> retiring;
    Envoy::Event::TimerPtr retry_timer;
    std::chrono::milliseconds backoff{0};
    // When the retry timer last ran.
    absl::optional<Envoy::MonotonicTime> last_retry;
  };

  // Opens a connection for the slot, as its active one or as the standby that will replace it.
  void open(uint32_t slot_index, Role role, bool initial);
  // The retry timer's work: whichever of the active connection and its replacement is missing.
  void connectNext(uint32_t slot_index);
  void scheduleRetry(uint32_t slot_index);
  void onEvent(Link& link, Envoy::Network::ConnectionEvent event);
  void onData(Link& link, Envoy::Buffer::Instance& data);
  void onWriteBlocked(Link& link, bool blocked);
  void onTimer(Link& link);
  void onEcho(Link& link, const std::string& bytes);
  void onEchoMismatch(Link& link);
  // The standby has connected: it becomes the active connection, and the one it replaces retires.
  void promote(Slot& slot);
  // Closes a link of our own accord: its unanswered messages are lost, and the close event that
  // follows lets go of it without counting anything. Whoever wants the slot connected again
  // closes with NoFlush, which raises that event before returning, and schedules the retry.
  void closeLink(Link& link, Envoy::Network::ConnectionCloseType type);
  // Takes the link out of its slot and hands it to the dispatcher to delete.
  Role release(Link& link);
  void initialAttemptDone(Link& link);
  bool rotationDue(const Link& link) const {
    return max_messages_per_connection_ > 0 && link.sent >= max_messages_per_connection_;
  }
  void completeInflight(Link& link, bool success);
  // Every link there is; a copy, since closing one takes it out of its slot.
  std::vector<Link*> links() const;
  // Exits the dispatcher run loop started by prepare()/finish() when its condition is met.
  void maybeExitWaitLoop();
  bool anyInflight() const;

  Envoy::Api::Api& api_;
  Envoy::Event::Dispatcher& dispatcher_;
  Envoy::Stats::ScopeSharedPtr scope_;
  StatisticPtr message_latency_statistic_;
  StatisticPtr connect_latency_statistic_;
  Envoy::Upstream::ClusterManagerPtr& cluster_manager_;
  const std::string cluster_name_;
  const RequestGenerator request_generator_;
  const uint32_t connection_count_;
  const uint32_t max_inflight_per_connection_;
  const bool expect_echo_;
  const std::chrono::nanoseconds drain_duration_;
  const std::chrono::seconds timeout_;
  const uint32_t max_messages_per_connection_;

  TcpCounters counters_;
  std::vector<Slot> slots_;
  std::string message_;
  uint32_t next_slot_{0};
  uint32_t pending_opens_{0};
  bool measure_latencies_{false};
  enum class WaitingFor { Nothing, Opens, Echoes };
  WaitingFor waiting_for_{WaitingFor::Nothing};
  Envoy::Event::TimerPtr drain_timer_;
  bool finished_{false};
  bool echo_mismatch_logged_{false};
};

} // namespace Client
} // namespace Nighthawk
