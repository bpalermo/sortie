#pragma once

#include <chrono>
#include <deque>
#include <memory>
#include <optional>
#include <vector>

#include "envoy/api/api.h"
#include "envoy/common/conn_pool.h"
#include "envoy/event/dispatcher.h"
#include "envoy/http/codec.h"
#include "envoy/http/conn_pool.h"
#include "envoy/stats/scope.h"
#include "envoy/stats/stats_macros.h"
#include "envoy/upstream/cluster_manager.h"
#include "envoy/upstream/load_balancer.h"

#include "nighthawk/client/benchmark_client.h"
#include "nighthawk/common/request_source.h"
#include "nighthawk/common/statistic.h"

#include "source/common/buffer/buffer_impl.h"
#include "source/common/common/logger.h"

#include "engine/source/common/websocket.h"

namespace Nighthawk {
namespace Client {

#define ALL_WEBSOCKET_STREAM_COUNTERS(COUNTER)                                                     \
  COUNTER(streams_opened)                                                                          \
  COUNTER(stream_open_failures)                                                                    \
  COUNTER(stream_upgrade_rejected)                                                                 \
  COUNTER(stream_messages_sent)                                                                    \
  COUNTER(stream_messages_received)                                                                \
  COUNTER(stream_deferred)                                                                         \
  COUNTER(stream_unavailable)                                                                      \
  COUNTER(stream_resets)                                                                           \
  COUNTER(stream_early_close)                                                                      \
  COUNTER(stream_unexpected_message)                                                               \
  COUNTER(stream_protocol_errors)                                                                  \
  COUNTER(stream_inflight_lost)                                                                    \
  COUNTER(stream_drain_incomplete)                                                                 \
  COUNTER(stream_write_blocked)

struct WebSocketStreamCounters {
  ALL_WEBSOCKET_STREAM_COUNTERS(GENERATE_COUNTER_STRUCT)
};

/**
 * BenchmarkClient that drives WebSocket connections, the way GrpcStreamBenchmarkClientImpl
 * drives gRPC bidi streams -- same schedule, same accounting, same statistic -- so a plan can
 * switch between the two and compare. In prepare() it opens a fixed number of HTTP/1.1
 * connections, each with an Upgrade request, and waits for their 101s. Every tryStartRequest()
 * then sends one message on the next open connection (round-robin), framed per RFC 6455 and
 * prefixed with a sequence number, and completes when a data frame carrying that sequence number
 * comes back on the same connection. A send scheduled for a connection that already has
 * max_inflight_per_stream unanswered messages, or whose write buffer is above the high watermark,
 * is dropped and counted as stream_deferred -- never queued or retried. finish() sends a Close
 * frame on every connection and collects echoes for the drain duration.
 *
 * Counters live under the "benchmark." scope and carry the gRPC stream client's names, plus
 * stream_upgrade_rejected (the server did not answer the upgrade with a matching 101) and
 * stream_protocol_errors (a frame the decoder rejected). Statistic:
 * benchmark_stream.message_latency.
 */
class WebSocketStreamBenchmarkClientImpl : public BenchmarkClient,
                                           public Envoy::Logger::Loggable<Envoy::Logger::Id::main> {
public:
  /**
   * @param api Envoy api.
   * @param dispatcher the worker's dispatcher.
   * @param scope the worker's stats scope; counters are created under "benchmark." in it.
   * @param message_latency_statistic statistic that records per-message send-to-echo latencies.
   * @param cluster_manager cluster manager holding the worker's cluster.
   * @param cluster_name name of the worker's cluster.
   * @param request_generator yields the request whose headers (path, authority, extra headers)
   * make the upgrade request and whose body is the message sent on the connections.
   * @param streams number of connections this worker upgrades.
   * @param max_inflight_per_stream maximum unanswered messages per connection before sends are
   * deferred.
   * @param drain_duration how long finish() waits for echoes after sending Close.
   * @param open_timeout how long prepare() waits for the upgrades.
   * @param binary whether messages go as binary frames rather than text.
   */
  WebSocketStreamBenchmarkClientImpl(Envoy::Api::Api& api, Envoy::Event::Dispatcher& dispatcher,
                                     Envoy::Stats::Scope& scope,
                                     StatisticPtr&& message_latency_statistic,
                                     Envoy::Upstream::ClusterManagerPtr& cluster_manager,
                                     absl::string_view cluster_name,
                                     RequestGenerator request_generator, uint32_t streams,
                                     uint32_t max_inflight_per_stream,
                                     std::chrono::nanoseconds drain_duration,
                                     std::chrono::seconds open_timeout, bool binary);
  ~WebSocketStreamBenchmarkClientImpl() override;

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
   * @return uint32_t the number of connections currently open (including closing ones).
   */
  uint32_t openStreams() const;

  /**
   * @param sequence a message's sequence number.
   * @return std::string the prefix it is sent with: 16 hex digits, so the payload stays valid
   * UTF-8 for text frames.
   */
  static std::string sequencePrefix(uint64_t sequence);

private:
  // Opening: the upgrade request is out and no 101 came yet. Open: upgraded. Closing: we sent a
  // Close frame and wait for the server's. Closed: done, no encoder.
  enum class StreamState { Opening, Open, Closing, Closed };

  // Envoy callbacks for a single connection, forwarded to the owning client with the index.
  class StreamHandler : public Envoy::Http::ResponseDecoder,
                        public Envoy::Http::StreamCallbacks,
                        public Envoy::Http::ConnectionPool::Callbacks {
  public:
    StreamHandler(WebSocketStreamBenchmarkClientImpl& client, uint32_t index)
        : client_(client), index_(index) {}

    // Envoy::Http::ResponseDecoder
    void decode1xxHeaders(Envoy::Http::ResponseHeaderMapPtr&& headers) override {
      client_.onResponseHeaders(index_, std::move(headers), /*end_stream=*/false);
    }
    void decodeHeaders(Envoy::Http::ResponseHeaderMapPtr&& headers, bool end_stream) override {
      client_.onResponseHeaders(index_, std::move(headers), end_stream);
    }
    void decodeData(Envoy::Buffer::Instance& data, bool end_stream) override {
      client_.onResponseData(index_, data, end_stream);
    }
    void decodeTrailers(Envoy::Http::ResponseTrailerMapPtr&&) override {
      client_.onResponseEnd(index_);
    }
    void decodeMetadata(Envoy::Http::MetadataMapPtr&&) override {}
    void dumpState(std::ostream&, int) const override {}
    Envoy::Http::ResponseDecoderHandlePtr createResponseDecoderHandle() override { return nullptr; }

    // Envoy::Http::StreamCallbacks
    void onResetStream(Envoy::Http::StreamResetReason reason,
                       absl::string_view transport_failure_reason) override {
      client_.onStreamReset(index_, reason, transport_failure_reason);
    }
    void onAboveWriteBufferHighWatermark() override { client_.onWriteBlocked(index_, true); }
    void onBelowWriteBufferLowWatermark() override { client_.onWriteBlocked(index_, false); }

    // Envoy::Http::ConnectionPool::Callbacks
    void onPoolFailure(Envoy::Http::ConnectionPool::PoolFailureReason reason,
                       absl::string_view transport_failure_reason,
                       Envoy::Upstream::HostDescriptionConstSharedPtr) override {
      client_.onStreamOpenFailure(index_, reason, transport_failure_reason);
    }
    void onPoolReady(Envoy::Http::RequestEncoder& encoder,
                     Envoy::Upstream::HostDescriptionConstSharedPtr, Envoy::StreamInfo::StreamInfo&,
                     std::optional<Envoy::Http::Protocol>) override {
      client_.onStreamReady(index_, encoder);
    }

  private:
    WebSocketStreamBenchmarkClientImpl& client_;
    const uint32_t index_;
  };

  struct InflightMessage {
    uint64_t sequence;
    Envoy::MonotonicTime sent_at;
    CompletionCallback completion_callback;
  };

  struct Stream {
    StreamState state{StreamState::Opening};
    Envoy::Http::RequestEncoder* encoder{nullptr};
    Envoy::Http::ConnectionPool::Cancellable* cancellable{nullptr};
    std::unique_ptr<StreamHandler> handler;
    std::deque<InflightMessage> inflight;
    std::string key;
    Envoy::Buffer::OwnedImpl received;
    WebSocket::Decoder decoder;
    // A fragmented message being reassembled.
    std::string fragments;
    bool write_blocked{false};
  };

  std::optional<Envoy::Upstream::HttpPoolData> pool();
  void openStream(uint32_t index);
  void onStreamReady(uint32_t index, Envoy::Http::RequestEncoder& encoder);
  void onStreamOpenFailure(uint32_t index, Envoy::Http::ConnectionPool::PoolFailureReason reason,
                           absl::string_view transport_failure_reason);
  void onResponseHeaders(uint32_t index, Envoy::Http::ResponseHeaderMapPtr&& headers,
                         bool end_stream);
  void onResponseData(uint32_t index, Envoy::Buffer::Instance& data, bool end_stream);
  void onResponseEnd(uint32_t index);
  void onStreamReset(uint32_t index, Envoy::Http::StreamResetReason reason,
                     absl::string_view transport_failure_reason);
  void onWriteBlocked(uint32_t index, bool blocked);
  void onMessage(uint32_t index, const std::string& payload);
  void send(Stream& stream, const WebSocket::Frame& frame);
  // Marks the connection closed and fails any unanswered messages.
  void closeStream(uint32_t index);
  void completeInflight(Stream& stream, bool success);
  // Exits the dispatcher run loop started by prepare()/finish() when its condition is met.
  void maybeExitWaitLoop();

  Envoy::Api::Api& api_;
  Envoy::Event::Dispatcher& dispatcher_;
  Envoy::Stats::ScopeSharedPtr scope_;
  StatisticPtr message_latency_statistic_;
  Envoy::Upstream::ClusterManagerPtr& cluster_manager_;
  const std::string cluster_name_;
  const RequestGenerator request_generator_;
  const uint32_t stream_count_;
  const uint32_t max_inflight_per_stream_;
  const std::chrono::nanoseconds drain_duration_;
  const std::chrono::seconds open_timeout_;
  const bool binary_;

  WebSocketStreamCounters counters_;
  std::vector<Stream> streams_;
  HeaderMapPtr request_headers_;
  std::string message_;
  uint64_t next_sequence_{0};
  uint32_t next_stream_{0};
  uint32_t pending_opens_{0};
  bool measure_latencies_{false};
  // Set while prepare()/finish() run the dispatcher and wait for a condition.
  enum class WaitingFor { Nothing, Opens, Closes };
  WaitingFor waiting_for_{WaitingFor::Nothing};
  Envoy::Event::TimerPtr wait_timer_;
  bool finished_{false};
};

} // namespace Client
} // namespace Nighthawk
