#pragma once

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "envoy/api/api.h"
#include "envoy/event/dispatcher.h"
#include "envoy/network/socket.h"
#include "envoy/stats/scope.h"
#include "envoy/stats/stats_macros.h"
#include "envoy/upstream/cluster_manager.h"

#include "nighthawk/client/benchmark_client.h"
#include "nighthawk/common/request_source.h"
#include "nighthawk/common/statistic.h"

#include "source/common/common/logger.h"

namespace Nighthawk {
namespace Client {

#define ALL_UDP_COUNTERS(COUNTER)                                                                  \
  COUNTER(udp_datagrams_sent)                                                                      \
  COUNTER(udp_datagrams_received)                                                                  \
  COUNTER(udp_lost)                                                                                \
  COUNTER(udp_unexpected)                                                                          \
  COUNTER(udp_send_errors)                                                                         \
  COUNTER(udp_deferred)                                                                            \
  COUNTER(udp_unavailable)

struct UdpCounters {
  ALL_UDP_COUNTERS(GENERATE_COUNTER_STRUCT)
};

/**
 * BenchmarkClient for UDP: what Envoy's udp_proxy fronts. One datagram socket per worker,
 * connected to the target in prepare(). Every tryStartRequest() sends the message as one
 * datagram, prefixed with a sequence number, and completes when a datagram carrying that number
 * comes back -- datagrams reorder and vanish, so the match is by number, not order -- with the
 * round trip as the latency. A datagram unanswered for the timeout is lost (udp_lost) and its
 * request completes as a failure; that, not a connection error, is what the proxy drops. A send
 * scheduled with max_inflight datagrams unanswered is dropped and counted as udp_deferred, never
 * queued or retried, like the other clients.
 *
 * Counters live under the "benchmark." scope: udp_datagrams_sent, udp_datagrams_received,
 * udp_lost, udp_unexpected (a datagram matching no outstanding number: a duplicate, a late echo,
 * or not an echo), udp_send_errors, udp_deferred, udp_unavailable (the socket could not be set
 * up). Statistic: benchmark_udp.message_latency.
 */
class UdpBenchmarkClientImpl : public BenchmarkClient,
                               public Envoy::Logger::Loggable<Envoy::Logger::Id::main> {
public:
  /**
   * @param api Envoy api.
   * @param dispatcher the worker's dispatcher.
   * @param scope the worker's stats scope; counters are created under "benchmark." in it.
   * @param message_latency_statistic statistic that records per-datagram send-to-echo latencies.
   * @param cluster_manager cluster manager holding the worker's cluster, whose host is the target.
   * @param cluster_name name of the worker's cluster.
   * @param request_generator yields the request whose body is the message.
   * @param max_inflight unanswered datagrams allowed before sends are deferred.
   * @param timeout how long a datagram may go unanswered before it is lost.
   * @param drain_duration how long finish() waits for outstanding echoes.
   */
  UdpBenchmarkClientImpl(Envoy::Api::Api& api, Envoy::Event::Dispatcher& dispatcher,
                         Envoy::Stats::Scope& scope, StatisticPtr&& message_latency_statistic,
                         Envoy::Upstream::ClusterManagerPtr& cluster_manager,
                         absl::string_view cluster_name, RequestGenerator request_generator,
                         uint32_t max_inflight, std::chrono::nanoseconds timeout,
                         std::chrono::nanoseconds drain_duration);
  ~UdpBenchmarkClientImpl() override;

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
   * @return bool whether the socket is set up and sends are possible.
   */
  bool ready() const { return socket_ != nullptr; }

  /**
   * @param sequence a datagram's sequence number.
   * @return std::string the 16 hex digit prefix it is sent with.
   */
  static std::string sequencePrefix(uint64_t sequence);

  /**
   * How often the client looks for datagrams past their timeout.
   */
  static constexpr std::chrono::milliseconds kSweepInterval{50};

private:
  struct InflightDatagram {
    Envoy::MonotonicTime sent_at;
    CompletionCallback completion_callback;
  };

  void onReadable();
  void onDatagram(absl::string_view bytes);
  void sweep();
  void completeAll(bool success);

  Envoy::Api::Api& api_;
  Envoy::Event::Dispatcher& dispatcher_;
  Envoy::Stats::ScopeSharedPtr scope_;
  StatisticPtr message_latency_statistic_;
  Envoy::Upstream::ClusterManagerPtr& cluster_manager_;
  const std::string cluster_name_;
  const RequestGenerator request_generator_;
  const uint32_t max_inflight_;
  const std::chrono::nanoseconds timeout_;
  const std::chrono::nanoseconds drain_duration_;

  UdpCounters counters_;
  Envoy::Network::SocketPtr socket_;
  Envoy::Event::TimerPtr sweep_timer_;
  Envoy::Event::TimerPtr drain_timer_;
  std::string message_;
  // Keyed by sequence number; ordered, so the sweep stops at the first one still in time.
  std::map<uint64_t, InflightDatagram> inflight_;
  uint64_t next_sequence_{0};
  bool measure_latencies_{false};
  bool draining_{false};
  bool finished_{false};
};

} // namespace Client
} // namespace Nighthawk
