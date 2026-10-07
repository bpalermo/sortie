#pragma once

#include "envoy/api/api.h"
#include "envoy/event/dispatcher.h"
#include "envoy/http/conn_pool.h"
#include "envoy/network/address.h"
#include "envoy/runtime/runtime.h"
#include "envoy/stats/scope.h"
#include "envoy/stats/store.h"
#include "envoy/upstream/upstream.h"

#include "nighthawk/client/benchmark_client.h"
#include "nighthawk/common/request_source.h"
#include "nighthawk/common/sequencer.h"
#include "nighthawk/common/statistic.h"
#include "nighthawk/user_defined_output/user_defined_output_plugin.h"

#include "source/common/common/logger.h"
#include "source/common/common/random_generator.h"
#include "source/common/http/http1/conn_pool.h"
#include "source/common/http/http2/conn_pool.h"
#include "source/common/runtime/runtime_impl.h"

#include "engine/api/client/options.pb.h"

#include "engine/source/client/stream_decoder.h"
#include "engine/source/common/statistic_impl.h"

#include "absl/container/flat_hash_map.h"

namespace Nighthawk {
namespace Client {

using namespace std::chrono_literals;

// The failure classes below refine stream_resets and pool_connection_failure without changing
// them: every reset is counted once in stream_resets and once in exactly one of the
// stream_resets_<phase> counters (plus once in a lazily created stream_resets_<reason>, see
// streamResetReasonCounter()); every pool failure is counted once in one of the
// pool_failure_<reason> counters, where the overflow reason is the pre-existing pool_overflow.
// pool_failure_timeout (the connect timeout, --timeout) is the one reason that was previously
// not counted under benchmark.* at all; it stays outside pool_connection_failure.
// http_inflight_lost is not a failure class of its own making: it counts the requests that were
// issued and had neither completed nor failed when finish() stopped waiting for them (after
// --timeout, or at once when the execution was cancelled or failed). Nothing is known about how
// they would have ended, so they are in no other counter: not in stream_resets, since nothing
// reset them, and in no status class. With it, every request tryStartRequest() accepted is
// accounted for in the counters of the run it was issued in.
#define ALL_BENCHMARK_CLIENT_COUNTERS(COUNTER)                                                     \
  COUNTER(stream_resets)                                                                           \
  COUNTER(stream_resets_before_headers)                                                            \
  COUNTER(stream_resets_incomplete_body)                                                           \
  COUNTER(http_1xx)                                                                                \
  COUNTER(http_2xx)                                                                                \
  COUNTER(http_3xx)                                                                                \
  COUNTER(http_4xx)                                                                                \
  COUNTER(http_5xx)                                                                                \
  COUNTER(http_xxx)                                                                                \
  COUNTER(http_inflight_lost)                                                                      \
  COUNTER(pool_overflow)                                                                           \
  COUNTER(grpc_error)                                                                              \
  COUNTER(pool_connection_failure)                                                                 \
  COUNTER(pool_failure_local_connection_failure)                                                   \
  COUNTER(pool_failure_remote_connection_failure)                                                  \
  COUNTER(pool_failure_timeout)                                                                    \
  COUNTER(user_defined_plugin_handle_headers_failure)                                              \
  COUNTER(user_defined_plugin_handle_data_failure)

// For counter metrics, Nighthawk use Envoy Counter directly. For histogram metrics, Nighthawk uses
// its own Statistic instead of Envoy Histogram. Here BenchmarkClientCounters contains only counters
// while BenchmarkClientStatistic contains only histograms.
struct BenchmarkClientCounters {
  ALL_BENCHMARK_CLIENT_COUNTERS(GENERATE_COUNTER_STRUCT)
};

// BenchmarkClientStatistic contains only histogram metrics.
struct BenchmarkClientStatistic {
  BenchmarkClientStatistic(BenchmarkClientStatistic&& statistic) noexcept;
  BenchmarkClientStatistic(StatisticPtr&& connect_stat, StatisticPtr&& response_stat,
                           StatisticPtr&& response_header_size_stat,
                           StatisticPtr&& response_body_size_stat, StatisticPtr&& latency_1xx_stat,
                           StatisticPtr&& latency_2xx_stat, StatisticPtr&& latency_3xx_stat,
                           StatisticPtr&& latency_4xx_stat, StatisticPtr&& latency_5xx_stat,
                           StatisticPtr&& latency_xxx_stat, StatisticPtr&& origin_latency_statistic,
                           StatisticPtr&& latency_grpc_ok_stat);

  // These are declared order dependent. Changing ordering may trigger on assert upon
  // destruction when tls has been involved during usage.
  StatisticPtr connect_statistic;
  StatisticPtr response_statistic;
  StatisticPtr response_header_size_statistic;
  StatisticPtr response_body_size_statistic;
  StatisticPtr latency_1xx_statistic;
  StatisticPtr latency_2xx_statistic;
  StatisticPtr latency_3xx_statistic;
  StatisticPtr latency_4xx_statistic;
  StatisticPtr latency_5xx_statistic;
  StatisticPtr latency_xxx_statistic;
  StatisticPtr origin_latency_statistic;
  StatisticPtr latency_grpc_ok_statistic;
};

class Http1PoolImpl : public Envoy::Http::FixedHttpConnPoolImpl {
public:
  enum class ConnectionReuseStrategy {
    MRU,
    LRU,
  };
  using Envoy::Http::FixedHttpConnPoolImpl::FixedHttpConnPoolImpl;
  Envoy::Http::ConnectionPool::Cancellable*
  newStream(Envoy::Http::ResponseDecoder& response_decoder,
            Envoy::Http::ConnectionPool::Callbacks& callbacks,
            const Instance::StreamOptions& options) override;
  void setConnectionReuseStrategy(const ConnectionReuseStrategy connection_reuse_strategy) {
    connection_reuse_strategy_ = connection_reuse_strategy;
  }
  void setPrefetchConnections(const bool prefetch_connections) {
    prefetch_connections_ = prefetch_connections;
  }

private:
  ConnectionReuseStrategy connection_reuse_strategy_{};
  bool prefetch_connections_{};
};

class BenchmarkClientHttpImpl : public BenchmarkClient,
                                public StreamDecoderCompletionCallback,
                                public Envoy::Logger::Loggable<Envoy::Logger::Id::main> {
public:
  BenchmarkClientHttpImpl(Envoy::Api::Api& api, Envoy::Event::Dispatcher& dispatcher,
                          Envoy::Stats::Scope& scope, BenchmarkClientStatistic& statistic,
                          Envoy::Http::Protocol protocol,
                          Envoy::Upstream::ClusterManagerPtr& cluster_manager,
                          Envoy::Tracing::TracerSharedPtr& tracer, absl::string_view cluster_name,
                          RequestGenerator request_generator,
                          const bool provide_resource_backpressure,
                          absl::string_view latency_response_header_name,
                          std::vector<UserDefinedOutputNamePluginPair> user_defined_output_plugins);
  void setConnectionLimit(uint32_t connection_limit) { connection_limit_ = connection_limit; }
  void setMaxPendingRequests(uint32_t max_pending_requests) {
    max_pending_requests_ = max_pending_requests;
  }
  void setMaxActiveRequests(uint32_t max_active_requests) {
    max_active_requests_ = max_active_requests;
  }
  void setMaxRequestsPerConnection(uint32_t max_requests_per_connection) {
    max_requests_per_connection_ = max_requests_per_connection;
  }
  void setTimeout(std::chrono::seconds timeout) { timeout_ = timeout; }
  /**
   * Enables gRPC scoring: responses are judged by grpc-status rather than HTTP status alone.
   */
  void setGrpc(bool grpc) { grpc_ = grpc; }

  // BenchmarkClient
  void prepare() override {}
  /**
   * Waits for the requests that are still outstanding -- issued through tryStartRequest() and
   * neither completed nor failed, those queued in the pool for a connection included -- by running
   * the dispatcher until there are none or the timeout (setTimeout()) has passed. What completes
   * meanwhile is counted and timed as during the run. What is left is counted in
   * http_inflight_lost. Does not wait after abandonOutstandingWork().
   */
  void finish() override;
  void abandonOutstandingWork() override;
  void terminate() override;
  StatisticPtrMap statistics() const override;
  bool shouldMeasureLatencies() const override { return measure_latencies_; }
  void setShouldMeasureLatencies(bool measure_latencies) override {
    measure_latencies_ = measure_latencies;
  }
  bool tryStartRequest(CompletionCallback caller_completion_callback) override;
  Envoy::Stats::Scope& scope() const override { return *scope_; }

  /**
   * Returns additional output from any specified User Defined Output plugins.
   */
  std::vector<nighthawk::client::UserDefinedOutput> getUserDefinedOutputResults() const override;

  // StreamDecoderCompletionCallback
  void onComplete(bool success, const Envoy::Http::ResponseHeaderMap& headers,
                  GrpcStatusOpt grpc_status) override;
  void onStreamReset(StreamResetPhase phase, Envoy::Http::StreamResetReason reason) override;
  void onPoolFailure(Envoy::Http::ConnectionPool::PoolFailureReason reason) override;
  void exportLatency(const uint32_t response_code, const uint64_t latency_ns,
                     GrpcStatusOpt grpc_status) override;
  void handleResponseData(const Envoy::Buffer::Instance& response_data) override;

  // Helpers
  std::optional<::Envoy::Upstream::HttpPoolData> pool() {
    const auto thread_local_cluster = cluster_manager_->getThreadLocalCluster(cluster_name_);
    Envoy::Upstream::HostConstSharedPtr host =
        Envoy::Upstream::LoadBalancer::onlyAllowSynchronousHostSelection(
            thread_local_cluster->chooseHost(nullptr));
    return thread_local_cluster->httpConnPool(host, Envoy::Upstream::ResourcePriority::Default,
                                              protocol_, nullptr);
  }

private:
  /**
   * Records the outcome of a gRPC call in the per-status counters.
   * @return true when the call succeeded (grpc-status 0).
   */
  bool trackGrpcStatus(GrpcStatusOpt grpc_status);
  Envoy::Stats::Counter& grpcStatusCounter(GrpcStatusOpt grpc_status);
  Envoy::Stats::Counter& streamResetReasonCounter(Envoy::Http::StreamResetReason reason);
  /**
   * Takes one request off the outstanding ones, and ends the wait in finish() with the last.
   */
  void onRequestDone();

  Envoy::Api::Api& api_;
  Envoy::Event::Dispatcher& dispatcher_;
  Envoy::Stats::ScopeSharedPtr scope_;
  BenchmarkClientStatistic statistic_;
  const Envoy::Http::Protocol protocol_;
  std::chrono::seconds timeout_{30s};
  uint32_t connection_limit_{1};
  uint32_t max_pending_requests_{1};
  uint32_t max_active_requests_{UINT32_MAX};
  uint32_t max_requests_per_connection_{UINT32_MAX};
  Envoy::Event::TimerPtr timer_;
  Envoy::Random::RandomGeneratorImpl generator_;
  uint64_t requests_completed_{};
  uint64_t requests_initiated_{};
  // Requests tryStartRequest() handed to the pool that have neither completed nor failed in it.
  // Kept apart from requests_initiated_ - requests_completed_, which the closed-loop backpressure
  // in tryStartRequest() is computed from and which does not see pool failures.
  uint64_t requests_outstanding_{};
  Envoy::Event::TimerPtr finish_timer_;
  bool finished_{false};
  bool waiting_in_finish_{false};
  bool finish_timed_out_{false};
  bool abandon_outstanding_{false};
  // How long finish() waited. terminate() takes it off its own wait: both are bound by timeout_
  // and wait for the same requests.
  std::chrono::milliseconds finish_waited_{0};
  bool measure_latencies_{};
  BenchmarkClientCounters benchmark_client_counters_;
  Envoy::Upstream::ClusterManagerPtr& cluster_manager_;
  Envoy::Tracing::TracerSharedPtr& tracer_;
  std::string cluster_name_;
  const RequestGenerator request_generator_;
  const bool provide_resource_backpressure_;
  const std::string latency_response_header_name_;
  Envoy::Event::TimerPtr drain_timer_;
  std::vector<UserDefinedOutputNamePluginPair> user_defined_output_plugins_;
  bool grpc_{false};
  // Lazily created "grpc_status.<code>" counters, keyed by code; nullopt keys
  // "grpc_status.missing".
  absl::flat_hash_map<std::optional<Envoy::Grpc::Status::GrpcStatus>, Envoy::Stats::Counter*>
      grpc_status_counters_;
  // Lazily created "stream_resets_<reason>" counters, keyed by reset reason. Lazy so that the
  // set of names tracks Envoy's StreamResetReason without a hand-maintained list here.
  absl::flat_hash_map<Envoy::Http::StreamResetReason, Envoy::Stats::Counter*>
      stream_reset_reason_counters_;
};

} // namespace Client
} // namespace Nighthawk
