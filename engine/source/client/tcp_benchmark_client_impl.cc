#include "engine/source/client/tcp_benchmark_client_impl.h"

#include <utility>

namespace Nighthawk {
namespace Client {

using namespace std::chrono_literals;

TcpBenchmarkClientImpl::TcpBenchmarkClientImpl(
    Envoy::Api::Api& api, Envoy::Event::Dispatcher& dispatcher, Envoy::Stats::Scope& scope,
    StatisticPtr&& message_latency_statistic, Envoy::Upstream::ClusterManagerPtr& cluster_manager,
    absl::string_view cluster_name, RequestGenerator request_generator, uint32_t connections,
    uint32_t max_inflight_per_connection, bool expect_echo, std::chrono::nanoseconds drain_duration,
    std::chrono::seconds open_timeout)
    : api_(api), dispatcher_(dispatcher), scope_(scope.createScope("benchmark.")),
      message_latency_statistic_(std::move(message_latency_statistic)),
      cluster_manager_(cluster_manager), cluster_name_(std::string(cluster_name)),
      request_generator_(std::move(request_generator)), connection_count_(connections),
      max_inflight_per_connection_(max_inflight_per_connection), expect_echo_(expect_echo),
      drain_duration_(drain_duration), open_timeout_(open_timeout),
      counters_({ALL_TCP_COUNTERS(POOL_COUNTER(*scope_))}) {
  RELEASE_ASSERT(connection_count_ > 0, "at least one connection is required");
  RELEASE_ASSERT(max_inflight_per_connection_ > 0, "max_inflight_per_connection must be positive");
  message_latency_statistic_->setId("benchmark_tcp.message_latency");
  connections_.resize(connection_count_);
  for (uint32_t i = 0; i < connection_count_; i++) {
    connections_[i].handler = std::make_shared<Handler>(*this, i);
  }
}

TcpBenchmarkClientImpl::~TcpBenchmarkClientImpl() = default;

void TcpBenchmarkClientImpl::prepare() {
  RequestPtr request = request_generator_();
  RELEASE_ASSERT(request != nullptr, "the request source did not yield a request");
  message_ = request->body();
  RELEASE_ASSERT(
      !expect_echo_ || !message_.empty(),
      "an echoed TCP message cannot be empty: its bytes are what the echo is matched by");

  for (uint32_t i = 0; i < connection_count_; i++) {
    open(i);
  }
  if (pending_opens_ == 0) {
    return;
  }
  waiting_for_ = WaitingFor::Opens;
  wait_timer_ = dispatcher_.createTimer([this]() {
    ENVOY_LOG(warn, "Timed out waiting for {} of {} TCP connections.", pending_opens_,
              connection_count_);
    dispatcher_.exit();
  });
  wait_timer_->enableTimer(open_timeout_);
  dispatcher_.run(Envoy::Event::Dispatcher::RunType::RunUntilExit);
  wait_timer_.reset();
  waiting_for_ = WaitingFor::Nothing;
  // A connection still connecting when the wait expired is a connect failure, and closed: the
  // run starts with what it has, and nothing joins it partway through the measurement.
  for (uint32_t i = 0; i < connection_count_; i++) {
    if (connections_[i].state == State::Connecting) {
      counters_.tcp_connect_failures_.inc();
      closeConnection(i, Envoy::Network::ConnectionCloseType::NoFlush);
    }
  }
  ENVOY_LOG(info, "Opened {} of {} TCP connections.", openConnections(), connection_count_);
}

void TcpBenchmarkClientImpl::open(uint32_t index) {
  Connection& connection = connections_[index];
  const auto thread_local_cluster = cluster_manager_->getThreadLocalCluster(cluster_name_);
  Envoy::Upstream::Host::CreateConnectionData data =
      thread_local_cluster == nullptr
          ? Envoy::Upstream::Host::CreateConnectionData{nullptr, nullptr}
          : thread_local_cluster->tcpConn(nullptr);
  if (data.connection_ == nullptr) {
    connection.state = State::Closed;
    counters_.tcp_connect_failures_.inc();
    return;
  }
  connection.connection = std::move(data.connection_);
  connection.connection->addConnectionCallbacks(*connection.handler);
  connection.connection->addReadFilter(connection.handler);
  connection.connection->noDelay(true);
  pending_opens_++;
  connection.connection->connect();
}

void TcpBenchmarkClientImpl::onEvent(uint32_t index, Envoy::Network::ConnectionEvent event) {
  Connection& connection = connections_[index];
  switch (event) {
  case Envoy::Network::ConnectionEvent::Connected:
    if (connection.state == State::Connecting) {
      connection.state = State::Open;
      counters_.tcp_connections_opened_.inc();
      pending_opens_--;
      maybeExitWaitLoop();
    }
    break;
  case Envoy::Network::ConnectionEvent::ConnectedZeroRtt:
    break;
  case Envoy::Network::ConnectionEvent::RemoteClose:
  case Envoy::Network::ConnectionEvent::LocalClose:
    if (connection.state == State::Connecting) {
      counters_.tcp_connect_failures_.inc();
      pending_opens_--;
    } else if (connection.state == State::Open) {
      counters_.tcp_connection_closed_.inc();
    }
    if (connection.state != State::Closed) {
      connection.state = State::Closed;
      completeInflight(connection, /*success=*/false);
      maybeExitWaitLoop();
    }
    break;
  }
}

bool TcpBenchmarkClientImpl::tryStartRequest(CompletionCallback caller_completion_callback) {
  // Open-loop contract: this always "starts" the scheduled message. One that cannot be sent
  // right now is dropped (never queued or retried) and completes immediately.
  Connection& connection = connections_[next_connection_];
  next_connection_ = (next_connection_ + 1) % connection_count_;

  if (connection.state != State::Open) {
    counters_.tcp_unavailable_.inc();
    dispatcher_.post([cb = std::move(caller_completion_callback)]() { cb(true, false); });
    return true;
  }
  if (connection.write_blocked || connection.inflight.size() >= max_inflight_per_connection_) {
    counters_.tcp_deferred_.inc();
    dispatcher_.post([cb = std::move(caller_completion_callback)]() { cb(true, false); });
    return true;
  }

  Envoy::Buffer::OwnedImpl buffer(message_);
  if (expect_echo_) {
    connection.inflight.push_back(
        {api_.timeSource().monotonicTime(), std::move(caller_completion_callback)});
  }
  connection.connection->write(buffer, /*end_stream=*/false);
  counters_.tcp_messages_sent_.inc();
  if (!expect_echo_) {
    dispatcher_.post([cb = std::move(caller_completion_callback)]() { cb(true, true); });
  }
  return true;
}

void TcpBenchmarkClientImpl::onData(uint32_t index, Envoy::Buffer::Instance& data) {
  Connection& connection = connections_[index];
  if (!expect_echo_ || connection.state != State::Open) {
    data.drain(data.length());
    return;
  }
  // A stream of echoes, each exactly the message, possibly split or coalesced by TCP.
  connection.received.move(data);
  while (connection.received.length() >= message_.size()) {
    std::string bytes(message_.size(), '\0');
    connection.received.copyOut(0, message_.size(), bytes.data());
    connection.received.drain(message_.size());
    onEcho(connection, bytes);
  }
}

void TcpBenchmarkClientImpl::onEcho(Connection& connection, const std::string& bytes) {
  // In order on one connection: the echo is for the oldest outstanding message.
  if (bytes != message_ || connection.inflight.empty()) {
    counters_.tcp_echo_mismatch_.inc();
    return;
  }
  InflightMessage message = std::move(connection.inflight.front());
  connection.inflight.pop_front();
  counters_.tcp_messages_received_.inc();
  if (measure_latencies_) {
    message_latency_statistic_->addValue(
        (api_.timeSource().monotonicTime() - message.sent_at).count());
  }
  message.completion_callback(true, true);
  maybeExitWaitLoop();
}

void TcpBenchmarkClientImpl::onWriteBlocked(uint32_t index, bool blocked) {
  Connection& connection = connections_[index];
  if (blocked && !connection.write_blocked) {
    counters_.tcp_write_blocked_.inc();
  }
  connection.write_blocked = blocked;
}

void TcpBenchmarkClientImpl::closeConnection(uint32_t index,
                                             Envoy::Network::ConnectionCloseType type) {
  Connection& connection = connections_[index];
  if (connection.state == State::Closed || connection.connection == nullptr) {
    connection.state = State::Closed;
    return;
  }
  if (connection.state == State::Connecting) {
    pending_opens_--;
  }
  connection.state = State::Closed;
  completeInflight(connection, /*success=*/false);
  // The close event that follows finds the connection already Closed and does nothing.
  connection.connection->close(type);
}

void TcpBenchmarkClientImpl::completeInflight(Connection& connection, bool success) {
  while (!connection.inflight.empty()) {
    InflightMessage message = std::move(connection.inflight.front());
    connection.inflight.pop_front();
    if (!success) {
      counters_.tcp_inflight_lost_.inc();
    }
    message.completion_callback(true, success);
  }
}

bool TcpBenchmarkClientImpl::anyInflight() const {
  for (const Connection& connection : connections_) {
    if (!connection.inflight.empty()) {
      return true;
    }
  }
  return false;
}

void TcpBenchmarkClientImpl::maybeExitWaitLoop() {
  switch (waiting_for_) {
  case WaitingFor::Opens:
    if (pending_opens_ == 0) {
      dispatcher_.exit();
    }
    break;
  case WaitingFor::Echoes:
    if (!anyInflight()) {
      dispatcher_.exit();
    }
    break;
  case WaitingFor::Nothing:
    break;
  }
}

uint32_t TcpBenchmarkClientImpl::openConnections() const {
  uint32_t open = 0;
  for (const Connection& connection : connections_) {
    if (connection.state == State::Open) {
      open++;
    }
  }
  return open;
}

void TcpBenchmarkClientImpl::finish() {
  if (finished_) {
    return;
  }
  finished_ = true;
  if (anyInflight()) {
    waiting_for_ = WaitingFor::Echoes;
    wait_timer_ = dispatcher_.createTimer([this]() { dispatcher_.exit(); });
    wait_timer_->enableTimer(
        std::chrono::ceil<std::chrono::milliseconds>(drain_duration_));
    dispatcher_.run(Envoy::Event::Dispatcher::RunType::RunUntilExit);
    wait_timer_.reset();
    waiting_for_ = WaitingFor::Nothing;
  }
  // Echoes the drain window did not bring are lost; account for them before the worker
  // snapshots its counters, then close with what is written flushed.
  uint32_t incomplete = 0;
  for (Connection& connection : connections_) {
    if (!connection.inflight.empty()) {
      incomplete++;
      counters_.tcp_drain_incomplete_.inc();
      completeInflight(connection, /*success=*/false);
    }
  }
  if (incomplete > 0) {
    ENVOY_LOG(info,
              "{} TCP connection(s) still had unanswered messages after the {} ms drain window "
              "(counted in benchmark.tcp_drain_incomplete).",
              incomplete,
              std::chrono::duration_cast<std::chrono::milliseconds>(drain_duration_).count());
  }
  for (uint32_t i = 0; i < connection_count_; i++) {
    closeConnection(i, Envoy::Network::ConnectionCloseType::FlushWrite);
  }
}

void TcpBenchmarkClientImpl::terminate() {
  finish();
  setShouldMeasureLatencies(false);
  for (uint32_t i = 0; i < connection_count_; i++) {
    closeConnection(i, Envoy::Network::ConnectionCloseType::NoFlush);
  }
}

StatisticPtrMap TcpBenchmarkClientImpl::statistics() const {
  StatisticPtrMap statistics;
  statistics[message_latency_statistic_->id()] = message_latency_statistic_.get();
  return statistics;
}

} // namespace Client
} // namespace Nighthawk
