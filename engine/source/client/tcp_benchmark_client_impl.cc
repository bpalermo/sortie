#include "engine/source/client/tcp_benchmark_client_impl.h"

#include <algorithm>
#include <utility>

namespace Nighthawk {
namespace Client {

using namespace std::chrono_literals;

namespace {

// See the class comment for why these.
constexpr std::chrono::milliseconds InitialBackoff = 10ms;
constexpr std::chrono::milliseconds MaxBackoff = 1000ms;
constexpr std::chrono::milliseconds BackoffStartsOverAfter = 2 * MaxBackoff;

} // namespace

TcpBenchmarkClientImpl::TcpBenchmarkClientImpl(
    Envoy::Api::Api& api, Envoy::Event::Dispatcher& dispatcher, Envoy::Stats::Scope& scope,
    StatisticPtr&& message_latency_statistic, StatisticPtr&& connect_latency_statistic,
    Envoy::Upstream::ClusterManagerPtr& cluster_manager, absl::string_view cluster_name,
    RequestGenerator request_generator, uint32_t connections, uint32_t max_inflight_per_connection,
    bool expect_echo, std::chrono::nanoseconds drain_duration, std::chrono::seconds timeout,
    uint32_t max_messages_per_connection)
    : api_(api), dispatcher_(dispatcher), scope_(scope.createScope("benchmark.")),
      message_latency_statistic_(std::move(message_latency_statistic)),
      connect_latency_statistic_(std::move(connect_latency_statistic)),
      cluster_manager_(cluster_manager), cluster_name_(std::string(cluster_name)),
      request_generator_(std::move(request_generator)), connection_count_(connections),
      max_inflight_per_connection_(max_inflight_per_connection), expect_echo_(expect_echo),
      drain_duration_(drain_duration), timeout_(timeout),
      max_messages_per_connection_(max_messages_per_connection),
      counters_({ALL_TCP_COUNTERS(POOL_COUNTER(*scope_))}) {
  RELEASE_ASSERT(connection_count_ > 0, "at least one connection is required");
  RELEASE_ASSERT(max_inflight_per_connection_ > 0, "max_inflight_per_connection must be positive");
  message_latency_statistic_->setId("benchmark_tcp.message_latency");
  connect_latency_statistic_->setId("benchmark_tcp.connect_latency");
  slots_.resize(connection_count_);
  for (uint32_t i = 0; i < connection_count_; i++) {
    slots_[i].backoff = InitialBackoff;
    slots_[i].retry_timer = dispatcher_.createTimer([this, i]() {
      // When the retry runs, not when it was scheduled: the quiet spell that starts the backoff
      // over is measured from here, and at the cap the two are a second apart.
      slots_[i].last_retry = api_.timeSource().monotonicTime();
      connectNext(i);
    });
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
    open(i, Role::Active, /*initial=*/true);
  }
  if (pending_opens_ > 0) {
    // Every connect is bounded by the timeout, so this ends. It waits for each slot's first
    // attempt only: the run starts with what it has then, and a slot that is still retrying
    // joins when it connects.
    waiting_for_ = WaitingFor::Opens;
    dispatcher_.run(Envoy::Event::Dispatcher::RunType::RunUntilExit);
    waiting_for_ = WaitingFor::Nothing;
  }
  ENVOY_LOG(info, "Opened {} of {} TCP connections.", openConnections(), connection_count_);
}

void TcpBenchmarkClientImpl::open(uint32_t slot_index, Role role, bool initial) {
  Slot& slot = slots_[slot_index];
  const auto thread_local_cluster = cluster_manager_->getThreadLocalCluster(cluster_name_);
  Envoy::Upstream::Host::CreateConnectionData data =
      thread_local_cluster == nullptr
          ? Envoy::Upstream::Host::CreateConnectionData{nullptr, nullptr}
          : thread_local_cluster->tcpConn(nullptr);
  if (data.connection_ == nullptr) {
    counters_.tcp_connect_failures_.inc();
    scheduleRetry(slot_index);
    return;
  }
  auto owned = std::make_unique<Link>();
  Link* link = owned.get();
  link->slot = slot_index;
  link->initial = initial;
  link->initial_pending = initial;
  link->handler = std::make_shared<Handler>(*this, *link);
  link->connection = std::move(data.connection_);
  link->connection->addConnectionCallbacks(*link->handler);
  link->connection->addReadFilter(link->handler);
  link->connection->noDelay(true);
  link->timer = dispatcher_.createTimer([this, link]() { onTimer(*link); });
  link->timer->enableTimer(timeout_);
  link->connect_started = api_.timeSource().monotonicTime();
  if (initial) {
    pending_opens_++;
  }
  // In its slot before it connects: the events connect() may raise look it up there.
  (role == Role::Standby ? slot.standby : slot.active) = std::move(owned);
  link->connection->connect();
}

void TcpBenchmarkClientImpl::connectNext(uint32_t slot_index) {
  Slot& slot = slots_[slot_index];
  if (finished_) {
    return;
  }
  if (slot.active == nullptr) {
    // A replacement that is already connecting will do.
    if (slot.standby == nullptr) {
      open(slot_index, Role::Active, /*initial=*/false);
    }
  } else if (slot.standby == nullptr && slot.active->state == State::Open &&
             rotationDue(*slot.active)) {
    open(slot_index, Role::Standby, /*initial=*/false);
  }
}

void TcpBenchmarkClientImpl::scheduleRetry(uint32_t slot_index) {
  Slot& slot = slots_[slot_index];
  if (finished_ || slot.retry_timer->enabled()) {
    return;
  }
  const Envoy::MonotonicTime now = api_.timeSource().monotonicTime();
  if (slot.last_retry.has_value() && now - slot.last_retry.value() >= BackoffStartsOverAfter) {
    slot.backoff = InitialBackoff;
  }
  slot.retry_timer->enableTimer(slot.backoff);
  slot.backoff = std::min(slot.backoff * 2, MaxBackoff);
}

void TcpBenchmarkClientImpl::initialAttemptDone(Link& link) {
  if (link.initial_pending) {
    link.initial_pending = false;
    pending_opens_--;
  }
}

void TcpBenchmarkClientImpl::onEvent(Link& link, Envoy::Network::ConnectionEvent event) {
  Slot& slot = slots_[link.slot];
  switch (event) {
  case Envoy::Network::ConnectionEvent::Connected:
    if (link.state != State::Connecting) {
      break;
    }
    link.state = State::Open;
    link.timer->disableTimer();
    counters_.tcp_connections_opened_.inc();
    connect_latency_statistic_->addValue(
        (api_.timeSource().monotonicTime() - link.connect_started).count());
    initialAttemptDone(link);
    if (slot.standby.get() == &link) {
      promote(slot);
    } else if (!link.initial) {
      counters_.tcp_reconnects_.inc();
    }
    maybeExitWaitLoop();
    break;
  case Envoy::Network::ConnectionEvent::ConnectedZeroRtt:
    break;
  case Envoy::Network::ConnectionEvent::RemoteClose:
  case Envoy::Network::ConnectionEvent::LocalClose: {
    const State previous = link.state;
    link.state = State::Closed;
    link.timer->disableTimer();
    initialAttemptDone(link);
    if (previous == State::Connecting) {
      counters_.tcp_connect_failures_.inc();
    } else if (previous == State::Open || previous == State::Retiring) {
      counters_.tcp_connection_closed_.inc();
    }
    completeInflight(link, /*success=*/false);
    const Role role = release(link);
    // A link closed by closeLink() arrives here Closed: whoever closed it decides what follows.
    if (previous != State::Closed && (role == Role::Active || role == Role::Standby)) {
      scheduleRetry(link.slot);
    }
    maybeExitWaitLoop();
    break;
  }
  }
}

void TcpBenchmarkClientImpl::promote(Slot& slot) {
  std::unique_ptr<Link> retired = std::move(slot.active);
  slot.active = std::move(slot.standby);
  // A retry scheduled when the old connection closed under its replacement has nothing left to
  // do, and while it is pending no rotation is started.
  slot.retry_timer->disableTimer();
  if (retired == nullptr) {
    // The connection it was to replace closed in the meantime.
    counters_.tcp_reconnects_.inc();
    return;
  }
  counters_.tcp_connections_rotated_.inc();
  Link& link = *retired;
  slot.retiring.push_back(std::move(retired));
  if (link.state != State::Open) {
    return;
  }
  link.state = State::Retiring;
  if (link.inflight.empty()) {
    closeLink(link, Envoy::Network::ConnectionCloseType::FlushWrite);
  } else {
    link.timer->enableTimer(timeout_);
  }
}

void TcpBenchmarkClientImpl::onTimer(Link& link) {
  if (link.state == State::Connecting) {
    // A connect is an attempt until it completes; this one did not.
    const uint32_t slot_index = link.slot;
    counters_.tcp_connect_failures_.inc();
    closeLink(link, Envoy::Network::ConnectionCloseType::NoFlush);
    scheduleRetry(slot_index);
    maybeExitWaitLoop();
  } else if (link.state == State::Retiring) {
    // The echoes it waited for did not come: lost.
    closeLink(link, Envoy::Network::ConnectionCloseType::NoFlush);
  }
}

bool TcpBenchmarkClientImpl::tryStartRequest(CompletionCallback caller_completion_callback) {
  // Open-loop contract: this always "starts" the scheduled message. One that cannot be sent
  // right now is dropped (never queued or retried) and completes immediately.
  const uint32_t slot_index = next_slot_;
  next_slot_ = (next_slot_ + 1) % connection_count_;
  Slot& slot = slots_[slot_index];

  if (slot.active == nullptr || slot.active->state != State::Open) {
    counters_.tcp_unavailable_.inc();
    dispatcher_.post([cb = std::move(caller_completion_callback)]() { cb(true, false); });
    return true;
  }
  Link& link = *slot.active;
  if (link.write_blocked || link.inflight.size() >= max_inflight_per_connection_) {
    counters_.tcp_deferred_.inc();
    dispatcher_.post([cb = std::move(caller_completion_callback)]() { cb(true, false); });
    return true;
  }

  Envoy::Buffer::OwnedImpl buffer(message_);
  if (expect_echo_) {
    link.inflight.push_back(
        {api_.timeSource().monotonicTime(), std::move(caller_completion_callback)});
  }
  link.connection->write(buffer, /*end_stream=*/false);
  link.sent++;
  counters_.tcp_messages_sent_.inc();
  if (!expect_echo_) {
    dispatcher_.post([cb = std::move(caller_completion_callback)]() { cb(true, true); });
  }
  // Last, and nothing of the link is used after it: a replacement that connects at once retires
  // the link here.
  if (rotationDue(link) && !slot.retry_timer->enabled()) {
    connectNext(slot_index);
  }
  return true;
}

void TcpBenchmarkClientImpl::onData(Link& link, Envoy::Buffer::Instance& data) {
  if (!expect_echo_ || (link.state != State::Open && link.state != State::Retiring)) {
    data.drain(data.length());
    return;
  }
  // A stream of echoes, each exactly the message, possibly split or coalesced by TCP.
  link.received.move(data);
  while (link.received.length() >= message_.size() &&
         (link.state == State::Open || link.state == State::Retiring)) {
    std::string bytes(message_.size(), '\0');
    link.received.copyOut(0, message_.size(), bytes.data());
    link.received.drain(message_.size());
    onEcho(link, bytes);
  }
}

void TcpBenchmarkClientImpl::onEcho(Link& link, const std::string& bytes) {
  // In order on one connection: the echo is for the oldest outstanding message.
  if (bytes != message_ || link.inflight.empty()) {
    onEchoMismatch(link);
    return;
  }
  InflightMessage message = std::move(link.inflight.front());
  link.inflight.pop_front();
  counters_.tcp_messages_received_.inc();
  if (measure_latencies_) {
    message_latency_statistic_->addValue(
        (api_.timeSource().monotonicTime() - message.sent_at).count());
  }
  message.completion_callback(true, true);
  if (link.state == State::Retiring && link.inflight.empty()) {
    // That was the last echo a rotated connection waited for.
    closeLink(link, Envoy::Network::ConnectionCloseType::FlushWrite);
  }
  maybeExitWaitLoop();
}

void TcpBenchmarkClientImpl::onEchoMismatch(Link& link) {
  // What came back is not what an exact echo sends, so from here on nothing says where one
  // reply ends and the next begins on this connection: a later message's worth of bytes that
  // happens to equal the message would be matched to the wrong send, and timed. Nothing more is
  // matched on it. It is closed, which loses what it had outstanding without timing any of it,
  // and reopened like any closed connection, so that an echo that only went wrong once is
  // measured cleanly again -- and one that is never exact gets a connection a second, backed
  // off, instead of a full window of unanswerable messages.
  counters_.tcp_echo_mismatch_.inc();
  if (!echo_mismatch_logged_) {
    echo_mismatch_logged_ = true;
    ENVOY_LOG(
        error,
        "TCP echo mismatch: a connection returned bytes that are not the {}-byte message sent "
        "on it, so the target is not an exact echo -- it prefixes, frames or rewrites its "
        "reply, or sends something of its own. The connection was closed, to be reopened; its "
        "{} unanswered message(s) are lost (benchmark.tcp_inflight_lost) and none of them is "
        "timed. benchmark_tcp.message_latency only holds echoes that were exact up to that "
        "point on their connection; if the target is not meant to echo, send without expecting "
        "one (--tcp-no-echo, tcp.expect_echo false). Further mismatches are only counted, in "
        "benchmark.tcp_echo_mismatch.",
        message_.size(), link.inflight.size());
  }
  const uint32_t slot_index = link.slot;
  const bool active = slots_[slot_index].active.get() == &link;
  closeLink(link, Envoy::Network::ConnectionCloseType::NoFlush);
  if (active) {
    scheduleRetry(slot_index);
  }
  maybeExitWaitLoop();
}

void TcpBenchmarkClientImpl::onWriteBlocked(Link& link, bool blocked) {
  if (blocked && !link.write_blocked) {
    counters_.tcp_write_blocked_.inc();
  }
  link.write_blocked = blocked;
}

void TcpBenchmarkClientImpl::closeLink(Link& link, Envoy::Network::ConnectionCloseType type) {
  if (link.state == State::Closed) {
    return;
  }
  link.state = State::Closed;
  link.timer->disableTimer();
  initialAttemptDone(link);
  completeInflight(link, /*success=*/false);
  // The close event, now or when the flush is done, finds the link Closed and lets go of it.
  link.connection->close(type);
}

TcpBenchmarkClientImpl::Role TcpBenchmarkClientImpl::release(Link& link) {
  Slot& slot = slots_[link.slot];
  std::unique_ptr<Link> owned;
  Role role = Role::None;
  if (slot.active.get() == &link) {
    owned = std::move(slot.active);
    role = Role::Active;
  } else if (slot.standby.get() == &link) {
    owned = std::move(slot.standby);
    role = Role::Standby;
  } else {
    auto it =
        std::find_if(slot.retiring.begin(), slot.retiring.end(),
                     [&link](const std::unique_ptr<Link>& other) { return other.get() == &link; });
    if (it != slot.retiring.end()) {
      owned = std::move(*it);
      slot.retiring.erase(it);
      role = Role::Retiring;
    }
  }
  if (owned != nullptr) {
    // Not deleted here: this runs inside the callbacks of the connection the link owns.
    dispatcher_.deferredDelete(std::move(owned));
  }
  return role;
}

void TcpBenchmarkClientImpl::completeInflight(Link& link, bool success) {
  while (!link.inflight.empty()) {
    InflightMessage message = std::move(link.inflight.front());
    link.inflight.pop_front();
    if (!success) {
      counters_.tcp_inflight_lost_.inc();
    }
    message.completion_callback(true, success);
  }
}

std::vector<TcpBenchmarkClientImpl::Link*> TcpBenchmarkClientImpl::links() const {
  std::vector<Link*> all;
  for (const Slot& slot : slots_) {
    if (slot.active != nullptr) {
      all.push_back(slot.active.get());
    }
    if (slot.standby != nullptr) {
      all.push_back(slot.standby.get());
    }
    for (const std::unique_ptr<Link>& link : slot.retiring) {
      all.push_back(link.get());
    }
  }
  return all;
}

bool TcpBenchmarkClientImpl::anyInflight() const {
  for (const Link* link : links()) {
    if (!link->inflight.empty()) {
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
  for (const Slot& slot : slots_) {
    if (slot.active != nullptr && slot.active->state == State::Open) {
      open++;
    }
  }
  return open;
}

void TcpBenchmarkClientImpl::finish() {
  if (finished_) {
    return;
  }
  // From here on nothing is opened: retries are off, and a connect still under way is dropped,
  // which is not a failure of the target's.
  finished_ = true;
  for (Slot& slot : slots_) {
    slot.retry_timer->disableTimer();
  }
  for (Link* link : links()) {
    if (link->state == State::Connecting) {
      closeLink(*link, Envoy::Network::ConnectionCloseType::NoFlush);
    }
  }
  if (anyInflight()) {
    waiting_for_ = WaitingFor::Echoes;
    drain_timer_ = dispatcher_.createTimer([this]() { dispatcher_.exit(); });
    drain_timer_->enableTimer(std::chrono::ceil<std::chrono::milliseconds>(drain_duration_));
    dispatcher_.run(Envoy::Event::Dispatcher::RunType::RunUntilExit);
    drain_timer_.reset();
    waiting_for_ = WaitingFor::Nothing;
  }
  // Echoes the drain window did not bring are lost; account for them before the worker
  // snapshots its counters, then close with what is written flushed.
  uint32_t incomplete = 0;
  for (Link* link : links()) {
    if (!link->inflight.empty()) {
      incomplete++;
      counters_.tcp_drain_incomplete_.inc();
    }
    closeLink(*link, Envoy::Network::ConnectionCloseType::FlushWrite);
  }
  if (incomplete > 0) {
    ENVOY_LOG(info,
              "{} TCP connection(s) still had unanswered messages after the {} ms drain window "
              "(counted in benchmark.tcp_drain_incomplete).",
              incomplete,
              std::chrono::duration_cast<std::chrono::milliseconds>(drain_duration_).count());
  }
}

void TcpBenchmarkClientImpl::terminate() {
  finish();
  setShouldMeasureLatencies(false);
  // finish() closed with FlushWrite; a flush that has not completed is cut short here. These
  // are the links whose close event has not come, so Closed already, whatever the socket says.
  for (Link* link : links()) {
    link->connection->close(Envoy::Network::ConnectionCloseType::NoFlush);
  }
}

StatisticPtrMap TcpBenchmarkClientImpl::statistics() const {
  StatisticPtrMap statistics;
  statistics[message_latency_statistic_->id()] = message_latency_statistic_.get();
  statistics[connect_latency_statistic_->id()] = connect_latency_statistic_.get();
  return statistics;
}

} // namespace Client
} // namespace Nighthawk
