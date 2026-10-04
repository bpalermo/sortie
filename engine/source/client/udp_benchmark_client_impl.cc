#include "engine/source/client/udp_benchmark_client_impl.h"

#include <utility>

#include "envoy/upstream/load_balancer.h"

#include "source/common/buffer/buffer_impl.h"
#include "source/common/network/socket_impl.h"

#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"

namespace Nighthawk {
namespace Client {

using namespace std::chrono_literals;

namespace {
constexpr size_t kSequencePrefixLength = 16;
// Larger than any datagram a sane path delivers; what does not fit is truncated by the kernel and
// then does not match, which is the right outcome for it.
constexpr size_t kReadBufferSize = 65536;
} // namespace

UdpBenchmarkClientImpl::UdpBenchmarkClientImpl(
    Envoy::Api::Api& api, Envoy::Event::Dispatcher& dispatcher, Envoy::Stats::Scope& scope,
    StatisticPtr&& message_latency_statistic, Envoy::Upstream::ClusterManagerPtr& cluster_manager,
    absl::string_view cluster_name, RequestGenerator request_generator, uint32_t max_inflight,
    std::chrono::nanoseconds timeout, std::chrono::nanoseconds drain_duration)
    : api_(api), dispatcher_(dispatcher), scope_(scope.createScope("benchmark.")),
      message_latency_statistic_(std::move(message_latency_statistic)),
      cluster_manager_(cluster_manager), cluster_name_(std::string(cluster_name)),
      request_generator_(std::move(request_generator)), max_inflight_(max_inflight),
      timeout_(timeout), drain_duration_(drain_duration),
      counters_({ALL_UDP_COUNTERS(POOL_COUNTER(*scope_))}) {
  RELEASE_ASSERT(max_inflight_ > 0, "max_inflight must be positive");
  RELEASE_ASSERT(timeout_ > 0ns, "timeout must be positive");
  message_latency_statistic_->setId("benchmark_udp.message_latency");
}

UdpBenchmarkClientImpl::~UdpBenchmarkClientImpl() = default;

std::string UdpBenchmarkClientImpl::sequencePrefix(uint64_t sequence) {
  return absl::StrFormat("%016x", sequence);
}

void UdpBenchmarkClientImpl::prepare() {
  RequestPtr request = request_generator_();
  RELEASE_ASSERT(request != nullptr, "the request source did not yield a request");
  message_ = request->body();

  const auto thread_local_cluster = cluster_manager_->getThreadLocalCluster(cluster_name_);
  Envoy::Upstream::HostConstSharedPtr host =
      thread_local_cluster == nullptr
          ? nullptr
          : Envoy::Upstream::LoadBalancer::onlyAllowSynchronousHostSelection(
                thread_local_cluster->chooseHost(nullptr));
  if (host == nullptr || host->address() == nullptr) {
    ENVOY_LOG(error, "No target host for UDP in cluster {}.", cluster_name_);
    counters_.udp_unavailable_.inc();
    return;
  }
  const Envoy::Network::Address::InstanceConstSharedPtr peer = host->address();
  // Connected, so sends and reads need no addresses and only the target's datagrams come back.
  auto socket = std::make_unique<Envoy::Network::SocketImpl>(
      Envoy::Network::Socket::Type::Datagram, peer, nullptr,
      Envoy::Network::SocketCreationOptions{});
  if (!socket->isOpen()) {
    ENVOY_LOG(error, "Could not open a UDP socket towards {}.", peer->asString());
    counters_.udp_unavailable_.inc();
    return;
  }
  const Envoy::Api::SysCallIntResult connected = socket->ioHandle().connect(peer);
  if (connected.return_value_ != 0) {
    ENVOY_LOG(error, "Could not connect a UDP socket to {}: errno {}.", peer->asString(),
              connected.errno_);
    counters_.udp_unavailable_.inc();
    return;
  }
  socket_ = std::move(socket);
  socket_->ioHandle().initializeFileEvent(
      dispatcher_,
      [this](uint32_t) {
        onReadable();
        return absl::OkStatus();
      },
      Envoy::Event::FileTriggerType::Edge, Envoy::Event::FileReadyType::Read);
  sweep_timer_ = dispatcher_.createTimer([this]() { sweep(); });
  sweep_timer_->enableTimer(kSweepInterval);
}

bool UdpBenchmarkClientImpl::tryStartRequest(CompletionCallback caller_completion_callback) {
  // Open-loop contract: this always "starts" the scheduled datagram. One that cannot be sent
  // right now is dropped (never queued or retried) and completes immediately.
  if (socket_ == nullptr || draining_) {
    counters_.udp_unavailable_.inc();
    dispatcher_.post([cb = std::move(caller_completion_callback)]() { cb(true, false); });
    return true;
  }
  if (inflight_.size() >= max_inflight_) {
    counters_.udp_deferred_.inc();
    dispatcher_.post([cb = std::move(caller_completion_callback)]() { cb(true, false); });
    return true;
  }
  const uint64_t sequence = next_sequence_++;
  const std::string datagram = absl::StrCat(sequencePrefix(sequence), message_);
  Envoy::Buffer::RawSlice slice{const_cast<char*>(datagram.data()), datagram.size()};
  const Envoy::Api::IoCallUint64Result result = socket_->ioHandle().writev(&slice, 1);
  if (!result.ok()) {
    counters_.udp_send_errors_.inc();
    dispatcher_.post([cb = std::move(caller_completion_callback)]() { cb(true, false); });
    return true;
  }
  counters_.udp_datagrams_sent_.inc();
  inflight_.emplace(sequence, InflightDatagram{api_.timeSource().monotonicTime(),
                                               std::move(caller_completion_callback)});
  return true;
}

void UdpBenchmarkClientImpl::onReadable() {
  // Edge-triggered: drain everything that is there.
  std::string buffer(kReadBufferSize, '\0');
  while (socket_ != nullptr) {
    Envoy::Buffer::RawSlice slice{buffer.data(), buffer.size()};
    const Envoy::Api::IoCallUint64Result result =
        socket_->ioHandle().readv(buffer.size(), &slice, 1);
    if (!result.ok() || result.return_value_ == 0) {
      // EAGAIN, or an error the next send will report.
      return;
    }
    onDatagram(absl::string_view(buffer.data(), result.return_value_));
  }
}

void UdpBenchmarkClientImpl::onDatagram(absl::string_view bytes) {
  uint64_t sequence = 0;
  if (bytes.size() < kSequencePrefixLength ||
      !absl::SimpleHexAtoi(bytes.substr(0, kSequencePrefixLength), &sequence) ||
      bytes.substr(kSequencePrefixLength) != message_) {
    counters_.udp_unexpected_.inc();
    return;
  }
  auto it = inflight_.find(sequence);
  if (it == inflight_.end()) {
    // Answered already (a duplicate), given up on (late), or never sent.
    counters_.udp_unexpected_.inc();
    return;
  }
  InflightDatagram datagram = std::move(it->second);
  inflight_.erase(it);
  counters_.udp_datagrams_received_.inc();
  if (measure_latencies_) {
    message_latency_statistic_->addValue(
        (api_.timeSource().monotonicTime() - datagram.sent_at).count());
  }
  datagram.completion_callback(true, true);
  if (draining_ && inflight_.empty()) {
    dispatcher_.exit();
  }
}

void UdpBenchmarkClientImpl::sweep() {
  // Sent in sequence order, so the first one still in time ends the sweep.
  const Envoy::MonotonicTime now = api_.timeSource().monotonicTime();
  while (!inflight_.empty() && now - inflight_.begin()->second.sent_at >= timeout_) {
    InflightDatagram datagram = std::move(inflight_.begin()->second);
    inflight_.erase(inflight_.begin());
    counters_.udp_lost_.inc();
    datagram.completion_callback(true, false);
  }
  if (draining_ && inflight_.empty()) {
    dispatcher_.exit();
    return;
  }
  if (sweep_timer_ != nullptr) {
    sweep_timer_->enableTimer(kSweepInterval);
  }
}

void UdpBenchmarkClientImpl::completeAll(bool success) {
  while (!inflight_.empty()) {
    InflightDatagram datagram = std::move(inflight_.begin()->second);
    inflight_.erase(inflight_.begin());
    if (!success) {
      counters_.udp_lost_.inc();
    }
    datagram.completion_callback(true, success);
  }
}

void UdpBenchmarkClientImpl::finish() {
  if (finished_) {
    return;
  }
  finished_ = true;
  draining_ = true;
  if (!inflight_.empty() && socket_ != nullptr) {
    // Echoes still in flight are worth the shorter of the drain window and the timeout; the
    // sweep keeps running and exits the loop when nothing is outstanding.
    drain_timer_ = dispatcher_.createTimer([this]() { dispatcher_.exit(); });
    drain_timer_->enableTimer(
        std::chrono::ceil<std::chrono::milliseconds>(std::min(drain_duration_, timeout_)));
    dispatcher_.run(Envoy::Event::Dispatcher::RunType::RunUntilExit);
    drain_timer_.reset();
  }
  // Whatever the window did not bring is lost.
  completeAll(/*success=*/false);
  sweep_timer_.reset();
}

void UdpBenchmarkClientImpl::terminate() {
  finish();
  setShouldMeasureLatencies(false);
  if (socket_ != nullptr) {
    socket_->ioHandle().resetFileEvents();
    socket_->close();
    socket_.reset();
  }
}

StatisticPtrMap UdpBenchmarkClientImpl::statistics() const {
  StatisticPtrMap statistics;
  statistics[message_latency_statistic_->id()] = message_latency_statistic_.get();
  return statistics;
}

} // namespace Client
} // namespace Nighthawk
