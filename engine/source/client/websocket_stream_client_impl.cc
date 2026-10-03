#include "engine/source/client/websocket_stream_client_impl.h"

#include <utility>

#include "source/common/http/header_map_impl.h"
#include "source/common/http/headers.h"
#include "source/common/http/utility.h"

#include "absl/strings/escaping.h"
#include "absl/strings/str_split.h"
#include "absl/strings/match.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"

namespace Nighthawk {
namespace Client {

using namespace std::chrono_literals;

namespace {
const Envoy::Http::LowerCaseString& secWebSocketKey() {
  CONSTRUCT_ON_FIRST_USE(Envoy::Http::LowerCaseString, "sec-websocket-key");
}
const Envoy::Http::LowerCaseString& secWebSocketVersion() {
  CONSTRUCT_ON_FIRST_USE(Envoy::Http::LowerCaseString, "sec-websocket-version");
}
const Envoy::Http::LowerCaseString& secWebSocketAccept() {
  CONSTRUCT_ON_FIRST_USE(Envoy::Http::LowerCaseString, "sec-websocket-accept");
}
constexpr size_t kSequencePrefixLength = 16;
} // namespace

WebSocketStreamBenchmarkClientImpl::WebSocketStreamBenchmarkClientImpl(
    Envoy::Api::Api& api, Envoy::Event::Dispatcher& dispatcher, Envoy::Stats::Scope& scope,
    StatisticPtr&& message_latency_statistic, Envoy::Upstream::ClusterManagerPtr& cluster_manager,
    absl::string_view cluster_name, RequestGenerator request_generator, uint32_t streams,
    uint32_t max_inflight_per_stream, std::chrono::nanoseconds drain_duration,
    std::chrono::seconds open_timeout, bool binary)
    : api_(api), dispatcher_(dispatcher), scope_(scope.createScope("benchmark.")),
      message_latency_statistic_(std::move(message_latency_statistic)),
      cluster_manager_(cluster_manager), cluster_name_(std::string(cluster_name)),
      request_generator_(std::move(request_generator)), stream_count_(streams),
      max_inflight_per_stream_(max_inflight_per_stream), drain_duration_(drain_duration),
      open_timeout_(open_timeout), binary_(binary),
      counters_({ALL_WEBSOCKET_STREAM_COUNTERS(POOL_COUNTER(*scope_))}) {
  RELEASE_ASSERT(stream_count_ > 0, "at least one stream is required");
  RELEASE_ASSERT(max_inflight_per_stream_ > 0, "max_inflight_per_stream must be positive");
  message_latency_statistic_->setId("benchmark_stream.message_latency");
  streams_.resize(stream_count_);
  for (uint32_t i = 0; i < stream_count_; i++) {
    streams_[i].handler = std::make_unique<StreamHandler>(*this, i);
  }
}

WebSocketStreamBenchmarkClientImpl::~WebSocketStreamBenchmarkClientImpl() = default;

std::string WebSocketStreamBenchmarkClientImpl::sequencePrefix(uint64_t sequence) {
  return absl::StrFormat("%016x", sequence);
}

std::optional<Envoy::Upstream::HttpPoolData> WebSocketStreamBenchmarkClientImpl::pool() {
  const auto thread_local_cluster = cluster_manager_->getThreadLocalCluster(cluster_name_);
  Envoy::Upstream::HostConstSharedPtr host =
      Envoy::Upstream::LoadBalancer::onlyAllowSynchronousHostSelection(
          thread_local_cluster->chooseHost(nullptr));
  return thread_local_cluster->httpConnPool(host, Envoy::Upstream::ResourcePriority::Default,
                                            Envoy::Http::Protocol::Http11, nullptr);
}

void WebSocketStreamBenchmarkClientImpl::prepare() {
  RequestPtr request = request_generator_();
  RELEASE_ASSERT(request != nullptr, "the request source did not yield a request");
  request_headers_ = request->header();
  message_ = request->body();

  for (uint32_t i = 0; i < stream_count_; i++) {
    openStream(i);
  }
  if (pending_opens_ == 0) {
    return;
  }
  waiting_for_ = WaitingFor::Opens;
  wait_timer_ = dispatcher_.createTimer([this]() {
    ENVOY_LOG(warn, "Timed out waiting for {} of {} WebSocket upgrades.", pending_opens_,
              stream_count_);
    dispatcher_.exit();
  });
  wait_timer_->enableTimer(open_timeout_);
  dispatcher_.run(Envoy::Event::Dispatcher::RunType::RunUntilExit);
  wait_timer_.reset();
  waiting_for_ = WaitingFor::Nothing;
  // An upgrade still pending when the wait expired is an open failure, and the stream is
  // cancelled or reset: the run starts with what it has, and nothing joins it partway through
  // the measurement.
  for (uint32_t i = 0; i < stream_count_; i++) {
    Stream& stream = streams_[i];
    if (stream.state != StreamState::Opening) {
      continue;
    }
    counters_.stream_open_failures_.inc();
    pending_opens_--;
    Envoy::Http::RequestEncoder* encoder = stream.encoder;
    Envoy::Http::ConnectionPool::Cancellable* cancellable = stream.cancellable;
    stream.cancellable = nullptr;
    closeStream(i);
    if (encoder != nullptr) {
      encoder->getStream().resetStream(Envoy::Http::StreamResetReason::LocalReset);
    } else if (cancellable != nullptr) {
      cancellable->cancel(Envoy::ConnectionPool::CancelPolicy::Default);
    }
  }
  ENVOY_LOG(info, "Upgraded {} of {} WebSocket connections.", openStreams(), stream_count_);
}

void WebSocketStreamBenchmarkClientImpl::openStream(uint32_t index) {
  Stream& stream = streams_[index];
  std::optional<Envoy::Upstream::HttpPoolData> pool_data = pool();
  if (!pool_data.has_value()) {
    stream.state = StreamState::Closed;
    counters_.stream_open_failures_.inc();
    return;
  }
  stream.key = WebSocket::newKey();
  pending_opens_++;
  // The pool may invoke onPoolReady() synchronously, which is why pending_opens_ is bumped first.
  Envoy::Http::ConnectionPool::Cancellable* cancellable = pool_data.value().newStream(
      *stream.handler, *stream.handler, {/*can_send_early_data_=*/false, /*can_use_http3_=*/false});
  if (stream.state == StreamState::Opening && stream.encoder == nullptr) {
    stream.cancellable = cancellable;
  }
}

void WebSocketStreamBenchmarkClientImpl::onStreamReady(uint32_t index,
                                                       Envoy::Http::RequestEncoder& encoder) {
  Stream& stream = streams_[index];
  stream.cancellable = nullptr;
  stream.encoder = &encoder;
  encoder.getStream().addCallbacks(*stream.handler);
  // The generated request's path, authority and extra headers, with the upgrade on top. The
  // connection stays Opening until the 101 arrives.
  Envoy::Http::RequestHeaderMapPtr headers =
      Envoy::Http::createHeaderMap<Envoy::Http::RequestHeaderMapImpl>(*request_headers_);
  headers->setReferenceMethod(Envoy::Http::Headers::get().MethodValues.Get);
  headers->setConnection(Envoy::Http::Headers::get().ConnectionValues.Upgrade);
  headers->setUpgrade(Envoy::Http::Headers::get().UpgradeValues.WebSocket);
  headers->setCopy(secWebSocketVersion(), "13");
  headers->setCopy(secWebSocketKey(), stream.key);
  const Envoy::Http::Status status = encoder.encodeHeaders(*headers, /*end_stream=*/false);
  if (!status.ok()) {
    ENVOY_LOG(error, "Failed to encode WebSocket upgrade request headers: {}", status.message());
    counters_.stream_open_failures_.inc();
    pending_opens_--;
    closeStream(index);
  }
}

void WebSocketStreamBenchmarkClientImpl::onStreamOpenFailure(
    uint32_t index, Envoy::Http::ConnectionPool::PoolFailureReason reason,
    absl::string_view transport_failure_reason) {
  Stream& stream = streams_[index];
  ENVOY_LOG_EVERY_POW_2(error, "Failed to open WebSocket connection {}: reason {} ({})", index,
                        static_cast<int>(reason), transport_failure_reason);
  stream.cancellable = nullptr;
  stream.state = StreamState::Closed;
  counters_.stream_open_failures_.inc();
  pending_opens_--;
  maybeExitWaitLoop();
}

void WebSocketStreamBenchmarkClientImpl::onResponseHeaders(
    uint32_t index, Envoy::Http::ResponseHeaderMapPtr&& headers, bool end_stream) {
  Stream& stream = streams_[index];
  if (stream.state != StreamState::Opening) {
    return;
  }
  // RFC 6455 4.1: a 101 whose Upgrade is websocket, whose Connection names Upgrade, and whose
  // Sec-WebSocket-Accept matches the key. Anything short of that is not a WebSocket.
  const uint64_t response_code = Envoy::Http::Utility::getResponseStatus(*headers);
  const auto accept = headers->get(secWebSocketAccept());
  const bool upgrade_header_ok =
      headers->Upgrade() != nullptr &&
      absl::EqualsIgnoreCase(headers->Upgrade()->value().getStringView(),
                             Envoy::Http::Headers::get().UpgradeValues.WebSocket);
  bool connection_header_ok = false;
  if (headers->Connection() != nullptr) {
    for (absl::string_view token :
         absl::StrSplit(headers->Connection()->value().getStringView(), ',')) {
      if (absl::EqualsIgnoreCase(absl::StripAsciiWhitespace(token),
                                 Envoy::Http::Headers::get().ConnectionValues.Upgrade)) {
        connection_header_ok = true;
      }
    }
  }
  const bool accepted = response_code == 101 && upgrade_header_ok && connection_header_ok &&
                        accept.size() == 1 &&
                        accept[0]->value().getStringView() == WebSocket::acceptKey(stream.key);
  pending_opens_--;
  if (!accepted || end_stream) {
    ENVOY_LOG_EVERY_POW_2(warn, "WebSocket upgrade {} rejected: HTTP status {}", index,
                          response_code);
    counters_.stream_upgrade_rejected_.inc();
    Envoy::Http::RequestEncoder* encoder = stream.encoder;
    closeStream(index);
    if (encoder != nullptr && !end_stream) {
      encoder->getStream().resetStream(Envoy::Http::StreamResetReason::LocalReset);
    }
    return;
  }
  stream.state = StreamState::Open;
  counters_.streams_opened_.inc();
  maybeExitWaitLoop();
}

bool WebSocketStreamBenchmarkClientImpl::tryStartRequest(
    CompletionCallback caller_completion_callback) {
  // Open-loop contract: this always "starts" the scheduled message. A message that cannot be
  // sent right now is deferred (dropped, never queued or retried) and completes immediately.
  Stream& stream = streams_[next_stream_];
  next_stream_ = (next_stream_ + 1) % stream_count_;

  if (stream.state != StreamState::Open) {
    counters_.stream_unavailable_.inc();
    dispatcher_.post([cb = std::move(caller_completion_callback)]() { cb(true, false); });
    return true;
  }
  if (stream.write_blocked || stream.inflight.size() >= max_inflight_per_stream_) {
    counters_.stream_deferred_.inc();
    dispatcher_.post([cb = std::move(caller_completion_callback)]() { cb(true, false); });
    return true;
  }

  const uint64_t sequence = next_sequence_++;
  stream.inflight.push_back(
      {sequence, api_.timeSource().monotonicTime(), std::move(caller_completion_callback)});
  WebSocket::Frame frame;
  frame.opcode = binary_ ? WebSocket::Opcode::Binary : WebSocket::Opcode::Text;
  frame.payload = absl::StrCat(sequencePrefix(sequence), message_);
  send(stream, frame);
  counters_.stream_messages_sent_.inc();
  return true;
}

void WebSocketStreamBenchmarkClientImpl::send(Stream& stream, const WebSocket::Frame& frame) {
  Envoy::Buffer::OwnedImpl buffer(WebSocket::encodeFrame(frame, /*mask=*/true));
  stream.encoder->encodeData(buffer, /*end_stream=*/false);
}

void WebSocketStreamBenchmarkClientImpl::onResponseData(uint32_t index,
                                                        Envoy::Buffer::Instance& data,
                                                        bool end_stream) {
  Stream& stream = streams_[index];
  if (stream.state == StreamState::Open || stream.state == StreamState::Closing) {
    stream.received.move(data);
    std::vector<WebSocket::Frame> frames;
    if (!stream.decoder.feed(stream.received, frames)) {
      ENVOY_LOG_EVERY_POW_2(error, "WebSocket connection {}: protocol error in a frame", index);
      counters_.stream_protocol_errors_.inc();
      Envoy::Http::RequestEncoder* encoder = stream.encoder;
      closeStream(index);
      if (encoder != nullptr) {
        encoder->getStream().resetStream(Envoy::Http::StreamResetReason::LocalReset);
      }
      return;
    }
    for (WebSocket::Frame& frame : frames) {
      switch (frame.opcode) {
      case WebSocket::Opcode::Text:
      case WebSocket::Opcode::Binary:
      case WebSocket::Opcode::Continuation:
        stream.fragments.append(frame.payload);
        if (frame.fin) {
          onMessage(index, stream.fragments);
          stream.fragments.clear();
        }
        break;
      case WebSocket::Opcode::Ping:
        if (stream.state == StreamState::Open) {
          frame.opcode = WebSocket::Opcode::Pong;
          send(stream, frame);
        }
        break;
      case WebSocket::Opcode::Pong:
        break;
      case WebSocket::Opcode::Close:
        if (stream.state == StreamState::Open) {
          // The server closes first: answer its Close, as the protocol asks.
          counters_.stream_early_close_.inc();
          send(stream, WebSocket::closeFrame(1000));
        }
        closeStream(index);
        return;
      }
    }
  } else {
    data.drain(data.length());
  }
  if (end_stream) {
    onResponseEnd(index);
  }
}

void WebSocketStreamBenchmarkClientImpl::onMessage(uint32_t index, const std::string& payload) {
  Stream& stream = streams_[index];
  uint64_t sequence = 0;
  if (payload.size() < kSequencePrefixLength ||
      !absl::SimpleHexAtoi(absl::string_view(payload).substr(0, kSequencePrefixLength),
                           &sequence)) {
    counters_.stream_unexpected_message_.inc();
    return;
  }
  // Echoes arrive in order on one connection, so this is the front of the queue in the normal
  // case; a search covers a server that drops or reorders.
  auto it = stream.inflight.begin();
  while (it != stream.inflight.end() && it->sequence != sequence) {
    ++it;
  }
  if (it == stream.inflight.end()) {
    counters_.stream_unexpected_message_.inc();
    return;
  }
  InflightMessage message = std::move(*it);
  stream.inflight.erase(it);
  counters_.stream_messages_received_.inc();
  if (measure_latencies_) {
    message_latency_statistic_->addValue(
        (api_.timeSource().monotonicTime() - message.sent_at).count());
  }
  message.completion_callback(true, true);
}

void WebSocketStreamBenchmarkClientImpl::onResponseEnd(uint32_t index) {
  Stream& stream = streams_[index];
  if (stream.state == StreamState::Open) {
    counters_.stream_early_close_.inc();
  }
  closeStream(index);
}

void WebSocketStreamBenchmarkClientImpl::onStreamReset(uint32_t index,
                                                       Envoy::Http::StreamResetReason reason,
                                                       absl::string_view transport_failure_reason) {
  Stream& stream = streams_[index];
  if (stream.state == StreamState::Closed) {
    return;
  }
  ENVOY_LOG_EVERY_POW_2(warn, "WebSocket connection {} reset: reason {} ({})", index,
                        static_cast<int>(reason), transport_failure_reason);
  counters_.stream_resets_.inc();
  if (stream.state == StreamState::Opening) {
    // A reset while opening counts as an open failure; the pool will not call us again.
    counters_.stream_open_failures_.inc();
    pending_opens_--;
  }
  closeStream(index);
}

void WebSocketStreamBenchmarkClientImpl::onWriteBlocked(uint32_t index, bool blocked) {
  Stream& stream = streams_[index];
  if (blocked && !stream.write_blocked) {
    counters_.stream_write_blocked_.inc();
  }
  stream.write_blocked = blocked;
}

void WebSocketStreamBenchmarkClientImpl::closeStream(uint32_t index) {
  Stream& stream = streams_[index];
  if (stream.state == StreamState::Closed) {
    return;
  }
  stream.state = StreamState::Closed;
  stream.encoder = nullptr;
  completeInflight(stream, /*success=*/false);
  maybeExitWaitLoop();
}

void WebSocketStreamBenchmarkClientImpl::completeInflight(Stream& stream, bool success) {
  while (!stream.inflight.empty()) {
    InflightMessage message = std::move(stream.inflight.front());
    stream.inflight.pop_front();
    if (!success) {
      counters_.stream_inflight_lost_.inc();
    }
    message.completion_callback(true, success);
  }
}

void WebSocketStreamBenchmarkClientImpl::maybeExitWaitLoop() {
  switch (waiting_for_) {
  case WaitingFor::Opens:
    if (pending_opens_ == 0) {
      dispatcher_.exit();
    }
    break;
  case WaitingFor::Closes:
    if (openStreams() == 0) {
      dispatcher_.exit();
    }
    break;
  case WaitingFor::Nothing:
    break;
  }
}

uint32_t WebSocketStreamBenchmarkClientImpl::openStreams() const {
  uint32_t open = 0;
  for (const Stream& stream : streams_) {
    if (stream.state == StreamState::Open || stream.state == StreamState::Closing) {
      open++;
    }
  }
  return open;
}

void WebSocketStreamBenchmarkClientImpl::finish() {
  if (finished_) {
    return;
  }
  finished_ = true;
  // Start the closing handshake on every open connection: the server echoes what is in flight,
  // then answers the Close and ends the response.
  for (Stream& stream : streams_) {
    if (stream.state == StreamState::Open && stream.encoder != nullptr) {
      send(stream, WebSocket::closeFrame(1000));
      stream.state = StreamState::Closing;
    }
  }
  if (openStreams() == 0) {
    return;
  }
  waiting_for_ = WaitingFor::Closes;
  wait_timer_ = dispatcher_.createTimer([this]() { dispatcher_.exit(); });
  wait_timer_->enableTimer(
      std::chrono::ceil<std::chrono::milliseconds>(drain_duration_));
  dispatcher_.run(Envoy::Event::Dispatcher::RunType::RunUntilExit);
  wait_timer_.reset();
  waiting_for_ = WaitingFor::Nothing;
  // Connections the server did not close within the drain window: account for them now, before
  // the worker snapshots its counters. Their unanswered messages are lost; terminate() resets
  // them.
  uint32_t still_open = 0;
  for (Stream& stream : streams_) {
    if (stream.state == StreamState::Open || stream.state == StreamState::Closing) {
      still_open++;
      counters_.stream_drain_incomplete_.inc();
      completeInflight(stream, /*success=*/false);
    }
  }
  if (still_open > 0) {
    ENVOY_LOG(info,
              "{} WebSocket connection(s) still open after the {} ms drain window (counted in "
              "benchmark.stream_drain_incomplete).",
              still_open,
              std::chrono::duration_cast<std::chrono::milliseconds>(drain_duration_).count());
  }
}

void WebSocketStreamBenchmarkClientImpl::terminate() {
  finish();
  setShouldMeasureLatencies(false);
  for (uint32_t i = 0; i < stream_count_; i++) {
    Stream& stream = streams_[i];
    if (stream.state == StreamState::Opening && stream.encoder == nullptr &&
        stream.cancellable != nullptr) {
      stream.cancellable->cancel(Envoy::ConnectionPool::CancelPolicy::Default);
      stream.cancellable = nullptr;
      stream.state = StreamState::Closed;
      pending_opens_--;
    } else if (stream.state != StreamState::Closed) {
      Envoy::Http::RequestEncoder* encoder = stream.encoder;
      if (stream.state == StreamState::Opening) {
        pending_opens_--;
      }
      stream.state = StreamState::Closed;
      stream.encoder = nullptr;
      completeInflight(stream, /*success=*/false);
      if (encoder != nullptr) {
        encoder->getStream().resetStream(Envoy::Http::StreamResetReason::LocalReset);
      }
    }
  }
}

StatisticPtrMap WebSocketStreamBenchmarkClientImpl::statistics() const {
  StatisticPtrMap statistics;
  statistics[message_latency_statistic_->id()] = message_latency_statistic_.get();
  return statistics;
}

} // namespace Client
} // namespace Nighthawk
