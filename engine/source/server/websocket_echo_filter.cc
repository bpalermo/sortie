#include "engine/source/server/websocket_echo_filter.h"

#include "source/common/buffer/buffer_impl.h"
#include "source/common/http/header_map_impl.h"
#include "source/common/http/headers.h"
#include "source/common/http/utility.h"

namespace Nighthawk {
namespace Server {

namespace {
const Envoy::Http::LowerCaseString& secWebSocketKey() {
  CONSTRUCT_ON_FIRST_USE(Envoy::Http::LowerCaseString, "sec-websocket-key");
}
const Envoy::Http::LowerCaseString& secWebSocketAccept() {
  CONSTRUCT_ON_FIRST_USE(Envoy::Http::LowerCaseString, "sec-websocket-accept");
}
} // namespace

Envoy::Http::FilterHeadersStatus
WebSocketEchoFilter::decodeHeaders(Envoy::Http::RequestHeaderMap& headers, bool) {
  if (!Envoy::Http::Utility::isWebSocketUpgradeRequest(headers)) {
    decoder_callbacks_->sendLocalReply(Envoy::Http::Code::UpgradeRequired,
                                       "websocket-echo: expected a WebSocket upgrade\n", nullptr,
                                       std::nullopt, "websocket_echo_not_an_upgrade");
    return Envoy::Http::FilterHeadersStatus::StopIteration;
  }
  const auto key = headers.get(secWebSocketKey());
  if (key.size() != 1) {
    decoder_callbacks_->sendLocalReply(Envoy::Http::Code::BadRequest,
                                       "websocket-echo: expected one Sec-WebSocket-Key\n", nullptr,
                                       std::nullopt, "websocket_echo_bad_key");
    return Envoy::Http::FilterHeadersStatus::StopIteration;
  }
  auto response = Envoy::Http::ResponseHeaderMapImpl::create();
  response->setStatus(static_cast<uint64_t>(Envoy::Http::Code::SwitchingProtocols));
  response->setConnection(Envoy::Http::Headers::get().ConnectionValues.Upgrade);
  response->setUpgrade(Envoy::Http::Headers::get().UpgradeValues.WebSocket);
  response->addCopy(secWebSocketAccept(),
                    WebSocket::acceptKey(key[0]->value().getStringView()));
  upgraded_ = true;
  decoder_callbacks_->encodeHeaders(std::move(response), /*end_stream=*/false,
                                    "websocket_echo_upgrade");
  return Envoy::Http::FilterHeadersStatus::StopIteration;
}

Envoy::Http::FilterDataStatus WebSocketEchoFilter::decodeData(Envoy::Buffer::Instance& data,
                                                              bool end_stream) {
  if (!upgraded_ || closed_) {
    data.drain(data.length());
    return Envoy::Http::FilterDataStatus::StopIterationNoBuffer;
  }
  std::vector<WebSocket::Frame> frames;
  if (!decoder_.feed(data, frames)) {
    // 1002: protocol error.
    send(WebSocket::closeFrame(1002), /*end_stream=*/true);
    return Envoy::Http::FilterDataStatus::StopIterationNoBuffer;
  }
  for (WebSocket::Frame& frame : frames) {
    switch (frame.opcode) {
    case WebSocket::Opcode::Text:
    case WebSocket::Opcode::Binary:
    case WebSocket::Opcode::Continuation:
      send(frame, /*end_stream=*/false);
      break;
    case WebSocket::Opcode::Ping:
      frame.opcode = WebSocket::Opcode::Pong;
      send(frame, /*end_stream=*/false);
      break;
    case WebSocket::Opcode::Pong:
      break;
    case WebSocket::Opcode::Close:
      // Echo the close and end the response, which ends the upgraded connection.
      send(frame, /*end_stream=*/true);
      return Envoy::Http::FilterDataStatus::StopIterationNoBuffer;
    }
  }
  if (end_stream && !closed_) {
    closed_ = true;
    Envoy::Buffer::OwnedImpl empty;
    decoder_callbacks_->encodeData(empty, /*end_stream=*/true);
  }
  return Envoy::Http::FilterDataStatus::StopIterationNoBuffer;
}

void WebSocketEchoFilter::send(const WebSocket::Frame& frame, bool end_stream) {
  if (closed_) {
    return;
  }
  Envoy::Buffer::OwnedImpl buffer(WebSocket::encodeFrame(frame, /*mask=*/false));
  closed_ = end_stream;
  decoder_callbacks_->encodeData(buffer, end_stream);
}

} // namespace Server
} // namespace Nighthawk
