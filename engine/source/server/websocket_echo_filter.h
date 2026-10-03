#pragma once

#include "envoy/server/filter_config.h"

#include "engine/source/common/websocket.h"

namespace Nighthawk {
namespace Server {

/**
 * Answers a WebSocket upgrade itself -- 101, with the right Sec-WebSocket-Accept -- and then
 * echoes every data frame it receives, unmasked, as a server does; answers pings with pongs and a
 * close with a close. The target for WebSocket load, the way the test-server filter is the target
 * for HTTP load. Needs the connection manager to allow the upgrade (upgrade_configs:
 * [{upgrade_type: websocket}]); a request that is not an upgrade gets 426.
 */
class WebSocketEchoFilter : public Envoy::Http::StreamDecoderFilter {
public:
  // Http::StreamFilterBase
  void onDestroy() override {}

  // Http::StreamDecoderFilter
  Envoy::Http::FilterHeadersStatus decodeHeaders(Envoy::Http::RequestHeaderMap&, bool) override;
  Envoy::Http::FilterDataStatus decodeData(Envoy::Buffer::Instance&, bool) override;
  Envoy::Http::FilterTrailersStatus decodeTrailers(Envoy::Http::RequestTrailerMap&) override {
    return Envoy::Http::FilterTrailersStatus::Continue;
  }
  void setDecoderFilterCallbacks(Envoy::Http::StreamDecoderFilterCallbacks& callbacks) override {
    decoder_callbacks_ = &callbacks;
  }

private:
  void send(const WebSocket::Frame& frame, bool end_stream);

  Envoy::Http::StreamDecoderFilterCallbacks* decoder_callbacks_{nullptr};
  WebSocket::Decoder decoder_;
  bool upgraded_{false};
  bool closed_{false};
};

} // namespace Server
} // namespace Nighthawk
