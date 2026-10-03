#include <string>

#include "envoy/registry/registry.h"

#include "engine/api/server/response_options.pb.h"

#include "engine/source/server/websocket_echo_filter.h"

namespace Nighthawk {
namespace Server {
namespace Configuration {

class WebSocketEchoFilterConfig
    : public Envoy::Server::Configuration::NamedHttpFilterConfigFactory {
public:
  absl::StatusOr<Envoy::Http::FilterFactoryCb>
  createFilterFactoryFromProto(const Envoy::Protobuf::Message&, const std::string&,
                               Envoy::Server::Configuration::FactoryContext&) override {
    return [](Envoy::Http::FilterChainFactoryCallbacks& callbacks) -> void {
      callbacks.addStreamDecoderFilter(std::make_shared<WebSocketEchoFilter>());
    };
  }

  Envoy::ProtobufTypes::MessagePtr createEmptyConfigProto() override {
    return std::make_unique<nighthawk::server::WebSocketEchoConfiguration>();
  }

  std::string name() const override { return "websocket-echo"; }
};

static Envoy::Registry::RegisterFactory<WebSocketEchoFilterConfig,
                                        Envoy::Server::Configuration::NamedHttpFilterConfigFactory>
    register_;

} // namespace Configuration
} // namespace Server
} // namespace Nighthawk
