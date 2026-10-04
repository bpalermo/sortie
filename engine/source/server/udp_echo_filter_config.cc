#include <string>

#include "envoy/registry/registry.h"
#include "envoy/server/filter_config.h"

#include "engine/api/server/response_options.pb.h"

#include "engine/source/server/udp_echo_filter.h"

namespace Nighthawk {
namespace Server {
namespace Configuration {

class UdpEchoFilterConfig
    : public Envoy::Server::Configuration::NamedUdpListenerFilterConfigFactory {
public:
  Envoy::Network::UdpListenerFilterFactoryCb
  createFilterFactoryFromProto(const Envoy::Protobuf::Message&,
                               Envoy::Server::Configuration::ListenerFactoryContext&) override {
    return [](Envoy::Network::UdpListenerFilterManager& filter_manager,
              Envoy::Network::UdpReadFilterCallbacks& callbacks) -> void {
      filter_manager.addReadFilter(std::make_unique<UdpEchoFilter>(callbacks));
    };
  }

  Envoy::ProtobufTypes::MessagePtr createEmptyConfigProto() override {
    return std::make_unique<nighthawk::server::UdpEchoConfiguration>();
  }

  std::string name() const override { return "udp-echo"; }
};

static Envoy::Registry::RegisterFactory<
    UdpEchoFilterConfig, Envoy::Server::Configuration::NamedUdpListenerFilterConfigFactory>
    register_;

} // namespace Configuration
} // namespace Server
} // namespace Nighthawk
