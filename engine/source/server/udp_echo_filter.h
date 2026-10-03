#pragma once

#include "envoy/network/filter.h"
#include "envoy/network/listener.h"

namespace Nighthawk {
namespace Server {

/**
 * A UDP listener filter that sends every datagram back to where it came from. The target for UDP
 * load, the way Envoy's echo network filter is the target for TCP load. Configure it as a
 * listener filter on a UDP listener (address.socket_address.protocol: UDP).
 */
class UdpEchoFilter : public Envoy::Network::UdpListenerReadFilter {
public:
  explicit UdpEchoFilter(Envoy::Network::UdpReadFilterCallbacks& callbacks)
      : UdpListenerReadFilter(callbacks) {}

  // Envoy::Network::UdpListenerReadFilter
  Envoy::Network::FilterStatus onData(Envoy::Network::UdpRecvData& data) override;
  Envoy::Network::FilterStatus onReceiveError(Envoy::Api::IoError::IoErrorCode) override {
    return Envoy::Network::FilterStatus::StopIteration;
  }
};

} // namespace Server
} // namespace Nighthawk
