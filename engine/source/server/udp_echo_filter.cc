#include "engine/source/server/udp_echo_filter.h"

namespace Nighthawk {
namespace Server {

Envoy::Network::FilterStatus UdpEchoFilter::onData(Envoy::Network::UdpRecvData& data) {
  Envoy::Network::UdpSendData send{data.addresses_.local_->ip(), *data.addresses_.peer_,
                                   *data.buffer_};
  std::ignore = read_callbacks_->udpListener().send(send);
  return Envoy::Network::FilterStatus::StopIteration;
}

} // namespace Server
} // namespace Nighthawk
