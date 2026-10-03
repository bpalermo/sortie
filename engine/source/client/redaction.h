#pragma once

#include "envoy/config/bootstrap/v3/bootstrap.pb.h"
#include "envoy/extensions/transport_sockets/tls/v3/tls.pb.h"

#include "engine/api/client/service.pb.h"

namespace Nighthawk {
namespace Client {

/**
 * Replaces each inline client private key in a TLS context with a note of its size, in place.
 * For anything that is shown or logged; never for what is sent.
 */
void redactPrivateKeys(envoy::extensions::transport_sockets::tls::v3::UpstreamTlsContext& context);

/**
 * @return a copy of the request fit for a log line: the options' tls_context has its client
 * private keys redacted.
 */
nighthawk::client::ExecutionRequest
redactedForLog(const nighthawk::client::ExecutionRequest& request);

/**
 * @return a copy of the bootstrap fit for a log line: the clusters' transport sockets (TLS, or
 * QUIC wrapping TLS), which carry the options' tls_context, have their client private keys
 * redacted.
 */
envoy::config::bootstrap::v3::Bootstrap
redactedForLog(const envoy::config::bootstrap::v3::Bootstrap& bootstrap);

} // namespace Client
} // namespace Nighthawk
