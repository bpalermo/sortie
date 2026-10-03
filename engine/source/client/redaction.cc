#include "engine/source/client/redaction.h"

#include "envoy/extensions/transport_sockets/quic/v3/quic_transport.pb.h"

#include "absl/strings/str_cat.h"

namespace Nighthawk {
namespace Client {

void redactPrivateKeys(envoy::extensions::transport_sockets::tls::v3::UpstreamTlsContext& context) {
  for (auto& certificate : *context.mutable_common_tls_context()->mutable_tls_certificates()) {
    if (!certificate.has_private_key()) {
      continue;
    }
    // Either inline form; a filename is not a secret.
    const auto& key = certificate.private_key();
    const size_t size =
        key.specifier_case() == envoy::config::core::v3::DataSource::SpecifierCase::kInlineBytes
            ? key.inline_bytes().size()
            : key.inline_string().size();
    if (size > 0) {
      certificate.mutable_private_key()->set_inline_string(
          absl::StrCat("<redacted: ", size, " bytes>"));
    }
  }
}

nighthawk::client::ExecutionRequest
redactedForLog(const nighthawk::client::ExecutionRequest& request) {
  nighthawk::client::ExecutionRequest copy = request;
  if (copy.has_start_request() && copy.start_request().has_options() &&
      copy.start_request().options().has_tls_context()) {
    redactPrivateKeys(*copy.mutable_start_request()->mutable_options()->mutable_tls_context());
  }
  return copy;
}

envoy::config::bootstrap::v3::Bootstrap
redactedForLog(const envoy::config::bootstrap::v3::Bootstrap& bootstrap) {
  envoy::config::bootstrap::v3::Bootstrap copy = bootstrap;
  for (auto& cluster : *copy.mutable_static_resources()->mutable_clusters()) {
    if (!cluster.has_transport_socket() || !cluster.transport_socket().has_typed_config()) {
      continue;
    }
    auto* typed_config = cluster.mutable_transport_socket()->mutable_typed_config();
    envoy::extensions::transport_sockets::tls::v3::UpstreamTlsContext tls;
    envoy::extensions::transport_sockets::quic::v3::QuicUpstreamTransport quic;
    if (typed_config->Is<envoy::extensions::transport_sockets::tls::v3::UpstreamTlsContext>() &&
        typed_config->UnpackTo(&tls)) {
      redactPrivateKeys(tls);
      std::ignore = typed_config->PackFrom(tls);
    } else if (typed_config
                   ->Is<envoy::extensions::transport_sockets::quic::v3::QuicUpstreamTransport>() &&
               typed_config->UnpackTo(&quic)) {
      redactPrivateKeys(*quic.mutable_upstream_tls_context());
      std::ignore = typed_config->PackFrom(quic);
    }
  }
  return copy;
}

} // namespace Client
} // namespace Nighthawk
