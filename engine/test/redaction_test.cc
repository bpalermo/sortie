#include "envoy/extensions/transport_sockets/quic/v3/quic_transport.pb.h"

#include "engine/source/client/redaction.h"

#include "absl/strings/str_cat.h"
#include "gtest/gtest.h"

namespace Nighthawk {
namespace Client {
namespace {

envoy::extensions::transport_sockets::tls::v3::UpstreamTlsContext contextWithPair() {
  envoy::extensions::transport_sockets::tls::v3::UpstreamTlsContext context;
  auto* certificate = context.mutable_common_tls_context()->add_tls_certificates();
  certificate->mutable_certificate_chain()->set_inline_bytes("CERT");
  certificate->mutable_private_key()->set_inline_bytes("SECRET");
  context.mutable_common_tls_context()
      ->mutable_validation_context()
      ->mutable_trusted_ca()
      ->set_inline_bytes("CA");
  return context;
}

// The key goes, its size stays, and nothing else moves.
TEST(RedactionTest, RedactsTheKeyAndOnlyTheKey) {
  auto context = contextWithPair();
  redactPrivateKeys(context);
  const auto& certificate = context.common_tls_context().tls_certificates(0);
  EXPECT_EQ("<redacted: 6 bytes>", certificate.private_key().inline_string());
  EXPECT_TRUE(certificate.private_key().inline_bytes().empty());
  EXPECT_EQ("CERT", certificate.certificate_chain().inline_bytes());
  EXPECT_EQ("CA", context.common_tls_context().validation_context().trusted_ca().inline_bytes());
}

// A key given as inline_string -- the form --tls-context's JSON takes -- is redacted the same.
TEST(RedactionTest, RedactsAnInlineStringKeyToo) {
  auto context = contextWithPair();
  context.mutable_common_tls_context()->mutable_tls_certificates(0)->mutable_private_key()->set_inline_string("STRINGSECRET");
  redactPrivateKeys(context);
  const auto& key = context.common_tls_context().tls_certificates(0).private_key();
  EXPECT_EQ("<redacted: 12 bytes>", key.inline_string());
  // A key read from a file names the file, which is not a secret.
  context.mutable_common_tls_context()->mutable_tls_certificates(0)->mutable_private_key()->set_filename("/etc/key.pem");
  redactPrivateKeys(context);
  EXPECT_EQ("/etc/key.pem", context.common_tls_context().tls_certificates(0).private_key().filename());
}

// What the service logs on reading a request carries no key; the request itself is untouched.
TEST(RedactionTest, ExecutionRequestLogCopyCarriesNoKey) {
  nighthawk::client::ExecutionRequest request;
  *request.mutable_start_request()->mutable_options()->mutable_tls_context() = contextWithPair();
  const nighthawk::client::ExecutionRequest logged = redactedForLog(request);
  const std::string text = absl::StrCat(logged);
  EXPECT_EQ(std::string::npos, text.find("SECRET")) << text;
  EXPECT_NE(std::string::npos, text.find("redacted: 6 bytes")) << text;
  EXPECT_EQ("SECRET", request.start_request()
                          .options()
                          .tls_context()
                          .common_tls_context()
                          .tls_certificates(0)
                          .private_key()
                          .inline_bytes());
}

// The output echoes the options, key included; the response's log copy does not.
TEST(RedactionTest, ExecutionResponseLogCopyCarriesNoKey) {
  nighthawk::client::ExecutionResponse response;
  *response.mutable_output()->mutable_options()->mutable_tls_context() = contextWithPair();
  const std::string text = absl::StrCat(redactedForLog(response));
  EXPECT_EQ(std::string::npos, text.find("SECRET")) << text;
  EXPECT_NE(std::string::npos, text.find("redacted: 6 bytes")) << text;
  EXPECT_NE(std::string::npos, absl::StrCat(response).find("SECRET"));
}

// The bootstrap packs the context into each cluster's transport socket, as TLS or as QUIC
// wrapping TLS; both are redacted in the log copy.
TEST(RedactionTest, BootstrapLogCopyCarriesNoKeyForTlsOrQuicTransports) {
  envoy::config::bootstrap::v3::Bootstrap bootstrap;
  auto* tls_cluster = bootstrap.mutable_static_resources()->add_clusters();
  tls_cluster->mutable_transport_socket()->set_name("envoy.transport_sockets.tls");
  std::ignore =
      tls_cluster->mutable_transport_socket()->mutable_typed_config()->PackFrom(contextWithPair());
  auto* quic_cluster = bootstrap.mutable_static_resources()->add_clusters();
  envoy::extensions::transport_sockets::quic::v3::QuicUpstreamTransport quic;
  *quic.mutable_upstream_tls_context() = contextWithPair();
  quic_cluster->mutable_transport_socket()->set_name("envoy.transport_sockets.quic");
  std::ignore = quic_cluster->mutable_transport_socket()->mutable_typed_config()->PackFrom(quic);
  bootstrap.mutable_static_resources()->add_clusters(); // no transport socket at all

  const std::string text = redactedForLog(bootstrap).DebugString();
  EXPECT_EQ(std::string::npos, text.find("SECRET")) << text;
  size_t redactions = 0;
  for (size_t pos = text.find("redacted: 6 bytes"); pos != std::string::npos;
       pos = text.find("redacted: 6 bytes", pos + 1)) {
    redactions++;
  }
  EXPECT_EQ(2, redactions);
  // The original still has both keys: it is what configures Envoy.
  EXPECT_NE(std::string::npos, bootstrap.DebugString().find("SECRET"));
}

} // namespace
} // namespace Client
} // namespace Nighthawk
