#include "engine/source/client/backend_name.h"

#include <tuple>

#include "envoy/config/metrics/v3/stats.pb.h"

#include "source/common/protobuf/protobuf.h"

#include "engine/api/stats_sink/envoy_stats_sink_adapter.pb.h"

#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_replace.h"
#include "fmt/format.h"

namespace Nighthawk {
namespace Client {
namespace {

// Expands the token in the prefix of a sink configured by ConfigType, when config holds one.
// Returns whether config was rewritten.
template <class ConfigType>
absl::StatusOr<bool> expandIn(absl::string_view backend_name, Envoy::Protobuf::Any& config) {
  if (!config.Is<ConfigType>()) {
    return false;
  }
  ConfigType sink;
  // A configuration that does not parse is the sink factory's to report, with the sink's
  // name beside it.
  if (!config.UnpackTo(&sink) || !absl::StrContains(sink.prefix(), BackendNameToken)) {
    return false;
  }
  if (backend_name.empty()) {
    return absl::InvalidArgumentError(
        fmt::format("a stats sink's prefix ({}) names this backend with {}, and this service "
                    "has no name: start nighthawk_service with --backend-name",
                    sink.prefix(), BackendNameToken));
  }
  sink.set_prefix(absl::StrReplaceAll(sink.prefix(), {{BackendNameToken, backend_name}}));
  std::ignore = config.PackFrom(sink);
  return true;
}

} // namespace

std::string sanitizeBackendName(absl::string_view name) {
  std::string out;
  bool pending = false;
  for (const char c : name) {
    const char lower = absl::ascii_tolower(static_cast<unsigned char>(c));
    if ((lower >= 'a' && lower <= 'z') || (lower >= '0' && lower <= '9') || lower == '_') {
      // A run of other characters becomes one underscore, and only between two kept
      // characters: that is the trimming at both ends.
      if (pending && !out.empty()) {
        out.push_back('_');
      }
      pending = false;
      out.push_back(lower);
    } else {
      pending = true;
    }
  }
  return out;
}

absl::Status expandBackendName(absl::string_view backend_name,
                               nighthawk::client::CommandLineOptions& options) {
  for (envoy::config::metrics::v3::StatsSink& outer : *options.mutable_stats_sinks()) {
    // Judged by the type of the configuration, which is what decides the factory when the
    // name matches none.
    if (!outer.typed_config().Is<nighthawk::EnvoyStatsSinkAdapterConfig>()) {
      continue;
    }
    nighthawk::EnvoyStatsSinkAdapterConfig adapter;
    if (!outer.typed_config().UnpackTo(&adapter) || !adapter.sink().has_typed_config()) {
      continue;
    }
    Envoy::Protobuf::Any& inner = *adapter.mutable_sink()->mutable_typed_config();
    absl::StatusOr<bool> rewritten =
        expandIn<envoy::config::metrics::v3::StatsdSink>(backend_name, inner);
    if (rewritten.ok() && !*rewritten) {
      rewritten = expandIn<envoy::config::metrics::v3::DogStatsdSink>(backend_name, inner);
    }
    if (!rewritten.ok()) {
      return rewritten.status();
    }
    if (*rewritten) {
      std::ignore = outer.mutable_typed_config()->PackFrom(adapter);
    }
  }
  return absl::OkStatus();
}

} // namespace Client
} // namespace Nighthawk
