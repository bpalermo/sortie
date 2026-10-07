#pragma once

#include <string>

#include "engine/api/client/options.pb.h"

#include "absl/status/status.h"
#include "absl/strings/string_view.h"

namespace Nighthawk {
namespace Client {

/**
 * The placeholder a stats sink's prefix may carry for the name of the service that runs the
 * execution. Defined and documented in engine/api/stats_sink/envoy_stats_sink_adapter.proto,
 * which is the one place both sides of the API read; whoever writes the options writes this
 * exact string.
 */
inline constexpr absl::string_view BackendNameToken = "%BACKEND%";

/**
 * Reduces a backend name to one component of a metric name: lowercased, every run of
 * characters outside [a-z0-9_] replaced by a single underscore, trimmed at both ends.
 * `Node-A.example` is `node_a_example`. This is what the options' writer does to every other
 * component of the prefix, so a name reads the same as the labels around it and can never
 * add a level (a dot) to a metric name.
 *
 * @return the reduced name, empty when nothing of the name survives.
 */
std::string sanitizeBackendName(absl::string_view name);

/**
 * Replaces BackendNameToken with backend_name in the prefix of every statsd and dog_statsd
 * sink that the options host through the Envoy stats sink adapter. Every other sink, and a
 * prefix without the token, is left exactly as it was.
 *
 * @param backend_name the service's name, already sanitized; empty when it has none.
 * @param options the options of a start request, modified in place.
 * @return InvalidArgument when a prefix carries the token and the service has no name: the
 * token would otherwise go out as part of every metric name.
 */
absl::Status expandBackendName(absl::string_view backend_name,
                               nighthawk::client::CommandLineOptions& options);

} // namespace Client
} // namespace Nighthawk
