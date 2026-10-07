#include <string>
#include <tuple>

#include "envoy/config/metrics/v3/metrics_service.pb.h"
#include "envoy/config/metrics/v3/stats.pb.h"

#include "engine/api/client/options.pb.h"
#include "engine/api/stats_sink/envoy_stats_sink_adapter.pb.h"

#include "engine/source/client/backend_name.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace Nighthawk {
namespace Client {
namespace {

using ::testing::HasSubstr;

constexpr char AdapterName[] = "nighthawk.envoy_stats_sink_adapter";

// Adds to options an adapter sink hosting the Envoy sink `name`, configured by `config`.
template <class ConfigType>
void addAdapted(nighthawk::client::CommandLineOptions& options, const std::string& name,
                const ConfigType& config) {
  nighthawk::EnvoyStatsSinkAdapterConfig adapter;
  adapter.mutable_sink()->set_name(name);
  std::ignore = adapter.mutable_sink()->mutable_typed_config()->PackFrom(config);
  auto* sink = options.add_stats_sinks();
  sink->set_name(AdapterName);
  std::ignore = sink->mutable_typed_config()->PackFrom(adapter);
}

// The prefix of the sink hosted by the adapter at `index`.
template <class ConfigType>
std::string adaptedPrefix(const nighthawk::client::CommandLineOptions& options, int index) {
  nighthawk::EnvoyStatsSinkAdapterConfig adapter;
  EXPECT_TRUE(options.stats_sinks(index).typed_config().UnpackTo(&adapter));
  ConfigType config;
  EXPECT_TRUE(adapter.sink().typed_config().UnpackTo(&config));
  return config.prefix();
}

envoy::config::metrics::v3::StatsdSink statsd(const std::string& prefix) {
  envoy::config::metrics::v3::StatsdSink sink;
  sink.set_prefix(prefix);
  auto* address = sink.mutable_address()->mutable_socket_address();
  address->set_address("127.0.0.1");
  address->set_port_value(8125);
  return sink;
}

envoy::config::metrics::v3::DogStatsdSink dogStatsd(const std::string& prefix) {
  envoy::config::metrics::v3::DogStatsdSink sink;
  sink.set_prefix(prefix);
  auto* address = sink.mutable_address()->mutable_socket_address();
  address->set_address("127.0.0.1");
  address->set_port_value(8125);
  return sink;
}

// The reduction has to agree with what the options' writer does to the components around
// the name (sortie's plan.StatsLabel, one segment), so these are that function's cases.
TEST(SanitizeBackendName, ReducesToOneMetricNameComponent) {
  EXPECT_EQ("node_a_example", sanitizeBackendName("Node-A.example"));
  EXPECT_EQ("worker_1", sanitizeBackendName("worker_1"));
  EXPECT_EQ("10_244_1_7", sanitizeBackendName("10.244.1.7"));
  // A run of other characters is one underscore, and none at either end.
  EXPECT_EQ("a_b", sanitizeBackendName("--a ./ b.."));
  // An underscore of the name's own is kept as written.
  EXPECT_EQ("_a__b_", sanitizeBackendName("_a__b_"));
  // A slash is a level separator to the writer; in a name it is just another character.
  EXPECT_EQ("zone_node", sanitizeBackendName("zone/node"));
  // Bytes outside ASCII are separators, never letters.
  EXPECT_EQ("n_de", sanitizeBackendName("n\xc3\xb6"
                                        "de"));
}

TEST(SanitizeBackendName, NothingSurvivesOfANameWithoutLettersOrDigits) {
  EXPECT_EQ("", sanitizeBackendName(""));
  EXPECT_EQ("", sanitizeBackendName("-.- /"));
  EXPECT_EQ("", sanitizeBackendName("%"));
}

// The token must be something sanitizing can never produce, or a backend could be named it.
TEST(SanitizeBackendName, TheTokenIsNotASanitizedName) {
  EXPECT_NE(std::string(BackendNameToken), sanitizeBackendName(BackendNameToken));
  EXPECT_EQ("%BACKEND%", BackendNameToken);
}

TEST(ExpandBackendName, ReplacesTheTokenInAStatsdPrefix) {
  nighthawk::client::CommandLineOptions options;
  addAdapted(options, "envoy.stat_sinks.statsd", statsd("sortie.soak.%BACKEND%"));
  ASSERT_TRUE(expandBackendName("node_a", options).ok());
  EXPECT_EQ("sortie.soak.node_a",
            adaptedPrefix<envoy::config::metrics::v3::StatsdSink>(options, 0));
  // Nothing but the prefix moved.
  nighthawk::EnvoyStatsSinkAdapterConfig adapter;
  ASSERT_TRUE(options.stats_sinks(0).typed_config().UnpackTo(&adapter));
  EXPECT_EQ("envoy.stat_sinks.statsd", adapter.sink().name());
  envoy::config::metrics::v3::StatsdSink sink;
  ASSERT_TRUE(adapter.sink().typed_config().UnpackTo(&sink));
  EXPECT_EQ(8125, sink.address().socket_address().port_value());
  EXPECT_EQ(AdapterName, options.stats_sinks(0).name());
}

TEST(ExpandBackendName, ReplacesTheTokenInADogStatsdPrefix) {
  nighthawk::client::CommandLineOptions options;
  addAdapted(options, "envoy.stat_sinks.dog_statsd", dogStatsd("%BACKEND%.soak"));
  ASSERT_TRUE(expandBackendName("node_a", options).ok());
  EXPECT_EQ("node_a.soak", adaptedPrefix<envoy::config::metrics::v3::DogStatsdSink>(options, 0));
}

TEST(ExpandBackendName, ReplacesEveryOccurrenceInEverySink) {
  nighthawk::client::CommandLineOptions options;
  addAdapted(options, "envoy.stat_sinks.statsd", statsd("%BACKEND%.x.%BACKEND%"));
  addAdapted(options, "envoy.stat_sinks.dog_statsd", dogStatsd("y.%BACKEND%"));
  ASSERT_TRUE(expandBackendName("n1", options).ok());
  EXPECT_EQ("n1.x.n1", adaptedPrefix<envoy::config::metrics::v3::StatsdSink>(options, 0));
  EXPECT_EQ("y.n1", adaptedPrefix<envoy::config::metrics::v3::DogStatsdSink>(options, 1));
}

// A request that does not use the token is byte for byte what it was, name or no name.
TEST(ExpandBackendName, LeavesAPrefixWithoutTheTokenUntouched) {
  nighthawk::client::CommandLineOptions options;
  addAdapted(options, "envoy.stat_sinks.statsd", statsd("sortie.soak.10_0_0_1"));
  addAdapted(options, "envoy.stat_sinks.dog_statsd", dogStatsd(""));
  const std::string before = options.SerializeAsString();
  ASSERT_TRUE(expandBackendName("node_a", options).ok());
  EXPECT_EQ(before, options.SerializeAsString());
  ASSERT_TRUE(expandBackendName("", options).ok());
  EXPECT_EQ(before, options.SerializeAsString());
}

// A sink that is not the adapter is not this function's to read, whatever its prefix says:
// here a statsd sink named directly, which the engine's own registry does not host.
TEST(ExpandBackendName, LeavesASinkThatIsNotTheAdapterUntouched) {
  nighthawk::client::CommandLineOptions options;
  auto* sink = options.add_stats_sinks();
  sink->set_name("envoy.stat_sinks.statsd");
  std::ignore = sink->mutable_typed_config()->PackFrom(statsd("sortie.%BACKEND%"));
  const std::string before = options.SerializeAsString();
  ASSERT_TRUE(expandBackendName("node_a", options).ok());
  EXPECT_EQ(before, options.SerializeAsString());
  // And it is no error without a name either.
  ASSERT_TRUE(expandBackendName("", options).ok());
  EXPECT_EQ(before, options.SerializeAsString());
}

// An adapter hosting some other Envoy sink has no prefix this function knows of.
TEST(ExpandBackendName, LeavesAnAdapterHostingAnotherSinkUntouched) {
  nighthawk::client::CommandLineOptions options;
  envoy::config::metrics::v3::MetricsServiceConfig other;
  other.mutable_grpc_service()->mutable_envoy_grpc()->set_cluster_name("%BACKEND%");
  addAdapted(options, "envoy.stat_sinks.metrics_service", other);
  const std::string before = options.SerializeAsString();
  ASSERT_TRUE(expandBackendName("", options).ok());
  EXPECT_EQ(before, options.SerializeAsString());
}

TEST(ExpandBackendName, TheTokenWithoutANameIsAnError) {
  nighthawk::client::CommandLineOptions options;
  addAdapted(options, "envoy.stat_sinks.statsd", statsd("sortie.soak.%BACKEND%"));
  const std::string before = options.SerializeAsString();
  const absl::Status status = expandBackendName("", options);
  EXPECT_EQ(absl::StatusCode::kInvalidArgument, status.code());
  EXPECT_THAT(std::string(status.message()), HasSubstr("--backend-name"));
  EXPECT_THAT(std::string(status.message()), HasSubstr("sortie.soak.%BACKEND%"));
  EXPECT_EQ(before, options.SerializeAsString());
}

TEST(ExpandBackendName, TheTokenInADogStatsdPrefixWithoutANameIsAnError) {
  nighthawk::client::CommandLineOptions options;
  addAdapted(options, "envoy.stat_sinks.dog_statsd", dogStatsd("%BACKEND%"));
  EXPECT_EQ(absl::StatusCode::kInvalidArgument, expandBackendName("", options).code());
}

} // namespace
} // namespace Client
} // namespace Nighthawk
