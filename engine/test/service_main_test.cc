#include <grpc++/grpc++.h>

#include "nighthawk/common/exception.h"

#include "engine/test/test_common/environment.h"
#include "test/test_common/network_utility.h"
#include "test/test_common/utility.h"

#include "engine/source/client/service_main.h"

#include "gtest/gtest.h"

using namespace testing;

// TODO(oschaaf): this gets us some coverage, but we need more functional testing.
// See if we can add some python integration tests.

namespace Nighthawk {
namespace Client {

class ServiceMainTest : public Test {};

TEST_F(ServiceMainTest, BadArgs) {
  std::vector<const char*> argv = {"foo", "bar"};
  EXPECT_THROW(ServiceMain(argv.size(), argv.data()), std::exception);
}

// A service that allows no execution at all would start, look healthy and refuse every run.
TEST_F(ServiceMainTest, ZeroConcurrentExecutionsIsRejected) {
  std::vector<const char*> argv = {"foo", "--max-concurrent-executions", "0"};
  EXPECT_THROW_WITH_REGEX(ServiceMain(argv.size(), argv.data()), MalformedArgvException,
                          "--max-concurrent-executions must be at least 1");
}

// TCLAP reads -1 into the unsigned option as 4294967295, which would be no cap at all.
TEST_F(ServiceMainTest, NegativeConcurrentExecutionsIsRejected) {
  std::vector<const char*> argv = {"foo", "--max-concurrent-executions", "-1"};
  EXPECT_THROW_WITH_REGEX(ServiceMain(argv.size(), argv.data()), MalformedArgvException,
                          "Invalid value for --max-concurrent-executions");
}

TEST_F(ServiceMainTest, ConcurrentExecutionsFlagIsAccepted) {
  std::vector<const char*> argv = {"foo", "--max-concurrent-executions", "4", "--listen",
                                   "127.0.0.1:0"};
  EXPECT_NO_THROW(ServiceMain(argv.size(), argv.data()));
}

TEST_F(ServiceMainTest, BackendNameIsAccepted) {
  std::vector<const char*> argv = {"foo", "--backend-name", "Node-A.example", "--listen",
                                   "127.0.0.1:0"};
  EXPECT_NO_THROW(ServiceMain(argv.size(), argv.data()));
}

// A name nothing survives of would be an empty component in every metric name. It is what
// an unset environment variable expands to, so it is refused at startup, where it is seen.
TEST_F(ServiceMainTest, BackendNameThatReducesToNothingIsRejected) {
  for (const char* name : {"", "-.-", "%"}) {
    std::vector<const char*> argv = {"foo", "--backend-name", name};
    EXPECT_THROW_WITH_REGEX(ServiceMain(argv.size(), argv.data()), MalformedArgvException,
                            "--backend-name needs at least one letter");
  }
}

TEST_F(ServiceMainTest, BadHost) {
  std::vector<const char*> argv = {"foo", "--listen", "b|-%ar"};
  ServiceMain service_main(argv.size(), argv.data());
  EXPECT_THROW(service_main.start(), NighthawkException);
}

TEST_F(ServiceMainTest, UnkownHost) {
  std::vector<const char*> argv = {"foo", "--listen", "bar"};
  ServiceMain service_main(argv.size(), argv.data());
  EXPECT_THROW(service_main.start(), NighthawkException);
}

TEST_F(ServiceMainTest, NoArgs) {
  std::vector<const char*> argv = {"foo"};
  ServiceMain service(argv.size(), argv.data());
  service.start();
  service.shutdown();
}

TEST_F(ServiceMainTest, Unbindable) {
  const std::string dest = fmt::format("unknownhost:10");
  std::vector<const char*> argv = {"foo", "--listen", dest.c_str()};
  ServiceMain service_main(argv.size(), argv.data());
  EXPECT_THROW(service_main.start(), NighthawkException);
}

class ServiceMainTestP : public TestWithParam<Envoy::Network::Address::IpVersion> {
public:
  ServiceMainTestP()
      : loopback_address_(Envoy::Network::Test::getLoopbackAddressUrlString(GetParam())) {}

  const std::string loopback_address_;
};

INSTANTIATE_TEST_SUITE_P(IpVersions, ServiceMainTestP,
                         ValuesIn(Envoy::TestEnvironment::getIpVersionsForTest()),
                         Envoy::TestUtility::ipTestParamsToString);

TEST_P(ServiceMainTestP, OnlyIp) {
  const std::string dest = fmt::format("{}", loopback_address_);
  std::vector<const char*> argv = {"foo", "--listen", dest.c_str()};
  ServiceMain service(argv.size(), argv.data());
  service.start();
  service.shutdown();
}

TEST_P(ServiceMainTestP, PortZero) {
  // We should be able to bind to port 0
  const std::string dest = fmt::format("{}:0", loopback_address_);
  std::vector<const char*> argv = {"foo", "--listen", dest.c_str()};
  ServiceMain service_main(argv.size(), argv.data());
  EXPECT_NO_THROW(service_main.start());
  service_main.shutdown();
}

} // namespace Client
} // namespace Nighthawk
