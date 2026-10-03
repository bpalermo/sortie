load(
    "@envoy//bazel:envoy_build_system.bzl",
    "envoy_cc_test",
    "envoy_package",
)

licenses(["notice"])  # Apache 2

envoy_package()

envoy_cc_test(
    name = "nighthawk_distributor_client_test",
    srcs = ["nighthawk_distributor_client_test.cc"],
    deps = [
        "//engine/source/distributor:nighthawk_distributor_client_impl",
        "//engine/test/test_common:mock_stream",
        "//engine/test/test_common:proto_matchers",
    ],
)

envoy_cc_test(
    name = "distributor_service_test",
    srcs = ["distributor_service_test.cc"],
    deps = [
        "//engine/source/common:nighthawk_service_client_impl",
        "//engine/source/distributor:grpc_service_lib",
        "//engine/test/mocks/common:mock_nighthawk_service_client",
        "//engine/test/test_common:environment_lib",
        "@envoy//test/test_common:network_utility_lib",
    ],
)
