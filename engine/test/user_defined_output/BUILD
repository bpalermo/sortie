load(
    "@envoy//bazel:envoy_build_system.bzl",
    "envoy_cc_test",
    "envoy_package",
)

licenses(["notice"])  # Apache 2

envoy_package()

envoy_cc_test(
    name = "user_defined_output_plugin_creator_test",
    srcs = ["user_defined_output_plugin_creator_test.cc"],
    deps = [
        "//engine/api/user_defined_output:log_response_headers_proto_cc_proto",
        "//engine/include/nighthawk/user_defined_output:user_defined_output_plugin",
        "//engine/source/user_defined_output:log_response_headers_plugin",
        "//engine/source/user_defined_output:user_defined_output_plugin_creator",
        "//engine/test/user_defined_output/fake_plugin:fake_user_defined_output",
        "//engine/test/user_defined_output/fake_plugin:fake_user_defined_output_proto_cc_proto",
    ],
)

envoy_cc_test(
    name = "log_response_headers_plugin_test",
    srcs = ["log_response_headers_plugin_test.cc"],
    deps = [
        "//engine/api/user_defined_output:log_response_headers_proto_cc_proto",
        "//engine/include/nighthawk/user_defined_output:user_defined_output_plugin",
        "//engine/source/user_defined_output:log_response_headers_plugin",
        "//engine/test/test_common:proto_matchers",
        "@envoy//test/mocks/buffer:buffer_mocks",
    ],
)
