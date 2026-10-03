load("@envoy//bazel:envoy_build_system.bzl", "envoy_package")
load("@envoy_api//bazel:api_build_system.bzl", "api_cc_py_proto_library")
load("@rules_go//proto:def.bzl", "go_proto_library")

licenses(["notice"])

envoy_package()

api_cc_py_proto_library(
    name = "rate_limiter_plugin",
    srcs = [
        "linear_ramping_rate_limiter.proto",
        "stub_rate_limiter.proto",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//engine/api/client:base",
    ],
)

# Go bindings, for sortie; see //engine/api/client for the dependency rule.
go_proto_library(
    name = "rate_limiter_plugin_go_proto",
    importpath = "github.com/bpalermo/sortie/engine/api/rate_limiter",
    proto = ":rate_limiter_plugin",
    deps = ["@protoc-gen-validate//validate:go_default_library"],
)
