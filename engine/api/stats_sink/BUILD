load("@envoy//bazel:envoy_build_system.bzl", "envoy_package")
load("@envoy_api//bazel:api_build_system.bzl", "api_cc_py_proto_library")
load("@rules_go//proto:def.bzl", "go_proto_library")

licenses(["notice"])  # Apache 2

envoy_package()

api_cc_py_proto_library(
    name = "envoy_stats_sink_adapter_proto",
    srcs = [
        "envoy_stats_sink_adapter.proto",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@envoy_api//envoy/config/metrics/v3:pkg",
    ],
)

# Go bindings, for sortie; see //engine/api/client for the dependency rule.
go_proto_library(
    name = "envoy_stats_sink_adapter_go_proto",
    importpath = "github.com/bpalermo/sortie/engine/api/stats_sink",
    proto = ":envoy_stats_sink_adapter_proto",
    deps = ["@envoy_api//envoy/config/metrics/v3:pkg_go_proto"],
)
