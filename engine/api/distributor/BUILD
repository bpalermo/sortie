load("@envoy//bazel:envoy_build_system.bzl", "envoy_package")
load("@envoy_api//bazel:api_build_system.bzl", "api_cc_py_proto_library")
load("@grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")
load("@rules_go//proto:def.bzl", "go_proto_library")

licenses(["notice"])  # Apache 2

envoy_package()

api_cc_py_proto_library(
    name = "distributor",
    srcs = [
        "distributor.proto",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//engine/api/client:base",
        "@envoy_api//envoy/config/core/v3:pkg",
    ],
)

cc_grpc_library(
    name = "distributor_grpc_lib",
    srcs = [
        ":distributor",
    ],
    generate_mocks = True,
    grpc_only = True,
    proto_only = False,
    use_external = False,
    visibility = ["//visibility:public"],
    deps = [
        ":distributor_cc_proto",
    ],
)

# Go bindings, for sortie; see //engine/api/client for the dependency rule.
go_proto_library(
    name = "distributor_go_proto",
    compilers = [
        "@rules_go//proto:go_proto",
        "@rules_go//proto:go_grpc_v2",
    ],
    importpath = "github.com/bpalermo/sortie/engine/api/distributor",
    proto = ":distributor",
    deps = [
        "//engine/api/client:base_go_proto",
        "@envoy_api//envoy/config/core/v3:pkg_go_proto",
        "@org_golang_google_genproto_googleapis_rpc//status",
        "@protoc-gen-validate//validate:go_default_library",
    ],
)
