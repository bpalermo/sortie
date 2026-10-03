load("@envoy//bazel:envoy_build_system.bzl", "envoy_package")
load("@envoy_api//bazel:api_build_system.bzl", "api_cc_py_proto_library")
load("@grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")
load("@rules_go//proto:def.bzl", "go_proto_library")

licenses(["notice"])  # Apache 2

envoy_package()

api_cc_py_proto_library(
    name = "base",
    srcs = [
        "options.proto",
        "output.proto",
        "service.proto",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@envoy_api//envoy/config/core/v3:pkg",
        "@envoy_api//envoy/config/metrics/v3:pkg",
        "@envoy_api//envoy/extensions/transport_sockets/tls/v3:pkg",
    ],
)

cc_grpc_library(
    name = "grpc_service_lib",
    srcs = [
        ":base",
    ],
    generate_mocks = True,
    grpc_only = True,
    proto_only = False,
    use_external = False,
    visibility = ["//visibility:public"],
    deps = [
        ":base_cc_proto",
    ],
)

# Go bindings, for sortie. Every dependency here must be the same target the
# rest of the Go build links for that import path: envoy types from envoy_api
# (not the go-control-plane module), google/rpc/status from the genproto module
# gRPC-Go links. A second target for the same import path fails the link with
# "multiple copies of package".
go_proto_library(
    name = "base_go_proto",
    compilers = [
        "@rules_go//proto:go_proto",
        "@rules_go//proto:go_grpc_v2",
    ],
    importpath = "github.com/bpalermo/sortie/engine/api/client",
    proto = ":base",
    deps = [
        "@envoy_api//envoy/config/core/v3:pkg_go_proto",
        "@envoy_api//envoy/config/metrics/v3:pkg_go_proto",
        "@envoy_api//envoy/extensions/transport_sockets/tls/v3:pkg_go_proto",
        "@org_golang_google_genproto_googleapis_rpc//status",
        "@protoc-gen-validate//validate:go_default_library",
    ],
)
