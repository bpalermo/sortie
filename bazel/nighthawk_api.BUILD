# Proto and Go targets for Nighthawk's public API protos.
#
# Nighthawk builds these with Envoy's api_proto_package macro, which needs its
# whole C++ tree. Only the protos are wanted here, so this declares them
# directly. Imports inside the protos are repo-root relative ("api/client/...")
# and the archive is stripped to the repo root, so no import prefix is needed.
#
# The protos carry no `option go_package`, so each go_proto_library sets the
# importpath explicitly. They keep Nighthawk's own repository path: nothing else
# publishes it, and it says where the messages come from.
#
# Every Go dependency below must be the same target the rest of the build
# already links for that importpath, or the linker refuses the binary with a
# "multiple copies of package" conflict. That is why google/rpc's Go code comes
# from the genproto module that grpc-go already pulls in, while its .proto comes
# from @googleapis, and why validate uses envoy_api's choice of target.

load("@com_google_protobuf//bazel:proto_library.bzl", "proto_library")
load("@rules_go//proto:def.bzl", "go_proto_library")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # Apache 2

proto_library(
    name = "client_proto",
    srcs = [
        "api/client/options.proto",
        "api/client/output.proto",
        "api/client/service.proto",
    ],
    deps = [
        "@com_envoyproxy_protoc_gen_validate//validate:validate_proto",
        "@com_google_protobuf//:any_proto",
        "@com_google_protobuf//:duration_proto",
        "@com_google_protobuf//:timestamp_proto",
        "@com_google_protobuf//:wrappers_proto",
        "@envoy_api//envoy/config/core/v3:pkg",
        "@envoy_api//envoy/config/metrics/v3:pkg",
        "@envoy_api//envoy/extensions/transport_sockets/tls/v3:pkg",
        "@googleapis//google/rpc:status_proto",
    ],
)

go_proto_library(
    name = "client_go_proto",
    compilers = [
        "@rules_go//proto:go_proto",
        "@rules_go//proto:go_grpc_v2",
    ],
    importpath = "github.com/envoyproxy/nighthawk/api/client",
    proto = ":client_proto",
    deps = [
        "@com_envoyproxy_protoc_gen_validate//validate:go_default_library",
        "@envoy_api//envoy/config/core/v3:pkg_go_proto",
        "@envoy_api//envoy/config/metrics/v3:pkg_go_proto",
        "@envoy_api//envoy/extensions/transport_sockets/tls/v3:pkg_go_proto",
        "@org_golang_google_genproto_googleapis_rpc//status",
    ],
)

proto_library(
    name = "distributor_proto",
    srcs = ["api/distributor/distributor.proto"],
    deps = [
        ":client_proto",
        "@com_envoyproxy_protoc_gen_validate//validate:validate_proto",
        "@envoy_api//envoy/config/core/v3:pkg",
        "@googleapis//google/rpc:status_proto",
    ],
)

go_proto_library(
    name = "distributor_go_proto",
    compilers = [
        "@rules_go//proto:go_proto",
        "@rules_go//proto:go_grpc_v2",
    ],
    importpath = "github.com/envoyproxy/nighthawk/api/distributor",
    proto = ":distributor_proto",
    deps = [
        ":client_go_proto",
        "@com_envoyproxy_protoc_gen_validate//validate:go_default_library",
        "@envoy_api//envoy/config/core/v3:pkg_go_proto",
        "@org_golang_google_genproto_googleapis_rpc//status",
    ],
)

proto_library(
    name = "rate_limiter_proto",
    srcs = ["api/rate_limiter/linear_ramping_rate_limiter.proto"],
    deps = [
        "@com_envoyproxy_protoc_gen_validate//validate:validate_proto",
        "@com_google_protobuf//:duration_proto",
    ],
)

go_proto_library(
    name = "rate_limiter_go_proto",
    importpath = "github.com/envoyproxy/nighthawk/api/rate_limiter",
    proto = ":rate_limiter_proto",
    deps = ["@com_envoyproxy_protoc_gen_validate//validate:go_default_library"],
)
