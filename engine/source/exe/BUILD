load(
    "@envoy//bazel:envoy_build_system.bzl",
    "envoy_cc_library",
    "envoy_package",
)

licenses(["notice"])  # Apache 2

envoy_package()

envoy_cc_library(
    name = "nighthawk_client_entry_lib",
    srcs = ["client_main_entry.cc"],
    visibility = ["//visibility:public"],
    deps = [
        "//engine/source/client:nighthawk_client_lib",
        "//engine/source/common:version_linkstamp",
        "@com_google_absl//absl/debugging:symbolize",
    ],
    alwayslink = True,
)

envoy_cc_library(
    name = "nighthawk_service_entry_lib",
    srcs = ["service_main_entry.cc"],
    visibility = ["//visibility:public"],
    deps = [
        "//engine/source/client:nighthawk_service_lib",
        "//engine/source/common:version_linkstamp",
        "@com_google_absl//absl/debugging:symbolize",
    ],
    alwayslink = True,
)

envoy_cc_library(
    name = "output_transform_main_entry_lib",
    srcs = ["output_transform_main_entry.cc"],
    visibility = ["//visibility:public"],
    deps = [
        "//engine/source/client:output_transform_main_lib",
        "//engine/source/common:version_linkstamp",
    ],
    alwayslink = True,
)
