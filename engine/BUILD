load(
    "@envoy//bazel:envoy_build_system.bzl",
    "envoy_cc_binary",
    "envoy_package",
)
load("@rules_img//img:image.bzl", "image_manifest")
load("@rules_img//img:layer.bzl", "file_metadata", "image_layer")
load("@rules_img//img:load.bzl", "image_load")
load("@rules_img//img:push.bzl", "image_push")
load("//bazel:registry.bzl", "IMAGE_REGISTRY", "image_repository")

licenses(["notice"])  # Apache 2

# BUILD is read as data by //bazel/stamp:stamp_test, which checks that the
# engine image carries no commit.
exports_files([
    "BUILD",
    "LICENSE",
])

envoy_package()

filegroup(
    name = "nighthawk",
    srcs = [
        ":nighthawk_client.stripped",
        ":nighthawk_output_transform.stripped",
        ":nighthawk_service.stripped",
        ":nighthawk_test_server.stripped",
    ],
)

envoy_cc_binary(
    name = "nighthawk_client",
    linkopts = [
        "-l:libatomic.a",
        "-lrt",
    ],
    deps = [
        "//engine/source/exe:nighthawk_client_entry_lib",
    ],
)

# A testonly version of the nighthawk client, intended to be built with any required test plugins
# to enable integration test use cases.
envoy_cc_binary(
    name = "nighthawk_client_testonly",
    linkopts = [
        "-l:libatomic.a",
        "-lrt",
    ],
    deps = [
        "//engine/source/exe:nighthawk_client_entry_lib",
        "//engine/source/user_defined_output:log_response_headers_plugin",
        "//engine/test/user_defined_output/fake_plugin:fake_user_defined_output",
    ],
)

envoy_cc_binary(
    name = "nighthawk_test_server",
    linkopts = [
        "-l:libatomic.a",
        "-lrt",
    ],
    deps = [
        "//engine/source/server:http_dynamic_delay_filter_config",
        "//engine/source/server:http_test_server_filter_config",
        "//engine/source/server:http_time_tracking_filter_config",
        "//engine/source/server:udp_echo_filter_config",
        "//engine/source/server:websocket_echo_filter_config",
        "@envoy//source/exe:envoy_main_entry_lib",
    ],
)

envoy_cc_binary(
    name = "nighthawk_service",
    linkopts = [
        "-l:libatomic.a",
        "-lrt",
    ],
    deps = [
        "//engine/source/exe:nighthawk_service_entry_lib",
    ],
)

envoy_cc_binary(
    name = "nighthawk_output_transform",
    linkopts = [
        "-l:libatomic.a",
        "-lrt",
    ],
    deps = [
        "//engine/source/exe:output_transform_main_entry_lib",
    ],
)

# The engine image: one image carrying the binaries that run in a cluster, so
# a release is one tag. nighthawk_service is what sortie drives and the
# entrypoint; nighthawk_test_server is the target for calibration runs and the
# e2e test; nighthawk_client is there to debug a backend by hand. Each is a
# full static Envoy, so three binaries is three times the size of one -- there
# is nothing to share -- and that is accepted for the single tag.
#
# Built for the host platform only: an Envoy binary cannot be cross-built the
# way //bazel/image:defs.bzl cross-builds Go, so the release workflow builds
# this manifest natively on an amd64 and an arm64 runner, pushes each by
# digest, and stitches the two into one index outside Bazel.
image_layer(
    name = "image_layer",
    srcs = {
        "/usr/local/bin/nighthawk_service": ":nighthawk_service",
        "/usr/local/bin/nighthawk_test_server": ":nighthawk_test_server",
        "/usr/local/bin/nighthawk_client": ":nighthawk_client",
    },
    compress = "zstd",
    default_metadata = file_metadata(mode = "0755"),
    include_runfiles = False,
)

image_manifest(
    name = "image_manifest",
    base = "@distroless_cc",
    entrypoint = ["/usr/local/bin/nighthawk_service"],
    labels = {
        "org.opencontainers.image.description": "sortie's load engine: nighthawk_service, nighthawk_test_server and nighthawk_client built on Envoy",
        "org.opencontainers.image.licenses": "Apache-2.0",
        "org.opencontainers.image.source": "https://github.com/bpalermo/sortie",
    },
    # As in //bazel/image:defs.bzl, no label names the commit: the engine is a
    # large image that most commits do not touch, and its digest should move
    # only when it does. The release workflow's dev-<commit> tag and the
    # signature's certificate say which commit published it.
    layers = [":image_layer"],
    user = "65532:65532",
    visibility = ["//visibility:public"],
)

# Pushes this platform's manifest by digest, with no tag. The release workflow
# tags the stitched index, not the per-platform manifests.
image_push(
    name = "image_push_platform",
    image = ":image_manifest",
    registry = IMAGE_REGISTRY,
    repository = image_repository("engine"),
    visibility = ["//visibility:public"],
)

# Loads this platform's manifest into the local daemon, for the chart's kind
# test (test/chart) and for running the engine by hand. The name is local on
# purpose: nothing but a `kind load` or a `docker run` on this host reads it.
image_load(
    name = "image_load",
    image = ":image_manifest",
    tag = "localhost/sortie/engine:e2e",
)
