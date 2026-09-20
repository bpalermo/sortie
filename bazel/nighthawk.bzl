"""Fetches Nighthawk's public API protos at a pinned commit.

This is a module extension rather than a plain `use_repo_rule` http_archive so
that the BUILD file below resolves `@rules_proto`, `@rules_go` and the proto
dependencies through this module's repo mapping. A repo created by
`use_repo_rule` carries no mapping of its own, and every `load()` in its BUILD
file fails to resolve.

Only the protos are taken. Nighthawk builds them with Envoy's api_proto_package
macro, which needs its whole C++ tree; bazel/nighthawk_api.BUILD declares them
directly instead.
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Nighthawk commit the API protos are taken from. Bump together with the sha256.
NIGHTHAWK_COMMIT = "403f9773b6b1c7d843d21e5683bdac87ba3901ca"

NIGHTHAWK_SHA256 = "2e600ea2113a3e29a0f4a76564f11ad0796d4c7f744c917f44082dc132e0b260"

def _nighthawk_api_impl(_ctx):
    http_archive(
        name = "nighthawk_api",
        build_file = "@sortie//bazel:nighthawk_api.BUILD",
        # Nighthawk ships BUILD files beside its protos, which would make every
        # api/ directory a subpackage and put the .proto files out of reach of
        # the root BUILD file written above. Only the protos are wanted, so the
        # package structure that came with them is removed.
        # -mindepth 2 spares the BUILD file written above, which lands at the
        # repository root; patch_cmds run after it is written.
        patch_cmds = [
            "find . -mindepth 2 \\( -name BUILD -o -name BUILD.bazel \\) -delete",
        ],
        sha256 = NIGHTHAWK_SHA256,
        strip_prefix = "nighthawk-" + NIGHTHAWK_COMMIT,
        url = "https://github.com/envoyproxy/nighthawk/archive/{}.tar.gz".format(
            NIGHTHAWK_COMMIT,
        ),
    )

nighthawk_api = module_extension(implementation = _nighthawk_api_impl)
