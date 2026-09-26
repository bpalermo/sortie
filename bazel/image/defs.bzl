"""A multi-arch, digest-pinned OCI image for a Go binary.

`go_image` wraps the four rules_img rules that always appear together -- layer,
manifest, index, push -- plus the build setting that names the mutable tag. They
are separated here rather than in a package because each carries a decision that
is easy to undo by accident and expensive to notice: `stamp = "force"` on the
stamped rules, `STABLE_` rather than volatile status keys in the commit tag, and
`include_runfiles = False` for a static binary. Calling this macro gets all of
them; hand-assembling the four rules gets whichever the author remembered.
"""

load("@bazel_skylib//rules:common_settings.bzl", "string_flag")
load("@rules_img//img:image.bzl", "image_index", "image_manifest")
load("@rules_img//img:layer.bzl", "file_metadata", "image_layer")
load("@rules_img//img:push.bzl", "image_push")

DEFAULT_PLATFORMS = [
    "@rules_go//go/toolchain:linux_amd64",
    "@rules_go//go/toolchain:linux_arm64",
]

def go_image(
        name,
        binary,
        registry,
        repository,
        labels = {},
        entrypoint_path = None,
        base = "@distroless_static",
        platforms = DEFAULT_PLATFORMS,
        default_tag = "dev",
        visibility = ["//visibility:public"]):
    """Builds a multi-arch image around a Go binary and a target to push it.

    Defines, for `name = "image"`:

      //pkg:image_tag       string_flag, the mutable tag (override with
                            --//pkg:image_tag=vX.Y.Z)
      //pkg:image_layer     the binary, one layer
      //pkg:image_manifest  one platform's manifest
      //pkg:image_index     the multi-platform index
      //pkg:image_push      pushes the index under two tags

    Args:
      name: prefix for the generated targets.
      binary: the Go binary label to place in the image.
      registry: registry host, e.g. "ghcr.io".
      repository: repository path within the registry.
      labels: OCI labels. "org.opencontainers.image.revision" is added from the
        stamped commit unless already given.
      entrypoint_path: absolute path of the binary inside the image. Defaults to
        "/" + the binary's target name.
      base: base image. distroless static, since a static Go binary needs no
        libc and a smaller base is a smaller attack surface.
      platforms: rules_go toolchain constraints, one manifest per platform.
      default_tag: value of the tag flag when nothing overrides it.
      visibility: visibility of the index and push targets.
    """
    if entrypoint_path == None:
        entrypoint_path = "/" + binary.rsplit(":", 1)[-1].rsplit("/", 1)[-1]

    # STABLE_, not the volatile GIT_COMMIT. A volatile status value is constant
    # metadata to Bazel, so an action embedding one is not invalidated when it
    # changes, and a cached push would republish the previous commit's tag.
    image_labels = dict(labels)
    image_labels.setdefault("org.opencontainers.image.revision", "{{.STABLE_GIT_COMMIT}}")

    string_flag(
        name = name + "_tag",
        build_setting_default = default_tag,
        visibility = ["//visibility:public"],
    )

    image_layer(
        name = name + "_layer",
        srcs = {entrypoint_path: binary},
        compress = "zstd",
        default_metadata = file_metadata(mode = "0755"),
        # A static Go binary has no runfiles; including them adds an empty tree
        # and a second layer for nothing.
        include_runfiles = False,
    )

    image_manifest(
        name = name + "_manifest",
        base = base,
        entrypoint = [entrypoint_path],
        labels = image_labels,
        layers = [name + "_layer"],
        # "force" rather than the default "auto": auto defers to --stamp, which
        # only a release build passes, so every other build would bake the
        # literal "{{.STABLE_GIT_COMMIT}}" in as the revision. Stamped values
        # are constant metadata to Bazel, so builds at one commit stay
        # reproducible.
        stamp = "force",
    )

    # One manifest, many platforms: the index performs the transition itself, so
    # rules_go's toolchains cross-build the binary for each.
    image_index(
        name = name + "_index",
        manifests = [name + "_manifest"],
        platforms = platforms,
        stamp = "force",
        visibility = visibility,
    )

    # Two tags deliberately. The first is mutable and is what a human pulls; the
    # second is immutable and is the only one that can answer "which commit is
    # this?" after the fact.
    image_push(
        name = name + "_push",
        build_settings = {"tag": name + "_tag"},
        image = name + "_index",
        registry = registry,
        repository = repository,
        tag_list = [
            "{{.tag}}",
            "{{if .STABLE_GIT_COMMIT}}{{.tag}}-{{.STABLE_GIT_COMMIT}}{{end}}",
        ],
        visibility = visibility,
    )
