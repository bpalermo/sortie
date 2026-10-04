"""Where sortie's images and chart are published: ONE setting.

Every published coordinate is derived from the constants below -- the
`go_image` at the root (`//:image_push`), the engine's `image_push`
(`//engine:image_push_platform`), the chart push (`//charts/sortie:chart_push`),
the chart's default engine reference (charts/sortie/values.yaml, through the
template test), and -- through scripts/registry.sh, which parses THIS file --
every workflow step and verifier. Nothing else may spell the registry out:
scripts/check-registry-config.sh fails CI on a literal anywhere outside its
allow-list.

History. Until 2026-10-04 this repository published to ghcr.io as
`ghcr.io/bpalermo/sortie`, `ghcr.io/bpalermo/sortie/engine` and
`oci://ghcr.io/bpalermo/sortie/charts/sortie`, with cosign's signatures on
`sha256-<digest>` fallback tags because ghcr.io does not serve the OCI 1.1
Referrers API. Commits published before the move live there and stay there;
deleting those packages would break every historical digest pin.

scripts/registry.sh reads the assignments below with a strict, line-anchored
grep: keep each on ONE line, exactly `NAME = "value"`.
"""

# Registry host.
IMAGE_REGISTRY = "quay.io"

# Organization under the host. Quay has NO nested repositories: everything is
# `<host>/<namespace>/<name>`, one path segment after the org.
IMAGE_NAMESPACE = "sortie"

# Prepended to a chart's name to form its repository under IMAGE_NAMESPACE, so
# the chart `sortie` does not land on the driver image's repository `sortie`.
# A flat prefix, because Quay cannot hold `charts/sortie`; `helm push` cannot
# name such a repository (it appends Chart.yaml's `name:` to whatever base it
# gets), which is why the chart goes out through chart_push (oras).
CHART_REPOSITORY_PREFIX = "chart-"

# Where cosign's keyless signature lands on IMAGE_REGISTRY, which the publish
# workflow ASSERTS for every digest it signs: "referrer" (an OCI 1.1 referrer
# and no tag -- a registry that serves the Referrers API, as quay.io does) or
# "tag" (cosign 3's `sha256-<hex>` fallback tag -- a registry without it, as
# ghcr.io was). A commit whose registry.bzl predates this line was published
# under "tag".
SIGNATURE_LAYOUT = "referrer"

def image_repository(component):
    """Repository path (no host) of a component's image.

    Args:
      component: "sortie" (the driver) or "engine".

    Returns:
      e.g. "sortie/engine".
    """
    return "{}/{}".format(IMAGE_NAMESPACE, component)

def image_reference(component):
    """Host-qualified image repository of a component.

    Args:
      component: "sortie" or "engine".

    Returns:
      e.g. "quay.io/sortie/engine".
    """
    return "{}/{}".format(IMAGE_REGISTRY, image_repository(component))

def chart_repository(chart):
    """Repository path (no host) a chart is published under.

    Args:
      chart: the Chart.yaml name, "sortie".

    Returns:
      e.g. "sortie/chart-sortie".
    """
    return "{}/{}{}".format(IMAGE_NAMESPACE, CHART_REPOSITORY_PREFIX, chart)

def chart_registry_url(chart):
    """The host-qualified OCI repository a chart is pushed to (no scheme, no tag).

    chart_push publishes `<this>:<chart version>` with oras; consumers pull the
    same coordinate with `helm pull oci://<this> --version <X.Y.Z>`.

    Args:
      chart: the Chart.yaml name, "sortie".

    Returns:
      e.g. "quay.io/sortie/chart-sortie".
    """
    return "{}/{}".format(IMAGE_REGISTRY, chart_repository(chart))
