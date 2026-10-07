#!/usr/bin/env bash
# Supplies the build with the git state that ends up in image labels and tags.
#
# Every key here is STABLE_. Unprefixed keys go to volatile-status.txt, which
# Bazel treats as constant metadata: an action that embeds one is not
# invalidated when it changes, so a cached action can keep publishing a stale
# value. Everything below identifies a commit and must invalidate when the
# commit changes, so none of it belongs there. Nothing is lost to caching --
# these values change only with the commit, which changes the build anyway.
set -euo pipefail

commit="$(git rev-parse HEAD 2>/dev/null || echo unknown)"

# Chart.yaml stamps its appVersion from this; a tagged build reports the tag,
# an untagged one a describe string, so a chart can always be traced back.
version="$(git describe --tags --always --dirty 2>/dev/null || echo unknown)"

echo "STABLE_GIT_VERSION ${version}"
echo "STABLE_GIT_COMMIT ${commit}"

# How the chart's default engine reference ends: the tag of this commit, or --
# when the publish workflow hands over the digest of the engine index it has
# just pushed and signed -- that digest. A chart published from CI then pins
# the engine by digest, as it pins the driver; a chart built anywhere else
# names the tag this commit's engine would carry. Validated, because it is
# written into values.yaml: anything that is not a sha256 digest is ignored.
suffix=":dev-${commit}"
if [[ "${SORTIE_ENGINE_DIGEST:-}" =~ ^sha256:[0-9a-f]{64}$ ]]; then
	suffix="@${SORTIE_ENGINE_DIGEST}"
fi
echo "STABLE_ENGINE_REF_SUFFIX ${suffix}"
