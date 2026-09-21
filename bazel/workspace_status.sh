#!/usr/bin/env bash
# Supplies the build with the git state that ends up in image labels and tags.
#
# STABLE_ keys are part of the action cache key, so a value that changes every
# build would defeat caching. The unprefixed keys are volatile: Bazel treats
# them as constant metadata, so two builds at the same commit still produce
# byte-identical images.
set -euo pipefail

commit="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"

# Chart.yaml stamps its appVersion from this; a tagged build reports the tag,
# an untagged one a describe string, so a chart can always be traced back.
version="$(git describe --tags --always --dirty 2>/dev/null || echo unknown)"

echo "STABLE_GIT_VERSION ${version}"
echo "STABLE_GIT_COMMIT ${commit}"
echo "GIT_COMMIT ${commit}"
echo "GIT_BRANCH ${branch}"
