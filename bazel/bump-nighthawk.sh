#!/usr/bin/env bash
# Repin the Nighthawk archive that the API protos are generated from.
#
# Usage: bazel/bump-nighthawk.sh <ref>
#
# Rewrites NIGHTHAWK_COMMIT and NIGHTHAWK_SHA256 in bazel/nighthawk/nighthawk.bzl. The
# protos are not vendored and the Go bindings are not checked in, so this is the
# only place a Nighthawk version is recorded.
set -euo pipefail

cd "$(dirname "$0")/.."
readonly PIN=bazel/nighthawk/nighthawk.bzl

if [ $# -ne 1 ]; then
  echo "usage: $0 <nighthawk-ref>" >&2
  exit 2
fi

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# Resolve the ref to an immutable commit first: a branch name in the pin would
# make the build non-reproducible and the sha256 would drift out from under it.
git clone --quiet --filter=blob:none --no-checkout \
  https://github.com/envoyproxy/nighthawk.git "${work}/nighthawk"
commit="$(git -C "${work}/nighthawk" rev-parse "$1")"

url="https://github.com/envoyproxy/nighthawk/archive/${commit}.tar.gz"
curl -fsSL -o "${work}/nighthawk.tar.gz" "${url}"
sha256="$(sha256sum "${work}/nighthawk.tar.gz" | cut -d' ' -f1)"

sed -i \
  -e "s/^NIGHTHAWK_COMMIT = \".*\"$/NIGHTHAWK_COMMIT = \"${commit}\"/" \
  -e "s/^NIGHTHAWK_SHA256 = \".*\"$/NIGHTHAWK_SHA256 = \"${sha256}\"/" \
  "${PIN}"

echo "pinned envoyproxy/nighthawk@${commit}"
echo "run 'bazel test //...' to check the protos still generate and the API still matches"
