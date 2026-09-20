#!/usr/bin/env bash
# Re-vendor Nighthawk's protos and regenerate gen/.
#
# Usage: hack/sync-protos.sh [nighthawk-ref]
#
# The ref defaults to the commit recorded in third_party/nighthawk/NIGHTHAWK_COMMIT,
# so running this with no arguments reproduces the current checkout rather than
# silently moving to upstream HEAD.
set -euo pipefail

cd "$(dirname "$0")/.."
readonly VENDOR=third_party/nighthawk
readonly PROTOS=(
  api/client/options.proto
  api/client/output.proto
  api/client/service.proto
  api/distributor/distributor.proto
  api/sink/sink.proto
  api/rate_limiter/linear_ramping_rate_limiter.proto
)

ref="${1:-$(cat "${VENDOR}/NIGHTHAWK_COMMIT")}"
work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

git clone --quiet --filter=blob:none --no-checkout \
  https://github.com/envoyproxy/nighthawk.git "${work}"
resolved="$(git -C "${work}" rev-parse "${ref}")"

for proto in "${PROTOS[@]}"; do
  mkdir -p "${VENDOR}/$(dirname "${proto}")"
  git -C "${work}" show "${resolved}:${proto}" > "${VENDOR}/${proto}"
done
echo "${resolved}" > "${VENDOR}/NIGHTHAWK_COMMIT"

buf dep update
buf generate
go mod tidy
bazel run //:gazelle

echo "synced to envoyproxy/nighthawk@${resolved}"
