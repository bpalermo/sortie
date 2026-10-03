#!/usr/bin/env bash
# Moves the engine's Envoy pin to a commit.
#
# Usage: bazel/bump-envoy.sh <envoy commit sha>
#
# The pin is three things that have to move together, and this script moves
# all of them:
#   - ENVOY_COMMIT in MODULE.bazel, which git_override resolves `envoy` and
#     `envoy_api` to;
#   - the envoyproxy/bazel-registry commit in .bazelrc, taken from Envoy's own
#     .bazelrc at that commit, because Envoy's ".envoy"-suffixed transitive
#     modules exist only there and the registry drops old snapshots;
#   - the versions of the modules this workspace declares that Envoy also
#     pins, synced from Envoy's MODULE.bazel so one copy of each resolves.
# It then refreshes the lockfile and prints what Envoy changed in its .bazelrc
# since the section of ours that mirrors it was last synced, which is the part
# that needs a human: see AGENTS.md, "The Envoy pin".
#
# A commit rather than a registry snapshot: the registry publishes a snapshot
# every few weeks, and a security bump in Envoy should not wait for one.
set -euo pipefail

commit="${1:-}"
if ! [[ "${commit}" =~ ^[0-9a-f]{40}$ ]]; then
  echo "usage: $0 <40-hex envoy commit>" >&2
  exit 2
fi
cd "$(dirname "${BASH_SOURCE[0]}")/.."

raw="https://raw.githubusercontent.com/envoyproxy/envoy/${commit}"
tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT
for f in .bazelrc MODULE.bazel .bazelversion; do
  curl -fsSL "${raw}/${f}" -o "${tmp}/${f}" || { echo "could not fetch ${f} at ${commit}" >&2; exit 1; }
done

registry="$(sed -n 's|.*raw.githubusercontent.com/envoyproxy/bazel-registry/\([0-9a-f]\{40\}\).*|\1|p' "${tmp}/.bazelrc" | head -1)"
if [ -z "${registry}" ]; then
  echo "Envoy's .bazelrc at ${commit} pins no bazel-registry commit" >&2
  exit 1
fi

old="$(sed -n 's/^ENVOY_COMMIT = "\(.*\)"/\1/p' MODULE.bazel)"
sed -i "s/^ENVOY_COMMIT = \".*\"/ENVOY_COMMIT = \"${commit}\"/" MODULE.bazel
sed -i "s|raw.githubusercontent.com/envoyproxy/bazel-registry/[0-9a-f]\{40\}|raw.githubusercontent.com/envoyproxy/bazel-registry/${registry}|" .bazelrc
echo "ENVOY_COMMIT ${old:0:7} -> ${commit:0:7}; registry ${registry:0:8}"

# Shared module versions follow Envoy's.
while read -r dep; do
  ours="$(grep -oP "^bazel_dep\(name = \"${dep}\", version = \"\K[^\"]+" MODULE.bazel || true)"
  theirs="$(grep -oP "^bazel_dep\(name = \"${dep}\", version = \"\K[^\"]+" "${tmp}/MODULE.bazel" || true)"
  if [ -n "${ours}" ] && [ -n "${theirs}" ] && [ "${ours}" != "${theirs}" ]; then
    sed -i "s|^bazel_dep(name = \"${dep}\", version = \"${ours}\"|bazel_dep(name = \"${dep}\", version = \"${theirs}\"|" MODULE.bazel
    echo "${dep}: ${ours} -> ${theirs}"
  fi
done < <(grep -oP '^bazel_dep\(name = "\K[^"]+' MODULE.bazel)

if [ "$(cat .bazelversion)" != "$(cat "${tmp}/.bazelversion")" ]; then
  echo "NOTE: Envoy now pins Bazel $(cat "${tmp}/.bazelversion"); .bazelversion is $(cat .bazelversion). Move it in the same change."
fi

bazel mod deps --lockfile_mode=refresh > /dev/null
echo "MODULE.bazel.lock refreshed"

# Our ENGINE section against Envoy's .bazelrc: registry lines and the lines
# Envoy keeps in files we do not import are the expected noise.
awk '/^# ENGINE: /{f=1} /^# SORTIE: BuildBuddy/{f=0} f' .bazelrc \
  | grep -v -E '^#|^$|--registry=' | sort -u > "${tmp}/ours"
grep -v -E '^#|^$|--registry=|^try-import' "${tmp}/.bazelrc" | sort -u > "${tmp}/theirs"
echo
echo "Envoy .bazelrc lines not in our ENGINE section (consider adding):"
comm -13 "${tmp}/ours" "${tmp}/theirs" | sed 's/^/  + /'
echo "ENGINE section lines Envoy no longer has (consider dropping; ours marked # unique are ours):"
comm -23 "${tmp}/ours" "${tmp}/theirs" | sed 's/^/  - /'
echo
echo "Next: bazel test //... ; then open the PR with the Envoy range ${old:0:7}..${commit:0:7} in its description."
