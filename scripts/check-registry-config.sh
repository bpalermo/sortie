#!/usr/bin/env bash
# The registry is ONE setting: bazel/registry.bzl.
#
# Moving registries is only a one-file edit while nothing else spells the
# registry out, and a review cannot be trusted to keep that true: a literal
# pasted into a workflow keeps working until the day a move leaves it pointing
# at the old registry. So this asserts, on every change:
#
#   1. scripts/registry.sh parses the setting.
#   2. charts/sortie/values.yaml names the engine image as exactly
#      image_reference("engine"). That default is data (helm reads it, Bazel
#      only stamps the commit into it), so it cannot be derived.
#   3. No tracked file outside the allow-list contains `<host>/<namespace>`,
#      computed from the setting, not typed here.
#   4. No tracked file outside the legacy allow-list mentions the registry this
#      repository published to before 2026-10-04.
#
# Both hunts run a positive control first, so they cannot pass by matching
# nothing.
#
# ALLOW-LIST (the current coordinates):
#   bazel/registry.bzl             the setting
#   charts/sortie/values.yaml      the engine default (asserted by 2)
#   README.md, engine/README.md    what a user pulls and installs
#   AGENTS.md                      the publishing notes
#   scripts/registry.sh            its usage text
#   scripts/check-registry-config.sh
# LEGACY ALLOW-LIST (the old coordinates):
#   bazel/registry.bzl             the history note
#   README.md, AGENTS.md           where older commits live, and why it moved
#   scripts/registry-lib.sh        the token endpoint differs by registry
#   scripts/check-registry-config.sh
#
# No network, no Bazel. Exit 0 clean, 1 on a violation, 2 when the check itself
# cannot run.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 2

fail=0
bad() {
	echo "::error::$*"
	fail=1
}

# --- 1. the setting parses ---------------------------------------------------
if ! host="$(scripts/registry.sh host)" || ! engine="$(scripts/registry.sh ref engine)" ||
	! ns_repo="$(scripts/registry.sh repo engine)" || [ -z "$host" ] || [ -z "$engine" ]; then
	echo "::error::scripts/registry.sh cannot parse bazel/registry.bzl" >&2
	exit 2
fi
namespace="${ns_repo%/engine}"
echo "setting: ${host}/${namespace}"

esc() { printf '%s' "$1" | sed -E 's/[][\.^$*+?(){}|/]/\\&/g'; }

# --- 2. the chart's engine default agrees ------------------------------------
n="$(grep -cE "^[[:space:]]*ref:[[:space:]]*\"$(esc "$engine"):dev-\{STABLE_GIT_COMMIT\}\"[[:space:]]*$" charts/sortie/values.yaml || true)"
if [ "$n" != 1 ]; then
	bad "charts/sortie/values.yaml has ${n} \`ref:\` line(s) naming ${engine}:dev-{STABLE_GIT_COMMIT}, want exactly 1 -- the chart's engine default disagrees with bazel/registry.bzl"
else
	echo "ok: charts/sortie/values.yaml defaults the engine to ${engine}"
fi

# --- 3. no literal outside the allow-list ------------------------------------
allow=(
	':(exclude)bazel/registry.bzl'
	':(exclude)charts/sortie/values.yaml'
	':(exclude)README.md'
	':(exclude)engine/README.md'
	':(exclude)AGENTS.md'
	':(exclude)scripts/registry.sh'
	':(exclude)scripts/check-registry-config.sh'
)
pattern="$(esc "${host}/${namespace}")(?![[:alnum:]_.-])"
for probe in "registry: ${host}/${namespace}" "image=${engine}@sha256:0"; do
	if ! printf '%s\n' "$probe" | grep -qP -- "$pattern"; then
		echo "::error::the literal hunt does not match '${probe}' -- the pattern is broken" >&2
		exit 2
	fi
done
hits="$(git grep -nP -- "$pattern" -- . "${allow[@]}")"
rc=$?
case "$rc" in
0)
	bad "the registry is spelled out outside bazel/registry.bzl. Derive it (Bazel: load //bazel:registry.bzl; shell and workflows: scripts/registry.sh), or add the path to the allow-list in scripts/check-registry-config.sh with a reason:"
	printf '%s\n' "$hits" | sed 's/^/  /'
	;;
1) echo "ok: no registry literal outside the allow-list" ;;
*)
	echo "::error::git grep failed (exit ${rc})" >&2
	exit 2
	;;
esac

# --- 4. the previous registry ------------------------------------------------
legacy_host="ghcr.io"
legacy_allow=(
	':(exclude)bazel/registry.bzl'
	':(exclude)README.md'
	':(exclude)AGENTS.md'
	':(exclude)scripts/registry-lib.sh'
	':(exclude)scripts/check-registry-config.sh'
)
if [ "$legacy_host" = "$host" ]; then
	echo "::error::the legacy host is the current registry host (${host})" >&2
	exit 2
fi
legacy_pattern="(?<![[:alnum:]_.-])$(esc "$legacy_host")(?![[:alnum:]_-])"
for probe in "registry: ${legacy_host}" "oci://${legacy_host}/bpalermo/sortie/charts"; do
	if ! printf '%s\n' "$probe" | grep -qP -- "$legacy_pattern"; then
		echo "::error::the legacy hunt does not match '${probe}' -- the pattern is broken" >&2
		exit 2
	fi
done
hits="$(git grep -nP -- "$legacy_pattern" -- . "${legacy_allow[@]}")"
rc=$?
case "$rc" in
0)
	bad "the previous registry (${legacy_host}) is still mentioned. Point it at the setting, or add the path to the legacy allow-list in scripts/check-registry-config.sh with a reason:"
	printf '%s\n' "$hits" | sed 's/^/  /'
	;;
1) echo "ok: ${legacy_host} appears nowhere outside the legacy allow-list" ;;
*)
	echo "::error::git grep failed (exit ${rc})" >&2
	exit 2
	;;
esac

[ "$fail" -eq 0 ] || exit 1
echo "registry config: one setting, bazel/registry.bzl"
