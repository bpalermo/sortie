#!/usr/bin/env bash
# The pinned oras is the version MODULE.bazel says it is. Bumping the archives
# without moving EXPECTED fails here, which is the point.
# --- begin runfiles.bash initialization v3 ---
# shellcheck disable=SC1090
set -uo pipefail
set +e
f=bazel_tools/tools/bash/runfiles/runfiles.bash
source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null ||
	source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -f2- -d' ')" 2>/dev/null ||
	source "$0.runfiles/$f" 2>/dev/null ||
	source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null ||
	source "$(grep -sm1 "^$f " "$0.exe.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null ||
	{
		echo >&2 "ERROR: cannot find $f"
		exit 1
	}
f=
set -e
# --- end runfiles.bash initialization v3 ---
EXPECTED="1.3.4"
oras="$(rlocation "${ORAS_RLOCATIONPATH:?set by the BUILD target}")"
got="$("$oras" version | awk '/^Version:/ {print $2}')"
if [ "$got" != "$EXPECTED" ]; then
	echo "oras version ${got}, want ${EXPECTED} (MODULE.bazel and this test move together)" >&2
	exit 1
fi
echo "oras ${got}"
