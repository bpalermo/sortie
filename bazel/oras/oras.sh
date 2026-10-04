#!/usr/bin/env bash
# Runs the pinned oras (//bazel/oras:oras_bin) from the invoking directory.
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
oras="$(rlocation "${ORAS_RLOCATIONPATH:?set by the BUILD target}")"
if [ -z "$oras" ] || [ ! -x "$oras" ]; then
	echo "ERROR: oras binary not found in runfiles (${ORAS_RLOCATIONPATH})" >&2
	exit 2
fi
cd "${BUILD_WORKING_DIRECTORY:-.}"
exec "$oras" "$@"
