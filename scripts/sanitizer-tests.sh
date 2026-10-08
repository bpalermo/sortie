#!/usr/bin/env bash
# Run the engine's tests under a sanitizer, and run again the ones that could
# not start.
#
#   scripts/sanitizer-tests.sh <bazel test arguments...>
#
# The arguments are everything after `bazel test`: the targets, the sanitizer's
# --config, the remote execution flags. Flags in the `--name=value` form only:
# for the reruns, an argument is a flag when it starts with a dash and a target
# otherwise.
#
# WHY. A sanitizer needs its process laid out in memory a certain way. When the
# kernel's address-space randomization puts it elsewhere, the runtime
# re-executes the binary with randomization off; inside the sandbox of a remote
# executor that is not allowed, and the process ends before the test runs with
#
#   FATAL: ThreadSanitizer: encountered an incompatible memory layout but was
#   unable to disable ASLR (perhaps sandboxing is enabled?).
#
# A death test meets the same thing one level down: gtest starts the binary
# again for the statement that must die, that child ends with the message, and
# the test fails on "died but not with expected error". It goes with the
# executor: on some of them a test fails this way however often it is started,
# so restarting it in place does not help, and running it again as a new action
# -- which lands where the scheduler puts it -- does.
#
# WHAT IS RUN AGAIN. A failed test whose log has that message and NO sanitizer
# report, and nothing else. A test that reported a race or a bad access failed
# for what this run exists to find, and is never given a second chance to pass:
# that is why this is not --flaky_test_attempts, which would rerun a test that
# reported a data race until it did not. One failure of any other kind ends
# the run as failed, after the reruns of the rest.
#
# Exit status: bazel's. 0 when every test passed, the last round's otherwise.
set -uo pipefail

rounds=4
could_not_start='Sanitizer: encountered an incompatible memory layout'
report='(WARNING|ERROR): [A-Za-z]*Sanitizer'

if [ "$#" -eq 0 ]; then
	echo "usage: $0 <bazel test arguments...>" >&2
	exit 2
fi

# bazel-testlogs is the workspace's link to the logs of the last invocation
# (SANITIZER_TESTLOGS names another directory, for trying this script out).
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 2
testlogs="${SANITIZER_TESTLOGS:-bazel-testlogs}"

out="$(mktemp)"
trap 'rm -f "${out}"' EXIT

# flags: the arguments that are not target patterns, for the reruns.
flags=()
for arg in "$@"; do
	case "${arg}" in
	-*) flags+=("${arg}") ;;
	esac
done

args=("$@")
real_failure=0
for ((round = 1; ; round++)); do
	bazel test "${args[@]}" 2>&1 | tee "${out}"
	status="${PIPESTATUS[0]}"
	# 3 is "the build was fine and tests failed": the only outcome with
	# anything to look at. Everything else is bazel's to report.
	if [ "${status}" -ne 3 ]; then
		if [ "${status}" -eq 0 ] && [ "${real_failure}" -ne 0 ]; then
			exit 3
		fi
		exit "${status}"
	fi

	again=()
	while read -r target; do
		[ -n "${target}" ] || continue
		# //engine/test:name -> <testlogs>/engine/test/name/test.log
		path="${target#//}"
		log="${testlogs}/${path/://}/test.log"
		if [ -f "${log}" ] && grep -q "${could_not_start}" "${log}" &&
			! grep -q -E "${report}" "${log}"; then
			again+=("${target}")
		else
			echo "sanitizer-tests: ${target} failed, and not for want of a start" >&2
			real_failure=1
		fi
	done < <(grep -E '^//[^ ]+ +(FAILED|TIMEOUT|NO STATUS)' "${out}" | awk '{print $1}' | sort -u)

	if [ "${#again[@]}" -eq 0 ] || [ "${round}" -ge "${rounds}" ]; then
		exit 3
	fi
	echo "sanitizer-tests: round $((round + 1)) of ${rounds}: ${#again[@]} test(s) could not start under their memory layout: ${again[*]}" >&2
	args=("${flags[@]}" "${again[@]}")
done
