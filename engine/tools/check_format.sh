#!/bin/bash

set -e

FULL_CHECK="$PWD"

TO_CHECK="${2:-$FULL_CHECK}"
# TODO(https://github.com/envoyproxy/nighthawk/issues/165): fully excluding everything
# from the build fixer isn't ideal.
# The configuration lives beside this script, wherever the engine sits in the
# workspace.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
bazel run @envoy//tools/code_format:check_format -- \
  --path=${PWD} \
  --config_path=${SCRIPT_DIR}/code_format/config.yaml \
  --skip_envoy_build_rule_check  --namespace_check Nighthawk \
  --build_fixer_check_excluded_paths=$TO_CHECK \
  $1 $TO_CHECK

# The include checker doesn't support per-file checking, so we only
# run it when a full check is requested.
if [ $FULL_CHECK == $TO_CHECK ]; then
  bazel run //engine/tools:check_envoy_includes.py
fi
