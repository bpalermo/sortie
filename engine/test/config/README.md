# Stress Test Configuration

This directory contains Bazel build settings for controlling test execution behavior.

## Problem Solved

Previously, the project used `action_env` to pass environment variables to tests, which causes:
- Complete rebuild/retest on any environment change
- No cache sharing between different users/CI systems
- Non-hermetic builds

## Solution

We now use Bazel's modern build setting mechanism to control stress test execution:

```bash
# Run with stress tests enabled
bazel test --//engine/test/config:run_stress_tests=True //engine/test/...

# Run without stress tests (default)
bazel test --//engine/test/config:run_stress_tests=False //engine/test/...
# or simply
bazel test //engine/test/...
```

## How It Works

1. **Build Setting**: `//engine/test/config:run_stress_tests` is a `bool_flag` build setting (default: False)

2. **Config Setting**: `//engine/test/config:stress_tests_enabled` matches when the flag is set to True

3. **Test Environment**: The Python test binary uses `select()` to conditionally set environment variables based on the config_setting

4. **Test Detection**: Python tests check `os.environ.get("NH_RUN_STRESS_TESTS", "false") == "true"`

## Benefits

- **Modern**: Uses current Bazel best practices (build settings, not `--define`)
- **Hermetic**: Build configuration is explicit and reproducible
- **Cacheable**: Builds with the same flags share cache entries
- **Type-safe**: Boolean flags prevent typos and invalid values
- **Clear**: Test behavior is controlled by explicit command-line flags

## CI Usage

The CI automatically enables stress tests when running on branches:
- Pull requests and branch builds: `--//engine/test/config:run_stress_tests=True`
- Local development (no GH_BRANCH): `--//engine/test/config:run_stress_tests=False`

## Using .bazelrc config

```bash
bazel test --config=stress //engine/test/...
```
