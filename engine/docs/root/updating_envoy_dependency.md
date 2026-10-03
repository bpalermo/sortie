# Updating the Envoy dependency

The engine is built as part of the sortie workspace, and its Envoy pin lives
at the workspace root: `ENVOY_COMMIT` in `MODULE.bazel` and the registry commit
in `.bazelrc`. Move it with `bazel/bump-envoy.sh <commit>` from the repository
root; the procedure is described in the root `AGENTS.md` under "The Envoy pin".

Upstream Nighthawk's procedure, which this file used to hold, no longer applies:
there is no `WORKSPACE`, no copied CI scripts, and nothing is synced by hand.
