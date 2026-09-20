<!--
Keep it short. The interesting part is usually "how it was verified" and
"what could go wrong", not a restatement of the diff.
-->

## What

<!-- One or two sentences. What changes for someone writing a plan? -->

## Why

<!-- The problem this solves. If it fixes a bug, what did the bug look like? -->

## How it was verified

<!--
Say what you actually ran, and what the result was.

`bazel test //...` proves very little on its own here: the unit tests never talk
to Nighthawk, so anything touching the compiler, the gRPC clients or metric
resolution can pass them while generating the wrong load. The load numbers are
the verification. Run a plan against a real nighthawk_service and say what rate
and what counters came back.
-->

- [ ] `bazel test //...`
- [ ] `bazel run //:gazelle` leaves no diff (CI checks this)
- [ ] Ran a plan end to end against `nighthawk_service`, and the achieved rate
      matched what the plan asked for. State the numbers — a rate that is off by
      the worker count is the failure this catches.
- [ ] If the vendored protos moved: `hack/sync-protos.sh` was used, not a manual
      edit, and `NIGHTHAWK_COMMIT` reflects it.

## Risks and follow-ups
