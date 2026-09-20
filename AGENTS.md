# AGENTS.md

Guidance for agents working in this repository.

## What this is

`sortie` is a harness around [Nighthawk](https://github.com/envoyproxy/nighthawk),
not a load generator. It compiles a YAML plan into Nighthawk
`CommandLineOptions`, dispatches them over Nighthawk's gRPC control plane, and
evaluates thresholds against the returned `Output` proto. Nighthawk generates
all of the load and is never modified from here.

## Build and test

```bash
bazel test //...            # the supported path
bazel run //:gazelle        # after adding, removing or renaming any .go file
go test ./...               # works, but Bazel is what CI runs
```

CI requires that `bazel run //:gazelle` leaves no diff, so regenerate BUILD
files rather than hand-editing them.

## The two things that are easy to get wrong

Both were found by running against a real backend, not by reading code, and unit
tests do not catch either on their own.

**Nighthawk's `--rps` is per worker thread.** A backend running
`--concurrency 2 --rps 100` emits 200 requests per second. A plan's `rate` is
the aggregate the target receives, so `internal/compile.Divide` divides it by
`backends x concurrency`. Getting this wrong generates a multiple of the
requested load and every threshold still passes. A rate that no integer
per-worker `--rps` can express is refused rather than rounded; `concurrency:
"auto"` cannot be combined with a rate at all.

**Latency percentiles do not aggregate across a pool.** A pool-wide p95 cannot
be recovered from per-backend p95s — Nighthawk returns summarised percentiles,
not histograms, which is why its own sink service merges `Output`s by appending
results. Counters sum and are judged against the pool total; percentile and
statistic thresholds are evaluated against every backend and all must hold. Do
not add an "aggregate p95".

## Constraints inherited from Nighthawk

These are properties of the backend, not gaps to paper over. Documented in the
README's Limitations section; keep the two in sync.

- No mid-run control. `UpdateRequest` and `CancellationRequest` exist in
  `api/client/service.proto` and the service rejects both
  (envoyproxy/nighthawk#380). Interrupting a run abandons the gRPC streams while
  the backends keep generating load. Do not write code or docs implying a clean
  abort.
- No progress during a run. An execution returns nothing until it finishes.
- One execution per backend at a time; `nighthawk_service` refuses a second.
- `RequestSource` never sees responses, so there is no session flow and no
  response correlation.

## Protos

`third_party/nighthawk/` is vendored and pinned by
`third_party/nighthawk/NIGHTHAWK_COMMIT`; `gen/` is generated from it by `buf`.
Both are machine-written. Change them with `hack/sync-protos.sh`, never by hand.
Envoy, googleapis and protoc-gen-validate types deliberately resolve to their
published Go modules instead of being regenerated.

## Verifying a change

Run a plan against a real `nighthawk_service` and check the achieved rate
against what the plan asked for. `examples/smoke.yaml` is the shortest path.
A ramping executor is the sharpest check of the rate-limiter wiring: ramping to
R over T then holding for H should produce `R*T/2 + R*H` requests, and the count
comes back exact.
