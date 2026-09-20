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
bazel test //...            # builds, tests and lints
bazel run //:gazelle        # after adding, removing or renaming any .go file
bazel mod tidy              # after changing go.mod or a bazel_dep
```

Bazel is the only build. `go build` and `go mod tidy` cannot resolve the
Nighthawk proto packages, which exist only as Bazel targets — do not reach for
them, and do not "fix" go.mod by running `go mod tidy`. It is hand-maintained
and lists third-party modules only.

Change a Go dependency through rules_go's own SDK rather than a host toolchain,
so the version that resolves the module is the version that builds it:

```bash
bazel run @rules_go//go -- get github.com/some/module@v1.2.3
bazel run @rules_go//go -- get -tool github.com/some/cmd   # adds a tool directive
```

This updates go.mod and go.sum without the tidy pass that chokes on the
Bazel-only proto packages. Follow it with `bazel mod tidy` and `bazel run
//:gazelle`.

One trap worth knowing: `bazel mod tidy` rewrites `use_repo` from go.mod's
**direct** requirements and never looks at BUILD files. `google.golang.org/genproto/googleapis/rpc`
is a direct requirement even though no Go source imports it, because
`bazel/nighthawk_api.BUILD` names it. Demote it to `// indirect` and the next
tidy drops it and the build breaks.

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

Nighthawk's API protos are fetched at the commit pinned in `bazel/nighthawk.bzl`
and compiled by Bazel; `bazel/nighthawk_api.BUILD` declares the targets. Nothing
is vendored and no generated code is checked in. Move the pin with
`hack/bump-nighthawk.sh <ref>`, never by hand-editing the sha256.

Every Go dependency in `bazel/nighthawk_api.BUILD` must be the same target the
rest of the build already links for that import path. Envoy types come from
`envoy_api`, validate from the target `envoy_api` itself uses, and
`google/rpc/status.proto`'s Go code from the genproto module gRPC-Go links.
Picking a different target for the same import path fails the link with
"multiple copies of package", which is the error to expect if you change one.

## Linting

buildifier runs over the hand-written Starlark via `aspect_rules_lint`, as a
test target tagged `lint`. Go is covered by rules_go's `nogo` (`//tools/nogo`),
which runs during compilation. rules_lint ships no Go linter, which is why the
two are split. Do not add `go vet` or `gofmt` steps to CI.

Two traps here, both guarded by `//tools/deps:deps_test`:

- `nogo` on a Go 1.27 SDK needs `golang.org/x/tools` >= v0.48.0. It is an
  `// indirect` requirement that nothing imports, existing only to raise the MVS
  floor the analyzers are built against. Delete it and every Go compile fails
  with an export-data version error that never mentions `x/tools`.
- buildifier runs with `--mode=check`. Its own default is `fix`, and the srcs it
  lints are symlinks into the real checkout, so the default would have a test
  silently rewrite source files.

## Verifying a change

Run a plan against a real `nighthawk_service` and check the achieved rate
against what the plan asked for. `examples/smoke.yaml` is the shortest path.
A ramping executor is the sharpest check of the rate-limiter wiring: ramping to
R over T then holding for H should produce `R*T/2 + R*H` requests, and the count
comes back exact.
