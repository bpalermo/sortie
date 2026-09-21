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
**direct** requirements and never looks at BUILD files. A module named only by a
BUILD or `.bzl` file therefore has to stay a direct requirement even though no
Go source imports it, and it carries a `// bazel-only:` comment saying so.
Demote one to `// indirect` and the next tidy drops its `use_repo` entry and the
build breaks somewhere that says nothing about go.mod.

`//tools/deps:deps_test` enforces this for every annotated requirement, in both
directions: annotated means direct, and annotated means some hand-written Bazel
file still names it. Adding a bazel-only dependency needs only the annotation;
the test picks it up without being extended.

There are two today, and both are structural rather than oversights. Neither can
come from a BCR module instead: `envoy_api` and `xds` already link the go_deps
`google/rpc/status`, and `gazelle_override` cannot reach a bazel_dep's
checked-in BUILD files; the BCR `protovalidate` module ships no Go targets at
all, only the protos. The rule is to use whatever target the rest of the build
already links for a given Go import path.

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

## The plan schema

`api/sortie/plan/v1/plan.proto` is the schema, with constraints declared inline
via protovalidate; `internal/plan` parses YAML into it with `protoyaml` and adds
only what the schema cannot express — cross-message rules (a scenario naming a
declared pool), dispatch-dependent rules, and threshold expression syntax.

Put a new constraint in the proto rather than in Go when it concerns one message.
Keep `DiscardUnknown` false: a misspelled field failing the run is a property
there is a test for.

`Scenario.nighthawk_template` is the escape hatch for Nighthawk options the
schema does not model. Do not add a field mirroring a Nighthawk flag unless
sortie needs to reason about it — the template already reaches it. The fields
sortie overwrites are listed in `internal/compile.options`; everything else in a
template survives compilation.

## Protos

Nighthawk's API protos are fetched at the commit pinned in `bazel/nighthawk/nighthawk.bzl`
and compiled by Bazel; `bazel/nighthawk/nighthawk_api.BUILD` declares the targets. Nothing
is vendored and no generated code is checked in. Move the pin with
`bazel/bump-nighthawk.sh <ref>`, never by hand-editing the sha256.

Every Go dependency in `bazel/nighthawk/nighthawk_api.BUILD` must be the same target the
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

## Publishing

`//image` builds a multi-arch image, `//charts/sortie` packages the Helm chart,
and `.github/workflows/publish.yml` pushes and signs both on a push to main.
Four things there are deliberate and easy to undo by accident:

- **`stamp = "force"` on the image rules**, not the default `"auto"`. `"auto"`
  defers to `--stamp`, which only a release build passes, so every other build
  would bake the literal string `{{.STABLE_GIT_COMMIT}}` in as the revision.
- **`build --stamp` in .bazelrc.** rules_helm has no per-target equivalent:
  `helm_package` always defers to the flag, so without it a chart carries
  `0.1.0-GIT-COMMIT` as its version.
- **The signing step reads the digest from the build**, not from the registry.
  Resolving it by listing tags would have to follow ghcr's pagination — it
  returns 100 tags per page, ascending, so a freshly pushed tag sorts last and
  is never on the first page — and re-resolving a mutable tag reintroduces a
  time-of-check window. Bazel already wrote the digest it pushed.
- **Every workspace-status key is `STABLE_`.** Unprefixed keys land in
  volatile-status.txt, which Bazel treats as constant metadata: an action that
  embeds one is not invalidated when it changes, so a cached action can keep
  publishing a stale value. The image's commit tag and the chart's version both
  identify a commit, so both must invalidate. Do not add an unprefixed key for
  anything that identifies a build.
- **`concurrency` is keyed by branch, not by commit**, so publications are
  serialized. The `dev` tag is mutable: a commit-keyed group lets two pushes to
  main publish at once, and an older, slower run finishing last leaves the tag
  pointing at a stale commit. The cost is that GitHub keeps only one pending run
  per group, so a commit queued behind another is cancelled and publishes
  nothing. That is the better failure — a skipped commit is visible as a
  cancelled run and the commit that superseded it publishes seconds later, while
  a stale `dev` is silent.
- **Third-party actions are pinned to commit SHAs**, with the version in a
  comment. This job holds `packages: write` and `id-token: write`, and a
  signature does not help: a swapped action would sign with this repository's
  genuine identity, so `cosign verify` would pass. `.github/dependabot.yml`
  moves the SHA and the comment together so the pins stay current.
- **The chart push needs `HELM_REGISTRY_USERNAME`/`HELM_REGISTRY_PASSWORD`.**
  `docker/login-action` is not enough: rules_helm pins `HELM_REGISTRY_CONFIG` to
  a fresh temp directory per invocation, so Helm reads neither
  `~/.docker/config.json` nor anything a prior `helm registry login` wrote. Its
  pusher skips the login without failing when the variables are absent, so the
  push fails with a 401 rather than a clear error.

Signing cannot be done with rules_img's own support: ghcr does not implement the
OCI Referrers API (verified — `404 MANIFEST_UNKNOWN` for a real digest) and
rules_img attaches signatures only as referrers. cosign's tag scheme works.
