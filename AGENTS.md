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
test/chart/run.sh           # the chart on a kind cluster (needs kind, kubectl, helm)
```

`test/e2e` runs a plan through the binaries on the host; `test/chart` installs
the chart for real on a kind cluster you create first (`kind create cluster
--name sortie-e2e`), loading both images from `//:image_load` and
`//engine:image_load` as OCI archives. Not a Bazel test: it needs a cluster
and a daemon, so CI runs it as its own job after the Bazel one.

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

`//bazel/deps:deps_test` enforces this for every annotated requirement, in both
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

## The Envoy pin

The engine is Envoy, so an Envoy bump is how it gets CVE fixes and new client
features. The pin is three things that move together: `ENVOY_COMMIT` in
`MODULE.bazel` (a commit, resolved by `git_override`; the registry publishes a
snapshot only every few weeks and a security fix should not wait for one), the
`envoyproxy/bazel-registry` commit in `.bazelrc` (Envoy's `.envoy`-suffixed
transitive modules exist only there, and it drops old snapshots), and the
versions of the modules this workspace declares that Envoy also pins.

`bazel/bump-envoy.sh <commit>` moves all three, refreshes the lockfile and
prints the lines Envoy's `.bazelrc` has that the `ENGINE` section of ours does
not, and vice versa. Those are the human part: Envoy reorganises its build
flags from time to time (it removed `--config=clang` in Sep 2026), and a line
it drops usually has to be dropped here too. Lines marked `# unique` are ours.
Then `bazel test //...` and a PR whose description names the Envoy range.

Bazel and the Go SDK move only with this pin (Envoy pins both; we match). The
`bazel-arm64` and release jobs on `main` are the first to see a bump on arm64.
aether's proxy pins Envoy too; keep the two within a week of each other.

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

- No mid-run updates. `UpdateRequest` exists in `engine/api/client/service.proto`
  and the service rejects it. Cancellation works: `nh.Execute` sends a
  `CancellationRequest` when its context is cancelled and returns the backend's
  partial response with the context's error, so a cancelled run is never
  evaluated as a complete one.
- Progress is opt-in: `StartRequest.progress_interval` makes the engine write
  interim responses (`progress` set, `output` a snapshot) that `nh.Execute`
  hands to a `Progress.Fn`; `run.Observer.ExecutionProgress` and `--progress`
  surface them. A snapshot carries counters and statistic summaries, no
  percentiles; `StartRequest.progress_statistics` asks for full statistics at
  the price of a histogram copy per statistic per worker per snapshot, and
  sortie does not set it. Nothing on the distributor path.
- `Scenario.websocket` compiles to the engine's `websocket` options
  (`WebSocketStreamBenchmarkClientImpl`, the gRPC bidi-stream client's twin;
  framing in `engine/source/common/websocket.*`, shared with the test server's
  `websocket-echo` filter). The rate rule is bidi-stream's (`aggregateRate` in
  `internal/compile`); `protocol` http1 and `method` GET are forced.
- Protocol gaps and the designs for closing them (WebSocket, TCP, UDP) are in
  `docs/parity.md`; follow those designs rather than inventing a shape.
- `Scenario.tls` (ca_file, cert_file, key_file) compiles to the engine's
  `tls_context` with the files inline; the engine adds SNI and ALPN
  (`createTransportSocket` in `engine/source/client/process_bootstrap.cc`).
  Upstream deprecated `tls_context` for `transport_socket`; here it is the
  modeled path and stays. Without `tls`, https verifies nothing.
- A `tcp://`/`tcps://` target selects the engine's TCP mode
  (`TcpBenchmarkClientImpl`, a second `BenchmarkClient` on
  `ThreadLocalCluster::tcpConn`, so the cluster's transport socket applies);
  `Scenario.tcp` tunes it. Per-worker rate like HTTP; counters `benchmark.tcp_*`,
  statistic `benchmark_tcp.message_latency`.
- A `udp://` target selects the engine's UDP mode (`UdpBenchmarkClientImpl`:
  one connected datagram socket per worker, sequence-matched echoes, a sweep
  timer losing datagrams past `--udp-timeout`); `Scenario.udp` tunes it.
  Counters `benchmark.udp_*`, statistic `benchmark_udp.message_latency`. The
  test server's `udp-echo` is a UDP listener filter.
- A backend runs as many executions at once as `nighthawk_service
  --max-concurrent-executions` allows -- one by default -- and refuses the
  rest. sortie only ever asks for more than one for the targets of a weighted
  scenario; scenarios still run one after another.
- `RequestSource` never sees responses, so there is no session flow and no
  response correlation.

An execution is not ended by a failed request, and a run is not ended by a
lost backend. `compile.options` sets `no_default_failure_predicates` unless the
template sets predicates of its own; `Runner.dispatch` runs backends
independently and returns per-backend errors beside the outputs, and
`runExecution` judges whatever came back. Do not reintroduce an errgroup there:
its context cancels the survivors.

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

A scenario with `targets` is expanded by `compile.Expand` into one execution
per target (`plan.ForTarget`: the scenario with that target's url and its
share of the rate), all carrying the same `Group`. The runner starts a group's
executions together and everything else one at a time. The engine side of
that is `ServiceImpl`'s per-stream `Execution` and its
`--max-concurrent-executions` cap: a stream owns at most one running
execution, and a cancellation only ever reaches the stream's own.

## gRPC modes

`Scenario.grpc` maps to the engine's `grpc_mode` and `grpc_stream` options and
forces `protocol` http2 and `method` POST (the loader rejects anything else).
The one subtlety is the rate: in `bidi-stream` the engine's
`requests_per_second` is the backend's *aggregate* message rate, which it
divides over its workers itself (`perWorkerRequestsPerSecond` in
`engine/source/common/rate_limiter_impl.cc`, used by the linear and the
ramping limiter alike). `compile.Divide` and `uniformShare` therefore split
the plan's rate in per-worker units as always and, in that mode only,
multiply each backend's share back up by the workers (`backendRate`).
Everywhere else the engine's rate is per worker. Tests in `internal/compile`
pin both behaviours; keep them when touching `Divide`.

## Protos

The engine's API protos live in `engine/api` and are compiled by Bazel for
both C++ and Go; the `go_proto_library` targets beside them are what sortie
imports (`github.com/bpalermo/sortie/engine/api/...`). Nothing is vendored and
no generated code is checked in. Change a message in one place and both sides
see it.

Every Go dependency of those `go_proto_library` targets must be the same target
the rest of the build already links for that import path. Envoy types come from
`envoy_api`, validate from the target `envoy_api` itself uses, and
`google/rpc/status.proto`'s Go code from the genproto module gRPC-Go links.
Picking a different target for the same import path fails the link with
"multiple copies of package", which is the error to expect if you change one.

## Linting

buildifier runs over the hand-written Starlark via `aspect_rules_lint`, as a
test target tagged `lint`. Go is covered by rules_go's `nogo` (`//:nogo`),
which runs during compilation. rules_lint ships no Go linter, which is why the
two are split. Do not add `go vet` or `gofmt` steps to CI.

Two traps here, both guarded by `//bazel/deps:deps_test`:

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

`//:sortie` builds the binary and `//:image_push` its multi-arch image, `//charts/sortie` packages the Helm chart,
and `.github/workflows/publish.yml` pushes and signs both on a push to main,
to Quay (`quay.io/sortie/...`; ghcr.io until 2026-10-04).
These are deliberate and easy to undo by accident:

- **The registry is one setting, `bazel/registry.bzl`.** Bazel loads it,
  workflows and scripts read it through `scripts/registry.sh`, and
  `scripts/check-registry-config.sh` (a CI step) fails on a registry literal
  anywhere outside its allow-list. The chart's default engine reference in
  `values.yaml` is data, so that check asserts it instead of deriving it.
- **Quay is flat and its repositories are pre-created.** Everything is
  `quay.io/sortie/<name>`; the chart is `chart-sortie` so it does not land on
  the driver image's repository. The robot account behind `QUAY_USERNAME` /
  `QUAY_TOKEN` (secrets of the `release` environment) can write and cannot
  create: a new repository has to exist before the first push, or it 401s.

- **`stamp = "force"` on the image rules**, not the default `"auto"`. `"auto"`
  defers to `--stamp`, which only a release build passes, so every other build
  would bake the literal string `{{.STABLE_GIT_COMMIT}}` in as the revision.
- **`build --stamp` in .bazelrc.** rules_helm has no per-target equivalent:
  `helm_package` always defers to the flag, so without it a chart carries
  `0.1.0-GIT-COMMIT` as its version.
- **The signing step reads the digest from the build**, not from the registry.
  Resolving it by listing tags would have to follow the registry's pagination
  — a freshly pushed tag is not on the first page — and re-resolving a mutable
  tag reintroduces a time-of-check window. Bazel already wrote the digest it pushed.
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
- **cosign is the Bazel-pinned one, `//bazel/cosign`.** It is the release
  binary of the `rules_img_signer_cosign` bazel_dep, sha256-pinned in that
  module, so the workflow and a workstation sign and verify with the same
  build; `//bazel/cosign:version_test` fails until its expected version moves
  with the dep. A cosign major can change the signature format on the
  registry, so a bump of that dep is taken by hand, after establishing what the
  new version writes.
- **Third-party actions are pinned to commit SHAs**, with the version in a
  comment. These jobs hold the registry credential and `id-token: write`, and
  a signature does not help: a swapped action would sign with this repository's
  genuine identity, so `cosign verify` would pass. `.github/dependabot.yml`
  moves the SHA and the comment together so the pins stay current.
- **The chart is pushed by `//charts/sortie:sortie.chart_push` (oras), not by
  rules_helm's push targets.** `helm push` appends the chart's name to the base
  it is given, which on a flat registry is the driver image's repository.
  `chart_push` (`//bazel/helm:defs.bzl`) writes the same OCI artifact helm
  would to the repository it names, and reads the Docker config
  `docker/login-action` wrote; `helm pull oci://...` reads it back. Set
  `CHART_PUSH_REPOSITORY` and pass `-- --plain-http` to try it against a local
  registry.
- **The chart pins the engine by digest, through the workspace status.** The
  engine index is stitched and signed by the workflow, not built by Bazel, so
  Bazel cannot know its digest. The `engine-index` job outputs it, the
  `publish` job exports it as `SORTIE_ENGINE_DIGEST` for every step, and
  `bazel/workspace_status.sh` turns it into `STABLE_ENGINE_REF_SUFFIX`, which
  `values.yaml` is stamped with. Unset, or not a sha256 digest, it falls back
  to the commit's tag; the workflow refuses to publish in that case.
- **The chart is signed too**, by the digest oras printed when it pushed it,
  and verified with `verify_image -- --single`: a chart is one manifest, with
  no children to walk.
- **The verify step asserts where signatures are published**, by asking the
  registry (`scripts/verify-image-signatures.sh`): a referrer, and no
  `sha256-<digest>` tag, for the index and every child. `cosign verify` cannot
  be that gate, since it accepts every layout cosign has written; and it has
  no `--recursive`, so the script walks the children itself.

Signing is a separate `cosign sign --recursive` step rather than rules_img's
signing support: it signs exactly what was pushed, by digest, with the cosign
the verifier runs. Quay serves the OCI Referrers API, so cosign 3 attaches each
signature as a referrer and writes no tag. (ghcr.io does not serve that API --
`404` for a real digest, measured -- which is why signatures there sat on
`sha256-<digest>` fallback tags, and one reason for the move.)

Signatures are in cosign's bundle format and need **cosign 3+** to
verify; a cosign 2 client reports `no signatures found`. That break was taken
deliberately, while sortie had no consumers, rather than deferred to a point
where it would cost something.
