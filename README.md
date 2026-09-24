# sortie

Declarative load-test plans on top of [Nighthawk](https://github.com/envoyproxy/nighthawk).

Nighthawk is an excellent load generator and a poor test harness. It takes one
set of options, runs one benchmark, and prints one result. sortie adds the layer
around it that a benchmarking campaign actually needs: a plan file describing
several scenarios, rate shapes over time, fan-out across a pool of Nighthawk
instances, and thresholds that turn a run into a pass or a fail with an exit
code CI can read.

It drives Nighthawk's existing gRPC control plane — `NighthawkService` and, for
fan-out, `NighthawkDistributor` — so the load generation itself is entirely
Nighthawk's, unmodified.

```yaml
version: v1

pools:
  - name: local
    services: ["127.0.0.1:8443"]

defaults:
  pool: local
  target: http://127.0.0.1:8080/
  concurrency: "2"

thresholds:
  - "counter:benchmark.http_5xx == 0"

scenarios:
  - name: ramp
    executor:
      type: ramping-rate
      rate: 500
      ramp_time: 30s
      duration: 120s
    thresholds:
      - "latency_2xx.p95 < 50ms"
      - "latency_2xx.p99 < 200ms"
```

```console
$ sortie run plan.yaml
  ok   ramp  (ramp to 500 rps over 30s, then hold for 90s, pool "local", 2m1s)
       127.0.0.1:8443: 52500 requests in 2m0s
         ok  counter:benchmark.http_5xx == 0  actual 0           (pool)
         ok  latency_2xx.p95 < 50ms           actual 2.598527ms  (per-backend)
         ok  latency_2xx.p99 < 200ms          actual 8.114688ms  (per-backend)

PASS  1/1 executions passed
```

## Build

```console
bazel build //cmd/sortie
bazel test //...
```

Bazel is the only supported build. Nighthawk's protos are generated from a
pinned archive into packages under `github.com/envoyproxy/nighthawk/api/...`,
which no Go module publishes, so plain `go build` cannot resolve them. `go.mod`
exists for Gazelle and editor tooling and lists the third-party modules only.

Change dependencies through rules_go's SDK — `bazel run @rules_go//go -- get
<module>@<version>` — then `bazel mod tidy`. Not `go mod tidy`, which cannot
resolve the Bazel-only proto packages.

`bazel run //:gazelle` regenerates BUILD files after adding or renaming a Go
file. CI fails if it leaves a diff.

## Commands

| Command | What it does |
| --- | --- |
| `sortie run <plan.yaml>` | Run the plan and report a verdict. `-json` for CI, `-o FILE` to redirect. |
| `sortie validate <plan.yaml>` | Check the plan without running anything. |
| `sortie compile <plan.yaml>` | Print the `CommandLineOptions` it would send to each backend. |

Exit code is `0` when every threshold held, `1` when one failed or an execution
errored, and `2` when the plan or command line was invalid.

## Pools

A pool names the Nighthawk backends a scenario runs on.

```yaml
pools:
  # sortie drives each instance itself and merges the results.
  - name: local
    services: ["10.0.0.11:8443", "10.0.0.12:8443"]

  # A nighthawk_distributor fans the request out on sortie's behalf.
  - name: fleet
    distributor: "10.0.0.1:8442"
    targets: ["10.0.0.11:8443", "10.0.0.12:8443"]
```

The distributor forwards one `ExecutionRequest` unchanged to every target, so
sortie sends it the per-target share rather than the aggregate. `rate` therefore
means the same thing on both paths — what the target receives — but on this one
it must divide exactly by `targets x concurrency`, because a single request
leaves nowhere to put a remainder.

**Nighthawk ships no distributor binary.** `NighthawkDistributor` exists in its
sources as a library and is marked experimental (envoyproxy/nighthawk#369), but
no released binary hosts it and `nighthawk_service` does not. Using a
distributor pool means building a host for that service yourself. The direct
`services:` pool is the path that works out of the box.

## Executors

| Type | Shape |
| --- | --- |
| `constant-rate` | A fixed aggregate rate for `duration`. |
| `ramping-rate` | Linear from zero to `rate` over `ramp_time`, then holds `rate` for the rest of `duration`. Nighthawk's `nighthawk.linear-ramping-rate-limiter-plugin`. |
| `staircase` | One constant-rate step per entry in `stages`. |

`open_loop: true` selects Nighthawk's open-loop mode, where the rate limiter
never compensates for a client that cannot keep pace, so saturation shows up as
pool overflow rather than as latency.

### rate is aggregate, and that has consequences

`rate` is what the target receives. Nighthawk's own `--rps` is **per worker
thread**, so sortie divides by `backends x concurrency` before sending it.

Two things follow, both deliberate:

- A rate no integer per-worker `--rps` can express is an error, not a rounding.
  `rate: 101` with `concurrency: "2"` is refused. A load generator that silently
  produces a different rate than the plan asked for is worse than one that
  refuses.
- `concurrency: "auto"` cannot be combined with a rate, because the worker count
  is decided on the backend and sortie cannot divide by a number it does not know.

### staircase stages are separate runs

Nighthawk cannot change the rate of a run already in flight — the `UpdateRequest`
RPC exists in `api/client/service.proto` but the service rejects it. Each stage
is therefore its own execution: connections are re-established at every
boundary, and each stage is reported and judged separately.

## Anything this schema does not model

`nighthawk_template` on a scenario carries Nighthawk options straight through.
sortie overlays only the fields it owns — `requests_per_second`, `duration`,
`execution_id`, the rate-limiter plugin, and whatever the scenario's own fields
set — so everything else reaches the backend as written.

```yaml
scenarios:
  - name: passthrough
    executor: {type: constant-rate, rate: 100, duration: 30s}
    nighthawk_template:
      max_requests_per_connection: 1000
      burst_size: 5
```

This mirrors how Nighthawk's own adaptive load controller takes a
`nighthawk_traffic_template`, and it means sortie does not have to grow a field
for every Nighthawk flag — transport sockets, request-source plugins,
tunnelling and user-defined output plugins are all reachable without one.

## Thresholds

Thresholds are `<metric> <op> <value>` strings. Operators are `<`, `<=`, `>`,
`>=`, `==`, `!=`. Plan-level thresholds and scenario-level thresholds are
additive; both must hold.

| Selector | Meaning |
| --- | --- |
| `latency_2xx.p95` | A percentile of a statistic. Fractional percentiles like `p99.9` work. |
| `latency_2xx.mean` | `mean`, `min`, `max`, `pstdev` or `count`. |
| `counter:benchmark.http_5xx` | A counter's absolute value. |
| `rate:benchmark.http_2xx` | A counter divided by the execution duration — the achieved rate. |

Statistic and counter names match exactly or by dotted suffix, so `latency_2xx`
resolves `benchmark_http_client.latency_2xx`. An ambiguous suffix is an error
rather than an arbitrary pick.

Duration-valued metrics need duration values (`500ms`); counters and rates need
plain numbers. Mixing them is an error, not a comparison against nanoseconds.

A counter that never incremented is absent from Nighthawk's output rather than
present and zero, so a missing counter reads as zero — `counter:benchmark.http_5xx == 0`
passes on a clean run. A threshold naming a statistic that does not exist fails,
because it went unchecked rather than being satisfied.

### percentiles resolve to the next histogram bucket

Nighthawk returns an HdrHistogram's own buckets, not round percentiles. A
request for `p95` resolves to the first bucket at or above 0.95, which is the
same rule `nighthawk_client` applies to its own human-readable output.

### what aggregates across a pool, and what does not

Counters sum, and counter and rate thresholds are judged against the pool total.

Latency statistics do not aggregate. A pool-wide p95 cannot be recovered from
per-backend p95s, which is why Nighthawk's own sink service merges Outputs by
appending results rather than combining them. sortie evaluates a percentile or
statistic threshold against every backend individually and requires all of them
to hold. For a single-backend pool this is exactly the obvious behaviour.

## Limitations

- **No mid-run control.** Nighthawk's `UpdateRequest` and `CancellationRequest`
  are declared in the proto and rejected by the service
  (envoyproxy/nighthawk#380). Interrupting `sortie run` abandons the gRPC
  streams; the backends keep generating load until their configured duration
  elapses.
- **No progress during a run.** A Nighthawk execution returns nothing until it
  finishes, so sortie reports per execution, not continuously.
- **No scripting.** Nighthawk's `RequestSource` yields independent requests and
  never sees responses, so there is no session flow — no login, capture a token,
  reuse it. Scenarios are stateless load.
- **One execution per backend at a time.** `nighthawk_service` refuses a second
  concurrent run, so scenarios are sequential by design.

## Running it in Kubernetes

A multi-arch image and a Helm chart are published to GHCR on every push to main:

```
ghcr.io/bpalermo/sortie                 linux/amd64, linux/arm64
oci://ghcr.io/bpalermo/sortie/charts    the chart
```

The chart runs a plan as a Job, or as a CronJob with `cronJob.enabled=true`.
The plan is held in a ConfigMap and mounted read-only, so changing a run does
not mean republishing anything. The Job's exit code is the verdict: a breached
threshold fails it, and a malformed plan fails it differently.

The Job's name carries a digest of its rendered pod template, because a Job's
`spec.template` is immutable: with a stable name, `helm upgrade` with a changed
plan would fail with `field is immutable` rather than run it. With the suffix an
upgrade creates a new Job and Helm removes the previous one. Changing nothing
therefore re-applies the same Job rather than re-running it; to run an unchanged
plan again, delete the Job. Changing only `backoffLimit` or
`ttlSecondsAfterFinished` does not rename it -- those are mutable on a Job, so
they patch the existing run instead of starting a new one.

`job.ttlSecondsAfterFinished` defaults to an hour, and deleting the Job is also
what re-runs it: once the TTL has removed it, the next `helm upgrade` finds it
missing and creates it again, generating load even if the upgrade changed
nothing about the run. Set it to `null` to keep finished Jobs until something
deletes them.

**Wait for a run to finish before upgrading it.** Replacing the Job deletes the
running one, and terminating sortie does not stop the load: a Nighthawk backend
keeps generating until its configured duration elapses, because the gRPC
cancellation message is declared in Nighthawk's API but not implemented by the
service ([envoyproxy/nighthawk#380][nh380]). A backend also runs one execution
at a time, so the replacement Job starts, finds the backend still busy with the
run it just abandoned, and fails -- with `backoffLimit: 0` it does not retry.
The old run finishes on its own either way; what is lost is the new one.

[nh380]: https://github.com/envoyproxy/nighthawk/issues/380

The ConfigMap is named after the plan's digest for the same reason in reverse.
A stable name would be updated in place, and a CronJob's Job that was created
before an upgrade but had not started yet would mount the new plan while
reporting itself as the old one. Each plan gets its own object, so a Job can
only mount the plan it was created for.

```console
helm install nightly oci://ghcr.io/bpalermo/sortie/charts/sortie \
  --set-file plan=plan.yaml
```

The default `plan` in `values.yaml` names no backend that exists. That is
deliberate: a plausible-looking default would generate load against whatever
happened to answer.

### Provenance

Images are signed with cosign, keyless, through GitHub's OIDC identity — there
is no key to store or rotate. Signatures cover the index *and* every per-arch
manifest, so pulling by an architecture-specific digest is covered too:

```console
cosign verify \
  --certificate-identity-regexp '^https://github.com/bpalermo/sortie/' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/bpalermo/sortie@sha256:...
```

The chart pins the image by **digest**, injected at package time from the push
target, so a chart can only ever reference the image built alongside it.

Verification needs **cosign 3 or newer**. Signatures are written in cosign's
bundle format, which attaches them as OCI 1.1 referrers and falls back to a
`sha256-<digest>` tag on registries that do not serve the Referrers API — ghcr
being one. A cosign 2 client does not find them and reports `no signatures
found`, which is indistinguishable from an unsigned image, so check the version
before concluding anything from that.

Signing is a workflow step rather than a Bazel rule because of that fallback.
ghcr.io does not implement the Referrers API — verified, it returns `404` for a
real digest — and rules_img pushes signatures as referrers: its `signing_config`
docstring says "the signature is then pushed to the image's repository as an OCI
referrer". Whether it also falls back to a tag is not visible from its source,
since that push lives in a prebuilt binary; without a fallback it cannot work
here, which is the assumption this rests on. cosign's fallback is verified.

## The plan schema

The schema is a protobuf definition, `api/sortie/plan/v1/plan.proto`. Field
names in a plan file are the field names there, and most constraints are
declared inline with [protovalidate](https://github.com/bufbuild/protovalidate)
so a rule sits next to the field it governs.

Plans are parsed with `protoyaml`, which reports violations with a line and
column and the offending line:

```
plan.yaml:11:7 scenarios[0].executor: the ramping-rate executor requires a ramp_time
  11 |       type: ramping-rate
  11 | ......^
```

Unknown fields are rejected, so a misspelled key fails the run rather than being
silently ignored.

Two kinds of rule cannot live in the schema and stay in Go: those spanning
messages (a scenario naming a pool declared elsewhere in the file) and those
depending on dispatch (a rate divisible by `backends x concurrency`, which needs
the pool). Threshold expressions are also checked at load time, since their
grammar belongs to sortie rather than to protobuf.

Durations follow protobuf's own JSON mapping — decimal seconds ending in `s`,
so `90s` rather than Go's `1m30s`.

## Protos

Nighthawk's API protos are fetched at a commit pinned in `bazel/nighthawk/nighthawk.bzl`
and compiled by Bazel. Nothing is vendored and no generated code is checked in,
so the bindings cannot go stale against the `.proto` files they came from.
`bazel/bump-nighthawk.sh <ref>` moves the pin.

Envoy's types come from the `envoy_api` module rather than from the
`go-control-plane` Go module, because that is where the generated Nighthawk
bindings get theirs. Two targets carrying the same Go import path would put a
second copy of every Envoy message in the binary and panic at init on duplicate
registration. The same applies to `google/rpc/status.proto`, whose Go code is
taken from the genproto module gRPC-Go already links.

## Linting

`aspect_rules_lint` runs buildifier over the hand-written Starlark as an
ordinary test target, so `bazel test //...` covers it.

Go is analysed by rules_go's `nogo` (`//tools/nogo`), which runs as part of
compilation, so a vet finding fails the build rather than a separate job.
`aspect_rules_lint` ships no Go linter, which is why the two are split.

`nogo` on a Go 1.27 SDK needs `golang.org/x/tools` >= v0.48.0, and `go_deps`
resolves one `x/tools` across every module by MVS, so this repository's `go.mod`
is what the analyzers are built against. `//tools/deps:deps_test` fails if that
floor is breached, because the symptom otherwise is an export-data error that
never mentions `x/tools`.
