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

`go build ./cmd/sortie` works too; the Bazel build is the supported one.

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

The distributor forwards one `ExecutionRequest` unchanged, so every target runs
the same rate and sortie's rate division does not apply on that path. It is also
marked experimental upstream (envoyproxy/nighthawk#369).

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

## Protos

The Nighthawk protos under `third_party/nighthawk/` are vendored and pinned in
`third_party/nighthawk/NIGHTHAWK_COMMIT`. `buf generate` regenerates `gen/`;
envoy, googleapis and protoc-gen-validate types resolve to their published Go
modules rather than being regenerated.
