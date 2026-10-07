package run_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	client "github.com/bpalermo/sortie/engine/api/client"
	"google.golang.org/genproto/googleapis/rpc/status"

	"github.com/bpalermo/sortie/internal/plan"
	"github.com/bpalermo/sortie/internal/run"
)

// planFor builds a single-scenario plan pointing at the given backends.
func planFor(t *testing.T, body string, backends ...string) *plan.Plan {
	t.Helper()
	services := make([]string, 0, len(backends))
	for _, b := range backends {
		services = append(services, fmt.Sprintf("%q", b))
	}
	raw := fmt.Sprintf(`
version: v1
pools:
  - name: local
    services: [%s]
defaults:
  pool: local
  target: http://127.0.0.1:1/
%s
`, strings.Join(services, ", "), body)

	p, err := plan.Parse([]byte(raw))
	if err != nil {
		t.Fatalf("parsing test plan: %v\n%s", err, raw)
	}
	return p
}

func TestRunPassesWhenThresholdsHold(t *testing.T) {
	fake := startFake(t, func(int, *client.CommandLineOptions) *client.ExecutionResponse {
		return okResponse(1000, 10*time.Millisecond, 10*time.Second)
	})

	p := planFor(t, `
scenarios:
  - name: smoke
    executor: {type: constant-rate, rate: 100, duration: 10s}
    thresholds:
      - "latency_2xx.p95 < 50ms"
      - "counter:benchmark.http_5xx == 0"
`, fake.addr)

	report, err := (&run.Runner{Plan: p}).Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !report.Pass {
		t.Errorf("report should pass: %+v", report.Executions[0].Outcomes)
	}
	if got := len(report.Executions); got != 1 {
		t.Fatalf("got %d executions, want 1", got)
	}
	if got := report.Executions[0].Label; got != "smoke" {
		t.Errorf("label = %q, want smoke", got)
	}
}

// A breached threshold must fail the report without failing the execution:
// the run happened, the result was simply not good enough.
func TestRunFailsOnBreachedThreshold(t *testing.T) {
	fake := startFake(t, func(int, *client.CommandLineOptions) *client.ExecutionResponse {
		return okResponse(1000, 900*time.Millisecond, 10*time.Second)
	})

	p := planFor(t, `
scenarios:
  - name: slow
    executor: {type: constant-rate, rate: 100, duration: 10s}
    thresholds:
      - "latency_2xx.p95 < 50ms"
`, fake.addr)

	report, err := (&run.Runner{Plan: p}).Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.Pass {
		t.Fatal("a breached threshold must fail the report")
	}
	if report.Executions[0].Err != nil {
		t.Errorf("the execution itself succeeded; Err should be nil, got %v", report.Executions[0].Err)
	}
}

// Nighthawk reports its own failures in error_detail rather than as an RPC
// error, and that has to surface as a failed execution rather than a pass.
func TestRunSurfacesBackendFailure(t *testing.T) {
	fake := startFake(t, func(int, *client.CommandLineOptions) *client.ExecutionResponse {
		return &client.ExecutionResponse{
			ErrorDetail: &status.Status{Code: 13, Message: "Unknown failure"},
		}
	})

	p := planFor(t, `
scenarios:
  - name: broken
    executor: {type: constant-rate, rate: 100, duration: 10s}
`, fake.addr)

	report, err := (&run.Runner{Plan: p}).Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.Pass {
		t.Fatal("a backend failure must not pass")
	}
	if report.Executions[0].Err == nil {
		t.Fatal("expected the execution to carry the backend's error")
	}
	if !strings.Contains(report.Executions[0].Err.Error(), "Unknown failure") {
		t.Errorf("error = %v, want it to quote the backend", report.Executions[0].Err)
	}
}

// Stages run in order, each as its own execution, and each carries its own rate.
func TestRunStaircaseDispatchesEachStageInOrder(t *testing.T) {
	fake := startFake(t, func(int, *client.CommandLineOptions) *client.ExecutionResponse {
		return okResponse(100, time.Millisecond, time.Second)
	})

	p := planFor(t, `
scenarios:
  - name: steps
    executor:
      type: staircase
      stages:
        - {rate: 100, duration: 1s}
        - {rate: 300, duration: 1s}
`, fake.addr)

	report, err := (&run.Runner{Plan: p}).Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := len(report.Executions); got != 2 {
		t.Fatalf("got %d executions, want one per stage", got)
	}
	if report.Executions[0].Label != "steps/stage-1" || report.Executions[1].Label != "steps/stage-2" {
		t.Errorf("labels = %q, %q", report.Executions[0].Label, report.Executions[1].Label)
	}

	got := fake.received()
	if len(got) != 2 {
		t.Fatalf("backend saw %d requests, want 2", len(got))
	}
	if got[0].GetRequestsPerSecond().GetValue() != 100 || got[1].GetRequestsPerSecond().GetValue() != 300 {
		t.Errorf("rates reached the backend as %d then %d, want 100 then 300",
			got[0].GetRequestsPerSecond().GetValue(), got[1].GetRequestsPerSecond().GetValue())
	}
}

// With two backends the rate is split between them and counters are judged
// against the pool total, not each backend.
func TestRunSplitsRateAcrossBackendsAndTotalsCounters(t *testing.T) {
	respond := func(int, *client.CommandLineOptions) *client.ExecutionResponse {
		return okResponse(500, time.Millisecond, 10*time.Second)
	}
	a := startFake(t, respond)
	b := startFake(t, respond)

	p := planFor(t, `
scenarios:
  - name: split
    executor: {type: constant-rate, rate: 100, duration: 10s}
    thresholds:
      - "counter:benchmark.http_2xx == 1000"
`, a.addr, b.addr)

	report, err := (&run.Runner{Plan: p}).Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !report.Pass {
		t.Errorf("pool total should be 1000: %v", report.Executions[0].Outcomes[0].Detail())
	}
	for _, fake := range []*fakeService{a, b} {
		got := fake.received()
		if len(got) != 1 {
			t.Fatalf("backend %s saw %d requests, want 1", fake.addr, len(got))
		}
		if rps := got[0].GetRequestsPerSecond().GetValue(); rps != 50 {
			t.Errorf("backend %s got %d rps, want half of 100", fake.addr, rps)
		}
	}
}

// A plan the dispatcher cannot satisfy must fail before any load is generated:
// 100 rps over 3 worker threads is not expressible as an integer per-worker
// --rps, and discovering that after the run has started would be too late.
func TestRunRejectsUndispatchablePlanWithoutCallingBackend(t *testing.T) {
	fake := startFake(t, func(int, *client.CommandLineOptions) *client.ExecutionResponse {
		return okResponse(1, time.Millisecond, time.Second)
	})

	p := planFor(t, `
scenarios:
  - name: odd
    concurrency: "3"
    executor: {type: constant-rate, rate: 100, duration: 10s}
`, fake.addr)

	report, err := (&run.Runner{Plan: p}).Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.Pass {
		t.Fatal("an undispatchable plan must not pass")
	}
	if report.Executions[0].Err == nil {
		t.Fatal("expected a dispatch error")
	}
	if len(fake.received()) != 0 {
		t.Error("the backend was called despite the plan being undispatchable")
	}
}

// tableResolver answers LookupHost from a table; a missing host is an error.
type tableResolver map[string][]string

func (r tableResolver) LookupHost(_ context.Context, host string) ([]string, error) {
	addrs, ok := r[host]
	if !ok {
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}
	return addrs, nil
}

// dnsPlanFor builds a plan whose pool is a dns name, with the executor body
// given, so a test can point the name at its fakes through the resolver.
func dnsPlanFor(t *testing.T, executor string) *plan.Plan {
	t.Helper()
	raw := fmt.Sprintf(`
version: v1
pools:
  - name: nodes
    dns: engine.test:8443
scenarios:
  - name: soak
    pool: nodes
    target: http://127.0.0.1:1/
    concurrency: "2"
    executor: %s
    thresholds:
      - "counter:benchmark.http_2xx == 2000"
`, executor)
	return mustParse(t, raw)
}

func mustParse(t *testing.T, raw string) *plan.Plan {
	t.Helper()
	p, err := plan.Parse([]byte(raw))
	if err != nil {
		t.Fatalf("parsing test plan: %v\n%s", err, raw)
	}
	return p
}

// A dns pool is resolved once when the run starts, and every address it
// yields -- in sorted order -- is a backend that receives the per-backend
// rate. The report records the name and what it resolved to.
//
// A dns pool has one port for every address, so the two backends are two
// loopback addresses on one port: 127.0.0.1 and 127.0.0.2, which Linux
// routes to loopback without configuration.
func TestRunResolvesADnsPoolIntoBackends(t *testing.T) {
	respond := func(int, *client.CommandLineOptions) *client.ExecutionResponse {
		return okResponse(1000, time.Millisecond, 10*time.Second)
	}
	a := startFake(t, respond)
	_, port, _ := net.SplitHostPort(a.addr)
	b := startFakeOn(t, "127.0.0.2:"+port, respond)
	if b == nil {
		t.Skip("127.0.0.2 is not a loopback alias on this host")
	}
	// A third fake the name does not resolve to must see nothing.
	c := startFake(t, respond)

	p := dnsPlanFor(t, "{type: constant-rate, rate: 60, duration: 10s, per_backend: true}")
	p.GetPools()[0].Dns = "engine.test:" + port
	resolver := tableResolver{"engine.test": {"127.0.0.2", "127.0.0.1"}}

	runner := &run.Runner{Plan: p, Resolver: resolver}
	report, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !report.Pass {
		t.Errorf("two backends x 1000 requests should total 2000: %v", report.Executions[0].Outcomes)
	}
	er := report.Executions[0]
	want := []string{"127.0.0.1:" + port, "127.0.0.2:" + port}
	if strings.Join(er.Backends, " ") != strings.Join(want, " ") {
		t.Errorf("backends = %v, want %v (sorted)", er.Backends, want)
	}
	if er.Dns != "engine.test:"+port {
		t.Errorf("Dns = %q, want the pool's name", er.Dns)
	}
	if !er.PerBackend {
		t.Error("PerBackend must reach the report")
	}
	for _, fake := range []*fakeService{a, b} {
		got := fake.received()
		if len(got) != 1 {
			t.Fatalf("backend %s saw %d requests, want 1", fake.addr, len(got))
		}
		if rps := got[0].GetRequestsPerSecond().GetValue(); rps != 30 {
			t.Errorf("backend %s got %d rps, want 30 (60 per backend over 2 workers)", fake.addr, rps)
		}
	}
	if len(c.received()) != 0 {
		t.Error("a backend the name did not resolve to was called")
	}
}

// A name that resolves to nothing fails the run before any load, as a
// ResolveError and with no report, once the wait for a first address is over.
func TestRunFailsWhenADnsPoolResolvesToNothing(t *testing.T) {
	p := dnsPlanFor(t, "{type: constant-rate, rate: 60, duration: 10s, per_backend: true}")

	for name, resolver := range map[string]tableResolver{
		"empty answer": {"engine.test": {}},
		"not found":    {},
	} {
		t.Run(name, func(t *testing.T) {
			runner := &run.Runner{Plan: p, Resolver: resolver, ResolveTimeout: time.Millisecond}
			report, err := runner.Run(context.Background())
			var re *plan.ResolveError
			if !errors.As(err, &re) {
				t.Fatalf("err = %v, want a *plan.ResolveError", err)
			}
			if report != nil {
				t.Error("nothing ran, so there is nothing to report")
			}
		})
	}
}

// Interrupting a run that is still waiting for its dns pool is a cancelled
// run, not a plan naming something missing: the error is the context's, which
// the CLI does not report as bad usage, and it comes back at once rather than
// after the wait.
func TestRunCancelledWhileWaitingForADnsPoolReturnsTheContextError(t *testing.T) {
	p := dnsPlanFor(t, "{type: constant-rate, rate: 60, duration: 10s, per_backend: true}")
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)

	runner := &run.Runner{Plan: p, Resolver: tableResolver{"engine.test": {}}, ResolveTimeout: time.Minute}
	started := time.Now()
	_, err := runner.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	var re *plan.ResolveError
	if errors.As(err, &re) {
		t.Error("a cancellation was reported as a ResolveError, which the CLI treats as bad usage")
	}
	if waited := time.Since(started); waited > 5*time.Second {
		t.Errorf("the cancellation took %s to be noticed", waited)
	}
}

// The same when the cancellation lands while a lookup is blocked: the resolver
// returns the context's error, and it must not come back dressed as a
// ResolveError.
func TestRunCancelledDuringALookupReturnsTheContextError(t *testing.T) {
	p := dnsPlanFor(t, "{type: constant-rate, rate: 60, duration: 10s, per_backend: true}")
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)

	_, err := (&run.Runner{Plan: p, Resolver: blockingResolver{}}).Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	var re *plan.ResolveError
	if errors.As(err, &re) {
		t.Error("a cancellation during the lookup was reported as a ResolveError")
	}
}

// The timeout bounds a lookup in flight, not only the pauses between lookups:
// a resolver that never answers is given up on when the wait runs out, as a
// ResolveError, since nothing was resolved.
func TestRunDnsTimeoutBoundsABlockedLookup(t *testing.T) {
	p := dnsPlanFor(t, "{type: constant-rate, rate: 60, duration: 10s, per_backend: true}")
	started := time.Now()
	_, err := (&run.Runner{Plan: p, Resolver: blockingResolver{}, ResolveTimeout: 100 * time.Millisecond}).Run(context.Background())
	var re *plan.ResolveError
	if !errors.As(err, &re) {
		t.Fatalf("err = %v, want a *plan.ResolveError", err)
	}
	if waited := time.Since(started); waited > 5*time.Second {
		t.Errorf("a blocked lookup held a 100ms timeout for %s", waited)
	}
}

// With two dns pools, a name that has answered is not asked again while the
// other is still empty: its backends are the ones it gave the first time,
// even if the name server would now say something else.
func TestRunDoesNotReResolveAPoolThatAnswered(t *testing.T) {
	fake := startFake(t, func(int, *client.CommandLineOptions) *client.ExecutionResponse {
		return okResponse(10, time.Millisecond, time.Second)
	})
	host, port, _ := net.SplitHostPort(fake.addr)
	p, err := plan.Parse([]byte(fmt.Sprintf(`
version: v1
pools:
  - name: first
    dns: first.test:%[1]s
  - name: second
    dns: second.test:%[1]s
scenarios:
  - name: s
    pool: first
    target: http://127.0.0.1:1/
    executor: {type: constant-rate, rate: 10, duration: 1s}
`, port)))
	if err != nil {
		t.Fatal(err)
	}
	resolver := &perNameResolver{host: host, emptyFor: map[string]int{"second.test": 2}}
	if _, err := (&run.Runner{Plan: p, Resolver: resolver, ResolveTimeout: 30 * time.Second}).Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := resolver.calls["first.test"]; got != 1 {
		t.Errorf("first.test was looked up %d times, want once: it had answered", got)
	}
	if got := resolver.calls["second.test"]; got != 3 {
		t.Errorf("second.test was looked up %d times, want 3 (two empty answers, then one)", got)
	}
}

// perNameResolver answers every name with host, after emptyFor[name] empty
// answers, and counts the lookups per name.
type perNameResolver struct {
	mu       sync.Mutex
	host     string
	emptyFor map[string]int
	calls    map[string]int
}

func (c *perNameResolver) LookupHost(_ context.Context, name string) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.calls == nil {
		c.calls = map[string]int{}
	}
	c.calls[name]++
	if c.calls[name] <= c.emptyFor[name] {
		return nil, nil
	}
	return []string{c.host}, nil
}

// blockingResolver answers only when its context ends, as a resolver waiting
// on an unreachable name server does.
type blockingResolver struct{}

func (blockingResolver) LookupHost(ctx context.Context, _ string) ([]string, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// The wait never runs past its timeout: with less than a poll interval left,
// the last poll is that short.
func TestRunDnsWaitDoesNotOutliveItsTimeout(t *testing.T) {
	p := dnsPlanFor(t, "{type: constant-rate, rate: 60, duration: 10s, per_backend: true}")
	runner := &run.Runner{Plan: p, Resolver: tableResolver{"engine.test": {}}, ResolveTimeout: 100 * time.Millisecond}
	started := time.Now()
	if _, err := runner.Run(context.Background()); err == nil {
		t.Fatal("want a ResolveError")
	}
	if waited := time.Since(started); waited > 800*time.Millisecond {
		t.Errorf("a 100ms timeout was waited out for %s", waited)
	}
}

// An empty answer is asked again until the timeout: the engines often come up
// beside the run, and a headless Service lists a pod only once it is ready.
func TestRunWaitsForADnsPoolToAnswer(t *testing.T) {
	fake := startFake(t, func(int, *client.CommandLineOptions) *client.ExecutionResponse {
		return okResponse(2000, time.Millisecond, 10*time.Second)
	})
	_, port, _ := net.SplitHostPort(fake.addr)
	p := dnsPlanFor(t, "{type: constant-rate, rate: 60, duration: 10s, per_backend: true}")
	p.GetPools()[0].Dns = "engine.test:" + port

	resolver := &countingResolver{answerAfter: 2, addrs: []string{"127.0.0.1"}}
	runner := &run.Runner{Plan: p, Resolver: resolver, ResolveTimeout: 10 * time.Second}
	report, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if resolver.calls != 3 {
		t.Errorf("resolver was asked %d times, want 3 (two empty answers, then the address)", resolver.calls)
	}
	if !report.Pass || len(report.Executions[0].Backends) != 1 {
		t.Errorf("report = %+v", report.Executions[0])
	}
}

// countingResolver answers empty until it has been asked answerAfter times.
type countingResolver struct {
	answerAfter int
	addrs       []string
	calls       int
}

func (r *countingResolver) LookupHost(context.Context, string) ([]string, error) {
	r.calls++
	if r.calls <= r.answerAfter {
		return nil, nil
	}
	return r.addrs, nil
}

// A weighted scenario drives its targets at the same time: the backend sees
// every target's start before it answers any of them, each target gets its
// share of the rate and its own report entry, and a threshold is judged per
// target.
func TestRunWeightedTargetsRunConcurrently(t *testing.T) {
	// Hold every response until all three starts have arrived, so a sequential
	// dispatch would deadlock here (and fail the test by timeout) rather than
	// pass by accident.
	var mu sync.Mutex
	arrived := 0
	all := make(chan struct{})
	fake := startFake(t, func(_ int, opts *client.CommandLineOptions) *client.ExecutionResponse {
		mu.Lock()
		arrived++
		if arrived == 3 {
			close(all)
		}
		mu.Unlock()
		select {
		case <-all:
		case <-time.After(10 * time.Second):
			t.Error("the targets' executions were not started together")
		}
		// The slow target answers with a high p95; the others are fine.
		p95 := time.Millisecond
		if strings.HasSuffix(opts.GetUri().GetValue(), "/slow") {
			p95 = time.Second
		}
		return okResponse(100, p95, time.Second)
	})

	p := planFor(t, `
scenarios:
  - name: mix
    executor: {type: constant-rate, rate: 100, duration: 1s}
    targets:
      - {name: a, url: http://127.0.0.1:1/a, weight: 6}
      - {name: b, url: http://127.0.0.1:1/b, weight: 3}
      - {name: slow, url: http://127.0.0.1:1/slow, weight: 1}
    thresholds:
      - "latency_2xx.p95 < 50ms"
`, fake.addr)

	report, err := (&run.Runner{Plan: p}).Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := len(report.Executions); got != 3 {
		t.Fatalf("got %d executions, want one per target", got)
	}
	labels := []string{report.Executions[0].Label, report.Executions[1].Label, report.Executions[2].Label}
	if labels[0] != "mix/a" || labels[1] != "mix/b" || labels[2] != "mix/slow" {
		t.Errorf("labels = %q", labels)
	}
	if report.Executions[0].Rate != 60 || report.Executions[1].Rate != 30 || report.Executions[2].Rate != 10 {
		t.Errorf("shares = %d %d %d, want 60 30 10",
			report.Executions[0].Rate, report.Executions[1].Rate, report.Executions[2].Rate)
	}
	if !report.Executions[0].Pass || !report.Executions[1].Pass || report.Executions[2].Pass {
		t.Errorf("pass = %v %v %v, want only the slow target to fail",
			report.Executions[0].Pass, report.Executions[1].Pass, report.Executions[2].Pass)
	}
	if report.Pass {
		t.Error("the report passed although one target breached its threshold")
	}
	rates := map[string]uint32{}
	for _, o := range fake.received() {
		rates[o.GetUri().GetValue()] = o.GetRequestsPerSecond().GetValue()
	}
	if rates["http://127.0.0.1:1/a"] != 60 || rates["http://127.0.0.1:1/b"] != 30 || rates["http://127.0.0.1:1/slow"] != 10 {
		t.Errorf("backend rates = %v", rates)
	}
}

// A backend that goes away does not void the run on the others: with one
// engine per node, a node rebooting mid-soak takes its own numbers with it
// and nobody else's. The execution fails, names the lost backend, and still
// reports and judges what the survivor counted.
func TestRunKeepsTheOtherBackendsWhenOneIsLost(t *testing.T) {
	alive := startFake(t, func(int, *client.CommandLineOptions) *client.ExecutionResponse {
		return okResponse(1000, 10*time.Millisecond, 10*time.Second)
	})
	// A port nothing listens on: the dial fails.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := l.Addr().String()
	l.Close()

	p := planFor(t, `
scenarios:
  - name: soak
    executor: {type: constant-rate, rate: 100, duration: 10s}
    thresholds:
      - "latency_2xx.p95 < 50ms"
`, alive.addr, dead)

	runner := &run.Runner{Plan: p, DialTimeout: 200 * time.Millisecond}
	report, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	e := report.Executions[0]
	if e.Err != nil {
		t.Fatalf("the execution as a whole errored (%v); one backend returned results", e.Err)
	}
	if e.Pass || report.Pass {
		t.Error("a run that lost a backend must not pass, whatever the survivors' thresholds say")
	}
	if len(e.BackendErrors) != 1 || e.BackendErrors[0].Addr != dead {
		t.Fatalf("backend errors = %+v, want exactly the dead backend %s", e.BackendErrors, dead)
	}
	if e.Set == nil || len(e.Set.Backends) != 1 || e.Set.Backends[0].Addr != alive.addr {
		t.Fatalf("results = %+v, want the surviving backend's", e.Set)
	}
	if len(e.Outcomes) != 1 || !e.Outcomes[0].Pass {
		t.Errorf("the survivor's threshold was not evaluated, or did not hold: %+v", e.Outcomes)
	}
}

// An engine that reports a failure but returns what it counted -- a run
// stopped early by a failure predicate the plan asked for -- keeps its
// counters in the report. The execution fails; its numbers are not lost.
func TestRunKeepsTheCountersOfAnExecutionTheEngineFailed(t *testing.T) {
	fake := startFake(t, func(int, *client.CommandLineOptions) *client.ExecutionResponse {
		resp := okResponse(42, time.Millisecond, time.Second)
		resp.ErrorDetail = &status.Status{Code: 13, Message: "Exiting due to failing termination predicate"}
		return resp
	})
	p := planFor(t, `
scenarios:
  - name: stopped
    executor: {type: constant-rate, rate: 100, duration: 10s}
`, fake.addr)

	report, err := (&run.Runner{Plan: p}).Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	e := report.Executions[0]
	if e.Pass {
		t.Error("an execution the engine failed must not pass")
	}
	if e.Err != nil {
		t.Fatalf("Err = %v; the backend returned results, so this is a backend error, not a lost execution", e.Err)
	}
	if len(e.BackendErrors) != 1 || !strings.Contains(e.BackendErrors[0].Err.Error(), "termination predicate") {
		t.Errorf("backend errors = %+v, want the engine's message", e.BackendErrors)
	}
	if e.Set == nil || len(e.Set.Backends) != 1 {
		t.Fatalf("the engine's counters were discarded: %+v", e.Set)
	}
}

// The engine's own defaults end an execution at its first failed request.
// sortie turns them off, so the backend is told to run the distance, unless
// the plan's template asks for an early stop.
func TestRunDisablesTheEnginesDefaultFailurePredicates(t *testing.T) {
	fake := startFake(t, func(int, *client.CommandLineOptions) *client.ExecutionResponse {
		return okResponse(10, time.Millisecond, time.Second)
	})
	p := planFor(t, `
scenarios:
  - name: default
    executor: {type: constant-rate, rate: 10, duration: 1s}
  - name: asks-for-a-stop
    executor: {type: constant-rate, rate: 10, duration: 1s}
    nighthawk_template:
      failure_predicates: {"benchmark.http_5xx": 0}
  - name: asks-for-the-defaults
    executor: {type: constant-rate, rate: 10, duration: 1s}
    nighthawk_template:
      no_default_failure_predicates: false
`, fake.addr)
	if _, err := (&run.Runner{Plan: p}).Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := fake.received()
	if len(got) != 3 {
		t.Fatalf("backend saw %d requests, want 3", len(got))
	}
	if !got[0].GetNoDefaultFailurePredicates().GetValue() || len(got[0].GetFailurePredicates()) != 0 {
		t.Errorf("default: no_default_failure_predicates = %v, predicates = %v; want the defaults off",
			got[0].GetNoDefaultFailurePredicates(), got[0].GetFailurePredicates())
	}
	if got[1].GetNoDefaultFailurePredicates() != nil || len(got[1].GetFailurePredicates()) != 1 {
		t.Errorf("a template's failure_predicates were not left alone: %v / %v",
			got[1].GetNoDefaultFailurePredicates(), got[1].GetFailurePredicates())
	}
	if got[2].GetNoDefaultFailurePredicates() == nil || got[2].GetNoDefaultFailurePredicates().GetValue() {
		t.Errorf("a template's explicit no_default_failure_predicates: false was overridden: %v",
			got[2].GetNoDefaultFailurePredicates())
	}
}
