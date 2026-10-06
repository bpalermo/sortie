package run_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
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
