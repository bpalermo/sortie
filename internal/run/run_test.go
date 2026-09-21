package run_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	client "github.com/envoyproxy/nighthawk/api/client"
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
