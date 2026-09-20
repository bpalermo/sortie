package result

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	client "github.com/bpalermo/sortie/gen/api/client"
	"github.com/bpalermo/sortie/internal/threshold"
)

func backend(addr string, p95 time.Duration, ok2xx, overflow uint64) Backend {
	global := &client.Result{
		Name:              "global",
		ExecutionDuration: durationpb.New(10 * time.Second),
		Statistics: []*client.Statistic{{
			Id: "benchmark_http_client.latency_2xx",
			Percentiles: []*client.Percentile{
				{Percentile: 0.95, DurationType: &client.Percentile_Duration{Duration: durationpb.New(p95)}},
			},
		}},
		Counters: []*client.Counter{
			{Name: "benchmark.http_2xx", Value: ok2xx},
			{Name: "benchmark.pool_overflow", Value: overflow},
		},
	}
	return Backend{Addr: addr, Global: global, Output: &client.Output{Results: []*client.Result{global}}}
}

func TestTotalsSumCountersAcrossBackends(t *testing.T) {
	s := &Set{Backends: []Backend{
		backend("a:1", 10*time.Millisecond, 500, 2),
		backend("b:1", 20*time.Millisecond, 700, 3),
	}}
	totals := s.Totals()

	got := map[string]uint64{}
	for _, c := range totals.GetCounters() {
		got[c.GetName()] = c.GetValue()
	}
	if got["benchmark.http_2xx"] != 1200 {
		t.Errorf("http_2xx = %d, want 1200", got["benchmark.http_2xx"])
	}
	if got["benchmark.pool_overflow"] != 5 {
		t.Errorf("pool_overflow = %d, want 5", got["benchmark.pool_overflow"])
	}
	// Percentiles cannot be summed, so the totals must not claim to carry any.
	if len(totals.GetStatistics()) != 0 {
		t.Error("totals must not carry statistics; they do not aggregate")
	}
}

func TestCounterThresholdsUsePoolTotals(t *testing.T) {
	s := &Set{Backends: []Backend{
		backend("a:1", time.Millisecond, 500, 4),
		backend("b:1", time.Millisecond, 700, 4),
	}}
	// Each backend overflowed 4 times, under a limit of 5, but the pool
	// overflowed 8 times, over it.
	ts, err := threshold.ParseAll([]string{"counter:benchmark.pool_overflow < 5"})
	if err != nil {
		t.Fatal(err)
	}
	outcomes, pass := s.Evaluate(ts)
	if pass {
		t.Fatal("a counter threshold must be judged against the pool total, not per backend")
	}
	if outcomes[0].Scope != "pool" {
		t.Errorf("scope = %q, want pool", outcomes[0].Scope)
	}
}

// A pool-wide percentile cannot be recovered from per-backend percentiles, so
// every backend has to satisfy the threshold on its own.
func TestPercentileThresholdsRequireEveryBackend(t *testing.T) {
	s := &Set{Backends: []Backend{
		backend("fast:1", 10*time.Millisecond, 500, 0),
		backend("slow:1", 900*time.Millisecond, 500, 0),
	}}
	ts, err := threshold.ParseAll([]string{"latency_2xx.p95 < 100ms"})
	if err != nil {
		t.Fatal(err)
	}
	outcomes, pass := s.Evaluate(ts)
	if pass {
		t.Fatal("one slow backend must fail the execution")
	}
	if outcomes[0].Scope != "per-backend" {
		t.Errorf("scope = %q, want per-backend", outcomes[0].Scope)
	}
	if len(outcomes[0].PerBackend) != 2 {
		t.Fatalf("got %d per-backend outcomes, want 2", len(outcomes[0].PerBackend))
	}
	if !outcomes[0].PerBackend[0].Pass || outcomes[0].PerBackend[1].Pass {
		t.Error("expected the fast backend to pass and the slow one to fail")
	}
}

func TestSinglePoolHappyPath(t *testing.T) {
	s := &Set{Backends: []Backend{backend("a:1", 50*time.Millisecond, 1000, 0)}}
	ts, err := threshold.ParseAll([]string{
		"latency_2xx.p95 < 100ms",
		"counter:benchmark.http_5xx == 0",
		"rate:benchmark.http_2xx >= 99",
	})
	if err != nil {
		t.Fatal(err)
	}
	outcomes, pass := s.Evaluate(ts)
	if !pass {
		for _, o := range outcomes {
			if !o.Pass {
				t.Errorf("threshold %q failed: %s", o.Threshold.Raw, o.Detail())
			}
		}
	}
}

func TestNewSetRequiresMatchingLengths(t *testing.T) {
	if _, err := NewSet([]string{"a:1"}, nil); err == nil {
		t.Fatal("expected an error for mismatched addrs and outputs")
	}
}
