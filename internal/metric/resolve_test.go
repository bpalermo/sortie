package metric

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	client "github.com/envoyproxy/nighthawk/api/client"
)

func latencyStat() *client.Statistic {
	ms := func(n int) *durationpb.Duration { return durationpb.New(time.Duration(n) * time.Millisecond) }
	return &client.Statistic{
		Id:       "benchmark_http_client.latency_2xx",
		Count:    1000,
		MeanType: &client.Statistic_Mean{Mean: ms(20)},
		MinType:  &client.Statistic_Min{Min: ms(1)},
		MaxType:  &client.Statistic_Max{Max: ms(900)},
		Percentiles: []*client.Percentile{
			{Percentile: 0, DurationType: &client.Percentile_Duration{Duration: ms(1)}},
			{Percentile: 0.5, DurationType: &client.Percentile_Duration{Duration: ms(15)}},
			{Percentile: 0.9375, DurationType: &client.Percentile_Duration{Duration: ms(40)}},
			{Percentile: 0.96875, DurationType: &client.Percentile_Duration{Duration: ms(60)}},
			{Percentile: 0.9990234375, DurationType: &client.Percentile_Duration{Duration: ms(500)}},
			{Percentile: 1, DurationType: &client.Percentile_Duration{Duration: ms(900)}},
		},
	}
}

func sampleResult() *client.Result {
	return &client.Result{
		Name:              "global",
		Statistics:        []*client.Statistic{latencyStat()},
		ExecutionDuration: durationpb.New(10 * time.Second),
		Counters: []*client.Counter{
			{Name: "benchmark.http_2xx", Value: 1000},
			{Name: "benchmark.pool_overflow", Value: 7},
		},
	}
}

// Nighthawk emits an HdrHistogram's own buckets, not round percentiles, so a
// request for p95 must resolve to the first bucket at or above 0.95 -- the same
// rule nighthawk_client applies to its human-readable output.
func TestResolvePercentileRoundsUpToNextBucket(t *testing.T) {
	sel, err := ParseSelector("latency_2xx.p95")
	if err != nil {
		t.Fatal(err)
	}
	v, err := Resolve(sampleResult(), sel)
	if err != nil {
		t.Fatal(err)
	}
	if !v.IsDuration {
		t.Fatal("latency percentile should be a duration")
	}
	if got, want := time.Duration(int64(v.Num)), 60*time.Millisecond; got != want {
		t.Errorf("p95 = %s, want %s (the 0.96875 bucket)", got, want)
	}
	if v.ActualPercentile != 0.96875 {
		t.Errorf("actual percentile = %v, want 0.96875", v.ActualPercentile)
	}
}

func TestResolvePercentileAboveHighestBucket(t *testing.T) {
	sel, _ := ParseSelector("latency_2xx.p100")
	v, err := Resolve(sampleResult(), sel)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := time.Duration(int64(v.Num)), 900*time.Millisecond; got != want {
		t.Errorf("p100 = %s, want %s", got, want)
	}
}

func TestResolveAggregates(t *testing.T) {
	tests := map[string]struct {
		want  float64
		isDur bool
	}{
		"latency_2xx.mean":  {want: float64(20 * time.Millisecond), isDur: true},
		"latency_2xx.min":   {want: float64(1 * time.Millisecond), isDur: true},
		"latency_2xx.max":   {want: float64(900 * time.Millisecond), isDur: true},
		"latency_2xx.count": {want: 1000},
	}
	for expr, tc := range tests {
		t.Run(expr, func(t *testing.T) {
			sel, err := ParseSelector(expr)
			if err != nil {
				t.Fatal(err)
			}
			v, err := Resolve(sampleResult(), sel)
			if err != nil {
				t.Fatal(err)
			}
			if v.Num != tc.want || v.IsDuration != tc.isDur {
				t.Errorf("= (%v, dur=%v), want (%v, dur=%v)", v.Num, v.IsDuration, tc.want, tc.isDur)
			}
		})
	}
}

// A bare "latency_2xx" must resolve the fully-qualified
// "benchmark_http_client.latency_2xx" so plans stay readable.
func TestSuffixMatching(t *testing.T) {
	sel, _ := ParseSelector("latency_2xx.mean")
	if _, err := Resolve(sampleResult(), sel); err != nil {
		t.Fatalf("suffix match failed: %v", err)
	}
}

func TestAmbiguousSuffixIsAnError(t *testing.T) {
	r := sampleResult()
	r.Statistics = append(r.Statistics, &client.Statistic{
		Id:       "other_client.latency_2xx",
		MeanType: &client.Statistic_Mean{Mean: durationpb.New(time.Second)},
	})
	sel, _ := ParseSelector("latency_2xx.mean")
	_, err := Resolve(r, sel)
	if err == nil {
		t.Fatal("an ambiguous suffix must be an error, not an arbitrary pick")
	}
}

func TestResolveRate(t *testing.T) {
	sel, _ := ParseSelector("rate:benchmark.http_2xx")
	v, err := Resolve(sampleResult(), sel)
	if err != nil {
		t.Fatal(err)
	}
	if v.Num != 100 {
		t.Errorf("rate = %v, want 100 (1000 requests over 10s)", v.Num)
	}
	if v.IsDuration {
		t.Error("a rate is not a duration")
	}
}

func TestGlobalResultRequiresGlobal(t *testing.T) {
	out := &client.Output{Results: []*client.Result{{Name: "0"}, {Name: "1"}}}
	if _, err := GlobalResult(out); err == nil {
		t.Fatal("expected an error when no global result is present")
	}
	single := &client.Output{Results: []*client.Result{{Name: "0"}}}
	if _, err := GlobalResult(single); err != nil {
		t.Fatalf("a single result should stand in for global: %v", err)
	}
}
