package report

import (
	"bytes"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	client "github.com/bpalermo/sortie/engine/api/client"
	"github.com/bpalermo/sortie/internal/compile"
)

func latency(count uint64, mean, max time.Duration, percentiles ...*client.Percentile) *client.Statistic {
	return &client.Statistic{
		Id:          "benchmark_http_client.request_to_response",
		Count:       count,
		MeanType:    &client.Statistic_Mean{Mean: durationpb.New(mean)},
		MaxType:     &client.Statistic_Max{Max: durationpb.New(max)},
		Percentiles: percentiles,
	}
}

func snapshot(statistics ...*client.Statistic) *client.Output {
	return &client.Output{Results: []*client.Result{{
		Name:       "global",
		Counters:   []*client.Counter{{Name: "benchmark.http_2xx", Value: 42}},
		Statistics: statistics,
	}}}
}

func percentile(p float64, d time.Duration) *client.Percentile {
	return &client.Percentile{Percentile: p, DurationType: &client.Percentile_Duration{Duration: durationpb.New(d)}}
}

// The engine's default snapshot carries a statistic's summary and no
// percentiles: the line gives the mean and the max and claims no p99.
func TestSnapshotSummaryOfASummaryHasNoPercentile(t *testing.T) {
	got := snapshotSummary(snapshot(latency(42, 2*time.Millisecond, 9*time.Millisecond)))
	if want := "http_2xx 42  mean 2ms  max 9ms"; got != want {
		t.Errorf("summary = %q, want %q", got, want)
	}
}

// When the snapshot does carry percentiles, the p99 is the first histogram
// bucket at or above 0.99, never an exact 0.99 that Nighthawk rarely emits.
func TestSnapshotSummaryPicksTheFirstBucketAtOrAbove99(t *testing.T) {
	got := snapshotSummary(snapshot(latency(42, 2*time.Millisecond, 9*time.Millisecond,
		percentile(0.5, time.Millisecond),
		percentile(0.995, 7*time.Millisecond),
		percentile(1, 9*time.Millisecond),
	)))
	if want := "http_2xx 42  mean 2ms  max 9ms  p99 7ms"; got != want {
		t.Errorf("summary = %q, want the 0.995 bucket as p99: %q", got, want)
	}
}

// Before the first response the statistic exists with nothing in it; its zero
// mean and max are not measurements and are not printed.
func TestSnapshotSummaryOmitsLatencyOfAnEmptyStatistic(t *testing.T) {
	got := snapshotSummary(snapshot(latency(0, 0, 0)))
	if want := "http_2xx 42"; got != want {
		t.Errorf("summary = %q, want %q", got, want)
	}
}

// The targets of a weighted scenario run at once on one backend, so the line
// has to say which execution it belongs to.
func TestProgressLineNamesTheExecution(t *testing.T) {
	var buf bytes.Buffer
	Progress{W: &buf}.ExecutionProgress(compile.Execution{Label: "mix/a"}, "10.0.0.1:8443", 2340*time.Millisecond,
		snapshot(latency(42, 2*time.Millisecond, 9*time.Millisecond)))
	if got, want := buf.String(), "    mix/a  10.0.0.1:8443  2.3s  http_2xx 42  mean 2ms  max 9ms\n"; got != want {
		t.Errorf("line = %q, want %q", got, want)
	}
}
