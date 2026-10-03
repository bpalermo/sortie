package report

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	client "github.com/bpalermo/sortie/engine/api/client"
)

// The p99 in a progress line is the first histogram bucket at or above 0.99,
// never an exact 0.99 that Nighthawk rarely emits.
func TestSnapshotSummaryPicksTheFirstBucketAtOrAbove99(t *testing.T) {
	out := &client.Output{Results: []*client.Result{{
		Name:     "global",
		Counters: []*client.Counter{{Name: "benchmark.http_2xx", Value: 42}},
		Statistics: []*client.Statistic{{
			Id: "benchmark_http_client.request_to_response",
			Percentiles: []*client.Percentile{
				{Percentile: 0.5, DurationType: &client.Percentile_Duration{Duration: durationpb.New(time.Millisecond)}},
				{Percentile: 0.995, DurationType: &client.Percentile_Duration{Duration: durationpb.New(7 * time.Millisecond)}},
				{Percentile: 1, DurationType: &client.Percentile_Duration{Duration: durationpb.New(9 * time.Millisecond)}},
			},
		}},
	}}}
	if got := snapshotSummary(out); got != "http_2xx 42  p99 7ms" {
		t.Errorf("summary = %q, want the 0.995 bucket as p99", got)
	}
}
