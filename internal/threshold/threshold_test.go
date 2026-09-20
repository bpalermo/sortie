package threshold

import (
	"testing"

	"google.golang.org/protobuf/types/known/durationpb"

	client "github.com/bpalermo/sortie/gen/api/client"
	"github.com/bpalermo/sortie/internal/metric"
)

func TestParse(t *testing.T) {
	tests := []struct {
		expr       string
		wantOp     Op
		wantValue  float64
		wantIsDur  bool
		wantKind   metric.Kind
		wantErrStr string
	}{
		{expr: "latency_2xx.p95 < 500ms", wantOp: LT, wantValue: 5e8, wantIsDur: true, wantKind: metric.KindPercentile},
		{expr: "latency_2xx.p99.9 <= 1s", wantOp: LE, wantValue: 1e9, wantIsDur: true, wantKind: metric.KindPercentile},
		{expr: "counter:benchmark.http_5xx == 0", wantOp: EQ, wantValue: 0, wantKind: metric.KindCounter},
		{expr: "rate:benchmark.http_2xx >= 950", wantOp: GE, wantValue: 950, wantKind: metric.KindRate},
		{expr: "latency_2xx.mean<10ms", wantOp: LT, wantValue: 1e7, wantIsDur: true, wantKind: metric.KindAggregate},
		{expr: "counter:benchmark.pool_overflow != 3", wantOp: NE, wantValue: 3, wantKind: metric.KindCounter},

		{expr: "latency_2xx.p95 500ms", wantErrStr: "no comparison operator"},
		{expr: "latency_2xx.p95 < fast", wantErrStr: "neither a number"},
		{expr: "latency_2xx.p101 < 1s", wantErrStr: "must be in (0,100]"},
		{expr: "latency_2xx.median < 1s", wantErrStr: "unknown suffix"},
		{expr: "", wantErrStr: "empty threshold"},
	}

	for _, tc := range tests {
		t.Run(tc.expr, func(t *testing.T) {
			got, err := Parse(tc.expr)
			if tc.wantErrStr != "" {
				if err == nil {
					t.Fatalf("Parse(%q) succeeded, want error containing %q", tc.expr, tc.wantErrStr)
				}
				if !contains(err.Error(), tc.wantErrStr) {
					t.Fatalf("Parse(%q) error = %q, want it to contain %q", tc.expr, err, tc.wantErrStr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%q) = %v", tc.expr, err)
			}
			if got.Op != tc.wantOp {
				t.Errorf("op = %q, want %q", got.Op, tc.wantOp)
			}
			if got.Value != tc.wantValue {
				t.Errorf("value = %v, want %v", got.Value, tc.wantValue)
			}
			if got.IsDuration != tc.wantIsDur {
				t.Errorf("isDuration = %v, want %v", got.IsDuration, tc.wantIsDur)
			}
			if got.Selector.Kind != tc.wantKind {
				t.Errorf("kind = %v, want %v", got.Selector.Kind, tc.wantKind)
			}
		})
	}
}

// Parsing must not mistake the "=" of ">=" for an "==" or a bare ">".
func TestParseOperatorPrecedence(t *testing.T) {
	for _, expr := range []string{"counter:x >= 1", "counter:x <= 1", "counter:x != 1", "counter:x == 1"} {
		got, err := Parse(expr)
		if err != nil {
			t.Fatalf("Parse(%q) = %v", expr, err)
		}
		want := Op(expr[len("counter:x ") : len("counter:x ")+2])
		if got.Op != want {
			t.Errorf("Parse(%q).Op = %q, want %q", expr, got.Op, want)
		}
	}
}

func TestEvaluateRejectsUnitMismatch(t *testing.T) {
	result := &client.Result{
		Statistics: []*client.Statistic{{
			Id: "benchmark_http_client.latency_2xx",
			Percentiles: []*client.Percentile{
				{Percentile: 0.95, DurationType: &client.Percentile_Duration{Duration: durationpb.New(100)}},
			},
		}},
	}

	// A latency compared against a bare number is a mistake worth reporting,
	// not a comparison against nanoseconds.
	th, err := Parse("latency_2xx.p95 < 500")
	if err != nil {
		t.Fatal(err)
	}
	if o := th.Evaluate(result); o.Err == nil {
		t.Fatalf("expected a unit-mismatch error, got pass=%v", o.Pass)
	}

	// And the reverse: a counter compared against a duration.
	th, err = Parse("counter:benchmark.http_5xx < 5ms")
	if err != nil {
		t.Fatal(err)
	}
	if o := th.Evaluate(result); o.Err == nil {
		t.Fatalf("expected a unit-mismatch error, got pass=%v", o.Pass)
	}
}

func TestEvaluateMissingMetricFails(t *testing.T) {
	th, err := Parse("latency_2xx.p95 < 500ms")
	if err != nil {
		t.Fatal(err)
	}
	o := th.Evaluate(&client.Result{})
	if o.Pass {
		t.Fatal("a threshold naming an absent statistic must not pass")
	}
	if o.Err == nil {
		t.Fatal("expected an error explaining the metric was not found")
	}
}

// A counter that never incremented is absent from the Output, and the most
// common threshold of all asserts that it stayed at zero.
func TestEvaluateAbsentCounterIsZero(t *testing.T) {
	th, err := Parse("counter:benchmark.http_5xx == 0")
	if err != nil {
		t.Fatal(err)
	}
	o := th.Evaluate(&client.Result{})
	if o.Err != nil {
		t.Fatalf("unexpected error: %v", o.Err)
	}
	if !o.Pass {
		t.Fatal("an absent counter should read as zero and satisfy == 0")
	}
}

func contains(haystack, needle string) bool {
	return len(needle) == 0 || (len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0)
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
