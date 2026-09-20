package report_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	client "github.com/envoyproxy/nighthawk/api/client"

	"github.com/bpalermo/sortie/internal/report"
	"github.com/bpalermo/sortie/internal/result"
	"github.com/bpalermo/sortie/internal/run"
	"github.com/bpalermo/sortie/internal/threshold"
)

func backendResult(p95 time.Duration, ok2xx uint64) *client.Result {
	return &client.Result{
		Name:              "global",
		ExecutionDuration: durationpb.New(10 * time.Second),
		Statistics: []*client.Statistic{{
			Id: "benchmark_http_client.latency_2xx",
			Percentiles: []*client.Percentile{{
				Percentile:   0.95,
				DurationType: &client.Percentile_Duration{Duration: durationpb.New(p95)},
			}},
		}},
		Counters: []*client.Counter{{Name: "benchmark.http_2xx", Value: ok2xx}},
	}
}

func reportWith(t *testing.T, p95 time.Duration, exprs ...string) *run.Report {
	t.Helper()
	global := backendResult(p95, 1000)
	set := &result.Set{Backends: []result.Backend{{
		Addr:   "127.0.0.1:8443",
		Global: global,
		Output: &client.Output{Results: []*client.Result{global}},
	}}}

	ts, err := threshold.ParseAll(exprs)
	if err != nil {
		t.Fatal(err)
	}
	outcomes, pass := set.Evaluate(ts)

	return &run.Report{
		Pass: pass,
		Executions: []run.ExecutionReport{{
			Label:    "smoke",
			Scenario: "smoke",
			Pool:     "local",
			Rate:     100,
			Duration: 10 * time.Second,
			Elapsed:  11 * time.Second,
			Set:      set,
			Outcomes: outcomes,
			Pass:     pass,
		}},
	}
}

func TestTextReportsThePassingCase(t *testing.T) {
	var buf bytes.Buffer
	if err := report.Text(&buf, reportWith(t, 10*time.Millisecond, "latency_2xx.p95 < 50ms")); err != nil {
		t.Fatal(err)
	}
	out := buf.String()

	for _, want := range []string{"smoke", "100 rps for 10s", "latency_2xx.p95 < 50ms", "PASS", "1/1"} {
		if !strings.Contains(out, want) {
			t.Errorf("text report is missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "FAIL") {
		t.Errorf("a passing report must not say FAIL:\n%s", out)
	}
}

// The verdict line is what a human reads first, so a failure has to be visible
// there and not only in the per-threshold rows.
func TestTextReportsTheFailingCase(t *testing.T) {
	var buf bytes.Buffer
	if err := report.Text(&buf, reportWith(t, 900*time.Millisecond, "latency_2xx.p95 < 50ms")); err != nil {
		t.Fatal(err)
	}
	out := buf.String()

	if !strings.Contains(out, "FAIL") {
		t.Errorf("a failing report must say FAIL:\n%s", out)
	}
	if !strings.Contains(out, "0/1") {
		t.Errorf("the verdict should count 0 of 1 passing:\n%s", out)
	}
	// The observed value earns its place: without it the reader cannot tell
	// whether the threshold was missed narrowly or by an order of magnitude.
	if !strings.Contains(out, "900ms") {
		t.Errorf("the failing report should show the observed value:\n%s", out)
	}
}

func TestTextNotesWhenNoThresholdsAreDeclared(t *testing.T) {
	var buf bytes.Buffer
	if err := report.Text(&buf, reportWith(t, time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "no thresholds") {
		t.Errorf("a run with no thresholds should say so:\n%s", buf.String())
	}
}

func TestJSONIsParseableAndCarriesTheVerdict(t *testing.T) {
	var buf bytes.Buffer
	if err := report.JSON(&buf, reportWith(t, 900*time.Millisecond, "latency_2xx.p95 < 50ms")); err != nil {
		t.Fatal(err)
	}

	var got struct {
		Pass       bool `json:"pass"`
		Executions []struct {
			Label      string `json:"label"`
			Pool       string `json:"pool"`
			Rate       uint32 `json:"rate"`
			DurationMS int64  `json:"duration_ms"`
			Pass       bool   `json:"pass"`
			Backends   []string
			Thresholds []struct {
				Expr   string `json:"expr"`
				Scope  string `json:"scope"`
				Pass   bool   `json:"pass"`
				Actual string `json:"actual"`
			} `json:"thresholds"`
		} `json:"executions"`
	}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("JSON report does not parse: %v\n%s", err, buf.String())
	}

	if got.Pass {
		t.Error("pass should be false")
	}
	if len(got.Executions) != 1 {
		t.Fatalf("got %d executions, want 1", len(got.Executions))
	}
	e := got.Executions[0]
	if e.Label != "smoke" || e.Pool != "local" || e.Rate != 100 || e.DurationMS != 10000 {
		t.Errorf("execution fields = %+v", e)
	}
	if len(e.Thresholds) != 1 {
		t.Fatalf("got %d thresholds, want 1", len(e.Thresholds))
	}
	th := e.Thresholds[0]
	if th.Expr != "latency_2xx.p95 < 50ms" || th.Pass || th.Scope != "per-backend" {
		t.Errorf("threshold = %+v", th)
	}
	if th.Actual == "" {
		t.Error("the observed value should be reported")
	}
}

// An execution that never ran has no Set, and the reporters must not panic on it.
func TestReportersHandleAFailedExecution(t *testing.T) {
	r := &run.Report{Executions: []run.ExecutionReport{{
		Label: "broken", Pool: "local", Rate: 10, Duration: time.Second,
		Err: errFake{},
	}}}

	var text, jsonBuf bytes.Buffer
	if err := report.Text(&text, r); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text.String(), "boom") {
		t.Errorf("the error should be reported:\n%s", text.String())
	}
	if err := report.JSON(&jsonBuf, r); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(jsonBuf.String(), "boom") {
		t.Errorf("the JSON report should carry the error:\n%s", jsonBuf.String())
	}
}

type errFake struct{}

func (errFake) Error() string { return "boom" }
