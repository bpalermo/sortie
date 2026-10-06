package report_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	client "github.com/bpalermo/sortie/engine/api/client"

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

// A backend's failure classes are printed after its request count and carried
// in the JSON, only when non-zero: a clean backend's line and JSON do not change.
func TestReportsBreakDownFailuresPerBackend(t *testing.T) {
	clean := backendResult(time.Millisecond, 1000)
	failing := backendResult(time.Millisecond, 997)
	failing.Counters = append(failing.Counters,
		&client.Counter{Name: "benchmark.stream_resets", Value: 3},
		&client.Counter{Name: "benchmark.stream_resets_incomplete_body", Value: 3},
		&client.Counter{Name: "benchmark.stream_resets_remote_reset", Value: 3},
		&client.Counter{Name: "benchmark.pool_failure_timeout", Value: 1},
		&client.Counter{Name: "benchmark.pool_overflow", Value: 0},
		&client.Counter{Name: "upstream_cx_destroy_remote", Value: 12},
	)
	set := &result.Set{Backends: []result.Backend{
		{Addr: "10.0.0.1:8443", Global: clean, Output: &client.Output{Results: []*client.Result{clean}}},
		{Addr: "10.0.0.2:8443", Global: failing, Output: &client.Output{Results: []*client.Result{failing}}},
	}}
	// With the dispatch list set, as the runner always sets it: the failure
	// breakdown must not depend on it being absent.
	r := &run.Report{Executions: []run.ExecutionReport{{
		Label: "soak", Pool: "mesh", Rate: 100, Duration: 10 * time.Second, Set: set,
		Backends: []string{"10.0.0.1:8443", "10.0.0.2:8443"},
	}}}

	var text bytes.Buffer
	if err := report.Text(&text, r); err != nil {
		t.Fatal(err)
	}
	out := text.String()
	if !strings.Contains(out, "10.0.0.1:8443: 1000 requests in 10s\n") {
		t.Errorf("a clean backend's line must be unchanged:\n%s", out)
	}
	want := "10.0.0.2:8443: 997 requests in 10s  (stream_resets 3, stream_resets_incomplete_body 3, pool_failure_timeout 1, stream_resets_remote_reset 3)"
	if !strings.Contains(out, want) {
		t.Errorf("text report is missing %q:\n%s", want, out)
	}
	if strings.Contains(out, "pool_overflow") || strings.Contains(out, "upstream_cx") {
		t.Errorf("zero and non-failure counters must not be printed:\n%s", out)
	}

	var jsonBuf bytes.Buffer
	if err := report.JSON(&jsonBuf, r); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Executions []struct {
			Backends []string `json:"backends"`
			Failures []struct {
				Backend  string            `json:"backend"`
				Counters map[string]uint64 `json:"counters"`
			} `json:"failures"`
		} `json:"executions"`
	}
	if err := json.Unmarshal(jsonBuf.Bytes(), &got); err != nil {
		t.Fatalf("JSON report does not parse: %v\n%s", err, jsonBuf.String())
	}
	e := got.Executions[0]
	if len(e.Backends) != 2 {
		t.Errorf("backends = %v, the address list must keep its shape", e.Backends)
	}
	if len(e.Failures) != 1 || e.Failures[0].Backend != "10.0.0.2:8443" {
		t.Fatalf("failures = %+v, want only the failing backend", e.Failures)
	}
	wantCounters := map[string]uint64{
		"benchmark.stream_resets":                 3,
		"benchmark.stream_resets_incomplete_body": 3,
		"benchmark.stream_resets_remote_reset":    3,
		"benchmark.pool_failure_timeout":          1,
	}
	if len(e.Failures[0].Counters) != len(wantCounters) {
		t.Errorf("counters = %v, want %v", e.Failures[0].Counters, wantCounters)
	}
	for name, v := range wantCounters {
		if e.Failures[0].Counters[name] != v {
			t.Errorf("counters[%s] = %d, want %d", name, e.Failures[0].Counters[name], v)
		}
	}

	// A clean run has no failures key at all.
	jsonBuf.Reset()
	if err := report.JSON(&jsonBuf, reportWith(t, time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(jsonBuf.String(), "failures") {
		t.Errorf("a clean run's JSON must not carry a failures key:\n%s", jsonBuf.String())
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

// A dns pool's backends were decided when the run started, so the report is
// the only record of which nodes a run drove: both renderings list them with
// the name they came from, and say the rate was per backend.
func TestReportsListTheBackendsADnsPoolResolvedTo(t *testing.T) {
	r := reportWith(t, time.Millisecond)
	r.Executions[0].Dns = "nightly-sortie-engine.aether-test.svc.cluster.local:8443"
	r.Executions[0].Backends = []string{"10.0.1.9:8443", "10.0.2.5:8443"}
	r.Executions[0].PerBackend = true

	var text bytes.Buffer
	if err := report.Text(&text, r); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"100 rps per backend for 10s",
		"nightly-sortie-engine.aether-test.svc.cluster.local:8443 resolved to 10.0.1.9:8443, 10.0.2.5:8443",
	} {
		if !strings.Contains(text.String(), want) {
			t.Errorf("text report is missing %q:\n%s", want, text.String())
		}
	}

	var jsonBuf bytes.Buffer
	if err := report.JSON(&jsonBuf, r); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Executions []struct {
			Dns        string   `json:"dns"`
			Backends   []string `json:"backends"`
			PerBackend bool     `json:"per_backend"`
		} `json:"executions"`
	}
	if err := json.Unmarshal(jsonBuf.Bytes(), &got); err != nil {
		t.Fatalf("JSON report does not parse: %v\n%s", err, jsonBuf.String())
	}
	e := got.Executions[0]
	if e.Dns != r.Executions[0].Dns || !e.PerBackend || strings.Join(e.Backends, " ") != "10.0.1.9:8443 10.0.2.5:8443" {
		t.Errorf("execution = %+v", e)
	}
}

// Without a dns pool nothing changes: no resolution line, plain rps.
func TestReportsSayNothingAboutDnsForAServicesPool(t *testing.T) {
	var text bytes.Buffer
	if err := report.Text(&text, reportWith(t, time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(text.String(), "resolved to") || strings.Contains(text.String(), "per backend") {
		t.Errorf("a services pool has nothing to resolve:\n%s", text.String())
	}
}
