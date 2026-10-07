package report_test

import (
	"bytes"
	"encoding/json"
	"errors"
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
		&client.Counter{Name: "benchmark.http_inflight_lost", Value: 2},
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
	want := "10.0.0.2:8443: 997 requests in 10s  (stream_resets 3, stream_resets_incomplete_body 3, pool_failure_timeout 1, http_inflight_lost 2, stream_resets_remote_reset 3)"
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
		"benchmark.http_inflight_lost":            2,
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

// The JSON carries what each backend counted and the pool's totals, and
// names a backend that did not finish cleanly; the text report names it too.
// A TCP run against a target that is not an exact echo says so under the
// backend's line, whatever thresholds the plan has; a clean run says nothing.
func TestTextWarnsOfTcpEchoMismatches(t *testing.T) {
	text := func(sent, mismatches uint64) string {
		// As the engine reports them: a counter at zero is left out.
		var counters []*client.Counter
		if sent > 0 {
			counters = append(counters, &client.Counter{Name: "benchmark.tcp_messages_sent", Value: sent})
		}
		if mismatches > 0 {
			counters = append(counters, &client.Counter{Name: "benchmark.tcp_echo_mismatch", Value: mismatches})
		}
		global := &client.Result{
			Name:              "global",
			ExecutionDuration: durationpb.New(10 * time.Second),
			Counters:          counters,
		}
		set := &result.Set{Backends: []result.Backend{{Addr: "10.0.0.1:8443", Output: &client.Output{Results: []*client.Result{global}}, Global: global}}}
		r := &run.Report{Executions: []run.ExecutionReport{{
			Label: "raw", Pool: "nodes", Rate: 60, Duration: 10 * time.Second, Set: set,
			Backends: []string{"10.0.0.1:8443"},
		}}}
		var out bytes.Buffer
		if err := report.Text(&out, r); err != nil {
			t.Fatal(err)
		}
		return out.String()
	}
	if got := text(40, 9); !strings.Contains(got, "10.0.0.1:8443: 40 messages sent, 0 echoed in 10s") ||
		!strings.Contains(got, "10.0.0.1:8443: warning: 9 connection(s) closed on a reply that was not the message") {
		t.Errorf("the text report does not warn of the mismatches:\n%s", got)
	}
	if got := text(40, 0); strings.Contains(got, "warning") {
		t.Errorf("a run without mismatches is warned about:\n%s", got)
	}
	// A peer that speaks first: closed on its banner before anything is sent,
	// every time, so there is no sent counter to know the run as TCP by.
	if got := text(0, 9); !strings.Contains(got, "10.0.0.1:8443: 0 messages sent, 0 echoed in 10s") ||
		!strings.Contains(got, "10.0.0.1:8443: warning: 9 connection(s) closed on a reply that was not the message") {
		t.Errorf("the text report does not warn of mismatches when nothing was sent:\n%s", got)
	}
}

func TestReportsCarryPerBackendTotalsAndBackendErrors(t *testing.T) {
	backend := func(addr string, ok uint64) result.Backend {
		global := &client.Result{
			Name:              "global",
			ExecutionDuration: durationpb.New(10 * time.Second),
			Counters: []*client.Counter{
				{Name: "benchmark.http_2xx", Value: ok},
				{Name: "upstream_cx_total", Value: 2},
			},
		}
		return result.Backend{Addr: addr, Output: &client.Output{Results: []*client.Result{global}}, Global: global}
	}
	set := &result.Set{Backends: []result.Backend{backend("10.0.0.1:8443", 600), backend("10.0.0.2:8443", 599)}}
	r := &run.Report{Executions: []run.ExecutionReport{{
		Label: "soak", Pool: "nodes", Rate: 60, Duration: 10 * time.Second, Set: set,
		Backends:      []string{"10.0.0.1:8443", "10.0.0.2:8443", "10.0.0.3:8443"},
		BackendErrors: []run.BackendError{{Addr: "10.0.0.3:8443", Err: errors.New("connection refused")}},
	}}}

	var text bytes.Buffer
	if err := report.Text(&text, r); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text.String(), "10.0.0.3:8443: error: connection refused") {
		t.Errorf("the text report does not name the lost backend:\n%s", text.String())
	}
	if !strings.Contains(text.String(), "10.0.0.1:8443: 600 requests in 10s") {
		t.Errorf("the text report lost a surviving backend's line:\n%s", text.String())
	}

	var buf bytes.Buffer
	if err := report.JSON(&buf, r); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Executions []struct {
			Totals  map[string]uint64 `json:"totals"`
			Results []struct {
				Backend   string            `json:"backend"`
				ElapsedMS int64             `json:"elapsed_ms"`
				Counters  map[string]uint64 `json:"counters"`
			} `json:"results"`
			BackendErrors []struct {
				Backend string `json:"backend"`
				Error   string `json:"error"`
			} `json:"backend_errors"`
		} `json:"executions"`
	}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("%v\n%s", err, buf.String())
	}
	e := got.Executions[0]
	if e.Totals["benchmark.http_2xx"] != 1199 {
		t.Errorf("totals = %v, want http_2xx 1199", e.Totals)
	}
	if _, ok := e.Totals["upstream_cx_total"]; ok {
		t.Errorf("totals carry a counter that is not the engine's own: %v", e.Totals)
	}
	if len(e.Results) != 2 || e.Results[0].Backend != "10.0.0.1:8443" || e.Results[0].Counters["benchmark.http_2xx"] != 600 ||
		e.Results[1].Counters["benchmark.http_2xx"] != 599 || e.Results[0].ElapsedMS != 10000 {
		t.Errorf("results = %+v", e.Results)
	}
	if len(e.BackendErrors) != 1 || e.BackendErrors[0].Backend != "10.0.0.3:8443" || e.BackendErrors[0].Error != "connection refused" {
		t.Errorf("backend_errors = %+v", e.BackendErrors)
	}
}

// The JSON carries each backend's latency: the summary and the percentiles,
// resolved as thresholds resolve them, in nanoseconds. A statistic nothing
// was recorded into is left out, and so is a value the engine did not report.
func TestJSONCarriesPerBackendLatencyStatistics(t *testing.T) {
	pc := func(p float64, d time.Duration) *client.Percentile {
		return &client.Percentile{Percentile: p, DurationType: &client.Percentile_Duration{Duration: durationpb.New(d)}}
	}
	global := &client.Result{
		Name:              "global",
		ExecutionDuration: durationpb.New(10 * time.Second),
		Counters:          []*client.Counter{{Name: "benchmark.http_2xx", Value: 600}},
		Statistics: []*client.Statistic{
			{
				Id:       "benchmark_http_client.latency_2xx",
				Count:    600,
				MeanType: &client.Statistic_Mean{Mean: durationpb.New(2 * time.Millisecond)},
				MinType:  &client.Statistic_Min{Min: durationpb.New(time.Millisecond)},
				MaxType:  &client.Statistic_Max{Max: durationpb.New(9 * time.Millisecond)},
				Percentiles: []*client.Percentile{
					pc(0.5, 1700*time.Microsecond), pc(0.9, 2500*time.Microsecond),
					// The histogram's own buckets: nothing at exactly 0.99.
					pc(0.9902, 3300*time.Microsecond), pc(0.999, 8*time.Millisecond), pc(1, 9*time.Millisecond),
				},
			},
			{
				// A percentile the engine gave no value for.
				Id:          "benchmark_http_client.request_to_response",
				Count:       600,
				Percentiles: []*client.Percentile{{Percentile: 0.5}, pc(1, 9*time.Millisecond)},
			},
			{
				Id:       "benchmark_http_client.response_body_size",
				Count:    600,
				MeanType: &client.Statistic_RawMean{RawMean: 10},
			},
			{Id: "benchmark_http_client.latency_5xx", Count: 0},
		},
	}
	set := &result.Set{Backends: []result.Backend{{
		Addr: "10.0.0.1:8443", Output: &client.Output{Results: []*client.Result{global}}, Global: global,
	}}}
	r := &run.Report{Executions: []run.ExecutionReport{{
		Label: "soak", Pool: "nodes", Rate: 60, Duration: 10 * time.Second, Set: set,
		Backends: []string{"10.0.0.1:8443"},
	}}}

	var buf bytes.Buffer
	if err := report.JSON(&buf, r); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Executions []struct {
			Results []struct {
				Statistics map[string]map[string]any `json:"statistics"`
			} `json:"results"`
		} `json:"executions"`
	}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("%v\n%s", err, buf.String())
	}
	stats := got.Executions[0].Results[0].Statistics
	lat, ok := stats["benchmark_http_client.latency_2xx"]
	if !ok {
		t.Fatalf("no latency statistic in %v", stats)
	}
	for key, want := range map[string]any{
		"count": float64(600), "unit": "ns",
		"mean": 2e6, "min": 1e6, "max": 9e6,
		"p50": 1.7e6, "p90": 2.5e6, "p99": 3.3e6, "p99.9": 8e6,
	} {
		if lat[key] != want {
			t.Errorf("latency_2xx %s = %v, want %v", key, lat[key], want)
		}
	}
	if _, has := lat["pstdev"]; has {
		t.Errorf("pstdev was not reported by the engine and must be absent: %v", lat)
	}
	rtr := stats["benchmark_http_client.request_to_response"]
	if _, has := rtr["p50"]; has {
		t.Errorf("a percentile with no value must be absent, not zero: %v", rtr)
	}
	if rtr["p90"] != 9e6 {
		t.Errorf("request_to_response p90 = %v, want 9e6 (the next bucket with a value is not skipped)", rtr["p90"])
	}
	size := stats["benchmark_http_client.response_body_size"]
	if size["unit"] != "raw" || size["mean"] != float64(10) {
		t.Errorf("response_body_size = %v, want a raw mean of 10", size)
	}
	if _, has := stats["benchmark_http_client.latency_5xx"]; has {
		t.Errorf("an empty statistic was reported: %v", stats)
	}
}

// A stage that was never attempted -- the ones after a stage refused at an
// engine's execution cap -- is listed as skipped with the reason, in both
// renderings, and is not counted as an execution that failed.
func TestReportsMarkAStageThatWasNotRun(t *testing.T) {
	atCap := &run.CapError{Backend: "10.0.0.1:8443", Max: 16, Needed: 1, Err: errFake{}}
	r := &run.Report{Executions: []run.ExecutionReport{
		{Label: "ramp/stage-1", Pool: "local", Rate: 10, Duration: time.Second, Elapsed: 20 * time.Millisecond, Err: atCap},
		{Label: "ramp/stage-2", Pool: "local", Rate: 20, Duration: time.Second,
			Err: &run.NotRunError{Refused: "ramp/stage-1", Cap: atCap}},
	}}

	var text bytes.Buffer
	if err := report.Text(&text, r); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"FAIL   ramp/stage-1  (10 rps for 1s, pool \"local\", 20ms)",
		"SKIP   ramp/stage-2  (20 rps for 1s, pool \"local\")",
		"not run: ramp/stage-1 was refused because the engine on backend 10.0.0.1:8443 was at its cap of 16 concurrent executions",
		"FAIL  0/1 executions passed, 1 not run",
	} {
		if !strings.Contains(text.String(), want) {
			t.Errorf("the text report lacks %q:\n%s", want, text.String())
		}
	}

	var buf bytes.Buffer
	if err := report.JSON(&buf, r); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Executions []struct {
			Label     string `json:"label"`
			NotRun    bool   `json:"not_run"`
			Pass      bool   `json:"pass"`
			ElapsedMS int64  `json:"elapsed_ms"`
			Error     string `json:"error"`
		} `json:"executions"`
	}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Executions) != 2 {
		t.Fatalf("executions = %+v", got.Executions)
	}
	if first := got.Executions[0]; first.NotRun || first.Error == "" {
		t.Errorf("the refused stage = %+v, want an error and no not_run", first)
	}
	if second := got.Executions[1]; !second.NotRun || second.Pass || second.ElapsedMS != 0 ||
		!strings.Contains(second.Error, "not run: ramp/stage-1 was refused") {
		t.Errorf("the skipped stage = %+v", second)
	}
}
