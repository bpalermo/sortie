// Package report renders run verdicts for humans and for CI.
package report

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	client "github.com/bpalermo/sortie/engine/api/client"
	"github.com/bpalermo/sortie/internal/compile"
	"github.com/bpalermo/sortie/internal/metric"
	"github.com/bpalermo/sortie/internal/run"
)

// Text writes a human-readable report.
func Text(w io.Writer, r *run.Report) error {
	for i, e := range r.Executions {
		if i > 0 {
			fmt.Fprintln(w)
		}
		if err := execution(w, e); err != nil {
			return err
		}
	}

	passed := 0
	for _, e := range r.Executions {
		if e.Pass {
			passed++
		}
	}
	fmt.Fprintln(w)
	verdict := "FAIL"
	if r.Pass {
		verdict = "PASS"
	}
	fmt.Fprintf(w, "%s  %d/%d executions passed\n", verdict, passed, len(r.Executions))
	return nil
}

func execution(w io.Writer, e run.ExecutionReport) error {
	shape := fmt.Sprintf("%d rps for %s", e.Rate, e.Duration)
	if e.RampTime > 0 {
		shape = fmt.Sprintf("ramp to %d rps over %s, then hold for %s",
			e.Rate, e.RampTime, e.Duration-e.RampTime)
	}
	fmt.Fprintf(w, "%-6s %s  (%s, pool %q, %s)\n",
		status(e.Pass), e.Label, shape, e.Pool, e.Elapsed.Round(time.Millisecond))

	if e.Err != nil {
		fmt.Fprintf(w, "       error: %v\n", e.Err)
		return nil
	}

	for _, b := range e.Set.Backends {
		counters := map[string]uint64{}
		for _, c := range b.Global.GetCounters() {
			counters[c.GetName()] = c.GetValue()
		}
		elapsed := b.Global.GetExecutionDuration().AsDuration().Round(time.Millisecond)
		if sent, ok := counters["benchmark.stream_messages_sent"]; ok {
			// A stream run (gRPC bidi-stream, WebSocket): messages and their echoes,
			// not requests.
			fmt.Fprintf(w, "       %s: %d messages sent, %d echoed in %s\n",
				b.Addr, sent, counters["benchmark.stream_messages_received"], elapsed)
			continue
		}
		if sent, ok := counters["benchmark.tcp_messages_sent"]; ok {
			fmt.Fprintf(w, "       %s: %d messages sent, %d echoed in %s\n",
				b.Addr, sent, counters["benchmark.tcp_messages_received"], elapsed)
			continue
		}
		fmt.Fprintf(w, "       %s: %d requests in %s\n",
			b.Addr, counters["benchmark.http_2xx"]+counters["benchmark.http_3xx"]+
				counters["benchmark.http_4xx"]+counters["benchmark.http_5xx"], elapsed)
	}

	if len(e.Outcomes) == 0 {
		fmt.Fprintf(w, "       (no thresholds declared)\n")
		return nil
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for _, o := range e.Outcomes {
		fmt.Fprintf(tw, "       %s\t%s\tactual %s\t(%s)\n",
			status(o.Pass), o.Threshold.Raw, o.Detail(), o.Scope)
	}
	return tw.Flush()
}

func status(pass bool) string {
	if pass {
		return "  ok"
	}
	return "FAIL"
}

// jsonReport is the machine-readable shape. It is deliberately separate from
// the internal types so that refactoring those does not silently change a
// format that CI parses.
type jsonReport struct {
	Pass       bool            `json:"pass"`
	Executions []jsonExecution `json:"executions"`
}

type jsonExecution struct {
	Label      string          `json:"label"`
	Scenario   string          `json:"scenario"`
	Pool       string          `json:"pool"`
	Rate       uint32          `json:"rate"`
	DurationMS int64           `json:"duration_ms"`
	RampTimeMS int64           `json:"ramp_time_ms,omitempty"`
	ElapsedMS  int64           `json:"elapsed_ms"`
	Pass       bool            `json:"pass"`
	Error      string          `json:"error,omitempty"`
	Backends   []string        `json:"backends,omitempty"`
	Thresholds []jsonThreshold `json:"thresholds,omitempty"`
}

type jsonThreshold struct {
	Expr   string `json:"expr"`
	Scope  string `json:"scope"`
	Pass   bool   `json:"pass"`
	Actual string `json:"actual"`
}

// JSON writes a machine-readable report.
func JSON(w io.Writer, r *run.Report) error {
	out := jsonReport{Pass: r.Pass}
	for _, e := range r.Executions {
		je := jsonExecution{
			Label:      e.Label,
			Scenario:   e.Scenario,
			Pool:       e.Pool,
			Rate:       e.Rate,
			DurationMS: e.Duration.Milliseconds(),
			RampTimeMS: e.RampTime.Milliseconds(),
			ElapsedMS:  e.Elapsed.Milliseconds(),
			Pass:       e.Pass,
		}
		if e.Err != nil {
			je.Error = e.Err.Error()
		}
		if e.Set != nil {
			for _, b := range e.Set.Backends {
				je.Backends = append(je.Backends, b.Addr)
			}
		}
		for _, o := range e.Outcomes {
			je.Thresholds = append(je.Thresholds, jsonThreshold{
				Expr:   o.Threshold.Raw,
				Scope:  o.Scope,
				Pass:   o.Pass,
				Actual: o.Detail(),
			})
		}
		out.Executions = append(out.Executions, je)
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

// Progress is a run.Observer that narrates a run on the way through, since a
// Nighthawk execution produces no output at all until it finishes.
type Progress struct{ W io.Writer }

func (p Progress) ExecutionStarted(e compile.Execution, backends []string) {
	shape := fmt.Sprintf("%d rps for %s", e.Rate, e.Duration)
	if e.RampTime > 0 {
		shape = fmt.Sprintf("ramp to %d rps over %s, total %s", e.Rate, e.RampTime, e.Duration)
	}
	fmt.Fprintf(p.W, "  running %s (%s) on %s\n", e.Label, shape, strings.Join(backends, ", "))
}

func (p Progress) ExecutionProgress(e compile.Execution, backend string, elapsed time.Duration, out *client.Output) {
	fmt.Fprintf(p.W, "    %s  %s  %s\n", backend, elapsed.Truncate(100*time.Millisecond), snapshotSummary(out))
}

func (p Progress) ExecutionFinished(r run.ExecutionReport) {}

// snapshotSummary is one line from an interim Output: responses by class, the
// failure counters that explain a missing class, and the p99 of whichever
// latency statistic the run records.
func snapshotSummary(out *client.Output) string {
	global, err := metric.GlobalResult(out)
	if err != nil {
		return "no results yet"
	}
	var parts []string
	for _, c := range global.GetCounters() {
		switch c.GetName() {
		case "benchmark.http_2xx", "benchmark.http_3xx", "benchmark.http_4xx", "benchmark.http_5xx",
			"benchmark.pool_overflow", "benchmark.stream_resets", "benchmark.pool_connection_failure":
			parts = append(parts, fmt.Sprintf("%s %d", strings.TrimPrefix(c.GetName(), "benchmark."), c.GetValue()))
		}
	}
	for _, st := range global.GetStatistics() {
		if st.GetId() != "benchmark_http_client.request_to_response" && st.GetId() != "benchmark_stream.message_latency" {
			continue
		}
		// Nighthawk reports its histogram's own buckets, so p99 is the first
		// bucket at or above 0.99 -- the rule the threshold resolver applies.
		var best *client.Percentile
		for _, pc := range st.GetPercentiles() {
			if pc.GetPercentile() < 0.99 || pc.GetDuration() == nil {
				continue
			}
			if best == nil || pc.GetPercentile() < best.GetPercentile() {
				best = pc
			}
		}
		if best != nil {
			parts = append(parts, fmt.Sprintf("p99 %s", best.GetDuration().AsDuration()))
		}
	}
	if len(parts) == 0 {
		return "no responses yet"
	}
	return strings.Join(parts, "  ")
}
