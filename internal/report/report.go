// Package report renders run verdicts for humans and for CI.
package report

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/bpalermo/sortie/internal/compile"
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
		fmt.Fprintf(w, "       %s: %d requests in %s\n",
			b.Addr, counters["benchmark.http_2xx"]+counters["benchmark.http_3xx"]+
				counters["benchmark.http_4xx"]+counters["benchmark.http_5xx"],
			b.Global.GetExecutionDuration().AsDuration().Round(time.Millisecond))
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

func (p Progress) ExecutionFinished(r run.ExecutionReport) {}
