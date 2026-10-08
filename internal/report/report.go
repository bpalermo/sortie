// Package report renders run verdicts for humans and for CI.
package report

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	client "github.com/bpalermo/sortie/engine/api/client"
	"github.com/bpalermo/sortie/internal/compile"
	"github.com/bpalermo/sortie/internal/metric"
	"github.com/bpalermo/sortie/internal/nh"
	"github.com/bpalermo/sortie/internal/run"
)

// failureCounters are the engine's request failure classes, in the order they
// are printed. They overlap and must not be summed: a reset is in
// stream_resets and, for request/response load, in one stream_resets_<phase>
// and one stream_resets_<reason> counter; a failed unary gRPC call is in
// grpc_error as well as in what stopped it. The engine omits a counter that
// never incremented, so a clean run prints none of these. The last one is
// not a failure the engine saw: http_inflight_lost counts the requests that
// had no outcome when the execution was over, which is why a backend's
// responses can fall short of its requests with every other class at zero.
var failureCounters = []string{
	"benchmark.http_4xx",
	"benchmark.http_5xx",
	"benchmark.grpc_error",
	"benchmark.stream_resets",
	"benchmark.stream_resets_before_headers",
	"benchmark.stream_resets_incomplete_body",
	"benchmark.pool_overflow",
	"benchmark.pool_connection_failure",
	"benchmark.pool_failure_local_connection_failure",
	"benchmark.pool_failure_remote_connection_failure",
	"benchmark.pool_failure_timeout",
	"benchmark.http_inflight_lost",
}

// failures lists a backend's non-zero failure counters: the classes above in
// their order, then any stream_resets_<reason> counters in the order the
// engine reported them. Nil when the backend had none.
func failures(counters []*client.Counter) []*client.Counter {
	byName := map[string]*client.Counter{}
	for _, c := range counters {
		byName[c.GetName()] = c
	}
	var out []*client.Counter
	listed := map[string]bool{}
	for _, name := range failureCounters {
		listed[name] = true
		if c, ok := byName[name]; ok && c.GetValue() > 0 {
			out = append(out, c)
		}
	}
	for _, c := range counters {
		if !listed[c.GetName()] && strings.HasPrefix(c.GetName(), "benchmark.stream_resets_") && c.GetValue() > 0 {
			out = append(out, c)
		}
	}
	return out
}

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

	passed, notRun := 0, 0
	for _, e := range r.Executions {
		if e.Pass {
			passed++
		}
		if e.NotRun() {
			notRun++
		}
	}
	fmt.Fprintln(w)
	verdict := "FAIL"
	if r.Pass {
		verdict = "PASS"
	}
	if notRun > 0 {
		// Counted apart: they did not fail, they were never attempted.
		fmt.Fprintf(w, "%s  %d/%d executions passed, %d not run\n", verdict, passed, len(r.Executions)-notRun, notRun)
		return nil
	}
	fmt.Fprintf(w, "%s  %d/%d executions passed\n", verdict, passed, len(r.Executions))
	return nil
}

func execution(w io.Writer, e run.ExecutionReport) error {
	rps := rateUnit(e.PerBackend)
	shape := fmt.Sprintf("%d %s for %s", e.Rate, rps, e.Duration)
	if e.RampTime > 0 {
		shape = fmt.Sprintf("ramp to %d %s over %s, then hold for %s",
			e.Rate, rps, e.RampTime, e.Duration-e.RampTime)
	}
	if e.NotRun() {
		// Never attempted: no elapsed time to show, and not a FAIL of its
		// own -- the stage that was refused is the failure.
		fmt.Fprintf(w, "%-6s %s  (%s, pool %q)\n", "SKIP", e.Label, shape, e.Pool)
		fmt.Fprintf(w, "       %v\n", e.Err)
		return nil
	}
	// When it started, last: the line reads as before up to there, and a run
	// of hours is matched to what else happened by this.
	started := ""
	if at := timestamp(e.Started); at != "" {
		started = ", started " + at
	}
	fmt.Fprintf(w, "%-6s %s  (%s, pool %q, %s%s)\n",
		status(e.Pass), e.Label, shape, e.Pool, e.Elapsed.Round(time.Millisecond), started)

	// A dns pool's backends were decided when the run started; this is the
	// only record of which ones they were.
	if e.Dns != "" {
		fmt.Fprintf(w, "       %s resolved to %s\n", e.Dns, strings.Join(e.Backends, ", "))
	}

	if e.Err != nil {
		fmt.Fprintf(w, "       error: %v\n", e.Err)
		return nil
	}

	// Backends that did not finish cleanly, before the results: the counts
	// below are then read knowing which backends they do not include, or
	// which of them stopped early.
	for _, be := range e.BackendErrors {
		fmt.Fprintf(w, "       %s: error: %v\n", be.Addr, be.Err)
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
		if sent, ok := counters["benchmark.udp_datagrams_sent"]; ok {
			fmt.Fprintf(w, "       %s: %d datagrams sent, %d echoed, %d lost in %s\n",
				b.Addr, sent, counters["benchmark.udp_datagrams_received"], counters["benchmark.udp_lost"], elapsed)
			continue
		}
		// A counter at zero is not in the results, so a TCP run that sent
		// nothing -- a peer that speaks first is closed on its banner, every
		// time -- is known by its mismatches alone.
		sent, tcp := counters["benchmark.tcp_messages_sent"]
		if mismatches := counters["benchmark.tcp_echo_mismatch"]; tcp || mismatches > 0 {
			fmt.Fprintf(w, "       %s: %d messages sent, %d echoed in %s\n",
				b.Addr, sent, counters["benchmark.tcp_messages_received"], elapsed)
			// Said here, not left to a threshold someone may not have
			// written: the echoed count and the latency of such a run cover
			// next to nothing, and look like a slow target if not explained.
			if mismatches > 0 {
				fmt.Fprintf(w, "       %s: warning: %d connection(s) closed on a reply that was not the message: "+
					"the target is not an exact echo, and only exact echoes are counted and timed "+
					"(tcp.expect_echo: false sends without expecting one)\n", b.Addr, mismatches)
			}
			continue
		}
		fmt.Fprintf(w, "       %s: %d requests in %s%s\n",
			b.Addr, counters["benchmark.http_2xx"]+counters["benchmark.http_3xx"]+
				counters["benchmark.http_4xx"]+counters["benchmark.http_5xx"], elapsed,
			failureSuffix(b.Global.GetCounters()))
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

// failureSuffix is the per-backend failure breakdown, e.g.
// "  (stream_resets 3, stream_resets_incomplete_body 3, stream_resets_remote_reset 3)",
// counter names without their "benchmark." prefix; empty when the backend had
// no failures, so a clean run's line is unchanged.
func failureSuffix(counters []*client.Counter) string {
	fs := failures(counters)
	if len(fs) == 0 {
		return ""
	}
	parts := make([]string, 0, len(fs))
	for _, c := range fs {
		parts = append(parts, fmt.Sprintf("%s %d", strings.TrimPrefix(c.GetName(), "benchmark."), c.GetValue()))
	}
	return "  (" + strings.Join(parts, ", ") + ")"
}

// rateUnit names what a rate counts: the pool's requests per second, or --
// with per_backend -- each backend's.
func rateUnit(perBackend bool) string {
	if perBackend {
		return "rps per backend"
	}
	return "rps"
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
	Label      string `json:"label"`
	Scenario   string `json:"scenario"`
	Pool       string `json:"pool"`
	Rate       uint32 `json:"rate"`
	PerBackend bool   `json:"per_backend,omitempty"`
	DurationMS int64  `json:"duration_ms"`
	RampTimeMS int64  `json:"ramp_time_ms,omitempty"`
	ElapsedMS  int64  `json:"elapsed_ms"`
	// StartedAt is when sortie began dispatching the execution to its
	// backends and EndedAt when the last of them had answered or been given
	// up on -- StartedAt plus the elapsed time, both by this process's clock,
	// RFC 3339 in UTC. That brackets the load rather than timing it: a
	// backend is dialled, and may be told to wait for a scheduled start,
	// inside it. Closer to when the load began is the started_at of each
	// backend's entry in Results. Absent when the report carries no start
	// time.
	StartedAt string `json:"started_at,omitempty"`
	EndedAt   string `json:"ended_at,omitempty"`
	Pass      bool   `json:"pass"`
	// NotRun marks an execution that was never attempted -- a stage after one
	// refused at an engine's execution cap. Error says why; elapsed_ms is 0.
	NotRun bool `json:"not_run,omitempty"`
	// Refused is "execution_cap" for an execution of a stage that was stopped
	// because an engine refused one of its starts at its execution cap: it
	// did not fail on its own account, and Error is not the only way to tell.
	// The stages after it, which were not attempted, have NotRun instead.
	Refused    string          `json:"refused,omitempty"`
	Error      string          `json:"error,omitempty"`
	Dns        string          `json:"dns,omitempty"` // the name Backends were resolved from, for a dns pool
	Backends   []string        `json:"backends,omitempty"`
	Thresholds []jsonThreshold `json:"thresholds,omitempty"`
	// Failures is present only when some backend reported a non-zero failure
	// class, so a clean run's JSON is unchanged.
	Failures []jsonBackendFailures `json:"failures,omitempty"`
	// Totals are the pool's counters, summed over the backends that returned
	// results -- what counter and rate thresholds are judged against -- and
	// Results are the same per backend. Only the engine's own benchmark.*
	// counters: Envoy's cluster counters are in the engine's output, not here.
	Totals  map[string]uint64   `json:"totals,omitempty"`
	Results []jsonBackendResult `json:"results,omitempty"`
	// BackendErrors are the backends that did not finish cleanly. One listed
	// here and absent from Results returned nothing.
	BackendErrors []jsonBackendError `json:"backend_errors,omitempty"`
}

// jsonBackendResult is what one backend counted and measured.
type jsonBackendResult struct {
	Backend   string `json:"backend"`
	ElapsedMS int64  `json:"elapsed_ms"`
	// StartedAt is when the first of the backend's workers started its rate
	// limiter's clock, by the engine's own clock: just before a request is
	// asked for, and so not evidence that one was sent. Absent when the
	// engine supplied no timestamp.
	StartedAt string            `json:"started_at,omitempty"`
	Counters  map[string]uint64 `json:"counters"`
	// Statistics are the backend's statistics that recorded anything, by id:
	// its latencies above all. There is no pool-wide entry: a percentile over
	// a pool cannot be had from the backends' percentiles, which is also why
	// a latency threshold is judged per backend.
	Statistics map[string]jsonStatistic `json:"statistics,omitempty"`
}

// jsonStatistic is one statistic's summary and the percentiles a report is
// usually read for. Unit says what the numbers are: "ns" for a duration,
// "raw" for a plain quantity such as a response size. A value the engine did
// not report is absent rather than zero.
//
// A percentile is the first of the histogram's own buckets at or above it --
// the rule thresholds are resolved by, so a p99 here is the p99 a threshold
// on the same statistic was judged against.
type jsonStatistic struct {
	Count  uint64   `json:"count"`
	Unit   string   `json:"unit"`
	Mean   *float64 `json:"mean,omitempty"`
	Pstdev *float64 `json:"pstdev,omitempty"`
	Min    *float64 `json:"min,omitempty"`
	Max    *float64 `json:"max,omitempty"`
	P50    *float64 `json:"p50,omitempty"`
	P90    *float64 `json:"p90,omitempty"`
	P99    *float64 `json:"p99,omitempty"`
	P999   *float64 `json:"p99.9,omitempty"`
}

// statistics summarises a result's statistics for the JSON report, through
// the same resolver thresholds use.
func statistics(result *client.Result) map[string]jsonStatistic {
	out := map[string]jsonStatistic{}
	for _, st := range result.GetStatistics() {
		if st.GetCount() == 0 || st.GetId() == "" {
			continue
		}
		js := jsonStatistic{Count: st.GetCount(), Unit: "raw"}
		for _, field := range []struct {
			name string
			dst  **float64
		}{
			{"mean", &js.Mean}, {"pstdev", &js.Pstdev}, {"min", &js.Min}, {"max", &js.Max},
			{"p50", &js.P50}, {"p90", &js.P90}, {"p99", &js.P99}, {"p99.9", &js.P999},
		} {
			sel, err := metric.ParseSelector(st.GetId() + "." + field.name)
			if err != nil {
				continue
			}
			v, err := metric.Resolve(result, sel)
			if err != nil {
				// Not reported by the engine for this statistic: left out.
				continue
			}
			if v.IsDuration {
				js.Unit = "ns"
			}
			num := v.Num
			*field.dst = &num
		}
		out[st.GetId()] = js
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

type jsonBackendError struct {
	Backend string `json:"backend"`
	Error   string `json:"error"`
	// Refused is "execution_cap" for a backend that did not fail but refused
	// the start, its engine being at its execution cap: not a backend lost.
	Refused string `json:"refused,omitempty"`
}

// refusedAtCap is the value of "refused" for a start an engine turned down at
// its execution cap.
const refusedAtCap = "execution_cap"

// benchmarkCounters are a result's benchmark.* counters by name.
func benchmarkCounters(counters []*client.Counter) map[string]uint64 {
	out := map[string]uint64{}
	for _, c := range counters {
		if strings.HasPrefix(c.GetName(), "benchmark.") {
			out[c.GetName()] = c.GetValue()
		}
	}
	return out
}

// jsonBackendFailures is one backend's non-zero failure counters, keyed by
// the engine's full counter name.
type jsonBackendFailures struct {
	Backend  string            `json:"backend"`
	Counters map[string]uint64 `json:"counters"`
}

type jsonThreshold struct {
	Expr   string `json:"expr"`
	Scope  string `json:"scope"`
	Pass   bool   `json:"pass"`
	Actual string `json:"actual"`
}

// timeLayout is RFC 3339 in UTC with milliseconds, always three digits: the
// precision elapsed_ms has, and a fixed width, so the timestamps of a report
// sort as text.
const timeLayout = "2006-01-02T15:04:05.000Z"

// timestamp renders a time for a report; empty for the zero time, which is a
// time nobody recorded and must not be printed as the year 1.
func timestamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(timeLayout)
}

// JSON writes a machine-readable report.
func JSON(w io.Writer, r *run.Report) error {
	out := jsonReport{Pass: r.Pass}
	for _, e := range r.Executions {
		out.Executions = append(out.Executions, newJSONExecution(e))
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

// newJSONExecution is one execution in the machine-readable shape.
func newJSONExecution(e run.ExecutionReport) jsonExecution {
	je := jsonExecution{
		Label:      e.Label,
		Scenario:   e.Scenario,
		Pool:       e.Pool,
		Rate:       e.Rate,
		PerBackend: e.PerBackend,
		DurationMS: e.Duration.Milliseconds(),
		RampTimeMS: e.RampTime.Milliseconds(),
		ElapsedMS:  e.Elapsed.Milliseconds(),
		StartedAt:  timestamp(e.Started),
		Pass:       e.Pass,
		NotRun:     e.NotRun(),
		Dns:        e.Dns,
		Backends:   append([]string(nil), e.Backends...),
	}
	// An end is a start plus what was measured from it: with no start there
	// is no end to give either. It is made of the two as they are printed,
	// to the millisecond, so that ended_at is started_at plus elapsed_ms
	// exactly and not a millisecond off by rounding.
	if !e.Started.IsZero() {
		je.EndedAt = timestamp(e.Started.Truncate(time.Millisecond).Add(e.Elapsed.Truncate(time.Millisecond)))
	}
	if e.Err != nil {
		je.Error = e.Err.Error()
	}
	var atCap *run.CapError
	if !e.NotRun() && errors.As(e.Err, &atCap) {
		je.Refused = refusedAtCap
	}
	for _, be := range e.BackendErrors {
		jb := jsonBackendError{Backend: be.Addr, Error: be.Err.Error()}
		var busy *nh.BusyError
		if errors.As(be.Err, &busy) {
			jb.Refused = refusedAtCap
		}
		je.BackendErrors = append(je.BackendErrors, jb)
	}
	if e.Set != nil {
		je.Totals = benchmarkCounters(e.Set.Totals().GetCounters())
		// The dispatch list is normally there; only a report assembled
		// without it, as some tests do, takes its backends from the results.
		fromSet := je.Backends == nil
		for _, b := range e.Set.Backends {
			if fromSet {
				je.Backends = append(je.Backends, b.Addr)
			}
			br := jsonBackendResult{
				Backend:    b.Addr,
				ElapsedMS:  b.Global.GetExecutionDuration().AsDuration().Milliseconds(),
				Counters:   benchmarkCounters(b.Global.GetCounters()),
				Statistics: statistics(b.Global),
			}
			if start := b.Global.GetExecutionStart(); start != nil {
				br.StartedAt = timestamp(start.AsTime())
			}
			je.Results = append(je.Results, br)
			if fs := failures(b.Global.GetCounters()); len(fs) > 0 {
				bf := jsonBackendFailures{Backend: b.Addr, Counters: map[string]uint64{}}
				for _, c := range fs {
					bf.Counters[c.GetName()] = c.GetValue()
				}
				je.Failures = append(je.Failures, bf)
			}
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
	return je
}

// Progress is a run.Observer that narrates a run on the way through, since a
// Nighthawk execution produces no output at all until it finishes.
type Progress struct{ W io.Writer }

func (p Progress) ExecutionStarted(e compile.Execution, backends []string) {
	rps := rateUnit(e.PerBackend)
	shape := fmt.Sprintf("%d %s for %s", e.Rate, rps, e.Duration)
	if e.RampTime > 0 {
		shape = fmt.Sprintf("ramp to %d %s over %s, total %s", e.Rate, rps, e.RampTime, e.Duration)
	}
	fmt.Fprintf(p.W, "  running %s (%s) on %s\n", e.Label, shape, strings.Join(backends, ", "))
}

// ExecutionProgress prints one line per snapshot. The label comes first: the
// targets of a weighted scenario run at once on the same backend, and a line
// naming only the address would not say which of them it is about.
func (p Progress) ExecutionProgress(e compile.Execution, backend string, elapsed time.Duration, out *client.Output) {
	fmt.Fprintf(p.W, "    %s  %s  %s  %s\n", e.Label, backend, elapsed.Truncate(100*time.Millisecond), snapshotSummary(out))
}

// ExecutionFinished prints the execution's verdict, whatever the progress
// interval: a run of hundreds of stages is hours long, and a stage that
// failed in the first of them should be in the log then, not in a report
// that is written when the last one ends -- or never, if the run dies first.
func (p Progress) ExecutionFinished(r run.ExecutionReport) {
	fmt.Fprintf(p.W, "  %s\n", verdictLine(r))
}

// verdictLine is one finished execution on one line: the verdict, the label
// and scenario, how long it took and, for a failure, everything that failed
// it -- the execution's own error, each backend that did not finish cleanly,
// and each threshold that did not hold with the values observed. Thresholds
// that held are left to the report.
func verdictLine(r run.ExecutionReport) string {
	verdict := "FAIL"
	if r.Pass {
		verdict = "PASS"
	}
	if r.NotRun() {
		// Never attempted: not a failure of its own, and no time to show.
		return oneLine.Replace(fmt.Sprintf("SKIP %s (scenario %s): %v", r.Label, r.Scenario, r.Err))
	}
	// One line whatever the names hold: a scenario's name is any nonempty
	// string, and a verdict that wraps is not found by a grep for it.
	line := oneLine.Replace(fmt.Sprintf("%s %s (scenario %s, %s)", verdict, r.Label, r.Scenario, r.Elapsed.Round(time.Millisecond)))

	var why []string
	if r.Err != nil {
		why = append(why, fmt.Sprintf("error: %v", r.Err))
	}
	for _, be := range r.BackendErrors {
		why = append(why, fmt.Sprintf("%s: error: %v", be.Addr, be.Err))
	}
	for _, o := range r.Outcomes {
		if !o.Pass {
			why = append(why, fmt.Sprintf("%s actual %s", o.Threshold.Raw, o.Detail()))
		}
	}
	if len(why) == 0 {
		return line
	}
	// One line whatever the errors hold: several backends' errors joined are
	// one to a line, and a verdict that wraps is not found by a grep for it.
	return line + ": " + oneLine.Replace(strings.Join(why, "; "))
}

var oneLine = strings.NewReplacer("\r\n", "; ", "\n", "; ", "\r", " ")

// Stream is a run.Observer that writes every execution as it finishes, as one
// line of JSON: JSON Lines, each line the object the execution will be in the
// final report's "executions", built by the same code. It is the record of a
// long run that exists before the run is over.
//
// A line goes out in a single Write, newline included, so a reader following
// the file sees whole lines or, at worst, a last one still without its
// newline. A line that cannot be written is passed to Failed and the run goes
// on: the stream is a copy, and losing it is not a reason to lose the run.
type Stream struct {
	W io.Writer
	// Failed, when set, is told of each line that could not be written.
	Failed func(label string, err error)

	// torn is set by a write that failed: it may have left part of a line
	// behind -- a disk that filled does -- and the next line would be joined
	// to that part and lost with it.
	torn bool
}

func (s *Stream) ExecutionStarted(compile.Execution, []string) {}

func (s *Stream) ExecutionProgress(compile.Execution, string, time.Duration, *client.Output) {}

func (s *Stream) ExecutionFinished(r run.ExecutionReport) {
	line, err := json.Marshal(newJSONExecution(r))
	if err == nil {
		line = append(line, '\n')
		if s.torn {
			// Close whatever the failed write left, so that it is a line of
			// its own that does not parse and this one is whole.
			line = append([]byte{'\n'}, line...)
		}
		var n int
		n, err = s.W.Write(line)
		// A write that took nothing leaves the stream as it was.
		s.torn = err != nil && (n > 0 || s.torn)
	}
	if err != nil && s.Failed != nil {
		s.Failed(r.Label, err)
	}
}

// Observers is a run.Observer that passes every callback to each of its
// members, in order.
type Observers []run.Observer

func (obs Observers) ExecutionStarted(e compile.Execution, backends []string) {
	for _, o := range obs {
		o.ExecutionStarted(e, backends)
	}
}

func (obs Observers) ExecutionProgress(e compile.Execution, backend string, elapsed time.Duration, out *client.Output) {
	for _, o := range obs {
		o.ExecutionProgress(e, backend, elapsed, out)
	}
}

func (obs Observers) ExecutionFinished(r run.ExecutionReport) {
	for _, o := range obs {
		o.ExecutionFinished(r)
	}
}

// snapshotSummary is one line from an interim Output: responses by class, the
// failure counters that explain a missing class, and the latency so far of
// whichever statistic the run records.
//
// By default the engine's snapshots carry each statistic's summary and no
// percentiles (a percentile needs a copy of every worker's histogram, which is
// what made snapshots expensive), so the line gives the mean and the max. A
// p99 is printed only when the snapshot has percentiles to read it from --
// never estimated from the summary.
// progressLatency is the latency statistic each client mode records: HTTP and
// unary gRPC, the stream modes (gRPC bidi, WebSocket), raw TCP and UDP. A run
// records one of them.
var progressLatency = map[string]bool{
	"benchmark_http_client.request_to_response": true,
	"benchmark_stream.message_latency":          true,
	"benchmark_tcp.message_latency":             true,
	"benchmark_udp.message_latency":             true,
}

func snapshotSummary(out *client.Output) string {
	global, err := metric.GlobalResult(out)
	if err != nil {
		return "no results yet"
	}
	var parts []string
	for _, c := range global.GetCounters() {
		switch c.GetName() {
		case "benchmark.http_2xx", "benchmark.http_3xx", "benchmark.http_4xx", "benchmark.http_5xx",
			"benchmark.pool_overflow", "benchmark.stream_resets", "benchmark.pool_connection_failure",
			"benchmark.stream_resets_before_headers", "benchmark.stream_resets_incomplete_body",
			"benchmark.pool_failure_timeout",
			// What the stream, TCP and UDP modes count in place of responses.
			"benchmark.stream_messages_sent", "benchmark.stream_messages_received",
			"benchmark.tcp_messages_sent", "benchmark.tcp_messages_received",
			"benchmark.udp_datagrams_sent", "benchmark.udp_datagrams_received", "benchmark.udp_lost":
			parts = append(parts, fmt.Sprintf("%s %d", strings.TrimPrefix(c.GetName(), "benchmark."), c.GetValue()))
		}
	}
	for _, st := range global.GetStatistics() {
		if !progressLatency[st.GetId()] {
			continue
		}
		// A statistic nothing was recorded into has a zero mean and max that
		// are not measurements.
		if st.GetCount() > 0 && st.GetMean() != nil && st.GetMax() != nil {
			parts = append(parts, fmt.Sprintf("mean %s  max %s", st.GetMean().AsDuration(), st.GetMax().AsDuration()))
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
