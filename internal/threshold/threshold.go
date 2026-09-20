// Package threshold parses and evaluates sortie's pass/fail assertions against
// a Nighthawk Result.
package threshold

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	client "github.com/envoyproxy/nighthawk/api/client"
	"github.com/bpalermo/sortie/internal/metric"
)

// Op is a comparison operator.
type Op string

const (
	LT Op = "<"
	LE Op = "<="
	GT Op = ">"
	GE Op = ">="
	EQ Op = "=="
	NE Op = "!="
)

// ops is ordered longest-first so that "<=" is not mistaken for "<".
var ops = []Op{LE, GE, EQ, NE, LT, GT}

// Threshold is one parsed assertion, such as "latency_2xx.p95 < 500ms".
type Threshold struct {
	Raw      string
	Selector metric.Selector
	Op       Op

	// Value is nanoseconds when IsDuration is set, and a plain number otherwise.
	Value      float64
	IsDuration bool
	ValueText  string
}

func (t Threshold) String() string { return t.Raw }

// Parse reads an assertion of the form "<metric> <op> <value>".
//
// The value is a Go duration ("500ms", "1s") for duration-valued metrics such
// as latency percentiles, and a plain number for counters and rates.
func Parse(expr string) (Threshold, error) {
	raw := strings.TrimSpace(expr)
	if raw == "" {
		return Threshold{}, fmt.Errorf("empty threshold")
	}

	for _, op := range ops {
		idx := strings.Index(raw, string(op))
		if idx < 0 {
			continue
		}
		lhs := strings.TrimSpace(raw[:idx])
		rhs := strings.TrimSpace(raw[idx+len(op):])
		if lhs == "" || rhs == "" {
			return Threshold{}, fmt.Errorf("%q: expected <metric> %s <value>", raw, op)
		}
		sel, err := metric.ParseSelector(lhs)
		if err != nil {
			return Threshold{}, err
		}
		t := Threshold{Raw: raw, Selector: sel, Op: op, ValueText: rhs}
		// Numbers are tried first because time.ParseDuration accepts a bare
		// "0", and "counter:... == 0" must not be read as a duration.
		if n, err := strconv.ParseFloat(rhs, 64); err == nil {
			t.Value = n
			return t, nil
		}
		d, err := time.ParseDuration(rhs)
		if err != nil {
			return Threshold{}, fmt.Errorf(
				"%q: value %q is neither a number nor a duration (500ms)", raw, rhs)
		}
		t.Value = float64(d.Nanoseconds())
		t.IsDuration = true
		return t, nil
	}
	return Threshold{}, fmt.Errorf(
		"%q: no comparison operator found (want one of <, <=, >, >=, ==, !=)", raw)
}

// ParseAll parses every expression, reporting all failures at once so a plan
// with several typos does not have to be fixed one run at a time.
func ParseAll(exprs []string) ([]Threshold, error) {
	out := make([]Threshold, 0, len(exprs))
	var errs []string
	for _, e := range exprs {
		t, err := Parse(e)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		out = append(out, t)
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return out, nil
}

// Outcome is the result of evaluating one threshold.
type Outcome struct {
	Threshold Threshold
	Actual    metric.Value
	Pass      bool

	// Err is set when the metric could not be resolved at all, which counts as
	// a failure: a threshold naming a metric that does not exist has not been
	// satisfied, it has gone unchecked.
	Err error
}

// ActualText renders the observed value in the same units as the threshold.
func (o Outcome) ActualText() string {
	if o.Actual.IsDuration {
		return time.Duration(int64(o.Actual.Num)).String()
	}
	return strconv.FormatFloat(o.Actual.Num, 'f', -1, 64)
}

// Evaluate resolves the threshold's metric and applies its comparison.
func (t Threshold) Evaluate(result *client.Result) Outcome {
	v, err := metric.Resolve(result, t.Selector)
	if err != nil {
		return Outcome{Threshold: t, Err: err}
	}
	if v.IsDuration != t.IsDuration {
		if v.IsDuration {
			return Outcome{Threshold: t, Actual: v, Err: fmt.Errorf(
				"%s is a duration; write the threshold as a duration, e.g. %s %s 500ms",
				t.Selector, t.Selector, t.Op)}
		}
		return Outcome{Threshold: t, Actual: v, Err: fmt.Errorf(
			"%s is not a duration; write the threshold as a plain number, e.g. %s %s 0",
			t.Selector, t.Selector, t.Op)}
	}
	return Outcome{Threshold: t, Actual: v, Pass: compare(v.Num, t.Op, t.Value)}
}

// EvaluateAll evaluates every threshold and reports whether all of them held.
func EvaluateAll(result *client.Result, ts []Threshold) ([]Outcome, bool) {
	outcomes := make([]Outcome, 0, len(ts))
	allPass := true
	for _, t := range ts {
		o := t.Evaluate(result)
		if !o.Pass {
			allPass = false
		}
		outcomes = append(outcomes, o)
	}
	return outcomes, allPass
}

func compare(actual float64, op Op, want float64) bool {
	switch op {
	case LT:
		return actual < want
	case LE:
		return actual <= want
	case GT:
		return actual > want
	case GE:
		return actual >= want
	case EQ:
		return actual == want
	case NE:
		return actual != want
	}
	return false
}
