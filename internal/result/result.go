// Package result aggregates the Outputs returned by the backends of a pool and
// evaluates thresholds against them.
//
// Counters aggregate by summation and are evaluated against the pool total.
// Latency statistics do not aggregate: a pool-wide p95 cannot be recovered from
// the per-backend p95s, because Nighthawk returns summarised percentiles rather
// than the underlying histograms, and Nighthawk's own sink service merges
// Outputs by appending results for the same reason. sortie therefore evaluates
// a percentile or statistic threshold against every backend individually and
// requires all of them to hold. For a single-backend pool -- the common case --
// this is exactly the obvious behaviour.
package result

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/types/known/durationpb"

	client "github.com/envoyproxy/nighthawk/api/client"
	"github.com/bpalermo/sortie/internal/metric"
	"github.com/bpalermo/sortie/internal/threshold"
)

// Backend is one Nighthawk instance's contribution to an execution.
type Backend struct {
	Addr   string
	Output *client.Output
	Global *client.Result
}

// Set is every backend's result for a single execution.
type Set struct {
	Backends []Backend
}

// NewSet extracts the global result from each Output.
func NewSet(addrs []string, outputs []*client.Output) (*Set, error) {
	if len(addrs) != len(outputs) {
		return nil, fmt.Errorf("got %d outputs for %d backends", len(outputs), len(addrs))
	}
	s := &Set{}
	for i, out := range outputs {
		global, err := metric.GlobalResult(out)
		if err != nil {
			return nil, fmt.Errorf("backend %s: %w", addrs[i], err)
		}
		s.Backends = append(s.Backends, Backend{Addr: addrs[i], Output: out, Global: global})
	}
	return s, nil
}

// Totals returns a synthetic Result carrying the summed counters of every
// backend and the longest execution duration, so that counter and rate
// thresholds describe the pool as a whole. It deliberately carries no
// statistics.
func (s *Set) Totals() *client.Result {
	sums := map[string]uint64{}
	var order []string
	var longest *durationpb.Duration

	for _, b := range s.Backends {
		for _, c := range b.Global.GetCounters() {
			if _, seen := sums[c.GetName()]; !seen {
				order = append(order, c.GetName())
			}
			sums[c.GetName()] += c.GetValue()
		}
		d := b.Global.GetExecutionDuration()
		if longest == nil || d.AsDuration() > longest.AsDuration() {
			longest = d
		}
	}

	total := &client.Result{Name: "global", ExecutionDuration: longest}
	for _, name := range order {
		total.Counters = append(total.Counters, &client.Counter{Name: name, Value: sums[name]})
	}
	return total
}

// Outcome is a threshold's verdict for an execution, together with the
// per-backend detail behind it.
type Outcome struct {
	Threshold threshold.Threshold
	Pass      bool

	// Scope is "pool" for counter and rate thresholds, evaluated against the
	// summed counters, and "per-backend" for statistics, evaluated against each
	// backend in turn.
	Scope string

	// PerBackend is populated when Scope is "per-backend", in the same order as
	// Set.Backends. It holds a single entry for pool-scoped thresholds.
	PerBackend []threshold.Outcome
}

// Detail renders the observed values behind the verdict.
func (o Outcome) Detail() string {
	parts := make([]string, 0, len(o.PerBackend))
	for _, po := range o.PerBackend {
		if po.Err != nil {
			parts = append(parts, po.Err.Error())
			continue
		}
		parts = append(parts, po.ActualText())
	}
	return strings.Join(parts, ", ")
}

// Evaluate applies every threshold to the set.
func (s *Set) Evaluate(ts []threshold.Threshold) ([]Outcome, bool) {
	totals := s.Totals()
	outcomes := make([]Outcome, 0, len(ts))
	allPass := true

	for _, t := range ts {
		var o Outcome
		switch t.Selector.Kind {
		case metric.KindCounter, metric.KindRate:
			po := t.Evaluate(totals)
			o = Outcome{Threshold: t, Pass: po.Pass, Scope: "pool",
				PerBackend: []threshold.Outcome{po}}
		default:
			o = Outcome{Threshold: t, Pass: true, Scope: "per-backend"}
			for _, b := range s.Backends {
				po := t.Evaluate(b.Global)
				if !po.Pass {
					o.Pass = false
				}
				o.PerBackend = append(o.PerBackend, po)
			}
		}
		if !o.Pass {
			allPass = false
		}
		outcomes = append(outcomes, o)
	}
	return outcomes, allPass
}
