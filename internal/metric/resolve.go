package metric

import (
	"fmt"
	"sort"
	"strings"

	client "github.com/envoyproxy/nighthawk/api/client"
)

// Value is a resolved metric. Duration-valued metrics carry nanoseconds in Num
// so that a threshold can reject comparing a latency against a bare number.
type Value struct {
	Num        float64
	IsDuration bool

	// ActualPercentile is the percentile the Output actually carried, as a
	// fraction in [0,1]. Nighthawk emits an HdrHistogram's own percentile
	// buckets rather than round numbers, so a request for p95 resolves to the
	// first bucket at or above 0.95 -- the same rule nighthawk_client uses for
	// its human-readable output. Only set for KindPercentile.
	ActualPercentile float64
}

// Resolve evaluates sel against a single Result, normally the "global" one.
func Resolve(result *client.Result, sel Selector) (Value, error) {
	switch sel.Kind {
	case KindPercentile:
		st, err := findStatistic(result, sel.Stat)
		if err != nil {
			return Value{}, err
		}
		return resolvePercentile(st, sel)
	case KindAggregate:
		st, err := findStatistic(result, sel.Stat)
		if err != nil {
			return Value{}, err
		}
		return resolveAggregate(st, sel)
	case KindCounter:
		c, err := findCounter(result, sel.Counter)
		if err != nil {
			return Value{}, err
		}
		return Value{Num: float64(c.GetValue())}, nil
	case KindRate:
		c, err := findCounter(result, sel.Counter)
		if err != nil {
			return Value{}, err
		}
		secs := result.GetExecutionDuration().AsDuration().Seconds()
		if secs <= 0 {
			return Value{}, fmt.Errorf("%s: execution duration is zero, cannot compute a rate", sel.Raw)
		}
		return Value{Num: float64(c.GetValue()) / secs}, nil
	}
	return Value{}, fmt.Errorf("%s: unsupported selector kind", sel.Raw)
}

func resolvePercentile(st *client.Statistic, sel Selector) (Value, error) {
	pcts := st.GetPercentiles()
	if len(pcts) == 0 {
		return Value{}, fmt.Errorf("%s: statistic %q carries no percentiles", sel.Raw, st.GetId())
	}
	sorted := append([]*client.Percentile(nil), pcts...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].GetPercentile() < sorted[j].GetPercentile()
	})

	target := sel.Percentile / 100.0
	var chosen *client.Percentile
	for _, p := range sorted {
		if p.GetPercentile() >= target {
			chosen = p
			break
		}
	}
	// Falling back to the highest available bucket would report, say, a p95
	// value as though it were the p99 the threshold asked for -- understating
	// the tail exactly where it matters. An unanswerable question is an error.
	if chosen == nil {
		highest := sorted[len(sorted)-1].GetPercentile()
		return Value{}, fmt.Errorf(
			"%s: statistic %q carries no percentile at or above %.4g; the highest is %.4g",
			sel.Raw, st.GetId(), target, highest)
	}
	v := Value{ActualPercentile: chosen.GetPercentile()}
	if d := chosen.GetDuration(); d != nil {
		v.Num = float64(d.AsDuration().Nanoseconds())
		v.IsDuration = true
	} else {
		v.Num = chosen.GetRawValue()
	}
	return v, nil
}

func resolveAggregate(st *client.Statistic, sel Selector) (Value, error) {
	// Each aggregate is a oneof: either a duration or a raw number, and
	// possibly neither. The generated getter for the raw arm returns zero when
	// the oneof is unset, so reading it without checking presence reports "no
	// measurement" as "zero" -- which satisfies every upper-bound threshold
	// written against it. Presence is checked first for that reason.
	switch sel.Aggregate {
	case Count:
		// Not a oneof: a count of zero is a real measurement.
		return Value{Num: float64(st.GetCount())}, nil
	case Mean:
		if st.GetMeanType() == nil {
			return Value{}, missingAggregate(st, sel)
		}
		if d := st.GetMean(); d != nil {
			return Value{Num: float64(d.AsDuration().Nanoseconds()), IsDuration: true}, nil
		}
		return Value{Num: st.GetRawMean()}, nil
	case Pstdev:
		if st.GetPstdevType() == nil {
			return Value{}, missingAggregate(st, sel)
		}
		if d := st.GetPstdev(); d != nil {
			return Value{Num: float64(d.AsDuration().Nanoseconds()), IsDuration: true}, nil
		}
		return Value{Num: st.GetRawPstdev()}, nil
	case Min:
		if st.GetMinType() == nil {
			return Value{}, missingAggregate(st, sel)
		}
		if d := st.GetMin(); d != nil {
			return Value{Num: float64(d.AsDuration().Nanoseconds()), IsDuration: true}, nil
		}
		return Value{Num: float64(st.GetRawMin())}, nil
	case Max:
		if st.GetMaxType() == nil {
			return Value{}, missingAggregate(st, sel)
		}
		if d := st.GetMax(); d != nil {
			return Value{Num: float64(d.AsDuration().Nanoseconds()), IsDuration: true}, nil
		}
		return Value{Num: float64(st.GetRawMax())}, nil
	}
	return Value{}, fmt.Errorf("%s: unknown aggregate %q", sel.Raw, sel.Aggregate)
}

func missingAggregate(st *client.Statistic, sel Selector) error {
	return fmt.Errorf("%s: statistic %q carries no %s", sel.Raw, st.GetId(), sel.Aggregate)
}

// findStatistic matches name exactly, or as a dotted suffix so that
// "latency_2xx" resolves "benchmark_http_client.latency_2xx". An ambiguous
// suffix is an error rather than an arbitrary pick.
func findStatistic(result *client.Result, name string) (*client.Statistic, error) {
	var matches []*client.Statistic
	for _, st := range result.GetStatistics() {
		if st.GetId() == name {
			return st, nil
		}
		if strings.HasSuffix(st.GetId(), "."+name) {
			matches = append(matches, st)
		}
	}
	switch len(matches) {
	case 1:
		return matches[0], nil
	case 0:
		return nil, fmt.Errorf("no statistic matches %q; available: %s",
			name, strings.Join(statisticIDs(result), ", "))
	default:
		ids := make([]string, 0, len(matches))
		for _, st := range matches {
			ids = append(ids, st.GetId())
		}
		return nil, fmt.Errorf("%q is ambiguous, matches: %s", name, strings.Join(ids, ", "))
	}
}

func findCounter(result *client.Result, name string) (*client.Counter, error) {
	var matches []*client.Counter
	for _, c := range result.GetCounters() {
		if c.GetName() == name {
			return c, nil
		}
		if strings.HasSuffix(c.GetName(), "."+name) {
			matches = append(matches, c)
		}
	}
	switch len(matches) {
	case 1:
		return matches[0], nil
	case 0:
		// A counter that never incremented is absent from the Output rather than
		// present and zero, and asserting "no 5xx happened" is the single most
		// common threshold there is. Treat a missing counter as zero.
		return &client.Counter{Name: name, Value: 0}, nil
	default:
		names := make([]string, 0, len(matches))
		for _, c := range matches {
			names = append(names, c.GetName())
		}
		return nil, fmt.Errorf("%q is ambiguous, matches: %s", name, strings.Join(names, ", "))
	}
}

func statisticIDs(result *client.Result) []string {
	ids := make([]string, 0, len(result.GetStatistics()))
	for _, st := range result.GetStatistics() {
		ids = append(ids, st.GetId())
	}
	sort.Strings(ids)
	return ids
}

// GlobalResult returns the aggregated result Nighthawk names "global", falling
// back to the sole result when a run produced exactly one.
func GlobalResult(out *client.Output) (*client.Result, error) {
	results := out.GetResults()
	for _, r := range results {
		if r.GetName() == "global" {
			return r, nil
		}
	}
	if len(results) == 1 {
		return results[0], nil
	}
	return nil, fmt.Errorf("output carries no \"global\" result (%d results)", len(results))
}
