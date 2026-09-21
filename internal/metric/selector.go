// Package metric resolves sortie metric selectors against the Output proto that
// a Nighthawk execution returns.
package metric

import (
	"fmt"
	"strconv"
	"strings"
)

// Kind distinguishes the four things a selector can name.
type Kind int

const (
	// KindPercentile names a percentile of a statistic, e.g. "latency_2xx.p95".
	KindPercentile Kind = iota
	// KindAggregate names a scalar summary of a statistic, e.g. "latency_2xx.mean".
	KindAggregate
	// KindCounter names a counter, e.g. "counter:benchmark.http_5xx".
	KindCounter
	// KindRate names a counter divided by the execution duration in seconds,
	// e.g. "rate:benchmark.http_2xx" for the achieved request rate.
	KindRate
)

// Aggregate names the scalar summaries a statistic exposes.
type Aggregate string

const (
	Mean   Aggregate = "mean"
	Min    Aggregate = "min"
	Max    Aggregate = "max"
	Pstdev Aggregate = "pstdev"
	Count  Aggregate = "count"
)

// Selector is a parsed metric reference.
type Selector struct {
	Raw  string
	Kind Kind

	// Stat is the statistic id, matched against Output ids either exactly or by
	// dotted suffix, so "latency_2xx" resolves "benchmark_http_client.latency_2xx".
	Stat string

	// Percentile is in [0,100] and set when Kind is KindPercentile.
	Percentile float64

	// Aggregate is set when Kind is KindAggregate.
	Aggregate Aggregate

	// Counter is the counter name, suffix-matched like Stat. Set when Kind is
	// KindCounter or KindRate.
	Counter string
}

func (s Selector) String() string { return s.Raw }

// ParseSelector parses a metric reference. Accepted forms:
//
//	latency_2xx.p95                 percentile of a statistic
//	latency_2xx.p99.9               fractional percentiles are allowed
//	latency_2xx.mean                mean | min | max | pstdev | count
//	counter:benchmark.http_5xx      absolute counter value
//	rate:benchmark.http_2xx         counter per second over the execution
func ParseSelector(raw string) (Selector, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return Selector{}, fmt.Errorf("empty metric selector")
	}

	if name, ok := strings.CutPrefix(s, "counter:"); ok {
		if name == "" {
			return Selector{}, fmt.Errorf("%q: counter name is empty", raw)
		}
		return Selector{Raw: raw, Kind: KindCounter, Counter: name}, nil
	}
	if name, ok := strings.CutPrefix(s, "rate:"); ok {
		if name == "" {
			return Selector{}, fmt.Errorf("%q: counter name is empty", raw)
		}
		return Selector{Raw: raw, Kind: KindRate, Counter: name}, nil
	}

	idx := strings.LastIndex(s, ".")
	if idx < 0 {
		return Selector{}, fmt.Errorf(
			"%q: expected <statistic>.<p95|mean|min|max|pstdev|count>, counter:<name> or rate:<name>", raw)
	}
	stat, suffix := s[:idx], s[idx+1:]

	// A fractional percentile such as "latency_2xx.p99.9" splits at the wrong
	// dot above, so retry one dot earlier when the tail is numeric.
	if _, err := strconv.ParseFloat(suffix, 64); err == nil {
		if prev := strings.LastIndex(stat, "."); prev >= 0 {
			stat, suffix = s[:prev], s[prev+1:]
		}
	}
	if stat == "" {
		return Selector{}, fmt.Errorf("%q: statistic name is empty", raw)
	}

	switch Aggregate(suffix) {
	case Mean, Min, Max, Pstdev, Count:
		return Selector{Raw: raw, Kind: KindAggregate, Stat: stat, Aggregate: Aggregate(suffix)}, nil
	}

	if pct, ok := strings.CutPrefix(suffix, "p"); ok {
		v, err := strconv.ParseFloat(pct, 64)
		if err != nil {
			return Selector{}, fmt.Errorf("%q: invalid percentile %q", raw, suffix)
		}
		if v <= 0 || v > 100 {
			return Selector{}, fmt.Errorf("%q: percentile %v must be in (0,100]", raw, v)
		}
		return Selector{Raw: raw, Kind: KindPercentile, Stat: stat, Percentile: v}, nil
	}

	return Selector{}, fmt.Errorf(
		"%q: unknown suffix %q, want p<N>, mean, min, max, pstdev or count", raw, suffix)
}
