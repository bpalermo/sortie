package plan

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/bpalermo/sortie/internal/threshold"
)

// validateBeyondSchema covers what the proto's own constraints cannot.
//
// protovalidate evaluates one message at a time, so a rule that spans messages
// -- a scenario naming a pool declared elsewhere in the file -- has nowhere to
// live in the schema. Threshold expressions are a second case: they are free
// text whose grammar belongs to internal/threshold, and catching a typo at load
// time rather than after a run has finished is the whole point of validating.
func validateBeyondSchema(p *Plan) error {
	pools := make(map[string]struct{}, len(p.GetPools()))
	for _, pool := range p.GetPools() {
		pools[pool.GetName()] = struct{}{}
	}

	for i, expr := range p.GetThresholds() {
		if _, err := threshold.Parse(expr); err != nil {
			return fmt.Errorf("thresholds[%d]: %w", i, err)
		}
	}

	for _, s := range p.GetScenarios() {
		if s.GetName() == "" {
			return fmt.Errorf("every scenario needs a name")
		}
		// Resolve against the effective value: defaults may supply the pool.
		pool := s.GetPool()
		if pool == "" {
			pool = p.GetDefaults().GetPool()
		}
		if pool == "" {
			return fmt.Errorf("scenario %q: pool is required (set it on the scenario or in defaults)",
				s.GetName())
		}
		if _, ok := pools[pool]; !ok {
			return fmt.Errorf("scenario %q: pool %q is not declared", s.GetName(), pool)
		}
		if s.GetTarget() == "" && p.GetDefaults().GetTarget() == "" {
			return fmt.Errorf("scenario %q: target is required (set it on the scenario or in defaults)",
				s.GetName())
		}
		if s.GetExecutor() == nil && p.GetDefaults().GetExecutor() == nil {
			return fmt.Errorf("scenario %q: executor is required (set it on the scenario or in defaults)",
				s.GetName())
		}
		for i, expr := range s.GetThresholds() {
			if _, err := threshold.Parse(expr); err != nil {
				return fmt.Errorf("scenario %q: thresholds[%d]: %w", s.GetName(), i, err)
			}
		}
		if err := validateGrpc(p, s); err != nil {
			return fmt.Errorf("scenario %q: %w", s.GetName(), err)
		}
	}
	return nil
}

// validateGrpc checks what the engine would otherwise reject at run time:
// gRPC is HTTP/2 POST, and bidi-stream spreads streams and rate over a known
// number of workers. Effective values, since defaults may supply any of them.
func validateGrpc(p *Plan, s *Scenario) error {
	if s.GetBody() != "" && s.GetBodyFile() != "" {
		return fmt.Errorf("body and body_file are mutually exclusive")
	}
	d := p.GetDefaults()
	g := s.GetGrpc()
	if g == nil {
		g = d.GetGrpc()
	}
	if g == nil {
		return nil
	}
	protocol := s.GetProtocol()
	if protocol == "" {
		protocol = d.GetProtocol()
	}
	if protocol != "" && protocol != "http2" {
		return fmt.Errorf("grpc requires protocol http2 (got %q); leave it unset", protocol)
	}
	method := s.GetMethod()
	if method == "" {
		method = d.GetMethod()
	}
	if method != "" && !strings.EqualFold(method, "POST") {
		return fmt.Errorf("grpc requires method POST (got %q); leave it unset", method)
	}
	if g.GetMode() == "bidi-stream" {
		concurrency := s.GetConcurrency()
		if concurrency == "" {
			concurrency = d.GetConcurrency()
		}
		if concurrency == "auto" {
			return fmt.Errorf(`grpc bidi-stream needs a numeric concurrency, not "auto": streams and rate are divided over the workers`)
		}
		if concurrency != "" && g.Streams != nil {
			workers, err := strconv.ParseUint(concurrency, 10, 32)
			if err == nil && workers > 0 && uint64(g.GetStreams())%workers != 0 {
				return fmt.Errorf("grpc.streams (%d) must be a multiple of concurrency (%d)", g.GetStreams(), workers)
			}
		}
	} else if g.Streams != nil || g.MaxInflightPerStream != nil || g.GetDrainDuration() != nil {
		return fmt.Errorf("grpc.streams, max_inflight_per_stream and drain_duration apply to mode bidi-stream only")
	}
	return nil
}
