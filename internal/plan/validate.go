package plan

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

func (p *Plan) validate() error {
	if p.Version != Version {
		return fmt.Errorf("unsupported plan version %q, want %q", p.Version, Version)
	}
	if len(p.Scenarios) == 0 {
		return fmt.Errorf("plan declares no scenarios")
	}

	pools := make(map[string]*Pool, len(p.Pools))
	for i := range p.Pools {
		pool := &p.Pools[i]
		if pool.Name == "" {
			return fmt.Errorf("pools[%d]: name is required", i)
		}
		if _, dup := pools[pool.Name]; dup {
			return fmt.Errorf("pool %q declared more than once", pool.Name)
		}
		if err := pool.validate(); err != nil {
			return fmt.Errorf("pool %q: %w", pool.Name, err)
		}
		pools[pool.Name] = pool
	}

	seen := make(map[string]struct{}, len(p.Scenarios))
	for i := range p.Scenarios {
		s := &p.Scenarios[i]
		if s.Name == "" {
			return fmt.Errorf("scenarios[%d]: name is required", i)
		}
		if _, dup := seen[s.Name]; dup {
			return fmt.Errorf("scenario %q declared more than once", s.Name)
		}
		seen[s.Name] = struct{}{}
	}

	// Defaults may supply pool, target and protocol, so validate each scenario
	// against its effective value rather than the literal one.
	for i := range p.Scenarios {
		s := p.effective(p.Scenarios[i])
		if err := s.validate(pools); err != nil {
			return fmt.Errorf("scenario %q: %w", s.Name, err)
		}
	}

	for i, expr := range p.Thresholds {
		if err := validateThresholdSyntax(expr); err != nil {
			return fmt.Errorf("thresholds[%d]: %w", i, err)
		}
	}
	return nil
}

func (pool *Pool) validate() error {
	hasServices := len(pool.Services) > 0
	hasDistributor := pool.Distributor != ""
	switch {
	case hasServices && hasDistributor:
		return fmt.Errorf("set either services or distributor, not both")
	case !hasServices && !hasDistributor:
		return fmt.Errorf("set one of services or distributor")
	case hasDistributor && len(pool.Targets) == 0:
		return fmt.Errorf("distributor requires at least one entry in targets")
	case hasServices && len(pool.Targets) > 0:
		return fmt.Errorf("targets is only meaningful together with distributor")
	}
	for _, addr := range append(append([]string{}, pool.Services...), pool.Targets...) {
		if _, _, err := splitHostPort(addr); err != nil {
			return fmt.Errorf("address %q: %w", addr, err)
		}
	}
	if hasDistributor {
		if _, _, err := splitHostPort(pool.Distributor); err != nil {
			return fmt.Errorf("distributor %q: %w", pool.Distributor, err)
		}
	}
	return nil
}

func (s *Scenario) validate(pools map[string]*Pool) error {
	if s.Pool == "" {
		return fmt.Errorf("pool is required (set it on the scenario or in defaults)")
	}
	if _, ok := pools[s.Pool]; !ok {
		return fmt.Errorf("pool %q is not declared", s.Pool)
	}
	if s.Target == "" {
		return fmt.Errorf("target is required (set it on the scenario or in defaults)")
	}
	u, err := url.Parse(s.Target)
	if err != nil {
		return fmt.Errorf("target %q: %w", s.Target, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("target %q: scheme must be http or https", s.Target)
	}
	if u.Host == "" {
		return fmt.Errorf("target %q: missing host", s.Target)
	}
	switch s.Protocol {
	case "", "http1", "http2", "http3":
	default:
		return fmt.Errorf("protocol %q: want http1, http2 or http3", s.Protocol)
	}
	if s.Concurrency != "" && s.Concurrency != "auto" {
		n, err := strconv.ParseUint(s.Concurrency, 10, 32)
		if err != nil || n == 0 {
			return fmt.Errorf("concurrency %q: want a positive integer or \"auto\"", s.Concurrency)
		}
	}
	for _, h := range s.Headers {
		if !strings.Contains(h, ":") {
			return fmt.Errorf("header %q: want \"Name: value\"", h)
		}
	}
	if err := s.Executor.validate(); err != nil {
		return fmt.Errorf("executor: %w", err)
	}
	for i, expr := range s.Thresholds {
		if err := validateThresholdSyntax(expr); err != nil {
			return fmt.Errorf("thresholds[%d]: %w", i, err)
		}
	}
	return nil
}

func (e *Executor) validate() error {
	switch e.Type {
	case ConstantRate:
		if e.Rate == 0 {
			return fmt.Errorf("%s: rate must be greater than zero", e.Type)
		}
		if e.Duration.IsZero() {
			return fmt.Errorf("%s: duration is required", e.Type)
		}
		if len(e.Stages) > 0 {
			return fmt.Errorf("%s: stages is only valid for %s", e.Type, Staircase)
		}
		if e.RampTime != nil {
			return fmt.Errorf("%s: ramp_time is only valid for %s", e.Type, RampingRate)
		}
	case RampingRate:
		if e.Rate == 0 {
			return fmt.Errorf("%s: rate must be greater than zero", e.Type)
		}
		if e.Duration.IsZero() {
			return fmt.Errorf("%s: duration is required", e.Type)
		}
		if e.RampTime == nil || e.RampTime.IsZero() {
			return fmt.Errorf("%s: ramp_time is required and must be greater than zero", e.Type)
		}
		// Nighthawk's linear ramping rate limiter requires ramp_time < duration.
		if e.RampTime.Std() >= e.Duration.Std() {
			return fmt.Errorf("%s: ramp_time (%s) must be shorter than duration (%s)",
				e.Type, e.RampTime, e.Duration)
		}
		if len(e.Stages) > 0 {
			return fmt.Errorf("%s: stages is only valid for %s", e.Type, Staircase)
		}
	case Staircase:
		if len(e.Stages) == 0 {
			return fmt.Errorf("%s: at least one stage is required", e.Type)
		}
		if e.Rate != 0 {
			return fmt.Errorf("%s: set rate on each stage, not on the executor", e.Type)
		}
		if !e.Duration.IsZero() {
			return fmt.Errorf("%s: set duration on each stage, not on the executor", e.Type)
		}
		if e.RampTime != nil {
			return fmt.Errorf("%s: ramp_time is only valid for %s", e.Type, RampingRate)
		}
		for i, st := range e.Stages {
			if st.Rate == 0 {
				return fmt.Errorf("%s: stages[%d]: rate must be greater than zero", e.Type, i)
			}
			if st.Duration.IsZero() {
				return fmt.Errorf("%s: stages[%d]: duration is required", e.Type, i)
			}
		}
	case "":
		return fmt.Errorf("type is required (%s, %s or %s)", ConstantRate, RampingRate, Staircase)
	default:
		return fmt.Errorf("unknown type %q (want %s, %s or %s)", e.Type, ConstantRate, RampingRate, Staircase)
	}
	return nil
}

// splitHostPort accepts "host:port" including bracketed IPv6 literals.
func splitHostPort(addr string) (string, uint32, error) {
	if addr == "" {
		return "", 0, fmt.Errorf("address is empty")
	}
	idx := strings.LastIndex(addr, ":")
	if idx < 0 {
		return "", 0, fmt.Errorf("want host:port")
	}
	host, portStr := addr[:idx], addr[idx+1:]
	host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	if host == "" {
		return "", 0, fmt.Errorf("want host:port")
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil || port == 0 {
		return "", 0, fmt.Errorf("invalid port %q", portStr)
	}
	return host, uint32(port), nil
}
