package plan

import "github.com/bpalermo/sortie/internal/threshold"

// validateThresholdSyntax reports whether a threshold expression parses. The
// evaluation semantics live in internal/threshold; this keeps a malformed
// expression a plan-load error rather than a surprise at the end of a run.
func validateThresholdSyntax(expr string) error {
	_, err := threshold.Parse(expr)
	return err
}

// effective returns s with any unset inheritable field filled in from
// p.Defaults. Thresholds are additive rather than inherited, so they are
// combined by EffectiveThresholds instead.
func (p *Plan) effective(s Scenario) Scenario {
	d := p.Defaults
	if d == nil {
		return s
	}
	if s.Pool == "" {
		s.Pool = d.Pool
	}
	if s.Target == "" {
		s.Target = d.Target
	}
	if s.Method == "" {
		s.Method = d.Method
	}
	if s.Body == "" {
		s.Body = d.Body
	}
	if s.Protocol == "" {
		s.Protocol = d.Protocol
	}
	if len(s.Headers) == 0 {
		s.Headers = d.Headers
	}
	if s.Connections == nil {
		s.Connections = d.Connections
	}
	if s.Concurrency == "" {
		s.Concurrency = d.Concurrency
	}
	if s.MaxPendingRequests == nil {
		s.MaxPendingRequests = d.MaxPendingRequests
	}
	if s.MaxConcurrentStream == nil {
		s.MaxConcurrentStream = d.MaxConcurrentStream
	}
	if s.Timeout == nil {
		s.Timeout = d.Timeout
	}
	if s.Executor.Type == "" {
		s.Executor = d.Executor
	}
	return s
}

// applyDefaults folds Defaults into every scenario so that consumers of a
// loaded Plan never have to consult Defaults again.
func (p *Plan) applyDefaults() {
	for i := range p.Scenarios {
		p.Scenarios[i] = p.effective(p.Scenarios[i])
	}
}

// PoolFor returns the pool a scenario runs on. The plan is validated at load
// time, so the pool is guaranteed to exist.
func (p *Plan) PoolFor(s Scenario) Pool {
	for _, pool := range p.Pools {
		if pool.Name == s.Pool {
			return pool
		}
	}
	return Pool{}
}

// EffectiveThresholds returns the plan-wide thresholds followed by the
// scenario's own. Both sets must hold for the scenario to pass.
func (p *Plan) EffectiveThresholds(s Scenario) []string {
	out := make([]string, 0, len(p.Thresholds)+len(s.Thresholds))
	out = append(out, p.Thresholds...)
	out = append(out, s.Thresholds...)
	return out
}
