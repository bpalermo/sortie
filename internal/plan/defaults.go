package plan

import (
	"google.golang.org/protobuf/proto"

	client "github.com/envoyproxy/nighthawk/api/client"
)

// applyDefaults folds the defaults block into every scenario so that consumers
// of a loaded Plan never have to consult it again.
//
// Defaults are per field and only fill in what a scenario left unset; the
// executor is treated as one unit rather than merged field by field, because a
// scenario that declares an executor means that executor and not a blend.
func applyDefaults(p *Plan) {
	d := p.GetDefaults()
	if d == nil {
		return
	}
	for _, s := range p.GetScenarios() {
		if s.GetPool() == "" {
			s.Pool = d.GetPool()
		}
		if s.GetTarget() == "" {
			s.Target = d.GetTarget()
		}
		if s.GetMethod() == "" {
			s.Method = d.GetMethod()
		}
		if s.GetBody() == "" {
			s.Body = d.GetBody()
		}
		if s.GetProtocol() == "" {
			s.Protocol = d.GetProtocol()
		}
		if len(s.GetHeaders()) == 0 {
			s.Headers = d.GetHeaders()
		}
		if s.Connections == nil {
			s.Connections = d.Connections
		}
		if s.GetConcurrency() == "" {
			s.Concurrency = d.GetConcurrency()
		}
		if s.MaxPendingRequests == nil {
			s.MaxPendingRequests = d.MaxPendingRequests
		}
		if s.MaxConcurrentStreams == nil {
			s.MaxConcurrentStreams = d.MaxConcurrentStreams
		}
		if s.GetTimeout() == nil {
			s.Timeout = d.GetTimeout()
		}
		if s.GetExecutor() == nil {
			s.Executor = proto.Clone(d.GetExecutor()).(*Executor)
		}
		if s.GetNighthawkTemplate() == nil && d.GetNighthawkTemplate() != nil {
			s.NighthawkTemplate = proto.Clone(d.GetNighthawkTemplate()).(*client.CommandLineOptions)
		}
	}
}

// PoolFor returns the pool a scenario runs on. The plan is validated at load
// time, so the pool is guaranteed to exist.
func PoolFor(p *Plan, s *Scenario) *Pool {
	for _, pool := range p.GetPools() {
		if pool.GetName() == s.GetPool() {
			return pool
		}
	}
	return &Pool{}
}

// EffectiveThresholds returns the plan-wide thresholds followed by the
// scenario's own. Both sets must hold for the scenario to pass.
func EffectiveThresholds(p *Plan, s *Scenario) []string {
	out := make([]string, 0, len(p.GetThresholds())+len(s.GetThresholds()))
	out = append(out, p.GetThresholds()...)
	out = append(out, s.GetThresholds()...)
	return out
}
