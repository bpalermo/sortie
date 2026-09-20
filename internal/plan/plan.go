// Package plan defines sortie's scenario file: the declarative description of
// what load to generate, where to generate it from, and what must hold for the
// run to be considered a pass.
package plan

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Version is the only plan schema version understood by this binary.
const Version = "v1"

// Plan is the root of a sortie scenario file.
type Plan struct {
	Version string `yaml:"version"`

	// Pools name the Nighthawk backends that scenarios run on.
	Pools []Pool `yaml:"pools"`

	// Defaults supplies field values inherited by every scenario that does not
	// set them itself. Its Name and Thresholds fields are ignored.
	Defaults *Scenario `yaml:"defaults,omitempty"`

	// Scenarios run in file order.
	Scenarios []Scenario `yaml:"scenarios"`

	// Thresholds apply to every scenario, in addition to any the scenario
	// declares itself.
	Thresholds []string `yaml:"thresholds,omitempty"`
}

// Pool is a set of Nighthawk backends that a scenario's load is spread over.
//
// Exactly one of Services or Distributor must be set. Services addresses a set
// of nighthawk_service instances directly, which sortie drives concurrently and
// whose results it merges itself. Distributor addresses a single
// nighthawk_distributor that fans the request out on sortie's behalf.
type Pool struct {
	Name        string   `yaml:"name"`
	Services    []string `yaml:"services,omitempty"`
	Distributor string   `yaml:"distributor,omitempty"`

	// Targets is the list of addresses the distributor should fan out to. It is
	// only meaningful together with Distributor.
	Targets []string `yaml:"targets,omitempty"`
}

// Scenario is one unit of load: a target, an executor that shapes the request
// rate over time, and the client tuning that goes with it.
type Scenario struct {
	Name string `yaml:"name"`
	Pool string `yaml:"pool,omitempty"`

	Target   string   `yaml:"target,omitempty"`
	Method   string   `yaml:"method,omitempty"`
	Headers  []string `yaml:"headers,omitempty"`
	Body     string   `yaml:"body,omitempty"`
	Protocol string   `yaml:"protocol,omitempty"`

	Executor Executor `yaml:"executor"`

	// Connections is Nighthawk's --connections: the per-worker connection
	// circuit-breaker cap, not a target concurrency.
	Connections *uint32 `yaml:"connections,omitempty"`

	// Concurrency is Nighthawk's --concurrency: the number of worker threads,
	// either a positive integer or "auto".
	Concurrency string `yaml:"concurrency,omitempty"`

	MaxPendingRequests  *uint32   `yaml:"max_pending_requests,omitempty"`
	MaxConcurrentStream *uint32   `yaml:"max_concurrent_streams,omitempty"`
	Timeout             *Duration `yaml:"timeout,omitempty"`

	// Thresholds apply to this scenario only.
	Thresholds []string `yaml:"thresholds,omitempty"`
}

// ExecutorType names the supported request-rate shapes.
type ExecutorType string

const (
	// ConstantRate holds a fixed aggregate request rate for the whole duration.
	ConstantRate ExecutorType = "constant-rate"

	// RampingRate ramps linearly from zero to Rate over RampTime, then holds
	// Rate for the remainder of Duration. This is Nighthawk's
	// nighthawk.linear-ramping-rate-limiter-plugin.
	RampingRate ExecutorType = "ramping-rate"

	// Staircase steps through Stages, each at a constant rate. Each stage is a
	// separate Nighthawk execution; see Stage.
	Staircase ExecutorType = "staircase"
)

// Executor shapes the request rate over time.
type Executor struct {
	Type ExecutorType `yaml:"type"`

	// Rate is the aggregate requests per second the target receives, across
	// every worker thread of every backend in the pool.
	//
	// Nighthawk's own --rps is per worker thread, so sortie divides this by
	// backends x concurrency before sending it. A rate that no integer
	// per-worker --rps can express is refused rather than rounded, and
	// concurrency "auto" cannot be combined with a rate at all, because the
	// worker count is only decided on the backend.
	Rate uint32 `yaml:"rate,omitempty"`

	Duration Duration `yaml:"duration,omitempty"`

	// RampTime is only used by RampingRate. It must be shorter than Duration.
	RampTime *Duration `yaml:"ramp_time,omitempty"`

	// Stages is only used by Staircase.
	Stages []Stage `yaml:"stages,omitempty"`

	// OpenLoop selects Nighthawk's open-loop mode, in which the rate limiter
	// never compensates for a client that cannot keep pace and pool overflows
	// are reported instead. Defaults to false, matching Nighthawk's own default.
	OpenLoop bool `yaml:"open_loop,omitempty"`
}

// Stage is one step of a Staircase executor.
//
// Each stage is dispatched as its own Nighthawk execution, because Nighthawk
// has no way to change the request rate of a run already in flight (the
// UpdateRequest RPC in api/client/service.proto is declared but unimplemented).
// Connections are therefore re-established at every stage boundary and each
// stage yields its own result, which sortie reports separately and evaluates
// thresholds against separately.
type Stage struct {
	Rate     uint32   `yaml:"rate"`
	Duration Duration `yaml:"duration"`
}

// Load reads and validates a plan from path.
func Load(path string) (*Plan, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(raw)
}

// Parse decodes and validates a plan. Unknown fields are rejected so that a
// typo in a scenario file fails the run instead of being silently ignored.
func Parse(raw []byte) (*Plan, error) {
	var p Plan
	dec := yaml.NewDecoder(newReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&p); err != nil {
		return nil, fmt.Errorf("parsing plan: %w", err)
	}
	if err := p.validate(); err != nil {
		return nil, err
	}
	p.applyDefaults()
	return &p, nil
}
