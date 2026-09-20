// Package plan loads a sortie plan file.
//
// The schema itself lives in api/sortie/plan/v1: field names, types and most
// constraints are declared in the proto, so a rule sits next to the field it
// governs rather than in a wall of Go. This package parses YAML into those
// messages and adds the checks the schema cannot express.
package plan

import (
	"fmt"
	"os"

	"buf.build/go/protovalidate"
	"buf.build/go/protoyaml"

	"google.golang.org/protobuf/proto"

	planv1 "github.com/bpalermo/sortie/api/sortie/plan/v1"
)

// yamlValidator adapts protovalidate to protoyaml's Validator interface.
// protovalidate's Validate takes variadic options, which does not satisfy
// protoyaml's single-argument signature.
type yamlValidator struct{ v protovalidate.Validator }

func (a yamlValidator) Validate(m proto.Message) error { return a.v.Validate(m) }

// Version is the only plan schema version understood by this binary.
const Version = "v1"

// Executor type names, as written in a plan file.
const (
	// ConstantRate holds a fixed aggregate request rate for the whole duration.
	ConstantRate = "constant-rate"

	// RampingRate ramps linearly from zero to the rate over ramp_time, then
	// holds it for the remainder of the duration. This is Nighthawk's
	// nighthawk.linear-ramping-rate-limiter-plugin.
	RampingRate = "ramping-rate"

	// Staircase steps through stages, each at a constant rate. Each stage is a
	// separate Nighthawk execution.
	Staircase = "staircase"
)

// The plan schema types, aliased so callers do not all have to import the
// generated package directly.
type (
	Plan     = planv1.Plan
	Pool     = planv1.Pool
	Scenario = planv1.Scenario
	Executor = planv1.Executor
	Stage    = planv1.Stage
)

// Load reads and validates a plan from path.
func Load(path string) (*Plan, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parse(raw, path)
}

// Parse decodes and validates a plan held in memory.
func Parse(raw []byte) (*Plan, error) { return parse(raw, "") }

func parse(raw []byte, path string) (*Plan, error) {
	validator, err := protovalidate.New()
	if err != nil {
		return nil, fmt.Errorf("building the plan validator: %w", err)
	}

	p := &planv1.Plan{}
	// DiscardUnknown stays false so a misspelled field fails the run instead of
	// being silently ignored. Passing the validator here rather than calling it
	// afterwards is what attaches line and column numbers to a violation.
	opts := protoyaml.UnmarshalOptions{Path: path, Validator: yamlValidator{validator}}
	if err := opts.Unmarshal(raw, p); err != nil {
		return nil, fmt.Errorf("parsing plan: %w", err)
	}

	if err := validateBeyondSchema(p); err != nil {
		return nil, err
	}
	applyDefaults(p)
	return p, nil
}
