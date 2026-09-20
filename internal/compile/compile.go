// Package compile turns a sortie scenario into the CommandLineOptions protos
// that Nighthawk's gRPC service accepts.
package compile

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	client "github.com/bpalermo/sortie/gen/api/client"
	ratelimiter "github.com/bpalermo/sortie/gen/api/rate_limiter"
	"github.com/bpalermo/sortie/internal/plan"
)

// LinearRampingRateLimiterPlugin is the name Nighthawk registers its linear
// ramping rate limiter under.
const LinearRampingRateLimiterPlugin = "nighthawk.linear-ramping-rate-limiter-plugin"

// Execution is a single Nighthawk run: one CommandLineOptions dispatched to one
// pool. A scenario expands to exactly one Execution unless it uses a staircase
// executor, which produces one per stage.
type Execution struct {
	// Scenario is the scenario this execution came from.
	Scenario plan.Scenario

	// Label identifies the execution in reports. It is the scenario name, with
	// a stage suffix when the scenario expands to more than one execution.
	Label string

	// Stage is the 1-based stage index, or 0 for a single-execution scenario.
	Stage int

	// Rate is the aggregate requests per second this execution targets across
	// the whole pool, before it is divided among backends.
	Rate uint32

	Duration time.Duration
	RampTime time.Duration

	// Options is the compiled request, with requests_per_second still set to
	// the aggregate Rate. Divide returns the per-backend copies.
	Options *client.CommandLineOptions
}

// Expand turns a scenario into the executions it runs as.
func Expand(s plan.Scenario) ([]Execution, error) {
	switch s.Executor.Type {
	case plan.ConstantRate:
		opts, err := options(s, s.Executor.Rate, s.Executor.Duration.Std(), 0, s.Name)
		if err != nil {
			return nil, err
		}
		return []Execution{{
			Scenario: s, Label: s.Name, Rate: s.Executor.Rate,
			Duration: s.Executor.Duration.Std(), Options: opts,
		}}, nil

	case plan.RampingRate:
		ramp := s.Executor.RampTime.Std()
		opts, err := options(s, s.Executor.Rate, s.Executor.Duration.Std(), ramp, s.Name)
		if err != nil {
			return nil, err
		}
		return []Execution{{
			Scenario: s, Label: s.Name, Rate: s.Executor.Rate,
			Duration: s.Executor.Duration.Std(), RampTime: ramp, Options: opts,
		}}, nil

	case plan.Staircase:
		out := make([]Execution, 0, len(s.Executor.Stages))
		for i, st := range s.Executor.Stages {
			label := fmt.Sprintf("%s/stage-%d", s.Name, i+1)
			opts, err := options(s, st.Rate, st.Duration.Std(), 0, label)
			if err != nil {
				return nil, err
			}
			out = append(out, Execution{
				Scenario: s, Label: label, Stage: i + 1, Rate: st.Rate,
				Duration: st.Duration.Std(), Options: opts,
			})
		}
		return out, nil
	}
	return nil, fmt.Errorf("scenario %q: unknown executor type %q", s.Name, s.Executor.Type)
}

// Divide splits an execution's aggregate rate across the backends of a pool,
// returning one CommandLineOptions per backend.
//
// Nighthawk's --rps is per worker thread, not per instance: a backend running
// --concurrency 2 --rps 100 emits 200 requests per second. A plan's rate is the
// aggregate the target actually sees, so the divisor is backends x workers.
//
// A rate that is not divisible by the worker count cannot be expressed at all,
// because Nighthawk takes an integer --rps per worker. That is reported as an
// error rather than rounded: a load generator that silently produces a
// different rate than the plan asked for is worse than one that refuses.
func Divide(e Execution, backends int) ([]*client.CommandLineOptions, error) {
	if backends <= 0 {
		return nil, fmt.Errorf("execution %q: pool has no backends", e.Label)
	}
	workers, err := workersPerBackend(e.Scenario)
	if err != nil {
		return nil, fmt.Errorf("execution %q: %w", e.Label, err)
	}

	if e.Rate%uint32(workers) != 0 {
		return nil, fmt.Errorf(
			"execution %q: rate %d is not divisible by %d workers per backend; "+
				"use a rate that is a multiple of %d, or set a different concurrency",
			e.Label, e.Rate, workers, workers)
	}
	perBackendTotal := e.Rate / uint32(workers)

	if uint32(backends) > perBackendTotal {
		return nil, fmt.Errorf(
			"execution %q: rate %d over %d backends x %d workers leaves less than 1 rps per worker",
			e.Label, e.Rate, backends, workers)
	}

	base := perBackendTotal / uint32(backends)
	remainder := perBackendTotal % uint32(backends)

	out := make([]*client.CommandLineOptions, 0, backends)
	for i := range backends {
		share := base
		if uint32(i) < remainder {
			share++
		}
		clone := cloneOptions(e.Options)
		clone.RequestsPerSecond = wrapperspb.UInt32(share)
		if backends > 1 {
			clone.ExecutionId = wrapperspb.String(fmt.Sprintf("%s#%d", e.Label, i))
		}
		out = append(out, clone)
	}
	return out, nil
}

// workersPerBackend reports how many worker threads each backend will run.
//
// Nighthawk defaults to one worker, and "auto" defers the decision to the
// backend's vCPU affinity, which sortie cannot know in advance and therefore
// cannot divide by.
func workersPerBackend(s plan.Scenario) (int, error) {
	switch s.Concurrency {
	case "":
		return 1, nil
	case "auto":
		return 0, fmt.Errorf(
			`concurrency "auto" cannot be combined with an aggregate rate, because ` +
				`Nighthawk's --rps is per worker and the worker count is only decided ` +
				`on the backend; set concurrency to a number`)
	}
	n, err := strconv.Atoi(s.Concurrency)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("concurrency %q is not a positive integer", s.Concurrency)
	}
	return n, nil
}

func options(s plan.Scenario, rate uint32, dur, ramp time.Duration, execID string) (*client.CommandLineOptions, error) {
	o := &client.CommandLineOptions{
		RequestsPerSecond:    wrapperspb.UInt32(rate),
		OneofDurationOptions: &client.CommandLineOptions_Duration{Duration: durationpb.New(dur)},
		OneofUri:             &client.CommandLineOptions_Uri{Uri: wrapperspb.String(s.Target)},
		ExecutionId:          wrapperspb.String(execID),
		OpenLoop:             wrapperspb.Bool(s.Executor.OpenLoop),
	}

	proto, err := protocol(s.Protocol)
	if err != nil {
		return nil, err
	}
	o.OneofProtocol = &client.CommandLineOptions_Protocol{
		Protocol: &client.Protocol{Value: proto},
	}

	reqOpts, err := requestOptions(s)
	if err != nil {
		return nil, err
	}
	o.OneofRequestOptions = &client.CommandLineOptions_RequestOptions{RequestOptions: reqOpts}

	if s.Connections != nil {
		o.Connections = wrapperspb.UInt32(*s.Connections)
	}
	if s.Concurrency != "" {
		o.Concurrency = wrapperspb.String(s.Concurrency)
	}
	if s.MaxPendingRequests != nil {
		o.MaxPendingRequests = wrapperspb.UInt32(*s.MaxPendingRequests)
	}
	if s.MaxConcurrentStream != nil {
		o.MaxConcurrentStreams = wrapperspb.UInt32(*s.MaxConcurrentStream)
	}
	if s.Timeout != nil {
		o.Timeout = s.Timeout.Proto()
	}

	if ramp > 0 {
		cfg, err := anypb.New(&ratelimiter.LinearRampingRateLimiterConfig{
			RampTime: durationpb.New(ramp),
		})
		if err != nil {
			return nil, fmt.Errorf("scenario %q: packing ramp config: %w", s.Name, err)
		}
		o.RateLimiterPluginConfig = &corev3.TypedExtensionConfig{
			Name:        LinearRampingRateLimiterPlugin,
			TypedConfig: cfg,
		}
	}
	return o, nil
}

func protocol(name string) (client.Protocol_ProtocolOptions, error) {
	switch name {
	case "", "http1":
		return client.Protocol_HTTP1, nil
	case "http2":
		return client.Protocol_HTTP2, nil
	case "http3":
		return client.Protocol_HTTP3, nil
	}
	return 0, fmt.Errorf("unknown protocol %q", name)
}

func requestOptions(s plan.Scenario) (*client.RequestOptions, error) {
	ro := &client.RequestOptions{}

	method, err := requestMethod(s.Method)
	if err != nil {
		return nil, fmt.Errorf("scenario %q: %w", s.Name, err)
	}
	ro.RequestMethod = method

	for _, h := range s.Headers {
		key, value, ok := strings.Cut(h, ":")
		if !ok {
			return nil, fmt.Errorf("scenario %q: header %q: want \"Name: value\"", s.Name, h)
		}
		ro.RequestHeaders = append(ro.RequestHeaders, &corev3.HeaderValueOption{
			Header: &corev3.HeaderValue{
				Key:   strings.TrimSpace(key),
				Value: strings.TrimSpace(value),
			},
		})
	}
	if s.Body != "" {
		// RequestBody is sent verbatim and sets no Content-Type, leaving the
		// header entirely under the plan's control.
		ro.RequestBody = []byte(s.Body)
	}
	return ro, nil
}

func requestMethod(name string) (corev3.RequestMethod, error) {
	if name == "" {
		return corev3.RequestMethod_GET, nil
	}
	v, ok := corev3.RequestMethod_value[strings.ToUpper(name)]
	if !ok || corev3.RequestMethod(v) == corev3.RequestMethod_METHOD_UNSPECIFIED {
		return 0, fmt.Errorf("unknown method %q", name)
	}
	return corev3.RequestMethod(v), nil
}
