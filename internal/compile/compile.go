// Package compile turns a sortie scenario into the CommandLineOptions protos
// that Nighthawk's gRPC service accepts.
package compile

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	tlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	client "github.com/bpalermo/sortie/engine/api/client"
	ratelimiter "github.com/bpalermo/sortie/engine/api/rate_limiter"
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
	Scenario *plan.Scenario

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
func Expand(s *plan.Scenario) ([]Execution, error) {
	switch s.Executor.Type {
	case plan.ConstantRate:
		opts, err := options(s, s.Executor.Rate, s.Executor.Duration.AsDuration(), 0, s.Name)
		if err != nil {
			return nil, err
		}
		return []Execution{{
			Scenario: s, Label: s.Name, Rate: s.Executor.Rate,
			Duration: s.Executor.Duration.AsDuration(), Options: opts,
		}}, nil

	case plan.RampingRate:
		ramp := s.Executor.RampTime.AsDuration()
		opts, err := options(s, s.Executor.Rate, s.Executor.Duration.AsDuration(), ramp, s.Name)
		if err != nil {
			return nil, err
		}
		return []Execution{{
			Scenario: s, Label: s.Name, Rate: s.Executor.Rate,
			Duration: s.Executor.Duration.AsDuration(), RampTime: ramp, Options: opts,
		}}, nil

	case plan.Staircase:
		out := make([]Execution, 0, len(s.Executor.Stages))
		for i, st := range s.Executor.Stages {
			label := fmt.Sprintf("%s/stage-%d", s.Name, i+1)
			opts, err := options(s, st.Rate, st.Duration.AsDuration(), 0, label)
			if err != nil {
				return nil, err
			}
			out = append(out, Execution{
				Scenario: s, Label: label, Stage: i + 1, Rate: st.Rate,
				Duration: st.Duration.AsDuration(), Options: opts,
			})
		}
		return out, nil
	}
	return nil, fmt.Errorf("scenario %q: unknown executor type %q", s.Name, s.Executor.Type)
}

// ForPool returns the backends an execution is dispatched to and the
// CommandLineOptions each of them receives, exactly as the runner sends them.
//
// This is the single source of truth for both dispatch shapes, so that
// `compile` prints what `run` would send and `validate` refuses what `run`
// would refuse. Computing the options separately in each command is how the
// two drifted apart before.
func ForPool(e Execution, pool *plan.Pool) ([]string, []*client.CommandLineOptions, error) {
	if pool.Distributor != "" {
		opts, err := uniformShare(e, len(pool.Targets))
		if err != nil {
			return nil, nil, err
		}
		// The distributor forwards one ExecutionRequest unchanged to every
		// target, so a single options object carries the per-target share and
		// the caller must not divide it again.
		return pool.Targets, []*client.CommandLineOptions{opts}, nil
	}
	perBackend, err := Divide(e, len(pool.Services))
	if err != nil {
		return nil, nil, err
	}
	return pool.Services, perBackend, nil
}

// uniformShare computes the one CommandLineOptions sent when every backend must
// receive identical options, as on the distributor path.
//
// Divide can spread a remainder across backends because it emits a different
// options object for each; here there is only one, so the rate has to divide
// exactly by targets x workers. The alternative would be for a plan's rate to
// mean something different on the distributor path than on the direct one,
// which is worse than refusing the plan.
func uniformShare(e Execution, targets int) (*client.CommandLineOptions, error) {
	if targets <= 0 {
		return nil, fmt.Errorf("execution %q: pool has no targets", e.Label)
	}
	workers, err := workersPerBackend(e.Scenario)
	if err != nil {
		return nil, fmt.Errorf("execution %q: %w", e.Label, err)
	}

	divisor := uint32(targets) * uint32(workers)
	if e.Rate%divisor != 0 {
		return nil, fmt.Errorf(
			"execution %q: rate %d is not divisible by %d targets x %d workers; "+
				"a distributor sends every target the same options, so the rate must be "+
				"a multiple of %d",
			e.Label, e.Rate, targets, workers, divisor)
	}

	clone := cloneOptions(e.Options)
	clone.RequestsPerSecond = wrapperspb.UInt32(backendRate(e, e.Rate/divisor, workers))
	return clone, nil
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
		clone.RequestsPerSecond = wrapperspb.UInt32(backendRate(e, share, workers))
		if backends > 1 {
			clone.ExecutionId = wrapperspb.String(fmt.Sprintf("%s#%d", e.Label, i))
		}
		out = append(out, clone)
	}
	return out, nil
}

// backendRate turns a backend's per-worker share into the --rps it is sent.
// That is the share itself, except in gRPC bidi-stream mode, where the engine
// takes a backend's aggregate and divides it over the workers itself: sending
// the per-worker share there would generate 1/workers of the load. Dividing in
// per-worker units first and multiplying back keeps uneven splits possible --
// 300 rps over two backends with four workers is 152 + 148 -- and guarantees
// each backend's aggregate is a multiple of its workers, which the engine
// requires.
func backendRate(e Execution, perWorkerShare uint32, workers int) uint32 {
	if aggregateRate(e.Scenario) {
		return perWorkerShare * uint32(workers)
	}
	return perWorkerShare
}

// aggregateRate reports whether the engine takes a backend's aggregate rate
// and divides it over the workers itself: in gRPC bidi-stream and WebSocket.
func aggregateRate(s *plan.Scenario) bool {
	return s.GetGrpc().GetMode() == "bidi-stream" || s.GetWebsocket() != nil
}

// workersPerBackend reports how many worker threads each backend will run.
//
// Nighthawk defaults to one worker, and "auto" defers the decision to the
// backend's vCPU affinity, which sortie cannot know in advance and therefore
// cannot divide by.
func workersPerBackend(s *plan.Scenario) (int, error) {
	switch s.Concurrency {
	case "":
		return 1, nil
	case "auto":
		return 0, fmt.Errorf(
			`concurrency "auto" cannot be combined with an aggregate rate, because ` +
				`Nighthawk's --rps is per worker and the worker count is only decided ` +
				`on the backend; set concurrency to a number`)
	}
	// Bounded to 32 bits because that is what Nighthawk's --concurrency is, and
	// because the rate division casts this to uint32: parsing as a plain int
	// would let 4294967296 wrap to zero and turn a bad plan into a divide-by-
	// zero panic rather than an error.
	n, err := strconv.ParseUint(s.GetConcurrency(), 10, 32)
	if err != nil || n < 1 {
		return 0, fmt.Errorf(
			"concurrency %q is not a positive integer that fits in 32 bits", s.GetConcurrency())
	}
	return int(n), nil
}

func options(s *plan.Scenario, rate uint32, dur, ramp time.Duration, execID string) (*client.CommandLineOptions, error) {
	// Start from the passthrough template so anything the plan set there is
	// carried through, then overwrite only what sortie owns. Cloning keeps one
	// scenario's executions from sharing (and mutating) a single template.
	o := &client.CommandLineOptions{}
	if tmpl := s.GetNighthawkTemplate(); tmpl != nil {
		o = proto.Clone(tmpl).(*client.CommandLineOptions)
	}

	// sortie owns the load shape and the identity of the execution.
	o.RequestsPerSecond = wrapperspb.UInt32(rate)
	o.OneofDurationOptions = &client.CommandLineOptions_Duration{Duration: durationpb.New(dur)}
	o.ExecutionId = wrapperspb.String(execID)
	o.OpenLoop = wrapperspb.Bool(s.GetExecutor().GetOpenLoop())

	// The rest is overwritten only when the scenario says something about it,
	// so a template can supply anything the schema does not model.
	if s.GetTarget() != "" {
		o.OneofUri = &client.CommandLineOptions_Uri{Uri: wrapperspb.String(s.GetTarget())}
	}
	if s.GetProtocol() != "" {
		value, err := protocol(s.GetProtocol())
		if err != nil {
			return nil, err
		}
		o.OneofProtocol = &client.CommandLineOptions_Protocol{
			Protocol: &client.Protocol{Value: value},
		}
	}
	if g := s.GetGrpc(); g != nil {
		// gRPC is HTTP/2 POST; the loader already rejected anything else, so
		// setting both here is what makes an unset protocol and method mean the
		// right thing rather than http1 GET.
		o.OneofProtocol = &client.CommandLineOptions_Protocol{
			Protocol: &client.Protocol{Value: client.Protocol_HTTP2},
		}
		mode := client.GrpcMode_UNARY
		if g.GetMode() == "bidi-stream" {
			mode = client.GrpcMode_BIDI_STREAM
		}
		o.GrpcMode = &client.GrpcMode{Value: mode}
		// The plan's grpc block is the whole of the stream configuration: a
		// template's grpc_stream is dropped, so a field the plan leaves unset
		// really is the engine's default -- the one the loader validated the
		// concurrency against.
		o.GrpcStream = nil
		if g.Streams != nil || g.MaxInflightPerStream != nil || g.GetDrainDuration() != nil {
			so := &client.CommandLineOptions_GrpcStreamOptions{}
			if g.Streams != nil {
				so.Streams = wrapperspb.UInt32(g.GetStreams())
			}
			if g.MaxInflightPerStream != nil {
				so.MaxInflightPerStream = wrapperspb.UInt32(g.GetMaxInflightPerStream())
			}
			if g.GetDrainDuration() != nil {
				so.DrainDuration = g.GetDrainDuration()
			}
			o.GrpcStream = so
		}
	}
	if w := s.GetWebsocket(); w != nil {
		// The upgrade is an HTTP/1.1 request; the loader already rejected
		// anything else.
		o.OneofProtocol = &client.CommandLineOptions_Protocol{
			Protocol: &client.Protocol{Value: client.Protocol_HTTP1},
		}
		o.GrpcMode = nil
		o.GrpcStream = nil
		wo := &client.CommandLineOptions_WebSocketOptions{}
		if w.Streams != nil {
			wo.Streams = wrapperspb.UInt32(w.GetStreams())
		}
		if w.MaxInflightPerStream != nil {
			wo.MaxInflightPerStream = wrapperspb.UInt32(w.GetMaxInflightPerStream())
		}
		if w.GetDrainDuration() != nil {
			wo.DrainDuration = w.GetDrainDuration()
		}
		if w.GetBinary() {
			wo.Binary = wrapperspb.Bool(true)
		}
		o.Websocket = wo
		// Every stream is a connection: unless the plan set connections, lift
		// the engine's per-worker connection cap to the streams per worker, so
		// the pool can open them all.
		if s.Connections == nil && o.Connections == nil {
			streams := uint32(20)
			if w.Streams != nil {
				streams = w.GetStreams()
			}
			if workers, err := workersPerBackend(s); err == nil && workers > 0 {
				if per := streams / uint32(workers); per > 0 {
					o.Connections = wrapperspb.UInt32(per)
				}
			}
		}
	}
	if t := s.GetTls(); t != nil {
		tlsCtx, err := tlsContext(t)
		if err != nil {
			return nil, fmt.Errorf("scenario %q: %w", s.GetName(), err)
		}
		o.TlsContext = tlsCtx
	}
	if s.GetMethod() != "" || len(s.GetHeaders()) > 0 || s.GetBody() != "" || s.GetBodyFile() != "" || s.GetGrpc() != nil || s.GetWebsocket() != nil {
		reqOpts, err := requestOptions(s, o.GetRequestOptions())
		if err != nil {
			return nil, err
		}
		o.OneofRequestOptions = &client.CommandLineOptions_RequestOptions{RequestOptions: reqOpts}
	}
	if s.Connections != nil {
		o.Connections = wrapperspb.UInt32(s.GetConnections())
	}
	if s.GetConcurrency() != "" {
		o.Concurrency = wrapperspb.String(s.GetConcurrency())
	}
	if s.MaxPendingRequests != nil {
		o.MaxPendingRequests = wrapperspb.UInt32(s.GetMaxPendingRequests())
	}
	if s.MaxConcurrentStreams != nil {
		o.MaxConcurrentStreams = wrapperspb.UInt32(s.GetMaxConcurrentStreams())
	}
	if s.GetTimeout() != nil {
		o.Timeout = s.GetTimeout()
	}

	// The rate limiter is sortie's to choose: it is what makes an executor mean
	// what it says. A template that carried one would otherwise survive into a
	// constant-rate or staircase execution and shape the load differently from
	// the plan, which is the failure mode every other rate decision here exists
	// to avoid.
	o.RateLimiterPluginConfig = nil

	if ramp > 0 {
		cfg, err := anypb.New(&ratelimiter.LinearRampingRateLimiterConfig{
			RampTime: durationpb.New(ramp),
		})
		if err != nil {
			return nil, fmt.Errorf("scenario %q: packing ramp config: %w", s.GetName(), err)
		}
		o.RateLimiterPluginConfig = &corev3.TypedExtensionConfig{
			Name:        LinearRampingRateLimiterPlugin,
			TypedConfig: cfg,
		}
	}
	return o, nil
}

// tlsContext builds the engine's tls_context from a scenario's tls block,
// with the files inline so the backend needs no access to them. The engine
// adds SNI (from the target, or a Host header) and ALPN for the protocol.
func tlsContext(t *plan.Tls) (*tlsv3.UpstreamTlsContext, error) {
	inline := func(b []byte) *corev3.DataSource {
		return &corev3.DataSource{Specifier: &corev3.DataSource_InlineBytes{InlineBytes: b}}
	}
	common := &tlsv3.CommonTlsContext{}
	if t.GetCaFile() != "" {
		ca, err := os.ReadFile(t.GetCaFile())
		if err != nil {
			return nil, fmt.Errorf("tls.ca_file: %w", err)
		}
		common.ValidationContextType = &tlsv3.CommonTlsContext_ValidationContext{
			ValidationContext: &tlsv3.CertificateValidationContext{TrustedCa: inline(ca)},
		}
	}
	if t.GetCertFile() != "" {
		cert, err := os.ReadFile(t.GetCertFile())
		if err != nil {
			return nil, fmt.Errorf("tls.cert_file: %w", err)
		}
		key, err := os.ReadFile(t.GetKeyFile())
		if err != nil {
			return nil, fmt.Errorf("tls.key_file: %w", err)
		}
		common.TlsCertificates = []*tlsv3.TlsCertificate{{CertificateChain: inline(cert), PrivateKey: inline(key)}}
	}
	return &tlsv3.UpstreamTlsContext{CommonTlsContext: common}, nil
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

// requestOptions builds the request options from what the scenario sets,
// starting from the template's (base may be nil) so a template's headers or
// body survive a scenario that says nothing about them. A scenario's headers
// replace the template's rather than add to them: a plan that lists headers
// means those headers.
func requestOptions(s *plan.Scenario, base *client.RequestOptions) (*client.RequestOptions, error) {
	ro := &client.RequestOptions{}
	if base != nil {
		ro = proto.Clone(base).(*client.RequestOptions)
	}

	methodName := s.GetMethod()
	if methodName == "" && s.GetWebsocket() != nil {
		// The upgrade is a GET whatever a template says.
		methodName = "GET"
	}
	if methodName == "" && s.GetGrpc() != nil {
		// gRPC is POST whatever a template says.
		methodName = "POST"
	}
	if methodName != "" {
		method, err := requestMethod(methodName)
		if err != nil {
			return nil, fmt.Errorf("scenario %q: %w", s.Name, err)
		}
		ro.RequestMethod = method
	}

	if len(s.Headers) > 0 {
		ro.RequestHeaders = nil
	}
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
	if s.Body != "" || s.GetBodyFile() != "" {
		// The scenario's body is the body: a template's request_body_size or
		// json_body would otherwise ride along, and the engine rejects them
		// together.
		ro.RequestBodySize = nil
		ro.JsonBody = ""
	}
	if s.Body != "" {
		// RequestBody is sent verbatim and sets no Content-Type, leaving the
		// header entirely under the plan's control.
		ro.RequestBody = []byte(s.Body)
	}
	if s.GetBodyFile() != "" {
		// Resolved against the plan's directory by the loader.
		body, err := os.ReadFile(s.GetBodyFile())
		if err != nil {
			return nil, fmt.Errorf("scenario %q: body_file: %w", s.Name, err)
		}
		ro.RequestBody = body
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

// Redacted returns a copy of the options fit to print: a client private key
// carried inline by a tls block is replaced with a note of its size. The copy
// is for showing, never for sending.
func Redacted(o *client.CommandLineOptions) *client.CommandLineOptions {
	c := proto.Clone(o).(*client.CommandLineOptions)
	for _, cert := range c.GetTlsContext().GetCommonTlsContext().GetTlsCertificates() {
		if key := cert.GetPrivateKey(); key != nil && len(key.GetInlineBytes()) > 0 {
			cert.PrivateKey = &corev3.DataSource{Specifier: &corev3.DataSource_InlineString{
				InlineString: fmt.Sprintf("<redacted: %d bytes>", len(key.GetInlineBytes())),
			}}
		}
	}
	return c
}
