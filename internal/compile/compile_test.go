package compile

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/bpalermo/sortie/internal/plan"
	client "github.com/envoyproxy/nighthawk/api/client"
	ratelimiter "github.com/envoyproxy/nighthawk/api/rate_limiter"
)

func dur(d time.Duration) *durationpb.Duration { return durationpb.New(d) }

func scenario(e *plan.Executor) *plan.Scenario {
	return &plan.Scenario{Name: "s", Pool: "p", Target: "http://example.test/", Executor: e}
}

func TestExpandConstantRate(t *testing.T) {
	execs, err := Expand(scenario(&plan.Executor{
		Type: plan.ConstantRate, Rate: 100, Duration: dur(30 * time.Second),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(execs) != 1 {
		t.Fatalf("got %d executions, want 1", len(execs))
	}
	o := execs[0].Options
	if got := o.GetRequestsPerSecond().GetValue(); got != 100 {
		t.Errorf("rps = %d, want 100", got)
	}
	if got := o.GetDuration().AsDuration(); got != 30*time.Second {
		t.Errorf("duration = %s, want 30s", got)
	}
	if o.GetUri().GetValue() != "http://example.test/" {
		t.Errorf("uri = %q", o.GetUri().GetValue())
	}
	if o.GetRateLimiterPluginConfig() != nil {
		t.Error("constant-rate must not configure a rate limiter plugin")
	}
}

func TestExpandRampingRateConfiguresPlugin(t *testing.T) {
	ramp := dur(10 * time.Second)
	execs, err := Expand(scenario(&plan.Executor{
		Type: plan.RampingRate, Rate: 500, Duration: dur(60 * time.Second), RampTime: ramp,
	}))
	if err != nil {
		t.Fatal(err)
	}
	cfg := execs[0].Options.GetRateLimiterPluginConfig()
	if cfg == nil {
		t.Fatal("ramping-rate must configure a rate limiter plugin")
	}
	if cfg.GetName() != LinearRampingRateLimiterPlugin {
		t.Errorf("plugin name = %q, want %q", cfg.GetName(), LinearRampingRateLimiterPlugin)
	}
	var got ratelimiter.LinearRampingRateLimiterConfig
	if err := cfg.GetTypedConfig().UnmarshalTo(&got); err != nil {
		t.Fatalf("unpacking plugin config: %v", err)
	}
	if got.GetRampTime().AsDuration() != 10*time.Second {
		t.Errorf("ramp_time = %s, want 10s", got.GetRampTime().AsDuration())
	}
}

func TestExpandStaircaseYieldsOneExecutionPerStage(t *testing.T) {
	execs, err := Expand(scenario(&plan.Executor{
		Type: plan.Staircase,
		Stages: []*plan.Stage{
			&plan.Stage{Rate: 100, Duration: dur(10 * time.Second)},
			&plan.Stage{Rate: 200, Duration: dur(20 * time.Second)},
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(execs) != 2 {
		t.Fatalf("got %d executions, want 2", len(execs))
	}
	if execs[0].Label != "s/stage-1" || execs[1].Label != "s/stage-2" {
		t.Errorf("labels = %q, %q", execs[0].Label, execs[1].Label)
	}
	if execs[1].Options.GetRequestsPerSecond().GetValue() != 200 {
		t.Errorf("stage 2 rps = %d, want 200", execs[1].Options.GetRequestsPerSecond().GetValue())
	}
}

// The plan's rate is the aggregate the target sees, so it must be divided
// across backends, with the remainder spread rather than dropped.
func TestDivideSpreadsRemainder(t *testing.T) {
	execs, err := Expand(scenario(&plan.Executor{
		Type: plan.ConstantRate, Rate: 100, Duration: dur(time.Second),
	}))
	if err != nil {
		t.Fatal(err)
	}
	perBackend, err := Divide(execs[0], 3)
	if err != nil {
		t.Fatal(err)
	}
	total := uint32(0)
	for _, o := range perBackend {
		total += o.GetRequestsPerSecond().GetValue()
	}
	if total != 100 {
		t.Errorf("rates sum to %d, want 100", total)
	}
	if got := perBackend[0].GetRequestsPerSecond().GetValue(); got != 34 {
		t.Errorf("first backend = %d, want 34", got)
	}
	if got := perBackend[2].GetRequestsPerSecond().GetValue(); got != 33 {
		t.Errorf("last backend = %d, want 33", got)
	}
}

// Nighthawk's --rps is per worker thread, so concurrency multiplies the load a
// backend generates and must divide the plan's aggregate rate.
func TestDivideAccountsForWorkerThreads(t *testing.T) {
	s := scenario(&plan.Executor{Type: plan.ConstantRate, Rate: 200, Duration: dur(time.Second)})
	s.Concurrency = "2"
	execs, err := Expand(s)
	if err != nil {
		t.Fatal(err)
	}
	perBackend, err := Divide(execs[0], 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := perBackend[0].GetRequestsPerSecond().GetValue(); got != 100 {
		t.Errorf("--rps = %d, want 100 so that 2 workers emit 200 rps", got)
	}
}

func TestDivideAcrossBackendsAndWorkers(t *testing.T) {
	s := scenario(&plan.Executor{Type: plan.ConstantRate, Rate: 1200, Duration: dur(time.Second)})
	s.Concurrency = "4"
	execs, _ := Expand(s)
	perBackend, err := Divide(execs[0], 3)
	if err != nil {
		t.Fatal(err)
	}
	total := uint32(0)
	for _, o := range perBackend {
		total += o.GetRequestsPerSecond().GetValue() * 4
	}
	if total != 1200 {
		t.Errorf("aggregate = %d, want 1200", total)
	}
}

// A rate that no integer per-worker --rps can express must be refused rather
// than rounded to something the plan did not ask for.
func TestDivideRejectsRateNotDivisibleByWorkers(t *testing.T) {
	s := scenario(&plan.Executor{Type: plan.ConstantRate, Rate: 101, Duration: dur(time.Second)})
	s.Concurrency = "2"
	execs, _ := Expand(s)
	if _, err := Divide(execs[0], 1); err == nil {
		t.Fatal("101 rps over 2 workers is not expressible and must be an error")
	}
}

// "auto" leaves the worker count to the backend, so sortie cannot honour an
// aggregate rate and must say so instead of silently generating the wrong load.
func TestDivideRejectsAutoConcurrency(t *testing.T) {
	s := scenario(&plan.Executor{Type: plan.ConstantRate, Rate: 100, Duration: dur(time.Second)})
	s.Concurrency = "auto"
	execs, _ := Expand(s)
	_, err := Divide(execs[0], 1)
	if err == nil {
		t.Fatal("auto concurrency must be refused when a rate is specified")
	}
}

func TestDivideRejectsRateBelowBackendCount(t *testing.T) {
	execs, _ := Expand(scenario(&plan.Executor{
		Type: plan.ConstantRate, Rate: 2, Duration: dur(time.Second),
	}))
	if _, err := Divide(execs[0], 3); err == nil {
		t.Fatal("dividing 2 rps over 3 backends should be an error, not a silent zero")
	}
}

// Divide must not alias the shared Options between backends.
func TestDivideDoesNotAlias(t *testing.T) {
	execs, _ := Expand(scenario(&plan.Executor{
		Type: plan.ConstantRate, Rate: 10, Duration: dur(time.Second),
	}))
	perBackend, err := Divide(execs[0], 2)
	if err != nil {
		t.Fatal(err)
	}
	if perBackend[0] == perBackend[1] {
		t.Fatal("backends share the same Options pointer")
	}
	perBackend[0].RequestsPerSecond = nil
	if perBackend[1].GetRequestsPerSecond().GetValue() != 5 {
		t.Fatal("mutating one backend's options affected another")
	}
	if execs[0].Options.GetRequestsPerSecond().GetValue() != 10 {
		t.Fatal("Divide mutated the source execution")
	}
}

func TestProtocolAndMethod(t *testing.T) {
	s := scenario(&plan.Executor{Type: plan.ConstantRate, Rate: 1, Duration: dur(time.Second)})
	s.Protocol = "http2"
	s.Method = "post"
	s.Headers = []string{"X-Test: yes"}
	s.Body = `{"a":1}`

	execs, err := Expand(s)
	if err != nil {
		t.Fatal(err)
	}
	o := execs[0].Options
	if o.GetProtocol().GetValue() != client.Protocol_HTTP2 {
		t.Errorf("protocol = %v, want HTTP2", o.GetProtocol().GetValue())
	}
	ro := o.GetRequestOptions()
	if ro.GetRequestMethod().String() != "POST" {
		t.Errorf("method = %v, want POST", ro.GetRequestMethod())
	}
	if len(ro.GetRequestHeaders()) != 1 ||
		ro.GetRequestHeaders()[0].GetHeader().GetKey() != "X-Test" ||
		ro.GetRequestHeaders()[0].GetHeader().GetValue() != "yes" {
		t.Errorf("headers = %v", ro.GetRequestHeaders())
	}
	if string(ro.GetRequestBody()) != `{"a":1}` {
		t.Errorf("body = %q", ro.GetRequestBody())
	}
}

// A distributor forwards one ExecutionRequest unchanged to every target, so the
// single options it receives must already carry the per-target share. Dividing
// again at dispatch, or not dividing at all, both make a plan's rate mean
// something different on this path than on the direct one.
func TestForPoolDistributorSharesRateAcrossTargets(t *testing.T) {
	s := scenario(&plan.Executor{Type: plan.ConstantRate, Rate: 600, Duration: dur(time.Second)})
	s.Concurrency = "2"
	execs, err := Expand(s)
	if err != nil {
		t.Fatal(err)
	}
	pool := &plan.Pool{
		Name:        "fleet",
		Distributor: "d:1",
		Targets:     []string{"a:1", "b:1", "c:1"},
	}

	addrs, perBackend, err := ForPool(execs[0], pool)
	if err != nil {
		t.Fatal(err)
	}
	if len(perBackend) != 1 {
		t.Fatalf("got %d options, want 1: the distributor receives a single request", len(perBackend))
	}
	if got := len(addrs); got != 3 {
		t.Fatalf("got %d addrs, want the 3 targets", got)
	}
	// 600 aggregate over 3 targets x 2 workers.
	if got := perBackend[0].GetRequestsPerSecond().GetValue(); got != 100 {
		t.Errorf("--rps = %d, want 100 so 3 targets x 2 workers emit 600 rps", got)
	}
}

// Only one options object goes to the distributor, so there is nowhere to put a
// remainder. Refusing beats emitting a rate the plan did not ask for.
func TestForPoolDistributorRejectsIndivisibleRate(t *testing.T) {
	s := scenario(&plan.Executor{Type: plan.ConstantRate, Rate: 100, Duration: dur(time.Second)})
	execs, _ := Expand(s)
	pool := &plan.Pool{Name: "fleet", Distributor: "d:1", Targets: []string{"a:1", "b:1", "c:1"}}

	if _, _, err := ForPool(execs[0], pool); err == nil {
		t.Fatal("100 rps over 3 targets is not expressible in one options and must be refused")
	}
}

func TestForPoolDirectPoolDividesPerBackend(t *testing.T) {
	s := scenario(&plan.Executor{Type: plan.ConstantRate, Rate: 100, Duration: dur(time.Second)})
	execs, _ := Expand(s)
	pool := &plan.Pool{Name: "local", Services: []string{"a:1", "b:1", "c:1"}}

	addrs, perBackend, err := ForPool(execs[0], pool)
	if err != nil {
		t.Fatal(err)
	}
	if len(perBackend) != 3 || len(addrs) != 3 {
		t.Fatalf("got %d options for %d addrs, want 3 and 3", len(perBackend), len(addrs))
	}
	total := uint32(0)
	for _, o := range perBackend {
		total += o.GetRequestsPerSecond().GetValue()
	}
	if total != 100 {
		t.Errorf("rates sum to %d, want 100", total)
	}
}

// nighthawk_template exists so a plan can reach Nighthawk options this schema
// does not model. Anything set there must survive compilation untouched.
func TestNighthawkTemplateIsCarriedThrough(t *testing.T) {
	s := scenario(&plan.Executor{Type: plan.ConstantRate, Rate: 10, Duration: dur(time.Second)})
	s.NighthawkTemplate = &client.CommandLineOptions{
		// Not modelled by the plan schema, and no sortie field touches it.
		MaxRequestsPerConnection: wrapperspb.UInt32(7),
		BurstSize:                wrapperspb.UInt32(3),
	}

	execs, err := Expand(s)
	if err != nil {
		t.Fatal(err)
	}
	o := execs[0].Options
	if got := o.GetMaxRequestsPerConnection().GetValue(); got != 7 {
		t.Errorf("max_requests_per_connection = %d, want the template's 7", got)
	}
	if got := o.GetBurstSize().GetValue(); got != 3 {
		t.Errorf("burst_size = %d, want the template's 3", got)
	}
}

// The fields sortie owns are its own, whatever the template says: a template
// rate would otherwise silently win over the plan's executor.
func TestNighthawkTemplateDoesNotOverrideOwnedFields(t *testing.T) {
	s := scenario(&plan.Executor{Type: plan.ConstantRate, Rate: 50, Duration: dur(30 * time.Second)})
	s.NighthawkTemplate = &client.CommandLineOptions{
		RequestsPerSecond: wrapperspb.UInt32(9999),
		ExecutionId:       wrapperspb.String("from-template"),
	}

	execs, err := Expand(s)
	if err != nil {
		t.Fatal(err)
	}
	o := execs[0].Options
	if got := o.GetRequestsPerSecond().GetValue(); got != 50 {
		t.Errorf("rps = %d, want the executor's 50, not the template's", got)
	}
	if got := o.GetExecutionId().GetValue(); got != "s" {
		t.Errorf("execution_id = %q, want the scenario name", got)
	}
	if got := o.GetDuration().AsDuration(); got != 30*time.Second {
		t.Errorf("duration = %s, want 30s", got)
	}
}

// Two executions of one staircase scenario must not share the template.
func TestNighthawkTemplateIsClonedPerExecution(t *testing.T) {
	s := scenario(&plan.Executor{
		Type: plan.Staircase,
		Stages: []*plan.Stage{
			{Rate: 10, Duration: dur(time.Second)},
			{Rate: 20, Duration: dur(time.Second)},
		},
	})
	s.NighthawkTemplate = &client.CommandLineOptions{BurstSize: wrapperspb.UInt32(3)}

	execs, err := Expand(s)
	if err != nil {
		t.Fatal(err)
	}
	if execs[0].Options == execs[1].Options {
		t.Fatal("stages share one options object")
	}
	execs[0].Options.BurstSize = nil
	if execs[1].Options.GetBurstSize().GetValue() != 3 {
		t.Error("mutating one stage's options affected another")
	}
	if s.NighthawkTemplate.GetBurstSize().GetValue() != 3 {
		t.Error("compilation mutated the scenario's template")
	}
}
