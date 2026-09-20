package compile

import (
	"testing"
	"time"

	client "github.com/envoyproxy/nighthawk/api/client"
	ratelimiter "github.com/envoyproxy/nighthawk/api/rate_limiter"
	"github.com/bpalermo/sortie/internal/plan"
)

func dur(d time.Duration) plan.Duration { return plan.Duration(d) }

func scenario(e plan.Executor) plan.Scenario {
	return plan.Scenario{Name: "s", Pool: "p", Target: "http://example.test/", Executor: e}
}

func TestExpandConstantRate(t *testing.T) {
	execs, err := Expand(scenario(plan.Executor{
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
	execs, err := Expand(scenario(plan.Executor{
		Type: plan.RampingRate, Rate: 500, Duration: dur(60 * time.Second), RampTime: &ramp,
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
	execs, err := Expand(scenario(plan.Executor{
		Type: plan.Staircase,
		Stages: []plan.Stage{
			{Rate: 100, Duration: dur(10 * time.Second)},
			{Rate: 200, Duration: dur(20 * time.Second)},
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
	execs, err := Expand(scenario(plan.Executor{
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
	s := scenario(plan.Executor{Type: plan.ConstantRate, Rate: 200, Duration: dur(time.Second)})
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
	s := scenario(plan.Executor{Type: plan.ConstantRate, Rate: 1200, Duration: dur(time.Second)})
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
	s := scenario(plan.Executor{Type: plan.ConstantRate, Rate: 101, Duration: dur(time.Second)})
	s.Concurrency = "2"
	execs, _ := Expand(s)
	if _, err := Divide(execs[0], 1); err == nil {
		t.Fatal("101 rps over 2 workers is not expressible and must be an error")
	}
}

// "auto" leaves the worker count to the backend, so sortie cannot honour an
// aggregate rate and must say so instead of silently generating the wrong load.
func TestDivideRejectsAutoConcurrency(t *testing.T) {
	s := scenario(plan.Executor{Type: plan.ConstantRate, Rate: 100, Duration: dur(time.Second)})
	s.Concurrency = "auto"
	execs, _ := Expand(s)
	_, err := Divide(execs[0], 1)
	if err == nil {
		t.Fatal("auto concurrency must be refused when a rate is specified")
	}
}

func TestDivideRejectsRateBelowBackendCount(t *testing.T) {
	execs, _ := Expand(scenario(plan.Executor{
		Type: plan.ConstantRate, Rate: 2, Duration: dur(time.Second),
	}))
	if _, err := Divide(execs[0], 3); err == nil {
		t.Fatal("dividing 2 rps over 3 backends should be an error, not a silent zero")
	}
}

// Divide must not alias the shared Options between backends.
func TestDivideDoesNotAlias(t *testing.T) {
	execs, _ := Expand(scenario(plan.Executor{
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
	s := scenario(plan.Executor{Type: plan.ConstantRate, Rate: 1, Duration: dur(time.Second)})
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
