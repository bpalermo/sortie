package plan

import (
	metricsv3 "github.com/envoyproxy/go-control-plane/envoy/config/metrics/v3"
	"google.golang.org/protobuf/types/known/anypb"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const minimal = `
version: v1
pools:
  - name: local
    services: ["127.0.0.1:8443"]
scenarios:
  - name: smoke
    pool: local
    target: http://127.0.0.1:8080/
    executor:
      type: constant-rate
      rate: 100
      duration: 30s
`

func TestParseMinimal(t *testing.T) {
	p, err := Parse([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Scenarios) != 1 {
		t.Fatalf("got %d scenarios, want 1", len(p.Scenarios))
	}
	if p.Scenarios[0].Executor.Duration.AsDuration() != 30*time.Second {
		t.Errorf("duration = %s", p.Scenarios[0].Executor.Duration.AsDuration())
	}
}

// A typo in a plan must fail the run rather than being silently ignored.
func TestParseRejectsUnknownFields(t *testing.T) {
	bad := strings.Replace(minimal, "      rate: 100", "      rat: 100", 1)
	if _, err := Parse([]byte(bad)); err == nil {
		t.Fatal("expected an error for an unknown field")
	}
}

func TestDefaultsAreInherited(t *testing.T) {
	src := `
version: v1
pools:
  - name: local
    services: ["127.0.0.1:8443"]
defaults:
  pool: local
  target: http://127.0.0.1:8080/
  protocol: http2
  concurrency: "4"
scenarios:
  - name: a
    executor: {type: constant-rate, rate: 10, duration: 5s}
  - name: b
    protocol: http1
    executor: {type: constant-rate, rate: 10, duration: 5s}
`
	p, err := Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	if p.Scenarios[0].Protocol != "http2" {
		t.Errorf("scenario a protocol = %q, want inherited http2", p.Scenarios[0].Protocol)
	}
	if p.Scenarios[1].Protocol != "http1" {
		t.Errorf("scenario b protocol = %q, want its own http1", p.Scenarios[1].Protocol)
	}
	for _, s := range p.Scenarios {
		if s.Pool != "local" || s.Target == "" || s.Concurrency != "4" {
			t.Errorf("scenario %q did not inherit defaults: %+v", s.Name, s)
		}
	}
}

func TestThresholdsAreAdditiveNotInherited(t *testing.T) {
	src := minimal + `
thresholds:
  - "counter:benchmark.http_5xx == 0"
`
	p, err := Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	p.Scenarios[0].Thresholds = []string{"latency_2xx.p95 < 1s"}
	got := EffectiveThresholds(p, p.Scenarios[0])
	if len(got) != 2 {
		t.Fatalf("got %d thresholds, want both the plan's and the scenario's: %v", len(got), got)
	}
}

func TestValidationErrors(t *testing.T) {
	tests := map[string]struct{ src, want string }{
		"wrong version": {
			src:  strings.Replace(minimal, "version: v1", "version: v2", 1),
			want: "unsupported plan version",
		},
		"undeclared pool": {
			src:  strings.Replace(minimal, "pool: local", "pool: nope", 1),
			want: `pool "nope" is not declared`,
		},
		"bad target scheme": {
			src:  strings.Replace(minimal, "http://127.0.0.1:8080/", "ftp://host/", 1),
			want: "target scheme must be http, https, tcp, tcps or udp",
		},
		"ramp without ramp_time": {
			src:  strings.Replace(minimal, "type: constant-rate", "type: ramping-rate", 1),
			want: "requires a ramp_time",
		},
		"stages on constant rate": {
			src: strings.Replace(minimal, "      duration: 30s",
				"      duration: 30s\n      stages: [{rate: 1, duration: 1s}]", 1),
			want: "stages is only valid for the staircase executor",
		},
		"bad threshold": {
			src:  minimal + "thresholds: [\"latency_2xx.p95 500ms\"]\n",
			want: "no comparison operator",
		},
		"grpc with http1": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: http://127.0.0.1:8080/\n    protocol: http1\n    grpc: {mode: unary}", 1),
			want: "grpc requires protocol http2",
		},
		"grpc with GET": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: http://127.0.0.1:8080/\n    method: GET\n    grpc: {mode: unary}", 1),
			want: "grpc requires method POST",
		},
		"unknown grpc mode": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: http://127.0.0.1:8080/\n    grpc: {mode: fast}", 1),
			want: "grpc.mode must be unary or bidi-stream",
		},
		"bidi streams not a multiple of concurrency": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: http://127.0.0.1:8080/\n    concurrency: \"4\"\n    grpc: {mode: bidi-stream, streams: 10}", 1),
			want: "must be a multiple of concurrency",
		},
		"udp block on an http target": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: http://127.0.0.1:8080/\n    udp: {}", 1),
			want: "udp applies to a udp:// target",
		},
		"udp target without a body": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: udp://127.0.0.1:9000", 1),
			want: "a udp target needs a body or body_file",
		},
		"udp target with connections": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: udp://127.0.0.1:9000\n    body: ping\n    connections: 2", 1),
			want: "connections has no meaning with a udp target",
		},
		"udp target with websocket": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: udp://127.0.0.1:9000\n    body: ping\n    websocket: {}", 1),
			want: "grpc, websocket and tcp cannot go with a udp target",
		},
		"tcp block on an http target": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: http://127.0.0.1:8080/\n    tcp: {}", 1),
			want: "tcp applies to a tcp:// or tcps:// target",
		},
		"tcp target without a port": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: tcp://127.0.0.1", 1),
			want: "a tcp target needs an explicit port",
		},
		"tcp target without a body": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: tcp://127.0.0.1:9000", 1),
			want: "a tcp target needs a body or body_file",
		},
		"tcp target with connections": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: tcp://127.0.0.1:9000\n    body: ping\n    connections: 4", 1),
			want: "set tcp.connections",
		},
		"tcp target with headers": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: tcp://127.0.0.1:9000\n    body: ping\n    headers: [\"x: y\"]", 1),
			want: "headers have no meaning with a tcp target",
		},
		"tcp target with websocket": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: tcp://127.0.0.1:9000\n    body: ping\n    websocket: {}", 1),
			want: "grpc and websocket cannot go with a tcp target",
		},
		"websocket with grpc": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: http://127.0.0.1:8080/\n    grpc: {mode: unary}\n    websocket: {}", 1),
			want: "websocket and grpc are mutually exclusive",
		},
		"websocket with http2": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: http://127.0.0.1:8080/\n    protocol: http2\n    websocket: {}", 1),
			want: "websocket requires protocol http1",
		},
		"websocket with POST": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: http://127.0.0.1:8080/\n    method: POST\n    websocket: {}", 1),
			want: "websocket requires method GET",
		},
		"websocket default streams not a multiple of concurrency": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: http://127.0.0.1:8080/\n    concurrency: \"3\"\n    websocket: {}", 1),
			want: "websocket.streams (20, the engine's default) must be a multiple of concurrency (3)",
		},
		"tls on a plain tcp target": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: tcp://127.0.0.1:9000\n    body: ping\n    tls: {ca_file: ca.pem}", 1),
			want: "tls needs an https or tcps target",
		},
		"tls on an http target": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: http://127.0.0.1:8080/\n    tls: {ca_file: ca.pem}", 1),
			want: "tls needs an https or tcps target",
		},
		"tls cert without key": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: https://127.0.0.1:8443/\n    tls: {cert_file: c.pem}", 1),
			want: "cert_file and key_file go together",
		},
		"bidi default streams not a multiple of concurrency": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: http://127.0.0.1:8080/\n    concurrency: \"3\"\n    grpc: {mode: bidi-stream}", 1),
			want: "the engine's default",
		},
		"defaults with body and body_file": {
			src:  minimal + "defaults:\n  body: x\n  body_file: x.bin\n",
			want: "defaults: body and body_file are mutually exclusive",
		},
		"bidi with auto concurrency": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: http://127.0.0.1:8080/\n    concurrency: auto\n    grpc: {mode: bidi-stream}", 1),
			want: "numeric concurrency",
		},
		"stream options on unary": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: http://127.0.0.1:8080/\n    grpc: {mode: unary, streams: 4}", 1),
			want: "apply to mode bidi-stream only",
		},
		"body and body_file": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: http://127.0.0.1:8080/\n    body: x\n    body_file: x.bin", 1),
			want: "mutually exclusive",
		},
		"pool with neither services nor distributor": {
			src:  strings.Replace(minimal, `    services: ["127.0.0.1:8443"]`, "    targets: []", 1),
			want: "set one of services, distributor or dns",
		},
		"pool with services and dns": {
			src:  strings.Replace(minimal, `    services: ["127.0.0.1:8443"]`, `    services: ["127.0.0.1:8443"]`+"\n    dns: engine.test:8443", 1),
			want: "set one of services, distributor or dns, not more than one",
		},
		"pool with distributor and dns": {
			src:  strings.Replace(minimal, `    services: ["127.0.0.1:8443"]`, "    distributor: d.test:8442\n    targets: [\"a.test:1\"]\n    dns: engine.test:8443", 1),
			want: "set one of services, distributor or dns, not more than one",
		},
		"dns without a port": {
			src:  strings.Replace(minimal, `    services: ["127.0.0.1:8443"]`, "    dns: engine.test", 1),
			want: "dns must be host:port",
		},
		"dns with targets": {
			src:  strings.Replace(minimal, `    services: ["127.0.0.1:8443"]`, "    dns: engine.test:8443\n    targets: [\"a.test:1\"]", 1),
			want: "targets is only meaningful together with distributor",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(tc.src))
			if err == nil {
				t.Fatalf("expected an error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to contain %q", err, tc.want)
			}
		})
	}
}

// Nighthawk's linear ramping rate limiter requires ramp_time < duration; a plan
// that violates it should fail at load rather than at the backend.
func TestRampTimeMustBeShorterThanDuration(t *testing.T) {
	src := `
version: v1
pools: [{name: local, services: ["127.0.0.1:8443"]}]
scenarios:
  - name: ramp
    pool: local
    target: http://127.0.0.1:8080/
    executor: {type: ramping-rate, rate: 100, duration: 30s, ramp_time: 30s}
`
	_, err := Parse([]byte(src))
	if err == nil || !strings.Contains(err.Error(), "must be shorter than duration") {
		t.Fatalf("error = %v, want a ramp_time/duration complaint", err)
	}
}

// body_file is resolved against the plan's directory, so a plan and the
// message it sends can be moved together and run from anywhere.
func TestBodyFileIsRelativeToThePlan(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "plan.yaml")
	src := strings.Replace(minimal, "    target: http://127.0.0.1:8080/",
		"    target: http://127.0.0.1:8080/\n    body_file: msgs/hello.bin\n    grpc: {mode: unary}", 1)
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	p, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(dir, "msgs", "hello.bin")
	if got := p.GetScenarios()[0].GetBodyFile(); got != want {
		t.Errorf("body_file = %q, want %q", got, want)
	}
}

func TestForTargetSharesRateByWeightRoundedToNearestAtLeastOne(t *testing.T) {
	s := &Scenario{
		Name:     "mix",
		Executor: &Executor{Type: "staircase", Stages: []*Stage{{Rate: 100}, {Rate: 7}}},
		Targets: []*Target{
			{Name: "a", Url: "http://a/", Weight: 6},
			{Name: "b", Url: "http://b/", Weight: 3},
			{Name: "c", Url: "http://c/"}, // weight defaults to 1
		},
	}
	want := map[string][2]uint32{"a": {60, 4}, "b": {30, 2}, "c": {10, 1}}
	for _, tg := range s.Targets {
		v := ForTarget(s, tg)
		if v.GetTarget() != tg.GetUrl() || len(v.GetTargets()) != 0 {
			t.Errorf("%s: target = %q, targets = %d", tg.Name, v.GetTarget(), len(v.GetTargets()))
		}
		got := [2]uint32{v.GetExecutor().GetStages()[0].GetRate(), v.GetExecutor().GetStages()[1].GetRate()}
		if got != want[tg.Name] {
			t.Errorf("%s: stage rates = %v, want %v", tg.Name, got, want[tg.Name])
		}
	}
	// 7 x 1/10 rounds to 1; a share can never round to zero.
	if v := ForTarget(s, &Target{Name: "tiny", Url: "http://t/", Weight: 1}); v.GetExecutor().GetStages()[1].GetRate() != 1 {
		t.Errorf("tiny share = %d, want 1", v.GetExecutor().GetStages()[1].GetRate())
	}
	if len(s.GetTargets()) != 3 || s.GetTargets()[0].GetWeight() != 6 {
		t.Error("ForTarget mutated the original scenario")
	}
}

func TestParseRejectsBadTargetLists(t *testing.T) {
	for name, scenario := range map[string]string{
		"target and targets": `
    target: http://127.0.0.1:1/
    targets: [{name: a, url: http://127.0.0.1:1/a}]`,
		"unnamed": `
    targets: [{url: http://127.0.0.1:1/a}]`,
		"duplicate name": `
    targets: [{name: a, url: http://127.0.0.1:1/a}, {name: a, url: http://127.0.0.1:1/b}]`,
		"slash in name": `
    targets: [{name: a/b, url: http://127.0.0.1:1/a}]`,
		"bad scheme": `
    targets: [{name: a, url: ftp://127.0.0.1:1/a}]`,
		"tls on a plain target": `
    tls: {ca_file: /dev/null}
    targets: [{name: a, url: https://127.0.0.1:1/a}, {name: b, url: http://127.0.0.1:1/b}]`,
	} {
		raw := `
version: v1
pools:
  - name: local
    services: ["127.0.0.1:1"]
scenarios:
  - name: mix
    pool: local
    executor: {type: constant-rate, rate: 10, duration: 1s}` + scenario + "\n"
		if _, err := Parse([]byte(raw)); err == nil {
			t.Errorf("%s: parsed, want an error", name)
		}
	}
}

// The division rounds from the remainder, so the largest rates and weights
// the schema admits stay exact instead of wrapping.
func TestShareDoesNotOverflow(t *testing.T) {
	const top = ^uint32(0)
	targets := make([]*Target, 5)
	for i := range targets {
		targets[i] = &Target{Name: string(rune('a' + i)), Url: "http://t/", Weight: top}
	}
	s := &Scenario{Name: "big", Executor: &Executor{Type: "constant-rate", Rate: top}, Targets: targets}
	if got := ForTarget(s, targets[0]).GetExecutor().GetRate(); got != 858993459 {
		t.Errorf("share = %d, want 858993459 (MaxUint32 / 5)", got)
	}
}

// defaults is a Scenario: a scenario that names no target takes defaults'
// target or its weighted list, and one that names either takes neither.
func TestTargetsInDefaults(t *testing.T) {
	const head = `
version: v1
pools:
  - name: local
    services: ["127.0.0.1:1"]
`
	p, err := Parse([]byte(head + `
defaults:
  pool: local
  executor: {type: constant-rate, rate: 10, duration: 1s}
  targets:
    - {name: a, url: http://127.0.0.1:1/a, weight: 3}
    - {name: b, url: http://127.0.0.1:1/b}
scenarios:
  - name: inherits
  - name: own-target
    target: http://127.0.0.1:1/own
  - name: own-targets
    targets: [{name: c, url: http://127.0.0.1:1/c}]
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	inherits, ownTarget, ownTargets := p.Scenarios[0], p.Scenarios[1], p.Scenarios[2]
	if len(inherits.GetTargets()) != 2 || inherits.GetTarget() != "" {
		t.Errorf("inherits: target %q, %d targets; want defaults' two targets", inherits.GetTarget(), len(inherits.GetTargets()))
	}
	inherits.Targets[0].Weight = 99
	if p.GetDefaults().GetTargets()[0].GetWeight() != 3 {
		t.Error("a scenario shares its inherited targets with defaults instead of owning a copy")
	}
	if ownTarget.GetTarget() != "http://127.0.0.1:1/own" || len(ownTarget.GetTargets()) != 0 {
		t.Errorf("own-target: target %q, %d targets", ownTarget.GetTarget(), len(ownTarget.GetTargets()))
	}
	if len(ownTargets.GetTargets()) != 1 || ownTargets.GetTargets()[0].GetName() != "c" {
		t.Errorf("own-targets: %v", ownTargets.GetTargets())
	}

	// defaults.target beside a scenario's own targets: the scenario's win, and
	// the plan is not rejected for carrying both forms.
	p, err = Parse([]byte(head + `
defaults:
  pool: local
  target: http://127.0.0.1:1/default
  executor: {type: constant-rate, rate: 10, duration: 1s}
scenarios:
  - name: weighted
    targets: [{name: c, url: http://127.0.0.1:1/c}]
`))
	if err != nil {
		t.Fatalf("Parse with defaults.target and a weighted scenario: %v", err)
	}
	if p.Scenarios[0].GetTarget() != "" || len(p.Scenarios[0].GetTargets()) != 1 {
		t.Errorf("weighted: target %q, %d targets", p.Scenarios[0].GetTarget(), len(p.Scenarios[0].GetTargets()))
	}

	// A bad list in defaults is caught there.
	if _, err := Parse([]byte(head + `
defaults:
  pool: local
  executor: {type: constant-rate, rate: 10, duration: 1s}
  targets: [{name: a, url: http://127.0.0.1:1/a}, {name: a, url: http://127.0.0.1:1/b}]
scenarios:
  - name: s
`)); err == nil || !strings.Contains(err.Error(), "defaults") {
		t.Errorf("err = %v, want the duplicate name reported against defaults", err)
	}
}

// stats has three levels: the plan's block, defaults.stats over it, and a
// scenario's own over both, each replacing wholesale and each scenario owning
// a copy.
func TestStatsPrecedence(t *testing.T) {
	p, err := Parse([]byte(`
version: v1
stats:
  prefix: fromplan
  statsd: {address: "10.0.0.1:8125"}
pools:
  - name: local
    services: ["127.0.0.1:1"]
defaults:
  pool: local
  target: http://127.0.0.1:1/
  executor: {type: constant-rate, rate: 10, duration: 1s}
scenarios:
  - name: inherits
  - name: own
    stats:
      statsd: {address: "10.0.0.3:8125"}
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := p.Scenarios[0].GetStats(); got.GetPrefix() != "fromplan" || got.GetStatsd().GetAddress() != "10.0.0.1:8125" {
		t.Errorf("inherits: stats = %v, want the plan's", got)
	}
	// Wholesale: the scenario's block does not pick up the plan's prefix.
	if got := p.Scenarios[1].GetStats(); got.GetPrefix() != "" || got.GetStatsd().GetAddress() != "10.0.0.3:8125" {
		t.Errorf("own: stats = %v, want only the scenario's", got)
	}
	p.Scenarios[0].Stats.Prefix = "changed"
	if p.GetStats().GetPrefix() != "fromplan" {
		t.Error("a scenario shares its inherited stats with the plan instead of owning a copy")
	}

	p, err = Parse([]byte(`
version: v1
stats:
  statsd: {address: "10.0.0.1:8125"}
pools:
  - name: local
    services: ["127.0.0.1:1"]
defaults:
  pool: local
  target: http://127.0.0.1:1/
  executor: {type: constant-rate, rate: 10, duration: 1s}
  stats:
    statsd: {address: "10.0.0.2:8125"}
scenarios:
  - name: inherits
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := p.Scenarios[0].GetStats().GetStatsd().GetAddress(); got != "10.0.0.2:8125" {
		t.Errorf("statsd address = %q, want defaults' over the plan's", got)
	}
}

func TestStatsRejectsWhatWouldMisbehaveOnTheBackend(t *testing.T) {
	const head = `
version: v1
pools:
  - name: local
    services: ["127.0.0.1:1"]
defaults:
  pool: local
  target: http://127.0.0.1:1/
  executor: {type: constant-rate, rate: 10, duration: 1s}
`
	for name, c := range map[string]struct{ body, want string }{
		"the otlp sink through the passthrough": {`
stats:
  sinks: [{name: envoy.stat_sinks.open_telemetry}]
scenarios: [{name: a}]`, "aborts on its first flush"},
		"a statsd host name": {`
stats:
  statsd: {address: "collector.monitoring:8125"}
scenarios: [{name: a}]`, "not an IP address"},
		"names that differ only in case": {`
stats:
  statsd: {address: "10.0.0.1:8125"}
scenarios: [{name: Foo}, {name: foo}]`, "same prefix"},
		"a name with nothing to keep": {`
stats:
  statsd: {address: "10.0.0.1:8125"}
scenarios: [{name: "///"}]`, "no letter, digit or underscore"},
	} {
		_, err := Parse([]byte(head + c.body + "\n"))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want one containing %q", name, err, c.want)
		}
	}
	for name, c := range map[string]struct{ body, want string }{
		"a flush interval below a millisecond": {`
stats:
  flush_interval: 0.000001s
  statsd: {address: "10.0.0.1:8125"}
scenarios: [{name: a}]`, "flush_interval"},
		"the adapter written out": {`
stats:
  sinks: [{name: nighthawk.envoy_stats_sink_adapter}]
scenarios: [{name: a}]`, "sortie adds the adapter"},
		"a stage label and a scenario of that name": {`
stats:
  statsd: {address: "10.0.0.1:8125"}
scenarios:
  - name: foo
    executor: {type: staircase, stages: [{rate: 10, duration: 1s}]}
  - name: foo/stage-1`, "same prefix (sortie.foo.stage_1)"},
		"the default prefix and the same one spelled out": {`
scenarios:
  - {name: foo, stats: {statsd: {address: "10.0.0.1:8125"}}}
  - {name: Foo, stats: {prefix: sortie, statsd: {address: "10.0.0.1:8125"}}}`, "same prefix (sortie.foo)"},
		"targets that differ only in case": {`
stats:
  statsd: {address: "10.0.0.1:8125"}
scenarios:
  - name: mix
    targets:
      - {name: US-East, url: "http://127.0.0.1:1/a"}
      - {name: us-east, url: "http://127.0.0.1:1/b"}`, "same prefix (sortie.mix.us_east)"},
		"a target and a scenario of that name": {`
stats:
  statsd: {address: "10.0.0.1:8125"}
scenarios:
  - name: mix
    targets: [{name: a, url: "http://127.0.0.1:1/a"}]
  - name: mix/a`, "same prefix (sortie.mix.a)"},
		"a statsd port out of range": {`
stats:
  statsd: {address: "10.0.0.1:99999"}
scenarios: [{name: a}]`, "not in 1..65535"},
		"a statsd port of zero": {`
stats:
  statsd: {address: "10.0.0.1:0"}
scenarios: [{name: a}]`, "not in 1..65535"},
		"a dotted prefix that meets another": {`
scenarios:
  - {name: b, stats: {prefix: x.a, statsd: {address: "10.0.0.1:8125"}}}
  - {name: a/b, stats: {prefix: x, statsd: {address: "10.0.0.1:8125"}}}`, "same prefix (x.a.b)"},
	} {
		_, err := Parse([]byte(head + c.body + "\n"))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want one containing %q", name, err, c.want)
		}
	}
	// Without a statsd sink the same names are fine -- a passthrough sink
	// names its own metrics -- and so are colliding names under different
	// prefixes.
	for name, body := range map[string]string{
		"no stats": `
scenarios: [{name: Foo}, {name: foo}]`,
		"only passthrough sinks": `
stats:
  sinks: [{name: envoy.stat_sinks.dog_statsd}]
scenarios: [{name: Foo}, {name: foo}]`,
		"different prefixes": `
scenarios:
  - {name: Foo, stats: {prefix: one, statsd: {address: "10.0.0.1:8125"}}}
  - {name: foo, stats: {prefix: two, statsd: {address: "10.0.0.1:8125"}}}`,
	} {
		if _, err := Parse([]byte(head + body + "\n")); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// A sink is judged by the type of its configuration as well as by its name:
// Envoy picks a sink's factory from the typed config when the name matches
// none, so a renamed OpenTelemetry sink, or a renamed adapter carrying one,
// is still what its config says. Built in Go: neither type is linked into
// sortie, so a YAML plan cannot even spell them, and this is the guard for a
// plan that arrives as a message.
func TestStatsJudgesASinkByItsConfigType(t *testing.T) {
	for name, c := range map[string]struct{ typeURL, want string }{
		"otlp under another name": {
			"type.googleapis.com/envoy.extensions.stat_sinks.open_telemetry.v3.SinkConfig", "aborts on its first flush",
		},
		"the adapter under another name": {
			"type.googleapis.com/nighthawk.EnvoyStatsSinkAdapterConfig", "sortie adds the adapter",
		},
	} {
		st := &Stats{Sinks: []*metricsv3.StatsSink{{
			Name:       "anything",
			ConfigType: &metricsv3.StatsSink_TypedConfig{TypedConfig: &anypb.Any{TypeUrl: c.typeURL}},
		}}}
		if err := validateStats(st); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want one containing %q", name, err, c.want)
		}
	}
	ok := &Stats{Sinks: []*metricsv3.StatsSink{{
		Name:       "envoy.stat_sinks.dog_statsd",
		ConfigType: &metricsv3.StatsSink_TypedConfig{TypedConfig: &anypb.Any{TypeUrl: "type.googleapis.com/envoy.config.metrics.v3.DogStatsdSink"}},
	}}}
	if err := validateStats(ok); err != nil {
		t.Errorf("an ordinary sink was refused: %v", err)
	}
}

// stats.sinks reaches the sinks configured by an envoy.config.metrics.v3
// message, which sortie links, and no extension's own config type: that one
// fails at parse, as the schema says.
func TestStatsSinksAcceptTheMetricsV3ConfigTypesOnly(t *testing.T) {
	plan := func(typ string) string {
		return `
version: v1
stats:
  sinks:
    - name: a-sink
      typed_config:
        "@type": type.googleapis.com/` + typ + `
pools:
  - name: local
    services: ["127.0.0.1:1"]
scenarios:
  - name: s
    pool: local
    target: http://127.0.0.1:1/
    executor: {type: constant-rate, rate: 10, duration: 1s}
`
	}
	for _, typ := range []string{
		"envoy.config.metrics.v3.DogStatsdSink",
		"envoy.config.metrics.v3.StatsdSink",
		"envoy.config.metrics.v3.HystrixSink",
		"envoy.config.metrics.v3.MetricsServiceConfig",
	} {
		if _, err := Parse([]byte(plan(typ))); err != nil {
			t.Errorf("%s: %v", typ, err)
		}
	}
	if _, err := Parse([]byte(plan("envoy.extensions.stat_sinks.graphite_statsd.v3.GraphiteStatsdSink"))); err == nil {
		t.Error("an extension's config type parsed; the schema and README say it cannot")
	}
}
