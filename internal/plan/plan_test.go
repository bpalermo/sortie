package plan

import (
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
		"tls on an http target": {
			src:  strings.Replace(minimal, "    target: http://127.0.0.1:8080/", "    target: http://127.0.0.1:8080/\n    tls: {ca_file: ca.pem}", 1),
			want: "tls needs an https target",
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
			want: "set one of services or distributor",
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
