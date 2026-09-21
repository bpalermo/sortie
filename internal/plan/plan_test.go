package plan

import (
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
			want: "target scheme must be http or https",
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
