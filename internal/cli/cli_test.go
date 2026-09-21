package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	client "github.com/envoyproxy/nighthawk/api/client"
)

// The exit codes are a contract with whatever runs sortie in CI: 2 means the
// plan or the command line was wrong, 1 means the load test said no. Confusing
// the two sends someone debugging a performance regression that is really a
// typo, so each one is pinned here rather than left to the error plumbing.

// fakeBackend answers every execution with a fixed, plausible result.
type fakeBackend struct {
	client.UnimplementedNighthawkServiceServer
	p95      time.Duration
	requests uint64
}

func (f *fakeBackend) ExecutionStream(stream client.NighthawkService_ExecutionStreamServer) error {
	for {
		if _, err := stream.Recv(); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := stream.Send(&client.ExecutionResponse{
			Output: &client.Output{Results: []*client.Result{{
				Name:              "global",
				ExecutionDuration: durationpb.New(time.Second),
				Statistics: []*client.Statistic{{
					Id: "benchmark_http_client.latency_2xx",
					Percentiles: []*client.Percentile{{
						Percentile:   1,
						DurationType: &client.Percentile_Duration{Duration: durationpb.New(f.p95)},
					}},
				}},
				Counters: []*client.Counter{{Name: "benchmark.http_2xx", Value: f.requests}},
			}}},
		}); err != nil {
			return err
		}
	}
}

func startBackend(t *testing.T, p95 time.Duration) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	client.RegisterNighthawkServiceServer(server, &fakeBackend{p95: p95, requests: 100})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

// writePlan writes a plan naming the given backend and returns its path.
func writePlan(t *testing.T, backend, thresholds string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "plan.yaml")
	body := fmt.Sprintf(`
version: v1
pools:
  - name: local
    services: ["%s"]
scenarios:
  - name: smoke
    pool: local
    target: http://127.0.0.1:1/
    executor: {type: constant-rate, rate: 10, duration: 1s}
%s
`, backend, thresholds)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func runCLI(args ...string) (code int, stdout, stderr string) {
	var out, errBuf bytes.Buffer
	code = execute("test", args, &out, &errBuf)
	return code, out.String(), errBuf.String()
}

func TestExitCodeZeroWhenThresholdsHold(t *testing.T) {
	plan := writePlan(t, startBackend(t, 5*time.Millisecond), `    thresholds: ["latency_2xx.p95 < 50ms"]`)

	code, stdout, stderr := runCLI("run", plan)
	if code != exitOK {
		t.Fatalf("exit = %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitOK, stdout, stderr)
	}
	if !strings.Contains(stdout, "PASS") {
		t.Errorf("report should say PASS:\n%s", stdout)
	}
}

func TestExitCodeOneWhenAThresholdFails(t *testing.T) {
	plan := writePlan(t, startBackend(t, 900*time.Millisecond), `    thresholds: ["latency_2xx.p95 < 50ms"]`)

	code, stdout, _ := runCLI("run", plan)
	if code != exitFailed {
		t.Fatalf("exit = %d, want %d for a failed threshold", code, exitFailed)
	}
	if !strings.Contains(stdout, "FAIL") {
		t.Errorf("report should say FAIL:\n%s", stdout)
	}
}

// An unreachable backend is a failed run, not a malformed plan.
func TestExitCodeOneWhenTheBackendIsUnreachable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := listener.Addr().String()
	_ = listener.Close()

	code, _, _ := runCLI("run", writePlan(t, dead, ""))
	if code != exitFailed {
		t.Fatalf("exit = %d, want %d for an unreachable backend", code, exitFailed)
	}
}

// Everything below is the caller's mistake rather than the run's result, and
// must be distinguishable from a load-test failure.
func TestExitCodeTwoForUsageErrors(t *testing.T) {
	good := writePlan(t, "127.0.0.1:1", "")
	badPlan := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(badPlan, []byte("version: v2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	undispatchable := filepath.Join(t.TempDir(), "odd.yaml")
	if err := os.WriteFile(undispatchable, []byte(`
version: v1
pools: [{name: local, services: ["127.0.0.1:1"]}]
scenarios:
  - name: odd
    pool: local
    target: http://127.0.0.1:1/
    concurrency: "3"
    executor: {type: constant-rate, rate: 100, duration: 1s}
`), 0o600); err != nil {
		t.Fatal(err)
	}

	for name, args := range map[string][]string{
		"no arguments":        {},
		"unknown command":     {"stampede", good},
		"missing plan file":   {"validate"},
		"too many plan files": {"validate", good, good},
		"nonexistent file":    {"validate", filepath.Join(t.TempDir(), "absent.yaml")},
		"unsupported version": {"validate", badPlan},
		"undispatchable plan": {"validate", undispatchable},
		"unknown flag":        {"run", "--charge", good},
		"bad plan to compile": {"compile", badPlan},
		"bad plan to run":     {"run", badPlan},
	} {
		t.Run(name, func(t *testing.T) {
			code, stdout, stderr := runCLI(args...)
			if code != exitBadUsage {
				t.Fatalf("exit = %d, want %d\nstdout:\n%s\nstderr:\n%s",
					code, exitBadUsage, stdout, stderr)
			}
		})
	}
}

func TestExitCodeZeroForInformationalCommands(t *testing.T) {
	good := writePlan(t, "127.0.0.1:1", "")
	for name, args := range map[string][]string{
		"help":     {"--help"},
		"version":  {"--version"},
		"validate": {"validate", good},
		"compile":  {"compile", good},
	} {
		t.Run(name, func(t *testing.T) {
			if code, stdout, stderr := runCLI(args...); code != exitOK {
				t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
			}
		})
	}
}

// -o redirects the report to a file, which is how a CI job keeps the artefact
// while leaving stdout for the human.
func TestRunWritesTheReportToAFile(t *testing.T) {
	plan := writePlan(t, startBackend(t, 5*time.Millisecond), `    thresholds: ["latency_2xx.p95 < 50ms"]`)
	out := filepath.Join(t.TempDir(), "report.json")

	code, stdout, stderr := runCLI("run", "--json", "-o", out, plan)
	if code != exitOK {
		t.Fatalf("exit = %d\nstderr:\n%s", code, stderr)
	}
	if strings.Contains(stdout, "{") {
		t.Errorf("the report went to stdout as well as the file:\n%s", stdout)
	}

	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("reading the report: %v", err)
	}
	var parsed struct {
		Pass bool `json:"pass"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("the written report does not parse: %v\n%s", err, raw)
	}
	if !parsed.Pass {
		t.Error("the written report should record a pass")
	}
}
