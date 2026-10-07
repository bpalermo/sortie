package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	client "github.com/bpalermo/sortie/engine/api/client"
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
	// calls counts the executions it was asked for.
	calls atomic.Int64
}

func (f *fakeBackend) ExecutionStream(stream client.NighthawkService_ExecutionStreamServer) error {
	for {
		if _, err := stream.Recv(); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		f.calls.Add(1)
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
		t.Errorf("the JSON report went to stdout as well as the file:\n%s", stdout)
	}
	// stdout keeps the readable summary, so a pod's log is never empty.
	if !strings.Contains(stdout, "PASS  1/1 executions passed") {
		t.Errorf("stdout lacks the text summary:\n%s", stdout)
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

// A dns pool is accepted by validate and compile without being resolved: the
// backends exist only once the run looks the name up, and neither command
// should need a network to do its job.
func TestValidateAndCompileAcceptADnsPoolWithoutResolving(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dns.yaml")
	if err := os.WriteFile(path, []byte(`
version: v1
pools: [{name: nodes, dns: engine.sortie-test.invalid:8443}]
scenarios:
  - name: soak
    pool: nodes
    target: http://target.invalid/
    concurrency: "2"
    executor: {type: constant-rate, rate: 60, duration: 1s, per_backend: true}
`), 0o600); err != nil {
		t.Fatal(err)
	}

	code, stdout, stderr := runCLI("validate", path)
	if code != exitOK {
		t.Fatalf("validate exit = %d\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "1 resolved from DNS when the run starts") {
		t.Errorf("validate should say the pool is resolved later:\n%s", stdout)
	}

	code, stdout, stderr = runCLI("compile", path)
	if code != exitOK {
		t.Fatalf("compile exit = %d\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "engine.sortie-test.invalid:8443 (resolved when the run starts; each backend receives this)") {
		t.Errorf("compile should name the unresolved pool:\n%s", stdout)
	}
	// protojson varies its whitespace on purpose.
	if !regexp.MustCompile(`"requestsPerSecond":\s+30,`).MatchString(stdout) {
		t.Errorf("compile should show one backend's share (60 per backend over 2 workers):\n%s", stdout)
	}
}

// emptyResolver answers every name with no address.
type emptyResolver struct{}

func (emptyResolver) LookupHost(context.Context, string) ([]string, error) { return nil, nil }

// A dns pool whose name has no address is the plan's fault, not the run's:
// the run fails before any load with the usage exit code.
func TestExitCodeTwoWhenADnsPoolResolvesToNothing(t *testing.T) {
	// An empty answer is retried for the grace period, which the test cuts short.
	prevResolver, prevTimeout := resolver, resolveTimeout
	resolver, resolveTimeout = emptyResolver{}, time.Millisecond
	t.Cleanup(func() { resolver, resolveTimeout = prevResolver, prevTimeout })

	path := filepath.Join(t.TempDir(), "dns.yaml")
	if err := os.WriteFile(path, []byte(`
version: v1
pools: [{name: nodes, dns: engine.sortie-test.invalid:8443}]
scenarios:
  - name: soak
    pool: nodes
    target: http://target.invalid/
    executor: {type: constant-rate, rate: 60, duration: 1s, per_backend: true}
`), 0o600); err != nil {
		t.Fatal(err)
	}
	code, _, stderr := runCLI("run", path)
	if code != exitBadUsage {
		t.Fatalf("exit = %d, want %d for a name that resolves to nothing\nstderr:\n%s", code, exitBadUsage, stderr)
	}
	if !strings.Contains(stderr, `pool "nodes": resolving engine.sortie-test.invalid:8443`) {
		t.Errorf("the error should name the pool and the name:\n%s", stderr)
	}
}

// writeStaircase writes a two-stage plan against the backend, with a
// threshold, and returns its path.
func writeStaircase(t *testing.T, backend string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "plan.yaml")
	body := fmt.Sprintf(`
version: v1
pools:
  - name: local
    services: ["%s"]
scenarios:
  - name: steps
    pool: local
    target: http://127.0.0.1:1/
    executor:
      type: staircase
      stages:
        - {rate: 10, duration: 1s}
        - {rate: 20, duration: 1s}
    thresholds: ["latency_2xx.p95 < 50ms"]
`, backend)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// A stage's verdict is on stderr when the stage ends, with no --progress
// asked for, and the results stream holds one line per stage that is that
// stage's object in the JSON report.
func TestRunStreamsEachExecutionAsItFinishes(t *testing.T) {
	plan := writeStaircase(t, startBackend(t, 900*time.Millisecond))
	dir := t.TempDir()
	out, stream := filepath.Join(dir, "report.json"), filepath.Join(dir, "results.jsonl")
	// The stream is appended to: what a run that died left there is kept.
	if err := os.WriteFile(stream, []byte("{\"label\":\"earlier\"}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	code, stdout, stderr := runCLI("run", "--json", "-o", out, "--results-stream", stream, plan)
	if code != exitFailed {
		t.Fatalf("exit = %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitFailed, stdout, stderr)
	}
	for _, label := range []string{"steps/stage-1", "steps/stage-2"} {
		want := "  FAIL " + label + " (scenario steps, "
		if !strings.Contains(stderr, want) || !strings.Contains(stderr, "latency_2xx.p95 < 50ms actual 900ms") {
			t.Errorf("stderr lacks the verdict line %q with its failed threshold:\n%s", want, stderr)
		}
	}

	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var full struct {
		Executions []map[string]any `json:"executions"`
	}
	if err := json.Unmarshal(raw, &full); err != nil {
		t.Fatalf("the report does not parse: %v\n%s", err, raw)
	}
	streamed, err := os.ReadFile(stream)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(streamed), "\n"), "\n")
	if len(lines) != 3 || lines[0] != `{"label":"earlier"}` {
		t.Fatalf("want the earlier line and one per stage, got:\n%s", streamed)
	}
	for i, line := range lines[1:] {
		var got map[string]any
		if err := json.Unmarshal([]byte(line), &got); err != nil {
			t.Fatalf("stream line does not parse: %v\n%s", err, line)
		}
		if !reflect.DeepEqual(got, full.Executions[i]) {
			t.Errorf("stream line %d is not executions[%d] of the report:\n%s\nwant:\n%v", i+1, i, line, full.Executions[i])
		}
		for _, key := range []string{"started_at", "ended_at"} {
			at, _ := got[key].(string)
			if _, err := time.Parse(time.RFC3339Nano, at); err != nil || !strings.HasSuffix(at, "Z") {
				t.Errorf("%s = %q, want an RFC 3339 time in UTC", key, at)
			}
		}
	}
}

// A stream that cannot be written is said so, once per line, and the run and
// its verdict are what they would have been without it.
func TestRunCarriesOnWhenTheStreamCannotBeWritten(t *testing.T) {
	if _, err := os.Stat("/dev/full"); err != nil {
		t.Skip("no /dev/full to fail writes with")
	}
	plan := writeStaircase(t, startBackend(t, 5*time.Millisecond))

	code, stdout, stderr := runCLI("run", "--results-stream", "/dev/full", plan)
	if code != exitOK {
		t.Fatalf("exit = %d, want %d: a failed stream must not fail the run\nstderr:\n%s", code, exitOK, stderr)
	}
	if !strings.Contains(stdout, "PASS  2/2 executions passed") {
		t.Errorf("the report is not the full one:\n%s", stdout)
	}
	for _, label := range []string{"steps/stage-1", "steps/stage-2"} {
		if !strings.Contains(stderr, "sortie: results stream: "+label+" was not written: ") {
			t.Errorf("stderr does not report the lost line for %s:\n%s", label, stderr)
		}
	}
}

// What cannot be honoured is refused before any load: stdout, which is the
// report's; the report's own file; a file that cannot be opened.
func TestRunRefusesAStreamItCannotKeep(t *testing.T) {
	backend := &fakeBackend{p95: 5 * time.Millisecond, requests: 100}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	client.RegisterNighthawkServiceServer(server, backend)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	plan := writePlan(t, listener.Addr().String(), "")
	dir := t.TempDir()

	// A report that exists, and a second name for it.
	if err := os.WriteFile(filepath.Join(dir, "linked.json"), []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(dir, "linked.json"), filepath.Join(dir, "link.jsonl")); err != nil {
		t.Fatal(err)
	}

	for name, args := range map[string][]string{
		"stdout":                     {"--results-stream", "-"},
		"the report file":            {"-o", filepath.Join(dir, "r.json"), "--results-stream", filepath.Join(dir, ".", "r.json")},
		"the report file, by a link": {"-o", filepath.Join(dir, "linked.json"), "--results-stream", filepath.Join(dir, "link.jsonl")},
		"no such dir":                {"--results-stream", filepath.Join(dir, "missing", "results.jsonl")},
	} {
		code, _, stderr := runCLI(append(append([]string{"run"}, args...), plan)...)
		if code != exitBadUsage {
			t.Errorf("%s: exit = %d, want %d\nstderr:\n%s", name, code, exitBadUsage, stderr)
		}
		if !strings.Contains(stderr, "--results-stream") {
			t.Errorf("%s: the error does not name the flag:\n%s", name, stderr)
		}
	}
	if n := backend.calls.Load(); n != 0 {
		t.Errorf("the backend was driven %d times by runs that should not have started", n)
	}
}

// A run that died can leave the stream ending in part of a line. The retry's
// first line is not joined to it: the complete record before stays, the
// fragment becomes a line of its own, and the retry's lines are whole.
func TestRunKeepsItsLinesApartFromAnUnfinishedOne(t *testing.T) {
	plan := writeStaircase(t, startBackend(t, 900*time.Millisecond))
	stream := filepath.Join(t.TempDir(), "results.jsonl")
	if err := os.WriteFile(stream, []byte("{\"label\":\"earlier\"}\n{\"label\":\"cut sho"), 0o600); err != nil {
		t.Fatal(err)
	}

	code, stdout, stderr := runCLI("run", "--results-stream", stream, plan)
	if code != exitFailed {
		t.Fatalf("exit = %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitFailed, stdout, stderr)
	}
	if !strings.Contains(stderr, "ended in an unfinished line") {
		t.Errorf("stderr does not say the stream had an unfinished line:\n%s", stderr)
	}
	raw, err := os.ReadFile(stream)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	if len(lines) != 4 {
		t.Fatalf("got %d lines, want the earlier record, the fragment and two stages:\n%s", len(lines), raw)
	}
	for i, want := range []string{"earlier", "", "steps/stage-1", "steps/stage-2"} {
		var got struct {
			Label string `json:"label"`
		}
		err := json.Unmarshal([]byte(lines[i]), &got)
		if want == "" {
			if err == nil {
				t.Errorf("the fragment parsed: %q", lines[i])
			}
			continue
		}
		if err != nil || got.Label != want {
			t.Errorf("line %d = %q (%v), want the record of %s", i, lines[i], err, want)
		}
	}
}

// stdout under another name is still stdout: the file it was redirected to,
// given as the stream, is refused before any load, and nothing is added to it.
func TestRunRefusesAStreamThatIsStdout(t *testing.T) {
	backend := &fakeBackend{p95: 5 * time.Millisecond, requests: 100}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	client.RegisterNighthawkServiceServer(server, backend)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	plan := writePlan(t, listener.Addr().String(), "")

	path := filepath.Join(t.TempDir(), "stdout.json")
	// Ends mid-line, as a redirected stdout that something already wrote to may.
	if err := os.WriteFile(path, []byte("{\"earlier\":"), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer stdout.Close()

	var stderr bytes.Buffer
	code := execute("test", []string{"run", "--json", "--results-stream", path, plan}, stdout, &stderr)
	if code != exitBadUsage {
		t.Errorf("exit = %d, want %d\nstderr:\n%s", code, exitBadUsage, stderr.String())
	}
	if !strings.Contains(stderr.String(), "--results-stream names what stdout is") {
		t.Errorf("the error does not say the stream is stdout:\n%s", stderr.String())
	}
	if n := backend.calls.Load(); n != 0 {
		t.Errorf("the backend was driven %d times by a run that should not have started", n)
	}
	if raw, err := os.ReadFile(path); err != nil || string(raw) != "{\"earlier\":" {
		t.Errorf("the file was written to: %q (%v)", raw, err)
	}
}
