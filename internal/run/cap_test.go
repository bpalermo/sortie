package run_test

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	client "github.com/bpalermo/sortie/engine/api/client"
	distributorpb "github.com/bpalermo/sortie/engine/api/distributor"
	"github.com/bpalermo/sortie/internal/compile"
	"github.com/bpalermo/sortie/internal/nh"
	"github.com/bpalermo/sortie/internal/run"
)

// atCap ends a stream the way an engine at its execution cap does: nothing
// started, ResourceExhausted, and the cap in the trailer.
func atCap(stream client.NighthawkService_ExecutionStreamServer) error {
	stream.SetTrailer(metadata.Pairs(nh.MaxConcurrentExecutionsTrailer, "16"))
	return status.Error(codes.ResourceExhausted,
		"Busy: 16 executions are running, the maximum this service allows (--max-concurrent-executions).")
}

// untilCancelled is an execution that runs until its stream is cancelled and
// then answers with what it had counted, as the engine does. One that is
// never cancelled fails the test rather than hang it.
func untilCancelled(t *testing.T, cancelled *atomic.Int32, stream client.NighthawkService_ExecutionStreamServer) error {
	got := make(chan error, 1)
	go func() {
		req, err := stream.Recv()
		if err == nil && req.GetCancellationRequest() == nil {
			err = errors.New("expected a cancellation")
		}
		got <- err
	}()
	select {
	case err := <-got:
		if err != nil {
			return err
		}
	case <-time.After(20 * time.Second):
		t.Error("an execution that had started was left running after a sibling was refused")
		return errors.New("never cancelled")
	}
	cancelled.Add(1)
	return stream.Send(okResponse(5, time.Millisecond, time.Second))
}

// afterStarts holds a refusal back until n other executions have started, so
// that what a test counts as cancelled does not depend on which stream
// reached its backend first. It gives up after a while rather than hang.
func afterStarts(started *atomic.Int32, n int32) {
	for deadline := time.Now().Add(10 * time.Second); started.Load() < n && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
}

const weightedMinute = `
scenarios:
  - name: mix
    executor: {type: constant-rate, rate: 100, duration: 60s}
    targets:
      - {name: a, url: http://127.0.0.1:1/a, weight: 6}
      - {name: b, url: http://127.0.0.1:1/b, weight: 3}
      - {name: c, url: http://127.0.0.1:1/c, weight: 1}
    thresholds:
      - "latency_2xx.p95 < 50ms"
`

// An engine with fewer free slots than a weighted scenario has targets
// refuses some of the starts. The scenario then stops at once, everywhere:
// the targets that did get a slot are cancelled -- on the refusing backend
// and on the one that had room for all three -- instead of driving a partial
// mix for the whole duration, and every execution says what happened and what
// to change.
func TestRunStopsAStageWhenABackendIsAtItsExecutionCap(t *testing.T) {
	var roomyCancelled, fullCancelled, started atomic.Int32
	roomy := startFake(t, nil)
	roomy.serve = func(_ int, _ *client.CommandLineOptions, stream client.NighthawkService_ExecutionStreamServer) error {
		started.Add(1)
		return untilCancelled(t, &roomyCancelled, stream)
	}
	full := startFake(t, nil)
	full.serve = func(_ int, opts *client.CommandLineOptions, stream client.NighthawkService_ExecutionStreamServer) error {
		if strings.HasSuffix(opts.GetUri().GetValue(), "/c") {
			// Once the other five have their slots.
			afterStarts(&started, 5)
			return atCap(stream)
		}
		started.Add(1)
		return untilCancelled(t, &fullCancelled, stream)
	}

	began := time.Now()
	report, err := (&run.Runner{Plan: planFor(t, weightedMinute, roomy.addr, full.addr)}).Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if took := time.Since(began); took > 15*time.Second {
		t.Errorf("the run took %s: the siblings of the refused start were not stopped promptly", took)
	}
	if got := roomyCancelled.Load(); got != 3 {
		t.Errorf("%d executions cancelled on the backend with room, want all 3", got)
	}
	if got := fullCancelled.Load(); got != 2 {
		t.Errorf("%d executions cancelled on the backend at its cap, want the 2 that started", got)
	}
	if report.Pass {
		t.Error("the report passed")
	}
	if len(report.Executions) != 3 {
		t.Fatalf("got %d executions, want one per target", len(report.Executions))
	}
	for _, e := range report.Executions {
		var atCap *run.CapError
		if !errors.As(e.Err, &atCap) {
			t.Fatalf("%s: Err = %v, want a CapError", e.Label, e.Err)
		}
		if atCap.Backend != full.addr || atCap.Max != 16 || atCap.Needed != 3 {
			t.Errorf("%s: CapError = %+v, want backend %s, max 16, needed 3", e.Label, atCap, full.addr)
		}
		if status.Code(e.Err) != codes.ResourceExhausted {
			t.Errorf("%s: code = %s, want ResourceExhausted", e.Label, status.Code(e.Err))
		}
		if errors.Is(e.Err, context.Canceled) {
			t.Errorf("%s: reads as a run the caller cancelled: %v", e.Label, e.Err)
		}
		msg := e.Err.Error()
		for _, want := range []string{
			full.addr,
			"cap of 16 concurrent executions",
			"starts 3 at once on every backend",
			"--max-concurrent-executions",
			"engine.maxConcurrentExecutions",
		} {
			if !strings.Contains(msg, want) {
				t.Errorf("%s: the error does not say %q: %s", e.Label, want, msg)
			}
		}
		if e.Pass || e.Set != nil || len(e.Outcomes) != 0 {
			t.Errorf("%s: a stopped execution was judged: pass=%v outcomes=%+v", e.Label, e.Pass, e.Outcomes)
		}
		// Only the refusal is a backend's failure; the stops are not listed
		// as if each backend had failed on its own.
		wantErrors := 0
		if e.Label == "mix/c" {
			wantErrors = 1
		}
		if len(e.BackendErrors) != wantErrors {
			t.Errorf("%s: backend errors = %+v, want %d", e.Label, e.BackendErrors, wantErrors)
		}
		for _, be := range e.BackendErrors {
			if be.Addr != full.addr {
				t.Errorf("%s: backend error names %s, want %s", e.Label, be.Addr, full.addr)
			}
		}
	}
}

// The same for a scenario with one target: a backend whose engine is busy
// with someone else's run stops the execution on the others too.
func TestRunStopsASingleExecutionWhenABackendIsAtItsExecutionCap(t *testing.T) {
	var cancelled, started atomic.Int32
	roomy := startFake(t, nil)
	roomy.serve = func(_ int, _ *client.CommandLineOptions, stream client.NighthawkService_ExecutionStreamServer) error {
		started.Add(1)
		return untilCancelled(t, &cancelled, stream)
	}
	full := startFake(t, nil)
	full.serve = func(_ int, _ *client.CommandLineOptions, stream client.NighthawkService_ExecutionStreamServer) error {
		afterStarts(&started, 1)
		return atCap(stream)
	}
	p := planFor(t, `
scenarios:
  - name: soak
    executor: {type: constant-rate, rate: 100, duration: 60s}
`, roomy.addr, full.addr)

	report, err := (&run.Runner{Plan: p}).Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if cancelled.Load() != 1 {
		t.Error("the execution on the backend with room was not cancelled")
	}
	var atCap *run.CapError
	if !errors.As(report.Executions[0].Err, &atCap) {
		t.Fatalf("Err = %v, want a CapError", report.Executions[0].Err)
	}
	if atCap.Needed != 1 || !strings.Contains(atCap.Error(), "needs one free slot on every backend") {
		t.Errorf("CapError = %v", atCap)
	}
	if report.Pass {
		t.Error("the report passed")
	}
}

// Only the cap stops a stage. A target's execution that fails on one backend
// for any other reason -- ResourceExhausted without the engine's trailer
// included -- leaves its siblings, and the other backends, to finish and be
// judged.
func TestRunDoesNotStopAStageForAnyOtherBackendFailure(t *testing.T) {
	healthy := startFake(t, func(int, *client.CommandLineOptions) *client.ExecutionResponse {
		return okResponse(100, time.Millisecond, time.Second)
	})
	flaky := startFake(t, nil)
	flaky.serve = func(_ int, opts *client.CommandLineOptions, stream client.NighthawkService_ExecutionStreamServer) error {
		if strings.HasSuffix(opts.GetUri().GetValue(), "/c") {
			return status.Error(codes.ResourceExhausted, "out of something else")
		}
		// Long enough after the failure that a stop would have reached it.
		select {
		case <-time.After(500 * time.Millisecond):
		case <-stream.Context().Done():
		}
		return stream.Send(okResponse(100, time.Millisecond, time.Second))
	}

	report, err := (&run.Runner{Plan: planFor(t, weightedMinute, healthy.addr, flaky.addr)}).Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, e := range report.Executions {
		var atCap *run.CapError
		if errors.As(e.Err, &atCap) {
			t.Fatalf("%s: reported as stopped at the cap: %v", e.Label, e.Err)
		}
		if e.Err != nil {
			t.Fatalf("%s: Err = %v, want the execution judged on what came back", e.Label, e.Err)
		}
		wantBackends, wantPass := 2, true
		if e.Label == "mix/c" {
			wantBackends, wantPass = 1, false
		}
		if e.Set == nil || len(e.Set.Backends) != wantBackends {
			t.Errorf("%s: results = %+v, want %d backends'", e.Label, e.Set, wantBackends)
		}
		if e.Pass != wantPass {
			t.Errorf("%s: pass = %v, want %v", e.Label, e.Pass, wantPass)
		}
	}
}

// recorder is an Observer that notes which executions it was told started
// and finished, in order.
type recorder struct {
	started  []string
	finished []run.ExecutionReport
}

func (r *recorder) ExecutionStarted(e compile.Execution, _ []string) {
	r.started = append(r.started, e.Label)
}
func (r *recorder) ExecutionProgress(compile.Execution, string, time.Duration, *client.Output) {}
func (r *recorder) ExecutionFinished(er run.ExecutionReport) {
	r.finished = append(r.finished, er)
}

// A staircase whose first stage is refused at the cap does not go on to its
// other stages: they are reported as not run, naming the stage that was
// refused, with no start and no elapsed time, and the observer is told of
// each. The plan's next scenario runs as usual.
func TestRunDoesNotAttemptTheStagesAfterOneRefusedAtTheCap(t *testing.T) {
	fake := startFake(t, nil)
	fake.serve = func(n int, _ *client.CommandLineOptions, stream client.NighthawkService_ExecutionStreamServer) error {
		// Only the very first start finds the engine full: stages 2 and 3,
		// had they been attempted, would have run.
		if n == 0 {
			return atCap(stream)
		}
		return stream.Send(okResponse(100, time.Millisecond, time.Second))
	}
	p := planFor(t, `
scenarios:
  - name: steps
    executor:
      type: staircase
      stages:
        - {rate: 100, duration: 1s}
        - {rate: 200, duration: 1s}
        - {rate: 300, duration: 1s}
  - name: after
    executor: {type: constant-rate, rate: 100, duration: 1s}
`, fake.addr)

	observer := &recorder{}
	report, err := (&run.Runner{Plan: p, Observer: observer}).Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := len(fake.received()); got != 2 {
		t.Errorf("the backend saw %d starts, want 2: the refused stage and the next scenario", got)
	}
	if report.Pass {
		t.Error("the report passed")
	}
	if len(report.Executions) != 4 {
		t.Fatalf("got %d executions, want the three stages and the next scenario", len(report.Executions))
	}
	refused := report.Executions[0]
	var atCap *run.CapError
	if !errors.As(refused.Err, &atCap) || refused.NotRun() {
		t.Fatalf("stage 1: Err = %v, want the CapError of a stage that was attempted", refused.Err)
	}
	for i, e := range report.Executions[1:3] {
		wantLabel := []string{"steps/stage-2", "steps/stage-3"}[i]
		var notRun *run.NotRunError
		if e.Label != wantLabel || !errors.As(e.Err, &notRun) || !e.NotRun() {
			t.Fatalf("execution %d = %s with Err %v, want %s not run", i+1, e.Label, e.Err, wantLabel)
		}
		if notRun.Refused != "steps/stage-1" || notRun.Cap != atCap {
			t.Errorf("%s: NotRunError = %+v, want it to name steps/stage-1 and carry its CapError", e.Label, notRun)
		}
		for _, want := range []string{"not run: steps/stage-1 was refused", fake.addr, "cap of 16 concurrent executions"} {
			if !strings.Contains(e.Err.Error(), want) {
				t.Errorf("%s: the error does not say %q: %v", e.Label, want, e.Err)
			}
		}
		if !e.Started.IsZero() || e.Elapsed != 0 {
			t.Errorf("%s: started %v, elapsed %s for something that never ran", e.Label, e.Started, e.Elapsed)
		}
		if e.Pass || e.Set != nil {
			t.Errorf("%s: judged", e.Label)
		}
		if e.Scenario != "steps" || e.Rate != uint32(200+100*i) || e.Duration != time.Second || len(e.Backends) != 1 {
			t.Errorf("%s: the report does not say what was planned: %+v", e.Label, e)
		}
	}
	if after := report.Executions[3]; after.Label != "after" || after.Err != nil || after.Set == nil {
		t.Errorf("the next scenario = %s with Err %v, want it run", after.Label, after.Err)
	}

	if got := strings.Join(observer.started, " "); got != "steps/stage-1 after" {
		t.Errorf("started = %q: a stage that was not run was announced as started", got)
	}
	var finished []string
	for _, er := range observer.finished {
		finished = append(finished, er.Label)
	}
	if got := strings.Join(finished, " "); got != "steps/stage-1 steps/stage-2 steps/stage-3 after" {
		t.Errorf("finished = %q, want every execution, the ones not run included", got)
	}
}

// With weighted targets a stage is a group: every target's execution of the
// later stages is reported as not run, and the stage is named by its group.
func TestRunDoesNotAttemptTheLaterStagesOfAWeightedScenarioRefusedAtTheCap(t *testing.T) {
	fake := startFake(t, nil)
	fake.serve = func(_ int, _ *client.CommandLineOptions, stream client.NighthawkService_ExecutionStreamServer) error {
		return atCap(stream)
	}
	p := planFor(t, `
scenarios:
  - name: mix
    executor:
      type: staircase
      stages:
        - {rate: 100, duration: 1s}
        - {rate: 200, duration: 1s}
    targets:
      - {name: a, url: http://127.0.0.1:1/a, weight: 1}
      - {name: b, url: http://127.0.0.1:1/b, weight: 1}
`, fake.addr)

	report, err := (&run.Runner{Plan: p}).Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// One or two: the first stage's sibling may be stopped before its start
	// reaches the backend. Never the second stage's.
	if got := len(fake.received()); got < 1 || got > 2 {
		t.Errorf("the backend saw %d starts, want only the first stage's, 1 or 2", got)
	}
	if len(report.Executions) != 4 {
		t.Fatalf("got %d executions, want 2 targets x 2 stages", len(report.Executions))
	}
	for i, e := range report.Executions {
		if got, want := e.NotRun(), i >= 2; got != want {
			t.Errorf("%s: not run = %v, want %v", e.Label, got, want)
		}
		var notRun *run.NotRunError
		if errors.As(e.Err, &notRun) && notRun.Refused != "mix/stage-1" {
			t.Errorf("%s: refused stage = %q, want mix/stage-1", e.Label, notRun.Refused)
		}
	}
}

// Any other failed stage leaves the staircase going: the stages after it run.
func TestRunAttemptsTheStagesAfterOneThatFailedAnyOtherWay(t *testing.T) {
	fake := startFake(t, nil)
	fake.serve = func(n int, _ *client.CommandLineOptions, stream client.NighthawkService_ExecutionStreamServer) error {
		if n == 0 {
			return status.Error(codes.ResourceExhausted, "out of something else")
		}
		return stream.Send(okResponse(100, time.Millisecond, time.Second))
	}
	p := planFor(t, `
scenarios:
  - name: steps
    executor:
      type: staircase
      stages:
        - {rate: 100, duration: 1s}
        - {rate: 200, duration: 1s}
`, fake.addr)

	report, err := (&run.Runner{Plan: p}).Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(report.Executions) != 2 {
		t.Fatalf("got %d executions", len(report.Executions))
	}
	if first := report.Executions[0]; first.Err == nil || first.NotRun() {
		t.Errorf("stage 1: Err = %v, want its own failure", first.Err)
	}
	if second := report.Executions[1]; second.Err != nil || second.Set == nil || !second.Pass {
		t.Errorf("stage 2: Err = %v, pass = %v, want it run and passed", second.Err, second.Pass)
	}
}

// busyDistributor is a distributor whose every target is at its execution
// cap: it answers each request with the status it would relay from the
// target's stream, code and message and nothing else.
type busyDistributor struct {
	distributorpb.UnimplementedNighthawkDistributorServer
}

func (busyDistributor) DistributedRequestStream(stream distributorpb.NighthawkDistributor_DistributedRequestStreamServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp := &distributorpb.DistributedResponse{}
		for _, service := range req.GetServices() {
			resp.ServiceResponse = append(resp.ServiceResponse, &distributorpb.DistributedServiceResponse{
				Service: service,
				DistributedResponseType: &distributorpb.DistributedServiceResponse_Error{Error: &rpcstatus.Status{
					Code:    int32(codes.ResourceExhausted),
					Message: "Distributed Execution Request failed: Busy: 16 executions are running, the maximum this service allows (--max-concurrent-executions).",
				}},
			})
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

// A target refused at its cap behind a distributor ends the scenario the same
// way: a CapError, the later stages not attempted. The error does not claim
// that anything was stopped -- a distributor forwards no cancellation.
func TestRunStopsAScenarioWhenATargetBehindADistributorIsAtItsExecutionCap(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	distributorpb.RegisterNighthawkDistributorServer(server, busyDistributor{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	p := mustParse(t, `
version: v1
pools:
  - name: far
    distributor: "`+listener.Addr().String()+`"
    targets: ["10.0.0.11:8443"]
defaults:
  pool: far
  target: http://127.0.0.1:1/
scenarios:
  - name: steps
    executor:
      type: staircase
      stages:
        - {rate: 100, duration: 1s}
        - {rate: 200, duration: 1s}
`)
	report, err := (&run.Runner{Plan: p}).Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(report.Executions) != 2 {
		t.Fatalf("got %d executions", len(report.Executions))
	}
	var atCap *run.CapError
	if !errors.As(report.Executions[0].Err, &atCap) {
		t.Fatalf("stage 1: Err = %v, want a CapError", report.Executions[0].Err)
	}
	if atCap.Backend != "10.0.0.11:8443" || atCap.Max != 16 || !atCap.Distributed {
		t.Errorf("CapError = %+v, want the target, its cap of 16, and the distributor noted", atCap)
	}
	msg := atCap.Error()
	if !strings.Contains(msg, "abandoned but not stopped") || strings.Contains(msg, "nothing was left running") {
		t.Errorf("the error claims a stop a distributor cannot perform: %s", msg)
	}
	if !report.Executions[1].NotRun() {
		t.Errorf("stage 2: Err = %v, want it not run", report.Executions[1].Err)
	}
}

// A member of the stage that had already finished, and been judged, when a
// sibling was refused is not left standing as a run of its own: the stage was
// stopped at the cap for all of them.
func TestAnExecutionThatFinishedBeforeTheRefusalIsNotJudged(t *testing.T) {
	var cancelled, started atomic.Int32
	done := make(chan struct{})
	backend := startFake(t, nil)
	backend.serve = func(_ int, opts *client.CommandLineOptions, stream client.NighthawkService_ExecutionStreamServer) error {
		switch uri := opts.GetUri().GetValue(); {
		case strings.HasSuffix(uri, "/a"):
			// Over at once, and well inside its threshold.
			defer close(done)
			return stream.Send(okResponse(5, time.Millisecond, time.Second))
		case strings.HasSuffix(uri, "/c"):
			// Refused only once /a has answered and been judged.
			<-done
			afterStarts(&started, 1)
			time.Sleep(100 * time.Millisecond)
			return atCap(stream)
		}
		started.Add(1)
		return untilCancelled(t, &cancelled, stream)
	}

	report, err := (&run.Runner{Plan: planFor(t, weightedMinute, backend.addr)}).Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.Pass || len(report.Executions) != 3 {
		t.Fatalf("pass = %v with %d executions, want a failed report of 3", report.Pass, len(report.Executions))
	}
	for _, e := range report.Executions {
		var atCap *run.CapError
		if !errors.As(e.Err, &atCap) {
			t.Errorf("%s: err = %v, want the cap error", e.Label, e.Err)
		}
		if e.Pass || e.Set != nil || len(e.Outcomes) != 0 {
			t.Errorf("%s was judged: pass=%v set=%v outcomes=%d", e.Label, e.Pass, e.Set != nil, len(e.Outcomes))
		}
	}
	if got := cancelled.Load(); got != 1 {
		t.Errorf("%d executions cancelled, want the 1 still running", got)
	}
}
