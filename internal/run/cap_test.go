package run_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	client "github.com/bpalermo/sortie/engine/api/client"
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
	var roomyCancelled, fullCancelled atomic.Int32
	roomy := startFake(t, nil)
	roomy.serve = func(_ int, _ *client.CommandLineOptions, stream client.NighthawkService_ExecutionStreamServer) error {
		return untilCancelled(t, &roomyCancelled, stream)
	}
	full := startFake(t, nil)
	full.serve = func(_ int, opts *client.CommandLineOptions, stream client.NighthawkService_ExecutionStreamServer) error {
		if strings.HasSuffix(opts.GetUri().GetValue(), "/c") {
			return atCap(stream)
		}
		return untilCancelled(t, &fullCancelled, stream)
	}

	started := time.Now()
	report, err := (&run.Runner{Plan: planFor(t, weightedMinute, roomy.addr, full.addr)}).Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if took := time.Since(started); took > 15*time.Second {
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
	var cancelled atomic.Int32
	roomy := startFake(t, nil)
	roomy.serve = func(_ int, _ *client.CommandLineOptions, stream client.NighthawkService_ExecutionStreamServer) error {
		return untilCancelled(t, &cancelled, stream)
	}
	full := startFake(t, nil)
	full.serve = func(_ int, _ *client.CommandLineOptions, stream client.NighthawkService_ExecutionStreamServer) error {
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
