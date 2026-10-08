package nh_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	client "github.com/bpalermo/sortie/engine/api/client"
	"github.com/bpalermo/sortie/internal/nh"
)

// fakeCancellableService runs "forever" until it reads a CancellationRequest,
// then answers with an output, the way nighthawk_service does.
type fakeCancellableService struct {
	client.UnimplementedNighthawkServiceServer
	addr      string
	cancelled chan struct{}
}

func (f *fakeCancellableService) ExecutionStream(stream client.NighthawkService_ExecutionStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil
		}
		if req.GetCancellationRequest() != nil {
			close(f.cancelled)
			return stream.Send(&client.ExecutionResponse{Output: &client.Output{}})
		}
	}
}

func TestExecuteCancelsTheRunWhenTheContextIsCancelled(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeCancellableService{addr: listener.Addr().String(), cancelled: make(chan struct{})}
	server := grpc.NewServer()
	client.RegisterNighthawkServiceServer(server, fake)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := nh.Dial(context.Background(), fake.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	resp, err := nh.Execute(ctx, conn, &client.CommandLineOptions{}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled run must return the context's error, got: %v", err)
	}
	if resp == nil || resp.GetOutput() == nil {
		t.Fatal("the service's partial response must be returned with the error")
	}
	select {
	case <-fake.cancelled:
	default:
		t.Fatal("the service never received a CancellationRequest")
	}
}

// fakeProgressService answers a start request that asks for progress with
// two interim responses, then the final one.
type fakeProgressService struct {
	client.UnimplementedNighthawkServiceServer
	gotInterval time.Duration
}

func (f *fakeProgressService) ExecutionStream(stream client.NighthawkService_ExecutionStreamServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	f.gotInterval = req.GetStartRequest().GetProgressInterval().AsDuration()
	for i := 1; i <= 2; i++ {
		interim := &client.ExecutionResponse{
			Progress: &client.Progress{Elapsed: durationpb.New(time.Duration(i) * time.Second)},
			Output:   &client.Output{Results: []*client.Result{{Name: "global", Counters: []*client.Counter{{Name: "benchmark.http_2xx", Value: uint64(100 * i)}}}}},
		}
		if err := stream.Send(interim); err != nil {
			return err
		}
	}
	return stream.Send(&client.ExecutionResponse{Output: &client.Output{Results: []*client.Result{{Name: "global"}}}})
}

// Interim responses reach the Progress callback with their elapsed time, the
// requested interval reaches the service, and the final response is the one
// returned -- the first without `progress`.
func TestExecuteForwardsProgress(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	fake := &fakeProgressService{}
	client.RegisterNighthawkServiceServer(server, fake)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	conn, err := nh.Dial(context.Background(), listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var elapsed []time.Duration
	var counts []uint64
	resp, err := nh.Execute(context.Background(), conn, &client.CommandLineOptions{}, &nh.Progress{
		Interval: 500 * time.Millisecond,
		Fn: func(e time.Duration, out *client.Output) {
			elapsed = append(elapsed, e)
			counts = append(counts, out.GetResults()[0].GetCounters()[0].GetValue())
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetProgress() != nil {
		t.Error("the final response carries progress")
	}
	if fake.gotInterval != 500*time.Millisecond {
		t.Errorf("service saw interval %s, want 500ms", fake.gotInterval)
	}
	if len(elapsed) != 2 || elapsed[0] != time.Second || elapsed[1] != 2*time.Second || counts[1] != 200 {
		t.Errorf("progress = %v / %v, want two snapshots at 1s and 2s with 100 and 200", elapsed, counts)
	}
}

// A backend that starts a moment after the dial is waited for; one that never
// does is reported, with the wait bounded.
func TestDialWaitsForTheBackend(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	// Not serving yet: close it and reopen the same port after a delay.
	_ = listener.Close()
	server := grpc.NewServer()
	go func() {
		time.Sleep(500 * time.Millisecond)
		l, err := net.Listen("tcp", addr)
		if err != nil {
			t.Errorf("relisten: %v", err)
			return
		}
		_ = server.Serve(l)
	}()
	defer server.Stop()

	started := time.Now()
	conn, err := nh.DialTimeout(context.Background(), addr, 10*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn.Close()
	if waited := time.Since(started); waited < 400*time.Millisecond || waited > 8*time.Second {
		t.Errorf("waited %s; want roughly the backend's half-second start", waited)
	}

	started = time.Now()
	_, err = nh.DialTimeout(context.Background(), "127.0.0.1:1", time.Second)
	if err == nil {
		t.Fatal("dialled a port nothing listens on")
	}
	if !strings.Contains(err.Error(), "not reachable within 1s") {
		t.Errorf("error = %v; want the bounded wait reported", err)
	}
	if waited := time.Since(started); waited > 5*time.Second {
		t.Errorf("gave up after %s; want about the 1s bound", waited)
	}
}

// fakeRefusingService ends every stream with the given status, setting the
// engine's execution-cap trailer first when it has one to set.
type fakeRefusingService struct {
	client.UnimplementedNighthawkServiceServer
	trailer string
	err     error
	// eager refuses without reading the start request, so that the refusal
	// may reach the client before its Send does.
	eager bool
}

func (f *fakeRefusingService) ExecutionStream(stream client.NighthawkService_ExecutionStreamServer) error {
	if !f.eager {
		if _, err := stream.Recv(); err != nil {
			return err
		}
	}
	if f.trailer != "" {
		stream.SetTrailer(metadata.Pairs(nh.MaxConcurrentExecutionsTrailer, f.trailer))
	}
	return f.err
}

func executeAgainst(t *testing.T, service client.NighthawkServiceServer) error {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	client.RegisterNighthawkServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := nh.Dial(context.Background(), listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = nh.Execute(context.Background(), conn, &client.CommandLineOptions{}, nil)
	return err
}

// A start refused at the service's execution cap comes back as a BusyError
// that names the cap and still reads as ResourceExhausted.
func TestExecuteReportsAServiceAtItsExecutionCap(t *testing.T) {
	err := executeAgainst(t, &fakeRefusingService{
		trailer: "16",
		err:     status.Error(codes.ResourceExhausted, "Busy: 16 executions are running, the maximum this service allows (--max-concurrent-executions)."),
	})
	var refused *nh.BusyError
	if !errors.As(err, &refused) {
		t.Fatalf("err = %v, want a BusyError", err)
	}
	if refused.Max != 16 {
		t.Errorf("Max = %d, want 16", refused.Max)
	}
	if status.Code(err) != codes.ResourceExhausted {
		t.Errorf("code = %s, want ResourceExhausted", status.Code(err))
	}
	if !strings.Contains(err.Error(), "cap of 16 concurrent executions") {
		t.Errorf("the error does not name the cap: %v", err)
	}
}

// ResourceExhausted alone is not the cap -- gRPC uses the code for a message
// over the size limit, among others -- and neither is the trailer on some
// other failure.
func TestExecuteDoesNotMistakeOtherFailuresForTheExecutionCap(t *testing.T) {
	for name, service := range map[string]*fakeRefusingService{
		"exhausted, no trailer": {err: status.Error(codes.ResourceExhausted, "received message larger than max")},
		"trailer, other code":   {trailer: "16", err: status.Error(codes.Internal, "boom")},
	} {
		err := executeAgainst(t, service)
		if err == nil {
			t.Fatalf("%s: no error", name)
		}
		var refused *nh.BusyError
		if errors.As(err, &refused) {
			t.Errorf("%s: reported as a BusyError: %v", name, err)
		}
	}
}

// A service that refuses before it has read the start request can end the
// stream under the client's Send, which then fails with io.EOF and leaves the
// status to Recv. Whichever of the two the client meets, the refusal is the
// same BusyError.
func TestExecuteReportsTheExecutionCapWhenTheRefusalBeatsTheStart(t *testing.T) {
	service := &fakeRefusingService{
		eager:   true,
		trailer: "16",
		err:     status.Error(codes.ResourceExhausted, "Busy: 16 executions are running, the maximum this service allows (--max-concurrent-executions)."),
	}
	for i := 0; i < 50; i++ {
		err := executeAgainst(t, service)
		var refused *nh.BusyError
		if !errors.As(err, &refused) || refused.Max != 16 {
			t.Fatalf("attempt %d: err = %v, want a BusyError with the cap", i, err)
		}
	}
}

// slowService answers after a delay, with a complete response.
type slowService struct {
	client.UnimplementedNighthawkServiceServer
	after time.Duration
}

func (s *slowService) ExecutionStream(stream client.NighthawkService_ExecutionStreamServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	time.Sleep(s.after)
	return stream.Send(&client.ExecutionResponse{Output: &client.Output{Results: []*client.Result{{Name: "global"}}}})
}

// A deadline that has passed is not yet a backend that did not answer: a
// driver stopped for a while wakes past its deadline with the complete result
// waiting. The answer that is there, or arrives just after, is the result,
// and the run is neither cancelled nor an error.
func TestExecuteTakesAnAnswerThatArrivesJustAfterTheDeadline(t *testing.T) {
	defer nh.SetCancelGraceForTest(5 * time.Second)()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	client.RegisterNighthawkServiceServer(server, &slowService{after: 400 * time.Millisecond})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := nh.Dial(context.Background(), listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeoutCause(context.Background(), 100*time.Millisecond, nh.ErrBackendDeadline)
	defer cancel()
	resp, err := nh.Execute(ctx, conn, &client.CommandLineOptions{}, nil)
	if err != nil {
		t.Fatalf("Execute: %v, want the answer that arrived after the deadline", err)
	}
	if len(resp.GetOutput().GetResults()) != 1 {
		t.Errorf("response = %v, want the complete result", resp)
	}

	// A deadline that is the caller's own is a cancellation, acted on at
	// once: the run is cancelled and reported as that, well inside the grace.
	own, cancelOwn := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelOwn()
	started := time.Now()
	_, err = nh.Execute(own, conn, &client.CommandLineOptions{}, nil)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Execute under the caller's own deadline: err = %v, want it cancelled", err)
	}
	if took := time.Since(started); took > 2*time.Second {
		t.Errorf("the caller's own deadline was acted on after %s, want at once", took)
	}
}
