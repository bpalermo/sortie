package nh_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
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
