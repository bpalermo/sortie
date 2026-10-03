package nh_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

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
	resp, err := nh.Execute(ctx, conn, &client.CommandLineOptions{})
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
