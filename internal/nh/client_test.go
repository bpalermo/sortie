package nh_test

import (
	"context"
	"errors"
	"net"
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
