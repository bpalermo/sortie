// Package nh drives Nighthawk's gRPC control plane: the per-instance execution
// service, the distributor that fans one request out to several instances, and
// the sink that stores results by execution id.
package nh

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	client "github.com/envoyproxy/nighthawk/api/client"
)

// Dial opens a plaintext connection to a Nighthawk service.
//
// Nighthawk's own services speak plaintext gRPC and have no authentication, so
// they are expected to sit on a trusted network or behind a proxy that
// terminates TLS. sortie does not pretend otherwise.
func Dial(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dialing %s: %w", addr, err)
	}
	return conn, nil
}

// Execute runs one benchmark against a single nighthawk_service and returns its
// response.
//
// Nighthawk accepts only one execution at a time per service instance and
// answers on a bidirectional stream, writing a single ExecutionResponse when
// the run has finished. There is no progress on this stream and no way to stop
// a run once it has started: the UpdateRequest and CancellationRequest members
// of ExecutionRequest are declared in the proto but rejected by the service
// with "Request is not supported yet" (envoyproxy/nighthawk#380). Cancelling
// ctx therefore abandons the stream while the backend keeps generating load
// until its configured duration elapses.
func Execute(ctx context.Context, conn *grpc.ClientConn, opts *client.CommandLineOptions) (*client.ExecutionResponse, error) {
	stub := client.NewNighthawkServiceClient(conn)
	stream, err := stub.ExecutionStream(ctx)
	if err != nil {
		return nil, fmt.Errorf("opening execution stream: %w", err)
	}

	req := &client.ExecutionRequest{
		CommandSpecificOptions: &client.ExecutionRequest_StartRequest{
			StartRequest: &client.StartRequest{Options: opts},
		},
	}
	if err := stream.Send(req); err != nil {
		return nil, fmt.Errorf("sending start request: %w", err)
	}
	if err := stream.CloseSend(); err != nil {
		return nil, fmt.Errorf("closing send direction: %w", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("service closed the stream without returning a response")
		}
		return nil, fmt.Errorf("awaiting execution response: %w", err)
	}

	// Drain so the server sees a clean half-close rather than a cancelled RPC.
	//
	// Only io.EOF means the stream finished cleanly. A streaming RPC can
	// deliver a response and then end with a non-OK status, and swallowing that
	// would have us evaluate thresholds against a run whose backend failed
	// after reporting.
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("execution stream failed after responding: %w", err)
		}
	}

	if detail := resp.GetErrorDetail(); detail != nil && detail.GetCode() != 0 {
		return resp, fmt.Errorf("nighthawk reported failure: %s", detail.GetMessage())
	}
	return resp, nil
}
