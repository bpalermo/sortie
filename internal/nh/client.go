// Package nh drives Nighthawk's gRPC control plane: the per-instance execution
// service, the distributor that fans one request out to several instances, and
// the sink that stores results by execution id.
package nh

import (
	"context"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"google.golang.org/protobuf/types/known/durationpb"

	client "github.com/bpalermo/sortie/engine/api/client"
)

// readyTimeout bounds how long Dial waits for a backend to accept connections.
// Backends often start with the run that uses them -- the chart's engine
// Deployment comes up beside its Job -- and a listener that is seconds away
// is not an error. One that is not there after this long is.
const readyTimeout = 30 * time.Second

// Dial opens a plaintext connection to a Nighthawk service and waits, up to
// readyTimeout, for it to be reachable.
//
// Nighthawk's own services speak plaintext gRPC and have no authentication, so
// they are expected to sit on a trusted network or behind a proxy that
// terminates TLS. sortie does not pretend otherwise.
func Dial(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	return DialTimeout(ctx, addr, readyTimeout)
}

// DialTimeout is Dial with its own bound on the wait for the backend.
func DialTimeout(ctx context.Context, addr string, timeout time.Duration) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dialing %s: %w", addr, err)
	}
	if err := waitReady(ctx, conn, timeout); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("backend %s: %w", addr, err)
	}
	return conn, nil
}

// waitReady drives the connection and returns once it is READY, or an error
// when it is not within timeout or ctx ends first. gRPC's own behaviour --
// fail the first RPC fast on a refused connection -- is right for a client
// that retries; a load test runs once, so the wait happens here.
func waitReady(ctx context.Context, conn *grpc.ClientConn, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if !conn.WaitForStateChange(waitCtx, state) {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("not reachable within %s (last state %s)", timeout, state)
		}
	}
}

// Nighthawk accepts only one execution at a time per service instance and
// answers on a bidirectional stream: interim ExecutionResponses carrying
// `progress` when asked for, then exactly one without it when the run has
// finished.
//
// Cancelling ctx stops the run: a CancellationRequest goes out on the still-open
// stream, the service ends the execution early and answers with what it
// collected, and Execute returns that response together with ctx's error, so a
// cancelled run is never evaluated as a complete one. The stream itself lives on
// a context that ctx does not cancel, since the cancellation has to travel on it
// after ctx is done. If the service does not answer within cancelGrace, the
// stream is abandoned.
// cancelGrace bounds how long a cancelled Execute waits for the service to
// answer the cancellation with the run's partial response.
const cancelGrace = 30 * time.Second

// Progress asks the service for interim responses while a run is in flight
// and receives them: every Interval the service writes a snapshot of the run
// so far and Fn gets it. A snapshot carries the live counters and each
// statistic's summary -- count, mean, pstdev, min, max -- and no percentiles:
// those need StartRequest.progress_statistics, which has the engine copy every
// worker's histograms per snapshot and is not asked for here.
// Snapshots are advisory; the final response is what a run is judged on.
type Progress struct {
	Interval time.Duration
	// Fn runs on the goroutine reading the stream, so it should not block.
	Fn func(elapsed time.Duration, out *client.Output)
}

// Execute runs one benchmark against a single nighthawk_service and returns
// its final response; progress, when not nil, asks for interim ones on the way.
func Execute(ctx context.Context, conn *grpc.ClientConn, opts *client.CommandLineOptions, progress *Progress) (*client.ExecutionResponse, error) {
	stub := client.NewNighthawkServiceClient(conn)
	streamCtx, closeStream := context.WithCancel(context.WithoutCancel(ctx))
	defer closeStream()
	stream, err := stub.ExecutionStream(streamCtx)
	if err != nil {
		return nil, fmt.Errorf("opening execution stream: %w", err)
	}

	start := &client.StartRequest{Options: opts}
	if progress != nil && progress.Interval > 0 {
		start.ProgressInterval = durationpb.New(progress.Interval)
	}
	req := &client.ExecutionRequest{
		CommandSpecificOptions: &client.ExecutionRequest_StartRequest{StartRequest: start},
	}
	if err := stream.Send(req); err != nil {
		return nil, fmt.Errorf("sending start request: %w", err)
	}

	type received struct {
		resp *client.ExecutionResponse
		err  error
	}
	// The reader hands interim responses to progress and delivers the final
	// one -- the first without `progress` -- or the error that ended the stream.
	first := make(chan received, 1)
	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				first <- received{nil, err}
				return
			}
			if resp.GetProgress() != nil {
				if progress != nil && progress.Fn != nil {
					progress.Fn(resp.GetProgress().GetElapsed().AsDuration(), resp.GetOutput())
				}
				continue
			}
			first <- received{resp, nil}
			return
		}
	}()

	var cancelled bool
	var got received
	select {
	case got = <-first:
	case <-ctx.Done():
		cancelled = true
		cancel := &client.ExecutionRequest{
			CommandSpecificOptions: &client.ExecutionRequest_CancellationRequest{
				CancellationRequest: &client.CancellationRequest{},
			},
		}
		if err := stream.Send(cancel); err != nil {
			return nil, fmt.Errorf("%w (and sending the cancellation failed: %v)", ctx.Err(), err)
		}
		select {
		case got = <-first:
		case <-time.After(cancelGrace):
			return nil, fmt.Errorf("%w (the service did not answer the cancellation within %s)", ctx.Err(), cancelGrace)
		}
	}
	if err := stream.CloseSend(); err != nil {
		return nil, fmt.Errorf("closing send direction: %w", err)
	}
	resp, err := got.resp, got.err
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

	if cancelled {
		return resp, fmt.Errorf("execution cancelled: %w", ctx.Err())
	}
	if detail := resp.GetErrorDetail(); detail != nil && detail.GetCode() != 0 {
		return resp, fmt.Errorf("nighthawk reported failure: %s", detail.GetMessage())
	}
	return resp, nil
}
