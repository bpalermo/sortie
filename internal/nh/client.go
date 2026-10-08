// Package nh drives Nighthawk's gRPC control plane: the per-instance execution
// service, the distributor that fans one request out to several instances, and
// the sink that stores results by execution id.
package nh

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

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
var cancelGrace = 30 * time.Second

// arrivedGrace is how long an Execute whose deadline has passed still waits
// for an answer that may already be there, before it gives the run up and
// cancels it. A deadline is wall time, and the process that holds it can be
// stopped for a while -- its node frozen, its container paused -- and wake to
// find the deadline gone and the backend's complete result waiting to be
// read, or about to be retransmitted. Giving up at once would call a backend
// silent that had answered in time. It costs a backend that really is silent
// this much longer to be reported, after the minutes already waited.
var arrivedGrace = 10 * time.Second

// ErrBackendDeadline is the cause of a context made by WithBackendDeadline
// that ended on its deadline.
var ErrBackendDeadline = errors.New("the backend's answer is overdue")

type callerKey struct{}

// WithBackendDeadline returns a context for an Execute or a Distribute that
// bounds how long a backend's answer is waited for. It ends with parent, at
// once, like any derived context; and d after it was made, which is what
// arrivedGrace applies to. The two are kept apart because they are not the
// same event: parent ending is the caller wanting the run stopped, and is
// still acted on at once while a missed deadline is being given its grace.
func WithBackendDeadline(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeoutCause(parent, d, ErrBackendDeadline)
	return context.WithValue(ctx, callerKey{}, parent), cancel
}

// overdue says whether ctx ended on the deadline for the backend's answer.
func overdue(ctx context.Context) bool {
	return ctx.Err() != nil && context.Cause(ctx) == ErrBackendDeadline
}

// callerDone is closed when the caller of WithBackendDeadline wants the run
// stopped; for any other context it is ctx's own Done.
func callerDone(ctx context.Context) <-chan struct{} {
	if parent, ok := ctx.Value(callerKey{}).(context.Context); ok {
		return parent.Done()
	}
	return ctx.Done()
}

// withArrivedGrace returns a context for a stream that is already open and
// must outlive ctx by arrivedGrace when ctx ends on the deadline for the
// backend's answer -- unless the caller wants the run stopped meanwhile --
// and ends with ctx otherwise. It carries ctx's values and not its deadline.
func withArrivedGrace(ctx context.Context) (context.Context, context.CancelFunc) {
	out, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	go func() {
		select {
		case <-out.Done():
			return
		case <-ctx.Done():
		}
		if overdue(ctx) {
			select {
			case <-out.Done():
				return
			case <-callerDone(ctx):
			case <-time.After(arrivedGrace):
			}
		}
		cancel(context.Cause(ctx))
	}()
	return out, func() { cancel(context.Canceled) }
}

// SetCancelGraceForTest shortens the wait for a cancelled execution's answer,
// and the wait for an answer that had already arrived when a deadline passed,
// so that a test of a silent backend does not take the real ones. It returns
// a function that restores them.
func SetCancelGraceForTest(d time.Duration) (restore func()) {
	oldCancel, oldArrived := cancelGrace, arrivedGrace
	cancelGrace, arrivedGrace = d, d
	return func() { cancelGrace, arrivedGrace = oldCancel, oldArrived }
}

// MaxConcurrentExecutionsTrailer is the trailing metadata a service sets on a
// stream whose start it refused for running as many executions as it allows
// (ServiceImpl::MaxConcurrentExecutionsTrailer in the engine); its value is
// that maximum.
const MaxConcurrentExecutionsTrailer = "nighthawk-max-concurrent-executions"

// BusyError is a start the service refused because it is already running as
// many executions as it allows. Nothing was started. It unwraps to the stream's
// status, whose code is ResourceExhausted.
//
// The code alone does not identify it -- gRPC reports a message over the size
// limit with the same one -- so Execute returns a BusyError only for a
// ResourceExhausted that carries the service's trailer.
type BusyError struct {
	// Max is how many executions the service runs at once, 0 when it did not
	// say so legibly.
	Max int
	Err error
}

func (e *BusyError) Error() string {
	if e.Max > 0 {
		return fmt.Sprintf("the service refused the start: it is at its cap of %d concurrent executions (%v)", e.Max, e.Err)
	}
	return fmt.Sprintf("the service refused the start: it is at its cap of concurrent executions (%v)", e.Err)
}

func (e *BusyError) Unwrap() error { return e.Err }

// busy returns err as a BusyError when the stream that ended with it was
// refused at the service's execution cap, and nil otherwise. The trailer is
// readable once Recv has returned an error, which is where err comes from.
func busy(stream grpc.ClientStream, err error) *BusyError {
	if status.Code(err) != codes.ResourceExhausted {
		return nil
	}
	values := stream.Trailer().Get(MaxConcurrentExecutionsTrailer)
	if len(values) == 0 {
		return nil
	}
	limit, convErr := strconv.Atoi(values[0])
	if convErr != nil || limit < 0 {
		limit = 0
	}
	return &BusyError{Max: limit, Err: err}
}

// The service's two refusals at its execution cap, as worded in the engine's
// ServiceImpl::ExecutionStream. With a cap above one the message gives the
// number running, which at a refusal is the cap.
var busyMessage = regexp.MustCompile(`Busy: (\d+) executions are running, the maximum this service allows`)

const busySingleMessage = "Only a single benchmark session is allowed at a time."

// busyFromStatus is busy for a refusal that arrives second hand, as the
// google.rpc.Status a distributor hands back for one of its targets: the
// code it copied from the service's stream and the service's message. The
// trailer that marks the refusal on a direct stream does not travel that way,
// so here it is the code together with the service's own wording that
// identifies it, and the wording that gives the cap.
func busyFromStatus(code int32, message string) *BusyError {
	if codes.Code(code) != codes.ResourceExhausted {
		return nil
	}
	limit := 0
	if m := busyMessage.FindStringSubmatch(message); m != nil {
		limit, _ = strconv.Atoi(m[1])
	} else if strings.Contains(message, busySingleMessage) {
		limit = 1
	} else {
		return nil
	}
	return &BusyError{Max: limit, Err: status.Error(codes.ResourceExhausted, message)}
}

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
// A start the service refused because it is at its execution cap is returned
// as a *BusyError.
func Execute(ctx context.Context, conn *grpc.ClientConn, opts *client.CommandLineOptions, progress *Progress) (*client.ExecutionResponse, error) {
	stub := client.NewNighthawkServiceClient(conn)
	// The grace is for a run that was in flight when its deadline passed. A
	// context that has already ended starts nothing.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("not started: %w", err)
	}
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
		// A stream the service has already ended fails a Send with io.EOF and
		// keeps its status for Recv. That status may be the refusal of a
		// service at its execution cap, which the caller must be able to tell
		// from any other failed start.
		if err == io.EOF {
			if _, recvErr := stream.Recv(); recvErr != nil && recvErr != io.EOF {
				if refused := busy(stream, recvErr); refused != nil {
					return nil, refused
				}
				err = recvErr
			}
		}
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
	answered := false
	select {
	case got = <-first:
		answered = true
	case <-ctx.Done():
	}
	// Both can be ready at once, and select then picks either. The deadline
	// for the backend's answer having passed is not yet a backend that did
	// not answer: see arrivedGrace. A caller's own cancellation or deadline
	// is acted on at once.
	if !answered && overdue(ctx) {
		select {
		case got = <-first:
			answered = true
		case <-callerDone(ctx):
		case <-time.After(arrivedGrace):
		}
	}
	if !answered {
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
		if refused := busy(stream, err); refused != nil {
			return nil, refused
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
