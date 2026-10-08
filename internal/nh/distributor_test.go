package nh_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	client "github.com/bpalermo/sortie/engine/api/client"
	distributorpb "github.com/bpalermo/sortie/engine/api/distributor"

	"github.com/bpalermo/sortie/internal/nh"
)

// fakeDistributor stands in for nighthawk_distributor, which Nighthawk itself
// ships no binary for: the service exists in its sources as a library and no
// released binary hosts it. Without a fake there is no way to exercise this
// dispatch path at all, and it would be the one path in sortie that nothing
// ever runs.
type fakeDistributor struct {
	distributorpb.UnimplementedNighthawkDistributorServer

	addr string

	// answerAs names the services the fake reports results for, which need not
	// match the targets it was asked about -- that is the point of the tests.
	answerAs func(requested []string) []string

	// emptyFor names a service the fake answers for with a response that has
	// no error and no results.
	emptyFor string

	// errorFor gives the services the fake reports an error for instead of a
	// response, the way a distributor relays a target's failed stream.
	errorFor map[string]*rpcstatus.Status

	// delay is how long the fake takes to answer.
	delay time.Duration
}

func (f *fakeDistributor) DistributedRequestStream(
	stream distributorpb.NighthawkDistributor_DistributedRequestStreamServer,
) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		requested := make([]string, 0, len(req.GetServices()))
		for _, svc := range req.GetServices() {
			sa := svc.GetSocketAddress()
			// A real distributor echoes back the address it was handed. Join
			// rather than format, so an IPv6 literal stays unambiguous.
			requested = append(requested,
				net.JoinHostPort(sa.GetAddress(), strconv.FormatUint(uint64(sa.GetPortValue()), 10)))
		}

		time.Sleep(f.delay)
		resp := &distributorpb.DistributedResponse{}
		for _, name := range f.answerAs(requested) {
			host, port := splitHostPort(name)
			if failure := f.errorFor[name]; failure != nil {
				resp.ServiceResponse = append(resp.ServiceResponse, &distributorpb.DistributedServiceResponse{
					Service: &corev3.Address{Address: &corev3.Address_SocketAddress{
						SocketAddress: &corev3.SocketAddress{
							Address:       host,
							PortSpecifier: &corev3.SocketAddress_PortValue{PortValue: port},
						},
					}},
					DistributedResponseType: &distributorpb.DistributedServiceResponse_Error{Error: failure},
				})
				continue
			}
			resp.ServiceResponse = append(resp.ServiceResponse, &distributorpb.DistributedServiceResponse{
				Service: &corev3.Address{Address: &corev3.Address_SocketAddress{
					SocketAddress: &corev3.SocketAddress{
						Address:       host,
						PortSpecifier: &corev3.SocketAddress_PortValue{PortValue: port},
					},
				}},
				DistributedResponseType: &distributorpb.DistributedServiceResponse_ExecutionResponse{
					ExecutionResponse: f.responseFor(name),
				},
			})
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func splitHostPort(addr string) (string, uint32) {
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, 0
	}
	var port uint32
	_, _ = fmt.Sscanf(portText, "%d", &port)
	return host, port
}

func (f *fakeDistributor) responseFor(name string) *client.ExecutionResponse {
	if name == f.emptyFor {
		return &client.ExecutionResponse{}
	}
	return &client.ExecutionResponse{
		Output: &client.Output{Results: []*client.Result{{
			Name:              "global",
			ExecutionDuration: durationpb.New(time.Second),
		}}},
	}
}

func startFakeDistributor(t *testing.T, answerAs func([]string) []string) *fakeDistributor {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	fake := &fakeDistributor{addr: listener.Addr().String(), answerAs: answerAs}

	server := grpc.NewServer()
	distributorpb.RegisterNighthawkDistributorServer(server, fake)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return fake
}

func distribute(t *testing.T, fake *fakeDistributor, targets []string) ([]string, error) {
	t.Helper()
	ctx := context.Background()
	conn, err := nh.Dial(ctx, fake.addr)
	if err != nil {
		t.Fatalf("dialling the fake distributor: %v", err)
	}
	defer conn.Close()

	names, _, err := nh.Distribute(ctx, conn, &client.CommandLineOptions{}, targets)
	return names, err
}

func TestDistributeAcceptsOneResultPerTarget(t *testing.T) {
	targets := []string{"10.0.0.11:8443", "10.0.0.12:8443"}
	fake := startFakeDistributor(t, func(requested []string) []string { return requested })

	names, err := distribute(t, fake, targets)
	if err != nil {
		t.Fatalf("Distribute: %v", err)
	}
	if len(names) != 2 {
		t.Fatalf("got %d results, want one per target", len(names))
	}
}

// A target that never answered means the pool the plan described did not run,
// so thresholds must not be evaluated against what did.
func TestDistributeRejectsAMissingTarget(t *testing.T) {
	targets := []string{"10.0.0.11:8443", "10.0.0.12:8443"}
	fake := startFakeDistributor(t, func(requested []string) []string {
		return requested[:1]
	})

	_, err := distribute(t, fake, targets)
	if err == nil {
		t.Fatal("a missing target must be rejected")
	}
	if !strings.Contains(err.Error(), "10.0.0.12:8443") {
		t.Errorf("error should name the silent target, got: %v", err)
	}
}

// Counting results is not enough: two answers from one target and none from
// another has the right total and the wrong pool.
func TestDistributeRejectsDuplicateResults(t *testing.T) {
	targets := []string{"10.0.0.11:8443", "10.0.0.12:8443"}
	fake := startFakeDistributor(t, func(requested []string) []string {
		return []string{requested[0], requested[0]}
	})

	_, err := distribute(t, fake, targets)
	if err == nil {
		t.Fatal("duplicate results must be rejected even though the count matches")
	}
	if !strings.Contains(err.Error(), "repeated") || !strings.Contains(err.Error(), "no result from") {
		t.Errorf("error should name both the duplicate and the silent target, got: %v", err)
	}
}

func TestDistributeRejectsAnUntargetedResult(t *testing.T) {
	targets := []string{"10.0.0.11:8443"}
	fake := startFakeDistributor(t, func([]string) []string {
		return []string{"10.0.0.99:8443"}
	})

	_, err := distribute(t, fake, targets)
	if err == nil {
		t.Fatal("a result from a backend nobody asked about must be rejected")
	}
	if !strings.Contains(err.Error(), "untargeted") {
		t.Errorf("error should flag the untargeted result, got: %v", err)
	}
}

// fakeFailingService responds and then ends the stream with a non-OK status,
// which a streaming RPC is free to do. Treating that as success would have
// sortie evaluate thresholds against a run whose backend failed after
// reporting.
type fakeFailingService struct {
	client.UnimplementedNighthawkServiceServer
	addr string
}

func (f *fakeFailingService) ExecutionStream(stream client.NighthawkService_ExecutionStreamServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	if err := stream.Send(&client.ExecutionResponse{
		Output: &client.Output{Results: []*client.Result{{Name: "global"}}},
	}); err != nil {
		return err
	}
	return status.Error(codes.Internal, "backend died after responding")
}

func TestExecuteSurfacesAFailureAfterTheResponse(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeFailingService{addr: listener.Addr().String()}
	server := grpc.NewServer()
	client.RegisterNighthawkServiceServer(server, fake)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	ctx := context.Background()
	conn, err := nh.Dial(ctx, fake.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := nh.Execute(ctx, conn, &client.CommandLineOptions{}, nil); err == nil {
		t.Fatal("a non-OK status after the response must not read as success")
	} else if !strings.Contains(err.Error(), "backend died after responding") {
		t.Errorf("error should quote the backend, got: %v", err)
	}
}

// An IPv6 target must survive the round trip through the distributor. sortie
// strips the brackets when building the request and the distributor echoes the
// bare literal back, so an identity check that compares raw strings rejects a
// perfectly good pool.
func TestDistributeAcceptsIPv6Targets(t *testing.T) {
	targets := []string{"[2001:db8::1]:8443", "[2001:db8::2]:8443"}
	fake := startFakeDistributor(t, func(requested []string) []string { return requested })

	names, err := distribute(t, fake, targets)
	if err != nil {
		t.Fatalf("Distribute rejected valid IPv6 targets: %v", err)
	}
	if len(names) != 2 {
		t.Fatalf("got %d results, want one per target", len(names))
	}
	for _, name := range names {
		if !strings.HasPrefix(name, "[") {
			t.Errorf("result %q should be bracketed; %q is ambiguous", name, name)
		}
	}
}

// And a missing IPv6 target is still caught, so the normalisation has not
// turned the check into one that accepts anything.
func TestDistributeRejectsAMissingIPv6Target(t *testing.T) {
	targets := []string{"[2001:db8::1]:8443", "[2001:db8::2]:8443"}
	fake := startFakeDistributor(t, func(requested []string) []string { return requested[:1] })

	if _, err := distribute(t, fake, targets); err == nil {
		t.Fatal("a missing IPv6 target must still be rejected")
	}
}

// One target failing does not cost the others their results: DistributePartial
// returns what answered and names what did not.
func TestDistributePartialKeepsTheTargetsThatAnswered(t *testing.T) {
	targets := []string{"10.0.0.11:8443", "10.0.0.12:8443", "10.0.0.13:8443"}
	// The fake answers for the first two only.
	fake := startFakeDistributor(t, func(requested []string) []string { return requested[:2] })

	ctx := context.Background()
	conn, err := nh.Dial(ctx, fake.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	names, outputs, failed, err := nh.DistributePartial(ctx, conn, &client.CommandLineOptions{}, targets)
	if err != nil {
		t.Fatalf("DistributePartial: %v", err)
	}
	if len(names) != 2 || len(outputs) != 2 {
		t.Fatalf("got %d names and %d outputs, want the two targets that answered", len(names), len(outputs))
	}
	if len(failed) != 1 || failed[0].Target != "10.0.0.13:8443" {
		t.Fatalf("failed = %+v, want the silent target", failed)
	}
	// Distribute, the all-or-nothing form, still refuses the same run.
	if _, err := distribute(t, fake, targets); err == nil || !strings.Contains(err.Error(), "10.0.0.13:8443") {
		t.Errorf("Distribute err = %v, want it to name the silent target", err)
	}
}

// A target that answers with no error and no results is a failed target: left
// in as a success, or left out silently, it would let the others' thresholds
// pass a pool one member of which reported nothing.
func TestDistributePartialFailsATargetThatReturnsNothing(t *testing.T) {
	targets := []string{"10.0.0.11:8443", "10.0.0.12:8443"}
	fake := startFakeDistributor(t, func(requested []string) []string { return requested })
	fake.emptyFor = "10.0.0.12:8443"

	ctx := context.Background()
	conn, err := nh.Dial(ctx, fake.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	names, outputs, failed, err := nh.DistributePartial(ctx, conn, &client.CommandLineOptions{}, targets)
	if err != nil {
		t.Fatalf("DistributePartial: %v", err)
	}
	if len(names) != 1 || names[0] != "10.0.0.11:8443" || len(outputs) != 1 || outputs[0] == nil {
		t.Errorf("names = %v with %d outputs, want only the target that returned results", names, len(outputs))
	}
	if len(failed) != 1 || failed[0].Target != "10.0.0.12:8443" || !strings.Contains(failed[0].Err.Error(), "no results") {
		t.Errorf("failed = %+v, want the empty target named", failed)
	}
}

// A target the distributor reports as refused at its execution cap comes back
// as the BusyError a direct backend gives, with the cap read from the
// service's message -- the code and the message are all a distributor passes
// on. Anything else stays an ordinary failure: the code with other wording,
// or the wording under another code, as an engine from before the refusal was
// ResourceExhausted sends it.
func TestDistributePartialRecognisesATargetAtItsExecutionCap(t *testing.T) {
	const relayed = "Distributed Execution Request failed: "
	targets := []string{"10.0.0.11:8443", "10.0.0.12:8443", "10.0.0.13:8443", "10.0.0.14:8443", "10.0.0.15:8443"}
	fake := startFakeDistributor(t, func(requested []string) []string { return requested })
	fake.errorFor = map[string]*rpcstatus.Status{
		"10.0.0.12:8443": {Code: 8, Message: relayed + "Busy: 16 executions are running, the maximum this service allows (--max-concurrent-executions)."},
		"10.0.0.13:8443": {Code: 8, Message: relayed + "Only a single benchmark session is allowed at a time."},
		"10.0.0.14:8443": {Code: 8, Message: relayed + "received message larger than max"},
		"10.0.0.15:8443": {Code: 13, Message: relayed + "Busy: 16 executions are running, the maximum this service allows (--max-concurrent-executions)."},
	}

	ctx := context.Background()
	conn, err := nh.Dial(ctx, fake.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	names, _, failed, err := nh.DistributePartial(ctx, conn, &client.CommandLineOptions{}, targets)
	if err != nil {
		t.Fatalf("DistributePartial: %v", err)
	}
	if len(names) != 1 || names[0] != "10.0.0.11:8443" {
		t.Errorf("names = %v, want the one target that ran", names)
	}
	wantMax := map[string]int{"10.0.0.12:8443": 16, "10.0.0.13:8443": 1, "10.0.0.14:8443": -1, "10.0.0.15:8443": -1}
	if len(failed) != len(wantMax) {
		t.Fatalf("failed = %+v, want %d targets", failed, len(wantMax))
	}
	for _, f := range failed {
		var refused *nh.BusyError
		isBusy := errors.As(f.Err, &refused)
		want := wantMax[f.Target]
		switch {
		case want < 0 && isBusy:
			t.Errorf("%s: reported as at its cap: %v", f.Target, f.Err)
		case want >= 0 && (!isBusy || refused.Max != want):
			t.Errorf("%s: err = %v, want a BusyError with a cap of %d", f.Target, f.Err, want)
		case want >= 0 && status.Code(f.Err) != codes.ResourceExhausted:
			t.Errorf("%s: code = %s, want ResourceExhausted", f.Target, status.Code(f.Err))
		}
	}
}

// As for a direct backend: the deadline for the targets' answers having
// passed, an answer that arrives just after it is still the result. A
// deadline that is the caller's own ends the stream at once.
func TestDistributeTakesAnAnswerThatArrivesJustAfterTheDeadline(t *testing.T) {
	defer nh.SetCancelGraceForTest(5 * time.Second)()
	targets := []string{"10.0.0.11:8443"}
	fake := startFakeDistributor(t, func(requested []string) []string { return requested })
	fake.delay = 400 * time.Millisecond
	conn, err := nh.Dial(context.Background(), fake.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ctx, cancel := nh.WithBackendDeadline(context.Background(), 100*time.Millisecond)
	defer cancel()
	names, _, err := nh.Distribute(ctx, conn, &client.CommandLineOptions{}, targets)
	if err != nil || len(names) != 1 {
		t.Fatalf("Distribute: %v with %d results, want the answer that arrived after the deadline", err, len(names))
	}

	own, cancelOwn := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelOwn()
	started := time.Now()
	if _, _, err := nh.Distribute(own, conn, &client.CommandLineOptions{}, targets); err == nil {
		t.Error("Distribute under the caller's own deadline succeeded")
	}
	if took := time.Since(started); took > 2*time.Second {
		t.Errorf("the caller's own deadline was acted on after %s, want at once", took)
	}
}

// As for a direct backend: the caller cancelling during the grace ends the
// stream at once, and a context that has already ended sends no request.
func TestDistributeGraceYieldsToTheCallerAndStartsNothingLate(t *testing.T) {
	defer nh.SetCancelGraceForTest(20 * time.Second)()
	targets := []string{"10.0.0.11:8443"}
	var asked atomic.Int32
	fake := startFakeDistributor(t, func(requested []string) []string {
		asked.Add(1)
		return requested
	})
	fake.delay = 30 * time.Second
	conn, err := nh.Dial(context.Background(), fake.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	parent, stop := context.WithCancel(context.Background())
	ctx, cancel := nh.WithBackendDeadline(parent, 100*time.Millisecond)
	defer cancel()
	time.AfterFunc(400*time.Millisecond, stop)
	started := time.Now()
	if _, _, err := nh.Distribute(ctx, conn, &client.CommandLineOptions{}, targets); err == nil {
		t.Error("Distribute succeeded against a distributor that never answers")
	}
	if took := time.Since(started); took > 5*time.Second {
		t.Errorf("the caller's cancellation during the grace was acted on after %s, want at once", took)
	}

	if _, _, err := nh.Distribute(ctx, conn, &client.CommandLineOptions{}, targets); err == nil {
		t.Error("Distribute sent a request on a context that had already ended")
	}
}
