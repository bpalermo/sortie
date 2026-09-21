package nh_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	client "github.com/envoyproxy/nighthawk/api/client"
	distributorpb "github.com/envoyproxy/nighthawk/api/distributor"

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

		resp := &distributorpb.DistributedResponse{}
		for _, name := range f.answerAs(requested) {
			host, port := splitHostPort(name)
			resp.ServiceResponse = append(resp.ServiceResponse, &distributorpb.DistributedServiceResponse{
				Service: &corev3.Address{Address: &corev3.Address_SocketAddress{
					SocketAddress: &corev3.SocketAddress{
						Address:       host,
						PortSpecifier: &corev3.SocketAddress_PortValue{PortValue: port},
					},
				}},
				DistributedResponseType: &distributorpb.DistributedServiceResponse_ExecutionResponse{
					ExecutionResponse: &client.ExecutionResponse{
						Output: &client.Output{Results: []*client.Result{{
							Name:              "global",
							ExecutionDuration: durationpb.New(time.Second),
						}}},
					},
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

	if _, err := nh.Execute(ctx, conn, &client.CommandLineOptions{}); err == nil {
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
