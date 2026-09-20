package nh

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"google.golang.org/grpc"

	client "github.com/envoyproxy/nighthawk/api/client"
	distributor "github.com/envoyproxy/nighthawk/api/distributor"
)

// Distribute sends one execution request to a nighthawk_distributor, which
// fans it out to targets and streams back one response per target.
//
// The distributor forwards a single ExecutionRequest unchanged, so every target
// runs the same CommandLineOptions. compile.ForPool has therefore already
// divided the plan's aggregate rate by targets x concurrency: opts carries the
// per-target share, and this function must not divide it again.
//
// Marked experimental upstream (envoyproxy/nighthawk#369), and no released
// Nighthawk binary hosts this service.
func Distribute(
	ctx context.Context,
	conn *grpc.ClientConn,
	opts *client.CommandLineOptions,
	targets []string,
) ([]string, []*client.Output, error) {
	addrs := make([]*corev3.Address, 0, len(targets))
	for _, t := range targets {
		addr, err := socketAddress(t)
		if err != nil {
			return nil, nil, err
		}
		addrs = append(addrs, addr)
	}

	stub := distributor.NewNighthawkDistributorClient(conn)
	stream, err := stub.DistributedRequestStream(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("opening distributor stream: %w", err)
	}

	req := &distributor.DistributedRequest{
		ExecutionRequest: &client.ExecutionRequest{
			CommandSpecificOptions: &client.ExecutionRequest_StartRequest{
				StartRequest: &client.StartRequest{Options: opts},
			},
		},
		Services: addrs,
	}
	if err := stream.Send(req); err != nil {
		return nil, nil, fmt.Errorf("sending distributed request: %w", err)
	}
	if err := stream.CloseSend(); err != nil {
		return nil, nil, fmt.Errorf("closing send direction: %w", err)
	}

	var names []string
	var outputs []*client.Output
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("awaiting distributed response: %w", err)
		}
		for _, sr := range resp.GetServiceResponse() {
			name := formatAddress(sr.GetService())
			if e := sr.GetError(); e != nil && e.GetCode() != 0 {
				return nil, nil, fmt.Errorf("backend %s: %s", name, e.GetMessage())
			}
			er := sr.GetExecutionResponse()
			if detail := er.GetErrorDetail(); detail != nil && detail.GetCode() != 0 {
				return nil, nil, fmt.Errorf("backend %s: %s", name, detail.GetMessage())
			}
			names = append(names, name)
			outputs = append(outputs, er.GetOutput())
		}
	}
	// Anything less than one result per target means the pool that ran is not
	// the pool the plan described. Evaluating thresholds against the subset
	// would report a pass for a run that never happened in full.
	if len(outputs) != len(targets) {
		return nil, nil, fmt.Errorf(
			"distributor returned %d results for %d targets (%s); refusing to judge a partial pool",
			len(outputs), len(targets), strings.Join(names, ", "))
	}
	return names, outputs, nil
}

func socketAddress(hostPort string) (*corev3.Address, error) {
	idx := strings.LastIndex(hostPort, ":")
	if idx < 0 {
		return nil, fmt.Errorf("address %q: want host:port", hostPort)
	}
	host := strings.TrimSuffix(strings.TrimPrefix(hostPort[:idx], "["), "]")
	port, err := strconv.ParseUint(hostPort[idx+1:], 10, 16)
	if err != nil {
		return nil, fmt.Errorf("address %q: invalid port: %w", hostPort, err)
	}
	return &corev3.Address{
		Address: &corev3.Address_SocketAddress{
			SocketAddress: &corev3.SocketAddress{
				Address:       host,
				PortSpecifier: &corev3.SocketAddress_PortValue{PortValue: uint32(port)},
			},
		},
	}, nil
}

func formatAddress(a *corev3.Address) string {
	sa := a.GetSocketAddress()
	if sa == nil {
		return "unknown"
	}
	return fmt.Sprintf("%s:%d", sa.GetAddress(), sa.GetPortValue())
}
