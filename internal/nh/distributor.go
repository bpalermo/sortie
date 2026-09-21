package nh

import (
	"context"
	"fmt"
	"io"
	"net"
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
	// The pool that ran has to be the pool the plan described. Counting results
	// is not enough: two results for one target and none for another would
	// satisfy a count check while a backend never ran at all, and thresholds
	// would then pass on a pool that was never fully exercised. Match identities.
	if err := checkTargetsAnswered(targets, names); err != nil {
		return nil, nil, err
	}
	return names, outputs, nil
}

// checkTargetsAnswered reports whether every requested target answered exactly
// once, naming what was missing or duplicated rather than only the counts.
func checkTargetsAnswered(targets, answered []string) error {
	// Compare canonical forms: what comes back is whatever the distributor
	// echoed, which need not be spelled the way the plan wrote it.
	wanted := make(map[string]string, len(targets))
	for _, target := range targets {
		wanted[canonicalAddress(target)] = target
	}

	seen := make(map[string]int, len(answered))
	var unexpected []string
	for _, name := range answered {
		key := canonicalAddress(name)
		if _, ok := wanted[key]; !ok {
			unexpected = append(unexpected, name)
			continue
		}
		seen[key]++
	}

	var missing, duplicated []string
	for _, target := range targets {
		switch seen[canonicalAddress(target)] {
		case 1:
		case 0:
			missing = append(missing, target)
		default:
			duplicated = append(duplicated, fmt.Sprintf("%s (x%d)", target, seen[canonicalAddress(target)]))
		}
	}

	var problems []string
	if len(missing) > 0 {
		problems = append(problems, "no result from "+strings.Join(missing, ", "))
	}
	if len(duplicated) > 0 {
		problems = append(problems, "repeated results from "+strings.Join(duplicated, ", "))
	}
	if len(unexpected) > 0 {
		problems = append(problems, "results from untargeted "+strings.Join(unexpected, ", "))
	}
	if len(problems) > 0 {
		return fmt.Errorf("distributor did not run the pool as requested: %s",
			strings.Join(problems, "; "))
	}
	return nil
}

func socketAddress(hostPort string) (*corev3.Address, error) {
	// net.SplitHostPort understands the bracketed form an IPv6 literal must be
	// written in; splitting on the last colon by hand does not.
	host, portText, err := net.SplitHostPort(hostPort)
	if err != nil {
		return nil, fmt.Errorf("address %q: want host:port: %w", hostPort, err)
	}
	port, err := strconv.ParseUint(portText, 10, 16)
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
	// JoinHostPort brackets an IPv6 literal. Formatting it as "%s:%d" would
	// produce 2001:db8::1:8443, which is ambiguous and matches no target.
	return net.JoinHostPort(sa.GetAddress(), strconv.FormatUint(uint64(sa.GetPortValue()), 10))
}

// canonicalAddress normalises host:port so the bracketed and unbracketed
// spellings of the same IPv6 literal compare equal. An address that does not
// parse is returned unchanged, so it fails identity matching as itself rather
// than as something else.
func canonicalAddress(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return net.JoinHostPort(host, port)
}
