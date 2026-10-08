package nh

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"google.golang.org/grpc"

	client "github.com/bpalermo/sortie/engine/api/client"
	distributor "github.com/bpalermo/sortie/engine/api/distributor"
)

// TargetError is one target of a distributed execution that did not finish
// cleanly: it reported an error, or the distributor returned nothing for it.
type TargetError struct {
	Target string
	Err    error
}

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
//
// There is no progress on this path: the distributor API carries no
// progress_interval and relays nothing before the targets finish.
//
// Cancelling ctx abandons the distributor RPC and nothing more: the
// distributor API carries one ExecutionRequest and no distributor hosted here
// forwards a CancellationRequest to its targets, so they run to their
// configured duration. Compare Execute, which does cancel a service backend.
func Distribute(
	ctx context.Context,
	conn *grpc.ClientConn,
	opts *client.CommandLineOptions,
	targets []string,
) ([]string, []*client.Output, error) {
	names, outputs, failed, err := DistributePartial(ctx, conn, opts, targets)
	if err != nil {
		return nil, nil, err
	}
	if len(failed) > 0 {
		// All or nothing, for a caller that wants the whole pool or an error.
		problems := make([]string, 0, len(failed))
		for _, f := range failed {
			problems = append(problems, fmt.Sprintf("backend %s: %v", f.Target, f.Err))
		}
		return nil, nil, fmt.Errorf("distributor did not run the pool as requested: %s", strings.Join(problems, "; "))
	}
	return names, outputs, nil
}

// DistributePartial is Distribute without the all-or-nothing rule: it returns
// the targets that answered with an output, in the order they answered, and
// names the ones that did not finish cleanly beside them. A target that
// reported an error but still returned what it counted is in both. So one
// target going away does not cost the run every other target's results.
//
// A target the distributor reports as refused at its execution cap is named
// with a *BusyError.
//
// The error return is for what leaves nothing to trust: the request could not
// be sent, or what came back does not match the pool -- a target answered
// twice, or something answered that was never targeted.
func DistributePartial(
	ctx context.Context,
	conn *grpc.ClientConn,
	opts *client.CommandLineOptions,
	targets []string,
) ([]string, []*client.Output, []TargetError, error) {
	addrs := make([]*corev3.Address, 0, len(targets))
	for _, t := range targets {
		addr, err := socketAddress(t)
		if err != nil {
			return nil, nil, nil, err
		}
		addrs = append(addrs, addr)
	}

	// The stream outlives the deadline for the targets' answers by a little:
	// see arrivedGrace. It ends with ctx for any other reason ctx ends.
	streamCtx, closeStream := withArrivedGrace(ctx)
	defer closeStream()
	stub := distributor.NewNighthawkDistributorClient(conn)
	stream, err := stub.DistributedRequestStream(streamCtx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("opening distributor stream: %w", err)
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
		return nil, nil, nil, fmt.Errorf("sending distributed request: %w", err)
	}
	if err := stream.CloseSend(); err != nil {
		return nil, nil, nil, fmt.Errorf("closing send direction: %w", err)
	}

	// answered is every target heard from, with or without an output: it is
	// what the pool is checked against. names and outputs are the ones with
	// something to judge.
	var answered, names []string
	var outputs []*client.Output
	var failed []TargetError
	var streamErr error
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			// The stream broke. What arrived before it did still counts; the
			// targets not heard from are reported with this as the reason.
			streamErr = fmt.Errorf("awaiting distributed response: %w", err)
			break
		}
		for _, sr := range resp.GetServiceResponse() {
			name := formatAddress(sr.GetService())
			answered = append(answered, name)
			if e := sr.GetError(); e != nil && e.GetCode() != 0 {
				// A target at its execution cap is the same *BusyError a
				// direct backend gives, so the runner treats both alike.
				if refused := busyFromStatus(e.GetCode(), e.GetMessage()); refused != nil {
					failed = append(failed, TargetError{Target: name, Err: refused})
					continue
				}
				failed = append(failed, TargetError{Target: name, Err: errors.New(e.GetMessage())})
				continue
			}
			er := sr.GetExecutionResponse()
			if detail := er.GetErrorDetail(); detail != nil && detail.GetCode() != 0 {
				failed = append(failed, TargetError{Target: name, Err: errors.New(detail.GetMessage())})
			}
			out := er.GetOutput()
			if len(out.GetResults()) == 0 {
				// Answered, with nothing to judge. Without an error of its
				// own that is still a failed target: silently leaving it out
				// would let the others' thresholds pass a pool one member of
				// which reported nothing.
				if er.GetErrorDetail().GetCode() == 0 {
					failed = append(failed, TargetError{Target: name, Err: errors.New("returned no results")})
				}
				continue
			}
			names = append(names, name)
			outputs = append(outputs, out)
		}
	}
	// The pool that ran has to be the pool the plan described. Counting results
	// is not enough: two results for one target and none for another would
	// satisfy a count check while a backend never ran at all, and thresholds
	// would then pass on a pool that was never fully exercised. Match
	// identities. A target answering twice, or an untargeted one answering,
	// means the answers cannot be attributed and is an error outright; a
	// target that did not answer is a failed target.
	missing, problems := matchTargets(targets, answered)
	if len(problems) > 0 {
		// Say the whole of what went wrong, the silent targets included.
		if len(missing) > 0 {
			problems = append([]string{"no result from " + strings.Join(missing, ", ")}, problems...)
		}
		return nil, nil, nil, fmt.Errorf("distributor did not run the pool as requested: %s",
			strings.Join(problems, "; "))
	}
	for _, target := range missing {
		reason := errors.New("no result")
		if streamErr != nil {
			reason = streamErr
		}
		failed = append(failed, TargetError{Target: target, Err: reason})
	}
	return names, outputs, failed, nil
}

// matchTargets compares the targets that answered with the ones requested. It
// returns the requested targets that did not answer, and a description of
// anything that makes the answers unattributable: a target that answered more
// than once, or an answer from something that was not targeted.
func matchTargets(targets, answered []string) (missing, problems []string) {
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

	var duplicated []string
	for _, target := range targets {
		switch seen[canonicalAddress(target)] {
		case 1:
		case 0:
			missing = append(missing, target)
		default:
			duplicated = append(duplicated, fmt.Sprintf("%s (x%d)", target, seen[canonicalAddress(target)]))
		}
	}
	if len(duplicated) > 0 {
		problems = append(problems, "repeated results from "+strings.Join(duplicated, ", "))
	}
	if len(unexpected) > 0 {
		problems = append(problems, "results from untargeted "+strings.Join(unexpected, ", "))
	}
	return missing, problems
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
