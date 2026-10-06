package plan

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strconv"

	"google.golang.org/protobuf/proto"
)

// Resolver looks a host name up. *net.Resolver satisfies it, and
// net.DefaultResolver is what `run` uses; tests substitute a fake.
type Resolver interface {
	// LookupHost returns the host's addresses, A and AAAA records both, as
	// strings.
	LookupHost(ctx context.Context, host string) ([]string, error)
}

// ErrNoAddress is the cause of a ResolveError for a name that resolved to no
// address at all.
var ErrNoAddress = errors.New("the name resolved to no address")

// ResolveError is a dns pool whose name could not be turned into backends:
// the lookup failed, or it answered with no address. It happens before any
// load is generated and is the plan's (or its environment's) fault, which is
// why the CLI reports it as a usage error.
type ResolveError struct {
	Pool string
	Dns  string
	Err  error
}

func (e *ResolveError) Error() string {
	return fmt.Sprintf("pool %q: resolving %s: %v", e.Pool, e.Dns, e.Err)
}

func (e *ResolveError) Unwrap() error { return e.Err }

// Resolve returns a copy of p in which every dns pool also lists, as its
// services, the addresses its name resolved to, each at the pool's port and
// in sorted order: the pool a run dispatches to. Pools without dns are
// copied as they are. The name is looked up once; the addresses stand for
// the whole run, so a node joining halfway gets no load and a node leaving
// fails its backend's execution visibly rather than silently shrinking the
// pool.
//
// Sorting makes the backend order -- and so the execution ids and the order
// of the report -- a property of the addresses rather than of the order the
// name server happened to answer in.
func Resolve(ctx context.Context, p *Plan, r Resolver) (*Plan, error) {
	out := proto.Clone(p).(*Plan)
	for _, pool := range out.GetPools() {
		if pool.GetDns() == "" {
			continue
		}
		services, err := ResolvePool(ctx, pool, r)
		if err != nil {
			return nil, err
		}
		pool.Services = services
	}
	return out, nil
}

// ResolvePool looks one dns pool's name up and returns its backends, sorted,
// or a *ResolveError. It is what Resolve does per pool, exported so a caller
// that retries can retry the one name that is not answering yet and keep the
// answers it already has.
func ResolvePool(ctx context.Context, pool *Pool, r Resolver) ([]string, error) {
	services, err := resolvePool(ctx, pool, r)
	if err != nil {
		return nil, &ResolveError{Pool: pool.GetName(), Dns: pool.GetDns(), Err: err}
	}
	return services, nil
}

// SplitDns splits a dns pool's host:port and checks it the way the resolver
// will: the schema's pattern admits shapes net.SplitHostPort does not, such
// as an unclosed bracket or an IPv6 literal without brackets.
func SplitDns(dns string) (host, port string, err error) {
	host, port, err = net.SplitHostPort(dns)
	if err != nil {
		return "", "", err
	}
	if host == "" {
		return "", "", errors.New("the host is empty")
	}
	if n, err := strconv.ParseUint(port, 10, 16); err != nil || n == 0 {
		return "", "", fmt.Errorf("port %q is not in 1..65535", port)
	}
	return host, port, nil
}

func resolvePool(ctx context.Context, pool *Pool, r Resolver) ([]string, error) {
	host, port, err := SplitDns(pool.GetDns())
	if err != nil {
		return nil, err
	}
	raw, err := r.LookupHost(ctx, host)
	if err != nil {
		return nil, err
	}
	addrs := make([]netip.Addr, 0, len(raw))
	for _, a := range raw {
		addr, err := netip.ParseAddr(a)
		if err != nil {
			return nil, fmt.Errorf("the resolver returned %q, which is not an IP address", a)
		}
		addrs = append(addrs, addr)
	}
	slices.SortFunc(addrs, netip.Addr.Compare)
	addrs = slices.Compact(addrs)
	if len(addrs) == 0 {
		return nil, ErrNoAddress
	}
	services := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		services = append(services, net.JoinHostPort(addr.String(), port))
	}
	return services, nil
}
