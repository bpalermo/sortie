package plan

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// fakeResolver answers from a table, and records what it was asked.
type fakeResolver struct {
	hosts map[string][]string
	asked []string
}

func (f *fakeResolver) LookupHost(_ context.Context, host string) ([]string, error) {
	f.asked = append(f.asked, host)
	addrs, ok := f.hosts[host]
	if !ok {
		return nil, fmt.Errorf("lookup %s: no such host", host)
	}
	return addrs, nil
}

const dnsPlan = `
version: v1
pools:
  - name: nodes
    dns: engine.aether-test.svc.cluster.local:8443
  - name: fixed
    services: ["127.0.0.1:8443"]
scenarios:
  - name: smoke
    pool: nodes
    target: http://127.0.0.1:8080/
    executor: {type: constant-rate, rate: 60, duration: 30s, per_backend: true}
`

// A dns pool parses without being resolved: validate and compile must accept
// it with no network, and per_backend is a plain executor field.
func TestParseAcceptsADnsPoolWithoutResolving(t *testing.T) {
	p, err := Parse([]byte(dnsPlan))
	if err != nil {
		t.Fatal(err)
	}
	pool := PoolFor(p, p.GetScenarios()[0])
	if pool.GetDns() != "engine.aether-test.svc.cluster.local:8443" || len(pool.GetServices()) != 0 {
		t.Errorf("pool = %v, want the dns name and no services", pool)
	}
	if !p.GetScenarios()[0].GetExecutor().GetPerBackend() {
		t.Error("per_backend did not parse")
	}
}

// Resolve turns the name into sorted <ip>:<port> services, IPv6 bracketed,
// and leaves the other pools and the source plan alone.
func TestResolveFillsServicesFromDns(t *testing.T) {
	p, err := Parse([]byte(dnsPlan))
	if err != nil {
		t.Fatal(err)
	}
	r := &fakeResolver{hosts: map[string][]string{
		"engine.aether-test.svc.cluster.local": {"10.0.2.5", "fd00::7", "10.0.1.9", "10.0.2.5"},
	}}

	resolved, err := Resolve(context.Background(), p, r)
	if err != nil {
		t.Fatal(err)
	}
	got := PoolFor(resolved, resolved.GetScenarios()[0])
	want := []string{"10.0.1.9:8443", "10.0.2.5:8443", "[fd00::7]:8443"}
	if strings.Join(got.GetServices(), " ") != strings.Join(want, " ") {
		t.Errorf("services = %v, want %v (sorted, deduplicated, IPv6 bracketed)", got.GetServices(), want)
	}
	if got.GetDns() == "" {
		t.Error("the resolved pool must keep its dns name, for the report")
	}
	if len(r.asked) != 1 || r.asked[0] != "engine.aether-test.svc.cluster.local" {
		t.Errorf("resolver asked %v, want the host alone, once", r.asked)
	}
	if fixed := resolved.GetPools()[1]; fixed.GetServices()[0] != "127.0.0.1:8443" || fixed.GetDns() != "" {
		t.Errorf("the services pool changed: %v", fixed)
	}
	if len(PoolFor(p, p.GetScenarios()[0]).GetServices()) != 0 {
		t.Error("Resolve mutated the source plan")
	}
}

func TestResolveErrors(t *testing.T) {
	p, err := Parse([]byte(dnsPlan))
	if err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		hosts     map[string][]string
		want      string
		noAddress bool
	}{
		"no such host": {
			hosts: map[string][]string{},
			want:  "no such host",
		},
		"no address": {
			hosts:     map[string][]string{"engine.aether-test.svc.cluster.local": {}},
			want:      "resolved to no address",
			noAddress: true,
		},
		"not an address": {
			hosts: map[string][]string{"engine.aether-test.svc.cluster.local": {"node-1"}},
			want:  `"node-1", which is not an IP address`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Resolve(context.Background(), p, &fakeResolver{hosts: tc.hosts})
			var re *ResolveError
			if !errors.As(err, &re) {
				t.Fatalf("err = %v, want a *ResolveError", err)
			}
			if re.Pool != "nodes" || re.Dns != "engine.aether-test.svc.cluster.local:8443" {
				t.Errorf("ResolveError names pool %q, dns %q", re.Pool, re.Dns)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %q, want it to contain %q", err, tc.want)
			}
			if errors.Is(err, ErrNoAddress) != tc.noAddress {
				t.Errorf("errors.Is(err, ErrNoAddress) = %v, want %v", !tc.noAddress, tc.noAddress)
			}
		})
	}
}
