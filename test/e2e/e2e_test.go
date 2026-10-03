// Package e2e runs a plan through the sortie binary against the engine built in
// this workspace: nighthawk_service as the backend and nighthawk_test_server as
// the target. The unit tests never talk to Nighthawk, so this is the only test
// that can catch sortie generating the wrong load.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
)

// The test server's Envoy configuration: one HTTP listener with the test-server
// filter chain, both it and the admin on ephemeral ports.
const testServerConfig = `admin:
  address:
    socket_address: { address: 127.0.0.1, port_value: 0 }
static_resources:
  listeners:
  - address:
      socket_address: { address: 127.0.0.1, port_value: 0 }
    filter_chains:
    - filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          codec_type: AUTO
          stat_prefix: ingress_http
          route_config:
            name: local_route
            virtual_hosts:
            - name: service
              domains: ["*"]
          http_filters:
          - name: test-server
            typed_config:
              "@type": type.googleapis.com/nighthawk.server.ResponseOptions
              response_body_size: 10
          - name: envoy.filters.http.router
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
              dynamic_stats: false
`

// A short constant-rate plan. 100 rps for 5 s is 500 requests. Two workers, so
// the aggregate rate has to be divided between them: forwarding 100 rps to each
// would produce 1000, which is the failure the exact count below catches.
const planTemplate = `version: v1
pools:
  - name: local
    services:
      - "%s"
defaults:
  pool: local
  target: http://127.0.0.1:%d/
  protocol: http1
  concurrency: "2"
  connections: 4
thresholds:
  - "counter:benchmark.http_5xx == 0"
  - "counter:benchmark.pool_connection_failure == 0"
scenarios:
  - name: e2e
    executor:
      type: constant-rate
      rate: 100
      duration: 5s
    thresholds:
      - "latency_2xx.p99 < 500ms"
`

func rlocation(t *testing.T, path string) string {
	t.Helper()
	p, err := runfiles.Rlocation(path)
	if err != nil {
		t.Fatalf("runfile %s: %v", path, err)
	}
	return p
}

// start runs a binary for the duration of the test, with its output in the test log.
func start(t *testing.T, ctx context.Context, name string, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = testWriter{t, filepath.Base(name)}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
}

type testWriter struct {
	t    *testing.T
	name string
}

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("[%s] %s", w.name, strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// waitForAddress polls a file a binary writes its bound address to.
func waitForAddress(t *testing.T, path string) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(path); err == nil {
			if addr := strings.TrimSpace(string(b)); strings.Contains(addr, ":") {
				return addr
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no address in %s after 30s", path)
	return ""
}

// listenerPort asks the test server's admin interface which port its listener got.
func listenerPort(t *testing.T, adminAddr string) int {
	t.Helper()
	var payload struct {
		ListenerStatuses []struct {
			LocalAddress struct {
				SocketAddress struct {
					PortValue int `json:"port_value"`
				} `json:"socket_address"`
			} `json:"local_address"`
		} `json:"listener_statuses"`
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + adminAddr + "/listeners?format=json")
		if err == nil {
			err = json.NewDecoder(resp.Body).Decode(&payload)
			resp.Body.Close()
			if err == nil && len(payload.ListenerStatuses) > 0 {
				return payload.ListenerStatuses[0].LocalAddress.SocketAddress.PortValue
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("test server listener not reported by %s after 30s", adminAddr)
	return 0
}

func TestSmokePlanAgainstTheEngine(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tmp := t.TempDir()

	configPath := filepath.Join(tmp, "test_server.yaml")
	if err := os.WriteFile(configPath, []byte(testServerConfig), 0o644); err != nil {
		t.Fatal(err)
	}
	adminPath := filepath.Join(tmp, "admin_address")
	// Hot restart is disabled so two test servers on one host (another test, a
	// developer's own) cannot collide on Envoy's shared-memory base id.
	start(t, ctx, rlocation(t, "_main/engine/nighthawk_test_server"),
		"--config-path", configPath, "--admin-address-path", adminPath,
		"--disable-hot-restart", "--concurrency", "1")
	targetPort := listenerPort(t, waitForAddress(t, adminPath))

	servicePath := filepath.Join(tmp, "service_address")
	start(t, ctx, rlocation(t, "_main/engine/nighthawk_service"),
		"--listen", "127.0.0.1:0", "--listener-address-file", servicePath)
	serviceAddr := waitForAddress(t, servicePath)
	if _, _, err := net.SplitHostPort(serviceAddr); err != nil {
		t.Fatalf("service address %q: %v", serviceAddr, err)
	}

	planPath := filepath.Join(tmp, "plan.yaml")
	if err := os.WriteFile(planPath, []byte(fmt.Sprintf(planTemplate, serviceAddr, targetPort)), 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.CommandContext(ctx, rlocation(t, "_main/sortie_/sortie"), "run", planPath)
	out, err := cmd.CombinedOutput()
	t.Logf("sortie run:\n%s", out)
	if err != nil {
		t.Fatalf("sortie run failed: %v", err)
	}
	// Anchored on the count, so that 1500 or 2500 requests cannot satisfy it; the
	// duration is the backend's measured one and may read 5.001s.
	if !regexp.MustCompile(`(?m)^\s+\S+: 500 requests in \S+$`).Match(out) {
		t.Errorf("sortie output lacks the backend line with exactly 500 requests")
	}
	if !strings.Contains(string(out), "PASS  1/1 executions passed") {
		t.Errorf("sortie output lacks the PASS verdict")
	}
}
