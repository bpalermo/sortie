// Package e2e runs a plan through the sortie binary against the engine built in
// this workspace: nighthawk_service as the backend and nighthawk_test_server as
// the target. The unit tests never talk to Nighthawk, so this is the only test
// that can catch sortie generating the wrong load.
package e2e

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
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

// assertHealthy checks the service answers the gRPC health protocol with
// SERVING, which is what a Kubernetes gRPC probe asks it.
func assertHealthy(t *testing.T, ctx context.Context, addr string) {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer conn.Close()
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := grpc_health_v1.NewHealthClient(conn).Check(checkCtx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("health check: %v", err)
	}
	if got := resp.GetStatus(); got != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("health status %v, want SERVING", got)
	}
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
		"--listen", "127.0.0.1:0", "--listener-address-file", servicePath,
		"--max-concurrent-executions", "4")
	serviceAddr := waitForAddress(t, servicePath)
	if _, _, err := net.SplitHostPort(serviceAddr); err != nil {
		t.Fatalf("service address %q: %v", serviceAddr, err)
	}
	assertHealthy(t, ctx, serviceAddr)

	planPath := filepath.Join(tmp, "plan.yaml")
	if err := os.WriteFile(planPath, []byte(fmt.Sprintf(planTemplate, serviceAddr, targetPort)), 0o644); err != nil {
		t.Fatal(err)
	}

	// --progress exercises the engine's interim responses on the way: a 5 s run
	// at 1 s should narrate at least three snapshots on stderr.
	cmd := exec.CommandContext(ctx, rlocation(t, "_main/sortie_/sortie"), "run", "--progress", "1s", planPath)
	out, err := cmd.CombinedOutput()
	t.Logf("sortie run:\n%s", out)
	if err != nil {
		t.Fatalf("sortie run failed: %v", err)
	}
	if n := len(regexp.MustCompile(`(?m)^\s+\S+:\d+  \d+(\.\d+)?s  .*http_2xx \d+`).FindAll(out, -1)); n < 3 {
		t.Errorf("expected at least three progress lines with an http_2xx count, found %d", n)
	}
	// Anchored on the count, so that 1000 requests (the rate forwarded to each
	// worker rather than divided) cannot satisfy it. Not exact: a request still
	// in flight when the clock runs out makes it 499 on a slow runner. The
	// duration is the backend's measured one and may read 5.001s.
	// The line may go on: the report appends a backend's non-zero failure
	// counters, and one pool overflow while the connections are still opening
	// is not unusual on a loaded runner.
	if !regexp.MustCompile(`(?m)^\s+\S+: (49[0-9]|50[0-9]) requests in \S+(  \(.*\))?$`).Match(out) {
		t.Errorf("sortie output lacks the backend line with about 500 requests")
	}
	if !strings.Contains(string(out), "PASS  1/1 executions passed") {
		t.Errorf("sortie output lacks the PASS verdict")
	}
}

// Three weighted targets on one backend, driven at the same time: 100 rps for
// 5 s split 6:3:1 is 300, 150 and 50 requests, each target with its own counters
// and judged on its own. Two workers, so each share has to divide by two.
const weightedPlanTemplate = `version: v1
pools:
  - name: local
    services:
      - "%s"
defaults:
  pool: local
  protocol: http1
  concurrency: "2"
  connections: 2
thresholds:
  - "counter:benchmark.http_5xx == 0"
  - "counter:benchmark.pool_connection_failure == 0"
scenarios:
  - name: mix
    executor:
      type: constant-rate
      rate: 100
      duration: 5s
    targets:
      - {name: a, url: "http://127.0.0.1:%d/a", weight: 6}
      - {name: b, url: "http://127.0.0.1:%d/b", weight: 3}
      - {name: c, url: "http://127.0.0.1:%d/c", weight: 1}
    thresholds:
      - "latency_2xx.p99 < 500ms"
`

func TestWeightedTargetsRunConcurrentlyAgainstTheEngine(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tmp := t.TempDir()

	configPath := filepath.Join(tmp, "test_server.yaml")
	if err := os.WriteFile(configPath, []byte(testServerConfig), 0o644); err != nil {
		t.Fatal(err)
	}
	adminPath := filepath.Join(tmp, "admin_address")
	start(t, ctx, rlocation(t, "_main/engine/nighthawk_test_server"),
		"--config-path", configPath, "--admin-address-path", adminPath,
		"--disable-hot-restart", "--concurrency", "1")
	targetPort := listenerPort(t, waitForAddress(t, adminPath))

	servicePath := filepath.Join(tmp, "service_address")
	start(t, ctx, rlocation(t, "_main/engine/nighthawk_service"),
		"--listen", "127.0.0.1:0", "--listener-address-file", servicePath,
		"--max-concurrent-executions", "4")
	serviceAddr := waitForAddress(t, servicePath)
	assertHealthy(t, ctx, serviceAddr)

	planPath := filepath.Join(tmp, "plan.yaml")
	plan := fmt.Sprintf(weightedPlanTemplate, serviceAddr, targetPort, targetPort, targetPort)
	if err := os.WriteFile(planPath, []byte(plan), 0o644); err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	cmd := exec.CommandContext(ctx, rlocation(t, "_main/sortie_/sortie"), "run", planPath)
	out, err := cmd.CombinedOutput()
	elapsed := time.Since(started)
	t.Logf("sortie run:\n%s", out)
	if err != nil {
		t.Fatalf("sortie run failed: %v", err)
	}
	// Three 5 s executions run together take about 5 s. Two together and then
	// the third would take 10 s, and one after another 15 s, so the bound sits
	// below the first of those: it leaves room for process start-up and the
	// drain, not for a second batch.
	if elapsed > 9*time.Second {
		t.Errorf("the three targets took %s, so they did not run concurrently", elapsed)
	}
	for _, want := range []string{`mix/a`, `mix/b`, `mix/c`} {
		if !strings.Contains(string(out), want) {
			t.Errorf("sortie output lacks the per-target execution %q", want)
		}
	}
	// Per-target request counts: 300, 150 and 50, each within a few in-flight
	// requests at the deadline.
	for _, re := range []string{
		`(?s)mix/a.*?\S+: (29[0-9]|30[0-9]) requests in`,
		`(?s)mix/b.*?\S+: (14[0-9]|15[0-9]) requests in`,
		`(?s)mix/c.*?\S+: (4[5-9]|5[0-5]) requests in`,
	} {
		if !regexp.MustCompile(re).Match(out) {
			t.Errorf("sortie output lacks a backend line matching %q", re)
		}
	}
	if !strings.Contains(string(out), "PASS  3/3 executions passed") {
		t.Errorf("sortie output lacks the PASS verdict")
	}
}

// A plan with live metrics: the statsd sink, to a UDP socket this test owns.
const statsPlanTemplate = `version: v1
pools:
  - name: local
    services:
      - "%s"
stats:
  flush_interval: 1s
  statsd:
    address: "%s"
defaults:
  pool: local
  target: http://127.0.0.1:%d/
  protocol: http1
  concurrency: "1"
  connections: 2
scenarios:
  - name: Live Metrics
    executor:
      type: constant-rate
      rate: 50
      duration: 4s
    thresholds:
      - "counter:benchmark.http_5xx == 0"
`

// awaitStatsdLines waits until every pattern has matched some line the UDP
// reader has collected, and returns the patterns still unmatched when the wait
// runs out. The run ending says the engine has sent its datagrams, not that
// this process's reader goroutine has read them: one still queued in the
// socket would be missed by a single look.
func awaitStatsdLines(mu *sync.Mutex, lines *[]string, patterns map[string]*regexp.Regexp) []string {
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		var missing []string
		for name, re := range patterns {
			seen := false
			for _, l := range *lines {
				if re.MatchString(l) {
					seen = true
					break
				}
			}
			if !seen {
				missing = append(missing, name)
			}
		}
		mu.Unlock()
		if len(missing) == 0 || time.Now().After(deadline) {
			sort.Strings(missing)
			return missing
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestLiveMetricsReachAStatsdSink(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tmp := t.TempDir()

	// The statsd "server": every datagram's lines, collected.
	udp, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { udp.Close() })
	var mu sync.Mutex
	var lines []string
	go func() {
		buf := make([]byte, 65536)
		for {
			n, _, err := udp.ReadFrom(buf)
			if err != nil {
				return
			}
			mu.Lock()
			lines = append(lines, strings.Split(strings.TrimSpace(string(buf[:n])), "\n")...)
			mu.Unlock()
		}
	}()
	configPath := filepath.Join(tmp, "test_server.yaml")
	if err := os.WriteFile(configPath, []byte(testServerConfig), 0o644); err != nil {
		t.Fatal(err)
	}
	adminPath := filepath.Join(tmp, "admin_address")
	start(t, ctx, rlocation(t, "_main/engine/nighthawk_test_server"),
		"--config-path", configPath, "--admin-address-path", adminPath,
		"--disable-hot-restart", "--concurrency", "1")
	targetPort := listenerPort(t, waitForAddress(t, adminPath))

	servicePath := filepath.Join(tmp, "service_address")
	start(t, ctx, rlocation(t, "_main/engine/nighthawk_service"),
		"--listen", "127.0.0.1:0", "--listener-address-file", servicePath)
	serviceAddr := waitForAddress(t, servicePath)
	assertHealthy(t, ctx, serviceAddr)

	planPath := filepath.Join(tmp, "plan.yaml")
	plan := fmt.Sprintf(statsPlanTemplate, serviceAddr, udp.LocalAddr().String(), targetPort)
	if err := os.WriteFile(planPath, []byte(plan), 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.CommandContext(ctx, rlocation(t, "_main/sortie_/sortie"), "run", planPath)
	out, err := cmd.CombinedOutput()
	t.Logf("sortie run:\n%s", out)
	if err != nil {
		t.Fatalf("sortie run failed: %v", err)
	}

	// The scenario's name, sanitized, under the default prefix; a counter the
	// engine keeps in its store, and a latency sample as a statsd timer.
	missing := awaitStatsdLines(&mu, &lines, map[string]*regexp.Regexp{
		"an http_2xx counter": regexp.MustCompile(`^sortie\.live_metrics\..*benchmark\.http_2xx:\d+\|c`),
		"a latency timer":     regexp.MustCompile(`^sortie\.live_metrics\..*latency.*:\d+(\.\d+)?\|ms`),
	})
	mu.Lock()
	t.Logf("statsd received %d lines; a sample:\n%s", len(lines), strings.Join(lines[:min(len(lines), 40)], "\n"))
	mu.Unlock()
	for _, m := range missing {
		t.Errorf("%s under sortie.live_metrics did not reach the statsd socket", m)
	}
}

// Weighted targets with live metrics: three executions at once on one backend,
// each with its own stats sink and its own flush worker inside the engine,
// each emitting under its own prefix. This is the per-target dashboard a soak
// reads, and the case in which several flush workers are alive in one engine.
const weightedStatsPlanTemplate = `version: v1
pools:
  - name: local
    services:
      - "%s"
stats:
  flush_interval: 1s
  statsd:
    address: "%s"
defaults:
  pool: local
  protocol: http1
  concurrency: "1"
  connections: 2
scenarios:
  - name: mix
    executor:
      type: constant-rate
      rate: 60
      duration: 4s
    targets:
      - {name: a, url: "http://127.0.0.1:%d/a", weight: 3}
      - {name: b, url: "http://127.0.0.1:%d/b", weight: 2}
      - {name: c, url: "http://127.0.0.1:%d/c", weight: 1}
    thresholds:
      - "counter:benchmark.http_5xx == 0"
`

func TestWeightedTargetsEachEmitTheirOwnLiveMetrics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tmp := t.TempDir()

	udp, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { udp.Close() })
	var mu sync.Mutex
	var lines []string
	go func() {
		buf := make([]byte, 65536)
		for {
			n, _, err := udp.ReadFrom(buf)
			if err != nil {
				return
			}
			mu.Lock()
			lines = append(lines, strings.Split(strings.TrimSpace(string(buf[:n])), "\n")...)
			mu.Unlock()
		}
	}()

	configPath := filepath.Join(tmp, "test_server.yaml")
	if err := os.WriteFile(configPath, []byte(testServerConfig), 0o644); err != nil {
		t.Fatal(err)
	}
	adminPath := filepath.Join(tmp, "admin_address")
	start(t, ctx, rlocation(t, "_main/engine/nighthawk_test_server"),
		"--config-path", configPath, "--admin-address-path", adminPath,
		"--disable-hot-restart", "--concurrency", "1")
	targetPort := listenerPort(t, waitForAddress(t, adminPath))

	servicePath := filepath.Join(tmp, "service_address")
	start(t, ctx, rlocation(t, "_main/engine/nighthawk_service"),
		"--listen", "127.0.0.1:0", "--listener-address-file", servicePath,
		"--max-concurrent-executions", "4")
	serviceAddr := waitForAddress(t, servicePath)
	assertHealthy(t, ctx, serviceAddr)

	planPath := filepath.Join(tmp, "plan.yaml")
	plan := fmt.Sprintf(weightedStatsPlanTemplate, serviceAddr, udp.LocalAddr().String(), targetPort, targetPort, targetPort)
	if err := os.WriteFile(planPath, []byte(plan), 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.CommandContext(ctx, rlocation(t, "_main/sortie_/sortie"), "run", planPath)
	out, err := cmd.CombinedOutput()
	t.Logf("sortie run:\n%s", out)
	if err != nil {
		t.Fatalf("sortie run failed: %v", err)
	}
	if !strings.Contains(string(out), "PASS  3/3 executions passed") {
		t.Errorf("sortie output lacks the PASS verdict")
	}

	want := map[string]*regexp.Regexp{}
	for _, target := range []string{"a", "b", "c"} {
		want["an http_2xx counter under sortie.mix."+target] = regexp.MustCompile(`^sortie\.mix\.` + target + `\..*benchmark\.http_2xx:\d+\|c`)
	}
	for _, m := range awaitStatsdLines(&mu, &lines, want) {
		t.Errorf("%s did not reach the statsd socket", m)
	}
}

// The test server with the upgrade allowed and the websocket-echo filter in
// front of the test-server one: a WebSocket echo endpoint at any path.
const wsTestServerConfig = `admin:
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
          stat_prefix: ingress_ws
          upgrade_configs:
          - upgrade_type: websocket
          route_config:
            name: local_route
            virtual_hosts:
            - name: service
              domains: ["*"]
          http_filters:
          - name: websocket-echo
            typed_config:
              "@type": type.googleapis.com/nighthawk.server.WebSocketEchoConfiguration
          - name: envoy.filters.http.router
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
              dynamic_stats: false
`

// 100 messages per second for 5 s over 4 connections and 2 workers: exactly
// 500 echoes, with no deferred or lost message.
const wsPlanTemplate = `version: v1
pools:
  - name: local
    services:
      - "%s"
defaults:
  pool: local
  target: http://127.0.0.1:%d/echo
  concurrency: "2"
  body: '{"type":"ping"}'
  websocket:
    streams: 4
thresholds:
  - "counter:benchmark.stream_upgrade_rejected == 0"
  - "counter:benchmark.stream_open_failures == 0"
  - "counter:benchmark.stream_deferred == 0"
  - "counter:benchmark.stream_inflight_lost == 0"
scenarios:
  - name: ws
    executor:
      type: constant-rate
      rate: 100
      duration: 5s
    thresholds:
      - "counter:benchmark.stream_messages_received == 500"
      - "benchmark_stream.message_latency.p99 < 500ms"
`

func TestWebSocketPlanAgainstTheEngine(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tmp := t.TempDir()

	configPath := filepath.Join(tmp, "test_server.yaml")
	if err := os.WriteFile(configPath, []byte(wsTestServerConfig), 0o644); err != nil {
		t.Fatal(err)
	}
	adminPath := filepath.Join(tmp, "admin_address")
	start(t, ctx, rlocation(t, "_main/engine/nighthawk_test_server"),
		"--config-path", configPath, "--admin-address-path", adminPath,
		"--disable-hot-restart", "--concurrency", "1", "--base-id", "3")
	targetPort := listenerPort(t, waitForAddress(t, adminPath))

	servicePath := filepath.Join(tmp, "service_address")
	start(t, ctx, rlocation(t, "_main/engine/nighthawk_service"),
		"--listen", "127.0.0.1:0", "--listener-address-file", servicePath,
		"--max-concurrent-executions", "4")
	serviceAddr := waitForAddress(t, servicePath)
	assertHealthy(t, ctx, serviceAddr)

	planPath := filepath.Join(tmp, "plan.yaml")
	if err := os.WriteFile(planPath, []byte(fmt.Sprintf(wsPlanTemplate, serviceAddr, targetPort)), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, rlocation(t, "_main/sortie_/sortie"), "run", planPath)
	out, err := cmd.CombinedOutput()
	t.Logf("sortie run:\n%s", out)
	if err != nil {
		t.Fatalf("sortie run failed: %v", err)
	}
	if !regexp.MustCompile(`(?m)^\s+\S+: 500 messages sent, 500 echoed in \S+$`).Match(out) {
		t.Errorf("sortie output lacks the backend line with 500 messages sent and echoed")
	}
	if !strings.Contains(string(out), "PASS  1/1 executions passed") {
		t.Errorf("sortie output lacks the PASS verdict")
	}
}

// The test server's configuration with TLS on the listener, requiring a
// client certificate: the mTLS target the tls block exists for.
const mtlsTestServerConfig = `admin:
  address:
    socket_address: { address: 127.0.0.1, port_value: 0 }
static_resources:
  listeners:
  - address:
      socket_address: { address: 127.0.0.1, port_value: 0 }
    filter_chains:
    - transport_socket:
        name: envoy.transport_sockets.tls
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext
          require_client_certificate: true
          common_tls_context:
            tls_certificates:
            - certificate_chain: { filename: %[1]s/server.pem }
              private_key: { filename: %[1]s/server-key.pem }
            validation_context:
              trusted_ca: { filename: %[1]s/ca.pem }
      filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          codec_type: AUTO
          stat_prefix: ingress_https
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

const mtlsPlanTemplate = `version: v1
pools:
  - name: local
    services:
      - "%s"
defaults:
  pool: local
  target: https://127.0.0.1:%d/
  concurrency: "2"
  connections: 4
  tls:
    ca_file: ca.pem
%s
thresholds:
  - "counter:benchmark.http_5xx == 0"
  - "counter:benchmark.pool_connection_failure == 0"
scenarios:
  - name: mtls
    executor:
      type: constant-rate
      rate: 100
      duration: 3s
    thresholds:
      # Not an exact count: the handshakes land the first requests late on a
      # slow runner, and what this test proves is that the pair is accepted.
      - "counter:benchmark.http_2xx >= 250"
`

// writeTestPKI writes a CA, a server certificate for 127.0.0.1 and a client
// certificate, all signed by the CA, into dir.
func writeTestPKI(t *testing.T, dir string) {
	t.Helper()
	newKey := func() *ecdsa.PrivateKey {
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	write := func(name, typ string, der []byte) {
		if err := os.WriteFile(filepath.Join(dir, name), pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	caKey := newKey()
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "sortie e2e CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	write("ca.pem", "CERTIFICATE", caDER)

	leaf := func(name string, serial int64, usage x509.ExtKeyUsage, ips []net.IP) {
		key := newKey()
		template := &x509.Certificate{
			SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name},
			NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour),
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}, IPAddresses: ips,
		}
		der, err := x509.CreateCertificate(rand.Reader, template, caTemplate, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		write(name+".pem", "CERTIFICATE", der)
		write(name+"-key.pem", "EC PRIVATE KEY", keyDER)
	}
	leaf("server", 2, x509.ExtKeyUsageServerAuth, []net.IP{net.ParseIP("127.0.0.1")})
	leaf("client", 3, x509.ExtKeyUsageClientAuth, nil)

	// A second, unrelated CA: trusting it instead must make the target's
	// certificate fail verification.
	otherKey := newKey()
	otherTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(4), Subject: pkix.Name{CommonName: "some other CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	otherDER, err := x509.CreateCertificate(rand.Reader, otherTemplate, otherTemplate, &otherKey.PublicKey, otherKey)
	if err != nil {
		t.Fatal(err)
	}
	write("other-ca.pem", "CERTIFICATE", otherDER)
}

// A tls block with the CA and a client pair passes against a target that
// requires a client certificate; the same plan without the pair does not --
// which is the target enforcing mTLS, not sortie being lenient.
func TestMutualTLSPlanAgainstTheEngine(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tmp := t.TempDir()
	writeTestPKI(t, tmp)

	configPath := filepath.Join(tmp, "test_server.yaml")
	if err := os.WriteFile(configPath, []byte(fmt.Sprintf(mtlsTestServerConfig, tmp)), 0o644); err != nil {
		t.Fatal(err)
	}
	adminPath := filepath.Join(tmp, "admin_address")
	start(t, ctx, rlocation(t, "_main/engine/nighthawk_test_server"),
		"--config-path", configPath, "--admin-address-path", adminPath,
		"--disable-hot-restart", "--concurrency", "1", "--base-id", "2")
	targetPort := listenerPort(t, waitForAddress(t, adminPath))

	servicePath := filepath.Join(tmp, "service_address")
	start(t, ctx, rlocation(t, "_main/engine/nighthawk_service"),
		"--listen", "127.0.0.1:0", "--listener-address-file", servicePath,
		"--max-concurrent-executions", "4")
	serviceAddr := waitForAddress(t, servicePath)
	assertHealthy(t, ctx, serviceAddr)

	withPair := "    cert_file: client.pem\n    key_file: client-key.pem"
	for _, tc := range []struct {
		name  string
		extra string
		pass  bool
	}{
		{"with the client pair", withPair, true},
		{"without the client pair", "", false},
		// ca_file is enforced: the right pair, but trusting a CA that did not
		// sign the target's certificate, fails the handshake on our side.
		{"with the client pair and the wrong CA", withPair + "\n    ca_file: other-ca.pem", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			planPath := filepath.Join(tmp, strings.ReplaceAll(tc.name, " ", "_")+".yaml")
			if err := os.WriteFile(planPath, []byte(fmt.Sprintf(mtlsPlanTemplate, serviceAddr, targetPort, tc.extra)), 0o644); err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(ctx, rlocation(t, "_main/sortie_/sortie"), "run", planPath)
			out, err := cmd.CombinedOutput()
			t.Logf("sortie run:\n%s", out)
			if tc.pass {
				if err != nil {
					t.Fatalf("sortie run failed: %v", err)
				}
				if !strings.Contains(string(out), "PASS  1/1 executions passed") {
					t.Errorf("sortie output lacks the PASS verdict")
				}
				return
			}
			if err == nil {
				t.Fatal("sortie run passed against a target that requires a client certificate")
			}
			if !strings.Contains(string(out), "FAIL") {
				t.Errorf("sortie output lacks a FAIL verdict")
			}
		})
	}
}

// The test server with Envoy's echo network filter on a plain listener.
const tcpTestServerConfig = `admin:
  address:
    socket_address: { address: 127.0.0.1, port_value: 0 }
static_resources:
  listeners:
  - address:
      socket_address: { address: 127.0.0.1, port_value: 0 }
    filter_chains:
    - filters:
      - name: envoy.filters.network.echo
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.echo.v3.Echo
`

// 100 messages per second per worker, 2 workers, 5 s: exactly 1000 messages
// sent -- the per-worker division of the rate -- on 2 connections per worker,
// and all but a message still in flight at the end echoed.
const tcpPlanTemplate = `version: v1
pools:
  - name: local
    services:
      - "%s"
defaults:
  pool: local
  target: tcp://127.0.0.1:%d
  concurrency: "2"
  body: "ping\\n"
  tcp:
    connections: 2
thresholds:
  - "counter:benchmark.tcp_connect_failures == 0"
  - "counter:benchmark.tcp_deferred == 0"
  - "counter:benchmark.tcp_inflight_lost == 0"
  - "counter:benchmark.tcp_echo_mismatch == 0"
scenarios:
  - name: tcp
    executor:
      type: constant-rate
      rate: 200
      duration: 5s
    thresholds:
      - "counter:benchmark.tcp_messages_sent == 1000"
      - "counter:benchmark.tcp_messages_received >= 990"
      - "benchmark_tcp.message_latency.p99 < 500ms"
`

func TestTcpPlanAgainstTheEngine(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tmp := t.TempDir()

	configPath := filepath.Join(tmp, "test_server.yaml")
	if err := os.WriteFile(configPath, []byte(tcpTestServerConfig), 0o644); err != nil {
		t.Fatal(err)
	}
	adminPath := filepath.Join(tmp, "admin_address")
	start(t, ctx, rlocation(t, "_main/engine/nighthawk_test_server"),
		"--config-path", configPath, "--admin-address-path", adminPath,
		"--disable-hot-restart", "--concurrency", "1", "--base-id", "4")
	targetPort := listenerPort(t, waitForAddress(t, adminPath))

	servicePath := filepath.Join(tmp, "service_address")
	start(t, ctx, rlocation(t, "_main/engine/nighthawk_service"),
		"--listen", "127.0.0.1:0", "--listener-address-file", servicePath,
		"--max-concurrent-executions", "4")
	serviceAddr := waitForAddress(t, servicePath)
	assertHealthy(t, ctx, serviceAddr)

	planPath := filepath.Join(tmp, "plan.yaml")
	if err := os.WriteFile(planPath, []byte(fmt.Sprintf(tcpPlanTemplate, serviceAddr, targetPort)), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, rlocation(t, "_main/sortie_/sortie"), "run", planPath)
	out, err := cmd.CombinedOutput()
	t.Logf("sortie run:\n%s", out)
	if err != nil {
		t.Fatalf("sortie run failed: %v", err)
	}
	if !regexp.MustCompile(`(?m)^\s+\S+: 1000 messages sent, (99[0-9]|1000) echoed in \S+$`).Match(out) {
		t.Errorf("sortie output lacks the backend line with 1000 messages sent and echoed")
	}
	if !strings.Contains(string(out), "PASS  1/1 executions passed") {
		t.Errorf("sortie output lacks the PASS verdict")
	}
}

// The test server with the udp-echo listener filter on a UDP listener.
const udpTestServerConfig = `admin:
  address:
    socket_address: { address: 127.0.0.1, port_value: 0 }
static_resources:
  listeners:
  - address:
      socket_address: { protocol: UDP, address: 127.0.0.1, port_value: 0 }
    listener_filters:
    - name: udp-echo
      typed_config:
        "@type": type.googleapis.com/nighthawk.server.UdpEchoConfiguration
`

// 100 datagrams per second per worker, 2 workers, 5 s: 1000 datagrams, all
// echoed on the loopback.
const udpPlanTemplate = `version: v1
pools:
  - name: local
    services:
      - "%s"
defaults:
  pool: local
  target: udp://127.0.0.1:%d
  concurrency: "2"
  body: "ping"
thresholds:
  - "counter:benchmark.udp_send_errors == 0"
  - "counter:benchmark.udp_deferred == 0"
  - "counter:benchmark.udp_lost == 0"
  - "counter:benchmark.udp_unexpected == 0"
scenarios:
  - name: udp
    executor:
      type: constant-rate
      rate: 200
      duration: 5s
    thresholds:
      - "counter:benchmark.udp_datagrams_sent == 1000"
      - "counter:benchmark.udp_datagrams_received >= 990"
      - "benchmark_udp.message_latency.p99 < 500ms"
`

func TestUdpPlanAgainstTheEngine(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tmp := t.TempDir()

	configPath := filepath.Join(tmp, "test_server.yaml")
	if err := os.WriteFile(configPath, []byte(udpTestServerConfig), 0o644); err != nil {
		t.Fatal(err)
	}
	adminPath := filepath.Join(tmp, "admin_address")
	start(t, ctx, rlocation(t, "_main/engine/nighthawk_test_server"),
		"--config-path", configPath, "--admin-address-path", adminPath,
		"--disable-hot-restart", "--concurrency", "1", "--base-id", "5")
	targetPort := listenerPort(t, waitForAddress(t, adminPath))

	servicePath := filepath.Join(tmp, "service_address")
	start(t, ctx, rlocation(t, "_main/engine/nighthawk_service"),
		"--listen", "127.0.0.1:0", "--listener-address-file", servicePath,
		"--max-concurrent-executions", "4")
	serviceAddr := waitForAddress(t, servicePath)
	assertHealthy(t, ctx, serviceAddr)

	planPath := filepath.Join(tmp, "plan.yaml")
	if err := os.WriteFile(planPath, []byte(fmt.Sprintf(udpPlanTemplate, serviceAddr, targetPort)), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, rlocation(t, "_main/sortie_/sortie"), "run", planPath)
	out, err := cmd.CombinedOutput()
	t.Logf("sortie run:\n%s", out)
	if err != nil {
		t.Fatalf("sortie run failed: %v", err)
	}
	if !regexp.MustCompile(`(?m)^\s+\S+: 1000 datagrams sent, (99[0-9]|1000) echoed, 0 lost in \S+$`).Match(out) {
		t.Errorf("sortie output lacks the backend line with 1000 datagrams sent and echoed")
	}
	if !strings.Contains(string(out), "PASS  1/1 executions passed") {
		t.Errorf("sortie output lacks the PASS verdict")
	}
}
