package e2e

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// One target for twenty seconds: the execution that holds one of the engine's
// two slots while the weighted scenario below asks for three.
const capHolderPlanTemplate = `version: v1
pools:
  - name: local
    services:
      - "%s"
defaults:
  pool: local
  target: http://127.0.0.1:%d/
  protocol: http1
  concurrency: "1"
  connections: 2
thresholds:
  - "counter:benchmark.http_5xx == 0"
scenarios:
  - name: holder
    executor:
      type: constant-rate
      rate: 50
      duration: 20s
`

// Three targets for a minute. Long, so that a run which ends in seconds can
// only have been stopped.
const capWeightedPlanTemplate = `version: v1
pools:
  - name: local
    services:
      - "%s"
defaults:
  pool: local
  protocol: http1
  concurrency: "1"
  connections: 2
scenarios:
  - name: mix
    executor:
      type: constant-rate
      rate: 30
      duration: 60s
    targets:
      - {name: a, url: "http://127.0.0.1:%d/a", weight: 1}
      - {name: b, url: "http://127.0.0.1:%d/b", weight: 1}
      - {name: c, url: "http://127.0.0.1:%d/c", weight: 1}
`

// A second of load on one target: fits in exactly one free slot.
const capProbePlanTemplate = `version: v1
pools:
  - name: local
    services:
      - "%s"
defaults:
  pool: local
  target: http://127.0.0.1:%d/
  protocol: http1
  concurrency: "1"
  connections: 2
scenarios:
  - name: probe
    executor:
      type: constant-rate
      rate: 20
      duration: 1s
`

// An engine that allows two executions has one running when a three-target
// scenario arrives. One target gets the last slot and two are refused. The
// scenario fails at once with an error naming the cap; the target that did
// start is stopped rather than left to run for its minute; and the execution
// that was already there is not touched.
func TestAWeightedScenarioOverTheExecutionCapStopsAtOnce(t *testing.T) {
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
		"--max-concurrent-executions", "2")
	serviceAddr := waitForAddress(t, servicePath)
	assertHealthy(t, ctx, serviceAddr)

	write := func(name, body string) string {
		path := filepath.Join(tmp, name)
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return path
	}
	holderPlan := write("holder.yaml", fmt.Sprintf(capHolderPlanTemplate, serviceAddr, targetPort))
	weightedPlan := write("weighted.yaml", fmt.Sprintf(capWeightedPlanTemplate, serviceAddr, targetPort, targetPort, targetPort))
	probePlan := write("probe.yaml", fmt.Sprintf(capProbePlanTemplate, serviceAddr, targetPort))
	sortie := rlocation(t, "_main/sortie_/sortie")

	// The holder, in the background. Its first progress line is the engine
	// saying the execution is under way, so its slot is taken from here on.
	holder := exec.CommandContext(ctx, sortie, "run", "--progress", "1s", holderPlan)
	var holderOut bytes.Buffer
	holder.Stdout = &holderOut
	holderErr, err := holder.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	holderStarted := time.Now()
	if err := holder.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = holder.Process.Kill() })
	running := make(chan struct{})
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		progress := regexp.MustCompile(`^\s+holder  \S+:\d+  `)
		seen := false
		scanner := bufio.NewScanner(holderErr)
		for scanner.Scan() {
			if !seen && progress.MatchString(scanner.Text()) {
				seen = true
				close(running)
			}
		}
	}()
	select {
	case <-running:
	case <-time.After(15 * time.Second):
		t.Fatal("the holder reported no progress within 15s")
	}

	started := time.Now()
	out, err := exec.CommandContext(ctx, sortie, "run", weightedPlan).CombinedOutput()
	elapsed := time.Since(started)
	t.Logf("sortie run (weighted, over the cap):\n%s", out)
	if err == nil {
		t.Fatal("the weighted scenario succeeded on an engine with one free slot")
	}
	// A minute was planned. Stopping takes the engine's wind-down of the one
	// target that started, which is about a second.
	if elapsed > 20*time.Second {
		t.Errorf("the refused scenario took %s to fail; its siblings were left running", elapsed)
	}
	for _, want := range []string{
		"cap of 2 concurrent executions",
		"starts 3 at once on every backend",
		"--max-concurrent-executions",
		"engine.maxConcurrentExecutions",
		"FAIL  0/3 executions passed",
	} {
		if !strings.Contains(string(out), want) {
			t.Errorf("the report does not say %q", want)
		}
	}
	// Nothing was judged, so no backend line with a request count: a target
	// that ran to the end would have one.
	if regexp.MustCompile(`requests in`).Match(out) {
		t.Error("the report carries results for a scenario that was stopped at the start")
	}

	// The engine is back to the holder alone: one slot is free, which a run
	// needing exactly one shows. Were the target that got the last slot still
	// running, this would be refused too.
	out, err = exec.CommandContext(ctx, sortie, "run", probePlan).CombinedOutput()
	t.Logf("sortie run (probe):\n%s", out)
	if err != nil {
		t.Fatalf("a one-target run did not fit after the refused scenario: %v", err)
	}
	if !strings.Contains(string(out), "PASS  1/1 executions passed") {
		t.Error("the probe did not pass")
	}

	// And the holder went its whole distance with everything it was to send:
	// 50 rps for 20 s is 1000 requests.
	<-drained
	if err := holder.Wait(); err != nil {
		t.Errorf("the holder failed: %v", err)
	}
	t.Logf("sortie run (holder):\n%s", holderOut.String())
	if took := time.Since(holderStarted); took < 19*time.Second {
		t.Errorf("the holder ended after %s; it was to last 20s", took)
	}
	if !regexp.MustCompile(`(?m)^\s+\S+: (9[5-9][0-9]|100[0-9]) requests in \S+(  \(.*\))?$`).MatchString(holderOut.String()) {
		t.Error("the holder did not send about 1000 requests")
	}
	if !strings.Contains(holderOut.String(), "PASS  1/1 executions passed") {
		t.Error("the holder did not pass")
	}
}
