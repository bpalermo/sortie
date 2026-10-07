package compile

import (
	"strings"
	"testing"
	"time"

	metricsv3 "github.com/envoyproxy/go-control-plane/envoy/config/metrics/v3"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	client "github.com/bpalermo/sortie/engine/api/client"
	statssink "github.com/bpalermo/sortie/engine/api/stats_sink"
	"github.com/bpalermo/sortie/internal/plan"
)

func TestStatsPrefix(t *testing.T) {
	for _, tc := range []struct{ prefix, label, want string }{
		{"", "smoke", "sortie.smoke"},
		{"", "smoke/stage-2", "sortie.smoke.stage_2"},
		{"soak", "smoke", "soak.smoke"},
		{"soak.eu", "smoke", "soak.eu.smoke"},
		// What the multi-target expansion will label executions.
		{"", "checkout/eu-west", "sortie.checkout.eu_west"},
		// Dots in a label would add levels of their own; they do not.
		{"", "a.b.c", "sortie.a_b_c"},
		{"", "Mixed Case & Spaces", "sortie.mixed_case_spaces"},
		{"", "--trimmed--", "sortie.trimmed"},
		{"", "///", "sortie"},
		{"", "", "sortie"},
		{"", "under_score", "sortie.under_score"},
	} {
		if got := StatsPrefix(tc.prefix, tc.label); got != tc.want {
			t.Errorf("StatsPrefix(%q, %q) = %q, want %q", tc.prefix, tc.label, got, tc.want)
		}
	}
}

// unwrap returns the Envoy sink an engine adapter sink hosts.
func unwrap(t *testing.T, sink *metricsv3.StatsSink) *metricsv3.StatsSink {
	t.Helper()
	if sink.GetName() != EnvoyStatsSinkAdapter {
		t.Fatalf("sink name = %q, want %q", sink.GetName(), EnvoyStatsSinkAdapter)
	}
	var cfg statssink.EnvoyStatsSinkAdapterConfig
	if err := sink.GetTypedConfig().UnmarshalTo(&cfg); err != nil {
		t.Fatalf("unpacking the adapter config: %v", err)
	}
	return cfg.GetSink()
}

func statsScenario(st *plan.Stats) *plan.Scenario {
	s := scenario(&plan.Executor{Type: plan.ConstantRate, Rate: 10, Duration: dur(time.Second)})
	s.Name = "smoke"
	s.Stats = st
	return s
}

func TestStatsStatsdBecomesAnAdaptedUdpSink(t *testing.T) {
	execs, err := Expand(statsScenario(&plan.Stats{
		Statsd: &plan.Statsd{Address: "10.0.0.5:8125"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	sinks := execs[0].Options.GetStatsSinks()
	if len(sinks) != 1 {
		t.Fatalf("got %d sinks, want 1", len(sinks))
	}
	inner := unwrap(t, sinks[0])
	if inner.GetName() != StatsdSink {
		t.Fatalf("inner sink = %q, want %q", inner.GetName(), StatsdSink)
	}
	var sd metricsv3.StatsdSink
	if err := inner.GetTypedConfig().UnmarshalTo(&sd); err != nil {
		t.Fatal(err)
	}
	sa := sd.GetAddress().GetSocketAddress()
	if sa.GetAddress() != "10.0.0.5" || sa.GetPortValue() != 8125 {
		t.Errorf("address = %v, want 10.0.0.5:8125", sa)
	}
	if sd.GetPrefix() != "sortie.smoke" {
		t.Errorf("prefix = %q, want sortie.smoke", sd.GetPrefix())
	}
	if !sd.GetScaleHistogramUnitsToMilliseconds() {
		t.Error("the engine's latency samples are microseconds; the sink must scale them")
	}
	// No flush interval set: the engine's default stands.
	if execs[0].Options.GetOneofStatsFlushInterval() != nil {
		t.Errorf("flush interval = %v, want unset", execs[0].Options.GetOneofStatsFlushInterval())
	}
}

func TestStatsIPv6StatsdAddress(t *testing.T) {
	execs, err := Expand(statsScenario(&plan.Stats{
		Statsd: &plan.Statsd{Address: "[fd00::5]:8125"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	var sd metricsv3.StatsdSink
	if err := unwrap(t, execs[0].Options.GetStatsSinks()[0]).GetTypedConfig().UnmarshalTo(&sd); err != nil {
		t.Fatal(err)
	}
	if got := sd.GetAddress().GetSocketAddress().GetAddress(); got != "fd00::5" {
		t.Errorf("address = %q, want the literal without brackets", got)
	}
}

func TestStatsPassthroughSinks(t *testing.T) {
	raw, err := anypb.New(&metricsv3.StatsdSink{Prefix: "mine"})
	if err != nil {
		t.Fatal(err)
	}
	own, err := anypb.New(&structpb.Struct{})
	if err != nil {
		t.Fatal(err)
	}
	execs, err := Expand(statsScenario(&plan.Stats{
		Sinks: []*metricsv3.StatsSink{
			{Name: "envoy.stat_sinks.dog_statsd", ConfigType: &metricsv3.StatsSink_TypedConfig{TypedConfig: raw}},
			{Name: "nighthawk.fake_stats_sink", ConfigType: &metricsv3.StatsSink_TypedConfig{TypedConfig: own}},
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	sinks := execs[0].Options.GetStatsSinks()
	if len(sinks) != 2 {
		t.Fatalf("got %d sinks, want 2", len(sinks))
	}
	inner := unwrap(t, sinks[0])
	if inner.GetName() != "envoy.stat_sinks.dog_statsd" {
		t.Errorf("inner sink = %q", inner.GetName())
	}
	var sd metricsv3.StatsdSink
	if err := inner.GetTypedConfig().UnmarshalTo(&sd); err != nil {
		t.Fatal(err)
	}
	if sd.GetPrefix() != "mine" {
		t.Errorf("a passthrough sink's prefix = %q, want it as written", sd.GetPrefix())
	}
	// Every entry is an Envoy sink and is hosted through the adapter, whatever
	// its name starts with: a name is the user's to choose, and Envoy finds
	// the factory from the config.
	if got := unwrap(t, sinks[1]).GetName(); got != "nighthawk.fake_stats_sink" {
		t.Errorf("the second sink should be wrapped under its own name, got %q", got)
	}
	// An unnamed sink has no factory to resolve to.
	_, err = Expand(statsScenario(&plan.Stats{Sinks: []*metricsv3.StatsSink{{}}}))
	if err == nil || !strings.Contains(err.Error(), "needs a name") {
		t.Errorf("err = %v, want a complaint about the missing name", err)
	}
}

// A staircase stage gets its own prefix level, so stages are distinguishable
// on a dashboard, and each execution's sinks are its own.
func TestStatsStaircaseStagesHaveTheirOwnPrefix(t *testing.T) {
	s := statsScenario(&plan.Stats{Statsd: &plan.Statsd{Address: "10.0.0.5:8125"}})
	s.Executor = &plan.Executor{Type: plan.Staircase, Stages: []*plan.Stage{
		{Rate: 10, Duration: dur(time.Second)},
		{Rate: 20, Duration: dur(time.Second)},
	}}
	execs, err := Expand(s)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"sortie.smoke.stage_1", "sortie.smoke.stage_2"} {
		var sd metricsv3.StatsdSink
		if err := unwrap(t, execs[i].Options.GetStatsSinks()[0]).GetTypedConfig().UnmarshalTo(&sd); err != nil {
			t.Fatal(err)
		}
		if sd.GetPrefix() != want {
			t.Errorf("stage %d prefix = %q, want %q", i+1, sd.GetPrefix(), want)
		}
	}
}

// A template's sinks are kept and the block's added after them; the block's
// flush interval replaces the template's, whichever form the template used.
func TestStatsAddsToATemplatesSinks(t *testing.T) {
	s := statsScenario(&plan.Stats{
		Statsd:        &plan.Statsd{Address: "10.0.0.5:8125"},
		FlushInterval: durationpb.New(2 * time.Second),
	})
	s.NighthawkTemplate = &client.CommandLineOptions{
		StatsSinks: []*metricsv3.StatsSink{{Name: "nighthawk.fake_stats_sink"}},
		OneofStatsFlushInterval: &client.CommandLineOptions_StatsFlushInterval{
			StatsFlushInterval: wrapperspb.UInt32(30),
		},
	}
	execs, err := Expand(s)
	if err != nil {
		t.Fatal(err)
	}
	o := execs[0].Options
	sinks := o.GetStatsSinks()
	if len(sinks) != 2 || sinks[0].GetName() != "nighthawk.fake_stats_sink" || sinks[1].GetName() != EnvoyStatsSinkAdapter {
		t.Errorf("sinks = %v, want the template's then the block's", sinks)
	}
	if got := o.GetStatsFlushIntervalDuration().AsDuration(); got != 2*time.Second {
		t.Errorf("flush interval = %s, want the block's 2s", got)
	}
	if o.GetStatsFlushInterval() != nil {
		t.Error("the template's uint32 flush interval should be gone")
	}
	// The template is not mutated: a second expansion gets the same result.
	if n := len(s.GetNighthawkTemplate().GetStatsSinks()); n != 1 {
		t.Errorf("template now has %d sinks, want 1", n)
	}
}

// Without a stats block nothing is added and a template's sinks are left as
// they are.
func TestNoStatsBlockLeavesSinksAlone(t *testing.T) {
	s := statsScenario(nil)
	s.NighthawkTemplate = &client.CommandLineOptions{
		StatsSinks: []*metricsv3.StatsSink{{Name: "nighthawk.fake_stats_sink"}},
	}
	execs, err := Expand(s)
	if err != nil {
		t.Fatal(err)
	}
	if sinks := execs[0].Options.GetStatsSinks(); len(sinks) != 1 || sinks[0].GetName() != "nighthawk.fake_stats_sink" {
		t.Errorf("sinks = %v, want the template's only", sinks)
	}
	if execs, err = Expand(statsScenario(nil)); err != nil {
		t.Fatal(err)
	} else if len(execs[0].Options.GetStatsSinks()) != 0 {
		t.Error("a scenario without stats should configure no sinks")
	}
}

func TestStatsRejectsABadAddress(t *testing.T) {
	for name, st := range map[string]*plan.Stats{
		"statsd without port": {Statsd: &plan.Statsd{Address: "10.0.0.5"}},
		"statsd port zero":    {Statsd: &plan.Statsd{Address: "10.0.0.5:0"}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Expand(statsScenario(st)); err == nil || !strings.Contains(err.Error(), "address") {
				t.Errorf("err = %v, want an address error", err)
			}
		})
	}
}

// statsdPrefix is the prefix of the statsd sink at index i of an options'
// stats sinks.
func statsdPrefix(t *testing.T, o *client.CommandLineOptions, i int) string {
	t.Helper()
	var sd metricsv3.StatsdSink
	if err := unwrap(t, o.GetStatsSinks()[i]).GetTypedConfig().UnmarshalTo(&sd); err != nil {
		t.Fatal(err)
	}
	return sd.GetPrefix()
}

// Every backend of a pool emits under a prefix that names it. Without that a
// statsd server holds one series fed by all of them, which reads as a single
// backend's worth; with it the series sum, over backends, to the report.
func TestForPoolGivesEachBackendItsOwnStatsPrefix(t *testing.T) {
	s := statsScenario(&plan.Stats{Statsd: &plan.Statsd{Address: "10.0.0.5:8125"}})
	s.NighthawkTemplate = &client.CommandLineOptions{
		StatsSinks: []*metricsv3.StatsSink{{Name: "nighthawk.fake_stats_sink"}},
	}
	execs, err := Expand(s)
	if err != nil {
		t.Fatal(err)
	}
	if got := statsdPrefix(t, execs[0].Options, 1); got != "sortie.smoke" {
		t.Errorf("before the pool is known the prefix = %q, want sortie.smoke", got)
	}

	_, opts, err := ForPool(execs[0], &plan.Pool{Name: "nodes", Services: []string{"10.0.0.11:8443", "10.0.0.12:8443"}})
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"sortie.smoke.10_0_0_11", "sortie.smoke.10_0_0_12"} {
		sinks := opts[i].GetStatsSinks()
		if len(sinks) != 2 || sinks[0].GetName() != "nighthawk.fake_stats_sink" {
			t.Fatalf("backend %d: sinks = %v, want the template's sink kept, then the statsd one", i, sinks)
		}
		if got := statsdPrefix(t, opts[i], 1); got != want {
			t.Errorf("backend %d: prefix = %q, want %q", i, got, want)
		}
	}
	// The execution's own options are not touched by dispatching it.
	if got := statsdPrefix(t, execs[0].Options, 1); got != "sortie.smoke" {
		t.Errorf("ForPool changed the execution's options: prefix = %q", got)
	}

	// Engines sharing a host are told apart by port; IPv6 is sanitized.
	_, opts, err = ForPool(execs[0], &plan.Pool{Name: "local", Services: []string{"127.0.0.1:8443", "127.0.0.1:8444", "[2001:db8::1]:8443"}})
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"sortie.smoke.127_0_0_1_8443", "sortie.smoke.127_0_0_1_8444", "sortie.smoke.2001_db8_1"} {
		if got := statsdPrefix(t, opts[i], 1); got != want {
			t.Errorf("backend %d: prefix = %q, want %q", i, got, want)
		}
	}
}

// A scenario without a stats block is dispatched as before.
func TestForPoolLeavesOptionsWithoutStatsAlone(t *testing.T) {
	s := statsScenario(nil)
	s.NighthawkTemplate = &client.CommandLineOptions{
		StatsSinks: []*metricsv3.StatsSink{{Name: "nighthawk.fake_stats_sink"}},
	}
	execs, err := Expand(s)
	if err != nil {
		t.Fatal(err)
	}
	_, opts, err := ForPool(execs[0], &plan.Pool{Name: "nodes", Services: []string{"10.0.0.11:8443", "10.0.0.12:8443"}})
	if err != nil {
		t.Fatal(err)
	}
	for i := range opts {
		if sinks := opts[i].GetStatsSinks(); len(sinks) != 1 || sinks[0].GetName() != "nighthawk.fake_stats_sink" {
			t.Errorf("backend %d: sinks = %v", i, sinks)
		}
	}
}

// Distinct backends never share a segment, even when sanitizing makes their
// names alike: the two IPv6 addresses below both reduce to 2001_db8_1, with
// the same port, so only their position in the pool is left to tell them
// apart.
func TestBackendSegmentsAreUniqueAfterSanitizing(t *testing.T) {
	for name, addrs := range map[string][]string{
		"distinct hosts":           {"10.0.0.11:8443", "10.0.0.12:8443"},
		"one host, two ports":      {"127.0.0.1:8443", "127.0.0.1:8444"},
		"ipv6 alike, ports differ": {"[2001:db8::1]:8443", "[2001:db8:1::]:8444"},
		"ipv6 alike, same port":    {"[2001:db8::1]:8443", "[2001:db8:1::]:8443"},
		"three alike and one not":  {"[2001:db8::1]:8443", "[2001:db8:1::]:8443", "[2001:db8::1]:8443", "10.0.0.1:8443"},
		// The third is already named what the first's suffixed name would be.
		"a suffix that is taken": {"a-b:1", "a.b:1", "a-b-1-b0:2"},
	} {
		got := backendSegments(addrs)
		if len(got) != len(addrs) {
			t.Fatalf("%s: %d segments for %d addresses", name, len(got), len(addrs))
		}
		seen := map[string]bool{}
		for i, seg := range got {
			if seg == "" || strings.ContainsAny(seg, ".:[] ") {
				t.Errorf("%s: segment %d = %q is not a clean prefix component", name, i, seg)
			}
			if seen[seg] {
				t.Errorf("%s: segment %q is used twice in %v", name, seg, got)
			}
			seen[seg] = true
		}
	}
	if got := backendSegments([]string{"10.0.0.11:8443", "10.0.0.12:8443"}); got[0] != "10_0_0_11" || got[1] != "10_0_0_12" {
		t.Errorf("distinct hosts should stay bare hosts, got %v", got)
	}
	if got := backendSegments([]string{"[2001:db8::1]:8443", "[2001:db8:1::]:8443"}); got[0] != "2001_db8_1_8443_b0" || got[1] != "2001_db8_1_8443_b1" {
		t.Errorf("alike after sanitizing with the same port: got %v", got)
	}
}
