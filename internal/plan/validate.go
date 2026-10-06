package plan

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/bpalermo/sortie/internal/threshold"
)

// validateBeyondSchema covers what the proto's own constraints cannot.
//
// protovalidate evaluates one message at a time, so a rule that spans messages
// -- a scenario naming a pool declared elsewhere in the file -- has nowhere to
// live in the schema. Threshold expressions are a second case: they are free
// text whose grammar belongs to internal/threshold, and catching a typo at load
// time rather than after a run has finished is the whole point of validating.
func validateBeyondSchema(p *Plan) error {
	pools := make(map[string]struct{}, len(p.GetPools()))
	for _, pool := range p.GetPools() {
		pools[pool.GetName()] = struct{}{}
		// Checked here as the resolver will check it, so `validate` refuses
		// what `run` would only discover when it starts.
		if pool.GetDns() != "" {
			if _, _, err := SplitDns(pool.GetDns()); err != nil {
				return fmt.Errorf("pool %q: dns %q: %w (an IPv6 literal needs brackets)",
					pool.GetName(), pool.GetDns(), err)
			}
		}
	}

	for i, expr := range p.GetThresholds() {
		if _, err := threshold.Parse(expr); err != nil {
			return fmt.Errorf("thresholds[%d]: %w", i, err)
		}
	}

	// The defaults block is a Scenario too, and applyDefaults copies both of
	// these into every scenario that sets neither.
	if d := p.GetDefaults(); d.GetBody() != "" && d.GetBodyFile() != "" {
		return fmt.Errorf("defaults: body and body_file are mutually exclusive")
	}
	if err := validateStats(p.GetStats()); err != nil {
		return fmt.Errorf("stats: %w", err)
	}
	if err := validateStatsPrefixes(p); err != nil {
		return err
	}
	if err := validateStats(p.GetDefaults().GetStats()); err != nil {
		return fmt.Errorf("defaults: stats: %w", err)
	}

	// defaults is a Scenario too: its target list is copied into every
	// scenario that names no target of its own, so it is held to the same rule.
	if err := validateTargets(p.GetDefaults()); err != nil {
		return fmt.Errorf("defaults: %w", err)
	}

	for _, s := range p.GetScenarios() {
		if s.GetName() == "" {
			return fmt.Errorf("every scenario needs a name")
		}
		// Resolve against the effective value: defaults may supply the pool.
		pool := s.GetPool()
		if pool == "" {
			pool = p.GetDefaults().GetPool()
		}
		if pool == "" {
			return fmt.Errorf("scenario %q: pool is required (set it on the scenario or in defaults)",
				s.GetName())
		}
		if _, ok := pools[pool]; !ok {
			return fmt.Errorf("scenario %q: pool %q is not declared", s.GetName(), pool)
		}
		if s.GetTarget() == "" && len(s.GetTargets()) == 0 &&
			p.GetDefaults().GetTarget() == "" && len(p.GetDefaults().GetTargets()) == 0 {
			return fmt.Errorf("scenario %q: target is required (set it on the scenario or in defaults)",
				s.GetName())
		}
		if err := validateTargets(s); err != nil {
			return fmt.Errorf("scenario %q: %w", s.GetName(), err)
		}
		if s.GetExecutor() == nil && p.GetDefaults().GetExecutor() == nil {
			return fmt.Errorf("scenario %q: executor is required (set it on the scenario or in defaults)",
				s.GetName())
		}
		for i, expr := range s.GetThresholds() {
			if _, err := threshold.Parse(expr); err != nil {
				return fmt.Errorf("scenario %q: thresholds[%d]: %w", s.GetName(), i, err)
			}
		}
		// The mode checks read the target: a weighted scenario is checked
		// once per target, as the scenario each target will run as.
		subject := s
		if s.GetTarget() == "" && len(s.GetTargets()) == 0 && len(p.GetDefaults().GetTargets()) > 0 {
			// Defaults are applied after validation; judge the scenario with
			// the target list it is about to inherit.
			subject = proto.Clone(s).(*Scenario)
			subject.Targets = p.GetDefaults().GetTargets()
		}
		for _, variant := range Variants(subject) {
			if err := validateModes(p, variant); err != nil {
				return fmt.Errorf("scenario %q: %w", variant.GetName(), err)
			}
		}
		if err := validateStats(s.GetStats()); err != nil {
			return fmt.Errorf("scenario %q: stats: %w", s.GetName(), err)
		}
	}
	return nil
}

// Variants is the scenario itself, or -- for one with weighted targets -- the
// per-target scenarios it runs as (ForTarget), named `<scenario>/<target>`.
func Variants(s *Scenario) []*Scenario {
	if len(s.GetTargets()) == 0 {
		return []*Scenario{s}
	}
	out := make([]*Scenario, 0, len(s.GetTargets()))
	for _, t := range s.GetTargets() {
		v := ForTarget(s, t)
		v.Name = s.GetName() + "/" + t.GetName()
		out = append(out, v)
	}
	return out
}

func validateModes(p *Plan, s *Scenario) error {
	if err := validateGrpc(p, s); err != nil {
		return err
	}
	if err := validateWebSocket(p, s); err != nil {
		return err
	}
	if err := validateTls(p, s); err != nil {
		return err
	}
	if err := validateTcp(p, s); err != nil {
		return err
	}
	return validateUdp(p, s)
}

// validateTls checks a tls block against the effective target: it means
// nothing without https, and a client certificate comes with its key.
func validateTls(p *Plan, s *Scenario) error {
	t := s.GetTls()
	if t == nil {
		t = p.GetDefaults().GetTls()
	}
	if t == nil {
		return nil
	}
	target := s.GetTarget()
	if target == "" {
		target = p.GetDefaults().GetTarget()
	}
	if !strings.HasPrefix(target, "https://") && !strings.HasPrefix(target, "tcps://") {
		return fmt.Errorf("tls needs an https or tcps target (got %q)", target)
	}
	// cert_file and key_file going together is the schema's rule (a CEL constraint on Tls).
	return nil
}

// defaultBidiStreams is the engine's --streams default, which the loader has
// to validate against when a plan leaves streams unset; the same for both
// kinds of stream.
const defaultBidiStreams = 20

// IsUdpTarget reports whether a target URL selects UDP load.
func IsUdpTarget(target string) bool {
	return strings.HasPrefix(target, "udp://")
}

// validateUdp checks a UDP scenario: the target decides the mode, the udp
// block only tunes it, nothing HTTP- or TCP-shaped goes with it, and the
// datagram must have content to be matched by.
func validateUdp(p *Plan, s *Scenario) error {
	d := p.GetDefaults()
	target := s.GetTarget()
	if target == "" {
		target = d.GetTarget()
	}
	udp := s.GetUdp()
	if udp == nil {
		udp = d.GetUdp()
	}
	if !IsUdpTarget(target) {
		if udp != nil {
			return fmt.Errorf("udp applies to a udp:// target (got %q)", target)
		}
		return nil
	}
	if _, port, err := splitTargetHostPort(target); err != nil || port == "" {
		return fmt.Errorf("a udp target needs an explicit port (got %q)", target)
	}
	effective := func(a, b string) string {
		if a != "" {
			return a
		}
		return b
	}
	if m := effective(s.GetMethod(), d.GetMethod()); m != "" {
		return fmt.Errorf("method %q has no meaning with a udp target: the body is the datagram", m)
	}
	if len(s.GetHeaders()) > 0 || len(d.GetHeaders()) > 0 {
		return fmt.Errorf("headers have no meaning with a udp target: the body is the datagram")
	}
	if pr := effective(s.GetProtocol(), d.GetProtocol()); pr != "" {
		return fmt.Errorf("protocol %q has no meaning with a udp target", pr)
	}
	if s.Connections != nil || (d != nil && d.Connections != nil) {
		return fmt.Errorf("connections has no meaning with a udp target: one socket per worker")
	}
	if s.GetGrpc() != nil || d.GetGrpc() != nil || s.GetWebsocket() != nil || d.GetWebsocket() != nil ||
		s.GetTcp() != nil || d.GetTcp() != nil {
		return fmt.Errorf("grpc, websocket and tcp cannot go with a udp target")
	}
	if effective(s.GetBody(), d.GetBody()) == "" && effective(s.GetBodyFile(), d.GetBodyFile()) == "" {
		return fmt.Errorf("a udp target needs a body or body_file: the datagram is what gets echoed and matched")
	}
	return nil
}

// IsTcpTarget reports whether a target URL selects raw TCP load.
func IsTcpTarget(target string) bool {
	return strings.HasPrefix(target, "tcp://") || strings.HasPrefix(target, "tcps://")
}

// validateTcp checks a raw TCP scenario: the target decides the mode, the tcp
// block only tunes it, and nothing HTTP-shaped goes with it.
func validateTcp(p *Plan, s *Scenario) error {
	d := p.GetDefaults()
	target := s.GetTarget()
	if target == "" {
		target = d.GetTarget()
	}
	tcp := s.GetTcp()
	if tcp == nil {
		tcp = d.GetTcp()
	}
	if !IsTcpTarget(target) {
		if tcp != nil {
			return fmt.Errorf("tcp applies to a tcp:// or tcps:// target (got %q)", target)
		}
		return nil
	}
	if _, port, err := splitTargetHostPort(target); err != nil || port == "" {
		return fmt.Errorf("a tcp target needs an explicit port (got %q)", target)
	}
	effective := func(a, b string) string {
		if a != "" {
			return a
		}
		return b
	}
	if m := effective(s.GetMethod(), d.GetMethod()); m != "" {
		return fmt.Errorf("method %q has no meaning with a tcp target: the body is the message", m)
	}
	if len(s.GetHeaders()) > 0 || len(d.GetHeaders()) > 0 {
		return fmt.Errorf("headers have no meaning with a tcp target: the body is the message")
	}
	if pr := effective(s.GetProtocol(), d.GetProtocol()); pr != "" {
		return fmt.Errorf("protocol %q has no meaning with a tcp target", pr)
	}
	if s.Connections != nil || (d != nil && d.Connections != nil) {
		return fmt.Errorf("connections is the HTTP pool's cap and does nothing for a tcp target; set tcp.connections")
	}
	if s.GetGrpc() != nil || d.GetGrpc() != nil || s.GetWebsocket() != nil || d.GetWebsocket() != nil {
		return fmt.Errorf("grpc and websocket cannot go with a tcp target")
	}
	expectEcho := true
	if tcp != nil && tcp.ExpectEcho != nil {
		expectEcho = tcp.GetExpectEcho()
	}
	if expectEcho && effective(s.GetBody(), d.GetBody()) == "" && effective(s.GetBodyFile(), d.GetBodyFile()) == "" {
		return fmt.Errorf("a tcp target needs a body or body_file: the message is what gets echoed and matched (or tcp.expect_echo: false)")
	}
	return nil
}

// splitTargetHostPort returns the host and port of a target URL's authority.
func splitTargetHostPort(target string) (string, string, error) {
	rest := target[strings.Index(target, "://")+3:]
	if i := strings.IndexAny(rest, "/?#"); i >= 0 {
		rest = rest[:i]
	}
	i := strings.LastIndex(rest, ":")
	if i < 0 || strings.Contains(rest[i:], "]") {
		return rest, "", nil
	}
	return rest[:i], rest[i+1:], nil
}

// validateWebSocket checks what the engine would otherwise reject at run time:
// the upgrade is an HTTP/1.1 GET, grpc is another thing entirely, and streams
// and rate are divided over a known number of workers.
func validateWebSocket(p *Plan, s *Scenario) error {
	d := p.GetDefaults()
	w := s.GetWebsocket()
	if w == nil {
		w = d.GetWebsocket()
	}
	if w == nil {
		return nil
	}
	if s.GetGrpc() != nil || d.GetGrpc() != nil {
		return fmt.Errorf("websocket and grpc are mutually exclusive")
	}
	protocol := s.GetProtocol()
	if protocol == "" {
		protocol = d.GetProtocol()
	}
	if protocol != "" && protocol != "http1" {
		return fmt.Errorf("websocket requires protocol http1 (got %q): the upgrade is an HTTP/1.1 request; leave it unset", protocol)
	}
	method := s.GetMethod()
	if method == "" {
		method = d.GetMethod()
	}
	if method != "" && !strings.EqualFold(method, "GET") {
		return fmt.Errorf("websocket requires method GET (got %q); leave it unset", method)
	}
	concurrency := s.GetConcurrency()
	if concurrency == "" {
		concurrency = d.GetConcurrency()
	}
	if concurrency == "auto" {
		return fmt.Errorf(`websocket needs a numeric concurrency, not "auto": streams and rate are divided over the workers`)
	}
	if concurrency != "" {
		streams := uint64(defaultBidiStreams)
		if w.Streams != nil {
			streams = uint64(w.GetStreams())
		}
		workers, err := strconv.ParseUint(concurrency, 10, 32)
		if err == nil && workers > 0 && streams%workers != 0 {
			return fmt.Errorf("websocket.streams (%d%s) must be a multiple of concurrency (%d)",
				streams, map[bool]string{true: ", the engine's default", false: ""}[w.Streams == nil], workers)
		}
	}
	return nil
}

// validateGrpc checks what the engine would otherwise reject at run time:
// gRPC is HTTP/2 POST, and bidi-stream spreads streams and rate over a known
// number of workers. Effective values, since defaults may supply any of them.
func validateGrpc(p *Plan, s *Scenario) error {
	if s.GetBody() != "" && s.GetBodyFile() != "" {
		return fmt.Errorf("body and body_file are mutually exclusive")
	}
	d := p.GetDefaults()
	g := s.GetGrpc()
	if g == nil {
		g = d.GetGrpc()
	}
	if g == nil {
		return nil
	}
	protocol := s.GetProtocol()
	if protocol == "" {
		protocol = d.GetProtocol()
	}
	if protocol != "" && protocol != "http2" {
		return fmt.Errorf("grpc requires protocol http2 (got %q); leave it unset", protocol)
	}
	method := s.GetMethod()
	if method == "" {
		method = d.GetMethod()
	}
	if method != "" && !strings.EqualFold(method, "POST") {
		return fmt.Errorf("grpc requires method POST (got %q); leave it unset", method)
	}
	if g.GetMode() == "bidi-stream" {
		concurrency := s.GetConcurrency()
		if concurrency == "" {
			concurrency = d.GetConcurrency()
		}
		if concurrency == "auto" {
			return fmt.Errorf(`grpc bidi-stream needs a numeric concurrency, not "auto": streams and rate are divided over the workers`)
		}
		if concurrency != "" {
			// The engine's default when streams is unset is 20, and the engine
			// applies the same rule to it.
			streams := uint64(defaultBidiStreams)
			if g.Streams != nil {
				streams = uint64(g.GetStreams())
			}
			workers, err := strconv.ParseUint(concurrency, 10, 32)
			if err == nil && workers > 0 && streams%workers != 0 {
				return fmt.Errorf("grpc.streams (%d%s) must be a multiple of concurrency (%d)",
					streams, map[bool]string{true: ", the engine's default", false: ""}[g.Streams == nil], workers)
			}
		}
	} else if g.Streams != nil || g.MaxInflightPerStream != nil || g.GetDrainDuration() != nil {
		return fmt.Errorf("grpc.streams, max_inflight_per_stream and drain_duration apply to mode bidi-stream only")
	}
	return nil
}

// validateTargets checks a scenario's weighted target list: names present
// and unique, so reports and thresholds can tell the targets apart.
func validateTargets(s *Scenario) error {
	seen := make(map[string]struct{}, len(s.GetTargets()))
	for i, t := range s.GetTargets() {
		if t.GetName() == "" {
			return fmt.Errorf("targets[%d]: name is required", i)
		}
		if strings.ContainsAny(t.GetName(), "/ ") {
			return fmt.Errorf("targets[%d]: name %q must not contain '/' or spaces", i, t.GetName())
		}
		if _, dup := seen[t.GetName()]; dup {
			return fmt.Errorf("targets[%d]: name %q is used twice", i, t.GetName())
		}
		seen[t.GetName()] = struct{}{}
	}
	return nil
}

// ForTarget returns the scenario as it applies to one of its weighted
// targets: a copy with target set to that entry's url and the list cleared,
// and the executor's rates scaled to the entry's share of the total weight
// (rounded to the nearest integer, at least 1). Everything that validates or
// compiles a single-target scenario works on the result unchanged.
func ForTarget(s *Scenario, t *Target) *Scenario {
	out := proto.Clone(s).(*Scenario)
	out.Target = t.GetUrl()
	out.Targets = nil
	var total uint64
	for _, x := range s.GetTargets() {
		total += uint64(weightOf(x))
	}
	if ex := out.GetExecutor(); ex != nil && total > 0 {
		w := uint64(weightOf(t))
		ex.Rate = share(ex.GetRate(), w, total)
		for _, st := range ex.GetStages() {
			st.Rate = share(st.GetRate(), w, total)
		}
	}
	return out
}

func weightOf(t *Target) uint32 {
	if t.GetWeight() == 0 {
		return 1
	}
	return t.GetWeight()
}

// share is rate x w / total, rounded to nearest, at least 1 when rate is set.
func share(rate uint32, w, total uint64) uint32 {
	if rate == 0 {
		return 0
	}
	// Divide first and round from the remainder: rate x w fits in 64 bits
	// (both are below 2^32), but adding total/2 to it before dividing does
	// not always.
	product := uint64(rate) * w
	v := product / total
	if rem := product % total; rem >= total-rem {
		v++
	}
	if v == 0 {
		v = 1
	}
	return uint32(v)
}

// validateStats checks what the schema's host:port rule cannot: the engine's
// statsd sink resolves its address with Envoy's IP resolver, which takes an
// IP literal and no name, so a hostname would fail on the backend, at sink
// creation, with the plan already dispatched.
func validateStats(st *Stats) error {
	for i, sink := range st.GetSinks() {
		// Judged by name AND by the type of the configuration: Envoy finds a
		// sink's factory by its typed config when the name matches none, so a
		// sink called anything at all still becomes the one its config says.
		typ := sink.GetTypedConfig().GetTypeUrl()
		if i := strings.LastIndex(typ, "/"); i >= 0 {
			typ = typ[i+1:]
		}
		// The field for it was withdrawn because the sink aborts the engine on
		// its first flush (see Stats in the schema); the passthrough must not
		// be a way to configure it anyway and take a backend down mid-run.
		if sink.GetName() == openTelemetrySink || typ == openTelemetrySinkConfig {
			return fmt.Errorf("sinks[%d]: %s cannot run in the engine: it aborts on its first flush. "+
				"Send statsd to the collector's statsd receiver instead", i, openTelemetrySink)
		}
		// sortie wraps every Envoy sink in the adapter itself. One written
		// out here would carry a sink this check cannot see into, the
		// OpenTelemetry one included, so it is not accepted at all.
		if sink.GetName() == envoyStatsSinkAdapter || typ == envoyStatsSinkAdapterConfig {
			return fmt.Errorf("sinks[%d]: name the Envoy sink itself, not %s: sortie adds the adapter",
				i, envoyStatsSinkAdapter)
		}
	}
	if st.GetStatsd() == nil {
		return nil
	}
	host, port, err := net.SplitHostPort(st.GetStatsd().GetAddress())
	if err != nil {
		return fmt.Errorf("statsd.address: %w", err)
	}
	// SplitHostPort only separates the two; the range is checked here so that
	// validate and compile, which builds a socket address from it, agree.
	if n, err := strconv.ParseUint(port, 10, 16); err != nil || n == 0 {
		return fmt.Errorf("statsd.address: port %q is not in 1..65535", port)
	}
	if net.ParseIP(host) == nil {
		return fmt.Errorf("statsd.address: %q is not an IP address; the engine's statsd sink "+
			"does not resolve names, so use the server's IP (a Service's clusterIP)", host)
	}
	return nil
}

const (
	openTelemetrySink     = "envoy.stat_sinks.open_telemetry"
	envoyStatsSinkAdapter = "nighthawk.envoy_stats_sink_adapter"

	// The configuration types of the two, as they end a type URL.
	openTelemetrySinkConfig     = "envoy.extensions.stat_sinks.open_telemetry.v3.SinkConfig"
	envoyStatsSinkAdapterConfig = "nighthawk.EnvoyStatsSinkAdapterConfig"

	// DefaultStatsPrefix is the first component of every metric name when a
	// stats block sets no prefix.
	DefaultStatsPrefix = "sortie"
)

// StatsLabel is an execution label as it appears in a metric prefix: each
// `/`-separated segment lowercased and reduced to [a-z0-9_] (every other run
// of characters becomes one underscore, trimmed at both ends), with segments
// that reduce to nothing dropped. `Live Metrics/stage-2` is
// ["live_metrics", "stage_2"].
func StatsLabel(label string) []string {
	var out []string
	for _, segment := range strings.Split(label, "/") {
		var b strings.Builder
		pending := false
		for _, r := range strings.ToLower(segment) {
			switch {
			case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
				if pending && b.Len() > 0 {
					b.WriteByte('_')
				}
				pending = false
				b.WriteRune(r)
			default:
				pending = true
			}
		}
		if b.Len() > 0 {
			out = append(out, b.String())
		}
	}
	return out
}

// validateStatsPrefixes refuses a plan in which two executions with a statsd
// sink would emit under one prefix. Names are unique as written, but a prefix
// is built from an execution's sanitized label, where `Foo` and `foo` are the
// same, `///` is nothing at all, and stage 1 of a staircase named `foo` is
// the constant scenario named `foo/stage-1`: their counters and timers would
// be summed by the statsd server with nothing to tell them apart.
//
// Only the statsd sink is concerned. It is the one whose prefix sortie
// writes; a sink passed through stats.sinks names its metrics itself.
func validateStatsPrefixes(p *Plan) error {
	seen := map[string]string{}
	for _, s := range p.GetScenarios() {
		st := s.GetStats()
		if st == nil {
			st = p.GetDefaults().GetStats()
		}
		if st == nil {
			st = p.GetStats()
		}
		if st.GetStatsd() == nil {
			continue
		}
		executor := s.GetExecutor()
		if executor == nil {
			executor = p.GetDefaults().GetExecutor()
		}
		// Every label compile.Expand gives this scenario's executions: one per
		// weighted target (its own or inherited from defaults), times one per
		// staircase stage. Each must have a prefix no other execution in the
		// plan has, in this scenario or another.
		subject := s
		if s.GetTarget() == "" && len(s.GetTargets()) == 0 && len(p.GetDefaults().GetTargets()) > 0 {
			subject = proto.Clone(s).(*Scenario)
			subject.Targets = p.GetDefaults().GetTargets()
		}
		var labels []string
		for _, variant := range Variants(subject) {
			if executor.GetType() != Staircase {
				labels = append(labels, variant.GetName())
				continue
			}
			for i := range executor.GetStages() {
				labels = append(labels, fmt.Sprintf("%s/stage-%d", variant.GetName(), i+1))
			}
		}
		if len(StatsLabel(s.GetName())) == 0 {
			return fmt.Errorf("scenario %q: its name has no letter, digit or underscore to name its metrics by; "+
				"rename it, or drop its statsd sink", s.GetName())
		}
		for _, label := range labels {
			// The prefix as emitted: see compile.StatsPrefix.
			prefix := StatsPrefix(st.GetPrefix(), label)
			if other, dup := seen[prefix]; dup {
				return fmt.Errorf("executions %q and %q would emit their metrics under the same prefix (%s); "+
					"rename a scenario or a target, or give the scenarios different stats.prefix values",
					other, label, prefix)
			}
			seen[prefix] = label
		}
	}
	return nil
}

// StatsPrefix is the prefix an execution's statsd sink emits its metrics
// under: the stats block's prefix (or DefaultStatsPrefix), then StatsLabel of
// the execution's label, joined with dots.
//
//	StatsPrefix("", "smoke")           == "sortie.smoke"
//	StatsPrefix("", "smoke/stage-2")   == "sortie.smoke.stage_2"
//	StatsPrefix("soak", "Checkout/eu") == "soak.checkout.eu"
func StatsPrefix(prefix, label string) string {
	if prefix == "" {
		prefix = DefaultStatsPrefix
	}
	return strings.Join(append([]string{prefix}, StatsLabel(label)...), ".")
}
