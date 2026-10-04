package plan

import (
	"fmt"
	"strconv"
	"strings"

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
		if s.GetTarget() == "" && p.GetDefaults().GetTarget() == "" {
			return fmt.Errorf("scenario %q: target is required (set it on the scenario or in defaults)",
				s.GetName())
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
		if err := validateGrpc(p, s); err != nil {
			return fmt.Errorf("scenario %q: %w", s.GetName(), err)
		}
		if err := validateWebSocket(p, s); err != nil {
			return fmt.Errorf("scenario %q: %w", s.GetName(), err)
		}
		if err := validateTls(p, s); err != nil {
			return fmt.Errorf("scenario %q: %w", s.GetName(), err)
		}
		if err := validateTcp(p, s); err != nil {
			return fmt.Errorf("scenario %q: %w", s.GetName(), err)
		}
		if err := validateUdp(p, s); err != nil {
			return fmt.Errorf("scenario %q: %w", s.GetName(), err)
		}
	}
	return nil
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
