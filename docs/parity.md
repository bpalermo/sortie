# Parity with Envoy, as a client

sortie exists to load-test what Envoy serves and proxies. "Parity" here means:
everything Envoy terminates or forwards can be driven at a controlled rate,
with latency measured per message, from a plan. This is the state of that, and
the order the gaps get closed in.

## Done

| Envoy does | sortie drives it with | Since |
|---|---|---|
| HTTP/1.1, HTTP/2, HTTP/3 (QUIC) | `protocol: http1 \| http2 \| http3` | Nighthawk |
| TLS, mTLS termination | `tls: {ca_file, cert_file, key_file}`; anything further through `nighthawk_template` (`tls_context`, `transport_socket`) | #30 |
| gRPC unary | `grpc: {mode: unary}` + a serialized `body_file` | engine fork |
| gRPC bidirectional streaming | `grpc: {mode: bidi-stream, streams, max_inflight_per_stream, drain_duration}`, `benchmark_stream.message_latency` | engine fork |
| Request routing on headers, bodies | `headers`, `body`, `body_file`, `method` | sortie |
| Rate shaping: constant, ramp, staircase; open and closed loop | `executor` | sortie |
| Stats sinks (statsd, OTLP) from the client's own counters | `nighthawk.envoy_stats_sink_adapter` via `nighthawk_template` | engine fork |

## Not yet, in order

### 1. WebSocket

Envoy's upgrade path (`upgrade_configs: [{upgrade_type: websocket}]`) is one of
the things people specifically run Envoy for, and no load tool in this space
drives it with per-message latency.

Design, mirroring the gRPC bidi-stream client (`engine/source/client/
grpc_stream_client_impl.*`), which is the same shape -- long-lived streams per
worker, messages sent round-robin at the executor's rate, each timed against
its echo:

- The engine opens `streams` HTTP/1.1 connections per worker through its
  existing pool and sends an `Upgrade: websocket` request. Envoy's HTTP/1
  client codec already handles the `101` and switches the stream to raw data
  (`upgrade_request_` in `source/common/http/http1/codec_impl.h`), so after the
  handshake `encodeData` on the stream is the WebSocket connection.
- A `WebSocketStreamClient` frames messages per RFC 6455 (client-side
  masking, text or binary opcode, ping/pong, close handshake on drain) and
  correlates each sent message with its echo by a sequence prefix, recording
  `benchmark_stream.message_latency` as the gRPC bidi client does. The
  `max_inflight_per_stream` and `drain_duration` semantics carry over as is.
- The plan gets `websocket: {streams, max_inflight_per_stream, drain_duration,
  binary: bool}`; the message is `body` or `body_file`; `protocol` must be
  unset or `http1`; the target is `ws://`-less -- plain `http://` or `https://`
  (`wss`), since the upgrade is an HTTP request.
- `nighthawk_test_server` gets a `websocket-echo` HTTP filter so the e2e test
  has something to talk to: it answers the upgrade itself (`encodeHeaders`
  with 101, then `encodeData` for every frame it unmasks), the way the
  `test-server` filter answers requests without a router.
- Rate semantics are the bidi-stream ones: a backend's aggregate, divided
  over its workers by the engine (`perWorkerRequestsPerSecond`).

Size: comparable to the gRPC bidi-stream work -- the client, the test-server
filter, options and plan schema, validation, e2e.

### 2. TCP

For Envoy's `tcp_proxy` and TLS-terminating listeners in front of non-HTTP
services. The engine's `BenchmarkClient` is built on Envoy's HTTP connection
pools; a TCP mode is a second implementation of that interface rather than a
new mode of the existing one:

- `TcpBenchmarkClient` keeps a fixed pool of `tcp.connections`
  `Network::ClientConnection`s per worker, opened in `prepare()` (with the
  transport socket the `tls` block configures, so TLS termination is covered)
  and, on each `tryStartRequest`, writes `body` -- exactly `body`, no prefix:
  a TCP connection delivers in order, so echoes are correlated FIFO against
  the bytes sent -- on the next connection and times the echo. With
  `expect_echo: false` nothing is timed: a write only queues bytes locally,
  so the run counts sends and reports no latency. This is its own field, not
  `Scenario.connections`: that one is the HTTP pool's circuit-breaker cap
  (default 100), and a TCP mode wants a small, exact, eagerly opened pool --
  default 1 per worker, the way `websocket.streams` is exact.
- Plan: `tcp: {connections, expect_echo}`; `target` becomes `tcp://host:port`;
  `method`, `headers`, `grpc`, `websocket` and `connections` are errors with
  it. Counters:
  `benchmark.tcp_connect_failure`, `benchmark.tcp_messages`,
  `benchmark.tcp_echo_mismatch`.
- `nighthawk_test_server` already links Envoy's `echo` network filter, which
  is the test target.

Size: smaller than WebSocket -- no framing, no handshake -- but it touches the
engine's factories, since today they assume HTTP.

### 3. UDP

For Envoy's `udp_proxy`. Same shape as TCP without connections: a
`Network::UdpListener`-backed client sends datagrams at rate and correlates
echoes by sequence number, counting loss (`benchmark.udp_lost`) instead of
connection failures, with a per-datagram deadline. The test server needs a UDP
echo listener, which Envoy does not ship as an extension; a small one in
`engine/source/server`. Last, because the question it answers -- does the
proxy keep up, and what does it drop -- is narrower.

## Not in scope

- Scripting and session flow (log in, take a token, use it): the engine's
  request source never sees responses, by design. A plan is stateless load.
- Browser or client-library emulation (HTTP/2 priority trees, QUIC migration
  behaviour): the engine is Envoy's own client stack, which is the point.
