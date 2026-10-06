package compile

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	metricsv3 "github.com/envoyproxy/go-control-plane/envoy/config/metrics/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	client "github.com/bpalermo/sortie/engine/api/client"
	statssink "github.com/bpalermo/sortie/engine/api/stats_sink"
	"github.com/bpalermo/sortie/internal/plan"
)

// Names of the stats sink extensions a stats block compiles to.
const (
	// EnvoyStatsSinkAdapter is the engine's own sink that hosts a stats sink
	// implemented as an Envoy extension. The engine resolves the sinks of its
	// bootstrap against its own sink registry, not Envoy's, so an Envoy sink
	// reaches it only wrapped in this one.
	EnvoyStatsSinkAdapter = "nighthawk.envoy_stats_sink_adapter"

	StatsdSink = "envoy.stat_sinks.statsd"

	// DefaultStatsPrefix is plan.DefaultStatsPrefix.
	DefaultStatsPrefix = plan.DefaultStatsPrefix
)

// StatsPrefix is plan.StatsPrefix: the prefix an execution's statsd sink
// emits under. It lives in the plan package so that plan validation, which
// refuses two executions that would share one, computes exactly what is
// compiled here.
func StatsPrefix(prefix, label string) string { return plan.StatsPrefix(prefix, label) }

// applyStats adds a stats block's sinks to the options, after the sinks a
// template may carry, and sets the flush interval when the block names one.
func applyStats(o *client.CommandLineOptions, st *plan.Stats, label string) error {
	sinks, err := statsSinks(st, label)
	if err != nil {
		return err
	}
	o.StatsSinks = append(o.StatsSinks, sinks...)
	if st.GetFlushInterval() != nil {
		o.OneofStatsFlushInterval = &client.CommandLineOptions_StatsFlushIntervalDuration{
			StatsFlushIntervalDuration: st.GetFlushInterval(),
		}
	}
	return nil
}

// statsSinks compiles a stats block into the engine's stats_sinks for an
// execution labelled label.
func statsSinks(st *plan.Stats, label string) ([]*metricsv3.StatsSink, error) {
	prefix := StatsPrefix(st.GetPrefix(), label)
	var out []*metricsv3.StatsSink

	if sd := st.GetStatsd(); sd != nil {
		addr, err := socketAddress(sd.GetAddress())
		if err != nil {
			return nil, fmt.Errorf("stats.statsd.address: %w", err)
		}
		sink, err := adapted(StatsdSink, &metricsv3.StatsdSink{
			StatsdSpecifier: &metricsv3.StatsdSink_Address{Address: addr},
			Prefix:          prefix,
			// The engine presents its latency samples in microseconds with
			// that unit declared; without this the sink would label the
			// microsecond value as milliseconds.
			ScaleHistogramUnitsToMilliseconds: true,
		})
		if err != nil {
			return nil, err
		}
		out = append(out, sink)
	}

	for i, sink := range st.GetSinks() {
		if sink.GetName() == "" {
			return nil, fmt.Errorf("stats.sinks[%d]: a sink needs a name", i)
		}
		wrapped, err := anypb.New(&statssink.EnvoyStatsSinkAdapterConfig{Sink: sink})
		if err != nil {
			return nil, fmt.Errorf("stats.sinks[%d]: %w", i, err)
		}
		out = append(out, &metricsv3.StatsSink{
			Name:       EnvoyStatsSinkAdapter,
			ConfigType: &metricsv3.StatsSink_TypedConfig{TypedConfig: wrapped},
		})
	}
	return out, nil
}

// adapted wraps an Envoy sink's configuration in the engine's adapter.
func adapted(name string, cfg proto.Message) (*metricsv3.StatsSink, error) {
	inner, err := anypb.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("packing %s: %w", name, err)
	}
	outer, err := anypb.New(&statssink.EnvoyStatsSinkAdapterConfig{
		Sink: &metricsv3.StatsSink{
			Name:       name,
			ConfigType: &metricsv3.StatsSink_TypedConfig{TypedConfig: inner},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("packing %s: %w", EnvoyStatsSinkAdapter, err)
	}
	return &metricsv3.StatsSink{
		Name:       EnvoyStatsSinkAdapter,
		ConfigType: &metricsv3.StatsSink_TypedConfig{TypedConfig: outer},
	}, nil
}

// socketAddress turns host:port into an Envoy socket address.
func socketAddress(hostPort string) (*corev3.Address, error) {
	host, portStr, err := net.SplitHostPort(hostPort)
	if err != nil {
		return nil, err
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil || port == 0 {
		return nil, fmt.Errorf("port %q is not in 1..65535", portStr)
	}
	return &corev3.Address{Address: &corev3.Address_SocketAddress{SocketAddress: &corev3.SocketAddress{
		Address:       host,
		PortSpecifier: &corev3.SocketAddress_PortValue{PortValue: uint32(port)},
	}}}, nil
}
