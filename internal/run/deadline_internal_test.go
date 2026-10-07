package run

import (
	"math"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	client "github.com/bpalermo/sortie/engine/api/client"
	"github.com/bpalermo/sortie/internal/compile"
)

// The deadline covers every wait the engine was asked for after the run's
// duration, read from the options it is actually sent: an engine still
// draining as it was told to must not be taken for a silent one.
func TestBackendDeadlineCoversWhatTheEngineWasAskedToWait(t *testing.T) {
	const grace = 2 * time.Minute
	d := durationpb.New
	for name, c := range map[string]struct {
		opts *client.CommandLineOptions
		want time.Duration
	}{
		"nothing set: the engine's own 30s timeout": {
			&client.CommandLineOptions{}, time.Minute + 30*time.Second + grace,
		},
		"a timeout, wherever it came from": {
			&client.CommandLineOptions{Timeout: d(5 * time.Minute)}, time.Minute + 5*time.Minute + grace,
		},
		"a grpc stream's drain": {
			&client.CommandLineOptions{GrpcStream: &client.CommandLineOptions_GrpcStreamOptions{DrainDuration: d(10 * time.Minute)}},
			time.Minute + 30*time.Second + 10*time.Minute + grace,
		},
		"a websocket's drain": {
			&client.CommandLineOptions{Websocket: &client.CommandLineOptions_WebSocketOptions{DrainDuration: d(4 * time.Minute)}},
			time.Minute + 30*time.Second + 4*time.Minute + grace,
		},
		"a tcp run's drain": {
			&client.CommandLineOptions{Tcp: &client.CommandLineOptions_TcpOptions{DrainDuration: d(3 * time.Minute)}},
			time.Minute + 30*time.Second + 3*time.Minute + grace,
		},
		"a udp run's loss timeout": {
			&client.CommandLineOptions{Udp: &client.CommandLineOptions_UdpOptions{Timeout: d(7 * time.Second)}},
			time.Minute + 30*time.Second + 7*time.Second + grace,
		},
	} {
		got := (&Runner{}).backendDeadline(compile.Execution{Duration: time.Minute, Options: c.opts})
		if got != c.want {
			t.Errorf("%s: deadline = %s, want %s", name, got, c.want)
		}
	}

	// A run of absurd length saturates rather than wrapping into the past.
	huge := compile.Execution{Duration: math.MaxInt64 - time.Second, Options: &client.CommandLineOptions{}}
	if got := (&Runner{}).backendDeadline(huge); got != math.MaxInt64 {
		t.Errorf("an overflowing deadline = %d, want it saturated", got)
	}
	// A caller's own grace replaces the default.
	if got := (&Runner{ResponseGrace: time.Second}).backendDeadline(compile.Execution{Duration: time.Minute, Options: &client.CommandLineOptions{}}); got != time.Minute+30*time.Second+time.Second {
		t.Errorf("with a 1s grace the deadline = %s", got)
	}
}
