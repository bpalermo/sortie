package run_test

import (
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	client "github.com/envoyproxy/nighthawk/api/client"
)

// fakeService is a stand-in for nighthawk_service.
//
// It listens on a real port and speaks the real ExecutionStream RPC rather than
// mocking the client, so the tests exercise dialling, the stream's half-close
// and response handling -- the parts most likely to break and least likely to
// be caught by testing against an interface.
type fakeService struct {
	client.UnimplementedNighthawkServiceServer

	mu sync.Mutex
	// requests records every StartRequest received, in order.
	requests []*client.CommandLineOptions

	// addr is where the fake is listening.
	addr string

	// respond builds the response for the nth request. A nil error and a nil
	// output means "report a failure", matching how Nighthawk answers when its
	// failure predicates fire.
	respond func(n int, opts *client.CommandLineOptions) *client.ExecutionResponse
}

func (f *fakeService) ExecutionStream(stream client.NighthawkService_ExecutionStreamServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		start := req.GetStartRequest()
		if start == nil {
			return fmt.Errorf("fake service got a non-start request")
		}

		f.mu.Lock()
		n := len(f.requests)
		f.requests = append(f.requests, start.GetOptions())
		f.mu.Unlock()

		if err := stream.Send(f.respond(n, start.GetOptions())); err != nil {
			return err
		}
	}
}

func (f *fakeService) received() []*client.CommandLineOptions {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*client.CommandLineOptions(nil), f.requests...)
}

// startFake runs the fake on a free port and returns its address.
func startFake(t *testing.T, respond func(n int, opts *client.CommandLineOptions) *client.ExecutionResponse) *fakeService {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	fake := &fakeService{respond: respond}

	server := grpc.NewServer()
	client.RegisterNighthawkServiceServer(server, fake)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	fake.addr = listener.Addr().String()
	return fake
}

// okResponse is a plausible successful Output: one global result carrying a
// latency statistic and the counters thresholds are usually written against.
func okResponse(requests uint64, p95 time.Duration, elapsed time.Duration) *client.ExecutionResponse {
	return &client.ExecutionResponse{
		Output: &client.Output{
			Results: []*client.Result{{
				Name:              "global",
				ExecutionDuration: durationpb.New(elapsed),
				Statistics: []*client.Statistic{{
					Id:    "benchmark_http_client.latency_2xx",
					Count: requests,
					Percentiles: []*client.Percentile{{
						Percentile:   0.95,
						DurationType: &client.Percentile_Duration{Duration: durationpb.New(p95)},
					}},
				}},
				Counters: []*client.Counter{
					{Name: "benchmark.http_2xx", Value: requests},
				},
			}},
		},
	}
}
