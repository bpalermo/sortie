// Package run executes a plan against Nighthawk backends and collects verdicts.
package run

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"math"
	"net"
	"sync"
	"time"

	client "github.com/bpalermo/sortie/engine/api/client"
	"github.com/bpalermo/sortie/internal/compile"
	"github.com/bpalermo/sortie/internal/nh"
	"github.com/bpalermo/sortie/internal/plan"
	"github.com/bpalermo/sortie/internal/result"
	"github.com/bpalermo/sortie/internal/threshold"
)

// ExecutionReport is the verdict for one Nighthawk execution.
type ExecutionReport struct {
	Label    string
	Scenario string
	Pool     string
	Rate     uint32
	// PerBackend says Rate is each backend's rather than the pool's total.
	PerBackend bool
	Duration   time.Duration
	RampTime   time.Duration
	Started    time.Time
	Elapsed    time.Duration

	// Backends are the addresses the execution was dispatched to, in order --
	// set whether or not the dispatch succeeded, so a failed run still says
	// where it was going. Dns is the name they came from, for a dns pool.
	Backends []string
	Dns      string

	Set      *result.Set
	Outcomes []result.Outcome
	Pass     bool

	// BackendErrors are the backends that did not finish cleanly: one that
	// could not be reached or went away mid-run, which has no results in Set,
	// and one whose engine reported a failure but still returned what it had
	// counted, which does. Either fails the execution; neither discards the
	// other backends' results.
	BackendErrors []BackendError

	// Err is set when the execution itself failed, as opposed to failing a
	// threshold: nothing could be dispatched, or no backend returned anything.
	Err error
}

// BackendError is one backend's failure within an execution.
type BackendError struct {
	Addr string
	Err  error
}

// Report is the verdict for a whole plan.
type Report struct {
	Executions []ExecutionReport
	Pass       bool
}

// Runner executes plans.
type Runner struct {
	Plan *plan.Plan

	// Observer, when set, is notified as executions start and finish, and --
	// with ProgressInterval -- as their backends report progress.
	Observer Observer

	// ProgressInterval, when positive, asks every service backend for a
	// snapshot of its run this often and passes each to the Observer. Backends
	// behind a distributor report nothing until they finish.
	ProgressInterval time.Duration

	// Resolver looks up the dns pools' names when the run starts. nil means
	// net.DefaultResolver.
	Resolver plan.Resolver

	// ResolveTimeout bounds how long Run waits for a dns pool's name to answer
	// with at least one address before giving up, lookups in flight included.
	// Zero means resolveTimeout.
	ResolveTimeout time.Duration

	// DialTimeout bounds the wait for each backend to accept connections.
	// Zero means nh.Dial's own default.
	DialTimeout time.Duration

	// ResponseGrace is how long past an execution's planned duration a
	// backend may take to answer before it is given up on. Zero means
	// responseGrace. See backendDeadline.
	ResponseGrace time.Duration

	// Serializes the backends' progress callbacks into the Observer.
	observerMu sync.Mutex
}

// resolveTimeout is how long a dns pool's name may answer with nothing before
// the run fails, for the same reason nh.Dial waits for a backend: the engines
// often start with the run that uses them -- the chart's DaemonSet comes up
// beside its Job -- and a headless Service lists a pod only once it is ready.
// Nothing after this long is an error.
const resolveTimeout = 30 * time.Second

// resolvePoll is how often an empty answer is asked again.
const resolvePoll = time.Second

// Observer receives progress callbacks. The Runner never calls it from two
// goroutines at once: the backends' progress arrives concurrently and is
// serialized before it reaches ExecutionProgress, so an observer needs no
// locking of its own.
type Observer interface {
	ExecutionStarted(e compile.Execution, backends []string)
	// ExecutionProgress carries one backend's interim snapshot; elapsed is the
	// time since that backend's workers started.
	ExecutionProgress(e compile.Execution, backend string, elapsed time.Duration, out *client.Output)
	ExecutionFinished(r ExecutionReport)
}

// Run executes every scenario in plan order and returns the collected report.
// Scenarios run sequentially: two scenarios sharing a pool would otherwise
// contend for the same Nighthawk instances, which accept a single execution at
// a time, and two scenarios on different pools would still distort each
// other's latency measurements if they share a target.
//
// The dns pools are resolved first, once, before the first execution, so every
// scenario in the run drives the same backends; a failure there is returned
// with no report, since nothing ran.
func (r *Runner) Run(ctx context.Context) (*Report, error) {
	p, err := r.resolve(ctx)
	if err != nil {
		return nil, err
	}

	report := &Report{Pass: true}

	for _, scenario := range p.GetScenarios() {
		executions, err := compile.Expand(scenario)
		if err != nil {
			return nil, err
		}
		thresholds, err := threshold.ParseAll(plan.EffectiveThresholds(p, scenario))
		if err != nil {
			return nil, fmt.Errorf("scenario %q: %w", scenario.GetName(), err)
		}
		pool := plan.PoolFor(p, scenario)

		// Executions that share a Group -- the targets of a weighted scenario
		// -- start together and are reported in plan order once all are done.
		// Everything else runs one at a time, as before.
		for i := 0; i < len(executions); {
			j := i + 1
			if executions[i].Group != "" {
				for j < len(executions) && executions[j].Group == executions[i].Group {
					j++
				}
			}
			reports := r.runGroup(ctx, executions[i:j], pool, thresholds)
			i = j
			for _, er := range reports {
				if !er.Pass {
					report.Pass = false
				}
				report.Executions = append(report.Executions, er)
				if r.Observer != nil {
					r.Observer.ExecutionFinished(er)
				}
			}
			if ctx.Err() != nil {
				for _, er := range reports {
					if er.Err != nil {
						return report, ctx.Err()
					}
				}
			}
		}
	}
	return report, nil
}

// resolve returns the plan with its dns pools resolved. Each name is looked
// up until it answers with at least one address, for up to ResolveTimeout of
// its own -- lookups in flight included -- and is then left alone: a name that
// has answered is not asked again while another is still empty, so its
// backends are the ones it gave the first time. "Empty" includes NXDOMAIN,
// which is how a headless Service with no ready pod often answers (see
// emptyAnswer), so a name that does not exist is waited on for the timeout
// too rather than failing at once. Any other resolver error is returned
// immediately; an answer still empty when the wait is over is the same
// ResolveError. A context cancelled during a wait ends it with the context's
// error.
func (r *Runner) resolve(ctx context.Context) (*plan.Plan, error) {
	resolver := r.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	timeout := r.ResolveTimeout
	if timeout <= 0 {
		timeout = resolveTimeout
	}
	out := proto.Clone(r.Plan).(*plan.Plan)
	for _, pool := range out.GetPools() {
		if pool.GetDns() == "" {
			continue
		}
		services, err := r.resolvePool(ctx, pool, resolver, timeout)
		if err != nil {
			return nil, err
		}
		pool.Services = services
	}
	return out, nil
}

// resolvePool is one name's wait: see resolve.
func (r *Runner) resolvePool(ctx context.Context, pool *plan.Pool, resolver plan.Resolver, timeout time.Duration) ([]string, error) {
	deadline := time.Now().Add(timeout)
	// The lookups themselves are bound by the deadline, not only the pauses
	// between them: a resolver waiting on a name server that never answers
	// would otherwise hold the run for as long as it liked.
	lookupCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	for {
		services, err := plan.ResolvePool(lookupCtx, pool, resolver)
		if err == nil {
			return services, nil
		}
		// Cancelled while a lookup was in flight: the resolver's error wraps
		// the context's, inside a ResolveError the CLI would report as bad
		// usage. An interrupted run is not a bad plan. (The deadline passing
		// is not this: it is the wait running out, reported below as the
		// ResolveError it is.)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// A lookup that errored is final; only an answer that is empty for
		// now is worth asking again, and only while there is time.
		var re *plan.ResolveError
		if !errors.As(err, &re) || !emptyAnswer(re) {
			return nil, err
		}
		// Never past the deadline: the last poll is as short as what is left.
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, err
		}
		select {
		case <-ctx.Done():
			// Interrupted while waiting. That is a cancelled run, not a plan
			// that names something missing, so it is the context's error and
			// not the ResolveError the CLI reports as bad usage.
			return nil, ctx.Err()
		case <-time.After(min(resolvePoll, remaining)):
		}
	}
}

// emptyAnswer reports whether a ResolveError is a name that exists but has no
// address yet. A Kubernetes headless Service with no ready pod answers either
// with no record or with NXDOMAIN, so both count.
func emptyAnswer(re *plan.ResolveError) bool {
	var dnsErr *net.DNSError
	if errors.As(re.Err, &dnsErr) {
		return dnsErr.IsNotFound
	}
	return errors.Is(re.Err, plan.ErrNoAddress)
}

// runGroup runs the executions concurrently, one goroutine each, and returns
// their reports in the same order. A single execution runs inline.
func (r *Runner) runGroup(
	ctx context.Context,
	group []compile.Execution,
	pool *plan.Pool,
	thresholds []threshold.Threshold,
) []ExecutionReport {
	reports := make([]ExecutionReport, len(group))
	if len(group) == 1 {
		reports[0] = r.runExecution(ctx, group[0], pool, thresholds)
		return reports
	}
	var wg sync.WaitGroup
	for i, e := range group {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reports[i] = r.runExecution(ctx, e, pool, thresholds)
		}()
	}
	wg.Wait()
	return reports
}

func (r *Runner) runExecution(
	ctx context.Context,
	e compile.Execution,
	pool *plan.Pool,
	thresholds []threshold.Threshold,
) ExecutionReport {
	er := ExecutionReport{
		Label:      e.Label,
		Scenario:   e.Scenario.Name,
		Pool:       pool.Name,
		Rate:       e.Rate,
		PerBackend: e.PerBackend,
		Duration:   e.Duration,
		RampTime:   e.RampTime,
		Started:    time.Now(),
		Dns:        pool.Dns,
	}

	backends := pool.Services
	if pool.Distributor != "" {
		backends = pool.Targets
	}
	er.Backends = backends
	if r.Observer != nil {
		// Grouped executions start from several goroutines at once.
		r.observerMu.Lock()
		r.Observer.ExecutionStarted(e, backends)
		r.observerMu.Unlock()
	}

	addrs, outputs, failed, err := r.dispatch(ctx, e, pool)
	er.Elapsed = time.Since(er.Started)
	if err != nil {
		er.Err = err
		return er
	}
	er.BackendErrors = failed

	// A run the caller cancelled is not judged. Its backends return what they
	// had counted along with the cancellation, and partial counts held up to
	// thresholds would read as a verdict on a run that never finished: a
	// rate threshold failing, or a count threshold passing, for no reason but
	// the interruption.
	if ctx.Err() != nil {
		er.Err = fmt.Errorf("execution cancelled: %w", ctx.Err())
		return er
	}

	// Judge what came back. A backend that returned nothing is left out of
	// the set and named in BackendErrors; the others' counters and latencies
	// are still the record of what the run did.
	var gotAddrs []string
	var gotOutputs []*client.Output
	for i, out := range outputs {
		if out != nil {
			gotAddrs = append(gotAddrs, addrs[i])
			gotOutputs = append(gotOutputs, out)
		}
	}
	if len(gotOutputs) == 0 {
		errs := make([]error, 0, len(failed))
		for _, f := range failed {
			errs = append(errs, fmt.Errorf("backend %s: %w", f.Addr, f.Err))
		}
		er.Err = errors.Join(errs...)
		if er.Err == nil {
			er.Err = errors.New("no backend returned a result")
		}
		return er
	}
	set, err := result.NewSet(gotAddrs, gotOutputs)
	if err != nil {
		er.Err = err
		return er
	}
	er.Set = set
	er.Outcomes, er.Pass = set.Evaluate(thresholds)
	// Thresholds that hold over the survivors do not make a run that lost a
	// backend a pass: part of the load was never generated or never reported.
	if len(failed) > 0 {
		er.Pass = false
	}
	return er
}

// responseGrace is how long past its planned duration an execution may take
// to answer. An engine needs some of it honestly: it opens its connections
// before the clock starts, drains what is in flight when it stops, and
// assembles the report, which together run to seconds. The rest is margin.
const responseGrace = 2 * time.Minute

// engineTimeout is the engine's default for the timeout option, which bounds
// connecting and the final drain of an HTTP run, when the options name none.
const engineTimeout = 30 * time.Second

// engineDrain is the longest wait an engine performs by default at the end of
// a run when the options set none: see backendDeadline.
const engineDrain = time.Second

// backendDeadline is how long a backend has, from dispatch, to return an
// execution's result: the planned duration, every wait the engine was asked
// to perform after it, and the grace.
//
// Those waits are read from the compiled options, which is what the engine
// actually receives: the timeout, whether the scenario set it or a template
// did, and whichever mode's drain window applies -- a gRPC stream's, a
// WebSocket's or a TCP run's wait for outstanding echoes, a UDP run's wait
// before a datagram is lost. A plan may set any of them to minutes, and an
// engine still draining as it was told to is not a silent one.
//
// It exists for the backend that goes SILENT. One that dies audibly -- a
// deleted pod, a refused connection -- breaks the stream and is reported at
// once. One whose node freezes or loses its network sends nothing, no FIN and
// no RST, and a stream with no deadline waits on it for ever: the run then
// has no report for any backend, long after every other one has finished. An
// execution has a planned length, so "no answer well past it" is a failure
// that can be recognised without hearing from the peer at all.
func (r *Runner) backendDeadline(e compile.Execution) time.Duration {
	grace := r.ResponseGrace
	if grace <= 0 {
		grace = responseGrace
	}
	o := e.Options
	timeout := engineTimeout
	if o.GetTimeout() != nil {
		timeout = o.GetTimeout().AsDuration()
	}
	// The mode's window, as set -- or, when the options name none, the
	// engine's own default for it. Those defaults are not zero: 500ms of
	// drain for a gRPC stream, WebSocket or TCP run, 1s before a UDP datagram
	// is lost, and a run can be in one of those modes with no tuning block
	// at all (a tcp:// or udp:// target says enough). engineDrain is the
	// largest of them, so it covers whichever mode this is without having to
	// work out which; an HTTP run is given a second it does not need.
	var windows []time.Duration
	for _, set := range []*durationpb.Duration{
		o.GetGrpcStream().GetDrainDuration(),
		o.GetWebsocket().GetDrainDuration(),
		o.GetTcp().GetDrainDuration(),
		o.GetUdp().GetTimeout(),
	} {
		if set != nil {
			windows = append(windows, set.AsDuration())
		}
	}
	if len(windows) == 0 {
		windows = []time.Duration{engineDrain}
	}
	budget := addDuration(e.Duration, timeout)
	for _, w := range windows {
		budget = addDuration(budget, w)
	}
	return addDuration(budget, grace)
}

// addDuration adds two non-negative durations without wrapping: a plan can
// carry durations of years, and a sum that overflowed would be a deadline in
// the past.
func addDuration(a, b time.Duration) time.Duration {
	if b <= 0 {
		return a
	}
	if a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}

// dispatch runs the execution on every backend of the pool and returns, in
// the pool's order, each backend's output (nil when it returned none) and the
// backends that failed. Backends are independent: one that cannot be reached,
// or goes away halfway, does not stop the others -- with one engine per node,
// a node rebooting mid-soak must not void the run on every other node. Only
// the caller's context ends them all. The error return is for a plan that
// could not be dispatched at all.
func (r *Runner) dispatch(
	ctx context.Context,
	e compile.Execution,
	pool *plan.Pool,
) ([]string, []*client.Output, []BackendError, error) {
	addrs, perBackend, err := compile.ForPool(e, pool)
	if err != nil {
		return nil, nil, nil, err
	}

	// Every backend gets the same budget, measured from here. A backend that
	// has not answered when it runs out is cancelled and reported; the parent
	// context, which is the caller's, is untouched and tells the two apart.
	// runBound is how long the run itself may take, and what a backend's own
	// account of it is held to. budget is how long to wait for the answer: the
	// same, plus however long the engine was told to wait before starting. A
	// scheduled start reaches the engine through the template, and the engine
	// sits idle until then -- time that is not part of the execution it
	// reports, but is very much part of how long its answer takes.
	runBound := r.backendDeadline(e)
	budget := runBound
	if start := e.Options.GetScheduledStart(); start != nil {
		budget = addDuration(budget, time.Until(start.AsTime()))
	}
	parent := ctx
	ctx, cancel := context.WithTimeout(parent, budget)
	defer cancel()
	// silent rewrites the error of a backend the deadline gave up on, so the
	// report says what happened rather than "context deadline exceeded".
	silent := func(err error) error {
		// The deadline shows up in two shapes: the context's own error, from
		// code that watched the context, and a gRPC status with the
		// DeadlineExceeded code, from an RPC the context ended -- which does
		// not wrap the context's error, so errors.Is alone would miss it.
		expired := errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded
		if err == nil || parent.Err() != nil || !expired {
			return err
		}
		return fmt.Errorf("no result %s after dispatch, for an execution planned to last %s: "+
			"the backend went silent or is far behind (%w)", budget, e.Duration, err)
	}

	if pool.Distributor != "" {
		conn, err := nh.Dial(ctx, pool.Distributor)
		if err != nil {
			return nil, nil, nil, err
		}
		defer conn.Close()
		// ForPool returns exactly one options for this path: the distributor
		// forwards it unchanged to every target. A target that fails does not
		// cost the others their results here either.
		targets, outputs, bad, err := nh.DistributePartial(ctx, conn, perBackend[0], pool.Targets)
		if err != nil {
			return nil, nil, nil, err
		}
		var failed []BackendError
		for _, b := range bad {
			failed = append(failed, BackendError{Addr: b.Target, Err: silent(b.Err)})
		}
		return targets, outputs, append(failed, overdue(e, runBound, targets, outputs, failed)...), nil
	}

	outputs := make([]*client.Output, len(addrs))
	errs := make([]error, len(addrs))
	var wg sync.WaitGroup
	for i, addr := range addrs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var conn *grpc.ClientConn
			var err error
			if r.DialTimeout > 0 {
				conn, err = nh.DialTimeout(ctx, addr, r.DialTimeout)
			} else {
				conn, err = nh.Dial(ctx, addr)
			}
			if err != nil {
				errs[i] = silent(err)
				return
			}
			defer conn.Close()
			var progress *nh.Progress
			if r.ProgressInterval > 0 && r.Observer != nil {
				progress = &nh.Progress{
					Interval: r.ProgressInterval,
					Fn: func(elapsed time.Duration, out *client.Output) {
						r.observerMu.Lock()
						defer r.observerMu.Unlock()
						r.Observer.ExecutionProgress(e, addr, elapsed, out)
					},
				}
			}
			resp, err := nh.Execute(ctx, conn, perBackend[i], progress)
			errs[i] = silent(err)
			// An engine that reports a failure still returns what it counted
			// (a run stopped by a failure predicate the plan asked for, say).
			// Keep it: the error says the run was not clean, the output says
			// what it did.
			if out := resp.GetOutput(); len(out.GetResults()) > 0 {
				outputs[i] = out
			} else if err == nil {
				// No error and nothing to judge: a failed backend all the
				// same. Left out silently, the other backends' thresholds
				// could pass a pool one member of which reported nothing.
				errs[i] = errors.New("returned no results")
			}
		}()
	}
	wg.Wait()

	var failed []BackendError
	for i, err := range errs {
		if err != nil {
			failed = append(failed, BackendError{Addr: addrs[i], Err: err})
		}
	}
	var gotAddrs []string
	var gotOutputs []*client.Output
	for i, out := range outputs {
		if out != nil {
			gotAddrs = append(gotAddrs, addrs[i])
			gotOutputs = append(gotOutputs, out)
		}
	}
	return addrs, outputs, append(failed, overdue(e, runBound, gotAddrs, gotOutputs, failed)...), nil
}

// overdue names the backends whose own account of the execution is far longer
// than the plan: a result that says it ran for twenty minutes when two were
// asked for. That is what a backend frozen mid-run and thawed later returns,
// and its counters describe a run nobody planned -- its rate over that span is
// a fraction of the target's, its latencies include the freeze. The result is
// kept, for whoever reads the report, and the backend is failed. One already
// in failed is not named twice.
func overdue(e compile.Execution, budget time.Duration, addrs []string, outputs []*client.Output, failed []BackendError) []BackendError {
	already := map[string]bool{}
	for _, f := range failed {
		already[f.Addr] = true
	}
	var out []BackendError
	for i, o := range outputs {
		if o == nil || already[addrs[i]] {
			continue
		}
		for _, res := range o.GetResults() {
			if res.GetName() != "global" {
				continue
			}
			if took := res.GetExecutionDuration().AsDuration(); took > budget {
				out = append(out, BackendError{Addr: addrs[i], Err: fmt.Errorf(
					"reported an execution of %s for one planned to last %s: it stalled, and its results are not those of the plan",
					took.Round(time.Millisecond), e.Duration)})
			}
		}
	}
	return out
}
