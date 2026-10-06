// Package run executes a plan against Nighthawk backends and collects verdicts.
package run

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	"net"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

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

	// Err is set when the execution itself failed, as opposed to failing a
	// threshold.
	Err error
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

		for _, e := range executions {
			er := r.runExecution(ctx, e, pool, thresholds)
			if !er.Pass {
				report.Pass = false
			}
			report.Executions = append(report.Executions, er)
			if r.Observer != nil {
				r.Observer.ExecutionFinished(er)
			}
			if er.Err != nil && ctx.Err() != nil {
				return report, ctx.Err()
			}
		}
	}
	return report, nil
}

// resolve returns the plan with its dns pools resolved. Each name is looked
// up until it answers with at least one address, for up to ResolveTimeout of
// its own -- lookups in flight included -- and is then left alone: a name that
// has answered is not asked again while another is still empty, so its
// backends are the ones it gave the first time. Every other failure -- a name
// that does not exist, a resolver error -- is returned at once; so is an empty
// answer once its wait is over, as the same ResolveError. A context cancelled
// during a wait ends it with the context's error.
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
		r.Observer.ExecutionStarted(e, backends)
	}

	addrs, outputs, err := r.dispatch(ctx, e, pool)
	er.Elapsed = time.Since(er.Started)
	if err != nil {
		er.Err = err
		return er
	}

	set, err := result.NewSet(addrs, outputs)
	if err != nil {
		er.Err = err
		return er
	}
	er.Set = set
	er.Outcomes, er.Pass = set.Evaluate(thresholds)
	return er
}

func (r *Runner) dispatch(
	ctx context.Context,
	e compile.Execution,
	pool *plan.Pool,
) ([]string, []*client.Output, error) {
	addrs, perBackend, err := compile.ForPool(e, pool)
	if err != nil {
		return nil, nil, err
	}

	if pool.Distributor != "" {
		conn, err := nh.Dial(ctx, pool.Distributor)
		if err != nil {
			return nil, nil, err
		}
		defer conn.Close()
		// ForPool returns exactly one options for this path: the distributor
		// forwards it unchanged to every target.
		return nh.Distribute(ctx, conn, perBackend[0], pool.Targets)
	}

	outputs := make([]*client.Output, len(addrs))
	var mu sync.Mutex
	group, gctx := errgroup.WithContext(ctx)

	for i, addr := range addrs {
		group.Go(func() error {
			conn, err := nh.Dial(gctx, addr)
			if err != nil {
				return err
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
			resp, err := nh.Execute(gctx, conn, perBackend[i], progress)
			if err != nil {
				return fmt.Errorf("backend %s: %w", addr, err)
			}
			mu.Lock()
			outputs[i] = resp.GetOutput()
			mu.Unlock()
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, nil, err
	}
	return addrs, outputs, nil
}
