// Package run executes a plan against Nighthawk backends and collects verdicts.
package run

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	client "github.com/envoyproxy/nighthawk/api/client"
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
	Duration time.Duration
	RampTime time.Duration
	Started  time.Time
	Elapsed  time.Duration

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

	// Observer, when set, is notified as executions start and finish so a CLI
	// can report progress during a run that produces no output until it ends.
	Observer Observer
}

// Observer receives progress callbacks.
type Observer interface {
	ExecutionStarted(e compile.Execution, backends []string)
	ExecutionFinished(r ExecutionReport)
}

// Run executes every scenario in plan order and returns the collected report.
// Scenarios run sequentially: two scenarios sharing a pool would otherwise
// contend for the same Nighthawk instances, which accept a single execution at
// a time, and two scenarios on different pools would still distort each
// other's latency measurements if they share a target.
func (r *Runner) Run(ctx context.Context) (*Report, error) {
	report := &Report{Pass: true}

	for _, scenario := range r.Plan.Scenarios {
		executions, err := compile.Expand(scenario)
		if err != nil {
			return nil, err
		}
		thresholds, err := threshold.ParseAll(r.Plan.EffectiveThresholds(scenario))
		if err != nil {
			return nil, fmt.Errorf("scenario %q: %w", scenario.Name, err)
		}
		pool := r.Plan.PoolFor(scenario)

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

func (r *Runner) runExecution(
	ctx context.Context,
	e compile.Execution,
	pool plan.Pool,
	thresholds []threshold.Threshold,
) ExecutionReport {
	er := ExecutionReport{
		Label:    e.Label,
		Scenario: e.Scenario.Name,
		Pool:     pool.Name,
		Rate:     e.Rate,
		Duration: e.Duration,
		RampTime: e.RampTime,
		Started:  time.Now(),
	}

	backends := pool.Services
	if pool.Distributor != "" {
		backends = pool.Targets
	}
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
	pool plan.Pool,
) ([]string, []*client.Output, error) {
	if pool.Distributor != "" {
		conn, err := nh.Dial(ctx, pool.Distributor)
		if err != nil {
			return nil, nil, err
		}
		defer conn.Close()
		return nh.Distribute(ctx, conn, e.Options, pool.Targets)
	}

	perBackend, err := compile.Divide(e, len(pool.Services))
	if err != nil {
		return nil, nil, err
	}

	outputs := make([]*client.Output, len(pool.Services))
	var mu sync.Mutex
	group, gctx := errgroup.WithContext(ctx)

	for i, addr := range pool.Services {
		group.Go(func() error {
			conn, err := nh.Dial(gctx, addr)
			if err != nil {
				return err
			}
			defer conn.Close()
			resp, err := nh.Execute(gctx, conn, perBackend[i])
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
	return pool.Services, outputs, nil
}
