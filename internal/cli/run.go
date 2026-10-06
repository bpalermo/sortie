package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/bpalermo/sortie/internal/plan"
	"github.com/bpalermo/sortie/internal/report"
	"github.com/bpalermo/sortie/internal/run"
)

// resolver looks dns pools up when a run starts, and resolveTimeout bounds
// the wait for a name that answers with nothing (zero: the runner's default).
// Package variables so the tests can substitute a fake and neither need a
// network nor wait out the grace period.
var (
	resolver       plan.Resolver = net.DefaultResolver
	resolveTimeout time.Duration
)

func newRunCmd() *cobra.Command {
	var (
		asJSON   bool
		out      string
		progress time.Duration
	)

	cmd := &cobra.Command{
		Use:   "run <plan.yaml>",
		Short: "Run the plan and report a verdict",
		Args:  planArg(),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPlan(cmd.Context(), args[0], asJSON, out, progress, cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "write the report as JSON")
	cmd.Flags().StringVarP(&out, "output", "o", "", "write the report to this file; stdout then gets the text summary")
	cmd.Flags().DurationVar(&progress, "progress", 0,
		"print each backend's progress this often while it runs, on stderr (0: only when it finishes)")
	return cmd
}

func runPlan(parent context.Context, path string, asJSON bool, out string, progress time.Duration, stdout, stderr io.Writer) error {
	if progress < 0 {
		return &usageError{fmt.Errorf("--progress must not be negative (got %s)", progress)}
	}
	p, err := plan.Load(path)
	if err != nil {
		return &usageError{err}
	}

	if parent == nil {
		parent = context.Background()
	}

	// A signal cancels ctx, which sends each backend a CancellationRequest and
	// waits for its partial response (see nh.Execute). The notice is driven off
	// the signal itself rather than off ctx.Done, which also fires on the
	// ordinary cancel at the end of a successful run.
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	go func() {
		select {
		case <-signals:
			fmt.Fprintln(stderr, "\nsortie: interrupted; cancelling the backends' executions")
			cancel()
		case <-ctx.Done():
		}
	}()

	runner := &run.Runner{
		Plan: p, Observer: report.Progress{W: stderr}, ProgressInterval: progress,
		Resolver: resolver, ResolveTimeout: resolveTimeout,
	}
	r, runErr := runner.Run(ctx)
	if r == nil {
		// A dns pool that resolved to nothing failed before any load: the
		// plan names something the environment does not have, which is the
		// caller's problem, not the run's result.
		var re *plan.ResolveError
		if errors.As(runErr, &re) {
			return &usageError{runErr}
		}
		return runErr
	}

	w := stdout
	if out != "" {
		f, err := os.Create(out)
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}

	if asJSON {
		err = report.JSON(w, r)
	} else {
		err = report.Text(w, r)
	}
	if err != nil {
		return err
	}
	// With the report in a file, stdout still gets the readable summary: in a
	// pod that is the log, and a run whose only record is a file on a volume
	// nobody mounted afterwards has no record at all.
	if out != "" {
		if err := report.Text(stdout, r); err != nil {
			return err
		}
	}
	if runErr != nil {
		return runErr
	}
	if !r.Pass {
		return errors.New("one or more thresholds failed")
	}
	return nil
}
