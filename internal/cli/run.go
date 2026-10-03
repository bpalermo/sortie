package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/bpalermo/sortie/internal/plan"
	"github.com/bpalermo/sortie/internal/report"
	"github.com/bpalermo/sortie/internal/run"
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
	cmd.Flags().StringVarP(&out, "output", "o", "", "write the report to this file instead of stdout")
	cmd.Flags().DurationVar(&progress, "progress", 0,
		"print each backend's progress this often while it runs, on stderr (0: only when it finishes)")
	return cmd
}

func runPlan(parent context.Context, path string, asJSON bool, out string, progress time.Duration, stdout, stderr io.Writer) error {
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

	runner := &run.Runner{Plan: p, Observer: report.Progress{W: stderr}, ProgressInterval: progress}
	r, runErr := runner.Run(ctx)
	if r == nil {
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
	if runErr != nil {
		return runErr
	}
	if !r.Pass {
		return errors.New("one or more thresholds failed")
	}
	return nil
}
