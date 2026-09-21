package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/bpalermo/sortie/internal/plan"
	"github.com/bpalermo/sortie/internal/report"
	"github.com/bpalermo/sortie/internal/run"
)

func newRunCmd() *cobra.Command {
	var (
		asJSON bool
		out    string
	)

	cmd := &cobra.Command{
		Use:   "run <plan.yaml>",
		Short: "Run the plan and report a verdict",
		Args:  planArg(),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPlan(cmd.Context(), args[0], asJSON, out, cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "write the report as JSON")
	cmd.Flags().StringVarP(&out, "output", "o", "", "write the report to this file instead of stdout")
	return cmd
}

func runPlan(parent context.Context, path string, asJSON bool, out string, stdout, stderr io.Writer) error {
	p, err := plan.Load(path)
	if err != nil {
		return &usageError{err}
	}

	if parent == nil {
		parent = context.Background()
	}

	// Nighthawk cannot stop a run in flight (envoyproxy/nighthawk#380), so a
	// signal abandons the streams and leaves the backends generating load until
	// their configured duration elapses. Say so rather than implying a clean
	// stop. The notice is driven off the signal itself rather than off
	// ctx.Done, which also fires on the ordinary cancel at the end of a
	// successful run.
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	go func() {
		select {
		case <-signals:
			fmt.Fprintln(stderr,
				"\nsortie: interrupted; Nighthawk backends keep running until their configured duration elapses")
			cancel()
		case <-ctx.Done():
		}
	}()

	runner := &run.Runner{Plan: p, Observer: report.Progress{W: stderr}}
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
