package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
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
		stream   string
		progress time.Duration
	)

	cmd := &cobra.Command{
		Use:   "run <plan.yaml>",
		Short: "Run the plan and report a verdict",
		Args:  planArg(),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPlan(cmd.Context(), args[0], asJSON, out, stream, progress, cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "write the report as JSON")
	cmd.Flags().StringVarP(&out, "output", "o", "", "write the report to this file; stdout then gets the text summary")
	cmd.Flags().StringVar(&stream, "results-stream", "",
		"append each execution's result to this file as it finishes, one JSON object per line: "+
			"the objects of the JSON report's \"executions\"")
	cmd.Flags().DurationVar(&progress, "progress", 0,
		"print each backend's progress this often while it runs, on stderr (0: only each execution's verdict when it finishes)")
	return cmd
}

// syncedFile writes through to a file and asks for each write to reach the
// disk: the results stream exists for the run that does not end well, and a
// line still in a page cache when the node goes is a line nobody reads. The
// sync is best effort -- a pipe or a device has nothing to sync and says so
// with an error that is not a failure to write.
type syncedFile struct{ f *os.File }

func (s syncedFile) Write(b []byte) (int, error) {
	n, err := s.f.Write(b)
	if err == nil {
		_ = s.f.Sync()
	}
	return n, err
}

// openStream opens the results stream for appending. Appending, where --output
// truncates: what is in the file may be all that is left of a run that died,
// and the retry of that run must not be what erases it. The lines of two runs
// are told apart by their started_at.
func openStream(path, out string) (*os.File, error) {
	// stdout is the report's: a JSON document, or the text summary. Lines of
	// JSON interleaved with either would leave it something no parser reads.
	if path == "-" {
		return nil, badUsage("--results-stream needs a file: stdout carries the report, and the two cannot share it")
	}
	if out != "" && filepath.Clean(path) == filepath.Clean(out) {
		return nil, badUsage("--results-stream and --output name the same file, %s", path)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o666)
	if err != nil {
		// Before any load: a stream that cannot be opened is a command line
		// that cannot be honoured, better said now than after the run.
		return nil, badUsage("--results-stream: %w", err)
	}
	return f, nil
}

func runPlan(parent context.Context, path string, asJSON bool, out, stream string, progress time.Duration, stdout, stderr io.Writer) error {
	if progress < 0 {
		return &usageError{fmt.Errorf("--progress must not be negative (got %s)", progress)}
	}
	p, err := plan.Load(path)
	if err != nil {
		return &usageError{err}
	}

	// The observer narrates on stderr and, when asked, also keeps the stream.
	// The runner calls it from one goroutine at a time, so neither locks.
	var observer run.Observer = report.Progress{W: stderr}
	if stream != "" {
		f, err := openStream(stream, out)
		if err != nil {
			return err
		}
		defer func() {
			if err := f.Close(); err != nil {
				fmt.Fprintf(stderr, "sortie: results stream: %v\n", err)
			}
		}()
		observer = report.Observers{observer, report.Stream{
			W: syncedFile{f},
			// Reported and no more: the run and its report do not depend on
			// the stream, and the next line is tried all the same.
			Failed: func(label string, err error) {
				fmt.Fprintf(stderr, "sortie: results stream: %s was not written: %v\n", label, err)
			},
		}}
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
		Plan: p, Observer: observer, ProgressInterval: progress,
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
