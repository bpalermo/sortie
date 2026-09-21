// Package cli assembles sortie's command tree.
package cli

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
)

// Exit codes. They are part of the contract with CI, so they are named here
// rather than scattered as literals.
const (
	exitOK       = 0
	exitFailed   = 1 // a threshold failed, or an execution errored
	exitBadUsage = 2 // the plan or the command line was invalid
)

// usageError marks a failure that is the caller's fault rather than the run's,
// so it can be given a different exit code. Cobra's own argument errors are
// translated into one too.
type usageError struct{ err error }

func (e *usageError) Error() string { return e.err.Error() }
func (e *usageError) Unwrap() error { return e.err }

func badUsage(format string, args ...any) error {
	return &usageError{fmt.Errorf(format, args...)}
}

// Execute runs the command tree against the process arguments and returns the
// exit code.
func Execute(version string) int {
	return execute(version, os.Args[1:], os.Stdout, os.Stderr)
}

// execute is Execute with its inputs and outputs supplied rather than taken
// from the process, so the exit codes -- which are a contract with CI, not an
// implementation detail -- can be tested.
func execute(version string, args []string, stdout, stderr io.Writer) int {
	root := newRootCmd(version)
	root.SetArgs(args)
	root.SetOut(stdout)
	root.SetErr(stderr)

	if err := root.Execute(); err != nil {
		fmt.Fprintf(stderr, "sortie: %v\n", err)
		var ue *usageError
		if errors.As(err, &ue) {
			return exitBadUsage
		}
		// An error raised before any subcommand could run -- an unknown
		// command, say -- is the caller's mistake, not a failed load test.
		// Cobra reports it as an ordinary error, which would otherwise exit 1
		// and be indistinguishable from a breached threshold.
		if _, _, findErr := root.Find(args); findErr != nil {
			return exitBadUsage
		}
		return exitFailed
	}
	return exitOK
}

func newRootCmd(version string) *cobra.Command {
	root := &cobra.Command{
		Use:   "sortie",
		Short: "Run declarative load-test plans on Nighthawk",
		Long: `sortie compiles a YAML plan into Nighthawk executions, dispatches them over
Nighthawk's gRPC control plane, and evaluates thresholds against the results.

Exit codes:
  0  every threshold held
  1  a threshold failed or an execution errored
  2  the plan or the command line was invalid`,
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,

		// Without this, cobra prints the help text for a bare `sortie` and
		// exits 0, which tells a script that a load test passed when none ran.
		RunE: func(cmd *cobra.Command, _ []string) error {
			_ = cmd.Help()
			return badUsage("a command is required")
		},
	}

	// Cobra reports a bad flag or a wrong argument count as an ordinary error,
	// which would exit 1 and be indistinguishable from a failed threshold.
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return &usageError{err}
	})

	root.AddCommand(newRunCmd(), newValidateCmd(), newCompileCmd())
	return root
}

// planArg builds a cobra Args function that requires exactly one plan file and
// reports a violation as a usage error.
func planArg() cobra.PositionalArgs {
	return func(_ *cobra.Command, args []string) error {
		if len(args) != 1 {
			return badUsage("expected exactly one plan file, got %d", len(args))
		}
		return nil
	}
}
