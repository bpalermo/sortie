// Package cli assembles sortie's command tree.
package cli

import (
	"errors"
	"fmt"
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

// Execute runs the command tree and returns the process exit code.
func Execute(version string) int {
	root := newRootCmd(version)
	if err := root.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "sortie: %v\n", err)
		var ue *usageError
		if errors.As(err, &ue) {
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
