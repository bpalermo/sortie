package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/bpalermo/sortie/internal/compile"
	"github.com/bpalermo/sortie/internal/plan"
)

func newValidateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate <plan.yaml>",
		Short: "Check the plan without running it",
		Args:  planArg(),
		RunE: func(cmd *cobra.Command, args []string) error {
			p, err := plan.Load(args[0])
			if err != nil {
				return &usageError{err}
			}
			total := 0
			for _, s := range p.Scenarios {
				executions, err := compile.Expand(s)
				if err != nil {
					return &usageError{err}
				}
				total += len(executions)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "ok: %d scenarios, %d executions, %d pools\n",
				len(p.Scenarios), total, len(p.Pools))
			return nil
		},
	}
}
