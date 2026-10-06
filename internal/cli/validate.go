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
			// Expanding alone would accept a plan that run refuses: the rate
			// division happens at dispatch, so validate has to go through the
			// same path to be worth anything.
			total := 0
			for _, s := range p.GetScenarios() {
				executions, err := compile.Expand(s)
				if err != nil {
					return &usageError{err}
				}
				pool := plan.PoolFor(p, s)
				for _, e := range executions {
					if _, _, err := compile.ForPool(e, pool); err != nil {
						return &usageError{err}
					}
				}
				total += len(executions)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "ok: %d scenarios, %d executions, %d pools%s\n",
				len(p.GetScenarios()), total, len(p.GetPools()), dnsNote(p))
			return nil
		},
	}
}

// dnsNote says how many pools validate took on trust: a dns pool's backends
// exist only once the run resolves the name, so their number -- and with it
// the one rule that depends on it -- is checked then, not here.
func dnsNote(p *plan.Plan) string {
	n := 0
	for _, pool := range p.GetPools() {
		if compile.Unresolved(pool) {
			n++
		}
	}
	if n == 0 {
		return ""
	}
	return fmt.Sprintf(" (%d resolved from DNS when the run starts)", n)
}
