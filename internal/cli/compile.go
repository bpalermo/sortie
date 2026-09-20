package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/bpalermo/sortie/internal/compile"
	"github.com/bpalermo/sortie/internal/plan"
)

func newCompileCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "compile <plan.yaml>",
		Short: "Print the CommandLineOptions the plan would send to each backend",
		Long: `Print the CommandLineOptions the plan would send to each backend.

This is the plan after defaults, executor expansion and rate division have been
applied, so it is the way to check what load a plan actually asks for without
generating any.`,
		Args: planArg(),
		RunE: func(cmd *cobra.Command, args []string) error {
			p, err := plan.Load(args[0])
			if err != nil {
				return &usageError{err}
			}
			w := cmd.OutOrStdout()
			marshal := protojson.MarshalOptions{Multiline: true, Indent: "  "}

			for _, s := range p.GetScenarios() {
				executions, err := compile.Expand(s)
				if err != nil {
					return &usageError{err}
				}
				pool := plan.PoolFor(p, s)
				for _, e := range executions {
					addrs, perBackend, err := compile.ForPool(e, pool)
					if err != nil {
						return &usageError{err}
					}
					for i, opts := range perBackend {
						if pool.GetDistributor() != "" {
							fmt.Fprintf(w, "# %s -> distributor %s, forwarded unchanged to %s\n",
								e.Label, pool.GetDistributor(), strings.Join(addrs, ", "))
						} else {
							fmt.Fprintf(w, "# %s -> %s (backend %d/%d)\n",
								e.Label, addrs[i], i+1, len(perBackend))
						}
						raw, err := marshal.Marshal(opts)
						if err != nil {
							return err
						}
						fmt.Fprintf(w, "%s\n\n", raw)
					}
				}
			}
			return nil
		},
	}
}
