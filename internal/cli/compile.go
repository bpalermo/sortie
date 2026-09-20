package cli

import (
	"fmt"

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

			for _, s := range p.Scenarios {
				executions, err := compile.Expand(s)
				if err != nil {
					return &usageError{err}
				}
				pool := p.PoolFor(s)
				for _, e := range executions {
					n := len(pool.Services)
					if pool.Distributor != "" {
						n = 1
					}
					perBackend, err := compile.Divide(e, n)
					if err != nil {
						return &usageError{err}
					}
					for i, opts := range perBackend {
						fmt.Fprintf(w, "# %s -> backend %d/%d\n", e.Label, i+1, len(perBackend))
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
