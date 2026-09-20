// Command sortie runs declarative load-test plans against Nighthawk's gRPC
// control plane and turns the results into a pass/fail verdict.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/bpalermo/sortie/internal/compile"
	"github.com/bpalermo/sortie/internal/plan"
	"github.com/bpalermo/sortie/internal/report"
	"github.com/bpalermo/sortie/internal/run"
)

// version is overridden at link time with -X main.version=...
var version = "dev"

const usage = `sortie runs declarative load-test plans on Nighthawk.

Usage:
  sortie run <plan.yaml> [-json] [-o FILE]   run the plan and report a verdict
  sortie validate <plan.yaml>                check the plan without running it
  sortie compile <plan.yaml>                 print the CommandLineOptions it would send
  sortie version                             print the version

Exit codes:
  0  every threshold held
  1  a threshold failed or an execution errored
  2  the plan or the command line was invalid
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "run":
		err = cmdRun(os.Args[2:])
	case "validate":
		err = cmdValidate(os.Args[2:])
	case "compile":
		err = cmdCompile(os.Args[2:])
	case "version":
		fmt.Println(version)
		return
	case "-h", "--help", "help":
		fmt.Print(usage)
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", os.Args[1], usage)
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "sortie: %v\n", err)
		var ue *usageError
		if errors.As(err, &ue) {
			os.Exit(2)
		}
		os.Exit(1)
	}
}

type usageError struct{ err error }

func (e *usageError) Error() string { return e.err.Error() }
func (e *usageError) Unwrap() error { return e.err }

func badUsage(format string, args ...any) error {
	return &usageError{fmt.Errorf(format, args...)}
}

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "write the report as JSON")
	out := fs.String("o", "", "write the report to this file instead of stdout")
	if err := fs.Parse(args); err != nil {
		return &usageError{err}
	}
	if fs.NArg() != 1 {
		return badUsage("run takes exactly one plan file")
	}

	p, err := plan.Load(fs.Arg(0))
	if err != nil {
		return &usageError{err}
	}

	// Nighthawk cannot stop a run in flight (envoyproxy/nighthawk#380), so a
	// signal abandons the streams and leaves the backends generating load until
	// their configured duration elapses. Say so rather than implying a clean
	// stop. The notice is driven off the signal itself rather than off ctx.Done,
	// which also fires on the ordinary cancel at the end of a successful run.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	go func() {
		select {
		case <-signals:
			fmt.Fprintln(os.Stderr,
				"\nsortie: interrupted; Nighthawk backends keep running until their configured duration elapses")
			cancel()
		case <-ctx.Done():
		}
	}()

	runner := &run.Runner{Plan: p, Observer: report.Progress{W: os.Stderr}}
	r, runErr := runner.Run(ctx)
	if r == nil {
		return runErr
	}

	w := os.Stdout
	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}

	if *asJSON {
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

func cmdValidate(args []string) error {
	if len(args) != 1 {
		return badUsage("validate takes exactly one plan file")
	}
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
	fmt.Printf("ok: %d scenarios, %d executions, %d pools\n",
		len(p.Scenarios), total, len(p.Pools))
	return nil
}

func cmdCompile(args []string) error {
	if len(args) != 1 {
		return badUsage("compile takes exactly one plan file")
	}
	p, err := plan.Load(args[0])
	if err != nil {
		return &usageError{err}
	}
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
				fmt.Printf("# %s -> backend %d/%d\n", e.Label, i+1, len(perBackend))
				raw, err := marshal.Marshal(opts)
				if err != nil {
					return err
				}
				fmt.Printf("%s\n\n", raw)
			}
		}
	}
	return nil
}
