// Command sortie runs declarative load-test plans against Nighthawk's gRPC
// control plane and turns the results into a pass/fail verdict.
package main

import (
	"os"

	"github.com/bpalermo/sortie/internal/cli"
)

// version is overridden at link time with -X main.version=...
var version = "dev"

func main() {
	os.Exit(cli.Execute(version))
}
