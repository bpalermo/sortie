// Command chartconfig reads a packaged Helm chart (the .tgz `helm package`
// writes) and emits the OCI config blob `helm push` would attach to it, plus
// the chart's name and version.
//
// Why it exists. The chart is published to Quay, which has no nested
// repositories, under a flat `chart-<name>` repository that `helm push` cannot
// address (it always appends Chart.yaml's `name:`). chart_push in
// //bazel/helm:defs.bzl therefore writes the same OCI artifact helm would --
// the .tgz as an `application/vnd.cncf.helm.chart.content.v1.tar+gzip` layer
// and the chart metadata as an `application/vnd.cncf.helm.config.v1+json`
// config -- with `oras push`, naming the repository outright. helm builds that
// config as json.Marshal(chart.Metadata), whose JSON keys are Chart.yaml's own
// keys; converting Chart.yaml from YAML to JSON yields the same document (up
// to key order and omitted empties, which no reader depends on), and
// `helm pull oci://...` reads it back unchanged.
//
// It runs as a build action on the STAMPED package, so the version it reports
// is the one the push publishes under. It does not validate the version as an
// OCI tag: an unstamped build is legal to build, and chart_push refuses to
// push a tag that is not one.
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	in := flag.String("chart", "", "the packaged chart (.tgz) to read")
	configOut := flag.String("config_out", "", "where to write the helm OCI config (Chart.yaml as JSON)")
	metaOut := flag.String("meta_out", "", "where to write `<name> <version>` (one line)")
	flag.Parse()
	if *in == "" || *configOut == "" || *metaOut == "" {
		fmt.Fprintln(os.Stderr, "usage: chartconfig -chart <pkg.tgz> -config_out <file> -meta_out <file>")
		os.Exit(2)
	}
	if err := run(*in, *configOut, *metaOut); err != nil {
		fmt.Fprintf(os.Stderr, "chartconfig: %s: %v\n", *in, err)
		os.Exit(1)
	}
}

func run(in, configOut, metaOut string) error {
	f, err := os.Open(in)
	if err != nil {
		return err
	}
	defer f.Close()
	chart, err := Read(f)
	if err != nil {
		return err
	}
	if err := os.WriteFile(configOut, chart.Config, 0o644); err != nil {
		return err
	}
	return os.WriteFile(metaOut, fmt.Appendf(nil, "%s %s\n", chart.Name, chart.Version), 0o644)
}
