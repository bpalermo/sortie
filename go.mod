module github.com/bpalermo/sortie

// Bazel is the only supported build. Nighthawk's protos are generated from a
// pinned archive into Bazel-only packages under github.com/envoyproxy/nighthawk/api/...,
// which no Go module publishes, so `go build` and `go mod tidy` cannot resolve
// them. This file exists for gazelle's go_deps.from_file and for editor
// tooling: it lists the third-party modules and nothing else.

go 1.27.1

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.12-20260825204119-511051f7f437.1 // bazel-only: referenced from BUILD/.bzl, no Go import
	github.com/envoyproxy/go-control-plane/envoy v1.39.0
	github.com/spf13/cobra v1.10.1
	golang.org/x/sync v0.22.0
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260918162117-cecb64721679 // bazel-only: referenced from BUILD/.bzl, no Go import
	google.golang.org/grpc v1.84.0
	google.golang.org/protobuf v1.36.12
	gopkg.in/yaml.v3 v3.0.1
)

require (
	buf.build/go/protovalidate v1.4.0
	buf.build/go/protoyaml v0.7.0
	cel.dev/cel-go v0.32.0 // indirect
	cel.dev/expr v0.25.3 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/cncf/xds/go v0.0.0-20260202195803-dba9d589def2 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.3.3 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/exp v0.0.0-20260820142414-ca536658362e // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	// nogo's analyzers (TOOLS_NOGO) are @org_golang_x_tools//go/analysis/...;
	// go_deps resolves one x/tools across all bzlmod modules by MVS, and this
	// go.mod is what sets our floor. rules_go 0.63.0's own go.mod pins x/tools
	// v0.34.0, whose export data reader tops out at version 2 and cannot read
	// Go 1.27's version 4 ("export data version 4 is greater than maximum
	// supported version 2"); >= v0.48.0 fixes that (bazel-contrib/rules_go#4701,
	// which postdates rules_go 0.63.0 and only bumped rules_go's own go.mod).
	// Nothing here imports x/tools directly - this line exists purely to
	// raise the MVS floor, so it stays indirect.
	golang.org/x/tools v0.49.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260819154853-08b0e4226688 // indirect
)
