// Package deps_test enforces dependency invariants that nothing else checks.
//
// These are version relationships the build cannot express and a comment cannot
// enforce: a requirement that looks unused, or an "indirect" line whose only job
// is to raise a resolution floor, is exactly the kind of thing a future cleanup
// deletes. A failing test is a gate; a comment asking people not to touch
// something is decoration.
package deps_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
)

// xToolsFloors maps a Go SDK minor version to the minimum golang.org/x/tools
// that nogo needs on it.
//
// nogo's analyzers are compiled from @org_golang_x_tools//go/analysis/..., and
// gazelle's go_deps resolves a single x/tools across every bzlmod module by
// MVS. rules_go pins an older one in its own go.mod, so this repository's go.mod
// is what decides the version the analyzers are built against. Too old and every
// Go compile fails with "export data version N is greater than maximum supported
// version M" rather than anything mentioning x/tools.
//
// Add an entry when the SDK moves to a new minor version.
var xToolsFloors = map[int]version{
	// Go 1.27 emits export data version 4. x/tools gained support in v0.48.0
	// (bazel-contrib/rules_go#4701), which postdates rules_go 0.63.0.
	27: {0, 48, 0},
}

type version struct{ major, minor, patch int }

func (v version) String() string { return fmt.Sprintf("v%d.%d.%d", v.major, v.minor, v.patch) }

func (v version) less(o version) bool {
	if v.major != o.major {
		return v.major < o.major
	}
	if v.minor != o.minor {
		return v.minor < o.minor
	}
	return v.patch < o.patch
}

var (
	goSDKRe   = regexp.MustCompile(`go_sdk\.download\(\s*version\s*=\s*"(\d+)\.(\d+)(?:\.(\d+))?"`)
	xToolsRe  = regexp.MustCompile(`golang\.org/x/tools\s+v(\d+)\.(\d+)\.(\d+)`)
	genprotoR = regexp.MustCompile(`(?m)^\s*google\.golang\.org/genproto/googleapis/rpc\s+\S+(.*)$`)
)

func TestNogoXToolsFloor(t *testing.T) {
	module := readRepoFile(t, "MODULE.bazel")
	goMod := readRepoFile(t, "go.mod")

	sdk := matchVersion(t, goSDKRe, module, "go_sdk.download in MODULE.bazel")
	floor, known := xToolsFloors[sdk.minor]
	if !known {
		t.Fatalf("Go SDK is 1.%d but tools/deps/floor_test.go has no x/tools floor for it.\n"+
			"Look up the minimum golang.org/x/tools that supports this SDK's export data "+
			"version and add it to xToolsFloors, or nogo will fail every Go compile with a "+
			"message that never mentions x/tools.", sdk.minor)
	}

	got := matchVersion(t, xToolsRe, goMod, "golang.org/x/tools in go.mod")
	if got.less(floor) {
		t.Fatalf("golang.org/x/tools is %s but the Go %d.%d SDK needs at least %s.\n"+
			"This requirement exists only to raise the MVS floor that nogo's analyzers are "+
			"built against; nothing imports x/tools. Raise it rather than removing it.",
			got, sdk.major, sdk.minor, floor)
	}
}

// TestGenprotoStaysDirect guards a requirement that looks unused and is not.
//
// bazel/nighthawk_api.BUILD names @org_golang_google_genproto_googleapis_rpc so
// that google/rpc/status.proto's Go code is the same package grpc-go links.
// `bazel mod tidy` rewrites use_repo from go.mod's DIRECT requirements and never
// reads BUILD files, so demoting this to indirect drops the use_repo entry and
// breaks the build.
func TestGenprotoStaysDirect(t *testing.T) {
	goMod := readRepoFile(t, "go.mod")

	m := genprotoR.FindStringSubmatch(goMod)
	if m == nil {
		t.Fatal("google.golang.org/genproto/googleapis/rpc is missing from go.mod; " +
			"bazel/nighthawk_api.BUILD needs it for google/rpc/status.proto's Go code")
	}
	if trailer := m[1]; regexp.MustCompile(`//\s*indirect`).MatchString(trailer) {
		t.Fatal("google.golang.org/genproto/googleapis/rpc must stay a DIRECT requirement.\n" +
			"No Go source imports it, so it looks removable, but bazel/nighthawk_api.BUILD " +
			"names it and `bazel mod tidy` only exports use_repo entries for direct " +
			"requirements. Keep it direct with its // bazel-only: annotation.")
	}
}

// readRepoFile finds a file at the repository root. Under `bazel test` the
// working directory is the runfiles tree; under `go test` it is the package
// directory. Both are handled so the test is not tied to one runner.
func readRepoFile(t *testing.T, name string) string {
	t.Helper()
	candidates := []string{name, filepath.Join("..", "..", name)}
	if dir := os.Getenv("BUILD_WORKSPACE_DIRECTORY"); dir != "" {
		candidates = append(candidates, filepath.Join(dir, name))
	}
	for _, path := range candidates {
		if raw, err := os.ReadFile(path); err == nil {
			return string(raw)
		}
	}
	t.Fatalf("could not find %s; looked in %v", name, candidates)
	return ""
}

func matchVersion(t *testing.T, re *regexp.Regexp, haystack, what string) version {
	t.Helper()
	m := re.FindStringSubmatch(haystack)
	if m == nil {
		t.Fatalf("could not find %s", what)
	}
	atoi := func(s string) int {
		if s == "" {
			return 0
		}
		n, err := strconv.Atoi(s)
		if err != nil {
			t.Fatalf("bad version component %q in %s: %v", s, what, err)
		}
		return n
	}
	return version{atoi(m[1]), atoi(m[2]), atoi(m[3])}
}
