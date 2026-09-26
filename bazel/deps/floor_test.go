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
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
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
	goSDKRe  = regexp.MustCompile(`go_sdk\.download\(\s*version\s*=\s*"(\d+)\.(\d+)(?:\.(\d+))?"`)
	xToolsRe = regexp.MustCompile(`golang\.org/x/tools\s+v(\d+)\.(\d+)\.(\d+)`)
)

func TestNogoXToolsFloor(t *testing.T) {
	module := readRepoFile(t, "MODULE.bazel")
	goMod := readRepoFile(t, "go.mod")

	sdk := matchVersion(t, goSDKRe, module, "go_sdk.download in MODULE.bazel")
	floor, known := xToolsFloors[sdk.minor]
	if !known {
		t.Fatalf("Go SDK is 1.%d but bazel/deps/floor_test.go has no x/tools floor for it.\n"+
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

// bazelOnlyRe matches a go.mod requirement annotated as needed by the build
// rather than by any Go source. The annotation is the convention that tells a
// reader why a requirement nothing imports is not dead weight.
var bazelOnlyRe = regexp.MustCompile(`(?m)^\s*(\S+)\s+(\S+)\s*//\s*bazel-only:.*$`)

var indirectRe = regexp.MustCompile(`//\s*indirect`)

// TestBazelOnlyRequirementsStayDirect guards the requirements that look unused
// and are not.
//
// A module named only by a BUILD or .bzl file has no Go import to justify it,
// so it reads as removable. It is not: `bazel mod tidy` rewrites use_repo from
// go.mod's DIRECT requirements and never reads BUILD files, so demoting one to
// indirect silently drops its use_repo entry and breaks the build somewhere
// that says nothing about go.mod.
//
// This checks every annotated requirement rather than a list of known ones, so
// the next one added is covered without anybody remembering to extend a test.
func TestBazelOnlyRequirementsStayDirect(t *testing.T) {
	matches := bazelOnlyRe.FindAllStringSubmatch(readRepoFile(t, "go.mod"), -1)
	if len(matches) == 0 {
		t.Skip("no requirements are annotated // bazel-only:")
	}

	for _, m := range matches {
		module, line := m[1], m[0]
		t.Run(module, func(t *testing.T) {
			if indirectRe.MatchString(line) {
				t.Fatalf("%s is annotated // bazel-only: but marked // indirect.\n"+
					"A bazel-only requirement must stay DIRECT: no Go source imports it, "+
					"so it looks removable, but a BUILD or .bzl file names it and "+
					"`bazel mod tidy` only exports use_repo entries for direct requirements.",
					module)
			}
		})
	}
}

// TestBazelOnlyRequirementsAreReferenced is the other half: an annotation that
// has outlived the BUILD file that needed it turns into a direct requirement
// nothing uses, which is the thing the annotation exists to rule out.
//
// Only annotated modules are checked. The converse -- every direct requirement
// with no Go import must be annotated -- would be stronger, and is deliberately
// not done here: it would fail the build on a heuristic about what counts as a
// reference.
func TestBazelOnlyRequirementsAreReferenced(t *testing.T) {
	matches := bazelOnlyRe.FindAllStringSubmatch(readRepoFile(t, "go.mod"), -1)
	if len(matches) == 0 {
		t.Skip("no requirements are annotated // bazel-only:")
	}
	starlark := readStarlarkFiles(t)

	for _, m := range matches {
		module := m[1]
		t.Run(module, func(t *testing.T) {
			repo := gazelleRepoName(module)
			for _, content := range starlark {
				if strings.Contains(content, repo) {
					return
				}
			}
			t.Fatalf("%s is annotated // bazel-only: but no hand-written Bazel file "+
				"mentions %q.\n"+
				"Either the reference was removed, in which case move the requirement to "+
				"the indirect block, or it moved to a generated BUILD file, in which case "+
				"the annotation is wrong.", module, repo)
		})
	}
}

// gazelleRepoName reproduces the repository name gazelle derives from a module
// path: the host reversed, then the remaining path elements, with everything
// that is not alphanumeric replaced by an underscore.
//
//	google.golang.org/genproto/googleapis/rpc -> org_golang_google_genproto_googleapis_rpc
func gazelleRepoName(module string) string {
	parts := strings.Split(module, "/")
	host := strings.Split(parts[0], ".")
	slices.Reverse(host)

	segments := append(host, parts[1:]...)
	name := strings.Join(segments, "_")
	return nonAlphanumeric.ReplaceAllString(strings.ToLower(name), "_")
}

var nonAlphanumeric = regexp.MustCompile(`[^a-z0-9]`)

// readStarlarkFiles returns the contents of the hand-written BUILD and .bzl
// files, which are the only ones that name an external repository directly:
// the rest are generated by Gazelle, which resolves repositories itself.
//
// MODULE.bazel is deliberately excluded. Its use_repo list is derived from the
// very requirement being checked, so a match there would be circular and the
// check would pass even after the last real reference was deleted.
func readStarlarkFiles(t *testing.T) []string {
	t.Helper()
	var out []string
	for _, root := range repoRoots() {
		_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil //nolint:nilerr // an unreadable tree is simply not searched
			}
			name := d.Name()
			if name == "MODULE.bazel" {
				return nil
			}
			if !strings.HasSuffix(name, ".bzl") &&
				!strings.HasSuffix(name, ".BUILD") && name != "BUILD.bazel" {
				return nil
			}
			if raw, err := os.ReadFile(path); err == nil {
				out = append(out, string(raw))
			}
			return nil
		})
	}
	if len(out) == 0 {
		t.Fatal("found no Bazel files to search")
	}
	return out
}

// readRepoFile finds a file at the repository root. Under `bazel test` the
// working directory is the runfiles tree; under `go test` it is the package
// directory. Both are handled so the test is not tied to one runner.
func readRepoFile(t *testing.T, name string) string {
	t.Helper()
	var candidates []string
	for _, root := range repoRoots() {
		candidates = append(candidates, filepath.Join(root, name))
	}
	for _, path := range candidates {
		if raw, err := os.ReadFile(path); err == nil {
			return string(raw)
		}
	}
	t.Fatalf("could not find %s; looked in %v", name, candidates)
	return ""
}

// repoRoots lists where the repository root may be. Under `bazel test` the
// working directory is the runfiles tree; under `go test` it is the package
// directory. Both are handled so the test is not tied to one runner.
func repoRoots() []string {
	roots := []string{".", filepath.Join("..", "..")}
	if dir := os.Getenv("BUILD_WORKSPACE_DIRECTORY"); dir != "" {
		roots = append(roots, dir)
	}
	return roots
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
