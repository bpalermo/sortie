// Package stamp_test enforces that the binary can say what it is.
//
// sortie reports a verdict on someone else's performance. The first question
// asked of a surprising number is which build produced it, and a binary that
// answers "dev" cannot participate in that conversation. The plumbing for this
// lives in three places that have to agree -- a linker-set variable in main, an
// x_defs entry naming it, and `build --stamp` -- and none of them fails loudly
// when one goes missing: the binary just quietly reports "dev" again. This was
// found by running sortie in a pod, which is a slow way to learn it.
package stamp_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The symbol x_defs must set: the importpath of the package declaring the
// variable, then the variable name.
const versionSymbol = "github.com/bpalermo/sortie/cmd/sortie.version"

// read resolves a repository-relative path. Under `bazel test` the working
// directory is the runfiles tree; under `go test` it is the package directory.
// Both are handled so the test is not tied to one runner -- the same approach
// //tools/deps:deps_test takes.
func read(t *testing.T, name string) string {
	t.Helper()
	roots := []string{".", filepath.Join("..", "..")}
	if dir := os.Getenv("BUILD_WORKSPACE_DIRECTORY"); dir != "" {
		roots = append(roots, dir)
	}
	var tried []string
	for _, root := range roots {
		path := filepath.Join(root, name)
		tried = append(tried, path)
		if b, err := os.ReadFile(path); err == nil {
			return string(b)
		}
	}
	t.Fatalf("could not find %s; looked in %v", name, tried)
	return ""
}

// TestVersionIsStamped checks the three parts together, because each is useless
// alone and each looks like dead weight to someone tidying up.
func TestVersionIsStamped(t *testing.T) {
	build := read(t, "cmd/sortie/BUILD.bazel")
	if !strings.Contains(build, versionSymbol) {
		t.Errorf("cmd/sortie/BUILD.bazel does not set %q via x_defs;\n"+
			"without it the binary reports the %q default and cannot identify itself",
			versionSymbol, "dev")
	}
	// A stamped value, not a literal. A hardcoded version is worse than "dev":
	// it is wrong rather than obviously absent.
	if strings.Contains(build, versionSymbol) &&
		!regexp.MustCompile(`\{STABLE_[A-Z_]+\}`).MatchString(build) {
		t.Errorf("cmd/sortie/BUILD.bazel sets %q to something other than a {STABLE_*} "+
			"workspace-status key; a literal version goes stale silently", versionSymbol)
	}

	// Volatile keys do not invalidate the actions that embed them, so a version
	// stamped from one can be served stale from cache.
	if regexp.MustCompile(`x_defs.*\{(?:GIT_COMMIT|GIT_BRANCH)\}`).MatchString(build) {
		t.Error("cmd/sortie/BUILD.bazel stamps from an unprefixed workspace-status key; " +
			"those are volatile and do not invalidate the action that embeds them")
	}

	if main := read(t, "cmd/sortie/main.go"); !strings.Contains(main, "var version") {
		t.Error("cmd/sortie/main.go no longer declares `version`; x_defs sets a symbol " +
			"that must exist, and a rename makes the stamping silently do nothing")
	}

	if rc := read(t, ".bazelrc"); !strings.Contains(rc, "--stamp") {
		t.Error(".bazelrc no longer sets --stamp; x_defs placeholders are then left " +
			"unsubstituted or defaulted")
	}
}
