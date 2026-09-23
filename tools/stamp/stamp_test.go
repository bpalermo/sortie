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

// The exact value it must be assigned. Asserting on the value rather than on
// the file's contents is deliberate: a check that the file mentions some
// {STABLE_*} placeholder somewhere passes for a hardcoded version as soon as
// any other stamped attribute exists, and a check that it does not mention
// {GIT_COMMIT} is a blocklist that any other volatile key walks past. Naming
// the one correct value covers every wrong one.
const versionValue = "{STABLE_GIT_VERSION}"

// xDefValue captures what x_defs assigns to versionSymbol.
var xDefValue = regexp.MustCompile(regexp.QuoteMeta(`"`+versionSymbol+`"`) + `\s*:\s*"([^"]*)"`)

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

// TestVersionIsStamped checks the parts together, because each is useless alone
// and each looks like dead weight to someone tidying up.
func TestVersionIsStamped(t *testing.T) {
	build := read(t, "cmd/sortie/BUILD.bazel")

	got := xDefValue.FindStringSubmatch(build)
	if got == nil {
		// Fatal: every check below is about the value, and there is none.
		t.Fatalf("cmd/sortie/BUILD.bazel does not set %q via x_defs;\n"+
			"without it the binary reports the %q default and cannot identify itself",
			versionSymbol, "dev")
	}
	if got[1] != versionValue {
		t.Errorf("cmd/sortie/BUILD.bazel assigns %q to %s, want %s.\n"+
			"  a literal version is worse than %q -- it is wrong rather than obviously absent\n"+
			"  an unprefixed key is volatile, and volatile status does not invalidate the\n"+
			"    action that embeds it, so the version can be served stale from cache\n"+
			"  another {STABLE_*} key diverges from the chart, which stamps appVersion\n"+
			"    from %s -- a pod and its release would then disagree",
			got[1], versionSymbol, versionValue, "dev", versionValue)
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
