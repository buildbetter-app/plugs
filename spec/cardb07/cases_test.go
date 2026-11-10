// Package cardb20 deliberately shares the unchanged B20 canonical build helper
// through a separate B07-only Bazel target. No general builder is introduced.
package cardb20

import "strings"

// Declared in the pinned 9d6fff native DependencyType constants, in their source
// order. Native observations are retained independently from Plug projections.
var b07KnownKinds = []string{
	"blocks", "parent-child", "conditional-blocks", "waits-for",
	"related", "discovered-from", "replies-to", "relates-to",
	"duplicates", "supersedes", "authored-by", "assigned-to",
	"approved-by", "attests", "tracks", "until", "caused-by",
	"validates", "delegated-from",
}

var b07OpaqueKindInputs = []struct {
	Name         string
	Kind         string
	ValidStorage bool
}{
	{"opaque", "custom kind", true},
	{"whitespace_only", " ", true},
	{"spaced_fixed_spelling", " blocks ", true},
	{"ascii_32_bytes", strings.Repeat("k", 32), true},
	{"utf8_32_bytes", strings.Repeat("é", 16), true},
	{"ascii_33_bytes", strings.Repeat("k", 33), false},
	{"utf8_34_bytes", strings.Repeat("é", 17), false},
	{"empty", "", false},
}
