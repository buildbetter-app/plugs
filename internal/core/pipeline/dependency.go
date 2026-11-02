package pipeline

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// The Beads dependency column is VARCHAR(32). Plug keeps the v1 vocabulary
// closed for known workflow/graph/entity/tracker meanings while retaining
// opaque custom values for import and round-trip. Custom values have no core
// readiness, guard, or cycle effect.
const maxDependencyKindBytes = 32

var wellKnownDependencyKinds = map[string]struct{}{
	"blocks": {}, "parent-child": {}, "conditional-blocks": {}, "waits-for": {},
	"related": {}, "discovered-from": {}, "replies-to": {}, "relates-to": {},
	"duplicates": {}, "supersedes": {}, "authored-by": {}, "assigned-to": {},
	"approved-by": {}, "attests": {}, "tracks": {}, "until": {},
	"caused-by": {}, "validates": {}, "delegated-from": {},
}

func validDependencyKind(kind string) bool {
	if kind == "" || strings.TrimSpace(kind) != kind || len([]byte(kind)) > maxDependencyKindBytes || !utf8.ValidString(kind) {
		return false
	}
	for _, r := range kind {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return false
		}
	}
	return true
}

func dependencyAffectsReady(kind string) bool {
	switch kind {
	case "blocks", "parent-child", "conditional-blocks", "waits-for":
		return true
	default:
		return false
	}
}

func dependencyIsBlocking(kind string) bool {
	switch kind {
	case "blocks", "conditional-blocks", "waits-for":
		return true
	default:
		return false
	}
}

func dependencyIsScheduling(kind string) bool {
	return kind == "blocks" || kind == "conditional-blocks" || kind == "parent-child"
}
