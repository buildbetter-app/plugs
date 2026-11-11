// Package cardb20 reuses the unchanged, pinned native source-build support.
package cardb20

// These expectations are authored from canonical 9d6 issueops/gates.go,
// blocked_state.go:38-129 and the B14 card. Neither product supplies them.
// All/any are child predicates; also_blocks independently holds on the spawner.
var b14Predicates = []struct {
	Name, Gate string
	Also       bool
}{
	{"default", "", false},
	{"all", "all-children", false},
	{"any", "any-children", false},
	{"all-also", "all-children", true},
	{"any-also", "any-children", true},
}

var b14Stages = []struct {
	Name  string
	Ready []bool
}{
	{"empty", []bool{true, true, true, false, false}},
	{"open-children", []bool{false, false, false, false, false}},
	{"mixed-children", []bool{false, false, true, false, false}},
	{"closed-children", []bool{true, true, true, false, false}},
	{"closed-spawner", []bool{true, true, true, true, true}},
	{"closed-spawner-mixed", []bool{false, false, true, false, true}},
	{"closed-spawner-open", []bool{false, false, false, false, false}},
}

type b14Projection struct {
	DefaultType string
	BareAwait   string
	DefaultList []string
	AllList     []string
	CheckLimit  []string
	AwaitLimit  []string
	Waiters     []string
	Readiness   map[string][]bool
}
