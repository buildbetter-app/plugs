package cardb20

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

type frontNode struct {
	Role, Status, Kind string
	Priority           int
	Age                time.Duration
	Wisp               bool
}
type frontCase struct {
	Name                  string
	Nodes                 []frontNode
	ParentWisp, ChildWisp bool
}
type frontVector struct {
	Default, Hybrid, Plug []string
}

func frontCases() []frontCase {
	return []frontCase{
		{Name: "category_and_blocked_front", Nodes: []frontNode{
			{"blocked", "open", "task", 0, 4 * time.Hour, false},
			{"blocker", "in_progress", "task", 0, 4 * time.Hour, false},
			{"hidden", "open", "agent", 0, 4 * time.Hour, false},
			{"pinned", "pinned", "task", 0, 4 * time.Hour, false},
			{"closed", "closed", "task", 0, 4 * time.Hour, false},
			{"custom", "b01active", "task", 1, 2 * time.Hour, false},
			{"open", "open", "task", 2, time.Hour, false},
		}},
		{Name: "priority_created_id", Nodes: []frontNode{
			{"old-p4", "open", "task", 4, 96 * time.Hour, false},
			{"old-p0", "open", "task", 0, 72 * time.Hour, false},
			{"z-earlier", "open", "task", 1, 3 * time.Hour, false},
			{"a-later", "open", "task", 1, 2 * time.Hour, false},
			// Reverse insertion distinguishes the ID tie from input order.
			{"tie-z", "open", "task", 3, time.Hour, false},
			{"tie-a", "open", "task", 3, time.Hour, false},
			{"recent-p2", "open", "task", 2, 45 * time.Minute, false},
			{"urgent", "open", "task", 0, 30 * time.Minute, false},
		}},
		hierarchyCase("durable_durable", false, false),
		hierarchyCase("durable_wisp", false, true),
		hierarchyCase("wisp_durable", true, false),
		hierarchyCase("wisp_wisp", true, true),
	}
}
func hierarchyCase(name string, parentWisp, childWisp bool) frontCase {
	return frontCase{Name: name, ParentWisp: parentWisp, ChildWisp: childWisp, Nodes: []frontNode{
		{"parent", "open", "task", 0, time.Hour, parentWisp},
		{"child", "open", "task", 1, time.Hour, childWisp},
		{"grandchild", "open", "task", 1, time.Hour, !childWisp},
		{"free", "open", "task", 2, time.Hour, false},
		{"blocker", "in_progress", "task", 0, time.Hour, false},
	}}
}

// Independent literal oracles: no production sorting or native output is used
// to calculate an expected sequence. Adopted deviations stay separate.
func frontWant(name, phase string) frontVector {
	if name == "category_and_blocked_front" {
		return frontVector{[]string{"custom", "open"}, []string{"custom", "open"}, []string{"custom", "open"}}
	}
	if name == "priority_created_id" {
		return frontVector{
			[]string{"old-p0", "urgent", "z-earlier", "a-later", "recent-p2", "tie-a", "tie-z", "old-p4"},
			[]string{"urgent", "z-earlier", "a-later", "recent-p2", "tie-a", "tie-z", "old-p4", "old-p0"},
			[]string{"urgent", "z-earlier", "a-later", "recent-p2", "tie-a", "tie-z", "old-p4", "old-p0"},
		}
	}
	switch phase {
	case "open", "unblocked":
		a := []string{"parent", "child", "grandchild", "free"}
		return frontVector{a, a, a}
	case "blocked":
		a := []string{"free"}
		return frontVector{a, a, a}
	case "indefinite":
		a := []string{"child", "grandchild", "free"}
		return frontVector{a, a, []string{"free"}}
	case "timed":
		a := []string{"grandchild", "free"}
		return frontVector{a, a, []string{"free"}}
	default:
		panic("undeclared B01 phase")
	}
}
func frontPhases(c frontCase) []string {
	if c.Name == "category_and_blocked_front" || c.Name == "priority_created_id" {
		return []string{"baseline"}
	}
	return []string{"open", "blocked", "unblocked", "indefinite", "timed"}
}
func evidenceHash(data []byte) string { h := sha256.Sum256(data); return hex.EncodeToString(h[:]) }
func frontJSON(t testing.TB, v any) []byte {
	t.Helper()
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	return append(b, '\n')
}
func frontSave(t testing.TB, root, name string, data []byte) {
	t.Helper()
	if e := os.MkdirAll(root, 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(root, name), data, 0600); e != nil {
		t.Fatal(e)
	}
}
func frontEqual(t testing.TB, label string, got, want []string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s literal ordered membership got %v want %v", label, got, want)
	}
}
