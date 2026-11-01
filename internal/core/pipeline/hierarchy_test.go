package pipeline

import (
	"fmt"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestAncestorCodecAndParentEnvelope(t *testing.T) {
	for _, pair := range [][2]string{{"pl-a", "pl-a"}, {"pl-a", "pl-b"}, {"a.b", "ab"}, {"ab", "a.b"}} {
		depth := uint64(1)
		if pair[0] == pair[1] {
			depth = 0
		}
		row, err := AncestorRow(pair[0], pair[1], depth)
		if err != nil {
			t.Fatal(err)
		}
		d, a, n, err := DecodeAncestorRow(row)
		if err != nil || d != pair[0] || a != pair[1] || n != depth {
			t.Fatalf("codec lost pair/depth: %+v %v", row, err)
		}
		prefix, _ := AncestorPrefix(d)
		if !strings.HasPrefix(row.Key, prefix) {
			t.Fatal("descendant prefix is inconsistent")
		}
		for _, bad := range [][]byte{[]byte("01"), []byte("-1"), []byte("1.0"), []byte(" 1"), []byte("18446744073709551616")} {
			if _, _, _, err := DecodeAncestorRow(driver.DerivedRow{Key: row.Key, Value: bad}); err == nil {
				t.Fatalf("accepted noncanonical depth %q", bad)
			}
		}
	}
	for _, parent := range []string{`"pl-p"`, `{"key":"p"}`, `{"id":""}`, `{"id":"p","branch":"1"}`} {
		if _, err := DecodeDocument([]byte(`{"id":"pl-c","type":"issue","fields":{},"parent":` + parent + `}`)); err == nil {
			t.Fatalf("accepted malformed stored Parent %s", parent)
		}
	}
	for _, parent := range []string{`null`, `{"id":"pl-p"}`, `{"id":"pl-p","branch":"0"}`} {
		if _, err := DecodeDocument([]byte(`{"id":"pl-c","type":"issue","fields":{},"parent":` + parent + `}`)); err != nil {
			t.Fatalf("canonical Parent %s: %v", parent, err)
		}
	}
}

func TestHierarchyOrderedWorkCannotCancelTheBound(t *testing.T) {
	parents := map[string]string{"subtree": "", "ancestor-0": ""}
	for i := 1; i < 500; i++ {
		parents[fmt.Sprintf("leaf-%d", i)] = "subtree"
	}
	for i := 1; i < 10; i++ {
		parents[fmt.Sprintf("ancestor-%d", i)] = fmt.Sprintf("ancestor-%d", i-1)
	}
	p := &prepared{hierarchyEdits: []hierarchyEdit{{id: "subtree", parent: "ancestor-9"}, {id: "subtree", parent: ""}}}
	_, err := p.hierarchyAffected(parents)
	code(t, err, "closure_too_large")
}
