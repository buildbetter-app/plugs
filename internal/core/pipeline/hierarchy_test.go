package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type hierarchyQueryReader struct {
	driver.Reader
	records []driver.Record
	queries int
}

func (r *hierarchyQueryReader) Query(context.Context, driver.Query) (driver.Page[driver.Record], error) {
	r.queries++
	return driver.Page[driver.Record]{Items: r.records}, nil
}

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
	for _, parent := range []string{`"pl-p"`, `{"key":"p"}`, `{"id":""}`, `{"id":"p","branch":1}`} {
		if _, err := DecodeDocument([]byte(`{"id":"pl-c","type":"issue","fields":{},"parent":` + parent + `}`)); err == nil {
			t.Fatalf("accepted malformed stored Parent %s", parent)
		}
	}
	for _, parent := range []string{`null`, `{"id":"pl-p"}`, `{"id":"pl-p","branch":0}`} {
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

func TestParentedFieldUpdatesSkipGraphScanButParentEditsValidate(t *testing.T) {
	parent := model.Ref{ID: "parent"}
	child := model.Ref{ID: "child"}
	parentDoc := Document{ID: parent.ID, Type: "issue", Fields: map[string]any{"status": "todo"}}
	childDoc := Document{ID: child.ID, Type: "issue", Parent: &parent, Fields: map[string]any{"status": "todo"}}
	parentBody, err := json.Marshal(parentDoc)
	if err != nil {
		t.Fatal(err)
	}
	childBody, err := json.Marshal(childDoc)
	if err != nil {
		t.Fatal(err)
	}
	records := []driver.Record{
		{Ref: parent, Kind: "bead", Revision: model.Natural64(1), Body: parentBody},
		{Ref: child, Kind: "bead", Revision: model.Natural64(2), Body: childBody},
	}
	newSnapshot := func(reader driver.Reader) *snapshotReader {
		return &snapshotReader{reader: reader, reads: &readSet{states: map[string]driver.LockState{}}}
	}

	t.Run("field-update-does-not-enumerate", func(t *testing.T) {
		reader := &hierarchyQueryReader{records: records}
		p := &prepared{
			snapshot: newSnapshot(reader),
			writes:   map[string]*pending{child.ID: {ref: child, kind: "bead", doc: childDoc, before: childBody, setBy: map[string]string{"fields.title": "request"}}},
		}
		if docs, err := p.hierarchy(context.Background()); err != nil || docs != nil {
			t.Fatalf("untouched parent graph returned docs=%v err=%v", docs, err)
		}
		if reader.queries != 0 {
			t.Fatalf("ordinary parented update enumerated %d graph pages", reader.queries)
		}
	})

	t.Run("parent-edit-still-validates", func(t *testing.T) {
		reader := &hierarchyQueryReader{records: records}
		missing := model.Ref{ID: "missing"}
		changed := childDoc
		changed.Parent = &missing
		p := &prepared{
			snapshot:       newSnapshot(reader),
			hierarchyEdits: []hierarchyEdit{{id: child.ID, parent: missing.ID}},
			writes:         map[string]*pending{child.ID: {ref: child, kind: "bead", doc: changed, before: childBody, setBy: map[string]string{"parent": "request"}}},
		}
		_, err := p.hierarchy(context.Background())
		code(t, err, "ontology_guard_failed")
		if reader.queries != 1 {
			t.Fatalf("parent edit did not enumerate the stored graph once: %d", reader.queries)
		}
	})
}
