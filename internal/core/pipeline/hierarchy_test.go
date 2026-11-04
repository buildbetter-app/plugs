package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type hierarchyQueryReader struct {
	driver.Reader
	records []driver.Record
	queries int
}

func (r *hierarchyQueryReader) Snapshot() driver.View {
	revision := model.Natural64(3)
	return driver.View{Incarnation: "hierarchy-test", At: &revision}
}

func (r *hierarchyQueryReader) Observe(_ context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	states := make([]driver.LockState, 0, len(keys))
	for _, key := range keys {
		states = append(states, driver.LockState{Key: key, Revision: model.Natural64(3)})
	}
	return states, nil
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

func TestStoredHierarchyFormsCanonicalizeWithoutRewriting(t *testing.T) {
	for _, tc := range []struct {
		name, members, parent string
	}{
		{"root", ``, ""},
		{"canonical", `,"parent":{"id":"pl-p"}`, "pl-p"},
		{"legacy-only", `,"links":[{"to":{"id":"pl-p"},"kind":"parent-child"}]`, "pl-p"},
		{"legacy-branch-zero", `,"links":[{"to":{"id":"pl-p","branch":0},"kind":"parent-child"}]`, "pl-p"},
		{"null-parent-legacy", `,"parent":null,"links":[{"to":{"id":"pl-p"},"kind":"parent-child"}]`, "pl-p"},
		{"matching-forms", `,"parent":{"id":"pl-p","branch":0},"links":[{"to":{"id":"pl-p"},"kind":"parent-child"}]`, "pl-p"},
		{"unrelated-link", `,"links":[{"to":{"id":"pl-other"},"kind":"blocks"},{"to":{"id":"pl-p"},"kind":"parent-child"}]`, "pl-p"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{"id":"pl-c","type":"issue","fields":{"count":9007199254740993}` + tc.members + `}`)
			original := append([]byte(nil), body...)
			var stored Document
			if err := json.Unmarshal(body, &stored); err != nil {
				t.Fatal(err)
			}
			before, err := json.Marshal(stored)
			if err != nil {
				t.Fatal(err)
			}
			assertParent := func(parent *model.Ref, err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
				if tc.parent == "" {
					if parent != nil {
						t.Fatalf("root gained parent: %+v", parent)
					}
				} else if parent == nil || !parent.IsID() || parent.ID != tc.parent || !parent.Branch.IsZero() {
					t.Fatalf("parent=%+v, want literal canonical ID %q on branch zero", parent, tc.parent)
				}
			}
			assertParent(HierarchyParent(stored))
			after, err := json.Marshal(stored)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("hierarchy resolution changed its input: %s -> %s, %v", before, after, err)
			}
			decoded, err := DecodeDocument(body)
			assertParent(decoded.Parent, err)
			assertParent(HierarchyParent(decoded))
			if decoded.Fields["count"] != json.Number("9007199254740993") || !reflect.DeepEqual(decoded.Links, stored.Links) {
				t.Fatalf("canonicalization changed numbers or links: %+v", decoded)
			}
			if !bytes.Equal(body, original) {
				t.Fatal("decoding rewrote original stored bytes")
			}
		})
	}
}

func TestStoredHierarchyConflictsRefuseTyped(t *testing.T) {
	for _, tc := range []struct{ name, members string }{
		{"different-forms", `"parent":{"id":"pl-p"},"links":[{"to":{"id":"pl-other"},"kind":"parent-child"}]`},
		{"multiple-legacy-parents", `"links":[{"to":{"id":"pl-p"},"kind":"parent-child"},{"to":{"id":"pl-other"},"kind":"parent-child"}]`},
		{"duplicate-legacy-parent", `"links":[{"to":{"id":"pl-p"},"kind":"parent-child"},{"to":{"id":"pl-p"},"kind":"parent-child"}]`},
		{"matching-then-conflicting", `"parent":{"id":"pl-p"},"links":[{"to":{"id":"pl-p"},"kind":"parent-child"},{"to":{"id":"pl-other"},"kind":"parent-child"}]`},
		{"matching-then-duplicate", `"parent":{"id":"pl-p"},"links":[{"to":{"id":"pl-p"},"kind":"parent-child"},{"to":{"id":"pl-p"},"kind":"parent-child"}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{"id":"pl-c","type":"issue","fields":{},` + tc.members + `}`)
			var stored Document
			if err := json.Unmarshal(body, &stored); err != nil {
				t.Fatal(err)
			}
			_, err := HierarchyParent(stored)
			code(t, err, "invalid_params")
			_, err = DecodeDocument(body)
			code(t, err, "invalid_params")
		})
	}
}

func TestStoredHierarchyMalformedRefsStayRefused(t *testing.T) {
	for _, tc := range []struct{ name, ref string }{
		{"key", `{"key":"pl-p"}`},
		{"empty-id", `{"id":""}`},
		{"nonzero-branch", `{"id":"pl-p","branch":1}`},
		{"slash-id", `{"id":"pl/p"}`},
		{"spaced-id", `{"id":" pl-p"}`},
	} {
		for _, form := range []string{"canonical", "legacy"} {
			t.Run(form+"/"+tc.name, func(t *testing.T) {
				member := `"parent":` + tc.ref
				if form == "legacy" {
					member = `"links":[{"kind":"parent-child","to":` + tc.ref + `}]`
				}
				body := []byte(`{"id":"pl-c","type":"issue","fields":{},` + member + `}`)
				var stored Document
				if err := json.Unmarshal(body, &stored); err != nil {
					t.Fatal(err)
				}
				_, err := HierarchyParent(stored)
				code(t, err, "invalid_params")
				_, err = DecodeDocument(body)
				code(t, err, "invalid_params")
			})
		}
	}
	for _, ref := range []string{`"pl-p"`, `{}`, `{"id":"pl-p","key":"p"}`, `{"id":"pl-p","branch":-1}`} {
		for _, member := range []string{`"parent":` + ref, `"links":[{"kind":"parent-child","to":` + ref + `}]`} {
			if _, err := DecodeDocument([]byte(`{"id":"pl-c","type":"issue","fields":{},` + member + `}`)); err == nil {
				t.Fatalf("accepted malformed reference %s", member)
			}
		}
	}
	if _, err := DecodeDocument([]byte(`{"id":"pl-c","type":"issue","fields":{},"links":[{"kind":"parent-child","to":null}]}`)); err == nil {
		t.Fatal("accepted null legacy parent")
	}
}

func TestLegacyHierarchyKeepsEnvelopeAndCommentValidation(t *testing.T) {
	for _, tc := range []struct{ name, members string }{
		{"missing-id", `"type":"issue","fields":{}`},
		{"missing-type", `"id":"pl-c","fields":{}`},
		{"null-fields", `"id":"pl-c","type":"issue","fields":null`},
		{"legacy-comment-provenance", `"id":"pl-c","type":"issue","fields":{},"comments":[{"body":"old","sourceAuthor":"source"}]`},
		{"incomplete-imported-comment", `"id":"pl-c","type":"issue","fields":{},"comments":[{"body":"old","origin":"import"}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeDocument([]byte(`{` + tc.members + `,"links":[{"to":{"id":"pl-p"},"kind":"parent-child"}]}`)); err == nil {
				t.Fatal("legacy hierarchy bypassed document/comment validation")
			}
		})
	}
}

func TestHierarchyDetachRequiresExplicitLegacyUnlink(t *testing.T) {
	for _, tc := range []struct {
		name           string
		legacy, unlink bool
	}{
		{"canonical-detach", false, false},
		{"legacy-detach-refuses", true, false},
		{"legacy-unlink-then-detach", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := setup(t)
			w := h.writer()
			seed := apply(t, w, model.ChangeSet{Ops: []model.Op{create("parent"), create("child")}}, meta(1))
			parent, child := seed.Results[0].Ref, seed.Results[1].Ref
			detach := model.Op{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: child}}
			attach := model.Op{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: child, Parent: &parent}}
			if tc.legacy {
				attach = link(child, parent, "parent-child")
			}
			apply(t, w, model.ChangeSet{Ops: []model.Op{attach}}, meta(2))
			before := metadataGraph(t, h)
			ops := []model.Op{patch(child, map[string]any{"title": "detached"})}
			if tc.unlink {
				unlink := link(child, parent, "parent-child")
				unlink.Kind = model.OpUnlink
				ops = append(ops, unlink)
			}
			ops = append(ops, detach)
			_, err := w.Apply(context.Background(), by, model.ChangeSet{Ops: ops}, meta(3))
			if tc.legacy && !tc.unlink {
				code(t, err, "invalid_params")
				if after := metadataGraph(t, h); !reflect.DeepEqual(before, after) {
					t.Fatal("refused detach published staged fields, versions, history or observations")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			rows, err := h.read(t).Get(context.Background(), []model.Ref{child})
			if err != nil || len(rows) != 1 {
				t.Fatal(rows, err)
			}
			var stored Document
			if err := json.Unmarshal(rows[0].Body, &stored); err != nil {
				t.Fatal(err)
			}
			if stored.Parent != nil || len(stored.Links) != 0 || stored.Fields["title"] != "detached" {
				t.Fatalf("detach did not persist an unparented child: %+v", stored)
			}
		})
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

func TestMixedRootCreateAndParentedFieldUpdateRetainsGraphValidation(t *testing.T) {
	h := setup(t)
	w := h.writer()
	parentCommit := apply(t, w, model.ChangeSet{Ops: []model.Op{create("parent")}}, meta(100))
	parent := parentCommit.Results[0].Ref
	childCommit := apply(t, w, model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Key: "child", Type: "issue", Parent: &parent, Fields: map[string]any{"title": "child"}}}}}, meta(101))
	child := childCommit.Results[0].Ref

	commit := apply(t, w, model.ChangeSet{Ops: []model.Op{create("root"), patch(child, map[string]any{"title": "updated"})}}, meta(102))
	if len(commit.Results) != 2 {
		t.Fatalf("mixed root/parented update returned %d results", len(commit.Results))
	}
	updated, err := DecodeDocument(commit.Results[1].Record)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Parent == nil || updated.Parent.ID != parent.ID || updated.Fields["title"] != "updated" {
		t.Fatalf("mixed batch lost parent or field update: %+v", updated)
	}
}
