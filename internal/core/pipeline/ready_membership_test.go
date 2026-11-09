package pipeline

import (
	"encoding/json"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestReadySelectionLeafBindsAllocatedSourceRevision(t *testing.T) {
	doc := Document{ID: "pl-exact", Type: "issue", Project: "local", Fields: map[string]any{"title": "source", "status": "todo", "priority": json.Number("2"), "description": "full semantic tail"}, Metadata: map[string]json.RawMessage{"not-in-membership": json.RawMessage(`true`)}}
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	ref := model.RefByID(doc.ID, model.Natural64(0))
	bead := driver.SelectionBead{Ref: ref, Type: doc.Type, Status: "todo", Priority: 2}
	project := doc.Project
	bead.Project = &project
	value, err := json.Marshal(bead)
	if err != nil {
		t.Fatal(err)
	}
	index := &driver.SelectionIndex{Version: driver.SelectionIndexVersion, Incarnation: "test", Branch: model.Natural64(0), Row: driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "ready-test"}, Source: driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Present: true, Revision: model.Natural64(7), Digest: driver.SelectionDigest(body)}, Bead: &bead, ValueDigest: driver.SelectionDigest(value)}
	rows := []driver.DerivedRow{{Key: index.Row.Key, Value: value, Index: index}}
	if err = BindReadySelectionRows(doc, model.Natural64(11), rows); err != nil {
		t.Fatal(err)
	}
	// Independent original Ready JSON shape: no parent/metadata, but every
	// semantic field and the allocated source revision must be represented.
	wantJSON, err := json.Marshal(struct {
		ID       string         `json:"id"`
		Type     string         `json:"type"`
		Project  string         `json:"project"`
		Status   string         `json:"status"`
		Revision model.Revision `json:"revision"`
		Fields   map[string]any `json:"fields,omitempty"`
	}{doc.ID, doc.Type, doc.Project, "todo", model.Natural64(11), doc.Fields})
	if err != nil {
		t.Fatal(err)
	}
	want := driver.SelectionDigest(wantJSON)
	if rows[0].Index.Bead.MembershipLeaf != want {
		t.Fatal("semantic leaf did not bind full legacy shape and H+1")
	}
	if rows[0].Index.Source != index.Source {
		t.Fatal("preimage observation was changed")
	}
	if bead.MembershipLeaf != "" || string(value) == string(rows[0].Value) || rows[0].Index.ValueDigest == index.ValueDigest {
		t.Fatal("logical row digest not updated independently")
	}
	var persisted driver.SelectionBead
	if err = json.Unmarshal(rows[0].Value, &persisted); err != nil || persisted.MembershipLeaf != want {
		t.Fatal("logical leaf missing", err)
	}
	if err = rows[0].Index.Validate(driver.DerivedSortKey, rows[0]); err != nil {
		t.Fatal(err)
	}
}
