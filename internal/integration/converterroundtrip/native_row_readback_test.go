package converterroundtrip

import (
	"context"
	"fmt"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type nativeRowQueryFixture struct {
	driver.Reader
	view  driver.View
	pages []driver.Page[driver.Record]
	calls int
}

func (r *nativeRowQueryFixture) Snapshot() driver.View { return r.view }
func (r *nativeRowQueryFixture) Query(_ context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	if len(q.Filter) != 0 || q.AsOf == nil || *q.AsOf != *r.view.At {
		return driver.Page[driver.Record]{}, fmt.Errorf("query filtered or unpinned")
	}
	if r.calls > 0 && q.Cursor == nil {
		return driver.Page[driver.Record]{}, fmt.Errorf("pagination restarted")
	}
	if r.calls >= len(r.pages) {
		return driver.Page[driver.Record]{}, fmt.Errorf("query beyond terminal")
	}
	page := r.pages[r.calls]
	r.calls++
	return page, nil
}
func TestNativeRowReadbackEnumeratesWrongKindExtrasAndPinnedTerminalPages(t *testing.T) {
	source := dumpformat.SourceIdentity{StoreID: "public-store", Project: "public"}
	table := dumpformat.NativeTable{Columns: []string{"id", "value"}, Rows: []dumpformat.Row{{"id": "same", "value": nil}, {"id": "same", "value": nil}}}
	records, err := dumpformat.NativeV1TableRecords(source, "items", "retained-native-commit", table)
	if err != nil {
		t.Fatal(err)
	}
	at := model.Revision("17")
	view := driver.View{Incarnation: "public-incarnation", At: &at}
	cursor := func(index int) *driver.Cursor {
		return &driver.Cursor{Incarnation: view.Incarnation, Revision: at, Index: model.Natural64(uint64(index)), Method: "query"}
	}
	extra := driver.Record{Ref: model.RefByID(driver.NativeTableHistoryRecordPrefix+"row/wrong-kind-extra", model.Natural64(0)), Kind: "bead", Body: []byte(`{}`)}
	for _, fault := range []string{"none", "wrong-kind-extra", "malformed-extra", "duplicate-ref", "wrong-snapshot", "wrong-cursor", "wrong-method", "empty-nonterminal", "repeated-cursor"} {
		t.Run(fault, func(t *testing.T) {
			pages := []driver.Page[driver.Record]{{Items: records[:1], AsOf: at, NextCursor: cursor(1)}, {Items: records[1:], AsOf: at}}
			switch fault {
			case "wrong-kind-extra":
				pages[1].Items = append(append([]driver.Record{}, pages[1].Items...), extra)
			case "malformed-extra":
				extra.Kind = "beads-archive"
				pages[1].Items = append(append([]driver.Record{}, pages[1].Items...), extra)
			case "duplicate-ref":
				pages[1].Items = append(append([]driver.Record{}, pages[1].Items...), records[0])
			case "wrong-snapshot":
				pages[1].AsOf = model.Revision("18")
			case "wrong-cursor":
				pages[0].NextCursor.Revision = model.Revision("18")
			case "wrong-method":
				pages[0].NextCursor.Method = "versions"
			case "empty-nonterminal":
				pages[0].Items = nil
			case "repeated-cursor":
				pages[1].NextCursor = cursor(1)
			}
			reader := &nativeRowQueryFixture{view: view, pages: pages}
			groups, err := queryNativeRowGroups(context.Background(), reader, view.Incarnation, at, nil)
			if fault != "none" {
				if err == nil {
					t.Fatal("incomplete/invalid namespace query accepted")
				}
				return
			}
			group := nativeRowGroup{source, "items", "retained-native-commit"}
			if reader.calls != 2 || len(groups) != 1 || verifyNativeRowGroup(group, groups[group], records, table, nil) != nil {
				t.Fatal("complete pinned row group differs", err)
			}
		})
	}
}
func TestNativeRowReadbackRejectsMissingExtraAndOrphanMembership(t *testing.T) {
	source := dumpformat.SourceIdentity{StoreID: "public-store", Project: "public"}
	table := dumpformat.NativeTable{Columns: []string{"id"}, Rows: []dumpformat.Row{{"id": "x"}, {"id": "x"}}}
	expected, err := dumpformat.NativeV1TableRecords(source, "items", "retained-native-commit", table)
	if err != nil {
		t.Fatal(err)
	}
	group := nativeRowGroup{source, "items", "retained-native-commit"}
	for i := range expected {
		got := append([]driver.Record{}, expected[:i]...)
		got = append(got, expected[i+1:]...)
		if verifyNativeRowGroup(group, got, expected, table, nil) == nil {
			t.Fatal("missing meta/row accepted", i)
		}
	}
	foreign, err := dumpformat.NativeV1TableRecords(source, "other", "retained-native-commit", table)
	if err != nil {
		t.Fatal(err)
	}
	got := append([]driver.Record{}, expected...)
	got[0] = foreign[0]
	if verifyNativeRowGroup(group, got, expected, table, nil) == nil {
		t.Fatal("foreign/orphan membership accepted")
	}
	got = append(append([]driver.Record{}, expected...), expected[0])
	if verifyNativeRowGroup(group, got, expected, table, nil) == nil {
		t.Fatal("extra duplicate row accepted")
	}
}
