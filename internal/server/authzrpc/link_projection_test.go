package authzrpc_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestOrdinaryLinkProjectionUsesCurrentTargetAuthority(t *testing.T) {
	countDrivers(t, func(t *testing.T, open storeFactory) {
		f := newFixtureOn(t, open(t), nil)
		source, hidden, visible := fixtureRef(1), fixtureRef(2), fixtureRef(3)
		links := []map[string]any{
			{"to": hidden, "kind": "relates-to"},
			{"to": hidden, "kind": "relates-to", "targetKind": "external"},
			{"to": visible, "kind": "relates-to"},
		}
		var changes []conformance.FixtureChange
		for _, ref := range []model.Ref{source, hidden, visible} {
			body := map[string]any{"id": ref.ID, "type": "issue", "project": "project", "createdAt": "2026-11-03T00:00:00Z", "fields": map[string]any{"title": "link projection", "status": "todo", "priority": 0}}
			if ref.ID == source.ID {
				body["links"] = links
			}
			raw, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			op := model.OpCreate
			if ref.ID == source.ID {
				op = model.OpPatch
			}
			changes = append(changes, conformance.FixtureChange{Ref: ref, Kind: "bead", Op: op, Body: raw})
		}
		if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, changes); err != nil {
			t.Fatal(err)
		}
		by := model.PrincipalRef{Kind: "user", ID: "user:link-reader", Caveats: map[string]any{"refs": []model.Ref{source, visible}}}
		before, err := f.store.Meta(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		assertLinks := func(raw json.RawMessage, field string) {
			t.Helper()
			var result map[string]json.RawMessage
			if err := json.Unmarshal(raw, &result); err != nil {
				t.Fatal(err)
			}
			var beads []struct {
				ID    string
				Links []struct {
					From       string
					To         model.Ref
					TargetKind model.LinkTargetKind
				}
			}
			if field == "bead" {
				beads = append(beads, struct {
					ID    string
					Links []struct {
						From       string
						To         model.Ref
						TargetKind model.LinkTargetKind
					}
				}{})
				if err := json.Unmarshal(result[field], &beads[0]); err != nil {
					t.Fatal(err)
				}
			} else if err := json.Unmarshal(result[field], &beads); err != nil {
				t.Fatal(err)
			}
			found := false
			for _, bead := range beads {
				if bead.ID != source.ID {
					continue
				}
				found = true
				if len(bead.Links) != 2 {
					t.Fatalf("denied local target left a trace: %s", raw)
				}
				seenExternal, seenLocal := false, false
				for _, link := range bead.Links {
					if link.From != source.ID {
						t.Fatalf("wrong source: %+v", link)
					}
					if link.TargetKind == model.LinkTargetExternal && link.To.ID == hidden.ID {
						seenExternal = true
					} else if link.TargetKind != model.LinkTargetExternal && link.To.ID == visible.ID {
						seenLocal = true
					} else {
						t.Fatalf("wrong endpoint disclosure: %+v", link)
					}
				}
				if !seenExternal || !seenLocal {
					t.Fatalf("opaque or allowed endpoint lost: %s", raw)
				}
			}
			if !found {
				t.Fatal("readable source was omitted")
			}
		}
		point := f.call(t, by, "bead.get", map[string]any{"id": source}, 21001)
		success(t, point)
		assertLinks(point.Result, "bead")
		list := f.call(t, by, "bead.ready", map[string]any{"limit": 1000}, 21002)
		success(t, list)
		assertLinks(list.Result, "items")
		after, err := f.store.Meta(context.Background())
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("read projection published state: before=%+v after=%+v error=%v", before, after, err)
		}
	})
}
