package conformance

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func history(t *testing.T, factory Factory) {
	ctx, s, lease, meta := setup(t, factory)
	for i := 1; i <= 70; i++ {
		put(t, ctx, s, lease, meta, "pl-history", fmt.Sprintf(`{"value":%d}`, i))
	}
	r := read(t, ctx, s, meta, nil)
	page, err := r.Versions(ctx, ref("pl-history"), driver.PageReq{Limit: 20})
	if err != nil || len(page.Items) != 20 || page.NextCursor == nil {
		t.Fatalf("version page=%+v %v", page, err)
	}
	versions := page.Items
	for page.NextCursor != nil {
		page, err = r.Versions(ctx, ref("pl-history"), driver.PageReq{Limit: 20, Cursor: page.NextCursor})
		if err != nil {
			t.Fatal(err)
		}
		versions = append(versions, page.Items...)
	}
	if len(versions) != 70 {
		t.Fatalf("versions=%d", len(versions))
	}
	for i, version := range versions {
		if version.Base != (i%32 == 0) {
			t.Fatalf("base at index%d=%v", i, version.Base)
		}
		if !version.Base && !json.Valid(version.Patch) {
			t.Fatalf("invalid forward patch at%d", i)
		}
	}
	for _, revision := range []uint64{0, 1, 2, 31, 32, 33, 63, 64, 65, 70} {
		at := model.Natural64(revision)
		records, err := read(t, ctx, s, meta, &at).Get(ctx, []driver.Ref{ref("pl-history")})
		if err != nil {
			t.Fatal(err)
		}
		if revision == 0 {
			if len(records) != 0 {
				t.Fatal("record existed before creation")
			}
			continue
		}
		if len(records) != 1 || string(records[0].Body) != fmt.Sprintf(`{"value":%d}`, revision) || records[0].Revision.String() != at.String() {
			t.Fatalf("as-of%s=%v", at, records)
		}
	}
	requests := update(recordLock("pl-history"))
	tx := begin(t, ctx, s, lease, requests)
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Delete(ctx, ref("pl-history")); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx, header(meta, requests, model.ChangeRecord{Ref: ref("pl-history"), Op: model.OpDelete, FromRevision: model.Natural64(70)})); err != nil {
		t.Fatal(err)
	}
	afterDelete := read(t, ctx, s, meta, nil)
	records, err := afterDelete.Get(ctx, []driver.Ref{ref("pl-history")})
	if err != nil || len(records) != 0 {
		t.Fatalf("deleted current record=%v %v", records, err)
	}
	states, err := afterDelete.Observe(ctx, lockKeys(requests))
	if err != nil || states[0].Present || states[0].Revision.String() != "71" {
		t.Fatalf("tombstone=%v %v", states, err)
	}
	put(t, ctx, s, lease, meta, "pl-history", `{"value":72}`)
	recreated := read(t, ctx, s, meta, nil)
	records, err = recreated.Get(ctx, []driver.Ref{ref("pl-history")})
	if err != nil || len(records) != 1 || string(records[0].Body) != `{"value":72}` {
		t.Fatalf("recreate=%v %v", records, err)
	}
	at := model.Natural64(71)
	records, err = read(t, ctx, s, meta, &at).Get(ctx, []driver.Ref{ref("pl-history")})
	if err != nil || len(records) != 0 {
		t.Fatal("recreated record leaked into deleted historical view")
	}
	log, err := recreated.Changesets(ctx, &driver.Cursor{Incarnation: meta.Incarnation, Revision: model.Natural64(69)}, 100)
	if err != nil || len(log) != 3 || log[0].Revision.String() != "70" || log[2].Revision.String() != "72" {
		t.Fatalf("strict-after complete log=%v %v", log, err)
	}
	// Paginated queries remain bound to the original snapshot after a write.
	put(t, ctx, s, lease, meta, "pl-a", `{"value":1}`)
	put(t, ctx, s, lease, meta, "pl-b", `{"value":2}`)
	pinned := read(t, ctx, s, meta, nil)
	first, err := pinned.Query(ctx, driver.Query{Filter: map[string]any{"kind": "test"}, Limit: 1})
	if err != nil || first.NextCursor == nil {
		t.Fatalf("query page=%+v %v", first, err)
	}
	put(t, ctx, s, lease, meta, "pl-c", `{"value":3}`)
	second, err := pinned.Query(ctx, driver.Query{Filter: map[string]any{"kind": "test"}, Limit: 10, Cursor: first.NextCursor})
	if err != nil || len(second.Items) != 2 {
		t.Fatalf("pinned page=%+v %v", second, err)
	}
	if _, err := read(t, ctx, s, meta, nil).Query(ctx, driver.Query{Filter: map[string]any{"kind": "test"}, Cursor: first.NextCursor}); err == nil {
		t.Fatal("cross-snapshot cursor accepted")
	}
	if _, err := pinned.Query(ctx, driver.Query{Filter: map[string]any{"unknown": true}}); err == nil {
		t.Fatal("unsupported filter silently ignored")
	}
	if _, err := pinned.Query(ctx, driver.Query{Filter: map[string]any{"kind": "other"}, Cursor: first.NextCursor}); err == nil {
		t.Fatal("cross-filter cursor accepted")
	}
}

// Tombstones at scheduled base positions must not extend the replay chain
// across deleted lifetimes. Exercise two successive boundaries with real log
// commits, then move head so every checked view uses historical replay.
func historyRecreation(t *testing.T, factory Factory) {
	ctx, s, lease, meta := setup(t, factory)
	const id = "pl-recreated-history"
	for i := 1; i <= 96; i++ {
		if i == 33 || i == 65 {
			entry, err := Seed(ctx, s, lease, driver.ChangesetHeader{}, []FixtureChange{{Ref: ref(id), Kind: "test", Op: model.OpDelete}})
			if err != nil || entry.Revision.String() != model.Natural64(uint64(i)).String() {
				t.Fatalf("delete at%d: %+v %v", i, entry, err)
			}
			continue
		}
		put(t, ctx, s, lease, meta, id, fmt.Sprintf(`{"value":%d}`, i))
	}
	put(t, ctx, s, lease, meta, "pl-history-head", `{"value":97}`)
	r := read(t, ctx, s, meta, nil)
	page, err := r.Versions(ctx, ref(id), driver.PageReq{Limit: 100})
	if err != nil || len(page.Items) != 96 || page.NextCursor != nil {
		t.Fatalf("recreated versions=%+v %v", page, err)
	}
	for i, version := range page.Items {
		if version.Deleted {
			continue
		}
		if i == 0 || page.Items[i-1].Deleted {
			if !version.Base || !json.Valid(version.Body) || len(version.Patch) != 0 {
				t.Fatalf("live lifetime at version%d needs full base: %+v", i+1, version)
			}
		}
		patches := 0
		for j := i; !page.Items[j].Base; j-- {
			if !page.Items[j].Deleted {
				patches++
			}
			if patches >= driver.VersionBaseEvery || j == 0 {
				t.Fatalf("version%d requires %d patches without a full base", i+1, patches)
			}
		}
	}
	for _, revision := range []uint64{0, 1, 32, 33, 34, 64, 65, 66, 96} {
		at := model.Natural64(revision)
		historical := read(t, ctx, s, meta, &at)
		records, err := historical.Get(ctx, []driver.Ref{ref(id)})
		if err != nil {
			t.Fatalf("as-of%d: %v", revision, err)
		}
		if revision == 0 || revision == 33 || revision == 65 {
			if len(records) != 0 {
				t.Fatalf("deleted or unborn as-of%d: %+v", revision, records)
			}
			continue
		}
		if len(records) != 1 || string(records[0].Body) != fmt.Sprintf(`{"value":%d}`, revision) || records[0].Revision.String() != at.String() {
			t.Fatalf("historical recreation as-of%d: %+v", revision, records)
		}
	}
}
