package offlinehistory_test

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// This reuses the labelled driver fixture from history_test.go. Its raw JSON
// key order, null and wide integer are intentional byte-level oracles; this is
// not evidence of a canonical Beads producer or native full-history replay.
func TestNativeReversePartialHistoryBytesAndOrderAfterCloseReopen(t *testing.T) {
	ctx := context.Background()
	when := time.Date(2026, 11, 3, 0, 0, 0, 123456789, time.UTC)
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			open := persistentReverseOpener(t, engine)
			store := open()
			defer func() { _ = store.Close() }()
			tx, lease := begin(t, store)
			p, history := profile(driver.RestoreHistoryModePartial), payload(true)
			var err error
			p.HistoryDigest, err = driver.OfflineHistoryDigest(history)
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := tx.(driver.OfflineHistoryTx).PrepareOfflineHistory(ctx, p, history, when)
			if err != nil {
				t.Fatal(err)
			}
			stage(t, tx)
			if _, err := tx.(driver.OfflineHistoryTx).CommitOfflineHistory(ctx, header(p, prepared, when)); err != nil {
				t.Fatal(err)
			}
			if err := tx.Close(); err != nil {
				t.Fatal(err)
			}
			if err := lease.Release(ctx); err != nil {
				t.Fatal(err)
			}

			before := assertReverseFixtureHistory(t, store, p, when)
			assertNativeReverseUnsupported(t, store, before.Meta)
			if after := assertReverseFixtureHistory(t, store, p, when); !reflect.DeepEqual(after, before) {
				t.Fatal("native reverse boundary changed durable history before close")
			}
			closed := store
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store = open()
			if store == closed {
				t.Fatal("reopen reused the closed store handle")
			}
			if reopened := assertReverseFixtureHistory(t, store, p, when); !reflect.DeepEqual(reopened, before) {
				t.Fatal("physical reopen changed exact bodies, versions, changesets or floor")
			}
			assertNativeReverseUnsupported(t, store, before.Meta)
			if after := assertReverseFixtureHistory(t, store, p, when); !reflect.DeepEqual(after, before) {
				t.Fatal("native reverse boundary changed durable history after reopen")
			}
		})
	}
}

type reverseFixtureHistory struct {
	Meta       driver.Meta
	Rows       []driver.Record
	Versions   []driver.Version
	Changesets []model.Changeset
}

func assertReverseFixtureHistory(t *testing.T, store driver.Store, p driver.RestoreImportProfile, when time.Time) reverseFixtureHistory {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if meta.StoreID != p.SourceStoreID || meta.Head != model.Natural64(3) || meta.HistoryFloor != model.Natural64(1) {
		t.Fatalf("partial history metadata = %+v", meta)
	}
	ref := record("").Ref
	for _, image := range []struct {
		at, revision uint64
		title        string
	}{{1, 1, "before"}, {2, 2, "after"}, {3, 2, "after"}} {
		at := model.Natural64(image.at)
		r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &at})
		if err != nil {
			t.Fatal(err)
		}
		rows, err := r.Get(ctx, []model.Ref{ref})
		if err != nil || len(rows) != 1 || rows[0].Revision != model.Natural64(image.revision) || !bytes.Equal(rows[0].Body, record(image.title).Body) {
			r.Close()
			t.Fatalf("revision %d Get lost exact fixture bytes: %+v %v", image.at, rows, err)
		}
		page, err := r.Query(ctx, driver.Query{Limit: 10})
		r.Close()
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, row := range page.Items {
			if row.Ref == ref {
				found = true
				if !bytes.Equal(row.Body, record(image.title).Body) || row.Revision != model.Natural64(image.revision) {
					t.Fatalf("revision %d Query changed raw body/revision: %+v", image.at, row)
				}
			}
		}
		if !found {
			t.Fatalf("revision %d Query omitted the retained fixture", image.at)
		}
	}
	zero := model.Natural64(0)
	below, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &zero})
	if below != nil {
		below.Close()
	}
	var truncated driver.ErrTruncated
	if !errors.As(err, &truncated) || truncated.Floor != meta.HistoryFloor {
		t.Fatalf("below-floor read = %v, want exact floor %s", err, meta.HistoryFloor)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	page, err := r.Query(ctx, driver.Query{Limit: 10})
	if err != nil || len(page.Items) != 2 || page.NextCursor != nil {
		t.Fatalf("retained row census = %+v %v", page, err)
	}
	wantVersions := []driver.Version{
		{Ref: ref, Revision: model.Natural64(1), Base: true, Body: record("before").Body, CreatedAt: when, Principal: actor},
		{Ref: ref, Revision: model.Natural64(2), Patch: []byte(`[{"op":"replace","path":"","value":` + string(record("after").Body) + `}]`), CreatedAt: when, Principal: actor},
	}
	var versions []driver.Version
	var cursor *driver.Cursor
	for i, want := range wantVersions {
		got, err := r.Versions(ctx, ref, driver.PageReq{Limit: 1, Cursor: cursor})
		if err != nil || len(got.Items) != 1 || got.AsOf != meta.Head || !reflect.DeepEqual(got.Items[0], want) {
			t.Fatalf("ordered raw version %d = %+v, want %+v: %v", i, got, want, err)
		}
		if (got.NextCursor != nil) != (i == 0) {
			t.Fatalf("version page %d has wrong continuation: %+v", i, got.NextCursor)
		}
		versions = append(versions, got.Items...)
		cursor = got.NextCursor
	}
	if _, err := r.Changesets(ctx, nil, 1); !errors.As(err, &truncated) || truncated.Floor != meta.HistoryFloor {
		t.Fatalf("below-floor changesets = %v, want exact floor %s", err, meta.HistoryFloor)
	}
	wantChangesets := []model.Changeset{
		{Revision: model.Natural64(2), Branch: model.Natural64(0), Incarnation: meta.Incarnation, Kind: model.ChangesetRestore, Time: when, Principal: actor, Chain: []string{}, Changes: []model.ChangeRecord{{Ref: ref, Op: model.OpPatch, FromRevision: model.Natural64(1), ChangedPaths: []string{""}}}},
		{Revision: model.Natural64(3), Branch: model.Natural64(0), Incarnation: meta.Incarnation, Kind: model.ChangesetRestore, Time: when, Principal: actor, Chain: []string{}, IdempotencyKey: p.IdempotencyKey, Changes: []model.ChangeRecord{{Ref: model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0)), Op: model.OpCreate, FromRevision: model.Natural64(0), ChangedPaths: []string{""}}}},
	}
	var changesets []model.Changeset
	cursor = &driver.Cursor{Incarnation: meta.Incarnation, Revision: meta.HistoryFloor}
	for i, want := range wantChangesets {
		got, err := r.Changesets(ctx, cursor, 1)
		if err != nil || len(got) != 1 || !reflect.DeepEqual(got[0], want) {
			t.Fatalf("ordered changeset %d = %+v, want %+v: %v", i, got, want, err)
		}
		changesets = append(changesets, got...)
		cursor = &driver.Cursor{Incarnation: meta.Incarnation, Revision: got[0].Revision}
	}
	if extra, err := r.Changesets(ctx, cursor, 1); err != nil || len(extra) != 0 {
		t.Fatalf("unexpected changeset after final receipt: %+v %v", extra, err)
	}
	return reverseFixtureHistory{Meta: meta, Rows: page.Items, Versions: versions, Changesets: changesets}
}

type reverseWriteCounter struct {
	driver.Store
	writes int
}

func (s *reverseWriteCounter) ReadCheckpointInventory(ctx context.Context, view driver.View) (driver.CheckpointInventory, error) {
	capability, ok := s.Store.(driver.CheckpointInventoryStore)
	if !ok {
		return nil, driver.ErrUnsupported
	}
	return capability.ReadCheckpointInventory(ctx, view)
}

func (s *reverseWriteCounter) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	s.writes++
	return s.Store.Write(ctx, lease)
}

func assertNativeReverseUnsupported(t *testing.T, store driver.Store, meta driver.Meta) {
	t.Helper()
	ctx := context.Background()
	guard := &reverseWriteCounter{Store: store}
	data, manifest, err := restore.Dump(ctx, guard, "full")
	var truncated driver.ErrTruncated
	if !errors.As(err, &truncated) || truncated.Floor != meta.HistoryFloor || len(data) != 0 || !reflect.DeepEqual(manifest, dumpformat.Manifest{}) {
		t.Fatalf("full dump hid its retained floor or produced partial output: %+v %v", manifest, err)
	}
	data, manifest, err = restore.Dump(ctx, guard, "head")
	if err != nil {
		t.Fatal(err)
	}
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil || !archive.Native() || archive.Manifest().Scope != "plug-native-store" || manifest.NativeHead != meta.Head.String() || manifest.RecordCount != 2 || manifest.VersionCount != 2 || manifest.ChangesetCount != 0 {
		t.Fatalf("native head fixture is not valid: %+v %v", manifest, err)
	}
	result, err := reverseconverter.ConvertArchive(data)
	var refused *reverseconverter.Refusal
	if !errors.As(err, &refused) || !reflect.DeepEqual(refused.Reasons, []string{"unsupported plug-dump manifest"}) || len(result.JSONL) != 0 || result.Digest != "" || result.Report.OutputDigest != "" || refused.Report.OutputDigest != "" {
		t.Fatalf("unsupported native profile escaped the typed no-output boundary: %+v %v", result, err)
	}
	live, err := converterroundtrip.ReverseRestored(ctx, guard)
	var liveRefusal converterroundtrip.ReverseRefusal
	if !errors.As(err, &liveRefusal) || liveRefusal.Reason != "missing_source_evidence" || len(live.Result.JSONL) != 0 || live.Result.Digest != "" || live.Result.Report.OutputDigest != "" {
		t.Fatalf("native store was treated as an imported-source profile: %+v %v", live, err)
	}
	if guard.writes != 0 {
		t.Fatalf("read-only reverse boundary opened %d write transactions", guard.writes)
	}
}

// The unchanged public SQLSource fixture has a proven partial range. This
// checks Plug persistence and live head-state conversion, not Beads reimport
// or canonical native-producer qualification.
func TestPublicSourceLiveReverseAfterCloseReopen(t *testing.T) {
	ctx := context.Background()
	data, archive := doltProducerFixture(t)
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			inspection, err := converterroundtrip.InspectImport(ctx, bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			p := profile(driver.RestoreHistoryModePartial)
			p.SourceStoreID, p.ArchiveDigest, p.DerivedDigest = inspection.SourceStoreID, inspection.ArchiveSHA256, inspection.DerivedSHA256
			p, err = converterroundtrip.BindHistoryImport(ctx, bytes.NewReader(data), p, time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC))
			if err != nil {
				t.Fatal(err)
			}
			open := persistentReverseOpener(t, engine)
			store := open()
			defer func() { _ = store.Close() }()
			lease, err := store.Lease(ctx, "reverse-public-reopen", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			report, err := converterroundtrip.RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, p)
			if err != nil {
				_ = lease.Release(ctx)
				t.Fatal(err)
			}
			if err := lease.Release(ctx); err != nil {
				t.Fatal(err)
			}
			before := assertPublicReverseReopen(t, store, data, archive, report.Restored)
			closed := store
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store = open()
			if store == closed {
				t.Fatal("reopen reused the closed public-source handle")
			}
			after := assertPublicReverseReopen(t, store, data, archive, report.Restored)
			if !reflect.DeepEqual(after, before) {
				t.Fatal("physical reopen changed public reverse bytes, digest, snapshot or loss disclosure")
			}
		})
	}
}

func assertPublicReverseReopen(t *testing.T, store driver.Store, data []byte, archive *dumpformat.Archive, want driver.Meta) converterroundtrip.LiveReverse {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil || !reflect.DeepEqual(meta, want) || meta.Head != model.Natural64(3) || meta.HistoryFloor != model.Natural64(1) {
		t.Fatalf("public source lost its persisted floor/identity: %+v %v", meta, err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	assertProducerBeads(t, r, archive)
	envelope, err := r.Blob(ctx, driver.Hash(archive.SHA256()))
	r.Close()
	if err != nil || !bytes.Equal(envelope, data) {
		t.Fatalf("public source lost its exact validated envelope: %v", err)
	}
	guard := &reverseWriteCounter{Store: store}
	result, err := converterroundtrip.ReverseRestored(ctx, guard)
	if err != nil || len(result.Result.JSONL) == 0 || result.Result.Digest == "" || result.Result.Report.OutputDigest != result.Result.Digest || len(result.Result.Report.LostPostSwitchHistory) == 0 || result.Snapshot.At == nil || *result.Snapshot.At != meta.Head || result.Snapshot.Incarnation != meta.Incarnation {
		t.Fatalf("public partial source live reverse failed: %+v %v", result, err)
	}
	if guard.writes != 0 {
		t.Fatal("public source reverse opened a write transaction")
	}
	return result
}

// Both calls return distinct handles to the same disposable persistent store.
// The existing TestMain owns the PostgreSQL server and database cleanup.
func persistentReverseOpener(t *testing.T, engine string) func() driver.Store {
	t.Helper()
	dsn := filepath.Join(t.TempDir(), "reverse.db")
	if engine == "postgres" {
		dsn = newPostgresDatabase(t)
	}
	return func() driver.Store {
		t.Helper()
		var store driver.Store
		var err error
		if engine == "postgres" {
			store, err = (postgres.Driver{}).Open(context.Background(), driver.Config{DSN: dsn})
		} else {
			store, err = (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: dsn})
		}
		if err != nil {
			t.Fatal(err)
		}
		return store
	}
}
