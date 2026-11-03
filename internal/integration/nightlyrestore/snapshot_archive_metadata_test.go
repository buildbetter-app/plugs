package nightlyrestore_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Inject a storage fault only in the retained marker's Put. The normal import
// preparation, allocator restore, commit, versions and persisted reads still
// use the real driver. No source archive or admitted profile is changed.
type snapshotMarkerFaultStore struct {
	driver.Store
	field         string
	value         json.RawMessage
	before, after []byte
	puts          int
}

type snapshotMarkerFaultTx struct {
	driver.WriteTx
	driver.RestoreImportTx
	driver.AllocatorContinuation
	fault *snapshotMarkerFaultStore
}

func (s *snapshotMarkerFaultStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return snapshotMarkerFaultTx{tx, tx.(driver.RestoreImportTx), tx.(driver.AllocatorContinuation), s}, nil
}

func (tx snapshotMarkerFaultTx) Put(ctx context.Context, row driver.Record) error {
	if row.Ref.ID == "restore/archive" && tx.fault.field != "" {
		var body map[string]json.RawMessage
		if err := json.Unmarshal(row.Body, &body); err != nil {
			return err
		}
		tx.fault.before = append([]byte(nil), row.Body...)
		if tx.fault.value == nil {
			delete(body, tx.fault.field)
		} else {
			body[tx.fault.field] = tx.fault.value
		}
		var err error
		row.Body, err = json.Marshal(body)
		if err != nil {
			return err
		}
		tx.fault.after = append([]byte(nil), row.Body...)
		tx.fault.puts++
	}
	return tx.WriteTx.Put(ctx, row)
}

func TestNightlySnapshotArchiveMetadata(t *testing.T) {
	ctx := context.Background()
	data := snapshotFixture(t, publicFixture(t))
	a, p := readArchive(t, data), profile(t, data)
	for _, kind := range []string{"memory", "sqlite", "postgres"} {
		for _, tc := range []struct {
			name, field string
			value       json.RawMessage
		}{
			{name: "matching"},
			{name: "wrong-source-store", field: "source_store", value: json.RawMessage(`"different-public-fixture-store"`)},
			{name: "missing-source-store", field: "source_store"},
			{name: "wrong-profile", field: "profile", value: json.RawMessage(`"unsupported-head/1"`)},
			{name: "missing-profile", field: "profile"},
		} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				store, _ := freshStore(t, kind)
				fault := &snapshotMarkerFaultStore{Store: store, field: tc.field, value: tc.value}
				restored, err := converterroundtrip.RestoreImport(ctx, bytes.NewReader(data), fault, p)
				if err != nil {
					t.Fatal(err)
				}
				meta, err := store.Meta(ctx)
				if err != nil || meta.Head.String() != "1" || meta.HistoryFloor.String() != "1" {
					t.Fatal("fault fixture is not an actual snapshot restore", err)
				}
				same(t, "committed restore metadata", meta, restored.Restored)
				r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				rows, err := r.Get(ctx, []model.Ref{model.RefByID("restore/archive", model.Natural64(0))})
				if err != nil || len(rows) != 1 || rows[0].Kind != "restore-archive" {
					t.Fatal("committed archive marker is absent", err)
				}
				versions, err := r.Versions(ctx, rows[0].Ref, driver.PageReq{Limit: 2})
				if err != nil || len(versions.Items) != 1 || versions.NextCursor != nil {
					t.Fatal("committed marker base version is absent", err)
				}
				base := versions.Items[0]
				if !base.Base || base.Deleted || base.Revision.String() != "1" || !bytes.Equal(base.Body, rows[0].Body) {
					t.Fatal("marker fault did not retain matching live/base-version bytes")
				}
				_, err = snapshotArchiveMembers(rows[0].Body, a, p)
				if tc.field == "" {
					if err != nil || fault.puts != 0 {
						t.Fatal("matching retained metadata was refused", err)
					}
					collect(t, store, a, p)
					return
				}
				if fault.puts != 1 || !bytes.Equal(rows[0].Body, fault.after) {
					t.Fatal("chosen marker fault was not persisted exactly once")
				}
				var before, after map[string]json.RawMessage
				if json.Unmarshal(fault.before, &before) != nil || json.Unmarshal(rows[0].Body, &after) != nil {
					t.Fatal("invalid marker fault fixture")
				}
				same(t, "original source identity", before["source_store"], string(p.SourceStoreID))
				same(t, "original archive profile", before["profile"], "builtin-head/1")
				if bytes.Equal(before[tc.field], after[tc.field]) || !bytes.Equal(after[tc.field], tc.value) {
					t.Fatal("chosen retained metadata field did not change")
				}
				delete(before, tc.field)
				delete(after, tc.field)
				same(t, "all other marker data and digests", after, before)
				if err == nil {
					// The original comparison also accepts this entire corrupted
					// snapshot: matching versions/digests do not prove identity.
					collect(t, store, a, p)
					t.Fatalf("snapshot census silently accepted %s in a real %s restore", tc.name, kind)
				}
				if !strings.Contains(err.Error(), tc.field) {
					t.Fatalf("comparison failed for a cause other than %s: %v", tc.field, err)
				}
			})
		}
	}
}
