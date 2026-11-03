package nightlyrestore_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Change one archived record before the real driver's Put. Preparation,
// allocators, commit, live reads and base versions all use the real engine;
// the source archive, retained blobs and restore/archive marker stay intact.
type snapshotContentFaultStore struct {
	driver.Store
	targetID      string
	field         string
	value         json.RawMessage
	before, after []byte
	puts          int
}

type snapshotContentFaultTx struct {
	driver.WriteTx
	driver.RestoreImportTx
	driver.AllocatorContinuation
	fault *snapshotContentFaultStore
}

func (s *snapshotContentFaultStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return snapshotContentFaultTx{tx, tx.(driver.RestoreImportTx), tx.(driver.AllocatorContinuation), s}, nil
}

func (tx snapshotContentFaultTx) Put(ctx context.Context, row driver.Record) error {
	if row.Ref.ID == tx.fault.targetID && tx.fault.field != "" {
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

func TestNightlySnapshotArchiveContent(t *testing.T) {
	ctx := context.Background()
	data := snapshotFixture(t, publicFixture(t))
	a, p := readArchive(t, data), profile(t, data)
	sources := a.Records("beads-archive")
	if len(sources) == 0 {
		t.Fatal("snapshot fixture has no archived content")
	}
	source := sources[0]
	ref := model.RefByID(sourceTargetID(source), model.Natural64(0))
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, tc := range []struct {
			name, field string
			value       json.RawMessage
		}{
			{name: "matching"},
			{name: "wrong-source-id", field: "source_id", value: json.RawMessage(`"different-public-source-record"`)},
			{name: "missing-source-id", field: "source_id"},
			{name: "wrong-source-table", field: "source_table", value: json.RawMessage(`"different_public_source_table"`)},
			{name: "missing-source-table", field: "source_table"},
		} {
			t.Run(engine+"/"+tc.name, func(t *testing.T) {
				store, _ := freshStore(t, engine)
				fault := &snapshotContentFaultStore{Store: store, targetID: ref.ID, field: tc.field, value: tc.value}
				restored, err := converterroundtrip.RestoreImport(ctx, bytes.NewReader(data), fault, p)
				if err != nil {
					t.Fatal(err)
				}
				meta, err := store.Meta(ctx)
				if err != nil || meta.Head.String() != "1" || meta.HistoryFloor.String() != "1" {
					t.Fatal("content fixture is not an actual snapshot restore", err)
				}
				same(t, "committed restore metadata", meta, restored.Restored)
				r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				rows, err := r.Get(ctx, []model.Ref{ref})
				if err != nil || len(rows) != 1 || rows[0].Kind != source.Kind || rows[0].Ref != ref || rows[0].Revision.String() != "1" {
					t.Fatal("committed archived record is absent", err)
				}
				versions, err := r.Versions(ctx, ref, driver.PageReq{Limit: 2})
				if err != nil || len(versions.Items) != 1 || versions.NextCursor != nil {
					t.Fatal("committed archived record base version is absent", err)
				}
				base := versions.Items[0]
				if !base.Base || base.Deleted || base.Ref != ref || base.Revision.String() != "1" || !bytes.Equal(base.Body, rows[0].Body) || base.CreatedAt.IsZero() {
					t.Fatal("archived content fault did not retain matching live/base-version bytes")
				}
				same(t, "archived content restore actor", base.Principal, p.Admission.Actor)
				if tc.field == "" {
					if fault.puts != 0 {
						t.Fatal("matching control unexpectedly injected a fault")
					}
					collect(t, store, a, p)
					return
				}
				if fault.puts != 1 || !bytes.Equal(rows[0].Body, fault.after) {
					t.Fatal("chosen archived content fault was not persisted exactly once")
				}
				var before, after map[string]json.RawMessage
				if json.Unmarshal(fault.before, &before) != nil || json.Unmarshal(rows[0].Body, &after) != nil {
					t.Fatal("invalid archived content fault fixture")
				}
				same(t, "original source_id", before["source_id"], source.ID)
				same(t, "original source_table", before["source_table"], source.Source.Table)
				if bytes.Equal(before[tc.field], after[tc.field]) || !bytes.Equal(after[tc.field], tc.value) {
					t.Fatal("chosen archived content field did not change")
				}
				delete(before, tc.field)
				delete(after, tc.field)
				same(t, "all other archived content and policy flags", after, before)
				collect(t, store, a, p)
				t.Fatalf("snapshot census silently accepted %s in a real %s restore", tc.name, engine)
			})
		}
	}
}
