package compaction_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This is fixture setup through the existing guarded restore port. It seeds
// live hash policy; all parent/child records are then made by the public Writer.
// It is neither a reference recording nor an allocator continuation test.
func b09B10SeedHash(t *testing.T, f *fixture) {
	t.Helper()
	ctx := context.Background()
	key, err := driver.AllocatorLock("b12")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := f.store.Write(ctx, f.lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, err = tx.Lock(ctx, driver.LockSet{{Key: key, Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	restore, ok := tx.(driver.OfflineRestoreTx)
	if !ok {
		t.Fatal("real store lacks guarded fixture restore")
	}
	meta, err := restore.PrepareOfflineRestore(ctx, "b09-b10-compaction-fixture")
	if err != nil {
		t.Fatal(err)
	}
	allocator, ok := tx.(driver.AllocatorContinuation)
	if !ok {
		t.Fatal("real store lacks allocator setup")
	}
	if err = allocator.RestoreAllocators(ctx, []driver.OfflineAllocator{driver.NewHashAllocator("b12", 3, 10, 0.25)}); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Commit(ctx, driver.ChangesetHeader{
		Incarnation: meta.Incarnation, Kind: model.ChangesetRestore,
		Principal: model.PrincipalRef{Kind: "system", ID: "plug-converter"},
		Method:    "offline.restore", IdempotencyKey: "b09-b10-compaction-seed",
		RequestHash: strings.Repeat("b", 64), Time: f.clock,
		Response: model.Commit{Results: []model.OpResult{}, Warnings: []model.Warning{}},
		LockKeys: []driver.LockKey{key},
	})
	if err != nil {
		t.Fatal(err)
	}
	f.bind()
}

type b09B10Image struct {
	View      driver.View
	Records   []driver.Record
	Versions  map[string][]driver.Version
	Log       []model.Changeset
	Derived   map[driver.DerivedTable][]driver.DerivedRow
	Allocator driver.AllocatorSnapshot
	Receipt   *driver.IdempotencyRecord
}

// The oracle reads committed bytes, complete histories, every declared derived
// table, allocator inventory and the actual receipt. It imports no compaction
// selection, delta or projection helper. A supplied reader stays pinned.
func b09B10ReadImage(t *testing.T, r driver.Reader, floor model.Revision, key string) b09B10Image {
	t.Helper()
	ctx := context.Background()
	image := b09B10Image{View: r.Snapshot(), Versions: map[string][]driver.Version{}, Derived: map[driver.DerivedTable][]driver.DerivedRow{}}
	page, err := r.Query(ctx, driver.Query{Limit: 1000})
	if err != nil || page.NextCursor != nil {
		t.Fatalf("incomplete committed record inventory: %+v %v", page, err)
	}
	image.Records = page.Items
	for _, record := range image.Records {
		versions, err := r.Versions(ctx, record.Ref, driver.PageReq{Limit: 1000})
		if err != nil || versions.NextCursor != nil {
			t.Fatalf("incomplete versions for %s: %+v %v", record.Ref.ID, versions, err)
		}
		image.Versions[record.Ref.ID] = versions.Items
	}
	var cursor *driver.Cursor
	if !floor.IsZero() {
		cursor = &driver.Cursor{Incarnation: image.View.Incarnation, Revision: floor, Method: "changesets"}
	}
	image.Log, err = r.Changesets(ctx, cursor, 1000)
	if err != nil || len(image.Log) >= 1000 {
		t.Fatalf("incomplete changeset inventory: %v", err)
	}
	for _, table := range []driver.DerivedTable{
		driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure,
		driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex,
		driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins,
	} {
		image.Derived[table], err = r.DerivedRows(ctx, table, "")
		if err != nil {
			t.Fatal(err)
		}
	}
	allocator, ok := r.(driver.AllocatorSnapshotReader)
	if !ok {
		t.Fatal("real reader lacks complete allocator snapshot")
	}
	image.Allocator, err = allocator.AllocatorSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	image.Receipt, err = r.IdempotencyByKey(ctx, "anonymous:anonymous", "b12-public-recorder", "admin.compact", writeMeta("admin.compact", key).IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	return image
}

func b09B10FreshImage(t *testing.T, f *fixture, key string) b09B10Image {
	t.Helper()
	meta := f.meta()
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	return b09B10ReadImage(t, r, meta.HistoryFloor, key)
}

func b09B10Equal(t *testing.T, label string, want, got any) {
	t.Helper()
	if !reflect.DeepEqual(want, got) {
		a, _ := json.Marshal(want)
		b, _ := json.Marshal(got)
		t.Fatalf("%s changed:\nwant %s\n got %s", label, a, b)
	}
}

// SQL is an independent connection to the real disposable database. Include
// every durable data table, even empty ones; exclude only lease/lock mechanics.
// Ordering encoded whole rows avoids relying on engine-specific primary keys.
func b09B10SQLImage(t *testing.T, f *fixture) map[string][]string {
	t.Helper()
	if f.engine == "memory" {
		return nil
	}
	name, schema := "sqlite", ""
	tables := []string{"meta", "objects", "records", "versions", "key_history", "blobs", "changesets", "replays", "checkpoints", "core_sessions", "allocator_states"}
	if f.engine == "postgres" {
		name, schema = "pgx", "plug_postgres."
		tables = []string{"meta", "records", "versions", "states", "blobs", "changesets", "receipts", "checkpoints", "core_sessions", "allocator_states"}
	}
	db, err := sql.Open(name, f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	image := map[string][]string{}
	for _, table := range tables {
		rows, err := db.QueryContext(ctx, "SELECT * FROM "+schema+table)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		image[table] = []string{}
		for rows.Next() {
			values, pointers := make([]any, len(columns)), make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				_ = rows.Close()
				t.Fatal(err)
			}
			row := make([]string, len(values))
			for i, value := range values {
				row[i] = fmt.Sprintf("%T:%v", value, value)
			}
			raw, err := json.Marshal(row)
			if err != nil {
				_ = rows.Close()
				t.Fatal(err)
			}
			image[table] = append(image[table], string(raw))
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		sort.Strings(image[table])
	}
	return image
}
