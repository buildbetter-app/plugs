package compaction_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
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

	Predicates []driver.LockState
}

// Observe uses the same encoded ordering as Lock, but reads only committed
// predicate state at the reader's snapshot. Include both eligible children.
func b09B10PredicateKeys(t *testing.T, children ...model.Ref) []driver.LockKey {
	t.Helper()
	if len(children) != 2 {
		t.Fatal("predicate inventory requires both eligible children")
	}
	keys := []driver.LockKey{driver.GraphBlockingSentinelLock}
	for _, child := range children {
		sentinel, err := driver.NewBeadSentinel(child.ID)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
	}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	if err := driver.ValidateLockKeys(keys); err != nil {
		t.Fatal(err)
	}
	return keys
}

// The oracle reads committed bytes, complete histories, every declared derived
// table, allocator inventory, actual receipt and predicate states. It imports no
// compaction selection, delta or projection helper. A supplied reader stays pinned.
func b09B10ReadImage(t *testing.T, r driver.Reader, floor model.Revision, key string, predicates []driver.LockKey) b09B10Image {
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
	if err := driver.ValidateLockKeys(predicates); err != nil {
		t.Fatal(err)
	}
	image.Predicates, err = r.Observe(ctx, predicates)
	if err != nil || len(image.Predicates) != len(predicates) {
		t.Fatalf("incomplete committed predicate inventory: %+v %v", image.Predicates, err)
	}
	for i, state := range image.Predicates {
		want, err := driver.EncodeLockKey(predicates[i])
		if err != nil {
			t.Fatal(err)
		}
		got, err := driver.EncodeLockKey(state.Key)
		if err != nil || got != want {
			t.Fatalf("predicate observation identity/order differs at %d: want %q got %q err=%v", i, want, got, err)
		}
	}
	return image
}

func b09B10FreshImage(t *testing.T, f *fixture, key string, predicates []driver.LockKey) b09B10Image {
	t.Helper()
	meta := f.meta()
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	return b09B10ReadImage(t, r, meta.HistoryFloor, key, predicates)
}

func b09B10Equal(t *testing.T, label string, want, got any) {
	t.Helper()
	if !reflect.DeepEqual(want, got) {
		a, _ := json.Marshal(want)
		b, _ := json.Marshal(got)
		t.Fatalf("%s changed:\nwant %s\n got %s", label, a, b)
	}
}

// Compaction conserves the full inventory. Exactly five selected-child rows
// need fresh source provenance; its counting row also carries the canonical
// Ready leaf, whose full semantic preimage includes source revision and fields.
// All expectations start from the before image, never the published rows.
func b09B10CompactedDerived(t *testing.T, before, after b09B10Image, selected string) map[driver.DerivedTable][]driver.DerivedRow {
	t.Helper()
	selectedRecord := func(image b09B10Image) driver.Record {
		var found []driver.Record
		for _, record := range image.Records {
			if record.Ref.ID == selected {
				found = append(found, record)
			}
		}
		if len(found) != 1 || found[0].Kind != "bead" {
			t.Fatalf("selected source inventory is incomplete: %+v", found)
		}
		return found[0]
	}
	oldSource, source := selectedRecord(before), selectedRecord(after)
	if after.View.At == nil || source.Revision.String() != after.View.At.String() || source.Ref != oldSource.Ref || string(source.Body) == string(oldSource.Body) {
		t.Fatal("compaction did not publish the selected changed source at its committed revision")
	}
	digest := func(body []byte) string {
		sum := sha256.Sum256(body)
		return hex.EncodeToString(sum[:])
	}
	membership := func(record driver.Record) string {
		var doc struct {
			ID, Type, Project string
			Fields            map[string]any
		}
		decoder := json.NewDecoder(strings.NewReader(string(record.Body)))
		decoder.UseNumber()
		if err := decoder.Decode(&doc); err != nil {
			t.Fatal(err)
		}
		status, _ := doc.Fields["status"].(string)
		item := struct {
			ID       string         `json:"id"`
			Type     string         `json:"type"`
			Project  string         `json:"project"`
			Status   string         `json:"status"`
			Revision model.Revision `json:"revision"`
			Fields   map[string]any `json:"fields,omitempty"`
		}{doc.ID, doc.Type, doc.Project, status, record.Revision, doc.Fields}
		body, err := json.Marshal(item)
		if err != nil {
			t.Fatal(err)
		}
		return digest(body)
	}
	want := make(map[driver.DerivedTable][]driver.DerivedRow, len(before.Derived))
	refreshed := map[driver.DerivedTable]int{}
	for table, rows := range before.Derived {
		if rows == nil {
			want[table] = nil
			continue
		}
		want[table] = make([]driver.DerivedRow, len(rows))
		for i, row := range rows {
			row = row.Clone()
			index := row.Index
			if index != nil && index.Source.Key.Kind == driver.LockRecord && index.Source.Key.Ref != nil && index.Source.Key.Ref.ID == selected {
				// PostgreSQL stores annotations with gob, which omits Ref's
				// private JSON-arm discriminator. The complete canonical lock
				// identity still includes the reference arm, ID and branch.
				sameRef := driver.SelectionSameRef(*index.Source.Key.Ref, oldSource.Ref)
				validationErr := index.Validate(table, row)
				if validationErr != nil || !sameRef || !index.Source.Present || index.Source.Revision.String() != oldSource.Revision.String() || index.Source.Digest != digest(oldSource.Body) {
					t.Fatalf("baseline selected annotation does not bind its exact source: table=%s key=%s validation=%v canonical_ref=%t present=%t revision=%s/%s digest=%s/%s", table, row.Key, validationErr, sameRef, index.Source.Present, index.Source.Revision.String(), oldSource.Revision.String(), index.Source.Digest, digest(oldSource.Body))
				}
				if *index.Source.Key.Ref != oldSource.Ref && table == driver.DerivedSortKey {
					t.Logf("selected source has exact canonical identity despite private reference metadata: annotation=%#v record=%#v", *index.Source.Key.Ref, oldSource.Ref)
				}
				index.Source.Revision, index.Source.Digest = source.Revision, digest(source.Body)
				if index.Bead != nil {
					if index.Bead.MembershipLeaf != membership(oldSource) {
						t.Fatal("baseline counting leaf does not bind its exact source revision/body")
					}
					bead := *index.Bead
					bead.MembershipLeaf = membership(source)
					index.Bead = &bead
					body, err := json.Marshal(bead)
					if err != nil {
						t.Fatal(err)
					}
					row.Value, index.ValueDigest = body, digest(body)
				}
				refreshed[table]++
			}
			want[table][i] = row
		}
	}
	b09B10Equal(t, "complete selected annotation census", map[driver.DerivedTable]int{
		driver.DerivedAncestorClosure: 2,
		driver.DerivedAttribute:       1,
		driver.DerivedReferenceIndex:  1,
		driver.DerivedSortKey:         1,
	}, refreshed)
	return want
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
