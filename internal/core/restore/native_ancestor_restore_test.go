package restore_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

// TestNativeRestoreRebuildsAncestorClosure keeps the restore boundary
// engine-neutral while requiring the complete canonical closure for a real
// parent graph. The graph has six beads and therefore eleven rows: one root
// self row, three direct children with self+root rows, one grandchild with
// self+parent+root rows, and one unrelated root self row.
func TestNativeRestoreRebuildsAncestorClosure(t *testing.T) {
	ctx := context.Background()
	data, archive, want := nativeAncestorArchive(t)
	manifest, hash := manifestForRestore(t, archive)
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "ancestor.db")
			store := openNativeOntologyTarget(t, kind, path)
			defer store.Close()
			lease, err := store.Lease(ctx, "native-ancestor-restore", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			manager := restore.NewManager(store, lease, trustedPresenceVerifier())
			status, err := manager.Begin(ctx, manifest, "head", restore.Presence(presence))
			if err != nil {
				t.Fatal(err)
			}
			uploadManagerArchive(t, ctx, manager, status.RestoreID, data, restore.Presence(presence))
			status, err = manager.Finish(ctx, status.RestoreID, hash, model.PrincipalRef{Kind: "user", ID: "ancestor-owner"}, model.WriteMeta{Method: "admin.restore.finish", IdempotencyKey: "native-ancestor-restore"}, restore.Presence(presence))
			if err != nil || status.State != "complete" {
				t.Fatalf("native ancestor restore=%+v %v", status, err)
			}
			assertNativeSelectionBindings(t, store, lease)
			if err := lease.Release(ctx); err != nil {
				t.Fatal(err)
			}
			assertAncestorRows(t, store, want)
		})
	}
}

// Exercise the actual manager's complete source, selection and closure census
// for 20,000 roots, including shared fences, rather than a synthetic Lock call.
func TestNativeRestoreTwentyThousandRootBeadsSQLite(t *testing.T) {
	ctx := context.Background()
	const count = 20000
	source := memory.New()
	defer source.Close()
	changes := make([]conformance.FixtureChange, count)
	want := make(map[string]string, count)
	for i := range changes {
		id := fmt.Sprintf("pl-restore-root-%05d", i)
		body, err := json.Marshal(pipeline.Document{ID: id, Type: "issue", Project: "fixture", Fields: map[string]any{"title": id}})
		if err != nil {
			t.Fatal(err)
		}
		changes[i] = conformance.FixtureChange{Ref: model.RefByID(id, model.Natural64(0)), Kind: "bead", Op: model.OpCreate, Body: body}
		want[ancestorKey(id, id)] = "0"
	}
	sourceLease, err := source.Lease(ctx, "large-native-source", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conformance.Seed(ctx, source, sourceLease, driver.ChangesetHeader{}, changes); err != nil {
		t.Fatal(err)
	}
	if err := sourceLease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	data, _, err := restore.Dump(ctx, source, "head")
	if err != nil {
		t.Fatal(err)
	}
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	manifest, hash := manifestForRestore(t, archive)
	path := filepath.Join(t.TempDir(), "large-native-restore.db")
	store := openNativeOntologyTarget(t, "sqlite", path)
	defer store.Close()
	lease, err := store.Lease(ctx, "large-native-restore", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	renewCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				done <- nil
				return
			case <-ticker.C:
				if err := lease.Renew(renewCtx); err != nil {
					if errors.Is(err, context.Canceled) && renewCtx.Err() != nil {
						done <- nil
					} else {
						done <- err
					}
					return
				}
			}
		}
	}()
	stopRenewal := func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}
	// Register cleanup for failures too; stop explicitly before physical reopen.
	defer func() {
		if stopRenewal != nil {
			stopRenewal()
		}
	}()
	manager := restore.NewManager(store, lease, trustedPresenceVerifier())
	status, err := manager.Begin(ctx, manifest, "head", restore.Presence(presence))
	if err != nil {
		t.Fatal(err)
	}
	const chunkSize = 4 << 20
	for offset, page := 0, uint64(0); offset < len(data); page++ {
		end := min(offset+chunkSize, len(data))
		chunk := data[offset:end]
		if _, err := manager.Chunk(ctx, status.RestoreID, page, chunk, sha(chunk), restore.Presence(presence)); err != nil {
			t.Fatal(err)
		}
		offset = end
	}
	started := time.Now()
	status, err = manager.Finish(ctx, status.RestoreID, hash, model.PrincipalRef{Kind: "user", ID: "large-native-owner"}, model.WriteMeta{Method: "admin.restore.finish", IdempotencyKey: "large-native-restore"}, restore.Presence(presence))
	if err != nil || status.State != "complete" {
		t.Fatalf("20,000-root native SQLite restore=%+v err=%v", status, err)
	}
	elapsed := time.Since(started)
	stopRenewal()
	stopRenewal = nil
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openNativeOntologyTarget(t, "sqlite", path)
	defer reopened.Close()
	assertAncestorRows(t, reopened, want)
	meta, err := reopened.Meta(ctx)
	if err != nil || meta.Incarnation != status.Incarnation || meta.Head.String() != "1" {
		t.Fatalf("durable large native restore metadata=%+v status=%+v err=%v", meta, status, err)
	}
	reader, err := reopened.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	seen := make(map[string]bool, count)
	var cursor *driver.Cursor
	for {
		page, err := reader.Query(ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 10000, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range page.Items {
			if record.Kind != "bead" || want[ancestorKey(record.Ref.ID, record.Ref.ID)] != "0" || seen[record.Ref.ID] {
				t.Fatalf("unexpected or duplicate restored root: %+v", record)
			}
			seen[record.Ref.ID] = true
		}
		if page.NextCursor == nil {
			break
		}
		cursor = page.NextCursor
	}
	if len(seen) != count {
		t.Fatalf("durable restored root census=%d, want %d", len(seen), count)
	}
	t.Logf("actual native manager restored %d root beads with source/selection/closure guards in %s; all %d self-ancestor rows and records survived physical reopen", count, elapsed, len(want))
}

type nativeAncestorNode struct {
	id     string
	parent string
}

func nativeAncestorArchiveNodes() []nativeAncestorNode {
	return []nativeAncestorNode{
		{id: "epic"},
		{id: "todo", parent: "epic"},
		{id: "done", parent: "epic"},
		{id: "hidden", parent: "epic"},
		{id: "grandchild", parent: "todo"},
		{id: "unrelated"},
	}
}

func nativeAncestorArchive(t *testing.T) ([]byte, *dumpformat.Archive, map[string]string) {
	t.Helper()
	ctx := context.Background()
	store := memory.New()
	defer store.Close()
	ontologyRef, ontologyBody := nativeOntologySource(t, false)
	seedNativeOntology(t, store, conformance.FixtureChange{Ref: ontologyRef, Kind: "ontology", Op: model.OpCreate, Body: ontologyBody})
	lease, err := store.Lease(ctx, "native-ancestor-source", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	changes := make([]conformance.FixtureChange, 0, len(nativeAncestorArchiveNodes()))
	for _, node := range nativeAncestorArchiveNodes() {
		doc := pipeline.Document{ID: node.id, Type: "issue", Project: "fixture", Fields: map[string]any{"title": node.id}}
		if node.parent != "" {
			parent := model.RefByID(node.parent, model.Natural64(0))
			doc.Parent = &parent
		}
		body, err := json.Marshal(doc)
		if err != nil {
			_ = lease.Release(ctx)
			t.Fatal(err)
		}
		changes = append(changes, conformance.FixtureChange{Ref: model.RefByID(node.id, model.Natural64(0)), Kind: "bead", Op: model.OpCreate, Body: body})
	}
	if _, err := conformance.Seed(ctx, store, lease, driver.ChangesetHeader{}, changes); err != nil {
		_ = lease.Release(ctx)
		t.Fatal(err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	data, _, err := restore.Dump(ctx, store, "head")
	if err != nil {
		t.Fatal(err)
	}
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{}
	for _, node := range nativeAncestorArchiveNodes() {
		ancestors := []string{node.id}
		for parent := node.parent; parent != ""; {
			ancestors = append(ancestors, parent)
			parent = ancestorParent(parent)
		}
		for depth, ancestor := range ancestors {
			want[ancestorKey(node.id, ancestor)] = fmt.Sprintf("%d", depth)
		}
	}
	if len(want) != 11 {
		t.Fatalf("fixture closure rows=%d, want 11", len(want))
	}
	return data, archive, want
}

func ancestorParent(id string) string {
	for _, node := range nativeAncestorArchiveNodes() {
		if node.id == id {
			return node.parent
		}
	}
	return ""
}

func ancestorKey(descendant, ancestor string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(descendant)) + "." + base64.RawURLEncoding.EncodeToString([]byte(ancestor))
}

func assertAncestorRows(t *testing.T, store driver.Store, want map[string]string) {
	t.Helper()
	states := observeAncestorRows(t, store, want)
	for _, state := range states {
		key := state.Key.Derived.Key
		if !state.Present || state.Derived == nil || string(state.Derived.Value) != want[key] {
			t.Errorf("closure row %q=%+v, want present depth %q", key, state, want[key])
		}
	}
}

func assertNoAncestorRows(t *testing.T, store driver.Store, want map[string]string) {
	t.Helper()
	for _, state := range observeAncestorRows(t, store, want) {
		if state.Present || state.Derived != nil {
			t.Errorf("rollback left closure row=%+v", state)
		}
	}
}

func observeAncestorRows(t *testing.T, store driver.Store, want map[string]string) []driver.LockState {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	keys := make([]driver.LockKey, 0, len(want))
	for key := range want {
		rowKey := driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: key}
		keys = append(keys, driver.LockKey{Kind: driver.LockDerived, Derived: &rowKey})
	}
	sort.Slice(keys, func(i, j int) bool {
		left, _ := driver.EncodeLockKey(keys[i])
		right, _ := driver.EncodeLockKey(keys[j])
		return left < right
	})
	states, err := reader.Observe(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != len(want) {
		t.Fatalf("closure observations=%d, want %d", len(states), len(want))
	}
	return states
}

func TestNativeRestoreRejectsMalformedAncestorGraphsBeforeWrite(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		cycle bool
	}{
		{name: "missing-parent"},
		{name: "cycle", cycle: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, archive := malformedNativeAncestorArchive(t, tc.cycle)
			manifest, hash := manifestForRestore(t, archive)
			base := memory.New()
			defer base.Close()
			store := &nativeOntologyWriteCounter{Store: base}
			before, err := store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			lease, err := store.Lease(ctx, "malformed-ancestor", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			manager := restore.NewManager(store, lease, trustedPresenceVerifier())
			status, err := manager.Begin(ctx, manifest, "head", restore.Presence(presence))
			if err != nil {
				t.Fatal(err)
			}
			uploadManagerArchive(t, ctx, manager, status.RestoreID, data, restore.Presence(presence))
			_, err = manager.Finish(ctx, status.RestoreID, hash, model.PrincipalRef{Kind: "user", ID: "ancestor-owner"}, model.WriteMeta{Method: "admin.restore.finish", IdempotencyKey: "malformed-ancestor-" + tc.name}, restore.Presence(presence))
			var violation *model.Error
			if err == nil || !errors.As(err, &violation) || violation.Data.Code != "restore_violation" {
				t.Fatalf("malformed graph refusal=%T %v", err, err)
			}
			after, metaErr := store.Meta(ctx)
			if metaErr != nil || !reflect.DeepEqual(before, after) || store.writes != 0 {
				t.Fatalf("malformed graph opened %d writes or changed metadata: before=%+v after=%+v err=%v", store.writes, before, after, metaErr)
			}
			assertEmptyRestoreTarget(t, ctx, store)
		})
	}
}

func TestNativeRestoreDerivedFailureRollsBackRecordsAndRows(t *testing.T) {
	ctx := context.Background()
	data, archive, want := nativeAncestorArchive(t)
	manifest, hash := manifestForRestore(t, archive)
	base := memory.New()
	defer base.Close()
	store := &failDerivedStore{Store: base}
	before, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Lease(ctx, "derived-failure", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	manager := restore.NewManager(store, lease, trustedPresenceVerifier())
	status, err := manager.Begin(ctx, manifest, "head", restore.Presence(presence))
	if err != nil {
		t.Fatal(err)
	}
	uploadManagerArchive(t, ctx, manager, status.RestoreID, data, restore.Presence(presence))
	_, err = manager.Finish(ctx, status.RestoreID, hash, model.PrincipalRef{Kind: "user", ID: "ancestor-owner"}, model.WriteMeta{Method: "admin.restore.finish", IdempotencyKey: "derived-failure"}, restore.Presence(presence))
	if err == nil || !strings.Contains(err.Error(), "injected derived failure") {
		t.Fatalf("derived failure=%v", err)
	}
	after, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("derived failure changed metadata: before=%+v after=%+v", before, after)
	}
	assertEmptyRestoreTarget(t, ctx, store)
	assertNoAncestorRows(t, store, want)
	if len(store.sources) != 6 || len(store.staged) != 29 {
		t.Fatalf("failure preceded real source/projection staging: sources=%d rows=%d", len(store.sources), len(store.staged))
	}
	for _, table := range []driver.DerivedTable{driver.DerivedSortKey, driver.DerivedAttribute, driver.DerivedReferenceIndex, driver.DerivedAncestorClosure} {
		if store.typedCalls[table] != 1 {
			t.Fatalf("native restore staged %s in %d typed calls, want one complete batch", table, store.typedCalls[table])
		}
	}
	keys := make([]driver.LockKey, 0, len(store.staged))
	for key, row := range store.staged {
		if row.Index == nil || row.Index.Source.Key.Ref == nil || row.Index.Incarnation == before.Incarnation || row.Index.Source.Revision.String() != "0" || row.Index.Source.Digest != driver.SelectionDigest(store.sources[row.Index.Source.Key.Ref.ID].Body) {
			t.Fatal("restore fault lost actual typed new-incarnation source binding")
		}
		key := key
		keys = append(keys, driver.LockKey{Kind: driver.LockDerived, Derived: &key})
	}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	r, err := store.Read(ctx, driver.View{Incarnation: after.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	states, err := r.Observe(ctx, keys)
	_ = r.Close()
	if err != nil || len(states) != len(keys) {
		t.Fatal(err)
	}
	for _, state := range states {
		if state.Present || state.Derived != nil || !state.Revision.IsZero() {
			t.Fatal("native rollback leaked selection projection/tombstone")
		}
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: after.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	changesets, err := reader.Changesets(ctx, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(changesets) != 0 {
		t.Fatalf("derived failure left %d changesets", len(changesets))
	}
}

func malformedNativeAncestorArchive(t *testing.T, cycle bool) ([]byte, *dumpformat.Archive) {
	t.Helper()
	ctx := context.Background()
	store := memory.New()
	defer store.Close()
	ontologyRef, ontologyBody := nativeOntologySource(t, false)
	seedNativeOntology(t, store, conformance.FixtureChange{Ref: ontologyRef, Kind: "ontology", Op: model.OpCreate, Body: ontologyBody})
	lease, err := store.Lease(ctx, "malformed-ancestor-source", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	nodes := []nativeAncestorNode{{id: "orphan", parent: "missing"}}
	if cycle {
		nodes = []nativeAncestorNode{{id: "cycle-a", parent: "cycle-b"}, {id: "cycle-b", parent: "cycle-a"}}
	}
	changes := make([]conformance.FixtureChange, 0, len(nodes))
	for _, node := range nodes {
		doc := pipeline.Document{ID: node.id, Type: "issue", Project: "fixture", Fields: map[string]any{"title": node.id}}
		parent := model.RefByID(node.parent, model.Natural64(0))
		doc.Parent = &parent
		body, err := json.Marshal(doc)
		if err != nil {
			_ = lease.Release(ctx)
			t.Fatal(err)
		}
		changes = append(changes, conformance.FixtureChange{Ref: model.RefByID(node.id, model.Natural64(0)), Kind: "bead", Op: model.OpCreate, Body: body})
	}
	if _, err := conformance.Seed(ctx, store, lease, driver.ChangesetHeader{}, changes); err != nil {
		_ = lease.Release(ctx)
		t.Fatal(err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	data, _, err := restore.Dump(ctx, store, "head")
	if err != nil {
		t.Fatal(err)
	}
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return data, archive
}

type failDerivedStore struct {
	driver.Store
	staged     map[driver.DerivedRowKey]driver.DerivedRow
	sources    map[string]driver.Record
	typedCalls map[driver.DerivedTable]int
}

func (s *failDerivedStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	if s.staged == nil {
		s.staged = map[driver.DerivedRowKey]driver.DerivedRow{}
		s.sources = map[string]driver.Record{}
		s.typedCalls = map[driver.DerivedTable]int{}
	}
	return &failDerivedTx{WriteTx: tx, store: s}, nil
}

type failDerivedTx struct {
	driver.WriteTx
	store *failDerivedStore
}

func (t *failDerivedTx) RestoreNativeAllocators(ctx context.Context, snapshot driver.AllocatorSnapshot) error {
	return t.WriteTx.(driver.NativeAllocatorRestoreTx).RestoreNativeAllocators(ctx, snapshot)
}

func (t *failDerivedTx) PrepareNativeRestore(ctx context.Context, profile driver.NativeRestoreProfile) (driver.Meta, error) {
	native, ok := t.WriteTx.(driver.NativeRestoreTx)
	if !ok {
		return driver.Meta{}, errors.New("native restore capability missing")
	}
	return native.PrepareNativeRestore(ctx, profile)
}

func (t *failDerivedTx) PutDerived(context.Context, driver.DerivedTable, []driver.DerivedRow, []driver.DerivedRow) error {
	return errors.New("injected derived failure")
}

func (tx *failDerivedTx) PutGraphRecord(ctx context.Context, record driver.Record, parent *model.Ref, links []driver.SourceLink) error {
	writer, ok := tx.WriteTx.(driver.GraphSourceWriter)
	if !ok {
		return driver.ErrUnsupported
	}
	if err := writer.PutGraphRecord(ctx, record, parent, links); err != nil {
		return err
	}
	record.Body = bytes.Clone(record.Body)
	tx.store.sources[record.Ref.ID] = record
	return nil
}

func (tx *failDerivedTx) PutSelectionDerived(ctx context.Context, table driver.DerivedTable, del, add []driver.DerivedRow) error {
	writer, ok := tx.WriteTx.(driver.SelectionIndexWriter)
	if !ok {
		return driver.ErrUnsupported
	}
	if err := writer.PutSelectionDerived(ctx, table, del, add); err != nil {
		return err
	}
	tx.store.typedCalls[table]++
	for _, row := range add {
		tx.store.staged[driver.DerivedRowKey{Table: table, Key: row.Key}] = row.Clone()
	}
	if table == driver.DerivedAncestorClosure {
		return errors.New("injected derived failure after actual typed staging")
	}
	return nil
}

func assertNativeSelectionBindings(t *testing.T, store driver.Store, lease driver.ActiveLease) {
	t.Helper()
	ctx := context.Background()
	probe, err := store.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	_, typed := probe.(driver.SelectionIndexWriter)
	_ = probe.Close()
	if _, memoryEngine := store.(*memory.Store); memoryEngine && !typed {
		t.Fatal("W3 memory typed restore writer required")
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	page, err := r.Query(ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 100})
	if err != nil || len(page.Items) != 6 || page.NextCursor != nil {
		t.Fatal("native fixture census changed:", err)
	}
	sources := map[string]driver.Record{}
	for _, record := range page.Items {
		sources[record.Ref.ID] = record
	}
	for _, table := range []driver.DerivedTable{driver.DerivedSortKey, driver.DerivedAttribute, driver.DerivedReferenceIndex, driver.DerivedAncestorClosure} {
		rows, err := r.DerivedRows(ctx, table, "")
		if err != nil {
			t.Fatal(err)
		}
		want := 6
		if table == driver.DerivedAncestorClosure {
			want = 11
		}
		if len(rows) != want {
			t.Fatalf("native %s projection count=%d want%d", table, len(rows), want)
		}
		for _, row := range rows {
			index := row.Index
			if !typed {
				if index != nil {
					t.Fatal("unsupported writer certified native selection")
				}
				continue
			}
			if index == nil || index.Source.Key.Kind != driver.LockRecord || index.Source.Key.Ref == nil {
				t.Fatal("typed native restore omitted source annotation")
			}
			source, ok := sources[index.Source.Key.Ref.ID]
			if !ok || index.Incarnation != meta.Incarnation || index.Source.Revision.String() != source.Revision.String() || source.Revision.String() != "1" || index.Source.Digest != driver.SelectionDigest(source.Body) || !index.Source.Present || index.Validate(table, row) != nil {
				t.Fatal("native annotation lost admitted incarnation/exact source binding")
			}
		}
	}
}

func TestNativeRestoreRejectsUnrelatedInvalidGraphBeforeWrite(t *testing.T) {
	ctx := context.Background()
	for _, malformed := range []string{"blocking-cycle", "dangling-custom-link", "blocking-ancestor", "legacy-parent-cycle"} {
		t.Run(malformed, func(t *testing.T) {
			source := memory.New()
			defer source.Close()
			lease, err := source.Lease(ctx, "invalid-graph-source", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Release(ctx)
			docs := []pipeline.Document{
				{ID: "pl-parent", Type: "issue", Fields: map[string]any{}},
				{ID: "pl-child", Type: "issue", Parent: &model.Ref{ID: "pl-parent"}, Fields: map[string]any{}},
				{ID: "pl-u", Type: "issue", Fields: map[string]any{}},
				{ID: "pl-v", Type: "issue", Fields: map[string]any{}},
			}
			switch malformed {
			case "blocking-cycle":
				docs[2].Links = []pipeline.Link{{Kind: "blocks", To: model.Ref{ID: "pl-v"}}}
				docs[3].Links = []pipeline.Link{{Kind: "blocks", To: model.Ref{ID: "pl-u"}}}
			case "dangling-custom-link":
				docs[2].Links = []pipeline.Link{{Kind: "custom-link", To: model.Ref{ID: "pl-missing"}}}
			case "blocking-ancestor":
				docs[3].Parent = &model.Ref{ID: "pl-u"}
				docs[3].Links = []pipeline.Link{{Kind: "waits-for", To: model.Ref{ID: "pl-u"}}}
			case "legacy-parent-cycle":
				docs[2].Links = []pipeline.Link{{Kind: "parent-child", To: model.Ref{ID: "pl-v"}}}
				docs[3].Links = []pipeline.Link{{Kind: "parent-child", To: model.Ref{ID: "pl-u"}}}
			}
			changes := make([]conformance.FixtureChange, 0, len(docs))
			for _, doc := range docs {
				body, err := json.Marshal(doc)
				if err != nil {
					t.Fatal(err)
				}
				changes = append(changes, conformance.FixtureChange{Ref: model.Ref{ID: doc.ID}, Kind: "bead", Op: model.OpCreate, Body: body})
			}
			if _, err := conformance.Seed(ctx, source, lease, driver.ChangesetHeader{}, changes); err != nil {
				t.Fatal(err)
			}
			data, _, err := restore.Dump(ctx, source, "head")
			if err != nil {
				t.Fatal(err)
			}
			archive, err := dumpformat.Read(bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			manifest, hash := manifestForRestore(t, archive)
			for _, backend := range []string{"memory", "sqlite"} {
				t.Run(backend, func(t *testing.T) {
					base := openNativeOntologyTarget(t, backend, filepath.Join(t.TempDir(), "invalid-graph.db"))
					defer base.Close()
					store := &nativeOntologyWriteCounter{Store: base}
					before, err := store.Meta(ctx)
					if err != nil {
						t.Fatal(err)
					}
					targetLease, err := store.Lease(ctx, "invalid-graph-target", driver.LeaseTTL)
					if err != nil {
						t.Fatal(err)
					}
					defer targetLease.Release(ctx)
					manager := restore.NewManager(store, targetLease, trustedPresenceVerifier())
					status, err := manager.Begin(ctx, manifest, "head", restore.Presence(presence))
					if err != nil {
						t.Fatal(err)
					}
					uploadManagerArchive(t, ctx, manager, status.RestoreID, data, restore.Presence(presence))
					_, err = manager.Finish(ctx, status.RestoreID, hash, model.PrincipalRef{Kind: "user", ID: "operator"}, model.WriteMeta{Method: "admin.restore.finish", IdempotencyKey: "invalid-graph-" + malformed}, restore.Presence(presence))
					var violation *model.Error
					if !errors.As(err, &violation) || violation.Data.Code != "restore_violation" {
						t.Fatalf("unrelated invalid component was admitted: %v", err)
					}
					after, err := store.Meta(ctx)
					if err != nil || before != after || store.writes != 0 {
						t.Fatalf("invalid graph changed target: before=%+v after=%+v writes=%d err=%v", before, after, store.writes, err)
					}
					assertEmptyRestoreTarget(t, ctx, store)
					r, err := store.Read(ctx, driver.View{Incarnation: after.Incarnation})
					if err != nil {
						t.Fatal(err)
					}
					defer r.Close()
					states, err := r.Observe(ctx, []driver.LockKey{pipeline.GraphValidityLockKey()})
					if err != nil || len(states) != 1 || states[0].Present {
						t.Fatalf("invalid graph left a semantic certificate: %+v %v", states, err)
					}
				})
			}
		})
	}
}

func TestNativeRestoreOnlyCertifiesTypedNonemptySourceGraphs(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"empty", "ontology-only", "typed-bead", "untyped-bead"} {
		t.Run(mode, func(t *testing.T) {
			source := memory.New()
			defer source.Close()
			if mode == "ontology-only" {
				ref, body := nativeOntologySource(t, false)
				seedNativeOntology(t, source, conformance.FixtureChange{Ref: ref, Kind: "ontology", Op: model.OpCreate, Body: body})
			} else if mode != "empty" {
				seedNativeOntology(t, source, conformance.FixtureChange{Ref: model.Ref{ID: "pl-source"}, Kind: "bead", Op: model.OpCreate, Body: []byte(`{"id":"pl-source","type":"issue","fields":{}}`)})
			}
			data, _, err := restore.Dump(ctx, source, "head")
			if err != nil {
				t.Fatal(err)
			}
			archive, err := dumpformat.Read(bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			manifest, hash := manifestForRestore(t, archive)
			base := memory.New()
			defer base.Close()
			var target driver.Store = base
			if mode == "untyped-bead" {
				// This supported legacy restore wrapper deliberately exposes no
				// GraphSourceWriter, so core falls back to untyped publication.
				target = &nativeRestoreFaultStore{Store: base}
			}
			lease, err := target.Lease(ctx, "certificate-restore", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Release(ctx)
			manager := restore.NewManager(target, lease, trustedPresenceVerifier())
			status, err := manager.Begin(ctx, manifest, "head", restore.Presence(presence))
			if err != nil {
				t.Fatal(err)
			}
			uploadManagerArchive(t, ctx, manager, status.RestoreID, data, restore.Presence(presence))
			status, err = manager.Finish(ctx, status.RestoreID, hash, model.PrincipalRef{Kind: "user", ID: "operator"}, model.WriteMeta{Method: "admin.restore.finish", IdempotencyKey: "certificate-" + mode}, restore.Presence(presence))
			if err != nil || status.State != "complete" {
				t.Fatalf("restore=%+v err=%v", status, err)
			}
			assertCertificate := func() {
				t.Helper()
				r, err := base.Read(ctx, driver.View{Incarnation: status.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				states, err := r.Observe(ctx, []driver.LockKey{pipeline.GraphValidityLockKey(), driver.GraphBlockingSentinelLock})
				if err != nil || len(states) != 2 {
					t.Fatalf("certificate observations=%+v err=%v", states, err)
				}
				want := mode == "typed-bead"
				if states[0].Present != want || want && (!states[1].Present || states[0].Revision.String() != states[1].Revision.String()) {
					t.Fatalf("mode=%s semantic certificate=%+v", mode, states)
				}
			}
			assertCertificate()
			if mode != "typed-bead" {
				if err := pipeline.BuildSourceIndexes(ctx, base, lease); err != nil {
					t.Fatal(err)
				}
				assertCertificate()
			}
		})
	}
}
