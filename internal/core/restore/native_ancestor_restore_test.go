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
			if err := lease.Release(ctx); err != nil {
				t.Fatal(err)
			}
			assertAncestorRows(t, store, want)
		})
	}
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
}

func (s *failDerivedStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return &failDerivedTx{WriteTx: tx}, nil
}

type failDerivedTx struct {
	driver.WriteTx
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
