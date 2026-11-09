package memory

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestSelectionNativeRestoreUsesAdmittedIncarnation(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease := sourceLease(t, s)
	ref := sourceRef("restored")
	key := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: ref.ID}
	snapshot, err := driver.NewAllocatorSnapshot(driver.Meta{StoreID: "selection-native", Incarnation: "source-image"}, model.Natural64(1), model.Natural64(0), driver.AllocatorInventory{})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := snapshot.Binding()
	if err != nil {
		t.Fatal(err)
	}
	fence, err := driver.NativeAllocatorRestoreLock()
	if err != nil {
		t.Fatal(err)
	}
	tx, h := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{fence, {Kind: driver.LockDerived, Derived: &key}}, false, ref)
	old := tx.Snapshot().Incarnation
	profile := driver.NativeRestoreProfile{StoreID: "selection-native", Principal: model.PrincipalRef{Kind: "user", ID: "operator"}, Method: "admin.restore.finish", IdempotencyKey: "selection-restore", RequestHash: strings.Repeat("d", 64), AllocatorBinding: binding}
	meta, err := tx.PrepareNativeRestore(ctx, profile)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Incarnation == old || tx.Snapshot().Incarnation != old {
		t.Fatal("fixture lost admitted/snapshot incarnation distinction")
	}
	if err = tx.RestoreNativeAllocators(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"id":"restored","type":"issue","fields":{"template":false,"priority":0}}`)
	if err = tx.PutGraphRecord(ctx, driver.Record{Ref: ref, Kind: "bead", Body: body}, nil, nil); err != nil {
		t.Fatal(err)
	}
	value := []byte("canonical logical sort value")
	row := driver.DerivedRow{Key: key.Key, Value: value, Index: &driver.SelectionIndex{Version: driver.SelectionIndexVersion, Row: key, Incarnation: old, Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(value), Source: driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Revision: model.Natural64(0), Present: true, Digest: driver.SelectionDigest(body)}, Bead: &driver.SelectionBead{Ref: ref, Type: "issue", Absent: []driver.SelectionColumn{driver.SelectionStatus, driver.SelectionTitle}}}}
	if err = tx.PutSelectionDerived(ctx, key.Table, nil, []driver.DerivedRow{row}); !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("old incarnation accepted in restore: %v", err)
	}
	row.Index.Incarnation = meta.Incarnation
	if err = tx.PutSelectionDerived(ctx, key.Table, nil, []driver.DerivedRow{row}); err != nil {
		t.Fatal(err)
	}
	bead, _ := driver.NewBeadSentinel(ref.ID)
	if err = tx.BumpSentinels(ctx, []driver.Sentinel{bead, driver.SentinelGraphBlocking}); err != nil {
		t.Fatal(err)
	}
	h.Incarnation, h.Kind, h.Method, h.Principal, h.IdempotencyKey, h.RequestHash = meta.Incarnation, model.ChangesetRestore, profile.Method, profile.Principal, profile.IdempotencyKey, profile.RequestHash
	commitSourceTest(t, tx, h)
	r := sourceReader(t, s, nil)
	states, err := r.Observe(ctx, []driver.LockKey{{Kind: driver.LockDerived, Derived: &key}})
	if err != nil || len(states) != 1 {
		t.Fatalf("restored row: %v", err)
	}
	got := states[0].Derived.Index
	if got.Incarnation != meta.Incarnation || got.Source.Revision.String() != model.Natural64(1).String() || got.Source.Digest != driver.SelectionDigest(body) {
		t.Fatalf("restore stamp=%+v", got)
	}
	control := model.RefByKey("selection-native-control")
	boundary, err := driver.PrepareSelectionBoundary(driver.SelectionBoundarySpec{Version: 1, PreparedBinding: driver.SelectionDigest([]byte("restore-conformance-v1")), Incarnation: meta.Incarnation, Branch: model.Natural64(0), AuthorityRevision: model.Natural64(1), Sources: []driver.SelectionSource{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &control}, Revision: model.Natural64(0)}}, Chain: driver.SelectionChainNone, Authorization: driver.SelectionExpr{Op: driver.SelectionTrue}, Host: driver.SelectionHostAbsent})
	if err != nil {
		t.Fatal(err)
	}
	plan := driver.SelectionPlan{Incarnation: meta.Incarnation, Branch: model.Natural64(0), ContentRevision: model.Natural64(1), AuthorityRevision: model.Natural64(1), Membership: driver.SelectionExpr{Op: driver.SelectionTrue}, Authorization: driver.SelectionExpr{Op: driver.SelectionTrue}, Boundary: boundary, Mode: driver.SelectionCountMode}
	if got, err := r.Select(ctx, plan); err != nil || got.Total != 1 {
		t.Fatalf("restored selection=%+v/%v", got, err)
	}
}
