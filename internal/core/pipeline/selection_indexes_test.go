package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type selectionStartupStore struct {
	driver.Store
	onDecode func(int)
}

func measuredStartupApply(t *testing.T, h *harness, label string, cs model.ChangeSet, metadata model.WriteMeta) model.Commit {
	t.Helper()
	collector := NewCollector()
	started := time.Now()
	result, err := h.writer(WithObserver(collector)).Apply(context.Background(), by, cs, metadata)
	snapshot := collector.Snapshot()
	preMS, commitMS := 0.0, 0.0
	// This is the sum of observed preparation lanes, not an elapsed deadline
	// interval. Commit has its own lane; Response also includes post-commit
	// receipt loading and must not be counted as preparation.
	for _, phase := range []Phase{PhaseAdmission, PhaseReplay, PhasePrepare, PhasePre, PhaseGraph, PhaseClaims, PhaseChanges, PhaseAudit} {
		for _, millis := range snapshot.LaneMillis[phase] {
			preMS += millis
		}
	}
	for _, millis := range snapshot.LaneMillis[PhaseCommit] {
		commitMS += millis
	}
	t.Logf("write_timing label=%s operations=%d pre_phase_lanes_ms=%.3f commit_ms=%.3f wall_ms=%.3f error=%v", label, len(cs.Ops), preMS, commitMS, float64(time.Since(started))/float64(time.Millisecond), err)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func (s selectionStartupStore) BuildGraphSourceIndexes(ctx context.Context, lease driver.ActiveLease, decode driver.GraphSourceDecoder, projections ...driver.GraphSourceProjection) error {
	calls := 0
	return s.Store.(driver.GraphSourceIndexBuilder).BuildGraphSourceIndexes(ctx, lease, func(record driver.Record) (driver.GraphSourceMetadata, error) {
		calls++
		if s.onDecode != nil {
			s.onDecode(calls)
		}
		return decode(record)
	}, projections...)
}

func startupSelectionPlan(t *testing.T, r driver.Reader) driver.SelectionPlan {
	t.Helper()
	v := r.Snapshot()
	control := model.RefByKey("startup-selection-control")
	p := driver.SelectionPlan{Incarnation: v.Incarnation, Branch: model.Natural64(0), ContentRevision: *v.At, AuthorityRevision: *v.At, Mode: driver.SelectionCountMode, Membership: driver.SelectionExpr{Op: driver.SelectionTrue}, Authorization: driver.SelectionExpr{Op: driver.SelectionTrue}}
	var err error
	p.Boundary, err = driver.PrepareSelectionBoundary(driver.SelectionBoundarySpec{Version: 1, PreparedBinding: driver.SelectionDigest([]byte("actual-startup-control")), Incarnation: v.Incarnation, Branch: model.Natural64(0), AuthorityRevision: *v.At, Sources: []driver.SelectionSource{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &control}, Revision: model.Natural64(0)}}, Chain: driver.SelectionChainNone, Authorization: p.Authorization, Host: driver.SelectionHostAbsent})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// Publish real legacy sources and logical closures, without any selection
// annotation. Graph source metadata is explicit so selection alone is unbuilt.
func seedUnannotatedStartupChain(t *testing.T, h *harness, count int) []model.Ref {
	t.Helper()
	ctx := context.Background()
	refs := make([]model.Ref, count)
	changes := make([]conformance.FixtureChange, count)
	var closure []driver.DerivedRow
	for i := range refs {
		refs[i] = model.RefByID(fmt.Sprintf("pl-legacy-%03d", i), model.Natural64(0))
		var parent *model.Ref
		if i == 1 || i == 2 {
			value := refs[i-1]
			parent = &value
		}
		body, err := json.Marshal(Document{ID: refs[i].ID, Type: "issue", Project: "test", Parent: parent, Fields: map[string]any{"title": "legacy source", "status": "todo", "template": false, "priority": 0}})
		if err != nil {
			t.Fatal(err)
		}
		changes[i] = conformance.FixtureChange{Ref: refs[i], Kind: "bead", Op: model.OpCreate, Body: body, Graph: &conformance.FixtureGraphSource{Parent: parent}}
		targets := []model.Ref{refs[i]}
		if i == 1 || i == 2 {
			targets = append(targets, refs[i-1])
			if i == 2 {
				targets = append(targets, refs[0])
			}
		}
		for depth, target := range targets {
			row, err := AncestorRow(refs[i].ID, target.ID, uint64(depth))
			if err != nil {
				t.Fatal(err)
			}
			closure = append(closure, row)
		}
	}
	if _, err := conformance.Seed(ctx, h.store, h.lease, driver.ChangesetHeader{}, changes); err != nil {
		t.Fatal(err)
	}
	keys := []driver.LockKey{driver.GraphBlockingSentinelLock}
	sentinels := []driver.Sentinel{driver.SentinelGraphBlocking}
	for _, row := range closure {
		keys = append(keys, ancestorLock(row.Key))
	}
	for _, ref := range refs {
		sentinel, err := driver.NewClosureSentinel(ref.ID)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
		sentinels = append(sentinels, sentinel)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	locks := make(driver.LockSet, len(keys))
	for i, key := range keys {
		locks[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	tx, err := h.store.Write(ctx, h.lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, err := tx.Lock(ctx, locks); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.PutDerived(ctx, driver.DerivedAncestorClosure, nil, closure); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(ctx, sentinels); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: "pipeline-test", Kind: model.ChangesetDerived, Principal: by, LockKeys: keys}); err != nil {
		t.Fatal(err)
	}
	return refs
}

func TestSelectionStartupCatchesOrdinarySaveOnUnannotatedNonrootChain(t *testing.T) {
	h := setup(t)
	refs := seedUnannotatedStartupChain(t, h, driver.SelectionBackfillPageSize+4)
	before := h.read(t)
	for _, ref := range refs {
		for _, state := range projectionStates(t, before, ref) {
			if state.Present {
				t.Fatal("legacy setup accidentally published a selection row")
			}
		}
		for _, row := range projectionClosure(t, before, ref) {
			if row.Index != nil {
				t.Fatal("legacy setup accidentally annotated closure")
			}
		}
	}
	oldClosure := projectionClosure(t, before, refs[2])
	if len(oldClosure) != 3 {
		t.Fatal("legacy positive must contain a nonroot three-depth chain")
	}
	prebuild := *before.Snapshot().At
	var saved driver.Reader
	store := selectionStartupStore{Store: h.store, onDecode: func(n int) {
		if n != driver.SelectionBackfillPageSize+1 || saved != nil {
			return
		}
		if err := h.lease.Renew(context.Background()); err != nil {
			t.Fatal(err)
		}
		measuredStartupApply(t, h, "cold_body_save", model.ChangeSet{Ops: []model.Op{patch(refs[2], map[string]any{"title": "ordinary legacy save between pages"})}}, meta(91))
		saved = h.read(t)
		body := projectionSource(t, saved, refs[2])
		for _, row := range projectionClosure(t, saved, refs[2]) {
			if row.Index == nil || row.Index.Source.Revision.String() != body.Revision.String() || row.Index.Source.Digest != driver.SelectionDigest(body.Body) {
				t.Fatal("ordinary legacy save did not publish its complete bound closure")
			}
		}
		// Neither ancestor has been written by Writer. Their Parent metadata
		// remains private to startup, so it cannot be a publication prerequisite.
		for _, ref := range refs[:2] {
			for _, state := range projectionStates(t, saved, ref) {
				if state.Present {
					t.Fatal("ordinary leaf save rewrote an unwritten ancestor")
				}
			}
		}
		if _, err := saved.(driver.SelectionReader).Select(context.Background(), startupSelectionPlan(t, saved)); !errors.Is(err, driver.ErrSelectionUnavailable) {
			t.Fatalf("legacy page became readable before final activation: %v", err)
		}
	}}
	decode, projection := sourceSelectionDecoder("pipeline-test")
	if err := store.BuildGraphSourceIndexes(context.Background(), h.lease, decode, projection); err != nil {
		t.Fatalf("complete ordinary legacy publication prevented catch-up: %v", err)
	}
	if saved == nil {
		t.Fatal("ordinary legacy save did not run between source pages")
	}
	after := h.read(t)
	if *saved.Snapshot().At != *after.Snapshot().At || !reflect.DeepEqual(projectionSource(t, saved, refs[2]), projectionSource(t, after, refs[2])) {
		t.Fatal("startup overwrote the ordinary save or allocated a new Head")
	}
	gotClosure := projectionClosure(t, after, refs[2])
	if len(gotClosure) != len(oldClosure) {
		t.Fatal("startup changed logical legacy closure membership")
	}
	for i, row := range gotClosure {
		if row.Key != oldClosure[i].Key || !bytes.Equal(row.Value, oldClosure[i].Value) || row.Index == nil {
			t.Fatal("startup changed legacy logical bytes or lost a completed annotation")
		}
	}
	result, err := after.(driver.SelectionReader).Select(context.Background(), startupSelectionPlan(t, after))
	if err != nil || result.Total != uint64(len(refs)) {
		t.Fatalf("legacy final census=%+v/%v", result, err)
	}
	plan := startupSelectionPlan(t, after)
	plan.ContentRevision = prebuild
	if _, err := after.(driver.SelectionReader).Select(context.Background(), plan); !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("legacy build certified pre-build history: %v", err)
	}
}

func TestSelectionStartupCatchesColdAncestorMoveBetweenPages(t *testing.T) {
	ctx := context.Background()
	h := setup(t)
	refs := seedUnannotatedStartupChain(t, h, driver.SelectionBackfillPageSize+4)
	before := h.read(t)
	descendant := projectionSource(t, before, refs[2])
	oldClosure := projectionClosure(t, before, refs[2])
	if len(oldClosure) != 3 {
		t.Fatal("cold move must start with a nonroot three-depth chain")
	}
	for _, row := range oldClosure {
		if row.Index != nil {
			t.Fatal("cold move setup already has typed closure")
		}
	}
	var durable driver.Reader
	store := selectionStartupStore{Store: h.store, onDecode: func(n int) {
		if n != driver.SelectionBackfillPageSize+1 || durable != nil {
			return
		}
		measuredStartupApply(t, h, "cold_ancestor_move", model.ChangeSet{Ops: []model.Op{{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: refs[1], Parent: &refs[3]}}}}, meta(92))
		durable = h.read(t)
		if !reflect.DeepEqual(descendant, projectionSource(t, durable, refs[2])) {
			t.Fatal("ordinary ancestor move rewrote descendant source")
		}
		for _, state := range projectionStates(t, durable, refs[2]) {
			if state.Present {
				t.Fatal("closure-only descendant unexpectedly published a nonclosure family")
			}
		}
		rows := projectionClosure(t, durable, refs[2])
		if len(rows) != 3 {
			t.Fatal("ordinary move did not publish the complete descendant closure")
		}
		targets := []model.Ref{refs[2], refs[1], refs[3]}
		seen := map[uint64]bool{}
		for _, row := range rows {
			child, target, depth, err := DecodeAncestorRow(row)
			if err != nil || child != refs[2].ID || depth >= uint64(len(targets)) || seen[depth] || target != targets[depth].ID || row.Index == nil {
				t.Fatalf("ordinary move left incomplete, raw or stale descendant membership: key=%q child=%q target=%q depth=%d seen=%v annotation=%v decode=%v", row.Key, child, target, depth, seen[depth], row.Index != nil, err)
			}
			seen[depth] = true
			if row.Index.Source.Key.Ref == nil || !driver.SelectionSameRef(*row.Index.Source.Key.Ref, refs[2]) || row.Index.Source.Revision.String() != descendant.Revision.String() || row.Index.Source.Digest != driver.SelectionDigest(descendant.Body) || row.Index.Validate(driver.DerivedAncestorClosure, row) != nil {
				t.Fatal("ordinary move lost exact unchanged-descendant source binding")
			}
		}
		if _, err := durable.(driver.SelectionReader).Select(ctx, startupSelectionPlan(t, durable)); !errors.Is(err, driver.ErrSelectionUnavailable) {
			t.Fatalf("cold move certified an unfinished startup: %v", err)
		}
	}}
	decode, projection := sourceSelectionDecoder("pipeline-test")
	if err := store.BuildGraphSourceIndexes(ctx, h.lease, decode, projection); err != nil {
		t.Fatalf("complete cold closure-only move invalidated catch-up: %v", err)
	}
	if durable == nil {
		t.Fatal("cold ancestor move did not run between pages")
	}
	after := h.read(t)
	if *after.Snapshot().At != *durable.Snapshot().At || !reflect.DeepEqual(projectionClosure(t, after, refs[2]), projectionClosure(t, durable, refs[2])) {
		t.Fatal("startup changed durable move closure bytes, annotation or final Head")
	}
	var chains []driver.DerivedSelectionRow
	for _, ref := range refs {
		if !reflect.DeepEqual(projectionSource(t, after, ref), projectionSource(t, durable, ref)) {
			t.Fatal("startup rewrote a durable source")
		}
		for _, state := range projectionStates(t, after, ref) {
			if state.Derived == nil || state.Derived.Index == nil {
				t.Fatal("final current source census is incomplete")
			}
			chains = append(chains, driver.DerivedSelectionRow{Table: state.Key.Derived.Table, Row: *state.Derived})
		}
		for _, row := range projectionClosure(t, after, ref) {
			chains = append(chains, driver.DerivedSelectionRow{Table: driver.DerivedAncestorClosure, Row: row})
		}
		want, err := durable.Versions(ctx, ref, driver.PageReq{})
		if err != nil {
			t.Fatal(err)
		}
		got, err := after.Versions(ctx, ref, driver.PageReq{})
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatal("startup changed durable source history")
		}
	}
	if err := driver.ValidateSelectionParentChains(chains); err != nil {
		t.Fatalf("final current Parent/Ancestor proof: %v", err)
	}
	history, err := h.store.Read(ctx, before.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	defer history.Close()
	if !reflect.DeepEqual(projectionSource(t, history, refs[2]), descendant) || !reflect.DeepEqual(projectionClosure(t, history, refs[2]), oldClosure) {
		t.Fatal("startup changed the original raw source/closure history")
	}
	result, err := after.(driver.SelectionReader).Select(ctx, startupSelectionPlan(t, after))
	if err != nil || result.Total != uint64(len(refs)) {
		t.Fatalf("cold move final census=%+v/%v", result, err)
	}
	plan := startupSelectionPlan(t, after)
	plan.ContentRevision = *before.Snapshot().At
	if _, err := after.(driver.SelectionReader).Select(ctx, plan); !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("cold move startup certified before the final Head: %v", err)
	}
}

// These are actual accepted closure publications on a cold population. The
// raw-then-typed case leaves a completely repaired final image and must still
// refuse: only the same successful typed call may declare a legacy removal.
func TestSelectionStartupColdClosurePublicationProvenance(t *testing.T) {
	for _, mode := range []string{"complete", "delete_source", "raw_delete", "raw_then_typed", "wrong_delete_value", "incomplete_add", "incomplete_remove", "wrong_source", "failed_staging", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			h := setup(t)
			refs := seedUnannotatedStartupChain(t, h, driver.SelectionBackfillPageSize+4)
			before := h.read(t)
			source := projectionSource(t, before, refs[2])
			old := projectionClosure(t, before, refs[2])
			decode, projection := sourceSelectionDecoder("pipeline-test")
			projected, err := projection.Decode(source, old)
			if err != nil || len(projected) != 3 {
				t.Fatalf("cold full closure setup: %v", err)
			}
			var full []driver.DerivedRow
			var removed driver.DerivedRow
			for i, row := range old {
				if row.Index != nil {
					t.Fatal("provenance setup is not cold")
				}
				full = append(full, projected[i].Row.Clone())
				_, target, _, err := DecodeAncestorRow(row)
				if err != nil {
					t.Fatal(err)
				}
				if target == refs[0].ID {
					removed = row.Clone()
				}
			}
			if removed.Key == "" {
				t.Fatal("provenance control lacks an obsolete ancestor input")
			}
			fired := false
			store := selectionStartupStore{Store: h.store, onDecode: func(n int) {
				if n != driver.SelectionBackfillPageSize+1 || fired {
					return
				}
				fired = true
				if mode == "delete_source" {
					apply(t, h.writer(), model.ChangeSet{Ops: []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: refs[2]}}}}, meta(93))
					return
				}
				closurePredicate, err := driver.NewClosureSentinel(refs[2].ID)
				if err != nil {
					t.Fatal(err)
				}
				keys := []driver.LockKey{driver.GraphBlockingSentinelLock, {Kind: driver.LockSentinel, Sentinel: closurePredicate}, {Kind: driver.LockRecord, Ref: &refs[1]}, {Kind: driver.LockRecord, Ref: &refs[2]}}
				for _, row := range old {
					keys = append(keys, ancestorLock(row.Key))
				}
				sort.Slice(keys, func(i, j int) bool {
					a, _ := driver.EncodeLockKey(keys[i])
					b, _ := driver.EncodeLockKey(keys[j])
					return a < b
				})
				locks := make(driver.LockSet, len(keys))
				for i, key := range keys {
					locks[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
				}
				tx, err := h.store.Write(ctx, h.lease)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Close()
				if _, err := tx.Lock(ctx, locks); err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Head(ctx); err != nil {
					t.Fatal(err)
				}
				var del []driver.DerivedRow
				add := full
				switch mode {
				case "raw_delete", "raw_then_typed", "failed_staging":
					if err := tx.PutDerived(ctx, driver.DerivedAncestorClosure, []driver.DerivedRow{removed}, nil); err != nil {
						t.Fatal(err)
					}
					if mode == "raw_delete" {
						add = nil
					} else if mode == "failed_staging" {
						bad := full[0].Clone()
						bad.Index.ValueDigest = driver.SelectionDigest([]byte("wrong logical bytes"))
						if err := PutSelectionDerived(ctx, tx, driver.DerivedAncestorClosure, nil, []driver.DerivedRow{bad}); err == nil {
							t.Fatal("invalid typed staging unexpectedly succeeded")
						}
						add = nil
					}
				case "wrong_delete_value":
					wrong := removed.Clone()
					wrong.Value = []byte("not the stored legacy input")
					del = []driver.DerivedRow{wrong}
				case "incomplete_add":
					add = full[:1]
				case "incomplete_remove":
					del, add = []driver.DerivedRow{removed}, full[:1]
				case "wrong_source":
					other := projectionSource(t, before, refs[1])
					for i := range full {
						full[i].Index.Source = driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &refs[1]}, Revision: other.Revision, Present: true, Digest: driver.SelectionDigest(other.Body)}
					}
				case "rollback":
					del = []driver.DerivedRow{removed}
				}
				if err := PutSelectionDerived(ctx, tx, driver.DerivedAncestorClosure, del, add); err != nil {
					t.Fatal(err)
				}
				if mode == "rollback" {
					return
				}
				if err := tx.BumpSentinels(ctx, []driver.Sentinel{driver.SentinelGraphBlocking, closurePredicate}); err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: "pipeline-test", Kind: model.ChangesetDerived, Principal: by, LockKeys: keys}); err != nil {
					t.Fatal(err)
				}
			}}
			err = store.BuildGraphSourceIndexes(ctx, h.lease, decode, projection)
			if !fired {
				t.Fatal("cold provenance publication did not run between pages")
			}
			if mode == "complete" || mode == "delete_source" || mode == "rollback" {
				if err != nil {
					t.Fatalf("complete/rolled-back cold publication invalidated build: %v", err)
				}
			} else if !errors.Is(err, driver.ErrSelectionUnavailable) {
				t.Fatalf("incomplete or unowned cold delta certified: %v", err)
			}
			after := h.read(t)
			if mode == "delete_source" {
				records, err := after.Get(ctx, []model.Ref{refs[2]})
				if err != nil || len(records) != 0 || len(projectionClosure(t, after, refs[2])) != 0 {
					t.Fatal("cold source deletion left an orphan source/closure")
				}
				states, err := after.Observe(ctx, []driver.LockKey{{Kind: driver.LockRecord, Ref: &refs[2]}})
				if err != nil || len(states) != 1 || states[0].Present || states[0].Record != nil || states[0].Revision.String() != after.Snapshot().At.String() {
					t.Fatal("cold source deletion lost its exact committed tombstone")
				}
			} else if !reflect.DeepEqual(source, projectionSource(t, after, refs[2])) {
				t.Fatal("closure provenance control rewrote descendant source")
			}
		})
	}
}

func TestBodyOnlyWriteDoesNotRepairStaleUnannotatedParentChain(t *testing.T) {
	h := setup(t)
	refs := seedUnannotatedStartupChain(t, h, 3)
	before := h.read(t)
	closure := projectionClosure(t, before, refs[2])
	source := projectionSource(t, before, refs[2])
	doc, err := DecodeDocument(source.Body)
	if err != nil {
		t.Fatal(err)
	}
	doc.Parent = nil
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	rewriteGraphOnly(t, h, source, body)
	apply(t, h.writer(), model.ChangeSet{Ops: []model.Op{patch(refs[2], map[string]any{"title": "ordinary write remains accepted"})}}, meta(92))
	if got := projectionClosure(t, h.read(t), refs[2]); !reflect.DeepEqual(got, closure) {
		t.Fatal("ordinary save recertified stale but contiguous legacy closure")
	}
	decode, projection := sourceSelectionDecoder("pipeline-test")
	if err := h.store.BuildGraphSourceIndexes(context.Background(), h.lease, decode, projection); !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("stale legacy Parent chain became certified: %v", err)
	}
}

func TestSelectionStartupPreservesIndependentDerivedEnvelopeBetweenPages(t *testing.T) {
	ctx := context.Background()
	h := setup(t)
	refs := seedUnannotatedStartupChain(t, h, driver.SelectionBackfillPageSize+4)
	source := driver.DerivedRowKey{Table: driver.DerivedGrant, Key: "independent/opaque-source"}
	key := driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: "independent/complete-labels"}
	grantPredicate, err := driver.NewGrantSentinel("independent")
	if err != nil {
		t.Fatal(err)
	}
	closurePredicate, err := driver.NewClosureSentinel(refs[2].ID)
	if err != nil {
		t.Fatal(err)
	}
	keys := []driver.LockKey{{Kind: driver.LockSentinel, Sentinel: grantPredicate}, {Kind: driver.LockSentinel, Sentinel: closurePredicate}, {Kind: driver.LockDerived, Derived: &source}, {Kind: driver.LockDerived, Derived: &key}}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	publish := func(update bool) {
		t.Helper()
		r := h.read(t)
		observed, err := r.Observe(ctx, []driver.LockKey{{Kind: driver.LockDerived, Derived: &source}})
		if err != nil || len(observed) != 1 {
			t.Fatal(err)
		}
		tx, err := h.store.Write(ctx, h.lease)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Close()
		locks := make(driver.LockSet, len(keys))
		for i, key := range keys {
			locks[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
		}
		if _, err := tx.Lock(ctx, locks); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Head(ctx); err != nil {
			t.Fatal(err)
		}
		value := []byte("complete independent source")
		if update {
			value = []byte("changed complete independent source")
		}
		if err := tx.PutDerived(ctx, source.Table, nil, []driver.DerivedRow{{Key: source.Key, Value: value}}); err != nil {
			t.Fatal(err)
		}
		if !update {
			row := driver.DerivedRow{Key: key.Key, Value: []byte("opaque independent logical labels")}
			row.Index = &driver.SelectionIndex{Version: driver.SelectionIndexVersion, Row: key, Incarnation: "pipeline-test", Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(row.Value), Source: driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &source}, Revision: observed[0].Revision, Present: true, Digest: driver.SelectionDigest(value)}, Facts: &driver.SelectionFacts{Kind: driver.SelectionLabelFact, Rows: []driver.SelectionFact{{Kind: driver.SelectionLabelFact, Ref: &refs[2], Name: "own-label", Value: "own-value"}}}}
			if err := PutSelectionDerived(ctx, tx, key.Table, nil, []driver.DerivedRow{row}); err != nil {
				t.Fatal(err)
			}
		}
		if err := tx.BumpSentinels(ctx, []driver.Sentinel{grantPredicate, closurePredicate}); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: "pipeline-test", Kind: model.ChangesetDerived, Principal: by, LockKeys: keys}); err != nil {
			t.Fatal(err)
		}
	}
	var published driver.Reader
	store := selectionStartupStore{Store: h.store, onDecode: func(n int) {
		if n == driver.SelectionBackfillPageSize+1 && published == nil {
			publish(false)
			published = h.read(t)
		}
	}}
	decode, projection := sourceSelectionDecoder("pipeline-test")
	if err := store.BuildGraphSourceIndexes(ctx, h.lease, decode, projection); err != nil {
		t.Fatalf("independent derived-source Label was classified as a build orphan: %v", err)
	}
	if published == nil {
		t.Fatal("independent complete envelope did not publish between pages")
	}
	after := h.read(t)
	want, err := published.Observe(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	got, err := after.Observe(ctx, keys)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("startup changed an independent envelope/source Key/Value/Index")
	}
	history, err := h.store.Read(ctx, driver.View{Incarnation: "pipeline-test", At: published.Snapshot().At})
	if err != nil {
		t.Fatal(err)
	}
	defer history.Close()
	old, err := history.Observe(ctx, keys)
	if err != nil || !reflect.DeepEqual(old, want) {
		t.Fatal("startup rewrote independent envelope history")
	}
	plan := startupSelectionPlan(t, after)
	plan.Membership = driver.SelectionExpr{Op: driver.SelectionExistsOp, Exists: &driver.SelectionExists{Row: key, Kind: driver.SelectionLabelFact, Tests: []driver.SelectionFactTest{{Column: driver.SelectionFactRef, Candidate: driver.SelectionID}}}}
	result, err := after.(driver.SelectionReader).Select(ctx, plan)
	if err != nil || result.Total != 1 {
		t.Fatalf("independent source admission was lost: %+v/%v", result, err)
	}
	publish(true) // Change its actual source without replacing the complete set.
	current := h.read(t)
	plan = startupSelectionPlan(t, current)
	plan.Membership = driver.SelectionExpr{Op: driver.SelectionExistsOp, Exists: &driver.SelectionExists{Row: key, Kind: driver.SelectionLabelFact}}
	if result, err := current.(driver.SelectionReader).Select(ctx, plan); !errors.Is(err, driver.ErrSelectionUnavailable) || !reflect.DeepEqual(result, driver.SelectionResult{}) {
		t.Fatal("startup weakened the independent envelope's exact own-source check")
	}
}

func TestSelectionStartupReconcilesOrdinarySavesBetweenPages(t *testing.T) {
	h := setup(t)
	ops := make([]model.Op, 260)
	for i := range ops {
		ops[i] = create("")
	}
	ops[0].Create.Key = "startup-root"
	middle := create("startup-middle")
	middle.Create.Parent = &model.Ref{Key: "startup-root"}
	leaf := create("")
	leaf.Create.Parent = &model.Ref{Key: "startup-middle"}
	ops = append(ops, middle, leaf)
	created := measuredStartupApply(t, h, "initial_262_operations", model.ChangeSet{Ops: ops}, meta(1))
	refs := make([]model.Ref, len(created.Results))
	for i := range refs {
		refs[i] = created.Results[i].Ref
	}
	before := h.read(t)
	oldLeaf := projectionSource(t, before, refs[261])
	oldClosure := projectionClosure(t, before, refs[261])
	if len(oldClosure) != 3 {
		t.Fatal("positive chain must have three depths")
	}
	// This actually accepted graph-only save creates a selection gap, while
	// graph coverage stays ready. The selection phase must still run.
	source := projectionSource(t, before, refs[0])
	rewriteGraphOnly(t, h, source, source.Body)
	unbuilt := h.read(t)
	prebuild := *unbuilt.Snapshot().At
	if _, err := unbuilt.(driver.SelectionReader).Select(context.Background(), startupSelectionPlan(t, unbuilt)); !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("graph-only write did not leave the expected gap: %v", err)
	}
	var durable driver.Reader
	var newRef model.Ref
	raced := false
	store := selectionStartupStore{Store: h.store, onDecode: func(n int) {
		if n != driver.SelectionBackfillPageSize+1 || raced {
			return
		}
		raced = true
		// The first page has completed. Actual normal Writer transactions
		// must progress now; no scan-wide graph/Head fence may block them.
		measuredStartupApply(t, h, "racing_body_save", model.ChangeSet{Ops: []model.Op{patch(refs[1], map[string]any{"title": "saved between pages"})}}, meta(3))
		measuredStartupApply(t, h, "racing_delete", model.ChangeSet{Ops: []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: refs[2]}}}}, meta(4))
		newRef = measuredStartupApply(t, h, "racing_create", model.ChangeSet{Ops: []model.Op{create("")}}, meta(5)).Results[0].Ref
		measuredStartupApply(t, h, "racing_ancestor_move", model.ChangeSet{Ops: []model.Op{{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: refs[260], Parent: &refs[3]}}}}, meta(6))
		durable = h.read(t)
		unchangedLeaf := projectionSource(t, durable, refs[261])
		if !reflect.DeepEqual(unchangedLeaf, oldLeaf) {
			t.Fatal("move unexpectedly rewrote descendant source")
		}
		if reflect.DeepEqual(projectionClosure(t, durable, refs[261]), oldClosure) {
			t.Fatal("move control did not change descendant closure")
		}
		if _, err := durable.(driver.SelectionReader).Select(context.Background(), startupSelectionPlan(t, durable)); !errors.Is(err, driver.ErrSelectionUnavailable) {
			t.Fatalf("a staged page certified early: %v", err)
		}
	}}
	decode, projection := sourceSelectionDecoder("pipeline-test")
	if err := store.BuildGraphSourceIndexes(context.Background(), h.lease, decode, projection); err != nil {
		t.Fatal(err)
	}
	if !raced || durable == nil {
		t.Fatal("second source page never reached the real write-race control")
	}
	after := h.read(t)
	if *after.Snapshot().At != *durable.Snapshot().At {
		t.Fatal("startup allocated a graph revision")
	}
	for _, ref := range []model.Ref{refs[0], refs[1], refs[3], refs[260], refs[261], newRef} {
		if !reflect.DeepEqual(projectionSource(t, after, ref), projectionSource(t, durable, ref)) {
			t.Fatal("startup rewrote durable source")
		}
		for _, row := range projectionStates(t, after, ref) {
			if row.Derived == nil || row.Derived.Index == nil || row.Derived.Index.Source.Digest != driver.SelectionDigest(projectionSource(t, after, ref).Body) {
				t.Fatal("final projection lost a racing save binding")
			}
		}
	}
	for _, ref := range []model.Ref{refs[0], refs[261], newRef} {
		want := projectionClosure(t, durable, ref)
		got := projectionClosure(t, after, ref)
		if len(got) != len(want) {
			t.Fatal("startup changed closure membership")
		}
		for i := range got {
			if got[i].Key != want[i].Key || !bytes.Equal(got[i].Value, want[i].Value) {
				t.Fatal("startup changed logical closure bytes")
			}
		}
	}
	logBefore, err := durable.Changesets(context.Background(), nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	logAfter, err := after.Changesets(context.Background(), nil, 100)
	if err != nil || !reflect.DeepEqual(logBefore, logAfter) {
		t.Fatal("startup rewrote history")
	}
	result, err := after.(driver.SelectionReader).Select(context.Background(), startupSelectionPlan(t, after))
	if err != nil || result.Total != uint64(len(refs)) {
		t.Fatalf("final selected census=%+v/%v", result, err)
	}
	plan := startupSelectionPlan(t, after)
	plan.ContentRevision = prebuild
	if _, err := after.(driver.SelectionReader).Select(context.Background(), plan); !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("final annotations certified prebuild history: %v", err)
	}
}

// Publish an actually accepted partial optional-index delta through the core
// source adapter. Every remaining ancestor must already bind the final source;
// a backfill must not repair its missing publication from raw final values.
func partialStartupPublication(t *testing.T, h *harness, ref model.Ref, mode string) {
	t.Helper()
	ctx := context.Background()
	r := h.read(t)
	before := projectionSource(t, r, ref)
	doc, err := DecodeDocument(before.Body)
	if err != nil {
		t.Fatal(err)
	}
	doc.Metadata = map[string]json.RawMessage{"race": json.RawMessage(`"partial"`)}
	if mode == "partial_parent" {
		doc.Parent = nil
	}
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	record := before
	record.Body = body
	plan, err := PrepareSourceRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	var self driver.DerivedRow
	for _, row := range projectionClosure(t, r, ref) {
		_, _, depth, err := DecodeAncestorRow(row)
		if err != nil {
			t.Fatal(err)
		}
		if depth == 0 {
			self = row.Clone()
		}
	}
	if self.Index == nil {
		t.Fatal("partial control requires bound self and ancestor rows")
	}
	keys := plan.LockKeys()
	selfKey := driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: self.Key}
	keys = append(keys, driver.LockKey{Kind: driver.LockDerived, Derived: &selfKey})
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	locks := make(driver.LockSet, len(keys))
	for i, key := range keys {
		locks[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	tx, err := h.store.Write(ctx, h.lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, err := tx.Lock(ctx, locks); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if mode == "raw_closure" {
		self.Index = nil
		if err := tx.PutDerived(ctx, selfKey.Table, nil, []driver.DerivedRow{self}); err != nil {
			t.Fatal(err)
		}
	} else {
		if err := plan.Publish(ctx, tx); err != nil {
			t.Fatal(err)
		}
		self.Index.Source.Digest = driver.SelectionDigest(body)
		self.Index.Source.Revision = before.Revision
		if err := PutSelectionDerived(ctx, tx, selfKey.Table, nil, []driver.DerivedRow{self}); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.BumpSentinels(ctx, plan.Sentinels()); err != nil {
		t.Fatal(err)
	}
	if mode == "rollback" {
		return
	}
	header := driver.ChangesetHeader{Incarnation: "pipeline-test", Kind: model.ChangesetWrite, Principal: by, LockKeys: keys}
	if mode != "raw_closure" {
		header.Changes = []model.ChangeRecord{{Ref: ref, Op: model.OpPatch, FromRevision: before.Revision}}
	}
	if _, err := tx.Commit(ctx, header); err != nil {
		t.Fatal(err)
	}
}

func TestSelectionStartupRejectsIncompleteInterpageDeltas(t *testing.T) {
	for _, mode := range []string{"partial_binding", "partial_parent", "raw_closure", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			h := setup(t)
			w := h.writer()
			ops := make([]model.Op, 130)
			for i := range ops {
				ops[i] = create("")
			}
			roots := apply(t, w, model.ChangeSet{Ops: ops}, meta(1)).Results
			childOp := create("")
			childOp.Create.Parent = &roots[0].Ref
			child := apply(t, w, model.ChangeSet{Ops: []model.Op{childOp}}, meta(2)).Results[0].Ref
			original := projectionSource(t, h.read(t), roots[0].Ref)
			rewriteGraphOnly(t, h, original, original.Body)
			fired := false
			store := selectionStartupStore{Store: h.store, onDecode: func(n int) {
				if n == driver.SelectionBackfillPageSize+1 && !fired {
					fired = true
					partialStartupPublication(t, h, child, mode)
				}
			}}
			decode, projection := sourceSelectionDecoder("pipeline-test")
			err := store.BuildGraphSourceIndexes(context.Background(), h.lease, decode, projection)
			if !fired {
				t.Fatal("paused-build mutation did not run")
			}
			if mode == "rollback" {
				if err != nil {
					t.Fatalf("rolled-back delta dirtied the build: %v", err)
				}
			} else if !errors.Is(err, driver.ErrSelectionUnavailable) {
				t.Fatalf("incomplete accepted delta was repaired into certification: %v", err)
			}
		})
	}
}

func TestSelectionStartupInterruptionAndLeaseRenewal(t *testing.T) {
	for _, lose := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "lease_loss"}[lose], func(t *testing.T) {
			h := setup(t)
			ops := make([]model.Op, 130)
			for i := range ops {
				ops[i] = create("")
			}
			ref := apply(t, h.writer(), model.ChangeSet{Ops: ops}, meta(1)).Results[0].Ref
			r := h.read(t)
			source := projectionSource(t, r, ref)
			rewriteGraphOnly(t, h, source, source.Body)
			before := h.read(t)
			states := projectionStates(t, before, ref)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			renewals := 0
			store := selectionStartupStore{Store: h.store, onDecode: func(n int) {
				if n > driver.SelectionBackfillPageSize+1 {
					return
				}
				if err := h.lease.Renew(context.Background()); err != nil {
					t.Fatal(err)
				}
				renewals++
				if n == driver.SelectionBackfillPageSize+1 {
					if lose {
						if err := h.lease.Release(context.Background()); err != nil {
							t.Fatal(err)
						}
					} else {
						cancel()
					}
				}
			}}
			decode, projection := sourceSelectionDecoder("pipeline-test")
			if err := store.BuildGraphSourceIndexes(ctx, h.lease, decode, projection); err == nil {
				t.Fatal("interrupted build succeeded")
			}
			after := h.read(t)
			if renewals < driver.SelectionBackfillPageSize+1 || !reflect.DeepEqual(states, projectionStates(t, after, ref)) || *before.Snapshot().At != *after.Snapshot().At {
				t.Fatal("partial build escaped or heartbeat could not progress")
			}
			if lose {
				var err error
				h.lease, err = h.store.Lease(context.Background(), "retry-selection", driver.LeaseTTL)
				if err != nil {
					t.Fatal(err)
				}
				defer h.lease.Release(context.Background())
			}
			if err := h.store.BuildGraphSourceIndexes(context.Background(), h.lease, decode, projection); err != nil {
				t.Fatalf("clean retry failed: %v", err)
			}
		})
	}
}

func TestSelectionStartupRejectsStaleParentChainAndLeavesHistoryExact(t *testing.T) {
	h := setup(t)
	w := h.writer()
	root := apply(t, w, model.ChangeSet{Ops: []model.Op{create("")}}, meta(1)).Results[0].Ref
	op := create("")
	op.Create.Parent = &root
	child := apply(t, w, model.ChangeSet{Ops: []model.Op{op}}, meta(2)).Results[0].Ref
	before := h.read(t)
	source := projectionSource(t, before, child)
	doc, err := DecodeDocument(source.Body)
	if err != nil {
		t.Fatal(err)
	}
	doc.Parent = nil
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	rewriteGraphOnly(t, h, source, body)
	current := h.read(t)
	closure := projectionClosure(t, current, child)
	if len(closure) != 2 {
		t.Fatal("stale closure must still have self and contiguous depth1")
	}
	metaBefore, _ := h.store.Meta(context.Background())
	decode, projection := sourceSelectionDecoder("pipeline-test")
	err = h.store.BuildGraphSourceIndexes(context.Background(), h.lease, decode, projection)
	if !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("stale Parent/closure agreement certified: %v", err)
	}
	metaAfter, _ := h.store.Meta(context.Background())
	after := h.read(t)
	if metaBefore != metaAfter || !reflect.DeepEqual(closure, projectionClosure(t, after, child)) || !reflect.DeepEqual(projectionSource(t, current, child), projectionSource(t, after, child)) {
		t.Fatal("failed build published partial rows or changed bytes")
	}
}

func TestSelectionStartupClosureDecoderRefusesIncompleteRanges(t *testing.T) {
	h := setup(t)
	ref := apply(t, h.writer(), model.ChangeSet{Ops: []model.Op{create("")}}, meta(1)).Results[0].Ref
	r := h.read(t)
	record := projectionSource(t, r, ref)
	rows := projectionClosure(t, r, ref)
	_, projection := sourceSelectionDecoder("pipeline-test")
	if _, err := projection.Decode(record, rows); err != nil {
		t.Fatal(err)
	}
	badDepth, err := AncestorRow(ref.ID, "other", 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]driver.DerivedRow{nil, {rows[0], rows[0]}, {rows[0], badDepth}, make([]driver.DerivedRow, maxClosureRows+1)} {
		if _, err := projection.Decode(record, bad); !errors.Is(err, driver.ErrSelectionUnavailable) {
			t.Fatalf("invalid closure admitted: %v", err)
		}
	}
}
