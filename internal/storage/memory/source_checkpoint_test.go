package memory

import (
	"bytes"
	"context"
	"errors"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func sourceCheckpointNoop(t *testing.T, s *Store, lease driver.ActiveLease, key string) (*transaction, driver.ChangesetHeader) {
	t.Helper()
	ctx := context.Background()
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Close() })
	if _, err := tx.Lock(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	return tx.(*transaction), driver.ChangesetHeader{Kind: model.ChangesetWrite, Incarnation: meta.Incarnation, Principal: model.PrincipalRef{Kind: "system", ID: "source-checkpoint"}, Method: "bead.batch", IdempotencyKey: key, RequestHash: "same-exact-request"}
}

func TestSourceCheckpointNoopAtomicReceiptAndReplay(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease := sourceLease(t, s)
	if s.WorkflowCheckpointLifetime() != driver.CheckpointLifetimeProcess {
		t.Fatal("memory lifetime changed")
	}
	if persistent, ok := any(s).(driver.PersistentCheckpointStore); ok && persistent.Persistent() {
		t.Fatal("memory falsely promises restart persistence")
	}
	tx, h := sourceCheckpointNoop(t, s, lease, "active-only")
	payload := []byte("first exact bytes")
	if err := tx.StageCheckpoint(ctx, "plugin/workflow/default/durable", 0, payload); err != nil {
		t.Fatal(err)
	}
	payload[0] = '!'
	if err := tx.StageCheckpoint(ctx, "plugin/workflow/default/durable", 0, []byte("first exact bytes")); err != nil {
		t.Fatal("identical duplicate stage refused", err)
	}
	if err := tx.StageCheckpoint(ctx, "plugin/workflow/default/durable", 0, []byte("conflict")); !errors.Is(err, driver.ErrCheckpointConflict) {
		t.Fatal("conflicting duplicate accepted", err)
	}
	before, _ := s.LoadCheckpoint(ctx, "plugin/workflow/default/durable")
	if before.Generation != 0 {
		t.Fatal("staging published")
	}
	commit, err := tx.CommitNoop(ctx, h)
	if err != nil || !commit.Revision.IsZero() {
		t.Fatalf("active-only=%+v %v", commit, err)
	}
	value, err := s.LoadCheckpoint(ctx, "plugin/workflow/default/durable")
	if err != nil || value.Generation != 1 || string(value.Payload) != "first exact bytes" {
		t.Fatalf("checkpoint=%+v %v", value, err)
	}
	value.Payload[0] = '!'
	again, _ := s.LoadCheckpoint(ctx, "plugin/workflow/default/durable")
	if string(again.Payload) != "first exact bytes" {
		t.Fatal("published bytes aliased output")
	}
	if !s.state.meta.Head.IsZero() || len(s.state.log) != 0 || len(s.state.versions) != 0 || len(s.state.replays) != 1 {
		t.Fatal("active-only manufactured core history or lost receipt")
	}
	retry, h := sourceCheckpointNoop(t, s, lease, "active-only")
	if err := retry.StageCheckpoint(ctx, "plugin/workflow/default/durable", 0, []byte("must never publish")); err != nil {
		t.Fatal(err)
	}
	replayed, err := retry.CommitNoop(ctx, h)
	if err != nil || !reflect.DeepEqual(replayed, commit) {
		t.Fatal("exact replay failed", err)
	}
	again, _ = s.LoadCheckpoint(ctx, "plugin/workflow/default/durable")
	if again.Generation != 1 || !bytes.Equal(again.Payload, []byte("first exact bytes")) {
		t.Fatal("replay reapplied provider mutation")
	}
	conflict, h := sourceCheckpointNoop(t, s, lease, "active-only")
	h.RequestHash = "changed-plan"
	if err := conflict.StageCheckpoint(ctx, "plugin/workflow/default/durable", 1, []byte("changed")); err != nil {
		t.Fatal(err)
	}
	if _, err := conflict.CommitNoop(ctx, h); !errors.Is(err, driver.ErrConflict) {
		t.Fatal("changed request accepted", err)
	}
	_ = conflict.Close()
	again, _ = s.LoadCheckpoint(ctx, "plugin/workflow/default/durable")
	if again.Generation != 1 {
		t.Fatal("conflict published")
	}
}

func TestSourceCheckpointMixedCommitAndRollback(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease := sourceLease(t, s)
	ref := model.RefByID("pl-mixed-checkpoint", model.Natural64(0))
	for _, fail := range []bool{true, false} {
		tx, h := beginSourceTest(t, s, lease, ref)
		h.Method = "bead.batch"
		h.IdempotencyKey = "mixed-atomic"
		h.RequestHash = "exact-plan"
		if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"value":"core"}`)}); err != nil {
			t.Fatal(err)
		}
		if err := tx.StageCheckpoint(ctx, "workflow", 0, []byte("provider")); err != nil {
			t.Fatal(err)
		}
		if fail {
			h.Response.Warnings = []model.Warning{{Code: "subscriber_skipped", Details: map[string]any{"invalid": make(chan int)}}}
			h.Changes = []model.ChangeRecord{{Ref: ref, Op: model.OpCreate}}
			if _, err := tx.Commit(ctx, h); err == nil {
				t.Fatal("invalid response committed")
			}
			_ = tx.Close()
			cp, _ := s.LoadCheckpoint(ctx, "workflow")
			if cp.Generation != 0 || !s.state.meta.Head.IsZero() || len(s.state.records) != 0 || len(s.state.replays) != 0 {
				t.Fatal("failed mixed commit published a plane")
			}
		} else {
			entry := commitSourceTest(t, tx, h)
			cp, _ := s.LoadCheckpoint(ctx, "workflow")
			if cp.Generation != 1 || string(cp.Payload) != "provider" || entry.Revision.String() != "1" || len(s.state.log) != 1 || len(s.state.versions) != 1 || len(s.state.replays) != 1 {
				t.Fatal("mixed publication was not one revision")
			}
		}
	}
}

func TestSourceCheckpointChangedGenerationRefusesBothCommitPaths(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(map[bool]string{false: "active", true: "mixed"}[mixed], func(t *testing.T) {
			ctx := context.Background()
			s := New()
			defer s.Close()
			lease := sourceLease(t, s)
			var tx *transaction
			var h driver.ChangesetHeader
			if mixed {
				ref := model.RefByID("pl-conflict", model.Natural64(0))
				tx, h = beginSourceTest(t, s, lease, ref)
				if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{}`)}); err != nil {
					t.Fatal(err)
				}
				h.Changes = []model.ChangeRecord{{Ref: ref, Op: model.OpCreate}}
			} else {
				tx, h = sourceCheckpointNoop(t, s, lease, "guarded")
			}
			defer tx.Close()
			if err := tx.StageCheckpoint(ctx, "workflow", 0, []byte("unpublished")); err != nil {
				t.Fatal(err)
			}
			if _, err := s.SaveCheckpoint(ctx, lease, "workflow", 0, []byte("winner")); err != nil {
				t.Fatal(err)
			}
			var err error
			if mixed {
				_, err = tx.Commit(ctx, h)
			} else {
				_, err = tx.CommitNoop(ctx, h)
			}
			if !errors.Is(err, driver.ErrCheckpointConflict) {
				t.Fatalf("checkpoint guard=%v", err)
			}
			cp, _ := s.LoadCheckpoint(ctx, "workflow")
			if cp.Generation != 1 || string(cp.Payload) != "winner" || !s.state.meta.Head.IsZero() || len(s.state.records)+len(s.state.log)+len(s.state.replays) != 0 {
				t.Fatal("stale guard published")
			}
		})
	}
}

func TestSourceCheckpointFinalCancellationRollsBackNoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := New()
	defer s.Close()
	lease := sourceLease(t, s)
	tx, h := sourceCheckpointNoop(t, s, lease, "cancelled")
	defer tx.Close()
	if err := tx.StageCheckpoint(ctx, "workflow", 0, []byte("unpublished")); err != nil {
		t.Fatal(err)
	}
	h.Response.Warnings = []model.Warning{{Code: "subscriber_skipped", Details: map[string]any{"cancel": cancelOnMarshal{cancel}}}}
	if _, err := tx.CommitNoop(ctx, h); !errors.Is(err, context.Canceled) {
		t.Fatal("final cancellation not fenced", err)
	}
	cp, _ := s.LoadCheckpoint(context.Background(), "workflow")
	if cp.Generation != 0 || !s.state.meta.Head.IsZero() || len(s.state.replays) != 0 {
		t.Fatal("cancelled commit published")
	}
}

func TestSourceCheckpointHeadFenceAndStageValidation(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease := sourceLease(t, s)
	meta, _ := s.Meta(ctx)
	expect := driver.CoreSnapshotExpectation{StoreID: meta.StoreID, Incarnation: meta.Incarnation, Head: meta.Head}
	if _, err := s.SaveCheckpointAtHead(ctx, lease, "workflow", 0, []byte("first"), expect); err != nil {
		t.Fatal(err)
	}
	ref := model.RefByID("pl-head-changed", model.Natural64(0))
	tx, h := beginSourceTest(t, s, lease, ref)
	if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, h)
	if _, err := s.SaveCheckpointAtHead(ctx, lease, "workflow", 1, []byte("bad"), expect); !errors.Is(err, driver.ErrCheckpointConflict) {
		t.Fatal("stale head accepted", err)
	}
	meta, _ = s.Meta(ctx)
	expect.Head = meta.Head
	if _, err := s.SaveCheckpointAtHead(ctx, lease, "workflow", 1, []byte("second"), expect); err != nil {
		t.Fatal(err)
	}
	tx, h = sourceCheckpointNoop(t, s, lease, "invalid")
	defer tx.Close()
	for _, test := range []struct {
		key      string
		expected uint64
	}{{" invalid ", 0}, {"workflow", math.MaxUint64}} {
		if err := tx.StageCheckpoint(ctx, test.key, test.expected, []byte("bad")); err == nil {
			t.Fatal("invalid stage accepted")
		}
	}
	if err := tx.StageCheckpoint(ctx, "workflow", 2, []byte("valid")); err != nil {
		t.Fatal(err)
	}
	if err := tx.ExpectCheckpoint(ctx, "workflow", 3); !errors.Is(err, driver.ErrCheckpointConflict) {
		t.Fatal("stage guard replaced", err)
	}
	if _, err := tx.CommitNoop(ctx, h); err != nil {
		t.Fatal(err)
	}
}

func TestSourceCheckpointSnapshotPinsCoreAndProvider(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease := sourceLease(t, s)
	meta, _ := s.Meta(ctx)
	if _, err := s.SaveCheckpoint(ctx, lease, "workflow", 0, []byte("old")); err != nil {
		t.Fatal(err)
	}
	pinned, err := s.ReadSourcePlaneSnapshot(ctx, driver.View{Incarnation: meta.Incarnation}, []string{"missing", "workflow"})
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Close()
	ref := model.RefByID("pl-later", model.Natural64(0))
	tx, h := beginSourceTest(t, s, lease, ref)
	if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.StageCheckpoint(ctx, "workflow", 1, []byte("new")); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, h)
	if !pinned.Snapshot().At.IsZero() {
		t.Fatal("pinned core advanced")
	}
	if records, err := pinned.Get(ctx, []driver.Ref{ref}); err != nil || len(records) != 0 {
		t.Fatal("snapshot mixed new graph with old provider", err)
	}
	value, err := pinned.SourcePlaneCheckpoint("workflow")
	if err != nil || value.Generation != 1 || string(value.Payload) != "old" {
		t.Fatalf("pinned provider=%+v %v", value, err)
	}
	value.Payload[0] = '!'
	value, _ = pinned.SourcePlaneCheckpoint("workflow")
	if string(value.Payload) != "old" {
		t.Fatal("snapshot bytes aliased output")
	}
	missing, err := pinned.SourcePlaneCheckpoint("missing")
	if err != nil || missing.Generation != 0 || missing.Payload != nil {
		t.Fatal("absent namespace not pinned", err)
	}
	if _, err := pinned.SourcePlaneCheckpoint("unrequested"); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatal("unobserved namespace treated as absent", err)
	}
	if _, err := s.ReadSourcePlaneSnapshot(ctx, pinned.Snapshot(), []string{"workflow"}); !errors.Is(err, driver.ErrCheckpointConflict) {
		t.Fatal("historical core/current provider mixed", err)
	}
	meta, _ = s.Meta(ctx)
	current, err := s.ReadSourcePlaneSnapshot(ctx, driver.View{Incarnation: meta.Incarnation}, []string{"workflow"})
	if err != nil {
		t.Fatal(err)
	}
	value, err = current.SourcePlaneCheckpoint("workflow")
	if err != nil || value.Generation != 2 || string(value.Payload) != "new" || current.Snapshot().At.String() != "1" {
		t.Fatal("current snapshot incoherent", err)
	}
	_ = current.Close()
	if _, err := current.SourcePlaneCheckpoint("workflow"); err == nil {
		t.Fatal("closed snapshot readable")
	}
}

func TestSourceCheckpointStageCannotPrecedeRestorePreparation(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease := sourceLease(t, s)
	tx, h := sourceCheckpointNoop(t, s, lease, "ordinary-checkpoint")
	if err := tx.StageCheckpoint(ctx, "workflow", 0, []byte("ordinary")); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.PrepareOfflineRestore(ctx, "source"); err == nil {
		t.Fatal("restore adopted previously staged checkpoint")
	}
	if _, err := tx.CommitNoop(ctx, h); err != nil {
		t.Fatal("restore refusal left staged restore mode", err)
	}
	value, _ := s.LoadCheckpoint(ctx, "workflow")
	if value.Generation != 1 || string(value.Payload) != "ordinary" || !s.state.meta.Head.IsZero() {
		t.Fatal("ordinary checkpoint publication lost its original mode")
	}
}

func TestSourceCheckpointStageAfterHistoryPreparationRefusesWithoutChangingLegacyHistory(t *testing.T) {
	for _, mode := range []driver.RestoreHistoryMode{driver.RestoreHistoryModeFull, driver.RestoreHistoryModePartial} {
		for _, publish := range []bool{false, true} {
			t.Run(string(mode)+map[bool]string{false: "/rollback", true: "/commit"}[publish], func(t *testing.T) {
				ctx := context.Background()
				s := New()
				defer s.Close()
				lease := sourceLease(t, s)
				before := s.state
				when := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
				a := driver.Record{Ref: model.RefByID("history-checkpoint", model.Natural64(0)), Kind: "test", Body: []byte(`{"value":"before"}`)}
				b := a
				b.Body = []byte(`{"value":"after"}`)
				history := driver.OfflineHistory{Source: "explicit-history-control", Verified: true, Complete: true, First: 1, Head: 1, Batches: []driver.SourceHistoryBatch{{Sequence: 1, EventIDs: []string{"event:1"}, Changes: []driver.SourceHistoryChange{{After: &b}}}}}
				if mode == driver.RestoreHistoryModePartial {
					history.Complete, history.BaselineVerified = false, true
					history.First, history.Head = 2, 2
					history.Baseline = []driver.Record{a}
					history.Gaps = []driver.SourceHistoryGap{{First: 1, Last: 1, Reason: "unavailable prefix"}}
					history.Batches = []driver.SourceHistoryBatch{{Sequence: 2, EventIDs: []string{"event:2"}, Changes: []driver.SourceHistoryChange{{Before: &a, After: &b}}}}
				}
				p := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: "source-history", Method: "offline.restore.import", IdempotencyKey: "history-checkpoint", ArchiveDigest: strings.Repeat("b", 64), DerivedDigest: strings.Repeat("c", 64), HistoryMode: mode, HistoryTime: when.Format(time.RFC3339Nano)}
				var err error
				p.HistoryDigest, err = driver.OfflineHistoryDigest(history)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := driver.BuildOfflineHistory(before.meta, p, history, when, []driver.Record{b}); err != nil {
					t.Fatal("invalid positive history precondition", err)
				}
				marker := model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0))
				locks := driver.LockSet{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &b.Ref}, Mode: driver.LockUpdate}, {Key: driver.LockKey{Kind: driver.LockRecord, Ref: &marker}, Mode: driver.LockUpdate}}
				sort.Slice(locks, func(i, j int) bool { return keyID(locks[i].Key) < keyID(locks[j].Key) })
				write, err := s.Write(ctx, lease)
				if err != nil {
					t.Fatal(err)
				}
				tx := write.(*transaction)
				defer tx.Close()
				if _, err := tx.Lock(ctx, locks); err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Head(ctx); err != nil {
					t.Fatal(err)
				}
				meta, err := tx.PrepareOfflineHistory(ctx, p, history, when)
				if err != nil {
					t.Fatal("legacy preparation failed", err)
				}
				if err := tx.StageCheckpoint(ctx, "workflow", 0, []byte("must not be ignored")); !errors.Is(err, driver.ErrUnsupported) {
					t.Fatalf("checkpoint after history preparation=%v", err)
				}
				if len(tx.checkpointWrites)+len(tx.checkpointGuards) != 0 || s.state != before || len(s.checkpoints) != 0 {
					t.Fatal("refusal staged or published checkpoint, graph, metadata or receipt")
				}
				if !publish {
					if err := tx.Close(); err != nil {
						t.Fatal(err)
					}
					if s.state != before || len(s.checkpoints) != 0 {
						t.Fatal("rollback changed original graph, history, receipt or checkpoint")
					}
					return
				}
				// The SAME prepared transaction still publishes ordinary legacy
				// history and its receipt; refusal never poisons its mode.
				if err := tx.Put(ctx, b); err != nil {
					t.Fatal(err)
				}
				hash, err := p.RequestHash()
				if err != nil {
					t.Fatal(err)
				}
				keys := []driver.LockKey{}
				for _, held := range locks {
					keys = append(keys, held.Key)
				}
				header := driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetRestore, Time: when, Principal: p.Admission.Actor, Method: p.Method, IdempotencyKey: p.IdempotencyKey, RequestHash: hash, LockKeys: keys, Changes: []model.ChangeRecord{{Ref: b.Ref, Op: model.OpCreate, FromRevision: model.Natural64(0), ChangedPaths: []string{""}}}}
				entry, err := tx.CommitOfflineHistory(ctx, header)
				if err != nil {
					t.Fatal("refusal broke same-transaction legacy commit", err)
				}
				want := "2"
				wantEntries := 2
				if mode == driver.RestoreHistoryModePartial {
					want = "3"
					wantEntries = 3
				}
				if entry.Revision.String() != want || s.state.meta.Head != entry.Revision || s.state.meta.Incarnation != meta.Incarnation || len(s.state.records) != 2 || len(s.state.log) != wantEntries || len(s.state.replays) != 1 || len(s.checkpoints) != 0 {
					t.Fatal("legacy history/receipt correspondence changed or checkpoint leaked")
				}
				for _, replay := range s.state.replays {
					if replay.value.RequestHash != hash || !reflect.DeepEqual(replay.value.Changeset, entry) {
						t.Fatal("legacy immutable receipt changed")
					}
				}
			})
		}
	}
}
