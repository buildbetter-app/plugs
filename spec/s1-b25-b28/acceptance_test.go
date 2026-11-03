package s1b25b28_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

var acceptancePrincipal = model.PrincipalRef{Kind: "user", ID: "s1-acceptance"}

var acceptanceClock = time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

type stack struct {
	t         *testing.T
	ctx       context.Context
	store     driver.Store
	lease     driver.ActiveLease
	path      string
	dsn       string
	engine    driver.Driver
	clock     *testclock.Clock
	ids       []string
	nextID    int
	writer    *pipeline.Writer
	principal model.PrincipalRef
}

func openStack(t *testing.T, kind string, ids []string, options ...pipeline.Option) *stack {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	s := &stack{t: t, ctx: ctx, ids: append([]string(nil), ids...), principal: acceptancePrincipal, clock: testclock.New(t, acceptanceClock)}
	if kind == "memory" {
		store, err := memory.NewWithOptions(memory.Options{
			Clock:       s.clock.Now,
			StoreID:     driver.StoreID("s1-" + kind),
			Incarnation: "s1-" + kind + "-incarnation",
		})
		if err != nil {
			t.Fatal(err)
		}
		s.store = store
	} else if kind == "sqlite" {
		s.path = filepath.Join(t.TempDir(), "s1.sqlite")
		s.dsn, s.engine = s.path, sqlite.Driver{Clock: s.clock.Now}
	} else if kind == "postgres" {
		s.dsn = s.clock.BindPostgres(t, testclock.PostgresDSN(t))
		s.engine = postgres.Driver{}
		if at := testclock.PostgresNow(t, s.dsn); !at.Equal(acceptanceClock) {
			t.Fatalf("reached SQL clock=%s, Go clock=%s", at, acceptanceClock)
		}
	} else {
		t.Fatalf("unknown acceptance store %q", kind)
	}
	if s.engine != nil {
		var err error
		s.store, err = s.engine.Open(ctx, driver.Config{DSN: s.dsn})
		if err != nil {
			t.Fatal(err)
		}
	}
	lease, err := s.store.Lease(ctx, "s1-acceptance", driver.LeaseTTL)
	if err != nil {
		_ = s.store.Close()
		t.Fatal(err)
	}
	s.lease = lease
	t.Cleanup(func() {
		if s.lease != nil {
			_ = s.lease.Release(context.Background())
		}
		if s.store != nil {
			_ = s.store.Close()
		}
	})
	s.writer = s.newWriter(options...)
	return s
}

func (s *stack) newWriter(options ...pipeline.Option) *pipeline.Writer {
	s.t.Helper()
	idGenerator := func() (string, error) {
		if s.nextID == len(s.ids) {
			return "", fmt.Errorf("S1 fixture exhausted its declared IDs")
		}
		id := s.ids[s.nextID]
		s.nextID++
		return id, nil
	}
	base := []pipeline.Option{
		pipeline.WithClock(s.clock.Now),
		pipeline.WithIDGenerator(idGenerator),
		pipeline.WithOntology(service.NewOntology(nil)),
	}
	base = append(base, options...)
	return pipeline.New(s.store, s.lease, base...)
}

func (s *stack) reopen() {
	s.t.Helper()
	if s.engine == nil {
		s.t.Fatal("reopen requires a real SQL store")
	}
	if s.lease != nil {
		_ = s.lease.Release(context.Background())
	}
	if err := s.store.Close(); err != nil {
		s.t.Fatal(err)
	}
	store, err := s.engine.Open(s.ctx, driver.Config{DSN: s.dsn})
	if err != nil {
		s.t.Fatal(err)
	}
	lease, err := store.Lease(s.ctx, "s1-acceptance-reopen", driver.LeaseTTL)
	if err != nil {
		_ = store.Close()
		s.t.Fatal(err)
	}
	s.store, s.lease = store, lease
	s.writer = s.newWriter()
}

func writeMeta(n int) model.WriteMeta {
	return model.WriteMeta{Method: "tx.apply", Project: "s1", IdempotencyKey: fmt.Sprintf("%032x", n)}
}

func apply(t *testing.T, s *stack, cs model.ChangeSet, meta model.WriteMeta) model.Commit {
	t.Helper()
	commit, err := s.writer.Apply(s.ctx, s.principal, cs, meta)
	if err != nil {
		t.Fatalf("write refused: %v", err)
	}
	return commit
}

func ref(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }

func key(label string) string {
	digest := sha256.Sum256([]byte("s1-s1/" + label))
	return hex.EncodeToString(digest[:])
}

func readRecord(t *testing.T, s *stack, id string) (driver.Record, bool) {
	t.Helper()
	meta, err := s.store.Meta(s.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.store.Read(s.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Get(s.ctx, []model.Ref{ref(id)})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		return driver.Record{}, false
	}
	return rows[0], true
}

type relationalImage struct {
	Fields   map[string]any             `json:"fields"`
	Labels   []string                   `json:"labels,omitempty"`
	Parent   *model.Ref                 `json:"parent,omitempty"`
	Metadata map[string]json.RawMessage `json:"metadata,omitempty"`
	Comments []pipeline.Comment         `json:"comments,omitempty"`
	Links    []pipeline.Link            `json:"links,omitempty"`
}

func relational(t *testing.T, record driver.Record) relationalImage {
	t.Helper()
	doc, err := pipeline.DecodeDocument(record.Body)
	if err != nil {
		t.Fatal(err)
	}
	return relationalImage{Fields: doc.Fields, Labels: doc.Labels, Parent: doc.Parent, Metadata: doc.Metadata, Comments: doc.Comments, Links: doc.Links}
}

func encoded(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func assertJSONEqual(t *testing.T, want, got any) {
	t.Helper()
	if !bytes.Equal(encoded(t, want), encoded(t, got)) {
		t.Fatalf("JSON values differ:\nwant %s\n got %s", encoded(t, want), encoded(t, got))
	}
}

func setupRelational(t *testing.T, kind string) (*stack, string, string, relationalImage) {
	t.Helper()
	s := openStack(t, kind, []string{"s1-parent", "s1-child"})
	commit := apply(t, s, model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpCreate, Create: &model.CreateOp{Key: "parent", Type: "issue", Fields: map[string]any{"title": "original", "description": "keep", "priority": 2, "labels": []string{"alpha", "beta"}}}},
		{Kind: model.OpCreate, Create: &model.CreateOp{Key: "child", Type: "issue", Parent: ptr(model.RefByKey("parent")), Fields: map[string]any{"title": "child", "labels": []string{"child"}}}},
		{Kind: model.OpLink, Link: &model.LinkOp{Kind: model.OpLink, From: model.RefByKey("parent"), To: model.RefByKey("child"), LinkKind: "relates-to"}},
		{Kind: model.OpComment, Comment: &model.CommentOp{Kind: model.OpComment, Target: model.RefByKey("parent"), Body: "retain this exact comment"}},
		{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Kind: model.OpSetMeta, Target: model.RefByKey("parent"), Key: "plugin.note", Value: json.RawMessage(`{"nested":true}`)}},
	}}, writeMeta(1))
	seen := map[string]bool{}
	for _, result := range commit.Results {
		seen[result.Ref.ID] = true
	}
	if commit.Revision.String() != "1" || !seen["s1-parent"] || !seen["s1-child"] {
		t.Fatalf("relational seed commit=%+v", commit)
	}
	row, ok := readRecord(t, s, "s1-parent")
	if !ok {
		t.Fatal("seed parent missing")
	}
	return s, "s1-parent", "s1-child", relational(t, row)
}

func ptr(ref model.Ref) *model.Ref { return &ref }

func TestB25RelationalPreservationAndDeleteCascadeAcrossDrivers(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite", "postgres"} {
		t.Run(kind, func(t *testing.T) {
			s, parentID, childID, before := setupRelational(t, kind)
			patchMeta := writeMeta(2)
			commit := apply(t, s, model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Kind: model.OpPatch, Target: ref(parentID), Fields: map[string]any{"title": "updated", "description": "changed", "priority": 4}}}}}, patchMeta)
			if commit.Revision.String() != "2" {
				t.Fatalf("scalar update revision=%s", commit.Revision)
			}
			afterRecord, ok := readRecord(t, s, parentID)
			if !ok {
				t.Fatal("parent disappeared after scalar update")
			}
			after := relational(t, afterRecord)
			assertJSONEqual(t, []string{"alpha", "beta"}, after.Labels)
			if after.Parent != nil {
				// The parent is intentionally root; the child carries the parent edge.
				t.Fatalf("unexpected root parent edge: %+v", after.Parent)
			}
			if len(after.Comments) != 1 || after.Comments[0].Body != before.Comments[0].Body || len(after.Links) != 1 || after.Links[0].To.ID != childID || after.Links[0].Kind != "relates-to" {
				t.Fatalf("relational data was clobbered: before=%+v after=%+v", before, after)
			}
			if string(after.Metadata["plugin.note"]) != string(before.Metadata["plugin.note"]) {
				t.Fatalf("metadata was clobbered: before=%s after=%s", before.Metadata["plugin.note"], after.Metadata["plugin.note"])
			}
			if after.Fields["title"] != "updated" || after.Fields["description"] != "changed" || after.Fields["priority"] != json.Number("4") && after.Fields["priority"] != float64(4) {
				t.Fatalf("scalar patch not applied: %+v", after.Fields)
			}

			// The parent edge belongs to the child image and survives the parent's
			// scalar update. Read it separately to prove the owned relation was not
			// silently rewritten by the parent patch.
			childRecord, ok := readRecord(t, s, childID)
			if !ok {
				t.Fatal("child disappeared during parent update")
			}
			childDoc, err := pipeline.DecodeDocument(childRecord.Body)
			if err != nil || childDoc.Parent == nil || childDoc.Parent.ID != parentID {
				t.Fatalf("child parent edge was not preserved: %+v err=%v", childDoc, err)
			}

			if kind != "memory" {
				s.reopen()
				reopened, ok := readRecord(t, s, parentID)
				if !ok {
					t.Fatal("reopened parent missing")
				}
				assertJSONEqual(t, after, relational(t, reopened))
			}

			// Explicit unlink/delete removes all owned auxiliary data without
			// leaving a dangling link or a visible child. A deleted record is a
			// typed absence, not a successful empty update.
			apply(t, s, model.ChangeSet{Ops: []model.Op{{Kind: model.OpUnlink, Link: &model.LinkOp{Kind: model.OpUnlink, From: ref(parentID), To: ref(childID), LinkKind: "relates-to"}}}}, writeMeta(3))
			apply(t, s, model.ChangeSet{Ops: []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Kind: model.OpDelete, Target: ref(childID)}}}}, writeMeta(4))
			apply(t, s, model.ChangeSet{Ops: []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Kind: model.OpDelete, Target: ref(parentID)}}}}, writeMeta(5))
			if _, ok := readRecord(t, s, parentID); ok {
				t.Fatal("deleted parent remained visible")
			}
			if _, ok := readRecord(t, s, childID); ok {
				t.Fatal("deleted child remained visible")
			}
		})
	}
}

func TestB25FailedAuxiliaryWriteRollsBackScalarUpdateAcrossDrivers(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite", "postgres"} {
		t.Run(kind, func(t *testing.T) {

			s, parentID, _, before := setupRelational(t, kind)
			meta := writeMeta(2)
			_, err := s.writer.Apply(s.ctx, s.principal, model.ChangeSet{Ops: []model.Op{
				{Kind: model.OpPatch, Patch: &model.PatchOp{Kind: model.OpPatch, Target: ref(parentID), Fields: map[string]any{"title": "must-rollback"}}},
				{Kind: model.OpLink, Link: &model.LinkOp{Kind: model.OpLink, From: ref(parentID), To: ref(parentID), LinkKind: "blocks"}},
			}}, meta)
			if err == nil || !strings.Contains(err.Error(), "self-links are refused") {
				t.Fatalf("blocking auxiliary write was not refused atomically: %v", err)
			}
			row, ok := readRecord(t, s, parentID)
			if !ok {
				t.Fatal("parent disappeared after refused batch")
			}
			assertJSONEqual(t, before, relational(t, row))
			head, err := s.store.Meta(s.ctx)
			if err != nil {
				t.Fatal(err)
			}
			if head.Head.String() != "1" {
				t.Fatalf("refused batch advanced head to %s", head.Head)
			}
			reader, err := s.store.Read(s.ctx, driver.View{Incarnation: head.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			principal, _ := driver.PrincipalKey(s.principal)
			if receipt, err := reader.IdempotencyByKey(s.ctx, principal, "", meta.Method, meta.IdempotencyKey); err != nil || receipt != nil {
				t.Fatalf("refused auxiliary batch published receipt=%+v err=%v", receipt, err)
			}

		})
	}
}

type panicTimedPost struct{ calls *atomic.Int32 }

func (p *panicTimedPost) After(context.Context, port.CommittedChange, port.SnapshotReader) error {
	return errors.New("ordinary timed-post path was used")
}

func (p *panicTimedPost) AfterWithTimeout(context.Context, time.Duration, port.CommittedChange, port.SnapshotReader) error {
	p.calls.Add(1)
	panic("controlled S1 timed post panic")
}

func TestB28CommittedWarningDeadLetterAndReplayAcrossDrivers(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite", "postgres"} {
		t.Run(kind, func(t *testing.T) {

			var calls atomic.Int32
			s := openStack(t, kind, []string{"s1-warning"})
			postStore, ok := s.store.(driver.CheckpointStore)
			if !ok {
				t.Fatal("real fixture has no checkpoint store")
			}
			post := &panicTimedPost{calls: &calls}
			w := s.newWriter(pipeline.WithPostFailureStore(postStore), pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "s1/timed-panic", Subscriber: post}))
			cs := model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Kind: model.OpCreate, Type: "issue", Fields: map[string]any{"title": "warning"}}}}}
			meta := writeMeta(30)
			first, err := w.Apply(s.ctx, s.principal, cs, meta)
			if err != nil || first.Revision.String() != "1" || len(first.Warnings) != 1 || first.Warnings[0].Code != "post_subscriber_failed" {
				t.Fatalf("timed post did not become a committed warning: commit=%+v err=%v", first, err)
			}
			if calls.Load() != 1 {
				t.Fatalf("timed post callback count=%d", calls.Load())
			}
			incarnation, err := s.store.Meta(s.ctx)
			if err != nil {
				t.Fatal(err)
			}
			deadKey := pipeline.PostDeadLetterKey(incarnation.Incarnation, first.Revision, "s1/timed-panic")
			checkpoint, err := postStore.LoadCheckpoint(s.ctx, deadKey)
			var dead pipeline.PostDeadLetter
			if err != nil || checkpoint.Generation != 1 || json.Unmarshal(checkpoint.Payload, &dead) != nil || dead.Binding != "s1/timed-panic" || dead.Reason != "post_subscriber_failed" || dead.Revision.String() != "1" {
				t.Fatalf("missing durable sanitized dead letter: checkpoint=%+v dead=%+v err=%v", checkpoint, dead, err)
			}
			if bytes.Contains(checkpoint.Payload, []byte("controlled S1 timed post panic")) {
				t.Fatal("dead-letter metadata retained panic text")
			}

			replay, err := w.Apply(s.ctx, s.principal, cs, meta)
			if err != nil || !reflect.DeepEqual(first, replay) || calls.Load() != 1 {
				t.Fatalf("replay changed warning or reran callback: first=%+v replay=%+v calls=%d err=%v", first, replay, calls.Load(), err)
			}
			assertPersistedClock(t, s)
			if kind != "memory" {

				s.reopen()
				reopenedStore := s.store.(driver.CheckpointStore)
				reloaded, err := reopenedStore.LoadCheckpoint(s.ctx, deadKey)
				if err != nil || !bytes.Equal(reloaded.Payload, checkpoint.Payload) || reloaded.Generation != checkpoint.Generation {
					t.Fatalf("reopened dead letter changed: before=%+v after=%+v err=%v", checkpoint, reloaded, err)
				}
				reinstalled := s.newWriter(pipeline.WithPostFailureStore(reopenedStore), pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "s1/timed-panic", Subscriber: post}))
				w = reinstalled
				replay, err = reinstalled.Apply(s.ctx, s.principal, cs, meta)
				if err != nil || !reflect.DeepEqual(first, replay) || calls.Load() != 1 {
					t.Fatalf("reopened replay changed warning or reran callback: first=%+v replay=%+v calls=%d err=%v", first, replay, calls.Load(), err)
				}

			}

			noop, err := w.Apply(s.ctx, s.principal, model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref("s1-warning"), Fields: map[string]any{"title": "warning"}}}}}, writeMeta(31))
			if err != nil || noop.Revision.String() != "1" || len(noop.Warnings) != 0 || calls.Load() != 1 {
				t.Fatalf("no-op minted a revision or called a post subscriber: %+v calls=%d", noop, calls.Load())
			}

		})
	}
}

func TestI4LateEventLogFailureRollsBackWriteVersionHeadAndReceipt(t *testing.T) {
	var posts atomic.Int32
	s := openStack(t, "sqlite", []string{"s1-event"})
	postStore := s.store.(driver.CheckpointStore)
	w := s.newWriter(pipeline.WithPostFailureStore(postStore), pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "s1/observer", Subscriber: postFunc(func(context.Context, port.CommittedChange, port.SnapshotReader) error {
		posts.Add(1)
		return nil
	})}))
	createMeta := writeMeta(40)
	created, err := w.Apply(s.ctx, s.principal, model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Kind: model.OpCreate, Type: "issue", Fields: map[string]any{"title": "before"}}}}}, createMeta)
	if err != nil || created.Revision.String() != "1" || posts.Load() != 1 {
		t.Fatalf("seed write=%+v err=%v posts=%d", created, err, posts.Load())
	}
	trigger := `CREATE TRIGGER s1_reject_changeset BEFORE INSERT ON changesets BEGIN
SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM versions WHERE rev=NEW.rev) THEN RAISE(ABORT,'s1_version_not_staged') END;
SELECT RAISE(ABORT,'s1_reject_event_log'); END;`
	db, err := sql.Open("sqlite", s.path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(s.ctx, trigger); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	failedMeta := writeMeta(41)
	failed, err := w.Apply(s.ctx, s.principal, model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Kind: model.OpPatch, Target: created.Results[0].Ref, Fields: map[string]any{"title": "must-not-land"}}}}}, failedMeta)
	if err == nil || !strings.Contains(err.Error(), "s1_reject_event_log") {
		t.Fatalf("late event-log trigger was not reached: commit=%+v err=%v", failed, err)
	}
	if posts.Load() != 1 {
		t.Fatalf("rolled-back write ran post subscriber: %d", posts.Load())
	}
	row, ok := readRecord(t, s, created.Results[0].Ref.ID)
	if !ok || relational(t, row).Fields["title"] != "before" {
		t.Fatalf("rolled-back row changed: present=%v body=%s", ok, row.Body)
	}
	meta, _ := s.store.Meta(s.ctx)
	if meta.Head.String() != "1" {
		t.Fatalf("rolled-back head=%s", meta.Head)
	}
	r, err := s.store.Read(s.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	principal, _ := driver.PrincipalKey(s.principal)
	if receipt, err := r.IdempotencyByKey(s.ctx, principal, "", failedMeta.Method, failedMeta.IdempotencyKey); err != nil || receipt != nil {
		_ = r.Close()
		_ = db.Close()
		t.Fatalf("late refusal published receipt=%+v err=%v", receipt, err)
	}
	_ = r.Close()
	if _, err := db.ExecContext(s.ctx, "DROP TRIGGER s1_reject_changeset"); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s.reopen()
	w = s.newWriter(pipeline.WithPostFailureStore(s.store.(driver.CheckpointStore)), pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "s1/observer", Subscriber: postFunc(func(context.Context, port.CommittedChange, port.SnapshotReader) error {
		posts.Add(1)
		return nil
	})}))
	committed, err := w.Apply(s.ctx, s.principal, model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Kind: model.OpPatch, Target: created.Results[0].Ref, Fields: map[string]any{"title": "after"}}}}}, failedMeta)
	if err != nil || committed.Revision.String() != "2" || posts.Load() != 2 {
		t.Fatalf("same-key retry did not commit one write/event: commit=%+v err=%v posts=%d", committed, err, posts.Load())
	}
	meta, _ = s.store.Meta(s.ctx)
	r, err = s.store.Read(s.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	changesets, err := r.Changesets(s.ctx, nil, 10)
	_ = r.Close()
	if err != nil || len(changesets) != 2 {
		t.Fatalf("event log does not contain exactly seed+retry: len=%d err=%v", len(changesets), err)
	}
}

type postFunc func(context.Context, port.CommittedChange, port.SnapshotReader) error

func (f postFunc) After(ctx context.Context, c port.CommittedChange, r port.SnapshotReader) error {
	return f(ctx, c, r)
}
