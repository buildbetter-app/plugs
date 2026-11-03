package s1b25b28_test

import (
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type durableRelationState struct {
	Meta       driver.Meta
	Records    []driver.Record
	Versions   map[string][]driver.Version
	Changesets []model.Changeset
	Derived    map[driver.DerivedTable][]driver.DerivedRow
}

func relationState(t *testing.T, s *stack) durableRelationState {
	t.Helper()
	state := durableRelationState{Versions: map[string][]driver.Version{}, Derived: map[driver.DerivedTable][]driver.DerivedRow{}}
	var err error
	state.Meta, err = s.store.Meta(s.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.store.Read(s.ctx, driver.View{Incarnation: state.Meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	page, err := r.Query(s.ctx, driver.Query{Limit: 1000})
	if err != nil || page.NextCursor != nil {
		t.Fatalf("incomplete relation census: %v", err)
	}
	state.Records = page.Items
	for _, row := range page.Items {
		versions, err := r.Versions(s.ctx, row.Ref, driver.PageReq{Limit: 1000})
		if err != nil || versions.NextCursor != nil {
			t.Fatalf("incomplete relation versions: %v", err)
		}
		state.Versions[row.Ref.ID] = versions.Items
	}
	state.Changesets, err = r.Changesets(s.ctx, nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range []driver.DerivedTable{driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins, driver.DerivedReadyState, driver.DerivedReferenceIndex, driver.DerivedSortKey} {
		state.Derived[table], err = r.DerivedRows(s.ctx, table, "")
		if err != nil {
			t.Fatal(err)
		}
	}
	return state
}

func assertNoReceipt(t *testing.T, s *stack, meta model.WriteMeta) {
	t.Helper()
	m, err := s.store.Meta(s.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.store.Read(s.ctx, driver.View{Incarnation: m.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	principal, _ := driver.PrincipalKey(s.principal)
	if receipt, err := r.IdempotencyByKey(s.ctx, principal, "", meta.Method, meta.IdempotencyKey); err != nil || receipt != nil {
		t.Fatalf("refusal published receipt=%+v err=%v", receipt, err)
	}
}

func assertPersistedClock(t *testing.T, s *stack) {
	t.Helper()
	state := relationState(t, s)
	if len(state.Changesets) == 0 {
		t.Fatal("no actual persisted changeset to check")
	}
	for _, change := range state.Changesets {
		if !change.Time.Equal(acceptanceClock) {
			t.Fatalf("persisted driver changeset time=%s, want=%s", change.Time, acceptanceClock)
		}
	}
	if s.engine == nil {
		return
	}
	sqlDriver := "sqlite"
	if s.path == "" {
		sqlDriver = "pgx"
	}
	db, err := sql.Open(sqlDriver, s.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if sqlDriver == "pgx" {
		var first, last time.Time
		if err := db.QueryRowContext(s.ctx, `SELECT min(expires),max(expires) FROM plug_postgres.receipts`).Scan(&first, &last); err != nil || !first.Equal(acceptanceClock.Add(driver.IdempotencyWindow)) || !last.Equal(first) {
			t.Fatalf("actual SQL-generated receipt expiry=%s..%s: %v", first, last, err)
		}
	} else {
		var first, last int64
		if err := db.QueryRowContext(s.ctx, `SELECT min(created),max(created) FROM replays`).Scan(&first, &last); err != nil || first != acceptanceClock.UnixNano() || last != first {
			t.Fatalf("actual driver-persisted receipt time=%d..%d: %v", first, last, err)
		}
	}
	t.Logf("persisted Go/driver/SQL times checked at %s", acceptanceClock.Format(time.RFC3339Nano))
}

func TestB25DeletePolicyRefusalAndExplicitAtomicCleanupAcrossDrivers(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite", "postgres"} {
		t.Run(kind, func(t *testing.T) {
			s, parent, child, _ := setupRelational(t, kind)
			before := relationState(t, s)
			for i, id := range []string{child, parent, "s1-missing"} {
				meta := writeMeta(70 + i)
				_, err := s.writer.Apply(s.ctx, s.principal, model.ChangeSet{Ops: []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: ref(id)}}}}, meta)
				want := "ontology_guard_failed"
				if id == "s1-missing" {
					want = "not_found"
				}
				var typed *model.Error
				if !errors.As(err, &typed) || string(typed.Data.Code) != want {
					t.Fatalf("delete %s: got %v, want %s", id, err, want)
				}
				if !reflect.DeepEqual(before, relationState(t, s)) {
					t.Fatal("refused delete changed raw records, versions, log, head or derived rows")
				}
				assertNoReceipt(t, s, meta)
			}
			// The designed policy requires explicit relation edits. They and
			// deletion land in one atomic revision; no silent cascade is invented.
			committed := apply(t, s, model.ChangeSet{Ops: []model.Op{
				{Kind: model.OpUnlink, Link: &model.LinkOp{From: ref(parent), To: ref(child), LinkKind: "relates-to"}},
				{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: ref(child)}},
				{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: ref(parent)}},
			}}, writeMeta(73))
			if committed.Revision.String() != "2" {
				t.Fatalf("explicit cleanup split revision: %+v", committed)
			}
			if _, exists := readRecord(t, s, parent); exists {
				t.Fatal("deleted owner's labels/comments/metadata/links remained live")
			}
			row, exists := readRecord(t, s, child)
			if !exists || relational(t, row).Parent != nil {
				t.Fatal("surviving child retained the deleted parent")
			}
			assertPersistedClock(t, s)
			if kind != "memory" {
				s.reopen()
				if _, exists := readRecord(t, s, parent); exists {
					t.Fatal("deleted owner returned after SQL reopen")
				}
			}
		})
	}
}

func TestB25CloseProvenanceAndLabelsSurviveScalarPatchAcrossDrivers(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite", "postgres"} {
		t.Run(kind, func(t *testing.T) {
			s, parent, child, _ := setupRelational(t, kind)
			apply(t, s, model.ChangeSet{Ops: []model.Op{{Kind: model.OpClose, Close: &model.CloseOp{Target: ref(child), Reason: "completed with evidence"}}}}, writeMeta(80))
			closed, _ := readRecord(t, s, child)
			before := relational(t, closed)
			if len(before.Comments) != 1 || before.Comments[0].Body != "completed with evidence" || before.Fields["status"] != "done" || before.Parent == nil || before.Parent.ID != parent {
				t.Fatalf("actual close provenance missing: %+v", before)
			}
			apply(t, s, model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref(child), Fields: map[string]any{"title": "closed scalar update"}}}}}, writeMeta(81))
			updated, _ := readRecord(t, s, child)
			after := relational(t, updated)
			before.Fields["title"] = "closed scalar update"
			assertJSONEqual(t, before, after)
			assertPersistedClock(t, s)
			if kind != "memory" {
				s.reopen()
				row, _ := readRecord(t, s, child)
				assertJSONEqual(t, after, relational(t, row))
			}
		})
	}
}
