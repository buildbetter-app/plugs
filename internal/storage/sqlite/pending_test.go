package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func commitReceipt(t *testing.T, ctx context.Context, s driver.Store, l driver.ActiveLease, id string, pending bool) (model.Changeset, driver.ChangesetHeader) {
	t.Helper()
	tx, h := beginRecord(t, ctx, s, l, id)
	h.Method = "test.pending"
	h.IdempotencyKey = id
	h.RequestHash = "digest"
	h.ResponsePending = pending
	h.Response = model.Commit{Results: []model.OpResult{{Ref: testRef(id), Revision: model.Natural64(1)}}, Warnings: []model.Warning{{Code: "post_subscriber_failed", Message: "initial"}}}
	if err := tx.Put(ctx, driver.Record{Ref: testRef(id), Kind: "test", Body: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	entry, err := tx.Commit(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	return entry, h
}
func lookupReceipt(t *testing.T, ctx context.Context, r driver.Reader, h driver.ChangesetHeader) *driver.IdempotencyRecord {
	t.Helper()
	principal, err := driver.PrincipalKey(h.Principal)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := r.IdempotencyByKey(ctx, principal, h.AgentLabel, h.Method, h.IdempotencyKey)
	if err != nil || receipt == nil {
		t.Fatalf("lookup receipt=%+v %v", receipt, err)
	}
	return receipt
}
func TestPendingReceiptSurvivesReopenAndTakeover(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "pending.db")
	a := openStore(t, ctx, path)
	old := getLease(t, ctx, a, driver.LeaseTTL)
	entry, h := commitReceipt(t, ctx, a, old, "pl-pending", true)
	meta := getMeta(t, ctx, a)
	first := lookupReceipt(t, ctx, getReader(t, ctx, a, meta, nil), h)
	if !first.ResponsePending || len(first.Commit.Warnings) != 1 {
		t.Fatalf("initial pending receipt=%+v", first)
	}
	if err := old.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	b := openStore(t, ctx, path)
	active := getLease(t, ctx, b, driver.LeaseTTL)
	pinned := getReader(t, ctx, b, meta, nil)
	reopened := lookupReceipt(t, ctx, pinned, h)
	if !reflect.DeepEqual(first, reopened) {
		t.Fatalf("pending receipt changed on reopen: %+v %+v", first, reopened)
	}
	scope := scopeKey("system:engine-test", "", h.Method, h.IdempotencyKey)
	var age int64
	if err := b.db.QueryRowContext(ctx, `SELECT created FROM replays WHERE scope=?`, scope).Scan(&age); err != nil {
		t.Fatal(err)
	}
	post := []model.Warning{{Code: "post_subscriber_failed", Message: "post", Details: map[string]any{"binding": "test"}}}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := b.FinalizeResponse(canceled, active, "system:engine-test", "", h.Method, h.IdempotencyKey, entry.Revision, post); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled finalizer=%v", err)
	}
	foreign := openStore(t, ctx, filepath.Join(t.TempDir(), "foreign.db"))
	foreignLease := getLease(t, ctx, foreign, driver.LeaseTTL)
	requireFenced(t, b.FinalizeResponse(ctx, foreignLease, "system:engine-test", "", h.Method, h.IdempotencyKey, entry.Revision, post))
	if err := b.FinalizeResponse(ctx, active, "system:engine-test", "", h.Method, h.IdempotencyKey, model.Natural64(99), post); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("revision mismatch=%v", err)
	}
	if err := b.FinalizeResponse(ctx, active, "system:engine-test", "", h.Method, "missing", entry.Revision, post); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("scope mismatch=%v", err)
	}
	if err := b.FinalizeResponse(ctx, active, "system:engine-test", "", h.Method, h.IdempotencyKey, entry.Revision, []model.Warning{{Code: "invalid"}}); err == nil {
		t.Fatal("invalid post outcome accepted")
	}
	if got := lookupReceipt(t, ctx, getReader(t, ctx, b, meta, nil), h); !reflect.DeepEqual(got, first) {
		t.Fatalf("failed finalization changed receipt=%+v", got)
	}
	if err := b.FinalizeResponse(ctx, active, "system:engine-test", "", h.Method, h.IdempotencyKey, entry.Revision, post); err != nil {
		t.Fatal(err)
	}
	if got := lookupReceipt(t, ctx, pinned, h); !got.ResponsePending || !reflect.DeepEqual(got, first) {
		t.Fatalf("finalizer changed pinned pending snapshot=%+v", got)
	}
	fresh := lookupReceipt(t, ctx, getReader(t, ctx, b, meta, nil), h)
	if fresh.ResponsePending || len(fresh.Commit.Warnings) != 2 || fresh.Commit.Results[0].Revision.String() != "1" || fresh.RequestHash != h.RequestHash {
		t.Fatalf("final receipt=%+v", fresh)
	}
	if !reflect.DeepEqual(fresh.Changeset, first.Changeset) {
		t.Fatal("finalization changed original log receipt")
	}
	if err := b.FinalizeResponse(ctx, active, "system:engine-test", "", h.Method, h.IdempotencyKey, entry.Revision, post); err != nil {
		t.Fatal(err)
	}
	post[0].Details["binding"] = "mutated"
	if err := b.FinalizeResponse(ctx, active, "system:engine-test", "", h.Method, h.IdempotencyKey, entry.Revision, post); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("different final outcome=%v", err)
	}
	var afterAge int64
	if err := b.db.QueryRowContext(ctx, `SELECT created FROM replays WHERE scope=?`, scope).Scan(&afterAge); err != nil || afterAge != age {
		t.Fatalf("finalization refreshed 24h age=%d/%d %v", age, afterAge, err)
	}
	if getMeta(t, ctx, b).Head.String() != "1" {
		t.Fatal("finalization advanced graph head")
	}
	if err := active.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	c := openStore(t, ctx, path)
	durable := lookupReceipt(t, ctx, getReader(t, ctx, c, meta, nil), h)
	if !reflect.DeepEqual(durable, fresh) {
		t.Fatal("final response did not survive second reopen")
	}
	t.Log("pending survives reopen/epoch takeover; invalid or canceled outcomes leave it intact; finalized response is durable, immutable and snapshot isolated")
}
func TestSQLFinalizationFailureRemainsRecoverableAfterReopen(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "failed-finalization.db")
	s := openStore(t, ctx, path)
	old := getLease(t, ctx, s, driver.LeaseTTL)
	entry, h := commitReceipt(t, ctx, s, old, "pl-finalization-fault", true)
	meta := getMeta(t, ctx, s)
	before := lookupReceipt(t, ctx, getReader(t, ctx, s, meta, nil), h)
	if _, err := s.db.ExecContext(ctx, `CREATE TRIGGER reject_finalization BEFORE UPDATE ON replays BEGIN SELECT RAISE(ABORT,'injected finalization failure'); END`); err != nil {
		t.Fatal(err)
	}
	post := []model.Warning{{Code: "post_subscriber_failed", Message: "durable post outcome"}}
	if err := s.FinalizeResponse(ctx, old, "system:engine-test", "", h.Method, h.IdempotencyKey, entry.Revision, post); err == nil || !strings.Contains(err.Error(), "injected finalization failure") {
		t.Fatalf("expected actual SQL finalizer failure, got %v", err)
	}
	if got := lookupReceipt(t, ctx, getReader(t, ctx, s, meta, nil), h); !reflect.DeepEqual(before, got) {
		t.Fatalf("failed SQL finalization changed pending receipt: %+v", got)
	}
	if err := old.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openStore(t, ctx, path)
	active := getLease(t, ctx, reopened, driver.LeaseTTL)
	if active.Epoch() != old.Epoch()+1 {
		t.Fatalf("reopen did not advance lease epoch: %d", active.Epoch())
	}
	if got := lookupReceipt(t, ctx, getReader(t, ctx, reopened, meta, nil), h); !reflect.DeepEqual(before, got) {
		t.Fatalf("pending receipt did not survive failed finalization and reopen: %+v", got)
	}
	requireFenced(t, reopened.FinalizeResponse(ctx, old, "system:engine-test", "", h.Method, h.IdempotencyKey, entry.Revision, post))
	if _, err := reopened.db.ExecContext(ctx, `DROP TRIGGER reject_finalization`); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := reopened.FinalizeResponse(ctx, active, "system:engine-test", "", h.Method, h.IdempotencyKey, entry.Revision, post); err != nil {
			t.Fatal(err)
		}
	}
	r := getReader(t, ctx, reopened, meta, nil)
	after := lookupReceipt(t, ctx, r, h)
	if after.ResponsePending || len(after.Commit.Warnings) != 2 || after.Commit.Warnings[1].Message != post[0].Message || !reflect.DeepEqual(after.Commit.Results, before.Commit.Results) || !reflect.DeepEqual(after.Changeset, entry) {
		t.Fatalf("recovered response changed original publication or duplicated warnings: %+v", after)
	}
	versions, err := r.Versions(ctx, testRef("pl-finalization-fault"), driver.PageReq{})
	if err != nil || len(versions.Items) != 1 || r.Snapshot().At.String() != "1" {
		t.Fatalf("finalization retry changed graph history/head: %+v %v", versions, err)
	}
	log, err := r.Changesets(ctx, nil, 100)
	if err != nil || len(log) != 1 {
		t.Fatalf("finalization retry changed graph log: %+v %v", log, err)
	}
	t.Log("actual SQL finalizer abort leaves durable pending receipt; reopen and next epoch recover the same outcome without another graph write")
}

func TestPendingNoopAndUnkeyedHeadersRefuse(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "invalid-pending.db"))
	l := getLease(t, ctx, s, driver.LeaseTTL)
	tx, h := beginRecord(t, ctx, s, l, "pl-unkeyed")
	h.ResponsePending = true
	if err := tx.Put(ctx, driver.Record{Ref: testRef("pl-unkeyed"), Kind: "test", Body: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx, h); err == nil {
		t.Fatal("unkeyed pending commit accepted")
	}
	_ = tx.Close()
	if !getMeta(t, ctx, s).Head.IsZero() {
		t.Fatal("invalid pending header changed graph")
	}
	_, h = commitReceipt(t, ctx, s, l, "pl-pending-noop", true)
	noop, nh := beginRecord(t, ctx, s, l, "pl-pending-noop")
	nh.Method = h.Method
	nh.IdempotencyKey = h.IdempotencyKey
	nh.RequestHash = h.RequestHash
	nh.Changes = nil
	if _, err := noop.(driver.NoopCommitter).CommitNoop(ctx, nh); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("noop replay of pending receipt=%v", err)
	}
	_ = noop.Close()
	noop, nh = beginRecord(t, ctx, s, l, "pl-pending-noop")
	nh.Method = "test.noop"
	nh.IdempotencyKey = "new-noop"
	nh.RequestHash = "digest"
	nh.ResponsePending = true
	nh.Changes = nil
	if _, err := noop.(driver.NoopCommitter).CommitNoop(ctx, nh); err == nil {
		t.Fatal("pending noop header accepted")
	}
	_ = noop.Close()
	if getMeta(t, ctx, s).Head.String() != "1" {
		t.Fatal("invalid noop consumed revision")
	}
}
func TestFinalReceiptsAreImmutableAndEmptyOutcomesEquivalent(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "final.db"))
	l := getLease(t, ctx, s, driver.LeaseTTL)
	entry, h := commitReceipt(t, ctx, s, l, "pl-final", false)
	meta := getMeta(t, ctx, s)
	before := lookupReceipt(t, ctx, getReader(t, ctx, s, meta, nil), h)
	if err := s.FinalizeResponse(ctx, l, "system:engine-test", "", h.Method, h.IdempotencyKey, entry.Revision, []model.Warning{{Code: "post_subscriber_failed", Message: "late"}}); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("initially final response mutated=%v", err)
	}
	for _, post := range [][]model.Warning{nil, {}} {
		if err := s.FinalizeResponse(ctx, l, "system:engine-test", "", h.Method, h.IdempotencyKey, entry.Revision, post); err != nil {
			t.Fatal(err)
		}
	}
	if got := lookupReceipt(t, ctx, getReader(t, ctx, s, meta, nil), h); !reflect.DeepEqual(before, got) {
		t.Fatal("initially final response changed")
	}
	entry, h = commitReceipt(t, ctx, s, l, "pl-empty-final", true)
	for _, post := range [][]model.Warning{nil, {}} {
		if err := s.FinalizeResponse(ctx, l, "system:engine-test", "", h.Method, h.IdempotencyKey, entry.Revision, post); err != nil {
			t.Fatal(err)
		}
	}
	if got := lookupReceipt(t, ctx, getReader(t, ctx, s, meta, nil), h); got.ResponsePending {
		t.Fatal("empty finalizer left pending receipt")
	}
}
func TestSchemaUpgradePreservesFinalReceipts(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "schema1.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	legacy := `CREATE TABLE meta(singleton INTEGER PRIMARY KEY,store_id TEXT,incarnation TEXT,schema_version INTEGER,head TEXT,history_floor TEXT);
 CREATE TABLE active_lease(singleton INTEGER PRIMARY KEY,epoch TEXT,token TEXT,holder TEXT,expires INTEGER);
 CREATE TABLE replays(scope TEXT PRIMARY KEY,created INTEGER,rev TEXT,request_hash TEXT,response BLOB,changeset BLOB,finalized INTEGER,post BLOB);`
	if _, err = db.ExecContext(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `INSERT INTO meta VALUES(1,'original-store','original-incarnation',1,?,?)`, revKey(0), revKey(0)); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `INSERT INTO active_lease VALUES(1,?,'','',0)`, revKey(17)); err != nil {
		t.Fatal(err)
	}
	h := driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: "engine-test"}, Method: "old.noop", IdempotencyKey: "old"}
	scope := scopeKey("system:engine-test", "", h.Method, h.IdempotencyKey)
	if _, err = db.ExecContext(ctx, `INSERT INTO replays VALUES(?,strftime('%s','now')*1000000000,?,'old-digest',?,NULL,1,?)`, scope, revKey(0), []byte(`{"revision":0,"results":[],"warnings":[]}`), []byte("null")); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	s := openStore(t, ctx, path)
	meta := getMeta(t, ctx, s)
	if meta.SchemaVersion != schemaVersion || meta.StoreID != "original-store" || meta.Incarnation != "original-incarnation" || !meta.Head.IsZero() {
		t.Fatalf("schema upgrade changed identity/graph: %+v", meta)
	}
	l := getLease(t, ctx, s, driver.LeaseTTL)
	if l.Epoch() != 18 {
		t.Fatalf("upgrade changed epoch history: %d", l.Epoch())
	}
	receipt := lookupReceipt(t, ctx, getReader(t, ctx, s, meta, nil), h)
	if receipt.ResponsePending || receipt.RequestHash != "old-digest" || receipt.Changeset.Kind != "" {
		t.Fatalf("upgrade invented pending work: %+v", receipt)
	}
	for _, post := range [][]model.Warning{nil, {}} {
		if err = s.FinalizeResponse(ctx, l, "system:engine-test", "", h.Method, h.IdempotencyKey, model.Natural64(0), post); err != nil {
			t.Fatal(err)
		}
	}
	if err = l.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openStore(t, ctx, path)
	if got := lookupReceipt(t, ctx, getReader(t, ctx, reopened, meta, nil), h); !reflect.DeepEqual(receipt, got) {
		t.Fatal("upgraded receipt changed on reopen")
	}
	t.Log("real schema1 -> schema2 additive transaction preserved store/incarnation/head/epoch and final receipts")
}
