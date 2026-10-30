package sqlite

import (
	"bytes"
	"context"
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func openStore(t *testing.T, ctx context.Context, path string) *Store {
	t.Helper()
	value, err := (Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	s := value.(*Store)
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}
func getMeta(t *testing.T, ctx context.Context, s driver.Store) driver.Meta {
	t.Helper()
	m, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return m
}
func getLease(t *testing.T, ctx context.Context, s driver.Store, ttl time.Duration) driver.ActiveLease {
	t.Helper()
	l, err := s.Lease(ctx, "engine-test", ttl)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := l.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return l
}
func getReader(t *testing.T, ctx context.Context, s driver.Store, meta driver.Meta, at *model.Revision) driver.Reader {
	t.Helper()
	r, err := s.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: at})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	return r
}
func testRef(id string) driver.Ref { return model.RefByID(id, model.Natural64(0)) }
func testKey(id string) driver.LockKey {
	ref := testRef(id)
	return driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
}
func beginRecord(t *testing.T, ctx context.Context, s driver.Store, l driver.ActiveLease, id string) (driver.WriteTx, driver.ChangesetHeader) {
	t.Helper()
	tx, err := s.Write(ctx, l)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Close() })
	key := testKey(id)
	if _, err = tx.Lock(ctx, []driver.LockRequest{{Key: key, Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	h := driver.ChangesetHeader{Incarnation: getMeta(t, ctx, s).Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "system", ID: "engine-test"}, LockKeys: []driver.LockKey{key}}
	states, err := tx.Observe(ctx, []driver.LockKey{key})
	if err != nil {
		t.Fatal(err)
	}
	h.Changes = []model.ChangeRecord{{Ref: testRef(id), Op: model.OpPatch, FromRevision: states[0].Revision}}
	if !states[0].Present {
		h.Changes[0].Op = model.OpCreate
	}
	return tx, h
}
func seedRecord(t *testing.T, ctx context.Context, s driver.Store, l driver.ActiveLease, id, body string) model.Changeset {
	t.Helper()
	entry, err := conformance.Seed(ctx, s, l, driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: "engine-test"}}, []conformance.FixtureChange{{Ref: testRef(id), Kind: "test", Op: model.OpSetMeta, Body: []byte(body)}})
	if err != nil {
		t.Fatal(err)
	}
	return entry
}
func requireFenced(t *testing.T, err error) {
	t.Helper()
	var fenced driver.ErrFenced
	if !errors.As(err, &fenced) {
		t.Fatalf("want ErrFenced, got %v", err)
	}
}

func TestPersistenceAndIndependentHandles(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "persist.db")
	a := openStore(t, ctx, path)
	b := openStore(t, ctx, path)
	var engine string
	if err := a.db.QueryRowContext(ctx, `SELECT sqlite_version()`).Scan(&engine); err != nil {
		t.Fatal(err)
	}
	t.Logf("actual SQLite engine %s; Go %s; file-backed independent handles", engine, runtime.Version())
	l := getLease(t, ctx, a, driver.LeaseTTL)
	meta := getMeta(t, ctx, a)
	empty := getReader(t, ctx, b, meta, nil)
	ref := testRef("pl-persist")
	fact := driver.ReadFact("test/persist")
	sentinel, _ := driver.NewReadySentinel("project", "type")
	derived := driver.DerivedRowKey{Table: driver.DerivedReadyState, Key: "row"}
	keys := []driver.LockKey{testKey(ref.ID), {Kind: driver.LockReadFact, Fact: fact}, {Kind: driver.LockSentinel, Sentinel: sentinel}, {Kind: driver.LockDerived, Derived: &derived}}
	requests := make([]driver.LockRequest, len(keys))
	for i, key := range keys {
		requests[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	tx, err := a.Write(ctx, l)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, err = tx.Lock(ctx, requests); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	input := []byte(`{ "value": 1 }`)
	if err = tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: input}); err != nil {
		t.Fatal(err)
	}
	input[0] = '!'
	blob, err := tx.PutBlob(ctx, []byte("durable blob"))
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.PutDerived(ctx, derived.Table, nil, []driver.DerivedRow{{Key: derived.Key, Value: []byte("derived")}}); err != nil {
		t.Fatal(err)
	}
	if err = tx.BumpSentinels(ctx, []driver.Sentinel{sentinel}); err != nil {
		t.Fatal(err)
	}
	if err = tx.BumpReadFacts(ctx, []driver.ReadFact{fact}); err != nil {
		t.Fatal(err)
	}
	h := driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "user", ID: "persist"}, Method: "test.persist", AgentLabel: "ignored", IdempotencyKey: "write", RequestHash: "digest", LockKeys: keys, Changes: []model.ChangeRecord{{Ref: ref, Op: model.OpCreate}}, Response: model.Commit{Results: []model.OpResult{{Ref: ref, Revision: model.Natural64(1)}}, Warnings: []model.Warning{}}}
	h.ResponsePending = true
	entry, err := tx.Commit(ctx, h)
	if err != nil || entry.Revision.String() != "1" {
		t.Fatalf("commit=%+v %v", entry, err)
	}
	// Authenticated scope ignores anonymous agent labels, including finalizers.
	if err = a.FinalizeResponse(ctx, l, "user:persist", "different", "test.persist", "write", entry.Revision, []model.Warning{{Code: "post_subscriber_failed", Message: "durable warning"}}); err != nil {
		t.Fatal(err)
	}
	if _, err = a.SaveCheckpoint(ctx, l, "delivery", 0, []byte("durable checkpoint")); err != nil {
		t.Fatal(err)
	}
	noop, nh := beginRecord(t, ctx, a, l, ref.ID)
	nh.Changes = nil
	nh.Method = "test.persist"
	nh.IdempotencyKey = "noop"
	nh.RequestHash = "noop-digest"
	nh.Response = model.Commit{Results: []model.OpResult{{Ref: ref, Revision: model.Natural64(1)}}, Warnings: []model.Warning{}}
	if _, err = noop.(driver.NoopCommitter).CommitNoop(ctx, nh); err != nil {
		t.Fatal(err)
	}
	rows, err := empty.Get(ctx, []driver.Ref{ref})
	if err != nil || len(rows) != 0 {
		t.Fatalf("old handle snapshot=%v %v", rows, err)
	}
	if err = empty.Close(); err != nil {
		t.Fatal(err)
	}
	if err = l.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err = a.Close(); err != nil {
		t.Fatal(err)
	}
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openStore(t, ctx, path)
	after := getMeta(t, ctx, reopened)
	if !reflect.DeepEqual(meta.StoreID, after.StoreID) || meta.Incarnation != after.Incarnation || after.Head.String() != "1" {
		t.Fatalf("identity/head lost: before=%+v after=%+v", meta, after)
	}
	next := getLease(t, ctx, reopened, driver.LeaseTTL)
	if next.Epoch() <= l.Epoch() {
		t.Fatal("reopen reused epoch")
	}
	r := getReader(t, ctx, reopened, after, nil)
	rows, err = r.Get(ctx, []driver.Ref{ref})
	if err != nil || len(rows) != 1 || string(rows[0].Body) != `{ "value": 1 }` || rows[0].Revision.String() != "1" {
		t.Fatalf("persisted record=%v %v", rows, err)
	}
	data, err := r.Blob(ctx, blob)
	if err != nil || string(data) != "durable blob" {
		t.Fatalf("persisted blob=%q %v", data, err)
	}
	states, err := r.Observe(ctx, keys)
	if err != nil || len(states) != 4 {
		t.Fatalf("persisted observations=%v %v", states, err)
	}
	for _, state := range states {
		if !state.Present || state.Revision.String() != "1" {
			t.Fatalf("persisted state=%+v", state)
		}
	}
	if string(states[3].Derived.Value) != "derived" {
		t.Fatal("derived bytes lost")
	}
	versions, err := r.Versions(ctx, ref, driver.PageReq{})
	if err != nil || len(versions.Items) != 1 || !versions.Items[0].Base {
		t.Fatalf("persisted versions=%v %v", versions, err)
	}
	log, err := r.Changesets(ctx, nil, 100)
	if err != nil || len(log) != 1 {
		t.Fatalf("persisted log=%v %v", log, err)
	}
	receipt, err := r.IdempotencyByKey(ctx, "user:persist", "any", "test.persist", "write")
	if err != nil || receipt == nil || len(receipt.Commit.Warnings) != 1 || receipt.Commit.Warnings[0].Message != "durable warning" {
		t.Fatalf("persisted replay=%+v %v", receipt, err)
	}
	receipt, err = r.IdempotencyByKey(ctx, "system:engine-test", "", "test.persist", "noop")
	if err != nil || receipt == nil || receipt.Changeset.Kind != "" || receipt.Commit.Results[0].Revision.String() != "1" {
		t.Fatalf("persisted noop=%+v %v", receipt, err)
	}
	checkpoint, err := reopened.LoadCheckpoint(ctx, "delivery")
	if err != nil || checkpoint.Generation != 1 || string(checkpoint.Payload) != "durable checkpoint" {
		t.Fatalf("persisted checkpoint=%+v %v", checkpoint, err)
	}
	t.Logf("real file reopen preserved identity, head, records, history, blob, derived/fact/sentinel revisions, replay, noop and checkpoint; epochs %d -> %d", l.Epoch(), next.Epoch())
}

func TestWALReadersProceedDuringStagedWriter(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "wal.db")
	a := openStore(t, ctx, path)
	b := openStore(t, ctx, path)
	l := getLease(t, ctx, a, driver.LeaseTTL)
	seedRecord(t, ctx, a, l, "pl-wal", `{"value":1}`)
	meta := getMeta(t, ctx, a)
	pinned := getReader(t, ctx, b, meta, nil)
	tx, h := beginRecord(t, ctx, a, l, "pl-wal")
	if err := tx.Put(ctx, driver.Record{Ref: testRef("pl-wal"), Kind: "test", Body: []byte(`{"value":2}`)}); err != nil {
		t.Fatal(err)
	}
	quick, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	during := getReader(t, quick, b, meta, nil)
	before, err := during.Get(quick, []driver.Ref{testRef("pl-wal")})
	if err != nil || len(before) != 1 || string(before[0].Body) != `{"value":1}` {
		t.Fatalf("reader blocked/dirty=%v %v", before, err)
	}
	if _, err = tx.Commit(ctx, h); err != nil {
		t.Fatal(err)
	}
	for _, r := range []driver.Reader{pinned, during} {
		rows, err := r.Get(ctx, []driver.Ref{testRef("pl-wal")})
		if err != nil || string(rows[0].Body) != `{"value":1}` {
			t.Fatalf("WAL snapshot changed=%v %v", rows, err)
		}
	}
	current := getReader(t, ctx, b, meta, nil)
	rows, err := current.Get(ctx, []driver.Ref{testRef("pl-wal")})
	if err != nil || string(rows[0].Body) != `{"value":2}` {
		t.Fatalf("new snapshot=%v %v", rows, err)
	}
	var mode string
	if err = a.db.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&mode); err != nil || mode != "wal" {
		t.Fatalf("journal=%s %v", mode, err)
	}
	t.Log("independent handle readers completed during staged writer and retained WAL snapshots after commit")
}

func TestSQLFailureRollsBackWholePublication(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "fault.db"))
	l := getLease(t, ctx, s, driver.LeaseTTL)
	tx, h := beginRecord(t, ctx, s, l, "pl-fault")
	if err := tx.Put(ctx, driver.Record{Ref: testRef("pl-fault"), Kind: "test", Body: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	hash, err := tx.PutBlob(ctx, []byte("must roll back"))
	if err != nil {
		t.Fatal(err)
	}
	// A real SQL failure after earlier record/version statements must undo them.
	if _, err = s.db.ExecContext(ctx, `CREATE TRIGGER reject_blob BEFORE INSERT ON blobs BEGIN SELECT RAISE(ABORT,'injected publication failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Commit(ctx, h); err == nil || !strings.Contains(err.Error(), "injected publication failure") {
		t.Fatalf("expected real SQLite abort, got %v", err)
	}
	m := getMeta(t, ctx, s)
	r := getReader(t, ctx, s, m, nil)
	rows, err := r.Get(ctx, []driver.Ref{testRef("pl-fault")})
	if err != nil || len(rows) != 0 || !m.Head.IsZero() {
		t.Fatalf("failed SQL published rows/head=%v %+v %v", rows, m, err)
	}
	versions, err := r.Versions(ctx, testRef("pl-fault"), driver.PageReq{})
	if err != nil || len(versions.Items) != 0 {
		t.Fatalf("failed SQL published versions=%v %v", versions, err)
	}
	if _, err = r.Blob(ctx, hash); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("failed SQL published blob=%v", err)
	}
	log, err := r.Changesets(ctx, nil, 100)
	if err != nil || len(log) != 0 {
		t.Fatalf("failed SQL published log=%v %v", log, err)
	}
	if _, err = s.db.ExecContext(ctx, `DROP TRIGGER reject_blob`); err != nil {
		t.Fatal(err)
	}
	entry, err := tx.Commit(ctx, h)
	if err != nil || entry.Revision.String() != "1" {
		t.Fatalf("retry after atomic rollback=%+v %v", entry, err)
	}
	t.Log("SQLite trigger abort rolled back records, versions, blob and head/log; retry committed revision 1")
}

func TestUnsignedDomainsAndExhaustion(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "uint.db"))
	// Values above signed int64 survive the SQLite boundary unchanged.
	if _, err := s.db.ExecContext(ctx, `UPDATE active_lease SET epoch=?`, revKey(math.MaxUint64-1)); err != nil {
		t.Fatal(err)
	}
	l := getLease(t, ctx, s, driver.LeaseTTL)
	if l.Epoch() != math.MaxUint64 {
		t.Fatalf("epoch narrowed: %d", l.Epoch())
	}
	if err := l.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE meta SET head=?`, revKey(math.MaxUint64-1)); err != nil {
		t.Fatal(err)
	}
	tx, h := beginRecord(t, ctx, s, l, "pl-uint")
	if err := tx.Put(ctx, driver.Record{Ref: testRef("pl-uint"), Kind: "test", Body: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	entry, err := tx.Commit(ctx, h)
	if err != nil || entry.Revision.String() != model.Natural64(math.MaxUint64).String() {
		t.Fatalf("revision narrowed=%+v %v", entry, err)
	}
	r := getReader(t, ctx, s, getMeta(t, ctx, s), nil)
	rows, err := r.Get(ctx, []driver.Ref{testRef("pl-uint")})
	if err != nil || len(rows) != 1 || rows[0].Revision.String() != entry.Revision.String() {
		t.Fatalf("max revision read=%v %v", rows, err)
	}
	before := getMeta(t, ctx, s)
	next, nh := beginRecord(t, ctx, s, l, "pl-overflow")
	if _, err = next.Commit(ctx, nh); err == nil {
		t.Fatal("exhausted revision accepted")
	}
	_ = next.Close()
	after := getMeta(t, ctx, s)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("revision overflow changed metadata")
	}
	huge := model.Natural("18446744073709551616")
	if _, err = s.Read(ctx, driver.View{Incarnation: after.Incarnation, At: &huge}); err == nil {
		t.Fatal("oversized revision silently narrowed")
	}
	if _, err = s.db.ExecContext(ctx, `INSERT INTO checkpoints VALUES('max',?,?)`, revKey(math.MaxUint64), []byte("unchanged")); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SaveCheckpoint(ctx, l, "max", math.MaxUint64, []byte("overflow")); err == nil {
		t.Fatal("checkpoint generation overflow accepted")
	}
	cp, err := s.LoadCheckpoint(ctx, "max")
	if err != nil || cp.Generation != math.MaxUint64 || string(cp.Payload) != "unchanged" {
		t.Fatalf("checkpoint overflow mutated=%+v %v", cp, err)
	}
	if err = l.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Lease(ctx, "overflow", driver.LeaseTTL); err == nil {
		t.Fatal("epoch overflow accepted")
	}
	t.Log("full uint64 revision/epoch/checkpoint values retained; exhaustion and oversized Naturals refused without mutation")
}

func TestExpiryAndUnsupportedCapabilities(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "expiry.db"))
	l := getLease(t, ctx, s, driver.LeaseTTL)
	tx, h := beginRecord(t, ctx, s, l, "pl-before")
	h.IdempotencyKey = "reuse"
	h.Method = "test.write"
	h.RequestHash = "first"
	if err := tx.Put(ctx, driver.Record{Ref: testRef("pl-before"), Kind: "test", Body: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx, h); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE replays SET created=?`, time.Now().Add(-driver.IdempotencyWindow-time.Second).UnixNano()); err != nil {
		t.Fatal(err)
	}
	meta := getMeta(t, ctx, s)
	r := getReader(t, ctx, s, meta, nil)
	if receipt, err := r.IdempotencyByKey(ctx, "system:engine-test", "", h.Method, h.IdempotencyKey); err != nil || receipt != nil {
		t.Fatalf("expired receipt=%+v %v", receipt, err)
	}
	tx, h = beginRecord(t, ctx, s, l, "pl-after")
	h.IdempotencyKey = "reuse"
	h.Method = "test.write"
	h.RequestHash = "second"
	if err := tx.Put(ctx, driver.Record{Ref: testRef("pl-after"), Kind: "test", Body: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx, h); err != nil {
		t.Fatal(err)
	}
	log, err := getReader(t, ctx, s, meta, nil).Changesets(ctx, nil, 100)
	if err != nil || len(log) != 2 || log[0].IdempotencyKey != "reuse" || log[1].IdempotencyKey != "reuse" {
		t.Fatalf("expiry discarded historical keys=%v %v", log, err)
	}
	for _, cfg := range []driver.Config{{}, {DSN: ":memory:"}, {DSN: "file:memory?mode=memory"}, {DSN: filepath.Join(t.TempDir(), "option.db"), Options: map[string]any{"ignored": true}}} {
		if _, err := (Driver{}).Open(ctx, cfg); !errors.Is(err, driver.ErrUnsupported) {
			t.Fatalf("unsupported config=%+v error=%v", cfg, err)
		}
	}
	if _, err = s.Read(ctx, driver.View{Incarnation: meta.Incarnation, Branch: model.Natural64(1)}); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("unsupported branch=%v", err)
	}
	if _, err = s.Migrate(ctx, schemaVersion+1); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("unsupported migration=%v", err)
	}
	health, err := s.Health(ctx)
	if err != nil || health.Driver != "sqlite" || health.Role != "active" || !health.LastBackupCheckpoint.IsZero() {
		t.Fatalf("health=%+v %v", health, err)
	}
	for _, capability := range (Driver{}).Info().Capabilities {
		if strings.Contains(capability, "backup") || strings.Contains(capability, "failover") || capability == "standby" {
			t.Fatalf("unsupported topology advertised: %s", capability)
		}
	}
	t.Log("expired keys can be reused after 24h while log retains originals; unsupported configs/topology/migrations/branches refuse")
}

func TestCoreSessionPersistenceAcrossRestart(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "sessions.db")
	value, err := (Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	store := value.(driver.CoreSessionStore)
	now := time.Unix(1_800_000_000, 123)
	record := driver.CoreSessionRecord{
		ID: "0123456789abcdef0123456789abcdef", TokenDigest: bytes.Repeat([]byte{0x42}, 32),
		Principal: model.PrincipalRef{Kind: "user", ID: "user:sqlite"}, ExpiresAt: now.Add(time.Hour),
	}
	if err := store.PutCoreSession(ctx, record, now, 4); err != nil {
		t.Fatal(err)
	}
	active, err := store.LookupCoreSession(ctx, record.ID)
	if err != nil || active.Revoked || active.Principal.Kind != "user" || active.Principal.ID != record.Principal.ID {
		t.Fatalf("active core session=%+v err=%v", active, err)
	}
	if revoked, err := store.RevokeCoreSession(ctx, record.ID); err != nil || !revoked {
		t.Fatalf("revoke=%t err=%v", revoked, err)
	}
	if err := value.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedValue, err := (Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopenedValue.Close() })
	reopened := reopenedValue.(driver.CoreSessionStore)
	revoked, err := reopened.LookupCoreSession(ctx, record.ID)
	if err != nil || !revoked.Revoked || revoked.Principal.Kind != "user" {
		t.Fatalf("reopened revoked core session=%+v err=%v", revoked, err)
	}
	bad := record
	bad.ID = "fedcba9876543210fedcba9876543210"
	bad.Principal = model.PrincipalRef{Kind: "agent", ID: "agent:must-not-be-human"}
	if err := reopened.PutCoreSession(ctx, bad, now, 4); err == nil {
		t.Fatal("non-user core session was accepted")
	}
}
