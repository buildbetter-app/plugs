package claimsql_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

// Read the SQL table independently of the production Reader, then compare the
// complete serialized body. SQLite stores the driver envelope as a BLOB;
// PostgreSQL stores the body bytes directly. Neither read substitutes a result.
func sqlLifecycleBody(t *testing.T, f *fixture, ref model.Ref) []byte {
	t.Helper()
	key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
	if err != nil {
		t.Fatal(err)
	}
	db := f.engine.db(t)
	var raw []byte
	if f.engine.name == "postgres" {
		if err := db.QueryRowContext(f.ctx, `SELECT body FROM plug_postgres.records WHERE key=$1 AND NOT deleted`, []byte(key)).Scan(&raw); err != nil {
			t.Fatal(err)
		}
	} else {
		if err := db.QueryRowContext(f.ctx, `SELECT data FROM records WHERE key=?`, key).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var record driver.Record
		if err := json.Unmarshal(raw, &record); err != nil {
			t.Fatal(err)
		}
		raw = record.Body
	}
	if !bytes.Equal(raw, f.record(t, ref).Body) {
		t.Fatal("SQL bytes and production Reader body differ")
	}
	return raw
}

func requireLifecycleTimes(t *testing.T, raw []byte, created, started, closed *time.Time) {
	t.Helper()
	if _, err := pipeline.DecodeDocument(raw); err != nil {
		t.Fatal(err)
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(raw, &members); err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct {
		name string
		at   *time.Time
	}{{"createdAt", created}, {"startedAt", started}, {"closedAt", closed}} {
		got, exists := members[want.name]
		if want.at == nil {
			if exists {
				t.Fatalf("%s must be absent, got %s", want.name, got)
			}
			continue
		}
		var stamp string
		if !exists || json.Unmarshal(got, &stamp) != nil || stamp != want.at.UTC().Format(time.RFC3339Nano) {
			t.Fatalf("persisted %s=%s want=%s", want.name, got, want.at.UTC().Format(time.RFC3339Nano))
		}
	}
}

func requireLifecycleHistory(t *testing.T, f *fixture, ref model.Ref, revision model.Revision, body []byte) {
	t.Helper()
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: f.inc, At: &revision})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Get(f.ctx, []model.Ref{ref})
	if err != nil || len(rows) != 1 || !bytes.Equal(rows[0].Body, body) {
		t.Fatalf("historical persisted bytes changed at %s: rows=%+v err=%v", revision, rows, err)
	}
}

// Writer supplies the body, header and version time from the application
// clock. PostgreSQL independently generates receipt expiry in SQL; SQLite
// generates receipt creation with its driver Go clock (it has no SQL clock).
func requireLifecycleClockProvenance(t *testing.T, f *fixture, ref model.Ref, method, key, agent string, applicationTime time.Time) {
	t.Helper()
	principal, err := driver.PrincipalKey(anonymous)
	if err != nil {
		t.Fatal(err)
	}
	r := f.reader(t)
	defer r.Close()
	receipt, err := r.IdempotencyByKey(f.ctx, principal, agent, method, key)
	if err != nil || receipt == nil || receipt.Changeset.IdempotencyKey != key || !receipt.Changeset.Time.Equal(applicationTime) {
		t.Fatalf("application-supplied changeset time/receipt=%+v err=%v", receipt, err)
	}
	versions, err := r.Versions(f.ctx, ref, driver.PageReq{Limit: 1000})
	if err != nil || len(versions.Items) == 0 || versions.NextCursor != nil {
		t.Fatalf("version page=%+v err=%v", versions, err)
	}
	version := versions.Items[len(versions.Items)-1]
	if version.Revision != receipt.Commit.Revision || !version.CreatedAt.Equal(applicationTime) {
		t.Fatalf("application-supplied version time=%+v receipt=%+v", version, receipt)
	}
	scope, err := json.Marshal([]string{principal, agent, method, key})
	if err != nil {
		t.Fatal(err)
	}
	db := f.engine.db(t)
	if f.engine.name == "postgres" {
		var expires time.Time
		if err := db.QueryRowContext(f.ctx, `SELECT expires FROM plug_postgres.receipts WHERE scope=$1`, scope).Scan(&expires); err != nil {
			t.Fatal(err)
		}
		sqlNow := testclock.PostgresNow(t, f.engine.dsn)
		if !sqlNow.Equal(f.now()) || !expires.Equal(sqlNow.Add(driver.IdempotencyWindow)) || expires.Equal(applicationTime.Add(driver.IdempotencyWindow)) {
			t.Fatalf("SQL-generated receipt expiry=%s SQL clock=%s application clock=%s", expires, sqlNow, applicationTime)
		}
	} else {
		var created int64
		if err := db.QueryRowContext(f.ctx, `SELECT created FROM replays WHERE scope=?`, string(scope)).Scan(&created); err != nil {
			t.Fatal(err)
		}
		if created != f.now().UnixNano() || created == applicationTime.UnixNano() {
			t.Fatalf("driver-Go receipt creation=%d driver clock=%s application clock=%s", created, f.now(), applicationTime)
		}
	}
}

func TestSQLLifecycleTimesPersistThroughClaimCloseReopenAndReplay(t *testing.T) {
	eachEngine(t, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		// Deliberately separate the clocks so a supplied Writer timestamp cannot
		// pass as a driver-generated value merely because the seeds match.
		applicationTime := f.now().Add(37 * time.Second)
		composeClock := func() {
			f.writer, f.service, f.rpc = compose(t, f.store, f.lease, func() time.Time { return applicationTime }, f.inc)
		}
		composeClock()
		advance := func() {
			f.advance(2 * time.Second)
			applicationTime = f.now().Add(37 * time.Second)
			composeClock()
		}
		createdAt := applicationTime
		created := mustResult(t, dispatch(f.rpc, f.ctx, anonymous, "bead.create", params(map[string]any{
			"type": "issue", "fields": map[string]any{"title": "lifecycle SQL bytes"},
		}, writeKey(9001), "alice")))
		ref := f.refFromResult(t, created)
		requireLifecycleTimes(t, sqlLifecycleBody(t, f, ref), &createdAt, nil, nil)
		get := mustResult(t, dispatch(f.rpc, f.ctx, anonymous, "bead.get", params(map[string]any{"id": ref}, "", "")))
		if get["bead"].(map[string]any)["createdAt"] != createdAt.Format(time.RFC3339Nano) {
			t.Fatalf("existing createdAt projection missing: %+v", get)
		}
		requireLifecycleClockProvenance(t, f, ref, "bead.create", writeKey(9001), "alice", applicationTime)

		advance()
		startedAt := applicationTime
		claim := mustResult(t, dispatch(f.rpc, f.ctx, anonymous, "bead.claim", params(map[string]any{"id": ref}, writeKey(9002), "alice")))
		claimRecord := f.record(t, ref)
		claimBody := sqlLifecycleBody(t, f, ref)
		requireLifecycleTimes(t, claimBody, &createdAt, &startedAt, nil)
		if claim["leaseExpiresAt"] != startedAt.Add(5*time.Minute).Format(time.RFC3339Nano) || claim["leaseTtlMs"].(json.Number).String() != "300000" {
			t.Fatalf("claim changed the ordinary five-minute lease: %+v", claim)
		}
		requireLifecycleClockProvenance(t, f, ref, "bead.claim", writeKey(9002), "alice", applicationTime)
		advance()
		before := f.state(t, "")
		mustResult(t, dispatch(f.rpc, f.ctx, anonymous, "bead.claim", params(map[string]any{"id": ref}, writeKey(9003), "alice")))
		requireSameState(t, before, f.state(t, ""))
		if !bytes.Equal(claimBody, sqlLifecycleBody(t, f, ref)) {
			t.Fatal("same-holder claim rewrote persisted bytes")
		}
		mustCode(t, dispatch(f.rpc, f.ctx, anonymous, "bead.claim", params(map[string]any{"id": ref}, writeKey(9004), "bob")), "busy")
		requireSameState(t, before, f.state(t, ""))

		advance()
		closedAt := applicationTime
		closePayload := map[string]any{"id": ref, "reason": "first close"}
		closed := mustResult(t, dispatch(f.rpc, f.ctx, anonymous, "bead.close", params(closePayload, writeKey(9005), "alice")))
		closedRecord := f.record(t, ref)
		closedBody := sqlLifecycleBody(t, f, ref)
		requireLifecycleTimes(t, closedBody, &createdAt, &startedAt, &closedAt)
		requireLifecycleClockProvenance(t, f, ref, "bead.close", writeKey(9005), "alice", applicationTime)
		advance()
		before = f.state(t, "")
		mustResult(t, dispatch(f.rpc, f.ctx, anonymous, "bead.close", params(map[string]any{"id": ref}, writeKey(9006), "alice")))
		requireSameState(t, before, f.state(t, ""))
		mustResult(t, dispatch(f.rpc, f.ctx, anonymous, "bead.close", params(map[string]any{"id": ref, "reason": "settlement comment"}, writeKey(9007), "alice")))
		requireLifecycleTimes(t, sqlLifecycleBody(t, f, ref), &createdAt, &startedAt, &closedAt)

		advance()
		mustResult(t, dispatch(f.rpc, f.ctx, anonymous, "bead.reopen", params(map[string]any{"id": ref}, writeKey(9008), "alice")))
		requireLifecycleTimes(t, sqlLifecycleBody(t, f, ref), &createdAt, &startedAt, nil)
		requireLifecycleClockProvenance(t, f, ref, "bead.reopen", writeKey(9008), "alice", applicationTime)
		requireLifecycleHistory(t, f, ref, claimRecord.Revision, claimBody)
		requireLifecycleHistory(t, f, ref, closedRecord.Revision, closedBody)

		advance()
		startedAgain := applicationTime
		mustResult(t, dispatch(f.rpc, f.ctx, anonymous, "bead.claimNext", params(map[string]any{}, writeKey(9009), "alice")))
		requireLifecycleTimes(t, sqlLifecycleBody(t, f, ref), &createdAt, &startedAgain, nil)
		requireLifecycleClockProvenance(t, f, ref, "bead.claimNext", writeKey(9009), "alice", applicationTime)
		finalBody := sqlLifecycleBody(t, f, ref)
		before = f.state(t, "")
		replayed := mustResult(t, dispatch(f.rpc, f.ctx, anonymous, "bead.close", params(map[string]any{"id": ref, "reason": "first close"}, writeKey(9005), "alice")))
		if !reflect.DeepEqual(closed, replayed) {
			t.Fatalf("close receipt no longer answers original timestamp-bearing image: first=%+v replay=%+v", closed, replayed)
		}
		requireSameState(t, before, f.state(t, ""))
		f.reopen(t)
		composeClock()
		if !bytes.Equal(finalBody, sqlLifecycleBody(t, f, ref)) {
			t.Fatal("SQL close/reopen changed durable lifecycle bytes")
		}
		requireLifecycleHistory(t, f, ref, closedRecord.Revision, closedBody)
		if got := mustResult(t, dispatch(f.rpc, f.ctx, anonymous, "bead.close", params(map[string]any{"id": ref, "reason": "first close"}, writeKey(9005), "alice"))); !reflect.DeepEqual(got, closed) {
			t.Fatal("SQL close/reopen changed the original close receipt")
		}
		t.Logf("%s: persisted application lifecycle bytes, same-holder/close no-op, real driver receipt clock, historical bytes and store reopen/replay", e.name)
	})
}

func TestSQLLifecycleTimeRollbackAndTerminalSlots(t *testing.T) {
	eachEngine(t, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		createdAt := f.now()
		created := mustResult(t, dispatch(f.rpc, f.ctx, anonymous, "bead.create", params(map[string]any{
			"type": "issue", "fields": map[string]any{"title": "atomic lifecycle clock"},
		}, writeKey(9101), "alice")))
		ref := f.refFromResult(t, created)
		for i, method := range []string{"bead.claim", "bead.close", "bead.reopen", "bead.fail", "bead.reopen", "bead.cancel"} {
			f.advance(time.Second)
			key := writeKey(9110 + i)
			before := f.state(t, "")
			beforeBytes := sqlLifecycleBody(t, f, ref)
			f.engine.recoveryFault(t, true)
			mustError(t, dispatch(f.rpc, f.ctx, anonymous, method, params(map[string]any{"id": ref}, key, "alice")))
			f.engine.recoveryFault(t, false)
			requireSameState(t, before, f.state(t, ""))
			if !bytes.Equal(beforeBytes, sqlLifecycleBody(t, f, ref)) {
				t.Fatal("real SQL failure published a lifecycle timestamp")
			}
			r := f.reader(t)
			principal, err := driver.PrincipalKey(anonymous)
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := r.IdempotencyByKey(f.ctx, principal, "alice", method, key)
			_ = r.Close()
			if err != nil || receipt != nil {
				t.Fatalf("failed SQL lifecycle persisted receipt=%+v err=%v", receipt, err)
			}
			mustResult(t, dispatch(f.rpc, f.ctx, anonymous, method, params(map[string]any{"id": ref}, key, "alice")))
			doc, err := pipeline.DecodeDocument(sqlLifecycleBody(t, f, ref))
			if err != nil || doc.CreatedAt == nil || !doc.CreatedAt.Equal(createdAt) {
				t.Fatalf("lifecycle retry changed creation=%+v err=%v", doc, err)
			}
			if method == "bead.close" || method == "bead.fail" || method == "bead.cancel" {
				if doc.ClosedAt == nil || !doc.ClosedAt.Equal(f.now()) {
					t.Fatalf("terminal slot %s did not atomically persist its instant: %+v", method, doc)
				}
			} else if doc.ClosedAt != nil {
				t.Fatalf("nonterminal slot %s retained terminal time: %+v", method, doc)
			}
		}
	})
}

func TestSQLTypedClosePersistsTimesOnlyForItsOwnTargets(t *testing.T) {
	eachEngine(t, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		createdAt := f.now()
		created := mustResult(t, dispatch(f.rpc, f.ctx, anonymous, "tx.apply", params(map[string]any{
			"changeSet": model.ChangeSet{Ops: []model.Op{
				{Kind: model.OpCreate, Create: &model.CreateOp{Key: "fresh", Type: "issue", Fields: map[string]any{"title": "created then closed"}}},
				{Kind: model.OpClose, Close: &model.CloseOp{Target: model.Ref{Key: "fresh"}, Reason: "same atomic batch"}},
				{Kind: model.OpCreate, Create: &model.CreateOp{Key: "open", Type: "issue", Fields: map[string]any{"title": "unrelated open"}}},
			}},
		}, writeKey(9201), "alice")))
		results := created["results"].([]any)
		closed := f.refFromResult(t, map[string]any{"results": results[:1]})
		open := f.refFromResult(t, map[string]any{"results": results[2:]})
		closedBody := sqlLifecycleBody(t, f, closed)
		requireLifecycleTimes(t, closedBody, &createdAt, nil, &createdAt)
		requireLifecycleTimes(t, sqlLifecycleBody(t, f, open), &createdAt, nil, nil)
		f.advance(time.Second)
		mustResult(t, dispatch(f.rpc, f.ctx, anonymous, "tx.apply", params(map[string]any{
			"changeSet": model.ChangeSet{Ops: []model.Op{
				{Kind: model.OpPatch, Patch: &model.PatchOp{Target: open, Fields: map[string]any{"title": "unrelated patch"}}},
				{Kind: model.OpClose, Close: &model.CloseOp{Target: closed, Reason: "settlement"}},
			}},
		}, writeKey(9202), "alice")))
		requireLifecycleTimes(t, sqlLifecycleBody(t, f, closed), &createdAt, nil, &createdAt)
		requireLifecycleTimes(t, sqlLifecycleBody(t, f, open), &createdAt, nil, nil)
		f.reopen(t)
		requireLifecycleTimes(t, sqlLifecycleBody(t, f, closed), &createdAt, nil, &createdAt)
		requireLifecycleTimes(t, sqlLifecycleBody(t, f, open), &createdAt, nil, nil)
	})
}
