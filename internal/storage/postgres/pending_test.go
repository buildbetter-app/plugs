package postgres

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func receiptTest(t *testing.T, s *Store, key string) driver.IdempotencyRecord {
	t.Helper()
	r := testRead(t, s, nil)
	defer r.Close()
	value, err := r.IdempotencyByKey(context.Background(), "system:postgres-test", "", "test.pending", key)
	if err != nil || value == nil {
		t.Fatalf("receipt %s: %+v %v", key, value, err)
	}
	return *value
}

func pendingTest(t *testing.T, s *Store, lease driver.ActiveLease, key string, pending bool) driver.IdempotencyRecord {
	t.Helper()
	ref := testRef("pl-" + key)
	tx, h := beginTest(t, s, lease, testLock(ref))
	if err := tx.Put(context.Background(), driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"value":1}`)}); err != nil {
		t.Fatal(err)
	}
	h.Method, h.IdempotencyKey, h.RequestHash = "test.pending", key, "digest-"+key
	h.ResponsePending = pending
	h.Changes = []model.ChangeRecord{{Ref: ref, Op: model.OpCreate}}
	h.Response = model.Commit{
		Results:  []model.OpResult{{Ref: ref, Revision: model.Natural64(0), Record: []byte(`{"value":1}`)}},
		Warnings: []model.Warning{{Code: "post_subscriber_failed", Message: "preserved initial warning"}},
	}
	if _, err := tx.Commit(context.Background(), h); err != nil {
		t.Fatal(err)
	}
	return receiptTest(t, s, key)
}

func TestPendingReceiptSurvivesReopenAndSQLFinalizerFailure(t *testing.T) {
	ctx := context.Background()
	dsn := freshDSN(t)
	a := openTest(t, dsn)
	old := testLease(t, a)
	initial := pendingTest(t, a, old, "recovery", true)
	if !initial.ResponsePending {
		t.Fatal("ordinary graph publication lost pending state")
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	b := openTest(t, dsn)
	lease := testLease(t, b)
	if lease.Epoch() <= old.Epoch() || !reflect.DeepEqual(initial, receiptTest(t, b, "recovery")) {
		t.Fatal("pending receipt did not survive all pools closing and lease takeover")
	}
	scope, err := scopeKey("system:postgres-test", "", "test.pending", "recovery")
	if err != nil {
		t.Fatal(err)
	}
	var expires time.Time
	if err = b.db.QueryRowContext(ctx, `SELECT expires FROM plug_postgres.receipts WHERE scope=$1`, scope).Scan(&expires); err != nil {
		t.Fatal(err)
	}
	warnings := []model.Warning{{Code: "post_subscriber_failed", Message: "durable", Details: map[string]any{"binding": "post"}}}
	finalize := func(ctx context.Context, revision model.Revision, outcome []model.Warning) error {
		return b.FinalizeResponse(ctx, lease, "system:postgres-test", "", "test.pending", "recovery", revision, outcome)
	}
	// A real SQL error during the receipt update must roll back the whole
	// finalizer, leaving the known outcome retryable by the active holder.
	if _, err = b.db.ExecContext(ctx, `CREATE FUNCTION plug_postgres.reject_finalize() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'injected finalizer failure'; END$$; CREATE TRIGGER reject_finalize BEFORE UPDATE ON plug_postgres.receipts FOR EACH ROW EXECUTE FUNCTION plug_postgres.reject_finalize()`); err != nil {
		t.Fatal(err)
	}
	if err = finalize(ctx, initial.Commit.Revision, warnings); err == nil {
		t.Fatal("engine finalizer fault unexpectedly succeeded")
	}
	if !reflect.DeepEqual(initial, receiptTest(t, b, "recovery")) {
		t.Fatal("SQL finalizer failure changed pending receipt")
	}
	if _, err = b.db.ExecContext(ctx, `DROP TRIGGER reject_finalize ON plug_postgres.receipts; DROP FUNCTION plug_postgres.reject_finalize()`); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err = finalize(cancelled, initial.Commit.Revision, warnings); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled finalizer: %v", err)
	}
	if err = finalize(ctx, model.Natural64(2), warnings); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("wrong finalization revision: %v", err)
	}
	if !reflect.DeepEqual(initial, receiptTest(t, b, "recovery")) {
		t.Fatal("failed finalizer changed pending receipt")
	}
	pinned := testRead(t, b, nil)
	if err = finalize(ctx, initial.Commit.Revision, warnings); err != nil {
		t.Fatal(err)
	}
	if err = finalize(ctx, initial.Commit.Revision, warnings); err != nil {
		t.Fatalf("same outcome retry: %v", err)
	}
	expected := initial
	expected.ResponsePending = false
	expected.Commit.Warnings = append(expected.Commit.Warnings, warnings...)
	if got := receiptTest(t, b, "recovery"); !reflect.DeepEqual(got, expected) {
		t.Fatalf("finalizer did not preserve response/changeset/digest: %+v", got)
	}
	oldView, err := pinned.IdempotencyByKey(ctx, "system:postgres-test", "", "test.pending", "recovery")
	if err != nil || oldView == nil || !reflect.DeepEqual(*oldView, initial) {
		t.Fatalf("finalizer changed pinned snapshot: %+v %v", oldView, err)
	}
	var afterExpires time.Time
	if err = b.db.QueryRowContext(ctx, `SELECT expires FROM plug_postgres.receipts WHERE scope=$1`, scope).Scan(&afterExpires); err != nil || !expires.Equal(afterExpires) {
		t.Fatalf("finalization changed receipt age: %s -> %s %v", expires, afterExpires, err)
	}
	warnings[0].Details["binding"] = "different"
	if err = finalize(ctx, initial.Commit.Revision, warnings); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("different outcome: %v", err)
	}
	meta, err := b.Meta(ctx)
	if err != nil || meta.Head.String() != "1" {
		t.Fatalf("finalization changed graph head: %+v %v", meta, err)
	}
	if err = pinned.Close(); err != nil {
		t.Fatal(err)
	}
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
	c := openTest(t, dsn)
	if got := receiptTest(t, c, "recovery"); got.ResponsePending || got.Commit.Warnings[1].Details["binding"] != "post" {
		t.Fatalf("final outcome did not survive reopen or input mutation: %+v", got)
	}
	t.Log("real SQL finalizer failure and cancellation preserve pending receipt; active takeover finalizes exactly once without renewing its age or changing graph head")
}

func TestPendingReceiptNoopAndImmutableOutcomes(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	initial := pendingTest(t, s, lease, "empty", true)
	noop := func(pending bool) (model.Commit, error) {
		tx, h := beginTest(t, s, lease)
		defer tx.Close()
		h.Method, h.IdempotencyKey, h.RequestHash = "test.pending", "empty", "digest-empty"
		h.ResponsePending = pending
		return tx.(driver.NoopCommitter).CommitNoop(ctx, h)
	}
	if _, err := noop(true); err == nil {
		t.Fatal("no-op accepted a pending header")
	}
	if _, err := noop(false); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("no-op replayed unfinished pending response: %v", err)
	}
	finalize := func(key string, revision model.Revision, warnings []model.Warning) error {
		return s.FinalizeResponse(ctx, lease, "system:postgres-test", "", "test.pending", key, revision, warnings)
	}
	if err := finalize("empty", initial.Commit.Revision, nil); err != nil {
		t.Fatal(err)
	}
	if err := finalize("empty", initial.Commit.Revision, []model.Warning{}); err != nil {
		t.Fatalf("nil/empty outcome mismatch: %v", err)
	}
	if got := receiptTest(t, s, "empty"); got.ResponsePending || !reflect.DeepEqual(got.Commit, initial.Commit) {
		t.Fatalf("empty outcome did not clear pending without changing response: %+v", got)
	}
	if got, err := noop(false); err != nil || !reflect.DeepEqual(got, initial.Commit) {
		t.Fatalf("finalized graph receipt not replayed by no-op: %+v %v", got, err)
	}
	final := pendingTest(t, s, lease, "final", false)
	if err := finalize("final", final.Commit.Revision, []model.Warning{{Code: "post_subscriber_failed", Message: "forbidden"}}); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("initially final receipt accepted post warnings: %v", err)
	}
	if err := finalize("final", final.Commit.Revision, nil); err != nil {
		t.Fatalf("initially final receipt rejected empty outcome: %v", err)
	}
	if got := receiptTest(t, s, "final"); !reflect.DeepEqual(got, final) {
		t.Fatal("initially final receipt changed")
	}
	for _, mode := range []string{"unkeyed", "derived"} {
		t.Run(mode, func(t *testing.T) {
			tx, h := beginTest(t, s, lease)
			defer tx.Close()
			h.ResponsePending = true
			if mode == "derived" {
				h.Kind = model.ChangesetDerived
				h.Method, h.IdempotencyKey, h.RequestHash = "test.pending", "derived", "digest"
			}
			if _, err := tx.Commit(ctx, h); err == nil {
				t.Fatal("pending response accepted outside keyed ordinary graph write")
			}
		})
	}
}
