package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestCounterDomainsRefuseOverflow(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	// These are synthetic boundary values, not a claimed gapless production
	// history. The checked domain must survive SQL and JSON above MaxInt64.
	aboveInt64 := uint64(math.MaxInt64) + 42
	if _, err := s.db.ExecContext(ctx, `UPDATE plug_postgres.meta SET head=$1 WHERE singleton`, decimal(aboveInt64)); err != nil {
		t.Fatal(err)
	}
	entry := seedTest(t, s, lease, "pl-large-revision", `{"value":1}`)
	if entry.Revision.String() != decimal(aboveInt64+1) {
		t.Fatalf("revision narrowed: %s", entry.Revision)
	}
	records, err := testRead(t, s, nil).Get(ctx, []driver.Ref{testRef("pl-large-revision")})
	if err != nil || len(records) != 1 || records[0].Revision.String() != entry.Revision.String() {
		t.Fatalf("large revision read=%v %v", records, err)
	}
	if _, err = s.db.ExecContext(ctx, `UPDATE plug_postgres.meta SET head=$1 WHERE singleton`, decimal(math.MaxUint64)); err != nil {
		t.Fatal(err)
	}
	tx, h := beginTest(t, s, lease)
	if _, err = tx.Commit(ctx, h); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("revision overflow=%v", err)
	}
	_ = tx.Close()
	meta, err := s.Meta(ctx)
	if err != nil || meta.Head.String() != decimal(math.MaxUint64) {
		t.Fatalf("overflow changed head: %+v %v", meta, err)
	}
	if _, err = s.db.ExecContext(ctx, `INSERT INTO plug_postgres.checkpoints VALUES ('generation',$1,'value')`, decimal(math.MaxUint64)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SaveCheckpoint(ctx, lease, "generation", math.MaxUint64, []byte("overflow")); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("generation overflow=%v", err)
	}
	checkpoint, err := s.LoadCheckpoint(ctx, "generation")
	if err != nil || checkpoint.Generation != math.MaxUint64 || string(checkpoint.Payload) != "value" {
		t.Fatalf("generation narrowed: %+v %v", checkpoint, err)
	}
	if err = lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.ExecContext(ctx, `UPDATE plug_postgres.lease SET epoch=$1 WHERE singleton`, decimal(math.MaxUint64-1)); err != nil {
		t.Fatal(err)
	}
	last := testLease(t, s)
	if last.Epoch() != math.MaxUint64 {
		t.Fatalf("epoch narrowed: %d", last.Epoch())
	}
	if err = last.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Lease(ctx, "overflow", driver.LeaseTTL); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("epoch overflow=%v", err)
	}
	tooLarge := model.Natural("18446744073709551616")
	if _, err = s.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &tooLarge}); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("oversized as-of=%v", err)
	}
}

func TestIdempotencyWindowUsesDatabaseClockAndAnonymousScope(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	l := testLease(t, s)
	h := driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "anonymous"}, AgentLabel: "agent-a", Method: "bead.create", IdempotencyKey: "window", RequestHash: "hash"}
	seed := func(id string) (model.Changeset, error) {
		return conformance.Seed(ctx, s, l, h, []conformance.FixtureChange{{Ref: testRef(id), Kind: "test", Op: model.OpCreate, Body: []byte(`{}`)}})
	}
	first, err := seed("pl-original")
	if err != nil {
		t.Fatal(err)
	}
	if replayed, err := seed("pl-retry"); err != nil || replayed.Revision.String() != first.Revision.String() {
		t.Fatalf("live receipt replay=%+v %v", replayed, err)
	}
	scope, err := scopeKey("anonymous:", "agent-a", h.Method, h.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	// Advance only this disposable receipt's expiry boundary, without changing
	// header attribution time or waiting 24 hours.
	if _, err = s.db.ExecContext(ctx, `UPDATE plug_postgres.receipts SET expires=clock_timestamp()-interval '1 microsecond' WHERE scope=$1`, scope); err != nil {
		t.Fatal(err)
	}
	expired, err := testRead(t, s, nil).IdempotencyByKey(ctx, "anonymous:", "agent-a", h.Method, h.IdempotencyKey)
	if err != nil || expired != nil {
		t.Fatalf("expired receipt=%+v %v", expired, err)
	}
	next, err := seed("pl-after-window")
	if err != nil || next.Revision.String() != "2" {
		t.Fatalf("expired key refused reuse: %+v %v", next, err)
	}
	// Full-scope isolation: the same key on another anonymous label is fresh.
	h.AgentLabel = "agent-b"
	h.RequestHash = "different"
	third, err := seed("pl-other-agent")
	if err != nil || third.Revision.String() != "3" {
		t.Fatalf("anonymous isolation=%+v %v", third, err)
	}
	log, err := testRead(t, s, nil).Changesets(ctx, nil, 100)
	if err != nil || len(log) != 3 || log[0].IdempotencyKey != "window" {
		t.Fatalf("expiration removed history=%v %v", log, err)
	}
}

func TestHistoryFloorAndUnsupportedInputs(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	l := testLease(t, s)
	for i := 1; i <= 3; i++ {
		seedTest(t, s, l, "pl-history", fmt.Sprintf(`{"value":%d}`, i))
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE plug_postgres.meta SET history_floor=2 WHERE singleton`); err != nil {
		t.Fatal(err)
	}
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	at := model.Natural64(1)
	_, err = s.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &at})
	var truncated driver.ErrTruncated
	if !errors.As(err, &truncated) || truncated.Floor.String() != "2" {
		t.Fatalf("history floor read=%v", err)
	}
	r := testRead(t, s, nil)
	if _, err = r.Changesets(ctx, &driver.Cursor{Incarnation: meta.Incarnation, Revision: at}, 100); !errors.As(err, &truncated) {
		t.Fatalf("history floor cursor=%v", err)
	}
	if _, err = r.Query(ctx, driver.Query{Filter: map[string]any{"sql": "SELECT *"}}); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("unknown query=%v", err)
	}
	if _, err = s.Read(ctx, driver.View{Incarnation: meta.Incarnation, Branch: model.Natural64(1)}); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("unknown branch=%v", err)
	}
	if _, err = s.Migrate(ctx, 2); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("unknown migration=%v", err)
	}
	if _, err = Open(ctx, driver.Config{DSN: "not opened", Options: map[string]any{"ignored": true}}); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("unknown config=%v", err)
	}
	tx, err := s.Write(ctx, l)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	longRef := testRef(strings.Repeat("x", 2049))
	if _, err = tx.Lock(ctx, []driver.LockRequest{{Key: testLock(longRef), Mode: driver.LockUpdate}}); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("oversized key=%v", err)
	}
}
