package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

func TestS1PostgresClockControlsGeneratedCommitAndReceiptTime(t *testing.T) {
	ctx := context.Background()
	clock := testclock.New(t, time.Date(2037, 3, 29, 1, 59, 59, 123456000, time.FixedZone("seed", -4*60*60)))
	dsn := freshDSN(t)
	// Regression baseline: an application-only clock does not reach SQL.
	s := openTest(t, dsn)
	lease := testLease(t, s)
	h := driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: "clock"}, Method: "s1.clock", IdempotencyKey: "generated", RequestHash: "generated-hash"}
	entry, err := conformance.Seed(ctx, s, lease, h, []conformance.FixtureChange{{Ref: testRef("pl-clock"), Kind: "test", Op: model.OpCreate, Body: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	if !entry.Time.Equal(clock.Now()) {
		t.Fatalf("SQL-generated commit time=%s; application clock=%s", entry.Time, clock.Now())
	}
	var expires time.Time
	if err = s.db.QueryRowContext(ctx, `SELECT expires FROM plug_postgres.receipts`).Scan(&expires); err != nil {
		t.Fatal(err)
	}
	if !expires.Equal(clock.Now().Add(driver.IdempotencyWindow)) {
		t.Fatalf("SQL receipt expiry=%s want=%s", expires, clock.Now().Add(driver.IdempotencyWindow))
	}
}
