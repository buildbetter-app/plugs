package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

func TestS1SQLiteClockControlsGeneratedCommitAndReceiptTime(t *testing.T) {
	ctx := context.Background()
	clock := testclock.New(t, time.Date(2037, 3, 29, 1, 59, 59, 123456789, time.FixedZone("seed", -4*60*60)))
	value, err := (Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "clock.db")})
	if err != nil {
		t.Fatal(err)
	}
	s := value.(*Store)
	defer s.Close()
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	h := driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: "clock"}, Method: "s1.clock", IdempotencyKey: "generated", RequestHash: "generated-hash"}
	entry, err := conformance.Seed(ctx, s, lease, h, []conformance.FixtureChange{{Ref: model.RefByID("pl-clock", model.Natural64(0)), Kind: "test", Op: model.OpCreate, Body: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	if !entry.Time.Equal(clock.Now()) {
		t.Fatalf("driver-generated commit time=%s; application clock=%s", entry.Time, clock.Now())
	}
	var created int64
	if err = s.db.QueryRowContext(ctx, `SELECT created FROM replays`).Scan(&created); err != nil {
		t.Fatal(err)
	}
	if created != clock.Now().UnixNano() {
		t.Fatalf("driver receipt creation=%d want=%d", created, clock.Now().UnixNano())
	}
}
