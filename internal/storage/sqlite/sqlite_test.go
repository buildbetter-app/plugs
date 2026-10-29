package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// The shared full-cap fixture uses a minute-long setup context. SQLite's
// production lease policy is the fixed ten-second TTL, so this test adapter
// keeps the fixture on the real SQL store while capping only that requested
// lease duration to the driver's admitted maximum.
type auditFactoryStore struct{ *Store }

func (s auditFactoryStore) Lease(ctx context.Context, holder string, ttl time.Duration) (driver.ActiveLease, error) {
	extended := ttl > driver.LeaseTTL
	if ttl > driver.LeaseTTL {
		ttl = driver.LeaseTTL
	}
	lease, err := s.Store.Lease(ctx, holder, ttl)
	if err != nil || !extended {
		return lease, err
	}
	// The cap fixture can exceed one SQLite lease interval under -race. Keep
	// that real lease alive only for this test adapter; callers still receive
	// the concrete SQLite lease expected by Store.Write and finalization.
	go func() {
		ticker := time.NewTicker(driver.LeaseRenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := lease.Renew(context.Background()); err != nil {
					return
				}
			case <-lease.Lost():
				return
			}
		}
	}()
	return lease, nil
}

func TestConformance(t *testing.T) {
	conformance.Run(t, func(t *testing.T) driver.Store {
		t.Helper()
		s, err := (Driver{}).Open(context.Background(), driver.Config{DSN: filepath.Join(t.TempDir(), "store.db")})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		})
		return s
	})
}

func TestAuditConformance(t *testing.T) {
	conformance.RunAudit(t, func(t *testing.T) driver.Store {
		t.Helper()
		s, err := (Driver{}).Open(context.Background(), driver.Config{DSN: filepath.Join(t.TempDir(), "audit.db")})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		})
		return auditFactoryStore{Store: s.(*Store)}
	})
}

func TestAuditPlanSurvivesFileReopen(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "audit-reopen.db")
	first := openStore(t, ctx, path)
	lease := getLease(t, ctx, first, driver.LeaseTTL)
	entry, err := conformance.Seed(ctx, first, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: testRef("pl-reopen"), Kind: "bead", Op: model.OpCreate, Body: []byte(`{"status":"working"}`)}})
	if err != nil {
		t.Fatal(err)
	}
	meta := getMeta(t, ctx, first)
	provenance := model.EventProvenance{Method: "bead.close", Slot: "close", SetBy: map[string]string{"status": "request"}, Cause: model.EventCause{Root: "request", Chain: []string{}}, Chain: []string{}}
	h := driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "system", ID: "reopen"}, Method: "bead.close", IdempotencyKey: "reopen-audit", RequestHash: "reopen-digest", Events: []model.EventDescriptor{{Index: "0", ChangeIndex: "0", Type: model.EventBeadClosed, Resource: model.AuditResource{Kind: "bead", Ref: testRef("pl-reopen")}, Provenance: provenance}}}
	if _, err := conformance.Seed(ctx, first, lease, h, []conformance.FixtureChange{{Ref: testRef("pl-reopen"), Kind: "bead", Op: model.OpPatch, Body: []byte(`{"status":"finished"}`), ChangedPaths: []string{"/status"}}}); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := openStore(t, ctx, path)
	getLease(t, ctx, second, driver.LeaseTTL)
	r := getReader(t, ctx, second, meta, nil)
	log, err := r.Changesets(ctx, &driver.Cursor{Incarnation: meta.Incarnation, Revision: entry.Revision}, 10)
	if err != nil || len(log) != 1 || len(log[0].Events) != 1 || log[0].Events[0].Type != model.EventBeadClosed {
		t.Fatalf("reopened audit log=%+v %v", log, err)
	}
	receipt, err := r.ChangesetByKey(ctx, "system:reopen", "", h.Method, h.IdempotencyKey)
	if err != nil || receipt == nil || len(receipt.Events) != 1 || receipt.Events[0].Resource.Ref.ID != "pl-reopen" {
		t.Fatalf("reopened audit receipt=%+v %v", receipt, err)
	}
}
