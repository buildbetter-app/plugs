package memory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestAuditConformance(t *testing.T) {
	conformance.RunAudit(t, func(t *testing.T) driver.Store { return New() })
}

// The clock callback acts only after staging is complete. It cancels a live
// request or expires its real lease while Commit prepares the next snapshot.
// Both must still be checked immediately before publication.
func TestAuditPublicationRechecksCancellationAndLease(t *testing.T) {
	for _, mode := range []string{"cancel_during_commit", "expire_during_commit"} {
		t.Run(mode, func(t *testing.T) {
			fixed := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
			var mu sync.Mutex
			now := fixed
			var hook func()
			clock := func() time.Time {
				mu.Lock()
				defer mu.Unlock()
				if hook != nil {
					hook()
				}
				return now
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s, err := NewWithOptions(Options{Clock: clock})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			lease, err := s.Lease(ctx, "audit", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			meta, err := s.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			ref := model.RefByID("pl-audit", model.Natural64(0))
			sentinel, err := driver.NewBeadSentinel(ref.ID)
			if err != nil {
				t.Fatal(err)
			}
			keys := []driver.LockKey{{Kind: driver.LockRecord, Ref: &ref}, {Kind: driver.LockSentinel, Sentinel: sentinel}}
			tx, err := s.Write(ctx, lease)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Close()
			if _, err := tx.Lock(ctx, []driver.LockRequest{{Key: keys[0], Mode: driver.LockUpdate}, {Key: keys[1], Mode: driver.LockUpdate}}); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Head(ctx); err != nil {
				t.Fatal(err)
			}
			if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "bead", Body: []byte(`{}`)}); err != nil {
				t.Fatal(err)
			}
			if err := tx.BumpSentinels(ctx, []driver.Sentinel{sentinel}); err != nil {
				t.Fatal(err)
			}
			h := driver.ChangesetHeader{Incarnation: meta.Incarnation, Time: fixed, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "system", ID: "audit"}, Chain: []string{}, IdempotencyKey: "audit-key", Method: "bead.create", RequestHash: "digest", LockKeys: keys, Changes: []model.ChangeRecord{{Ref: ref, Op: model.OpCreate}}, Events: []model.EventDescriptor{{Type: model.EventBeadCreated, Resource: model.AuditResource{Kind: "bead", Ref: ref}, Provenance: model.EventProvenance{Method: "bead.create", SetBy: map[string]string{}, Chain: []string{}, Cause: model.EventCause{Root: "request", Chain: []string{}}}}}}
			calls := 0
			mu.Lock()
			hook = func() {
				calls++
				if mode == "cancel_during_commit" {
					cancel()
				} else if calls == 2 {
					now = fixed.Add(driver.LeaseTTL)
				}
			}
			mu.Unlock()
			_, err = tx.Commit(ctx, h)
			if mode == "cancel_during_commit" {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("mid-commit cancellation=%v", err)
				}
			} else {
				var fenced driver.ErrFenced
				if !errors.As(err, &fenced) {
					t.Fatalf("mid-commit lease expiry=%v", err)
				}
			}
			mu.Lock()
			hook = nil
			mu.Unlock()
			live := context.Background()
			r, err := s.Read(live, driver.View{Incarnation: meta.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			log, err := r.Changesets(live, nil, 100)
			if err != nil || len(log) != 0 || !r.Snapshot().At.IsZero() {
				t.Fatalf("refused audit published a revision/log: %+v %v", log, err)
			}
			rows, err := r.Get(live, []driver.Ref{ref})
			if err != nil || len(rows) != 0 {
				t.Fatalf("refused audit published a record: %+v %v", rows, err)
			}
			cached, err := r.IdempotencyByKey(live, "system:audit", "", h.Method, h.IdempotencyKey)
			if err != nil || cached != nil {
				t.Fatalf("refused audit published a receipt: %+v %v", cached, err)
			}
		})
	}
}
