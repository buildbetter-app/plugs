package readyorder

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// CertificateFixture creates its source through the real Writer after the
// canonical empty startup backfill. No test-produced leaf certifies this source.
type CertificateFixture struct {
	Store    driver.Store
	Lease    driver.ActiveLease
	Ontology *service.Ontology
	Service  *service.Service
	Ref      model.Ref
	sequence int
}

func NewCertificateFixture(t *testing.T, s driver.Store) *CertificateFixture {
	t.Helper()
	ctx := context.Background()
	lease, err := s.Lease(ctx, "ready-physical-proof", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(context.Background()) })
	if err = pipeline.BuildSourceIndexes(ctx, s, lease); err != nil {
		t.Fatal(err)
	}
	o := service.NewOntology(nil)
	w := pipeline.New(s, lease, pipeline.WithOntology(o))
	f := &CertificateFixture{Store: s, Lease: lease, Ontology: o, Service: service.New(s, lease, w, service.WithOntology(o))}
	commit, err := f.Service.Create(ctx, model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": "physically maintained", "priority": 2}}, f.meta())
	if err != nil {
		t.Fatal(err)
	}
	f.Ref = commit.Results[0].Ref
	if result, err := f.Select(ctx); err != nil || result.Total != 1 || len(result.Keys) != 1 || result.Keys[0].Ref.ID != f.Ref.ID {
		t.Fatalf("maintained producer did not certify: %+v %v", result, err)
	}
	return f
}

func (f *CertificateFixture) meta() model.WriteMeta {
	f.sequence++
	return model.WriteMeta{Project: "local", Agent: "ready-proof", AgentLabel: "ready-proof", IdempotencyKey: fmt.Sprintf("%064x", f.sequence)}
}

func (f *CertificateFixture) Select(ctx context.Context) (driver.ReadyOrderResult, error) {
	m, err := f.Store.Meta(ctx)
	if err != nil {
		return driver.ReadyOrderResult{}, err
	}
	r, err := f.Store.Read(ctx, driver.View{Incarnation: m.Incarnation})
	if err != nil {
		return driver.ReadyOrderResult{}, err
	}
	defer r.Close()
	binding, err := f.Ontology.ReadyProjectionBinding("local")
	if err != nil {
		return driver.ReadyOrderResult{}, err
	}
	project := "local"
	return r.(driver.ReadyOrderReader).SelectReadyOrder(ctx, driver.ReadyOrderPlan{Incarnation: m.Incarnation, ContentRevision: m.Head, AuthorityRevision: m.Head, Eligibility: binding, Scope: driver.ReadyOrderScope{Project: &project}, UniformAuthorization: true, Limit: 1})
}

func (f *CertificateFixture) RequireUnavailable(t *testing.T) {
	t.Helper()
	if result, err := f.Select(context.Background()); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("uncertified physical source accelerated: %+v %v", result, err)
	}
}

func (f *CertificateFixture) Update(t *testing.T) {
	t.Helper()
	_, err := f.Service.Update(context.Background(), model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, service.UpdateRequest{ID: f.Ref, Fields: map[string]any{"title": "ordinary write remains accepted"}}, f.meta())
	if err != nil {
		t.Fatal(err)
	}
}

// PublishEmptyCertificate exercises the real empty control publication. It
// returns the public selector result, allowing an upgrade fixture to require
// refusal without treating metadata publication itself as a failed write.
func PublishEmptyCertificate(t *testing.T, s driver.Store) (driver.ReadyOrderResult, error) {
	t.Helper()
	ctx := context.Background()
	m, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.Lease(ctx, "ready-empty-proof", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	r, err := s.Read(ctx, driver.View{Incarnation: m.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := pipeline.PrepareReadyProjection(ctx, r, nil, nil, nil)
	if err != nil || !plan.Available() {
		t.Fatal("empty prepare", err)
	}
	_ = r.Close()
	keys := plan.LockKeys()
	requests := make(driver.LockSet, len(keys))
	for i, key := range keys {
		requests[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	tx, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, err = tx.Lock(ctx, requests); err != nil {
		t.Fatal(err)
	}
	head, err := tx.Head(ctx)
	if err != nil {
		t.Fatal(err)
	}
	n, _ := head.Uint64()
	if _, err = plan.Publish(ctx, tx, model.Natural64(n+1)); err != nil {
		t.Fatal(err)
	}
	if err = plan.Finalize(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Commit(ctx, driver.ChangesetHeader{Incarnation: m.Incarnation, Kind: model.ChangesetDerived, Principal: model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, LockKeys: keys}); err != nil {
		t.Fatal(err)
	}
	f := CertificateFixture{Store: s, Ontology: service.NewOntology(nil)}
	return f.Select(ctx)
}

// RunCertificateOverlay retains the last-stage guard and tests failed staging
// without weakening write admission or transaction overlay refusal.
func RunCertificateOverlay(t *testing.T, factory func(*testing.T) driver.Store) {
	for _, mode := range []string{"accepted_source", "failed_source", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			f := NewCertificateFixture(t, factory(t))
			m, err := f.Store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			r, err := f.Store.Read(ctx, driver.View{Incarnation: m.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			records, err := r.Get(ctx, []driver.Ref{f.Ref})
			if err != nil || len(records) != 1 {
				t.Fatal(records, err)
			}
			plan, err := pipeline.PrepareReadyProjection(ctx, r, nil, nil, nil)
			if err != nil || !plan.Available() {
				t.Fatal("control prepare", err)
			}
			_ = r.Close()
			bead, _ := driver.NewBeadSentinel(f.Ref.ID)
			keys := plan.LockKeys()
			keys = append(keys, driver.LockKey{Kind: driver.LockRecord, Ref: &f.Ref}, driver.LockKey{Kind: driver.LockSentinel, Sentinel: bead}, driver.GraphBlockingSentinelLock)
			sort.Slice(keys, func(i, j int) bool {
				a, _ := driver.EncodeLockKey(keys[i])
				b, _ := driver.EncodeLockKey(keys[j])
				return a < b
			})
			requests := make(driver.LockSet, len(keys))
			for i, key := range keys {
				requests[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
			}
			tx, err := f.Store.Write(ctx, f.Lease)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Close()
			if _, err = tx.Lock(ctx, requests); err != nil {
				t.Fatal(err)
			}
			head, err := tx.Head(ctx)
			if err != nil {
				t.Fatal(err)
			}
			hn, _ := head.Uint64()
			if err = tx.BumpSentinels(ctx, []driver.Sentinel{bead, driver.SentinelGraphBlocking}); err != nil {
				t.Fatal(err)
			}
			if _, err = plan.Publish(ctx, tx, model.Natural64(hn+1)); err != nil {
				t.Fatal(err)
			}
			if err = plan.Finalize(ctx, tx); err != nil {
				t.Fatal(err)
			}
			putCtx := ctx
			cancel := func() {}
			if mode == "failed_source" {
				putCtx, cancel = context.WithCancel(ctx)
				cancel()
			}
			defer cancel()
			err = tx.(driver.GraphSourceWriter).PutGraphRecord(putCtx, records[0], nil, nil)
			if mode == "failed_source" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if mode == "rollback" {
				if err = tx.Rollback(ctx); err != nil {
					t.Fatal(err)
				}
				if _, err = f.Select(ctx); err != nil {
					t.Fatal("rollback damaged old proof", err)
				}
				return
			}
			changes := []model.ChangeRecord{}
			if mode == "accepted_source" {
				changes = append(changes, model.ChangeRecord{Ref: f.Ref, Op: model.OpPatch, FromRevision: records[0].Revision})
			}
			if _, err = tx.Commit(ctx, driver.ChangesetHeader{Incarnation: m.Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, LockKeys: keys, Changes: changes}); err != nil {
				t.Fatal(err)
			}
			if mode == "accepted_source" {
				f.RequireUnavailable(t)
			} else if result, err := f.Select(ctx); err != nil || result.Total != 1 {
				t.Fatalf("failed stage invalidated valid proof: %+v %v", result, err)
			}
		})
	}
}
