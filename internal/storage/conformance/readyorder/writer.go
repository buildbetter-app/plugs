// Package readyorder exercises Ready metadata produced by the real Writer.
// It does not substitute a point-image fixture for source publication.
package readyorder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func RunWriter(t *testing.T, factory func(*testing.T) driver.Store) {
	t.Helper()
	ctx := context.Background()
	s := factory(t)
	lease, err := s.Lease(ctx, "ready-writer-regression", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(ctx) })
	o := service.NewOntology(nil)
	next := 0
	w := pipeline.New(s, lease, pipeline.WithOntology(o), pipeline.WithIDGenerator(func() (string, error) { next++; return fmt.Sprintf("pl-ready-%03d", next), nil }))
	svc := service.New(s, lease, w, service.WithOntology(o))
	by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	sequence := 0
	meta := func() model.WriteMeta {
		sequence++
		return model.WriteMeta{Project: "local", Agent: "ready-test", AgentLabel: "ready-test", IdempotencyKey: fmt.Sprintf("%064x", sequence)}
	}
	selectPlan := func(refs, raw []model.Ref, seed string) (driver.ReadyOrderResult, error) {
		m, e := s.Meta(ctx)
		if e != nil {
			return driver.ReadyOrderResult{}, e
		}
		r, e := s.Read(ctx, driver.View{Incarnation: m.Incarnation})
		if e != nil {
			return driver.ReadyOrderResult{}, e
		}
		defer r.Close()
		binding, e := o.ReadyProjectionBinding("local")
		if e != nil {
			return driver.ReadyOrderResult{}, e
		}
		project := "local"
		return r.(driver.ReadyOrderReader).SelectReadyOrder(ctx, driver.ReadyOrderPlan{Incarnation: m.Incarnation, ContentRevision: m.Head, AuthorityRevision: m.Head, Scope: driver.ReadyOrderScope{Project: &project}, Eligibility: binding, UniformAuthorization: refs == nil, AuthorizedRefs: refs, RawRefs: raw, Seed: seed, Limit: 1000})
	}
	selectAt := func(refs []model.Ref) (driver.ReadyOrderResult, error) { return selectPlan(refs, nil, "") }
	// Compare every thin key and full semantic leaf against public Ready and
	// its actual source revision, rather than a derived-row revision oracle.
	check := func(refs []model.Ref) driver.ReadyOrderResult {
		t.Helper()
		got, e := selectAt(refs)
		if e != nil {
			t.Fatal(e)
		}
		page, e := svc.Ready(ctx, by, service.ReadyRequest{Project: "local", Limit: 1000})
		if e != nil {
			t.Fatal(e)
		}
		wanted := map[string]bool{}
		for _, ref := range refs {
			wanted[ref.ID] = true
		}
		leaves := []driver.ReadyOrderLeaf{}
		for _, item := range page.Items {
			if refs != nil && !wanted[item.ID] {
				continue
			}
			priority := int64(2)
			if p, ok := item.Fields["priority"]; ok {
				n, e := json.Number(fmt.Sprint(p)).Int64()
				if e != nil {
					t.Fatal(e)
				}
				priority = n
			}
			leaf, e := driver.ReadyOrderLeafFor(driver.ReadyMembershipItem{ID: item.ID, Type: item.Type, Project: item.Project, Status: item.Status, Revision: item.Revision, Fields: item.Fields, Plane: item.Plane, Kind: item.Kind, Plugin: item.Plugin, WorkflowRunID: item.WorkflowRunID}, priority)
			if e != nil {
				t.Fatal(e)
			}
			leaves = append(leaves, leaf)
		}
		want, e := driver.ReadyCommitmentFromLeaves(leaves)
		if e != nil {
			t.Fatal(e)
		}
		if got.Commitment != want || got.Total != uint64(len(leaves)) || len(got.Keys) != len(leaves) || got.More {
			t.Fatalf("Writer/public Ready mismatch: got=%+v want=%+v", got, want)
		}
		for i, leaf := range leaves {
			key := got.Keys[i]
			if key.Ref.ID != leaf.Key.Ref.ID || key.Revision.String() != leaf.Key.Revision.String() || key.MembershipLeaf != leaf.Digest {
				t.Fatalf("source revision/field/order mismatch at%d: %+v %+v", i, key, leaf)
			}
		}
		t.Logf("READY_WRITER total=%d point_nodes=%d leaves=%d digest_bytes=%d", got.Total, got.Work.Nodes, got.Work.Leaves, got.Work.DigestBytes)
		return got
	}
	refs := []model.Ref{}
	for i := 0; i < 24; i++ {
		commit, e := svc.Create(ctx, by, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": fmt.Sprintf("item%d", i), "priority": i % 5, "description": "complete semantic field"}}, meta())
		if e != nil {
			t.Fatal(e)
		}
		refs = append(refs, commit.Results[0].Ref)
	}
	check(nil)
	authorized := []model.Ref{refs[0], refs[8], refs[23]}
	for _, raw := range [][]model.Ref{nil, refs[:12]} {
		var filter *service.Filter
		if raw != nil {
			filter = &service.Filter{Resources: []string{}}
			for _, ref := range raw {
				filter.Resources = append(filter.Resources, ref.ID)
			}
		}
		for _, seed := range []string{"raw-a", "raw-b", "raw-c", "raw-d"} {
			oracle, e := svc.ClaimNextReadyPage(ctx, by, service.ReadyRequest{Project: "local", Limit: 1000, ClaimNextSeed: seed, Filter: filter})
			if e != nil {
				t.Fatal(e)
			}
			for _, auth := range [][]model.Ref{nil, authorized} {
				got, e := selectPlan(auth, raw, seed)
				if e != nil {
					t.Fatal(e)
				}
				want := []string{}
				for _, item := range oracle.Items {
					allowed := auth == nil
					for _, ref := range auth {
						allowed = allowed || ref.ID == item.ID
					}
					if allowed {
						want = append(want, item.ID)
					}
				}
				if len(want) != len(got.Keys) {
					t.Fatal("raw group membership differs")
				}
				for i, key := range got.Keys {
					if key.Ref.ID != want[i] {
						t.Fatalf("raw seed applied after authorization or before request refs: seed=%s index=%d got=%s want=%s", seed, i, key.Ref.ID, want[i])
					}
				}
			}
		}
	}
	before := check(authorized)
	if _, err = svc.Update(ctx, by, service.UpdateRequest{ID: refs[22], Fields: map[string]any{"description": "hidden changed field"}}, meta()); err != nil {
		t.Fatal(err)
	}
	if after := check(authorized); after.Commitment != before.Commitment {
		t.Fatal("hidden source changed authorized commitment")
	}
	if _, err = svc.Update(ctx, by, service.UpdateRequest{ID: refs[23], Fields: map[string]any{"description": "authorized tail changed field"}}, meta()); err != nil {
		t.Fatal(err)
	}
	if after := check(authorized); after.Commitment == before.Commitment {
		t.Fatal("authorized tail/field/revision omitted")
	}
	before = check(nil)
	if _, err = svc.MemorySave(ctx, by, service.MemorySaveRequest{Key: "control", Content: "carry forward"}, meta()); err != nil {
		t.Fatal(err)
	}
	if after := check(nil); after.Commitment != before.Commitment {
		t.Fatal("non-bead publication changed Ready membership")
	}
	for _, ref := range refs {
		if _, err = svc.Close(ctx, by, service.CloseRequest{ID: ref, Reason: "complete"}, meta()); err != nil {
			t.Fatal(err)
		}
	}
	check(nil)
	beforeHead, e := s.Meta(ctx)
	if e != nil {
		t.Fatal(e)
	}
	out, e := svc.ClaimNextWithOutcome(ctx, by, service.ClaimNextRequest{Project: "local"}, meta())
	if e != nil || out == nil || !out.NullResult {
		t.Fatalf("terminal receipt: %+v %v", out, e)
	}
	afterHead, e := s.Meta(ctx)
	if e != nil {
		t.Fatal(e)
	}
	bn, _ := beforeHead.Head.Uint64()
	an, _ := afterHead.Head.Uint64()
	if an != bn+1 {
		t.Fatal("terminal did not commit H+1")
	}
	check(nil)
	// Corrupt optional metadata must disable acceleration without refusing an
	// otherwise valid ordinary Writer mutation. Existing source guards remain.
	marker := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedReadyState, Key: "plug.ready.v2/coverage"}}
	sentinel, e := driver.NewReadySentinel("*", "*")
	if e != nil {
		t.Fatal(e)
	}
	keys := []driver.LockKey{marker, {Kind: driver.LockSentinel, Sentinel: sentinel}}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	locks := make(driver.LockSet, len(keys))
	for i, key := range keys {
		locks[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	tx, e := s.Write(ctx, lease)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Close()
	defer tx.Rollback(ctx)
	if _, e = tx.Lock(ctx, locks); e != nil {
		t.Fatal(e)
	}
	if _, e = tx.Head(ctx); e != nil {
		t.Fatal(e)
	}
	if e = tx.PutDerived(ctx, driver.DerivedReadyState, nil, []driver.DerivedRow{{Key: marker.Derived.Key, Value: []byte("malformed optional coverage")}}); e != nil {
		t.Fatal(e)
	}
	if e = tx.BumpSentinels(ctx, []driver.Sentinel{sentinel}); e != nil {
		t.Fatal(e)
	}
	if _, e = tx.Commit(ctx, driver.ChangesetHeader{Incarnation: afterHead.Incarnation, Kind: model.ChangesetDerived, Principal: by, LockKeys: keys}); e != nil {
		t.Fatal(e)
	}
	if _, err = svc.Update(ctx, by, service.UpdateRequest{ID: refs[0], Fields: map[string]any{"title": "accepted despite optional metadata"}}, meta()); err != nil {
		t.Fatalf("optional metadata refused source write: %v", err)
	}
	if _, err = selectAt(nil); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("malformed metadata accelerated: %v", err)
	}
}
