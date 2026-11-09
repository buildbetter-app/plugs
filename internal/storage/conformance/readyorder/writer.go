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
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/readyindex"
)

func RunWriter(t *testing.T, factory func(*testing.T) driver.Store) {
	runWriter(t, factory, false)
}

// RunWriterMetadata checks the unchanged exact metadata oracle produced by real
// SQL Writer transactions. It additionally requires the public reader to refuse
// acceleration while the complete physical source certificate is unavailable.
// Passing these checks does not qualify public SQL Ready selection.
func RunWriterMetadata(t *testing.T, factory func(*testing.T) driver.Store) {
	runWriter(t, factory, true)
}

func selectWriterOrder(ctx context.Context, t *testing.T, r driver.Reader, p driver.ReadyOrderPlan, metadataOnly bool) (driver.ReadyOrderResult, error) {
	t.Helper()
	result, err := r.(driver.ReadyOrderReader).SelectReadyOrder(ctx, p)
	if !metadataOnly {
		return result, err
	}
	if !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("uncertified SQL snapshot accelerated: %+v %v", result, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, canceledErr := r.(driver.ReadyOrderReader).SelectReadyOrder(canceled, p); !errors.Is(canceledErr, context.Canceled) {
		t.Fatalf("SQL certification refusal lost cancellation: %v", canceledErr)
	}
	return readyindex.Select(ctx, r, p)
}

func runWriter(t *testing.T, factory func(*testing.T) driver.Store, metadataOnly bool) {
	t.Helper()
	t.Run("same_document_plugin_refresh", func(t *testing.T) { runPluginRefresh(t, factory, metadataOnly) })
	t.Run("hybrid_order_boundaries", func(t *testing.T) { runHybridOrderBoundaries(t, factory, metadataOnly) })
	ctx := context.Background()
	s := factory(t)
	lease, err := s.Lease(ctx, "ready-writer-regression", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(ctx) })
	o := service.NewOntology(nil)
	clockAt := time.Now().UTC().Truncate(time.Second)
	clock := func() time.Time { return clockAt }
	next := 0
	w := pipeline.New(s, lease, pipeline.WithOntology(o), pipeline.WithClock(clock), pipeline.WithIDGenerator(func() (string, error) { next++; return fmt.Sprintf("pl-ready-%03d", next), nil }))
	svc := service.New(s, lease, w, service.WithOntology(o), service.WithClock(clock))
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
		return selectWriterOrder(ctx, t, r, driver.ReadyOrderPlan{Incarnation: m.Incarnation, ContentRevision: m.Head, AuthorityRevision: m.Head, OrderAt: clock(), Scope: driver.ReadyOrderScope{Project: &project}, Eligibility: binding, UniformAuthorization: refs == nil, AuthorizedRefs: refs, RawRefs: raw, Seed: seed, Limit: 1000}, metadataOnly)
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
	// Exercise real unsupported producers rather than toggling a fixture's
	// Local bit: a claim and quiet heartbeat, backlog state and a waits-for
	// gate relation can affect membership outside this source's local fields.
	if _, err = svc.Claim(ctx, by, service.ClaimRequest{ID: refs[1]}, meta()); err != nil {
		t.Fatal(err)
	}
	if _, err = selectAt(nil); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatal("real lease used local-only tree", err)
	}
	heartbeatHead, e := s.Meta(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if _, err = svc.HeartbeatWithOutcome(ctx, by, service.HeartbeatRequest{ID: refs[1]}, meta()); err != nil {
		t.Fatal(err)
	}
	afterHeartbeat, e := s.Meta(ctx)
	if e != nil || afterHeartbeat.Head.String() != heartbeatHead.Head.String() {
		t.Fatal("quiet heartbeat changed source Head", e)
	}
	if _, err = selectAt(nil); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatal("quiet heartbeat restored unsupported coverage", err)
	}
	if _, err = svc.ReleaseWithOutcome(ctx, by, service.ReleaseRequest{ID: refs[1], Reason: "finished heartbeat regression"}, meta()); err != nil {
		t.Fatal(err)
	}
	check(nil)
	if _, err = svc.Defer(ctx, by, service.DeferRequest{ID: refs[2], Reason: "deferred source"}, meta()); err != nil {
		t.Fatal(err)
	}
	if _, err = selectAt(nil); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatal("backlog nonlocal domain used local-only tree", err)
	}
	if _, err = svc.Reopen(ctx, by, service.ReopenRequest{ID: refs[2], Reason: "restore source"}, meta()); err != nil {
		t.Fatal(err)
	}
	check(nil)
	relation := service.LinkRequest{From: refs[3], To: refs[4], Kind: "waits-for", Metadata: &model.LinkMetadata{Gate: "any-children"}}
	if _, err = svc.Link(ctx, by, relation, meta()); err != nil {
		t.Fatal(err)
	}
	if _, err = selectAt(nil); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatal("nonlocal waits-for gate used local-only tree", err)
	}
	// The unlink identity names the edge. Metadata is legal only when adding
	// a waits-for link, so do not carry its creation predicate into removal.
	relation.Metadata = nil
	if _, err = svc.Unlink(ctx, by, relation, meta()); err != nil {
		t.Fatal(err)
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

// The priority radix can serve main's hybrid order only with a complete
// common-creation/recent-bucket certificate. These are real Writer rows and
// the public main order remains the oracle when the optional path refuses.
func runHybridOrderBoundaries(t *testing.T, factory func(*testing.T) driver.Store, metadataOnly bool) {
	for _, aged := range []bool{false, true} {
		name := "distinct_creation"
		if aged {
			name = "aged_bucket"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s := factory(t)
			defer s.Close()
			lease, err := s.Lease(ctx, "ready-hybrid-boundary", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Release(ctx)
			clockAt := time.Now().UTC().Truncate(time.Second)
			clock := func() time.Time { return clockAt }
			next := 0
			o := service.NewOntology(nil)
			w := pipeline.New(s, lease, pipeline.WithOntology(o), pipeline.WithClock(clock), pipeline.WithIDGenerator(func() (string, error) {
				next++
				return fmt.Sprintf("pl-hybrid-%03d", next), nil
			}))
			svc := service.New(s, lease, w, service.WithOntology(o), service.WithClock(clock))
			by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
			create := func(n, priority int) {
				t.Helper()
				_, err := svc.Create(ctx, by, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": fmt.Sprintf("hybrid%d", n), "priority": priority}}, model.WriteMeta{Project: "local", Agent: "hybrid", IdempotencyKey: fmt.Sprintf("%064x", n)})
				if err != nil {
					t.Fatal(err)
				}
			}
			firstPriority := 0
			if aged {
				firstPriority = 4
			}
			create(1, firstPriority)
			if !aged {
				clockAt = clockAt.Add(time.Nanosecond)
			}
			create(2, 0)
			if aged {
				clockAt = clockAt.Add(48 * time.Hour)
			}
			meta, err := s.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			r, err := s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			binding, err := o.ReadyProjectionBinding("local")
			if err != nil {
				t.Fatal(err)
			}
			project := "local"
			seed := "raw-a"
			if aged {
				seed = ""
			}
			plan := driver.ReadyOrderPlan{Incarnation: meta.Incarnation, ContentRevision: meta.Head, AuthorityRevision: meta.Head, OrderAt: clock(), Scope: driver.ReadyOrderScope{Project: &project}, Eligibility: binding, UniformAuthorization: true, Seed: seed, Limit: 2}
			if aged {
				atBoundary, e := selectWriterOrder(ctx, t, r, plan, metadataOnly)
				if e != nil || len(atBoundary.Keys) != 2 || atBoundary.Keys[0].Ref.ID != "pl-hybrid-002" {
					t.Fatalf("48-hour recent boundary lost priority order: %+v %v", atBoundary, e)
				}
				public, e := svc.ClaimNextReadyPage(ctx, by, service.ReadyRequest{Project: "local", ClaimNextSeed: seed, Limit: 2})
				if e != nil || len(public.Items) != 2 || public.Items[0].ID != atBoundary.Keys[0].Ref.ID {
					t.Fatalf("48-hour ordinary boundary disagrees: %+v %v", public, e)
				}
				clockAt = clockAt.Add(time.Nanosecond)
				plan.OrderAt = clock()
			}
			if _, err = selectWriterOrder(ctx, t, r, plan, metadataOnly); !errors.Is(err, driver.ErrUnsupported) {
				t.Fatalf("uncertified hybrid order accelerated: %v", err)
			}
			page, err := svc.ClaimNextReadyPage(ctx, by, service.ReadyRequest{Project: "local", ClaimNextSeed: seed, Limit: 2})
			if err != nil || len(page.Items) != 2 || page.Items[0].ID != "pl-hybrid-001" || page.Items[1].ID != "pl-hybrid-002" {
				t.Fatalf("ordinary hybrid order changed: %+v %v", page, err)
			}
		})
	}
}

func runPluginRefresh(t *testing.T, factory func(*testing.T) driver.Store, metadataOnly bool) {
	ctx := context.Background()
	s := factory(t)
	lease, e := s.Lease(ctx, "ready-plugin-refresh", driver.LeaseTTL)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = lease.Release(ctx) })
	doc := ontology.DefaultDocument()
	doc.Types["issue"].Fields["plugin_field"] = ontology.Field{Type: "string", Owner: "ready-owner"}
	available, problems := ontology.Compile(doc, nil, ontology.PluginCatalog{"ready-owner": true})
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	missing, problems := ontology.Compile(doc, nil, ontology.PluginCatalog{"ready-owner": false})
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	a, _ := json.Marshal(available.Effective("local").Document())
	b, _ := json.Marshal(missing.Effective("local").Document())
	if string(a) != string(b) || available.Digest() == missing.Digest() {
		t.Fatal("fixture must change only compiled availability")
	}
	o := service.NewOntology(available.Effective)
	w := pipeline.New(s, lease, pipeline.WithOntology(o))
	svc := service.New(s, lease, w, service.WithOntology(o))
	by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	_, e = svc.Create(ctx, by, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": "same-document plugin regression"}}, model.WriteMeta{Project: "local", Agent: "refresh", AgentLabel: "refresh", IdempotencyKey: fmt.Sprintf("%064x", 1)})
	if e != nil {
		t.Fatal(e)
	}
	meta, e := s.Meta(ctx)
	if e != nil {
		t.Fatal(e)
	}
	read := func() (driver.ReadyOrderResult, error) {
		r, e := s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
		if e != nil {
			return driver.ReadyOrderResult{}, e
		}
		defer r.Close()
		binding, e := o.ReadyProjectionBinding("local")
		if e != nil {
			return driver.ReadyOrderResult{}, e
		}
		project := "local"
		return selectWriterOrder(ctx, t, r, driver.ReadyOrderPlan{Incarnation: meta.Incarnation, ContentRevision: meta.Head, AuthorityRevision: meta.Head, OrderAt: time.Now().UTC(), Scope: driver.ReadyOrderScope{Project: &project}, Eligibility: binding, UniformAuthorization: true, Limit: 1}, metadataOnly)
	}
	before, e := read()
	if e != nil || before.Total != 1 {
		t.Fatalf("initial live member: %+v %v", before, e)
	}
	beforeBinding, e := o.ReadyProjectionBinding("local")
	if e != nil {
		t.Fatal(e)
	}
	o.Refresh(missing)
	afterBinding, e := o.ReadyProjectionBinding("local")
	if e != nil || beforeBinding == afterBinding {
		t.Fatal("compiled degradation not bound", e)
	}
	if _, e = read(); !errors.Is(e, driver.ErrUnsupported) {
		t.Fatalf("degraded type used old Ready root: %v", e)
	}
	page, e := svc.Ready(ctx, by, service.ReadyRequest{Project: "local", Limit: 1})
	if e != nil || len(page.Items) != 0 {
		t.Fatalf("current oracle must omit degraded type: %+v %v", page, e)
	}
	afterMeta, e := s.Meta(ctx)
	if e != nil || afterMeta.Head.String() != meta.Head.String() {
		t.Fatal("Refresh must be revision stable", e)
	}
	o.Refresh(available)
	after, e := read()
	if e != nil || after.Commitment != before.Commitment {
		t.Fatalf("equivalent compiled eligibility did not restore exact membership: %+v %v", after, e)
	}
}
