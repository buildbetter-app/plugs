package parentedcreateflat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestNumericChildIdentitySemantics(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			dsn := ""
			if backend == "postgres" {
				dsn = newPostgresHarness(t).dsn(t)
			}
			o := newOrdinaryB04(t, backend, dsn, nil)
			seed := func(changes ...conformance.FixtureChange) model.Changeset {
				t.Helper()
				entry, err := conformance.Seed(o.f.ctx, o.f.store, o.f.lease, driver.ChangesetHeader{}, changes)
				if err != nil {
					t.Fatal(err)
				}
				return entry
			}
			image := func(id, kind string) conformance.FixtureChange {
				raw, err := json.Marshal(pipeline.Document{ID: id, Type: "issue", Project: project, Fields: map[string]any{"title": id, "status": "todo"}})
				if err != nil {
					t.Fatal(err)
				}
				return conformance.FixtureChange{Ref: ref(id), Kind: kind, Op: model.OpCreate, Body: raw}
			}
			first := seed(image("p.9", "bead"), image("p.10", "bead"), image("p.0010", "bead"), image("p.0", "bead"), image("p.word", "bead"), image("p.10.99", "bead"), image("p-other.999", "bead"), image("p.100", "memory"), image("unicode-父.10", "bead"), image("overflow.18446744073709551616", "bead"), image("limit.18446744073709551615", "bead"))
			lookup := func(at *model.Revision, parent string, excluded []model.Ref, want uint64, overflow bool) {
				t.Helper()
				r, err := o.f.store.Read(o.f.ctx, driver.View{Incarnation: o.f.metaSnapshot().Incarnation, At: at})
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				capability, ok := r.(driver.NumericChildReader)
				if !ok {
					t.Fatal("real engine lacks NumericChildReader")
				}
				got, err := capability.MaxChildNumber(o.f.ctx, parent, excluded)
				if overflow {
					if !errors.Is(err, driver.ErrUnsupported) {
						t.Fatalf("overflow not retained: %d %v", got, err)
					}
					return
				}
				if err != nil || got != want {
					t.Fatalf("max(%s)=%d want=%d err=%v", parent, got, want, err)
				}
				ctx, cancel := context.WithCancel(o.f.ctx)
				cancel()
				if _, err := capability.MaxChildNumber(ctx, parent, nil); !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled lookup: %v", err)
				}
			}
			lookup(nil, "p", nil, 10, false)
			lookup(nil, "p", []model.Ref{ref("p.10"), ref("p.0010")}, 9, false)
			lookup(nil, "p.10", nil, 99, false)
			lookup(nil, "missing", nil, 0, false)
			lookup(nil, "unicode-父", nil, 10, false)
			lookup(nil, "overflow", nil, 0, true)
			lookup(nil, "limit", nil, ^uint64(0), false)
			seed(image("p.30", "bead"))
			seed(conformance.FixtureChange{Ref: ref("p.10"), Kind: "bead", Op: model.OpDelete}, conformance.FixtureChange{Ref: ref("p.0010"), Kind: "bead", Op: model.OpDelete}, conformance.FixtureChange{Ref: ref("p.30"), Kind: "bead", Op: model.OpDelete})
			lookup(nil, "p", nil, 9, false)
			lookup(&first.Revision, "p", nil, 10, false)
			lookup(&first.Revision, "p", []model.Ref{ref("p.10")}, 10, false)
			lookup(&first.Revision, "p", []model.Ref{ref("p.0010")}, 10, false)
			lookup(&first.Revision, "p", []model.Ref{ref("p.10"), ref("p.0010")}, 9, false)

			// The real WriteTx must suppress committed replacements, include its
			// staged live IDs, and let caller exclusions override staged images.
			tx, err := o.f.store.Write(o.f.ctx, o.f.lease)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Close()
			var locks driver.LockSet
			for _, id := range []string{"p.20", "p.9"} {
				r := ref(id)
				locks = append(locks, driver.LockRequest{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &r}, Mode: driver.LockUpdate})
			}
			if _, err := tx.Lock(o.f.ctx, locks); err != nil {
				t.Fatal(err)
			}
			if err := tx.Delete(o.f.ctx, ref("p.9")); err != nil {
				t.Fatal(err)
			}
			i := image("p.20", "bead")
			if err := tx.Put(o.f.ctx, driver.Record{Ref: i.Ref, Kind: i.Kind, Body: i.Body}); err != nil {
				t.Fatal(err)
			}
			c := tx.(driver.NumericChildReader)
			if n, err := c.MaxChildNumber(o.f.ctx, "p", nil); err != nil || n != 20 {
				t.Fatalf("staged max=%d err=%v", n, err)
			}
			if n, err := c.MaxChildNumber(o.f.ctx, "p", []model.Ref{ref("p.20")}); err != nil || n != 0 {
				t.Fatalf("staged-excluded max=%d err=%v", n, err)
			}
			if err := tx.Rollback(o.f.ctx); err != nil {
				t.Fatal(err)
			}
			lookup(nil, "p", nil, 9, false)
		})
	}
}

type requestProofKey struct{}
type queryProbe struct {
	mu                        sync.Mutex
	queryCalls, identityCalls int
	contextMissing            bool
	identityError             string
}
type noQueryStore struct {
	driver.Store
	driver.ResponseFinalizer
	driver.CheckpointStore
	probe *queryProbe
}
type noQueryReader struct {
	driver.Reader
	driver.AllocatorReader
	driver.ParentCounterReader
	driver.NumericChildReader
	driver.HierarchyChildrenExcludingReader
	driver.InverseSourceLinksReader
	driver.DerivedPrefixReader
	probe *queryProbe
}

func (s noQueryStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return &noQueryReader{Reader: r, AllocatorReader: r.(driver.AllocatorReader), ParentCounterReader: r.(driver.ParentCounterReader), NumericChildReader: r.(driver.NumericChildReader), HierarchyChildrenExcludingReader: r.(driver.HierarchyChildrenExcludingReader), InverseSourceLinksReader: r.(driver.InverseSourceLinksReader), DerivedPrefixReader: r.(driver.DerivedPrefixReader), probe: s.probe}, nil
}
func (r *noQueryReader) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	r.probe.mu.Lock()
	r.probe.queryCalls++
	r.probe.mu.Unlock()
	return driver.Page[driver.Record]{}, fmt.Errorf("ordinary create attempted a general Query")
}
func (r *noQueryReader) MaxChildNumber(ctx context.Context, parent string, excluded []model.Ref) (uint64, error) {
	r.probe.mu.Lock()
	r.probe.identityCalls++
	if ctx.Value(requestProofKey{}) != r.probe {
		r.probe.contextMissing = true
		r.probe.mu.Unlock()
		return 0, fmt.Errorf("request context did not reach real child capability")
	}
	r.probe.mu.Unlock()
	n, err := r.NumericChildReader.MaxChildNumber(ctx, parent, excluded)
	if err != nil {
		r.probe.mu.Lock()
		r.probe.identityError = err.Error()
		r.probe.mu.Unlock()
	}
	return n, err
}

func TestOrdinaryChildCreateUsesRealIdentityIndexWithoutQuery(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			dsn := ""
			if backend == "postgres" {
				dsn = newPostgresHarness(t).dsn(t)
			}
			o := newOrdinaryB04(t, backend, dsn, []string{"root"})
			o.seed("anchor", ordinaryCreateOp("root", project, map[string]any{"title": "root", "priority": 4}, ""))
			var changes []conformance.FixtureChange
			for _, v := range []struct {
				id, kind string
				op       model.OpKind
			}{{"root.009", "bead", model.OpCreate}, {"root.10", "memory", model.OpCreate}, {"root.11", "bead", model.OpCreate}} {
				body, _ := json.Marshal(pipeline.Document{ID: v.id, Type: "issue", Project: project, Fields: map[string]any{"title": v.id, "status": "todo"}})
				change := conformance.FixtureChange{Ref: ref(v.id), Kind: v.kind, Op: v.op, Body: body}
				if v.kind == "bead" {
					change.Graph = &conformance.FixtureGraphSource{}
				}
				changes = append(changes, change)
			}
			if _, err := conformance.Seed(o.f.ctx, o.f.store, o.f.lease, driver.ChangesetHeader{}, changes); err != nil {
				t.Fatal(err)
			}
			if _, err := conformance.Seed(o.f.ctx, o.f.store, o.f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref("root.11"), Kind: "bead", Op: model.OpDelete, Graph: &conformance.FixtureGraphSource{}}}); err != nil {
				t.Fatal(err)
			}
			// Direct storage fixtures intentionally invalidate the semantic graph
			// certificate. A genuine ordinary reparent validates that seeded graph
			// before this test refuses the general Query correctness fallback.
			parent := ref("root")
			o.commit("certify-seeded-graph", "tx.apply", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: ref("root.009"), Parent: &parent}}}}})
			r, err := o.f.store.Read(o.f.ctx, driver.View{Incarnation: o.f.metaSnapshot().Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			states, err := r.Observe(o.f.ctx, []driver.LockKey{pipeline.GraphValidityLockKey(), driver.GraphBlockingSentinelLock})
			r.Close()
			if err != nil || len(states) != 2 || !states[0].Present || !states[1].Present || states[0].Revision != states[1].Revision {
				t.Fatalf("seeded ordinary graph is not certified: %+v err=%v", states, err)
			}
			probe := &queryProbe{}
			s := noQueryStore{Store: o.f.store, ResponseFinalizer: o.f.store.(driver.ResponseFinalizer), CheckpointStore: o.f.store.(driver.CheckpointStore), probe: probe}
			o.server.Close()
			o.f.writer = pipeline.New(s, o.f.lease, pipeline.WithClock(o.f.clock.Now), pipeline.WithOntology(o.f.ontology))
			o.f.svc = service.New(s, o.f.lease, o.f.writer, service.WithClock(o.f.clock.Now), service.WithOntology(o.f.ontology))
			o.diagnosticHTTP = func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
					next.ServeHTTP(rw, r.WithContext(context.WithValue(r.Context(), requestProofKey{}, probe)))
				})
			}
			o.install()
			ops := []model.Op{ordinaryCreateOp("first", project, map[string]any{"title": "first", "priority": 4}, "root"), ordinaryCreateOp("second", project, map[string]any{"title": "second", "priority": 4}, "root")}
			raw := o.call("consume", "tx.apply", owner, map[string]any{"changeSet": model.ChangeSet{Ops: ops}})
			probe.mu.Lock()
			queryCalls, identityCalls := probe.queryCalls, probe.identityCalls
			contextMissing, identityError := probe.contextMissing, probe.identityError
			probe.mu.Unlock()
			if queryCalls != 0 || identityCalls != 2 || contextMissing || identityError != "" {
				t.Fatalf("real create query=%d identity=%d contextMissing=%t identityError=%s response=%s", queryCalls, identityCalls, contextMissing, identityError, raw)
			}
			commit := o.commitResponse("consume", "tx.apply", raw)
			if len(commit.Results) != 2 || commit.Results[0].Ref.ID != "root.12" || commit.Results[1].Ref.ID != "root.13" {
				t.Fatalf("stale counter/occupancy/staged-delete numbering changed: %+v", commit)
			}
			t.Logf("REAL_CHILD_CAPABILITY engine=%s calls=%d generalQuery=%d requestContext=true", backend, identityCalls, queryCalls)
			// Deletion retains its existing general graph-validation path. Check
			// its number consumption separately from the create-only Query refusal.
			o.server.Close()
			o.diagnosticHTTP = nil
			o.f.install()
			o.install()
			staged := []model.Op{ordinaryCreateOp("deleted", project, map[string]any{"title": "deleted", "priority": 4}, "root"), {Kind: model.OpDelete, Delete: &model.DeleteOp{Target: model.Ref{Key: "deleted"}}}, ordinaryCreateOp("survivor", project, map[string]any{"title": "survivor", "priority": 4}, "root")}
			raw = o.call("staged-delete", "tx.apply", owner, map[string]any{"changeSet": model.ChangeSet{Ops: staged}})
			commit = o.commitResponse("staged-delete", "tx.apply", raw)
			if len(commit.Results) != 3 || commit.Results[0].Ref.ID != "root.14" || commit.Results[1].Ref.ID != "root.14" || commit.Results[2].Ref.ID != "root.15" {
				t.Fatalf("create-delete number consumption changed: %+v", commit)
			}
		})
	}
}
