package authzrpc_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// Count the actual product's complete graph enumerations without supplying
// records or changing the real Reader, finalizer or allocator capabilities.
type readyScanStore struct {
	driver.Store
	driver.ResponseFinalizer
	driver.CheckpointStore
	queries, rows, selections atomic.Int64
}

type readyScanReader struct {
	driver.Reader
	driver.AllocatorReader
	store *readyScanStore
}

func (s *readyScanStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return &readyScanReader{Reader: r, AllocatorReader: r.(driver.AllocatorReader), store: s}, nil
}

func (r *readyScanReader) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	if q.ReadSelection != nil {
		r.store.selections.Add(1)
		return r.Reader.Query(ctx, q)
	}
	r.store.queries.Add(1)
	page, err := r.Reader.Query(ctx, q)
	r.store.rows.Add(int64(len(page.Items)))
	return page, err
}

func renewReadyScanLease(t *testing.T, lease driver.ActiveLease) {
	t.Helper()
	stop, done := make(chan struct{}), make(chan struct{})
	renewError := make(chan error, 1)
	go func() {
		defer close(done)
		ticker := time.NewTicker(driver.LeaseRenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if err := lease.Renew(context.Background()); err != nil {
					renewError <- err
					return
				}
			}
		}
	}()
	t.Cleanup(func() {
		close(stop)
		<-done
		select {
		case err := <-renewError:
			t.Errorf("ready fixture lease renewal: %v", err)
		default:
		}
	})
}

func readyScanChanges(t *testing.T, first, last int) []conformance.FixtureChange {
	t.Helper()
	changes := make([]conformance.FixtureChange, 0, last-first+1)
	for n := first; n <= last; n++ {
		ref := fixtureRef(n)
		doc := map[string]any{"id": ref.ID, "type": "issue", "project": "project", "fields": map[string]any{"title": "ready", "status": "todo", "priority": 0}}
		if n == 2001 {
			doc["parent"] = fixtureRef(1)
		}
		body, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		changes = append(changes, conformance.FixtureChange{Ref: ref, Kind: "bead", Op: model.OpCreate, Body: body})
	}
	return changes
}

func seedReadyScan(t *testing.T, f *fixture, first, last int) {
	t.Helper()
	// Setup is outside request timers. Keep each transaction short enough for
	// the normal active lease to renew, as in the 100k product fixture.
	for start := first; start <= last; start += 1000 {
		if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, readyScanChanges(t, start, min(start+999, last))); err != nil {
			t.Fatal(err)
		}
	}
}

func TestGuardedReadyEnumeratesOneCompleteProductSnapshot(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			var raw driver.Store
			if engine == "sqlite" {
				var err error
				raw, err = (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: filepath.Join(t.TempDir(), "ready-scan.db")})
				if err != nil {
					t.Fatal(err)
				}
			} else {
				raw = postgresFactory(t)(t)
			}
			s := &readyScanStore{Store: raw, ResponseFinalizer: raw.(driver.ResponseFinalizer), CheckpointStore: raw.(driver.CheckpointStore)}
			f := newFixtureOn(t, s, nil)
			renewReadyScanLease(t, f.lease)
			seedReadyScan(t, f, 2, 2001)
			for _, tc := range []struct {
				name string
				by   model.PrincipalRef
				ids  []int
				next bool
			}{
				{"all", model.PrincipalRef{Kind: "user", ID: "user:reader"}, nil, true},
				{"restricted_hidden_parent", model.PrincipalRef{Kind: "user", ID: "user:scoped", Caveats: map[string]any{"refs": []model.Ref{fixtureRef(1000), fixtureRef(2001)}}}, []int{1000, 2001}, false},
				{"agent_cannot_borrow_read", worker("user:operator"), []int{}, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					s.queries.Store(0)
					s.selections.Store(0)
					s.rows.Store(0)
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					started := time.Now()
					page, err := f.server.Service.Ready(ctx, tc.by, service.ReadyRequest{Project: "project", Limit: 100})
					queries, rows := s.queries.Load(), s.rows.Load()
					t.Logf("GUARDED_READY_WORK engine=%s corpus=2001 queries=%d returned_rows=%d visible=%d elapsed=%s", engine, queries, rows, len(page.Items), time.Since(started))
					if err != nil {
						t.Fatal(err)
					}
					want := tc.ids
					if tc.name == "all" {
						want = make([]int, 100)
						for i := range want {
							want[i] = i + 1
						}
					}
					if len(page.Items) != len(want) || (page.Next != nil) != tc.next {
						t.Fatalf("authorized page differs: items=%d next=%v", len(page.Items), page.Next)
					}
					for i, n := range want {
						if page.Items[i].ID != fixtureRef(n).ID {
							t.Fatal("ready membership or ordering changed")
						}
						if n == 2001 && (!page.Items[i].ParentHidden || page.Items[i].Parent != "") {
							t.Fatal("restricted readiness leaked its hidden parent")
						}
					}
					wantSelections := int64(1)
					if tc.name == "agent_cannot_borrow_read" {
						wantSelections = 0
					} // Existing projects caveat retains reference demand.
					if s.selections.Load() != wantSelections {
						t.Errorf("storage selection calls=%d want=%d", s.selections.Load(), wantSelections)
					}
					if queries > 3 || rows > 2001 {
						t.Errorf("one guarded request rebuilt the complete readiness graph: queries=%d rows=%d", queries, rows)
					}
				})
			}
			seedReadyScan(t, f, 2002, 20001)
			for _, by := range []model.PrincipalRef{
				{Kind: "user", ID: "user:reader"},
				{Kind: "user", ID: "user:scoped", Caveats: map[string]any{"refs": []model.Ref{fixtureRef(1000)}}},
			} {
				t.Run("compiled_above_check_only_cap_"+by.ID, func(t *testing.T) {
					before, err := s.Meta(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					s.queries.Store(0)
					s.selections.Store(0)
					s.rows.Store(0)
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					started := time.Now()
					page, err := f.server.Service.Ready(ctx, by, service.ReadyRequest{Project: "project", Limit: 100})
					t.Logf("GUARDED_READY_COMPILED engine=%s corpus=20001 queries=%d returned_rows=%d selections=%d elapsed=%s error=%v", engine, s.queries.Load(), s.rows.Load(), s.selections.Load(), time.Since(started), err)
					if err != nil {
						t.Fatal(err)
					}
					want, next := 100, true
					if by.ID == "user:scoped" {
						want, next = 1, false
					}
					if len(page.Items) != want || (page.Next != nil) != next {
						t.Fatalf("compiled membership/page differs: items=%d next=%v", len(page.Items), page.Next)
					}
					for i, bead := range page.Items {
						n := i + 1
						if by.ID == "user:scoped" {
							n = 1000
						}
						if bead.ID != fixtureRef(n).ID {
							t.Fatal("compiled order/membership changed")
						}
					}
					if s.selections.Load() != 1 || s.queries.Load() > 21 || s.rows.Load() > 20001 {
						t.Fatalf("compiled storage/graph work differs: selections=%d queries=%d rows=%d", s.selections.Load(), s.queries.Load(), s.rows.Load())
					}
					after, err := s.Meta(context.Background())
					if err != nil || after.Head.String() != before.Head.String() {
						t.Fatal("refused Ready changed product state")
					}
				})
			}
		})
	}
}
