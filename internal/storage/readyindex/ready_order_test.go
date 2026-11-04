package readyindex_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/readyindex"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

func sourceMutation(id, project string, priority int64) readyindex.Mutation {
	item := driver.ReadyMembershipItem{ID: id, Type: "issue", Project: project, Status: "open", Revision: model.Natural64(0), Fields: map[string]any{"priority": json.Number(strconv.FormatInt(priority, 10)), "status": "open", "title": id}}
	return readyindex.Mutation{Ref: model.RefByID(id, model.Natural64(0)), Item: &item, Priority: priority, Project: project, Type: "issue", Binding: "ontology", Eligible: true, Local: true}
}
func maintain(t *testing.T, s driver.Store, lease driver.ActiveLease, mutations []readyindex.Mutation, rollback bool) (driver.ReadyOrderWork, model.Revision) {
	t.Helper()
	ctx := context.Background()
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	delta, err := readyindex.Prepare(ctx, r, mutations)
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]driver.LockKey{}
	for _, key := range delta.Locks() {
		k, _ := driver.EncodeLockKey(key)
		keys[k] = key
	}
	changes := []model.ChangeRecord{}
	for _, m := range mutations {
		key := driver.LockKey{Kind: driver.LockRecord, Ref: &m.Ref}
		k, _ := driver.EncodeLockKey(key)
		keys[k] = key
		prior, e := r.Get(ctx, []model.Ref{m.Ref})
		if e != nil {
			t.Fatal(e)
		}
		op := model.OpCreate
		if len(prior) > 0 {
			op = model.OpPatch
		}
		if m.Item == nil {
			op = model.OpDelete
		}
		changes = append(changes, model.ChangeRecord{Ref: m.Ref, Op: op})
	}
	ordered := make([]driver.LockKey, 0, len(keys))
	for _, key := range keys {
		ordered = append(ordered, key)
	}
	sort.Slice(ordered, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(ordered[i])
		b, _ := driver.EncodeLockKey(ordered[j])
		return a < b
	})
	locks := make(driver.LockSet, len(ordered))
	for i, key := range ordered {
		locks[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	tx, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	defer tx.Rollback(ctx)
	if _, err = tx.Lock(ctx, locks); err != nil {
		t.Fatal(err)
	}
	head, err := tx.Head(ctx)
	if err != nil {
		t.Fatal(err)
	}
	n, _ := head.Uint64()
	revision := model.Natural64(n + 1)
	for _, m := range mutations {
		if m.Item == nil {
			err = tx.Delete(ctx, m.Ref)
		} else {
			body, e := json.Marshal(map[string]any{"id": m.Ref.ID, "type": m.Type, "project": m.Project, "fields": m.Item.Fields})
			if e != nil {
				t.Fatal(e)
			}
			err = tx.Put(ctx, driver.Record{Ref: m.Ref, Kind: "bead", Body: body})
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	work, err := delta.Publish(ctx, tx, revision)
	if err != nil {
		t.Fatal(err)
	}
	if rollback {
		if err = tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		return work, revision
	}
	_, err = tx.Commit(ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "system", ID: "ready-regression"}, LockKeys: ordered, Changes: changes})
	if err != nil {
		t.Fatal(err)
	}
	return work, revision
}
func selectOrder(t *testing.T, s driver.Store, modify func(*driver.ReadyOrderPlan)) (driver.ReadyOrderResult, error) {
	t.Helper()
	ctx := context.Background()
	m, e := s.Meta(ctx)
	if e != nil {
		t.Fatal(e)
	}
	r, e := s.Read(ctx, driver.View{Incarnation: m.Incarnation})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	p := driver.ReadyOrderPlan{Incarnation: m.Incarnation, ContentRevision: m.Head, AuthorityRevision: m.Head, Eligibility: "ontology", UniformAuthorization: true, Limit: 1000}
	if modify != nil {
		modify(&p)
	}
	selector, ok := r.(driver.ReadyOrderReader)
	if !ok {
		t.Fatal("missing Ready capability")
	}
	return selector.SelectReadyOrder(ctx, p)
}
func TestReadyOrderRealStoresMaintenanceAndSemantics(t *testing.T) {
	factories := map[string]func(*testing.T) driver.Store{
		"memory": func(*testing.T) driver.Store { return memory.New() },
		"sqlite": func(t *testing.T) driver.Store {
			s, e := (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: filepath.Join(t.TempDir(), "ready.db")})
			if e != nil {
				t.Fatal(e)
			}
			return s
		},
	}
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer s.Close()
			lease, err := s.Lease(context.Background(), "ready-regression", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Release(context.Background())
			stop := make(chan struct{})
			defer close(stop)
			go func() {
				ticker := time.NewTicker(driver.LeaseRenewEvery)
				defer ticker.Stop()
				for {
					select {
					case <-stop:
						return
					case <-ticker.C:
						if lease.Renew(context.Background()) != nil {
							return
						}
					}
				}
			}()
			mutations := make([]readyindex.Mutation, 1000)
			for i := range mutations {
				mutations[i] = sourceMutation(fmt.Sprintf("item-%04d", i), "visible", int64(i%5))
			}
			work, revision := maintain(t, s, lease, mutations, false)
			t.Logf("READY_WRITE sources=%d nodes=%d bytes=%d", len(mutations), work.WrittenNodes, work.WrittenBytes)
			all, err := selectOrder(t, s, nil)
			if err != nil {
				t.Fatal(err)
			}
			if all.Total != 1000 || all.More || len(all.Keys) != 1000 {
				t.Fatalf("incomplete page: %+v", all)
			}
			leaves := make([]driver.ReadyOrderLeaf, len(mutations))
			for i, m := range mutations {
				item := *m.Item
				item.Revision = revision
				leaves[i], err = driver.ReadyOrderLeafFor(item, m.Priority)
				if err != nil {
					t.Fatal(err)
				}
			}
			want, err := driver.ReadyCommitmentFromLeaves(leaves)
			if err != nil || all.Commitment != want {
				t.Fatalf("maintained/fallback disagreement: got=%+v want=%+v err=%v", all.Commitment, want, err)
			}
			for i, key := range all.Keys {
				if key.Revision.String() != revision.String() {
					t.Fatal("derived revision substituted for source")
				}
				if i > 0 {
					a, b := all.Keys[i-1], key
					if *a.Order[0].Int > *b.Order[0].Int || *a.Order[0].Int == *b.Order[0].Int && a.Ref.ID >= b.Ref.ID {
						t.Fatal("numeric priority/bytewise order")
					}
				}
			}
			refs := []model.Ref{}
			for i := 0; i < 100; i++ {
				refs = append(refs, mutations[i*10].Ref)
			}
			getSparse := func() (driver.ReadyOrderResult, error) {
				return selectOrder(t, s, func(p *driver.ReadyOrderPlan) { p.UniformAuthorization = false; p.AuthorizedRefs = refs })
			}
			sparse, err := getSparse()
			if err != nil || sparse.Total != 100 || sparse.Work.Leaves > 100 {
				t.Fatalf("sparse bound: result=%+v err=%v", sparse, err)
			}
			hidden := mutations[999]
			hidden.Item.Fields["title"] = "hidden tail"
			maintain(t, s, lease, []readyindex.Mutation{hidden}, false)
			after, err := getSparse()
			if err != nil || after.Commitment != sparse.Commitment {
				t.Fatal("hidden change invalidated authorized commitment", err)
			}
			authorized := mutations[990]
			authorized.Item.Fields["title"] = "authorized tail"
			maintain(t, s, lease, []readyindex.Mutation{authorized}, false)
			tail, err := getSparse()
			if err != nil || tail.Commitment == sparse.Commitment {
				t.Fatal("authorized tail was omitted", err)
			}
			beforeRollback := tail.Commitment
			authorized.Item.Fields["title"] = "rolled back"
			maintain(t, s, lease, []readyindex.Mutation{authorized}, true)
			tail, err = getSparse()
			if err != nil || tail.Commitment != beforeRollback {
				t.Fatal("rollback leaked", err)
			}
			// A source lease, pending/nonlocal predicate or backlog ancestor cannot
			// be hidden behind an old ready bit. Quiet checkpoint updates do not
			// re-enable the unavailable source domain.
			unsafe := mutations[0]
			unsafe.Local = false
			maintain(t, s, lease, []readyindex.Mutation{unsafe}, false)
			if _, err = selectOrder(t, s, nil); !errors.Is(err, driver.ErrUnsupported) {
				t.Fatal("unsafe source accelerated", err)
			}
			if _, err = s.(driver.CheckpointStore).SaveCheckpoint(context.Background(), lease, "ready-pending", 0, []byte("heartbeat overlay")); err != nil {
				t.Fatal(err)
			}
			if _, err = selectOrder(t, s, nil); !errors.Is(err, driver.ErrUnsupported) {
				t.Fatal("heartbeat restored coverage", err)
			}
			unsafe.Local = true
			maintain(t, s, lease, []readyindex.Mutation{unsafe}, false)
			if _, err = selectOrder(t, s, nil); err != nil {
				t.Fatal(err)
			}
			if _, err = selectOrder(t, s, func(p *driver.ReadyOrderPlan) { p.Eligibility = "refreshed ontology" }); !errors.Is(err, driver.ErrUnsupported) {
				t.Fatal("ontology refresh accepted", err)
			}
			for _, seed := range []string{"request-a", "request-b", "request-c"} {
				got, err := selectOrder(t, s, func(p *driver.ReadyOrderPlan) { p.UniformAuthorization = false; p.AuthorizedRefs = refs; p.Seed = seed })
				if err != nil {
					t.Fatal(err)
				}
				// All selected refs are P0; raw group cardinality is 200, not 100.
				h := sha256.Sum256([]byte(seed + "\x000"))
				pivot := int(binary.BigEndian.Uint64(h[:8]) % 200)
				ordered := []string{}
				for step := 0; step < 200; step++ {
					id := fmt.Sprintf("item-%04d", ((pivot+step)%200)*5)
					for _, ref := range refs {
						if id == ref.ID {
							ordered = append(ordered, id)
						}
					}
				}
				if len(got.Keys) != len(ordered) {
					t.Fatal("seed membership")
				}
				for i, key := range got.Keys {
					if key.Ref.ID != ordered[i] {
						t.Fatalf("raw rank differs at %d: %s/%s", i, key.Ref.ID, ordered[i])
					}
				}
			}
			empty, err := selectOrder(t, s, func(p *driver.ReadyOrderPlan) { p.RawRefs = []model.Ref{} })
			if err != nil || empty.Total != 0 {
				t.Fatal("present-empty raw refs", err)
			}
			// A genuine unmaintained publication creates an explicit gap. A later
			// one-source refresh must not infer coverage of that missing interval.
			tx, err := s.Write(context.Background(), lease)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = tx.Lock(context.Background(), nil); err != nil {
				t.Fatal(err)
			}
			if _, err = tx.Head(context.Background()); err != nil {
				t.Fatal(err)
			}
			meta, _ := s.Meta(context.Background())
			if _, err = tx.Commit(context.Background(), driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "system", ID: "unmaintained"}}); err != nil {
				t.Fatal(err)
			}
			tx.Close()
			if _, err = selectOrder(t, s, nil); !errors.Is(err, driver.ErrUnsupported) {
				t.Fatal("gap certified", err)
			}
			maintain(t, s, lease, []readyindex.Mutation{mutations[1]}, false)
			if _, err = selectOrder(t, s, nil); !errors.Is(err, driver.ErrUnsupported) {
				t.Fatal("gap recertified from final images", err)
			}
		})
	}
}
