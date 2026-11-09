package authzrpc_test

import (
	"context"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type claimEligibilityStore struct {
	driver.Store
	writes atomic.Int32
}

func (s *claimEligibilityStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	s.writes.Add(1)
	return s.Store.Write(ctx, lease)
}

// This uses the actual installed fixed policy. Modify is permitted for the
// sponsored Agent; its absent distinct Transition grant must remain denied.
// The denial yields one durable null instead of an unchanged ready-set loop.
func TestClaimNextTransitionDeniedCandidateProducesOneNullReceipt(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			var open func() (driver.Store, error)
			switch engine {
			case "memory":
				open = func() (driver.Store, error) { return memory.New(), nil }
			case "sqlite":
				dsn := filepath.Join(t.TempDir(), "eligibility.db")
				open = func() (driver.Store, error) { return (sqlite.Driver{}).Open(ctx, driver.Config{DSN: dsn}) }
			case "postgres":
				dsn := postgresDSNFactory(t)(t)
				open = func() (driver.Store, error) { return postgres.Open(ctx, driver.Config{DSN: dsn}) }
			}
			base, err := open()
			if err != nil {
				t.Fatal(err)
			}
			counted := &claimEligibilityStore{Store: base}
			f := newFixtureOn(t, counted, nil)
			by := worker("user:operator")
			ref := model.RefByID(beadID, model.Natural64(0))
			read := func() (driver.Meta, []driver.Record, []driver.Version) {
				t.Helper()
				meta, err := f.store.Meta(ctx)
				if err != nil {
					t.Fatal(err)
				}
				r, err := f.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				rows, err := r.Get(ctx, []model.Ref{ref})
				if err != nil || len(rows) != 1 {
					t.Fatalf("current candidate: rows=%d err=%v", len(rows), err)
				}
				versions, err := r.Versions(ctx, ref, driver.PageReq{Limit: 1000})
				if err != nil || versions.NextCursor != nil {
					t.Fatalf("complete candidate history: %v", err)
				}
				return meta, rows, versions.Items
			}
			before, rows, versions := read()
			writes := counted.writes.Load()
			params := map[string]any{}
			got := f.call(t, by, "bead.claimNext", params, 71001)
			if got.Error != nil || string(got.Result) != "null" || got.Meta.Replayed || counted.writes.Load()-writes != 1 {
				t.Fatalf("denied ready candidate did not terminate with one null receipt: error=%+v result=%s writes=%d", got.Error, got.Result, counted.writes.Load()-writes)
			}
			after, afterRows, afterVersions := read()
			beforeHead, _ := before.Head.Uint64()
			afterHead, _ := after.Head.Uint64()
			if afterHead != beforeHead+1 || !reflect.DeepEqual(rows, afterRows) || !reflect.DeepEqual(versions, afterVersions) {
				t.Fatal("terminal null changed the denied candidate or failed to publish exactly H+1")
			}
			if engine != "memory" {
				if err := f.lease.Release(ctx); err != nil {
					t.Fatal(err)
				}
				if err := f.store.Close(); err != nil {
					t.Fatal(err)
				}
				base, err = open()
				if err != nil {
					t.Fatal(err)
				}
				counted = &claimEligibilityStore{Store: base}
				f.store = counted
				f.lease, err = f.store.Lease(ctx, "claim-eligibility-reopen", driver.LeaseTTL)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = f.lease.Release(ctx) })
				f.server, err = authzrpc.New(ctx, authzrpc.Config{Store: f.store, Lease: f.lease, Provider: f.keys, Facts: f.facts, Namespace: f.base.Namespace, DefaultProject: "project"})
				if err != nil {
					t.Fatal(err)
				}
			}
			writes = counted.writes.Load()
			replay := f.call(t, by, "bead.claimNext", params, 71001)
			if replay.Error != nil || string(replay.Result) != "null" || !replay.Meta.Replayed || counted.writes.Load() != writes {
				t.Fatalf("terminal null did not replay without writes after reopen: %+v", replay)
			}
			replayed, replayRows, replayVersions := read()
			if replayed.Head.String() != after.Head.String() || !reflect.DeepEqual(afterRows, replayRows) || !reflect.DeepEqual(afterVersions, replayVersions) {
				t.Fatal("replay mutated the candidate or history")
			}
			// Current membership still fences the receipt. A revoked sponsor
			// cannot use the prior null as a bypass of installed authority.
			revoked := f.base
			revoked.Grants = nil
			if err := f.facts.Replace(revoked); err != nil {
				t.Fatal(err)
			}
			beforeDenied, denyRows, denyVersions := read()
			denied := f.call(t, by, "bead.claimNext", params, 71001)
			if denied.Error == nil || denied.Error.Data.Code != "forbidden" {
				t.Fatalf("revoked terminal receipt was admitted: %+v", denied)
			}
			afterDenied, finalRows, finalVersions := read()
			if beforeDenied.Head.String() != afterDenied.Head.String() || !reflect.DeepEqual(denyRows, finalRows) || !reflect.DeepEqual(denyVersions, finalVersions) {
				t.Fatal("revoked replay published state")
			}
		})
	}
}
