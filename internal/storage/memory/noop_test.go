package memory

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestNoopRejectsMutationsAndInvalidGuards(t *testing.T) {
	ctx := context.Background()
	ref := model.RefByID("pl-staged", model.Natural64(0))
	sentinel, _ := driver.NewBeadSentinel(ref.ID)
	fact, _ := driver.NewReadFact("query/fixture")
	row := driver.DerivedRowKey{Table: driver.DerivedReadyState, Key: "fixture"}
	keys := []driver.LockKey{{Kind: driver.LockRecord, Ref: &ref}, {Kind: driver.LockSentinel, Sentinel: sentinel}, {Kind: driver.LockReadFact, Fact: fact}, {Kind: driver.LockDerived, Derived: &row}}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	requests := make([]driver.LockRequest, len(keys))
	for i, key := range keys {
		requests[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	tests := []struct {
		name  string
		stage func(driver.WriteTx, *driver.ChangesetHeader) error
	}{
		{"record", func(tx driver.WriteTx, _ *driver.ChangesetHeader) error {
			return tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{}`)})
		}},
		{"tombstone", func(tx driver.WriteTx, _ *driver.ChangesetHeader) error {
			if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{}`)}); err != nil {
				return err
			}
			return tx.Delete(ctx, ref)
		}},
		{"blob", func(tx driver.WriteTx, _ *driver.ChangesetHeader) error {
			_, err := tx.PutBlob(ctx, []byte("staged"))
			return err
		}},
		{"derived", func(tx driver.WriteTx, _ *driver.ChangesetHeader) error {
			return tx.PutDerived(ctx, row.Table, nil, []driver.DerivedRow{{Key: row.Key, Value: []byte("staged")}})
		}},
		{"sentinel", func(tx driver.WriteTx, _ *driver.ChangesetHeader) error {
			return tx.BumpSentinels(ctx, []driver.Sentinel{sentinel})
		}},
		{"read_fact", func(tx driver.WriteTx, _ *driver.ChangesetHeader) error {
			return tx.BumpReadFacts(ctx, []driver.ReadFact{fact})
		}},
		{"header_changes", func(_ driver.WriteTx, h *driver.ChangesetHeader) error {
			h.Changes = []model.ChangeRecord{{Ref: ref, Op: model.OpPatch}}
			return nil
		}},
		{"incomplete_locks", func(_ driver.WriteTx, h *driver.ChangesetHeader) error { h.LockKeys = h.LockKeys[:1]; return nil }},
		{"unsorted_locks", func(_ driver.WriteTx, h *driver.ChangesetHeader) error {
			h.LockKeys[0], h.LockKeys[1] = h.LockKeys[1], h.LockKeys[0]
			return nil
		}},
		{"invalid_key", func(_ driver.WriteTx, h *driver.ChangesetHeader) error { h.IdempotencyKey = " invalid "; return nil }},
		{"missing_method", func(_ driver.WriteTx, h *driver.ChangesetHeader) error { h.Method = ""; return nil }},
		{"missing_hash", func(_ driver.WriteTx, h *driver.ChangesetHeader) error { h.RequestHash = ""; return nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := New()
			defer s.Close()
			lease, err := s.Lease(ctx, "test", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			meta, _ := s.Meta(ctx)
			tx, err := s.Write(ctx, lease)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Close()
			if _, err := tx.Lock(ctx, requests); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Head(ctx); err != nil {
				t.Fatal(err)
			}
			h := driver.ChangesetHeader{Kind: model.ChangesetWrite, Incarnation: meta.Incarnation, Principal: model.PrincipalRef{Kind: "system", ID: "test"}, Method: "bead.update", IdempotencyKey: "key", RequestHash: "digest", LockKeys: append([]driver.LockKey(nil), keys...)}
			if err := test.stage(tx, &h); err != nil {
				t.Fatal(err)
			}
			before := s.state
			if _, err := tx.(driver.NoopCommitter).CommitNoop(ctx, h); err == nil {
				t.Fatal("invalid no-op succeeded")
			}
			if s.state != before || !s.state.meta.Head.IsZero() || len(s.state.replays) != 0 || len(s.state.log) != 0 {
				t.Fatal("failed no-op published state")
			}
		})
	}
}

func TestNoopZeroHeadIsolationAndSharedOrdinaryScope(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease, err := s.Lease(ctx, "test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	meta, _ := s.Meta(ctx)
	h := driver.ChangesetHeader{Kind: model.ChangesetWrite, Incarnation: meta.Incarnation, Principal: model.PrincipalRef{Kind: "system", ID: "test"}, Method: "bead.update", IdempotencyKey: "key", RequestHash: "digest"}
	tx, _ := s.Write(ctx, lease)
	defer tx.Close()
	if _, err := tx.(driver.NoopCommitter).CommitNoop(ctx, h); err == nil {
		t.Fatal("no-op without locks/Head succeeded")
	}
	if _, err := tx.Lock(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	h.Response.Warnings = []model.Warning{{Code: "subscriber_skipped", Details: map[string]any{"binding": "original"}}}
	response, err := tx.(driver.NoopCommitter).CommitNoop(ctx, h)
	if err != nil || !response.Revision.IsZero() || response.Results == nil {
		t.Fatalf("zero-head receipt=%+v %v", response, err)
	}
	h.Response.Warnings[0].Details["binding"] = "input alias"
	response.Warnings[0].Details["binding"] = "output alias"
	r, _ := s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	defer r.Close()
	cached, err := r.IdempotencyByKey(ctx, "system:test", "", h.Method, h.IdempotencyKey)
	if err != nil || cached == nil || cached.Commit.Warnings[0].Details["binding"] != "original" {
		t.Fatalf("receipt alias=%+v %v", cached, err)
	}
	cached.Commit.Warnings[0].Details["binding"] = "lookup alias"
	cached, _ = r.IdempotencyByKey(ctx, "system:test", "", h.Method, h.IdempotencyKey)
	if cached.Commit.Warnings[0].Details["binding"] != "original" {
		t.Fatal("lookup response alias leaked")
	}
	// An ordinary Commit cannot fabricate a changeset for this no-op receipt.
	if _, err := conformance.Seed(ctx, s, lease, h, nil); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("ordinary commit against no-op=%v", err)
	}
	ordinary := h
	ordinary.IdempotencyKey = "ordinary"
	entry, err := conformance.Seed(ctx, s, lease, ordinary, nil)
	if err != nil {
		t.Fatal(err)
	}
	read, _ := s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	defer read.Close()
	previous, _ := read.IdempotencyByKey(ctx, "system:test", "", ordinary.Method, ordinary.IdempotencyKey)
	retry, _ := s.Write(ctx, lease)
	defer retry.Close()
	if _, err := retry.Lock(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := retry.Head(ctx); err != nil {
		t.Fatal(err)
	}
	replay, err := retry.(driver.NoopCommitter).CommitNoop(ctx, ordinary)
	if err != nil || !reflect.DeepEqual(replay, previous.Commit) {
		t.Fatalf("ordinary response replay=%+v %v", replay, err)
	}
	if current, _ := s.Meta(ctx); current.Head.String() != entry.Revision.String() {
		t.Fatal("ordinary replay advanced head")
	}
}
