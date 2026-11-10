package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestAcquiredCensusKeepsWriteModesAndOwnedKeys(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "acquired.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	busy := driver.LockKey{Kind: driver.LockReadFact, Fact: "census/busy"}
	shared := driver.LockKey{Kind: driver.LockReadFact, Fact: "census/share"}
	update := driver.LockKey{Kind: driver.LockReadFact, Fact: "census/update"}
	selected := driver.LockKey{Kind: driver.LockReadFact, Fact: "census/selected"}
	ref := model.RefByID("census-\xff", model.Natural64(0))
	opaque := driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
	originalRef := ref
	originalOpaque := driver.LockKey{Kind: driver.LockRecord, Ref: &originalRef}
	neighborRef := model.RefByID("census-\xfe", model.Natural64(0))
	neighbor := driver.LockKey{Kind: driver.LockRecord, Ref: &neighborRef}
	rowKey := driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: "census/source"}
	requests := driver.LockSet{
		{Key: busy, Mode: driver.LockSkipLocked}, {Key: shared, Mode: driver.LockShare},
		{Key: update, Mode: driver.LockUpdate}, {Key: selected, Mode: driver.LockSkipLocked},
		{Key: opaque, Mode: driver.LockUpdate}, {Key: driver.LockKey{Kind: driver.LockDerived, Derived: &rowKey}, Mode: driver.LockUpdate},
	}
	// Select an independent busy stripe, so this probe tests a skipped key
	// alongside successful locks rather than an incidental stripe collision.
	for n := 0; ; n++ {
		if n == guardStripeCount {
			t.Fatal("could not select an independent busy stripe")
		}
		collision := false
		for _, request := range requests[1:] {
			collision = collision || guardStripe(busy) == guardStripe(request.Key)
		}
		if !collision {
			break
		}
		busy.Fact += "x"
		requests[0].Key = busy
	}
	holder, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	if _, err = holder.Lock(ctx, driver.LockSet{{Key: busy, Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	sort.Slice(requests, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(requests[i].Key)
		b, _ := driver.EncodeLockKey(requests[j].Key)
		return a < b
	})
	write, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer write.Close()
	states, err := write.Lock(ctx, requests)
	if err != nil || len(states) != len(requests)-1 {
		t.Fatalf("actual skip-locked subset=%d: %v", len(states), err)
	}
	if _, err = write.Head(ctx); err != nil {
		t.Fatal(err)
	}
	tx := write.(*transaction)
	value := []byte(`{"facts":[]}`)
	row := driver.DerivedRow{Key: rowKey.Key, Value: value, Index: &driver.SelectionIndex{
		Version: driver.SelectionIndexVersion, Row: rowKey, Incarnation: write.Snapshot().Incarnation,
		Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(value),
		Source: driver.SelectionSource{Key: shared, Revision: model.Natural64(0)},
		Facts:  &driver.SelectionFacts{Kind: driver.SelectionLabelFact, Rows: []driver.SelectionFact{}},
	}}
	if err = write.(driver.SelectionIndexWriter).PutSelectionDerived(ctx, rowKey.Table, nil, []driver.DerivedRow{row}); err != nil {
		t.Fatalf("shared source membership refused: %v", err)
	}
	for _, test := range []struct {
		name string
		key  driver.LockKey
		want bool
	}{
		{"share", shared, false}, {"update", update, true}, {"selected_skip", selected, true},
		{"omitted_skip", busy, false}, {"opaque_owned", opaque, true},
		{"opaque_neighbor", neighbor, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := tx.writable(test.key)
			if (err == nil) != test.want || err != nil && !strings.Contains(err.Error(), "missing update lock") {
				t.Fatalf("write ownership=%v want=%v: %v", err == nil, test.want, err)
			}
		})
	}
	for i := range requests {
		requests[i].Mode = driver.LockUpdate
		if requests[i].Key.Ref != nil {
			requests[i].Key.Ref.ID = "changed-request"
		}
	}
	for i := range states {
		if states[i].Key.Ref != nil {
			states[i].Key.Ref.ID = "changed-result"
		}
	}
	if err = tx.writable(originalOpaque); err != nil {
		t.Fatalf("caller mutation lost original ownership: %v", err)
	}
	for _, id := range []string{"changed-request", "changed-result"} {
		forged := model.RefByID(id, model.Natural64(0))
		if err = tx.writable(driver.LockKey{Kind: driver.LockRecord, Ref: &forged}); err == nil {
			t.Fatalf("caller mutation granted ownership of %q", id)
		}
	}
	if err = write.BumpReadFacts(ctx, []driver.ReadFact{shared.Fact}); err == nil {
		t.Fatal("warm shared membership authorized a predicate mutation")
	}
	if err = write.BumpReadFacts(ctx, []driver.ReadFact{update.Fact, selected.Fact}); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err = write.BumpReadFacts(cancelled, []driver.ReadFact{update.Fact}); !errors.Is(err, context.Canceled) {
		t.Fatalf("warm ownership bypassed cancellation: %v", err)
	}
	if err = write.Close(); err != nil || tx.acquiredIndex != nil {
		t.Fatalf("rollback retained acquired census: %v", err)
	}
}
