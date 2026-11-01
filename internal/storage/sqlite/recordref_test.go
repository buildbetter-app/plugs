package sqlite

import (
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestRecordRefUsesCanonicalSortedAcquiredSubset(t *testing.T) {
	// This is the shape returned by Lock: one globally encoded-key-sorted
	// subset, with skipped requests omitted but every retained mode preserved.
	tombstone := model.RefByID("pl-record-tombstone", model.Natural64(0))
	alias := model.RefByKey("legacy-record")
	recordA := model.RefByID("pl-record-a", model.Natural64(0))
	recordSkipped := model.RefByID("pl-record-skipped", model.Natural64(0))
	recordC := model.RefByID("pl-record-c", model.Natural64(7))
	keys := []driver.LockRequest{
		{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &recordA}, Mode: driver.LockShare},
		{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &alias}, Mode: driver.LockUpdate},
		{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &recordSkipped}, Mode: driver.LockSkipLocked},
		{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &recordC}, Mode: driver.LockUpdate},
		{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &tombstone}, Mode: driver.LockSkipLocked},
		{Key: driver.LockKey{Kind: driver.LockReadFact, Fact: "record-ref/fact"}, Mode: driver.LockShare},
		{Key: driver.LockKey{Kind: driver.LockSentinel, Sentinel: driver.SentinelGraphBlocking}, Mode: driver.LockUpdate},
		{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: "pl-record-a"}}, Mode: driver.LockSkipLocked},
	}
	sort.Slice(keys, func(i, j int) bool {
		left, _ := driver.EncodeLockKey(keys[i].Key)
		right, _ := driver.EncodeLockKey(keys[j].Key)
		return left < right
	})
	if err := driver.ValidateLockSet(keys); err != nil {
		t.Fatalf("full mixed lock set is not canonical: %v", err)
	}

	// The skipped record is absent from the selected subset. Filtering a valid
	// lock set must retain its encoded ordering even with mixed key families.
	selected := make(driver.LockSet, 0, len(keys)-1)
	for _, request := range keys {
		if request.Key.Ref != nil && request.Key.Ref.IsID() && request.Key.Ref.ID == recordSkipped.ID {
			continue
		}
		selected = append(selected, request)
	}
	if err := driver.ValidateLockSet(selected); err != nil {
		t.Fatalf("selected mixed lock subset is not canonical: %v", err)
	}

	// A staged tombstone has no record value, but recordRef must still return
	// the exact acquired reference. The lookup is identity-only.
	transaction := &transaction{
		acquired: selected,
		records:  map[string]*driver.Record{keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &tombstone}): nil},
	}
	for _, ref := range []model.Ref{recordA, alias, recordC, tombstone} {
		key := driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
		got := transaction.recordRef(keyID(key))
		if !reflect.DeepEqual(got, ref) {
			t.Fatalf("recordRef(%+v) = %+v; want exact acquired ref", ref, got)
		}
	}

	assertRecordRefPanics(t, transaction, keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &recordSkipped}))
	missing := model.RefByID("pl-record-missing", model.Natural64(0))
	assertRecordRefPanics(t, transaction, keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &missing}))
	// Non-record keys deliberately have nil Ref. An exact non-record identity
	// must not be mistaken for a record reference.
	fact := driver.LockKey{Kind: driver.LockReadFact, Fact: "record-ref/fact"}
	assertRecordRefPanics(t, transaction, keyID(fact))
}

func assertRecordRefPanics(t *testing.T, transaction *transaction, id string) {
	t.Helper()
	defer func() {
		if got := recover(); got != "sqlite: record missing validated lock" {
			t.Fatalf("recordRef(%q) panic = %v; want missing validated lock", id, got)
		}
	}()
	transaction.recordRef(id)
}
