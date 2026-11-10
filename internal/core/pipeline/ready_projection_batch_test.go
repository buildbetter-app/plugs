package pipeline

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type readyBatchObservationReader struct {
	driver.Reader
	calls int
}

func (r *readyBatchObservationReader) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.calls++
	states := make([]driver.LockState, len(keys))
	for i, key := range keys {
		states[i] = driver.LockState{Key: key, Revision: model.Natural64(0)}
	}
	return states, nil
}

func TestInitialReadyBatchRetainsIsolatedReadSetAndLockRecheck(t *testing.T) {
	physical := &readyBatchObservationReader{}
	parent := &snapshotReader{reader: physical, reads: &readSet{states: map[string]driver.LockState{}}}
	isolated := &snapshotReader{reader: physical, reads: &readSet{states: map[string]driver.LockState{}}}
	r := readyProjectionReader{parent: parent, isolated: isolated}
	keys := make([]driver.LockKey, 256)
	for i := range keys {
		row := driver.DerivedRowKey{Table: driver.DerivedReadyState, Key: fmt.Sprintf("ready-batch-%03d", i)}
		keys[i] = driver.LockKey{Kind: driver.LockDerived, Derived: &row}
	}
	states, err := r.ObserveInitialReadyOrder(context.Background(), keys)
	if err != nil || physical.calls != 1 || parent.points != len(keys) || len(isolated.reads.states) != len(keys) {
		t.Fatalf("physical batch lost budget or observations: calls=%d points=%d reads=%d: %v", physical.calls, parent.points, len(isolated.reads.states), err)
	}
	if len(parent.reads.states) != 0 {
		t.Fatal("optional reads escaped isolation before the plan was accepted")
	}
	locks, err := isolated.locks(nil, nil)
	if err != nil || len(locks) != len(keys) {
		t.Fatalf("physical batch omitted locks: %v", err)
	}
	for i, request := range locks {
		got, _ := driver.EncodeLockKey(request.Key)
		want, _ := driver.EncodeLockKey(keys[i])
		if got != want || request.Mode != driver.LockShare {
			t.Fatalf("candidate %d missing from the ordinary shared lock census", i)
		}
	}
	if err := isolated.check(states); err != nil {
		t.Fatalf("unchanged batch failed recheck: %v", err)
	}
	states[len(states)-1].Revision = model.Natural64(1)
	if err := isolated.check(states); !errors.Is(err, errStale) {
		t.Fatalf("last physical candidate escaped stale recheck: %v", err)
	}
	if err := isolated.check(states[:len(states)-1]); err == nil {
		t.Fatal("an omitted physical candidate escaped lock recheck")
	}
}

func TestInitialReadyBatchRetainsParentPointBudget(t *testing.T) {
	physical := &readyBatchObservationReader{}
	parent := &snapshotReader{reader: physical, bounded: true}
	isolated := &snapshotReader{reader: physical, reads: &readSet{states: map[string]driver.LockState{}}}
	r := readyProjectionReader{parent: parent, isolated: isolated}
	if _, err := r.ObserveInitialReadyOrder(context.Background(), make([]driver.LockKey, 33)); err == nil || physical.calls != 0 || len(isolated.reads.states) != 0 {
		t.Fatalf("batch bypassed the existing parent point budget: %v", err)
	}
}
