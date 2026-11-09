package readyindex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type initialBatchImage struct {
	states        map[string]driver.LockState
	at            uint64
	scalar, pages [][]driver.LockKey
	alter         func([]driver.LockState) ([]driver.LockState, error)
}

func (r *initialBatchImage) Snapshot() driver.View {
	at := model.Natural64(r.at)
	return driver.View{Incarnation: "initial-batch", Branch: model.Natural64(0), At: &at}
}

func (r *initialBatchImage) observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := driver.ValidateLockKeys(keys); err != nil {
		return nil, err
	}
	states := make([]driver.LockState, len(keys))
	for i, key := range keys {
		states[i] = driver.LockState{Key: key, Revision: model.Natural64(0)}
		if state, ok := r.states[key.Derived.Key]; ok {
			states[i] = state
			if state.Derived != nil {
				row := state.Derived.Clone()
				states[i].Derived = &row
			}
		}
	}
	return states, nil
}

func (r *initialBatchImage) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	r.scalar = append(r.scalar, append([]driver.LockKey(nil), keys...))
	return r.observe(ctx, keys)
}

func (r *initialBatchImage) ObserveInitialReadyOrder(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	r.pages = append(r.pages, append([]driver.LockKey(nil), keys...))
	states, err := r.observe(ctx, keys)
	if err == nil && r.alter != nil {
		return r.alter(states)
	}
	return states, err
}

type initialBatchCapture struct {
	driver.WriteTx
	rows  []driver.DerivedRow
	bumps []driver.Sentinel
}

func (w *initialBatchCapture) PutDerived(_ context.Context, table driver.DerivedTable, remove, add []driver.DerivedRow) error {
	if table != driver.DerivedReadyState || len(remove) != 0 {
		return fmt.Errorf("unexpected initial publication table/removals")
	}
	for _, row := range add {
		w.rows = append(w.rows, row.Clone())
	}
	return nil
}

func (w *initialBatchCapture) BumpSentinels(_ context.Context, sentinels []driver.Sentinel) error {
	w.bumps = append(w.bumps, sentinels...)
	return nil
}

func initialBatchMutations() []Mutation {
	mutations := make([]Mutation, 128)
	created := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	for i := range mutations {
		id, project, kind := fmt.Sprintf("batch-%04d", i), fmt.Sprintf("project-%d", i%2), fmt.Sprintf("type-%d", i%3)
		item := driver.ReadyMembershipItem{ID: id, Project: project, Type: kind, Status: "open", Revision: model.Natural64(0), Fields: map[string]any{"title": id}}
		mutations[i] = Mutation{Ref: model.RefByID(id, model.Natural64(0)), Item: &item, Priority: int64(i % 5), Project: project, Type: kind, Binding: "ontology", Eligible: true, Local: true, CreatedAt: created}
	}
	return mutations
}

func TestInitialReadyBatchKeepsScalarImagesAndEveryScopeRoot(t *testing.T) {
	ctx := context.Background()
	mutations := initialBatchMutations()
	batch, scalar := &initialBatchImage{}, &initialBatchImage{}
	got, err := Prepare(ctx, batch, mutations)
	if err != nil {
		t.Fatal(err)
	}
	// Embedding only Observer deliberately removes the optional batch capability.
	want, err := Prepare(ctx, struct{ Observer }{scalar}, mutations)
	if err != nil || !got.Available() || !want.Available() {
		t.Fatalf("initial preparation unavailable: %v", err)
	}
	if len(batch.scalar) != 1 || batch.scalar[0][0].Derived.Key != markerKey || len(batch.pages) < 2 || len(scalar.pages) != 0 {
		t.Fatalf("initial physical reads were not bounded batches: scalar=%d pages=%d", len(batch.scalar), len(batch.pages))
	}
	observed := map[string]bool{}
	for _, page := range batch.pages {
		if len(page) == 0 || len(page) > initialReadBatch {
			t.Fatalf("physical page size=%d", len(page))
		}
		for _, key := range page {
			if observed[key.Derived.Key] {
				t.Fatal("candidate observed twice")
			}
			observed[key.Derived.Key] = true
		}
	}
	if !reflect.DeepEqual(initialBatchLockIDs(got), initialBatchLockIDs(want)) {
		t.Fatal("batch changed consumed optimistic locks")
	}
	for _, key := range got.Locks() {
		if key.Kind == driver.LockDerived && key.Derived.Key != markerKey && !observed[key.Derived.Key] {
			t.Fatalf("consumed path escaped physical batch: %s", key.Derived.Key)
		}
	}
	a, b := &initialBatchCapture{}, &initialBatchCapture{}
	wa, err := got.Publish(ctx, a, model.Natural64(1))
	if err != nil {
		t.Fatal(err)
	}
	wb, err := want.Publish(ctx, b, model.Natural64(1))
	if err != nil || !reflect.DeepEqual(a, b) || wa != wb {
		t.Fatalf("batch changed complete publication, predicates or work: %v", err)
	}
	// Derive every scope's entire canonical tree independently from final items.
	byScope := map[string][]driver.ReadyOrderLeaf{}
	for _, mutation := range mutations {
		item := *mutation.Item
		item.Revision = model.Natural64(1)
		leaf, err := driver.ReadyOrderLeafFor(item, mutation.Priority)
		if err != nil {
			t.Fatal(err)
		}
		for _, scope := range scopes(source{Project: mutation.Project, Type: mutation.Type}) {
			byScope[scope] = append(byScope[scope], leaf)
		}
	}
	rows := map[string][]byte{}
	for _, row := range a.rows {
		rows[row.Key] = row.Value
	}
	for scope, leaves := range byScope {
		if !observed[nodeKey(scope, "")] {
			t.Fatalf("scope root omitted from physical census: %s", scope)
		}
		nodes, err := driver.BuildReadyOrderNodes(leaves)
		if err != nil {
			t.Fatal(err)
		}
		for prefix, node := range nodes {
			encoded, err := json.Marshal(node)
			if err != nil || string(rows[nodeKey(scope, prefix)]) != string(encoded) {
				t.Fatalf("initial publication differs from complete canonical scope: %s/%s", scope, prefix)
			}
		}
	}
}

// Compare encoded lock identities independently of pointer storage.
func initialBatchLockIDs(d *Delta) []string {
	keys := []string{}
	for _, key := range d.Locks() {
		encoded, _ := driver.EncodeLockKey(key)
		keys = append(keys, encoded)
	}
	sort.Strings(keys)
	return keys
}

func TestInitialReadyBatchRefusesPhysicalCorruptionAndObservationFailures(t *testing.T) {
	mutations := initialBatchMutations()
	scoped := scopes(source{Project: mutations[0].Project, Type: mutations[0].Type})[3]
	for _, key := range []string{sourceKey(mutations[0].Ref.ID), nodeKey(scoped, "")} {
		t.Run(key, func(t *testing.T) {
			state := driver.LockState{Key: lock(key), Revision: model.Natural64(0), Present: true, Derived: &driver.DerivedRow{Key: key, Value: []byte(`{`)}}
			for _, batched := range []bool{false, true} {
				image := &initialBatchImage{states: map[string]driver.LockState{key: state}}
				var reader Observer = image
				if !batched {
					reader = struct{ Observer }{image}
				}
				if _, err := Prepare(context.Background(), reader, mutations); !errors.Is(err, driver.ErrUnsupported) {
					t.Fatalf("physical corruption accepted (batch=%t): %v", batched, err)
				}
			}
		})
	}
	for _, mode := range []string{"omitted", "wrong-key", "query-error", "cancelled", "cancel-after-page"} {
		t.Run(mode, func(t *testing.T) {
			failure := errors.New("physical observation failed")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			image := &initialBatchImage{}
			image.alter = func(states []driver.LockState) ([]driver.LockState, error) {
				switch mode {
				case "omitted":
					return states[:len(states)-1], nil
				case "wrong-key":
					states[0].Key = lock("wrong")
				case "query-error":
					return nil, failure
				case "cancelled":
					return nil, context.Canceled
				case "cancel-after-page":
					cancel()
				}
				return states, nil
			}
			_, err := Prepare(ctx, image, mutations)
			want := driver.ErrUnsupported
			if mode == "query-error" {
				want = failure
			} else if mode == "cancelled" || mode == "cancel-after-page" {
				want = context.Canceled
			}
			if !errors.Is(err, want) {
				t.Fatalf("physical failure changed: %v", err)
			}
		})
	}
}

func TestInitialReadyBatchRetainsScalarAdmissionAndCoverageFallback(t *testing.T) {
	for _, mode := range []string{"duplicate", "invalid-priority", "small", "coverage-gap", "existing-marker"} {
		t.Run(mode, func(t *testing.T) {
			mutations := initialBatchMutations()
			image := &initialBatchImage{}
			switch mode {
			case "duplicate":
				mutations[1] = mutations[0]
			case "invalid-priority":
				mutations[1].Priority = 5
			case "small":
				mutations = mutations[:63]
			case "coverage-gap":
				image.at = 1
			case "existing-marker":
				raw, err := json.Marshal(marker{Version: 3, Incarnation: "initial-batch", Through: model.Natural64(0)})
				if err != nil {
					t.Fatal(err)
				}
				image.states = map[string]driver.LockState{markerKey: {Key: lock(markerKey), Present: true, Revision: model.Natural64(0), Derived: &driver.DerivedRow{Key: markerKey, Value: raw}}}
			}
			other := &initialBatchImage{states: image.states, at: image.at}
			a, ea := Prepare(context.Background(), image, mutations)
			b, eb := Prepare(context.Background(), struct{ Observer }{other}, mutations)
			if fmt.Sprint(ea) != fmt.Sprint(eb) || a.Available() != b.Available() || len(image.pages) != 0 {
				t.Fatalf("scalar validation/fallback changed: %v / %v", ea, eb)
			}
		})
	}
}
