package readyindex

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type readyFinalImageCapture struct {
	driver.WriteTx
	remove, add []driver.DerivedRow
	bumps       []driver.Sentinel
}

func (w *readyFinalImageCapture) PutDerived(_ context.Context, table driver.DerivedTable, remove, add []driver.DerivedRow) error {
	if table != driver.DerivedReadyState {
		return fmt.Errorf("unexpected Ready publication table")
	}
	for _, row := range remove {
		w.remove = append(w.remove, row.Clone())
	}
	for _, row := range add {
		w.add = append(w.add, row.Clone())
	}
	return nil
}
func (w *readyFinalImageCapture) BumpSentinels(_ context.Context, sentinels []driver.Sentinel) error {
	w.bumps = append(w.bumps, sentinels...)
	return nil
}

func TestReadyFinalNodeImagesPreserveUpdatesDeletesAndDeclaredReads(t *testing.T) {
	ctx := context.Background()
	image := &initialBatchImage{states: map[string]driver.LockState{}}
	durable := map[string]driver.DerivedRow{}
	final := map[string]Mutation{}
	allScopes := map[string]bool{}
	mutations := initialBatchMutations()
	for round := 1; round <= 3; round++ {
		if round == 2 {
			mutations = make([]Mutation, 0, 128)
			for i, original := range initialBatchMutations() {
				m := original
				item := *m.Item
				item.Fields = map[string]any{"title": "updated " + m.Ref.ID}
				m.Item = &item
				switch i % 4 {
				case 0:
					m.Item = nil
				case 1:
					m.Project, m.Item.Project = "moved-project", "moved-project"
					m.Priority = (m.Priority + 1) % 5
				case 2:
					m.Eligible = false
				}
				mutations = append(mutations, m)
			}
		} else if round == 3 {
			mutations = make([]Mutation, 0, len(final))
			for _, original := range initialBatchMutations() {
				if _, ok := final[original.Ref.ID]; ok {
					mutations = append(mutations, Mutation{Ref: original.Ref})
				}
			}
		}
		plan, err := Prepare(ctx, image, mutations)
		if err != nil || !plan.Available() {
			t.Fatalf("round%d preparation: %v", round, err)
		}
		// Every output identity must already belong to the pre-Head lock census.
		declared := map[string]bool{}
		for _, key := range plan.Locks() {
			if key.Derived != nil {
				declared[key.Derived.Key] = true
			}
		}
		scalar, pages := len(image.scalar), len(image.pages)
		capture := &readyFinalImageCapture{}
		work, err := plan.Publish(ctx, capture, model.Natural64(uint64(round)))
		if err != nil || scalar != len(image.scalar) || pages != len(image.pages) {
			t.Fatalf("round%d publication read after preparation: %v", round, err)
		}
		writtenBytes := uint64(0)
		for _, row := range capture.remove {
			if !declared[row.Key] {
				t.Fatalf("undeclared removal: %s", row.Key)
			}
			delete(durable, row.Key)
		}
		for _, row := range capture.add {
			if !declared[row.Key] || !json.Valid(row.Value) {
				t.Fatalf("undeclared or unfinished final image: %s", row.Key)
			}
			writtenBytes += uint64(len(row.Value))
			durable[row.Key] = row.Clone()
		}
		if work.WrittenNodes != uint64(len(capture.remove)+len(capture.add)) || work.WrittenBytes != writtenBytes {
			t.Fatalf("round%d final publication work lost physical bytes/nodes: %+v", round, work)
		}
		for _, m := range mutations {
			if m.Item == nil {
				delete(final, m.Ref.ID)
			} else {
				final[m.Ref.ID] = m
			}
			if m.Item != nil {
				for _, scope := range scopes(source{Project: m.Project, Type: m.Type}) {
					allScopes[scope] = true
				}
			}
		}
		expected := map[string][]byte{}
		for scope := range allScopes {
			leaves := []driver.ReadyOrderLeaf{}
			for _, m := range final {
				if !m.Eligible {
					continue
				}
				present := false
				for _, candidate := range scopes(source{Project: m.Project, Type: m.Type}) {
					if candidate == scope {
						present = true
					}
				}
				if !present {
					continue
				}
				// Untouched sources retain their previously allocated revision. Every
				// surviving source here was touched in round2; round3 removes all of them.
				item := *m.Item
				item.Revision = model.Natural64(uint64(round))
				leaf, err := driver.ReadyOrderLeafFor(item, m.Priority)
				if err != nil {
					t.Fatal(err)
				}
				leaves = append(leaves, leaf)
			}
			nodes, err := driver.BuildReadyOrderNodes(leaves)
			if err != nil {
				t.Fatal(err)
			}
			for prefix, node := range nodes {
				raw, err := json.Marshal(node)
				if err != nil {
					t.Fatal(err)
				}
				expected[nodeKey(scope, prefix)] = raw
			}
		}
		actual := map[string][]byte{}
		for key, row := range durable {
			if strings.HasPrefix(key, prefix+"node/") {
				actual[key] = row.Value
			}
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("round%d complete final node inventory differs from canonical leaves: got%d want%d", round, len(actual), len(expected))
		}
		// A second publication must not stage any intermediate node image.
		if _, err := plan.Publish(ctx, &readyFinalImageCapture{}, model.Natural64(uint64(round))); err == nil {
			t.Fatal("double publication accepted")
		}
		image = &initialBatchImage{at: uint64(round), states: map[string]driver.LockState{}}
		for key, row := range durable {
			value := row.Clone()
			image.states[key] = driver.LockState{Key: lock(key), Revision: model.Natural64(uint64(round)), Present: true, Derived: &value}
		}
	}
}
