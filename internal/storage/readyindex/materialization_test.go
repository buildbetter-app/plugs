package readyindex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type materializationImage struct {
	at     model.Revision
	states map[string]driver.LockState
}

func (r materializationImage) Snapshot() driver.View {
	return driver.View{Incarnation: "materialization-oracle", At: &r.at, Branch: model.Natural64(0)}
}

func (r materializationImage) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	states := make([]driver.LockState, len(keys))
	for i, key := range keys {
		state, ok := r.states[key.Derived.Key]
		if !ok {
			state = driver.LockState{Key: key, Revision: model.Natural64(0)}
		}
		if state.Derived != nil {
			row := state.Derived.Clone()
			state.Derived = &row
		}
		states[i] = state
	}
	return states, nil
}

// This algorithm oracle compares all materialized node bytes with the separate
// full-set canonical builder. Real producer/SQL acceptance is tested elsewhere.
func TestReadyOrderMaterializedNodesMatchIndependentCanonicalBuild(t *testing.T) {
	ctx := context.Background()
	image := materializationImage{at: model.Natural64(0), states: map[string]driver.LockState{}}
	type currentItem struct {
		mutation Mutation
		revision model.Revision
	}
	current := map[string]currentItem{}
	knownScopes := map[string]bool{scopeID(driver.ReadyOrderScope{}): true}
	makeMutation := func(i int, project string, priority int64) Mutation {
		id := fmt.Sprintf("materialized-%03d", i)
		item := driver.ReadyMembershipItem{ID: id, Type: "issue", Project: project, Status: "open", Fields: map[string]any{"title": id, "priority": json.Number(fmt.Sprint(priority))}}
		return Mutation{Ref: model.RefByID(id, model.Natural64(0)), Item: &item, Priority: priority, Project: project, Type: item.Type,
			Binding: "canonical-ontology", Eligible: true, Local: true, SourceDigest: driver.SelectionDigest([]byte(id))}
	}
	initial := make([]Mutation, 257)
	for i := range initial {
		initial[i] = makeMutation(i, fmt.Sprintf("project-%d", i%3), int64(i%5))
	}
	changed := []Mutation{}
	for i := 0; i < 60; i++ {
		m := makeMutation(i, "moved-project", int64((i+2)%5))
		if i%3 == 0 {
			m.Item = nil
		} else if i%3 == 1 {
			m.Eligible = false
		}
		changed = append(changed, m)
	}
	changed = append(changed, makeMutation(900, "new-project", 4))
	for batch, mutations := range [][]Mutation{initial, changed, nil} {
		delta, err := Prepare(ctx, image, mutations)
		if err != nil || !delta.Available() {
			t.Fatalf("batch%d prepare: %v", batch, err)
		}
		for key := range delta.t.dirty {
			if !strings.HasPrefix(key, prefix+"node/") || delta.t.rows[key] == nil {
				continue
			}
			node := delta.t.nodes[key]
			if node == nil || node.Commitment.Hash != unpublishedNodeHash || len(delta.t.rows[key].Value) != 0 {
				t.Fatalf("batch%d intermediate node escaped private preparation: %s", batch, key)
			}
		}
		revision := model.Natural64(uint64(batch + 1))
		removed, added, err := delta.materialize(revision)
		if err != nil {
			t.Fatal(err)
		}
		publication, err := delta.publication(revision, removed, added)
		if err != nil {
			t.Fatal(err)
		}
		verified, err := ValidatePublication(ctx, publication)
		if err != nil {
			t.Fatalf("batch%d full independent publication replay: %v", batch, err)
		}
		for _, m := range mutations {
			for _, scope := range scopes(source{Project: m.Project, Type: m.Type}) {
				knownScopes[scope] = true
			}
			if m.Item == nil {
				delete(current, m.Ref.ID)
			} else {
				current[m.Ref.ID] = currentItem{mutation: m, revision: revision}
			}
		}
		for _, row := range removed {
			image.states[row.Key] = driver.LockState{Key: lock(row.Key), Revision: revision}
		}
		for _, row := range added {
			copy := row.Clone()
			image.states[row.Key] = driver.LockState{Key: lock(row.Key), Revision: revision, Present: true, Derived: &copy}
		}
		image.at = revision
		for scope := range knownScopes {
			leaves := []driver.ReadyOrderLeaf{}
			for _, value := range current {
				m := value.mutation
				if !m.Eligible {
					continue
				}
				belongs := false
				for _, candidate := range scopes(source{Project: m.Project, Type: m.Type}) {
					belongs = belongs || candidate == scope
				}
				if belongs {
					item := *m.Item
					item.Revision = value.revision
					leaf, err := driver.ReadyOrderLeafFor(item, m.Priority)
					if err != nil {
						t.Fatal(err)
					}
					leaves = append(leaves, leaf)
				}
			}
			nodes, err := driver.BuildReadyOrderNodes(leaves)
			if err != nil {
				t.Fatal(err)
			}
			seen := 0
			for key, state := range image.states {
				if !state.Present || !strings.HasPrefix(key, nodeKey(scope, "")) {
					continue
				}
				prefix := strings.TrimPrefix(key, nodeKey(scope, ""))
				node, exists := nodes[prefix]
				encoded, err := json.Marshal(node)
				if !exists || err != nil || !bytes.Equal(state.Derived.Value, encoded) {
					t.Fatalf("batch%d node differs from independent canonical builder: %s", batch, key)
				}
				seen++
			}
			if seen != len(nodes) {
				t.Fatalf("batch%d scope incomplete: got%d want%d", batch, seen, len(nodes))
			}
			if scope == scopeID(driver.ReadyOrderScope{}) && verified.Root != nodes[""].Commitment {
				t.Fatal("independently replayed root differs from full-set canonical root")
			}
		}
	}
	// Deferring serialization must preserve the original persisted-node seal
	// validation before any proposed transition can be materialized.
	key := nodeKey(scopeID(driver.ReadyOrderScope{}), "")
	state := image.states[key]
	row := state.Derived.Clone()
	var node driver.ReadyOrderNode
	if err := json.Unmarshal(row.Value, &node); err != nil {
		t.Fatal(err)
	}
	node.Commitment.Hash = strings.Repeat("0", 64)
	row.Value, _ = json.Marshal(node)
	state.Derived = &row
	image.states[key] = state
	if _, err := Prepare(ctx, image, nil); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("invalid predecessor seal accepted: %v", err)
	}
}
