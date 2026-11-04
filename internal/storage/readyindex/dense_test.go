package readyindex

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This is a structural work regression, separate from real-store publication
// and from product latency qualification. The backing observer admits only
// explicit point lookups, so a COUNT/skip or full-leaf stream cannot pass.
type pointImage struct {
	rows  map[string]driver.DerivedRow
	reads int
}

func (r *pointImage) Snapshot() driver.View {
	at := model.Natural64(1)
	return driver.View{Incarnation: "dense", Branch: model.Natural64(0), At: &at}
}
func (r *pointImage) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := driver.ValidateLockKeys(keys); err != nil {
		return nil, err
	}
	states := make([]driver.LockState, len(keys))
	for i, key := range keys {
		r.reads++
		states[i] = driver.LockState{Key: key, Revision: model.Natural64(1)}
		if row, ok := r.rows[key.Derived.Key]; ok {
			v := row
			states[i].Present = true
			states[i].Derived = &v
		}
	}
	return states, nil
}
func TestCountedStructure100000BoundedReads(t *testing.T) {
	const count = 100000
	leaves := make([]driver.ReadyOrderLeaf, count)
	r := &pointImage{rows: map[string]driver.DerivedRow{}}
	put := func(key string, value any) {
		b, e := json.Marshal(value)
		if e != nil {
			t.Fatal(e)
		}
		r.rows[key] = driver.DerivedRow{Key: key, Value: b}
	}
	scope := scopeID(driver.ReadyOrderScope{})
	for i := range leaves {
		id := fmt.Sprintf("item-%06d", i)
		item := driver.ReadyMembershipItem{ID: id, Type: "issue", Project: "project", Status: "open", Revision: model.Natural64(1), Fields: map[string]any{"priority": json.Number("0"), "title": id}}
		var err error
		leaves[i], err = driver.ReadyOrderLeafFor(item, 0)
		if err != nil {
			t.Fatal(err)
		}
		put(sourceKey(id), source{Project: "project", Type: "issue", Binding: "ontology", Local: true, Leaf: &leaves[i]})
	}
	nodes, err := driver.BuildReadyOrderNodes(leaves)
	if err != nil {
		t.Fatal(err)
	}
	for p, node := range nodes {
		put(nodeKey(scope, p), node)
	}
	put(markerKey, marker{Version: 2, Incarnation: "dense", Through: model.Natural64(1)})
	put(headerKey(scope), scopeHeader{Bindings: map[string]uint64{"ontology": count}})
	p := driver.ReadyOrderPlan{Incarnation: "dense", ContentRevision: model.Natural64(1), AuthorityRevision: model.Natural64(1), Eligibility: "ontology", UniformAuthorization: true, Limit: 100}
	for _, offset := range []uint64{0, 50000, 99900} {
		p.Offset = offset
		r.reads = 0
		result, e := Select(context.Background(), r, p)
		if e != nil {
			t.Fatal(e)
		}
		if result.Total != count || len(result.Keys) != 100 || result.Commitment != nodes[""].Commitment || result.Work.Leaves != 100 || result.Work.Nodes > 180 || r.reads > 180 {
			t.Fatalf("dense whole-set work: offset=%d work=%+v reads=%d err=%v", offset, result.Work, r.reads, e)
		}
		if result.Keys[0].Ref.ID != fmt.Sprintf("item-%06d", offset) {
			t.Fatal("deep page ordering")
		}
		t.Logf("READY_READ dense offset=%d nodes=%d leaves=%d", offset, result.Work.Nodes, result.Work.Leaves)
	}
	for _, authorized := range []int{1000, 100} {
		p.Offset = 0
		p.UniformAuthorization = false
		p.AuthorizedRefs = make([]model.Ref, authorized)
		for i := range p.AuthorizedRefs {
			p.AuthorizedRefs[i] = leaves[i*(count/authorized)].Key.Ref
		}
		result, e := Select(context.Background(), r, p)
		if e != nil {
			t.Fatal(e)
		}
		if result.Total != uint64(authorized) || len(result.Keys) != 100 || result.Work.Leaves != uint64(authorized) || result.Work.Nodes > uint64(authorized+3) {
			t.Fatalf("sparse whole-set work: refs=%d work=%+v err=%v", authorized, result.Work, e)
		}
		t.Logf("READY_READ sparse authorized=%d nodes=%d leaves=%d", authorized, result.Work.Nodes, result.Work.Leaves)
		p.Seed = "raw-priority-rank"
		seeded, e := Select(context.Background(), r, p)
		if e != nil {
			t.Fatal(e)
		}
		if seeded.Commitment != result.Commitment || seeded.Work.Leaves > uint64(authorized+1) || seeded.Work.Nodes > uint64(authorized+20) {
			t.Fatalf("raw rank enumerated group: refs=%d work=%+v err=%v", authorized, seeded.Work, e)
		}
		p.Seed = ""
	}
}
