package memory

import (
	"context"
	"encoding/json"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"reflect"
	"testing"
)

func TestB06PhysicalMissingTopologyMemory(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease, err := s.Lease(ctx, "b06", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	ids := []string{"b06-a", "b06-b", "b06-c"}
	changes := []conformance.FixtureChange{}
	for i, id := range ids {
		kind := "tracks"
		if i == 0 {
			kind = "blocks"
		}
		to := model.RefByID(ids[(i+1)%3], model.Natural64(0))
		body, err := json.Marshal(pipeline.Document{ID: id, Type: "issue", Project: "b06", Fields: map[string]any{"title": id}, Links: []pipeline.Link{{To: to, Kind: kind}}})
		if err != nil {
			t.Fatal(err)
		}
		changes = append(changes, conformance.FixtureChange{Ref: model.RefByID(id, model.Natural64(0)), Kind: "bead", Op: model.OpCreate, Body: body, Graph: &conformance.FixtureGraphSource{Links: []driver.SourceLink{{To: to, Kind: kind}}}})
	}
	if _, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, changes); err != nil {
		t.Fatal(err)
	}
	read := func() ([]driver.CycleTopologyEdge, service.CyclesResult) {
		meta, err := s.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		r, err := s.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		facts := []driver.CycleTopologyEdge{}
		var cursor *driver.Cursor
		for {
			page, err := r.(driver.CycleTopologyReader).CycleTopology(ctx, cursor, 1)
			if err != nil {
				t.Fatal(err)
			}
			facts = append(facts, page.Items...)
			if page.NextCursor == nil {
				break
			}
			cursor = page.NextCursor
		}
		got, err := service.ReadCycles(ctx, r, service.CyclesRequest{Project: "b06", IncludeTracks: true}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return facts, got
	}
	original, complete := read()
	if len(complete.Items) != 1 || complete.Items[0].Partial {
		t.Fatalf("complete graph: %+v", complete)
	}
	for i, id := range ids[:2] {
		// Deliberately corrupt this private test's hydration map only. No product
		// writer or deletion path claims to permit this inconsistent state.
		s.mu.Lock()
		next := s.state.fork()
		key, _ := recordKey(model.RefByID(id, model.Natural64(0)))
		delete(next.records, key)
		s.state = next
		s.mu.Unlock()
		facts, got := read()
		if !reflect.DeepEqual(original, facts) {
			t.Fatal("corruption changed facts")
		}
		if len(got.Items) != 1 || got.CycleCount.String() != "1" || got.Truncated {
			t.Fatalf("lost cycle: %+v", got)
		}
		c := got.Items[0]
		if !c.Partial || c.MemberCount.String() != "3" || c.UnreadableCount.String() != model.Natural64(uint64(i+1)).String() {
			t.Fatalf("partial: %+v", c)
		}
		for j, member := range c.Path {
			if j <= i {
				if member.Ref != nil || !member.Unreadable {
					t.Fatal("absent position leaked")
				}
			} else if member.Ref == nil || member.Ref.ID != ids[j] {
				t.Fatal("changed raw direction/order")
			}
		}
	}
	t.Log("B06_PHYSICAL_MISSING engine=memory same_store=true missing=1_then_2 topology=unchanged positions=exact members=3 ordinary_delete=NOT_CLAIMED")
}
