package service

import (
	"context"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"reflect"
	"testing"
)

func TestCyclePathsUsesThePinnedSelectionSet(t *testing.T) {
	type edge struct {
		from, to string
		blocking bool
	}
	for _, test := range []struct {
		name             string
		edges            []edge
		ordinary, tracks [][]string
	}{
		{"tie chooses return before rotation", []edge{
			{"z", "m", true}, {"m", "n", false}, {"n", "y", false}, {"y", "z", false},
			{"m", "p", false}, {"p", "a", false}, {"a", "z", false},
		}, [][]string{}, [][]string{{"m", "n", "y", "z"}}},
		{"chord widens without tracks rows", []edge{
			{"a", "b", true}, {"b", "c", true}, {"c", "a", true}, {"a", "c", true},
		}, [][]string{{"a", "b", "c"}}, [][]string{{"a", "b", "c"}, {"a", "c"}}},
		{"fused deadlocks keep shortest returns", []edge{
			{"a", "b", true}, {"b", "a", true}, {"c", "d", true}, {"d", "c", true},
			{"b", "c", false}, {"d", "a", false},
		}, [][]string{{"a", "b"}, {"c", "d"}}, [][]string{{"a", "b"}, {"c", "d"}}},
		{"parallel edges collapse", []edge{
			{"z", "a", true}, {"a", "z", true}, {"z", "a", true}, {"a", "z", false},
		}, [][]string{{"a", "z"}}, [][]string{{"a", "z"}}},
		{"pure tracks loop is excluded", []edge{{"a", "b", false}, {"b", "a", false}}, [][]string{}, [][]string{}},
		{"self edge reports once", []edge{{"a", "a", true}}, [][]string{{"a"}}, [][]string{{"a"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			base, combined := map[string]map[string]bool{}, map[string]map[string]bool{}
			add := func(graph map[string]map[string]bool, e edge) {
				if graph[e.from] == nil {
					graph[e.from] = map[string]bool{}
				}
				if graph[e.to] == nil {
					graph[e.to] = map[string]bool{}
				}
				graph[e.from][e.to] = true
			}
			for _, e := range test.edges {
				add(combined, e)
				if e.blocking {
					add(base, e)
				}
			}
			for _, includeTracks := range []bool{false, true} {
				want := test.ordinary
				if includeTracks {
					want = test.tracks
				}
				got, err := cyclePaths(context.Background(), base, combined, includeTracks)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("includeTracks=%v selected=%v want=%v error=%v", includeTracks, got, want, err)
				}
			}
		})
	}
}

// Topology and hydration are independent inputs. These controls bind the
// consumer's failure boundary without claiming a combined-plane provider.
type b06TopologyReader struct {
	driver.Reader
	pages  []driver.Page[driver.CycleTopologyEdge]
	calls  int
	failAt int
}

func (r *b06TopologyReader) CycleTopology(ctx context.Context, cursor *driver.Cursor, limit int) (driver.Page[driver.CycleTopologyEdge], error) {
	r.calls++
	if r.calls == r.failAt {
		return driver.Page[driver.CycleTopologyEdge]{}, driver.ErrUnsupported
	}
	return r.pages[r.calls-1], nil
}
func TestB06TopologyPaginationFailsClosed(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	defer store.Close()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ref := func(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
	page := driver.Page[driver.CycleTopologyEdge]{AsOf: meta.Head, Items: []driver.CycleTopologyEdge{{From: ref("a"), To: ref("b"), Kind: "blocks"}}, NextCursor: driver.CycleTopologyCursor(meta.Incarnation, meta.Head, 1)}
	for _, test := range []struct {
		name  string
		pages []driver.Page[driver.CycleTopologyEdge]
		fail  int
	}{
		{"later coverage error", []driver.Page[driver.CycleTopologyEdge]{page}, 2},
		{"non advancing", []driver.Page[driver.CycleTopologyEdge]{page, page}, 0},
		{"wrong snapshot", []driver.Page[driver.CycleTopologyEdge]{{AsOf: model.Natural64(1)}}, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader := &b06TopologyReader{Reader: r, pages: test.pages, failAt: test.fail}
			got, err := ReadCycles(ctx, reader, CyclesRequest{Project: "p", Limit: 1}, nil)
			if err == nil || !reflect.DeepEqual(got, CyclesResult{}) {
				t.Fatalf("partial success after topology error: %+v %v", got, err)
			}
		})
	}
}
func TestB06TopologyPresenceDoesNotGrantDisclosure(t *testing.T) {
	ref := func(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
	rows := map[string]cycleRow{
		"a": {project: "p", links: []pipeline.Link{{To: ref("b"), Kind: "blocks"}}},
		"b": {missing: true, links: []pipeline.Link{{To: ref("a"), Kind: "blocks"}}},
	}
	for _, test := range []struct {
		name, project string
		readable      func(context.Context, model.Ref) (bool, error)
	}{
		{"denied anchor", "p", func(context.Context, model.Ref) (bool, error) { return false, nil }},
		{"foreign project", "other", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := readCycleGraph(context.Background(), rows, CyclesRequest{Project: test.project}, 1, AsOf{}, test.readable)
			if err != nil || len(got.Items) != 0 || !got.CycleCount.IsZero() || got.Truncated {
				t.Fatalf("hidden cycle disclosed %+v %v", got, err)
			}
		})
	}
}
