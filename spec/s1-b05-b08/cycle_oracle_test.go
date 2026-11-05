package s1b05b08

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
)

type cycleOracleEdge struct{ from, to, kind string }

// This is an authored semantic oracle, never a reference recording. It uses
// an explicit DFS traversal trace for the default report, then exhaustively
// enumerates simple return paths and orders them by length and original path
// before rotation. It uses no production selection/BFS/SCC/projection helper.
func cycleReportOracle(edges []cycleOracleEdge, tracks bool) [][]string {
	base, mixed := map[string][]string{}, map[string][]string{}
	for _, e := range edges {
		blocking := e.kind == "blocks" || e.kind == "conditional-blocks"
		if blocking {
			base[e.from] = append(base[e.from], e.to)
			if base[e.to] == nil {
				base[e.to] = []string{}
			}
		}
		if blocking || e.kind == "tracks" {
			mixed[e.from] = append(mixed[e.from], e.to)
		}
	}
	for from := range base {
		sort.Strings(base[from])
	}
	selected := map[string][]string{}
	retain := func(path []string) {
		labels := append([]string{}, path...)
		sort.Strings(labels)
		at := 0
		for path[at] != labels[0] {
			at++
		}
		canonical := append(append([]string{}, path[at:]...), path[:at]...)
		selected[strings.Join(canonical, "\x00")] = canonical
	}
	// Iterative frame simulation selects only sorted DFS back edges. It does
	// not enumerate every simple cycle and then call that the default set.
	type frame struct {
		id   string
		edge int
	}
	roots := make([]string, 0, len(base))
	for id := range base {
		roots = append(roots, id)
	}
	sort.Strings(roots)
	finished := map[string]bool{}
	for _, root := range roots {
		if finished[root] {
			continue
		}
		frames := []frame{{id: root}}
		for len(frames) != 0 {
			top := &frames[len(frames)-1]
			if top.edge == len(base[top.id]) {
				finished[top.id] = true
				frames = frames[:len(frames)-1]
				continue
			}
			next := base[top.id][top.edge]
			top.edge++
			activeAt := -1
			for i, f := range frames {
				if f.id == next {
					activeAt = i
					break
				}
			}
			if activeAt >= 0 {
				path := []string{}
				for _, f := range frames[activeAt:] {
					path = append(path, f.id)
				}
				retain(path)
			} else if !finished[next] {
				frames = append(frames, frame{id: next})
			}
		}
	}
	if tracks {
		for from, tos := range base {
			for _, to := range tos {
				returns := [][]string{}
				var enumerate func([]string)
				enumerate = func(path []string) {
					last := path[len(path)-1]
					if last == from {
						returns = append(returns, append([]string{}, path...))
						return
					}
					for _, next := range mixed[last] {
						repeated := false
						for _, seen := range path {
							if next == seen {
								repeated = true
								break
							}
						}
						if !repeated {
							enumerate(append(append([]string{}, path...), next))
						}
					}
				}
				enumerate([]string{to})
				if len(returns) == 0 {
					continue
				}
				sort.Slice(returns, func(i, j int) bool {
					if len(returns[i]) != len(returns[j]) {
						return len(returns[i]) < len(returns[j])
					}
					return oraclePathLess(returns[i], returns[j])
				})
				best := returns[0]
				retain(append([]string{from}, best[:len(best)-1]...))
			}
		}
	}
	out := make([][]string, 0, len(selected))
	for _, path := range selected {
		out = append(out, path)
	}
	sort.Slice(out, func(i, j int) bool { return oraclePathLess(out[i], out[j]) })
	return out
}

func oraclePathLess(a, b []string) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return len(a) < len(b)
}

func cycleOracleRows(edges []cycleOracleEdge) []cycleSeedRow {
	rows := map[string]*cycleSeedRow{}
	for _, e := range edges {
		for _, id := range []string{e.from, e.to} {
			if rows[id] == nil {
				rows[id] = &cycleSeedRow{id: id, project: "visible"}
			}
		}
		rows[e.from].links = append(rows[e.from].links, pipeline.Link{To: model.RefByID(e.to, model.Natural64(0)), Kind: e.kind})
	}
	ids := make([]string, 0, len(rows))
	for id := range rows {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]cycleSeedRow, 0, len(ids))
	for _, id := range ids {
		out = append(out, *rows[id])
	}
	return out
}

func TestB06CycleReportPathOracle(t *testing.T) {
	graphs := [][]cycleOracleEdge{
		{{"a", "b", "blocks"}, {"b", "c", "blocks"}, {"c", "a", "conditional-blocks"}, {"a", "c", "blocks"}},
		{{"z", "m", "blocks"}, {"m", "n", "tracks"}, {"n", "y", "tracks"}, {"y", "z", "tracks"}, {"m", "p", "tracks"}, {"p", "a", "tracks"}, {"a", "z", "tracks"}},
		{{"a", "b", "blocks"}, {"b", "a", "blocks"}, {"c", "d", "blocks"}, {"d", "c", "blocks"}, {"b", "c", "tracks"}, {"d", "a", "tracks"}},
		{{"a", "b", "blocks"}, {"a", "b", "conditional-blocks"}, {"a", "b", "tracks"}, {"b", "a", "blocks"}},
		{{"a", "b", "tracks"}, {"b", "a", "tracks"}},
		{{"a", "b", "waits-for"}, {"b", "a", "blocks"}, {"a", "c", "parent-child"}, {"c", "a", "blocks"}},
		{{"a", "a", "blocks"}, {"b", "b", "tracks"}},
	}
	// These discriminators constrain the oracle itself before it is used to
	// judge production, including the pre-rotation shortest-return tie.
	for _, tc := range []struct {
		graph  int
		tracks bool
		want   [][]string
	}{
		{0, false, [][]string{{"a", "b", "c"}}}, {0, true, [][]string{{"a", "b", "c"}, {"a", "c"}}},
		{1, false, [][]string{}}, {1, true, [][]string{{"m", "n", "y", "z"}}},
		{2, true, [][]string{{"a", "b"}, {"c", "d"}}}, {3, true, [][]string{{"a", "b"}}},
		{4, true, [][]string{}}, {5, true, [][]string{}}, {6, true, [][]string{{"a"}}},
	} {
		if got := cycleReportOracle(graphs[tc.graph], tc.tracks); !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("oracle discriminator %d tracks=%v got=%v want=%v", tc.graph, tc.tracks, got, tc.want)
		}
	}
	random := rand.New(rand.NewSource(606))
	kinds := []string{"blocks", "conditional-blocks", "tracks", "waits-for", "parent-child", "relates-to"}
	for graph := 0; graph < 32; graph++ {
		edges := []cycleOracleEdge{}
		parentSeen := [5]bool{}
		for from := 0; from < 5; from++ {
			for to := 0; to < 5; to++ {
				if random.Intn(4) != 0 {
					continue
				}
				e := cycleOracleEdge{fmt.Sprintf("n%d", from), fmt.Sprintf("n%d", to), kinds[random.Intn(len(kinds))]}
				// Consume every duplicate draw before filtering hierarchy noise so
				// the scheduling edges retain their original random stream.
				duplicate := random.Intn(4) == 0
				if e.kind == "parent-child" {
					// Increasing IDs keep parents acyclic; each child has one row.
					if from >= to || parentSeen[from] {
						continue
					}
					parentSeen[from] = true
					duplicate = false
				}
				edges = append(edges, e)
				if duplicate {
					edges = append(edges, e)
				}
			}
		}
		graphs = append(graphs, edges)
	}
	all := []cycleOracleEdge{}
	for i, graph := range graphs {
		for _, e := range graph {
			e.from = fmt.Sprintf("g%02d-%s", i, e.from)
			e.to = fmt.Sprintf("g%02d-%s", i, e.to)
			all = append(all, e)
		}
	}
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			f := newCycleRPCFixtureConfigured(t, engine, cycleOracleRows(all), nil)
			for _, tracks := range []bool{false, true} {
				t.Run(fmt.Sprintf("includeTracks=%v", tracks), func(t *testing.T) {
					want := cycleReportOracle(all, tracks)
					got, err := f.call(t, model.PrincipalRef{Kind: "user", ID: "user:oracle-reader"}, "bead.cycles", map[string]any{"includeTracks": tracks, "limit": 1000})
					if err != nil || !reflect.DeepEqual(cycleRPCPaths(got), want) || got.Truncated || got.CycleCount.String() != fmt.Sprint(len(want)) {
						t.Fatalf("exact selected set differs: paths=%v want=%v count=%s truncated=%v error=%v", cycleRPCPaths(got), want, got.CycleCount, got.Truncated, err)
					}
					t.Logf("authenticated %s: %d graphs, %d exact selected paths, tracks=%v; independent exhaustive returns before rotation", engine, len(graphs), len(want), tracks)
				})
			}
		})
	}
}
