package service

import (
	"context"
	"reflect"
	"sort"
	"testing"
)

type oracleEdge struct {
	from, to string
	blocking bool
}

// This oracle is deliberately separate from cyclePaths. It selects the
// pinned default DFS back-edge set, then runs an independent sorted BFS for
// each blocking edge and rotates only after the shortest return is selected.
func TestCyclePathsUsesThePinnedSelectionSet(t *testing.T) {
	tests := []struct {
		name  string
		edges []oracleEdge
	}{
		{"tie chooses BFS predecessor before rotation", []oracleEdge{
			{from: "z", to: "m", blocking: true},
			{from: "m", to: "n"}, {from: "n", to: "y"}, {from: "y", to: "z"},
			{from: "m", to: "p"}, {from: "p", to: "a"}, {from: "a", to: "z"},
		}},
		{"chord adds one exact shortest cycle", []oracleEdge{
			{from: "a", to: "b", blocking: true}, {from: "b", to: "c", blocking: true},
			{from: "c", to: "a", blocking: true}, {from: "a", to: "c", blocking: true},
		}},
		{"fused deadlocks preserve both", []oracleEdge{
			{from: "a", to: "b", blocking: true}, {from: "b", to: "a", blocking: true},
			{from: "c", to: "d", blocking: true}, {from: "d", to: "c", blocking: true},
			{from: "b", to: "c"}, {from: "d", to: "a"},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base, combined := oracleGraphs(test.edges)
			got, err := cyclePaths(context.Background(), base, combined, true)
			if err != nil {
				t.Fatal(err)
			}
			want := independentCycleOracle(test.edges)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("selected cycles = %v, oracle = %v", got, want)
			}
		})
	}
}

func oracleGraphs(edges []oracleEdge) (map[string]map[string]bool, map[string]map[string]bool) {
	base, combined := map[string]map[string]bool{}, map[string]map[string]bool{}
	for _, edge := range edges {
		if combined[edge.from] == nil {
			combined[edge.from] = map[string]bool{}
		}
		combined[edge.from][edge.to] = true
		if combined[edge.to] == nil {
			combined[edge.to] = map[string]bool{}
		}
		if edge.blocking {
			if base[edge.from] == nil {
				base[edge.from] = map[string]bool{}
			}
			base[edge.from][edge.to] = true
			if base[edge.to] == nil {
				base[edge.to] = map[string]bool{}
			}
		}
	}
	return base, combined
}

func independentCycleOracle(edges []oracleEdge) [][]string {
	base, combined := map[string][]string{}, map[string][]string{}
	blocking := map[[2]string]bool{}
	for _, edge := range edges {
		combined[edge.from] = append(combined[edge.from], edge.to)
		if edge.blocking {
			base[edge.from] = append(base[edge.from], edge.to)
			blocking[[2]string{edge.from, edge.to}] = true
		}
	}
	for _, graph := range []map[string][]string{base, combined} {
		for from := range graph {
			sort.Strings(graph[from])
			graph[from] = compactStrings(graph[from])
		}
	}
	paths := map[string][]string{}
	rotate := func(path []string) []string {
		at := 0
		for i := range path {
			if path[i] < path[at] {
				at = i
			}
		}
		return append(append([]string{}, path[at:]...), path[:at]...)
	}
	nodes := map[string]bool{}
	for from, tos := range base {
		nodes[from] = true
		for _, to := range tos {
			nodes[to] = true
		}
	}
	roots := make([]string, 0, len(nodes))
	for node := range nodes {
		roots = append(roots, node)
	}
	sort.Strings(roots)
	active, finished := map[string]int{}, map[string]bool{}
	stack := []string{}
	var visit func(string)
	visit = func(node string) {
		if start, ok := active[node]; ok {
			path := rotate(stack[start:])
			paths[stringPath(path)] = path
			return
		}
		if finished[node] {
			return
		}
		active[node] = len(stack)
		stack = append(stack, node)
		for _, next := range base[node] {
			visit(next)
		}
		stack = stack[:len(stack)-1]
		delete(active, node)
		finished[node] = true
	}
	for _, node := range roots {
		visit(node)
	}
	for edge := range blocking {
		from, to := edge[0], edge[1]
		previous := map[string]string{to: ""}
		queue := []string{to}
		for head := 0; head < len(queue); head++ {
			at := queue[head]
			if at == from {
				path := []string{at}
				for at != to {
					at = previous[at]
					path = append(path, at)
				}
				for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
					path[i], path[j] = path[j], path[i]
				}
				cycle := rotate(append([]string{from}, path[:len(path)-1]...))
				paths[stringPath(cycle)] = cycle
				break
			}
			for _, next := range combined[at] {
				if _, seen := previous[next]; !seen {
					previous[next] = at
					queue = append(queue, next)
				}
			}
		}
	}
	out := make([][]string, 0, len(paths))
	for _, path := range paths {
		out = append(out, path)
	}
	sort.Slice(out, func(i, j int) bool {
		for k := 0; k < len(out[i]) && k < len(out[j]); k++ {
			if out[i][k] != out[j][k] {
				return out[i][k] < out[j][k]
			}
		}
		return len(out[i]) < len(out[j])
	})
	return out
}

func compactStrings(values []string) []string {
	out := values[:0]
	for _, value := range values {
		if len(out) == 0 || out[len(out)-1] != value {
			out = append(out, value)
		}
	}
	return out
}

func stringPath(path []string) string {
	value := ""
	for _, part := range path {
		value += string(rune(len(part))) + part
	}
	return value
}
