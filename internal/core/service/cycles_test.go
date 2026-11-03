package service

import (
	"context"
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
