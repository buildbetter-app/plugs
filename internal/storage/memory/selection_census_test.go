package memory

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
)

func TestSelectionCensusRetainsSnapshotsAndExactSourceRanges(t *testing.T) {
	var root *selectionCensusNode
	order := rand.New(rand.NewSource(7)).Perm(600)
	for _, i := range order {
		root = root.put(fmt.Sprintf("source/%03d\x00row/%03d", i%7, i), selectionRowOwner{source: fmt.Sprint(i)})
	}
	retained := root
	for _, i := range order[:300] {
		root = root.remove(fmt.Sprintf("source/%03d\x00row/%03d", i%7, i))
	}
	removed := map[int]bool{}
	for _, i := range order[:300] {
		removed[i] = true
	}
	for source := 0; source < 7; source++ {
		var got, want []string
		root.prefix(fmt.Sprintf("source/%03d\x00", source), func(id string, _ selectionRowOwner) { got = append(got, id) })
		for i := 0; i < 600; i++ {
			key := fmt.Sprintf("source/%03d\x00row/%03d", i%7, i)
			if _, exists := retained.get(key); !exists {
				t.Fatal("later deletion mutated retained census")
			}
			if _, exists := root.get(key); exists == removed[i] {
				t.Fatal("current census lost an insert/delete")
			}
			if i%7 == source && !removed[i] {
				want = append(want, fmt.Sprintf("row/%03d", i))
			}
		}
		if !slices.Equal(got, want) {
			t.Fatalf("source prefix crossed or lost rows: %v != %v", got, want)
		}
	}
	root.prefix("absent\x00", func(string, selectionRowOwner) { t.Fatal("absent source returned a row") })
}
