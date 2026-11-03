package memory

import (
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// A certified predecessor already proved every untouched chain. Recheck each
// affected chain, and the reverse-indexed descendants of a changed Parent,
// against current typed source rows. This proof never enumerates a family.
// Cold/unbuilt publication still has no interval; startup independently proves
// ALL final chains, including the privately decoded source rows.
func selectionAffectedParentChains(before, next *state, census *selectionCoverageState, affected map[string]bool, view selectionView) bool {
	beads := map[string]*driver.SelectionBead{}
	load := func(source string) *driver.SelectionBead {
		if bead, seen := beads[source]; seen {
			return bead
		}
		bead := selectionCensusBead(next, census, source, view)
		beads[source] = bead
		return bead
	}
	check := map[string]bool{}
	for source := range affected {
		check[source] = true
		previous := selectionCensusBead(before, before.selection, source, selectionView{state: before, at: mustSelectionHead(before)})
		current := load(source)
		changedParent := previous == nil || current == nil || (previous.Parent == nil) != (current.Parent == nil)
		if previous != nil && current != nil && previous.Parent != nil && current.Parent != nil {
			changedParent = !driver.SelectionSameRef(*previous.Parent, *current.Parent)
		}
		if changedParent {
			for _, index := range []*selectionCoverageState{before.selection, census} {
				if index != nil {
					index.targets.prefix(source+"\x00", func(_ string, owner selectionRowOwner) { check[owner.source] = true })
				}
			}
		}
	}
	for source := range check {
		bead := load(source)
		if bead == nil {
			// A fully removed source has no remaining chain. A live source
			// without exactly one current typed bead is unavailable.
			if _, live := next.records[source]; live {
				return false
			}
		}
		chain := map[uint64]model.Ref{}
		valid := true
		census.sourceRows(source, func(id string, owner selectionRowOwner) {
			if owner.family.Table != driver.DerivedAncestorClosure {
				return
			}
			actual := next.keys[id]
			if actual.Derived == nil || actual.Derived.Index == nil || actual.Derived.Index.Facts == nil {
				valid = false
				return
			}
			if _, err := view.index(*actual.Derived, owner.family.Table, view.at); err != nil {
				valid = false
			}
			for _, fact := range actual.Derived.Index.Facts.Rows {
				if bead == nil || fact.Ref == nil || !driver.SelectionSameRef(*fact.Ref, bead.Ref) || fact.Target == nil {
					valid = false
					continue
				}
				if _, duplicate := chain[fact.Depth]; duplicate {
					valid = false
				}
				chain[fact.Depth] = *fact.Target
			}
		})
		if !valid || bead != nil && len(chain) == 0 {
			return false
		}
		if bead == nil {
			continue
		}
		expected := bead.Ref
		for depth := uint64(0); depth < uint64(len(chain)); depth++ {
			actual, found := chain[depth]
			if !found || !driver.SelectionSameRef(actual, expected) {
				return false
			}
			target := load(keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &actual}))
			if target == nil || (target.Parent == nil) != (depth+1 == uint64(len(chain))) {
				return false
			}
			if target.Parent != nil {
				expected = *target.Parent
			}
		}
	}
	return true
}

func selectionCensusBead(state *state, census *selectionCoverageState, source string, view selectionView) *driver.SelectionBead {
	if census == nil {
		return nil
	}
	var bead *driver.SelectionBead
	count := 0
	census.sourceRows(source, func(id string, owner selectionRowOwner) {
		if owner.family.Table != driver.DerivedSortKey {
			return
		}
		count++
		actual := state.keys[id]
		if actual.Derived == nil || actual.Derived.Index == nil {
			return
		}
		if record, err := view.index(*actual.Derived, owner.family.Table, view.at); err == nil && record.Record != nil && record.Record.Kind == "bead" {
			bead = actual.Derived.Index.Bead
		}
	})
	if count != 1 {
		return nil
	}
	return bead
}
