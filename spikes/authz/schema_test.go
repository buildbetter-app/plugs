package authz

import (
	"context"
	"testing"

	_ "modernc.org/sqlite"
)

func TestParentAndDepthMatchesGraph(t *testing.T) {
	maxDepth := 0
	for id := 1; id <= BeadCount; id++ {
		parent, storedDepth := parentAndDepth(id)
		if storedDepth > maxDepth {
			maxDepth = storedDepth
		}
		if storedDepth != graphDepth(id) {
			t.Fatalf("id %d stores depth %d, graph depth is %d", id, storedDepth, graphDepth(id))
		}
		if parent < 0 || parent >= id {
			t.Fatalf("id %d has invalid parent %d", id, parent)
		}
	}
		if got, want := maxDepth, MaxGraphDepth; got != want {
		t.Fatalf("maximum stored depth = %d, want %d", got, want)
	}

	// IDs 3..50,002 are the 50,000 descendants of epic 2. Keep this count
	// explicit so a future fixture cannot silently shrink the migration case.
	descendants := 0
	for id := 1; id <= BeadCount; id++ {
		for cur, seen := id, 0; cur != 0; seen++ {
			if seen > MaxGraphDepth {
				t.Fatalf("id %d exceeded depth bound while walking parents", id)
			}
			if cur == 2 {
				if id != 2 {
					descendants++
				}
				break
			}
			cur, _ = parentAndDepth(cur)
		}
	}
	if got, want := descendants, ReparentSize; got != want {
		t.Fatalf("epic 2 descendants = %d, want %d", got, want)
	}
}

func graphDepth(id int) int {
	depth := 0
	for id != 1 {
		parent, _ := parentAndDepth(id)
		id = parent
		depth++
	}
	return depth
}

func TestSparseGrantVisibilityMatchesFraction(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLite("file:authz-schema-test?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, tc := range []struct {
		fraction float64
		visible  int
		grants   int
	}{
		{fraction: 1, visible: BeadCount, grants: 1},
		{fraction: .01, visible: 1_000, grants: 1_000},
		{fraction: .001, visible: 100, grants: 100},
	} {
		t.Run(fractionName(tc.fraction), func(t *testing.T) {
			if err := s.Reset(ctx); err != nil {
				t.Fatal(err)
			}
			if err := s.Seed(ctx, tc.fraction); err != nil {
				t.Fatal(err)
			}
			var grants, visible, maxBeadDepth, maxClosureDepth int
			if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM plugin_grant WHERE subject='principal:sample'`).Scan(&grants); err != nil {
				t.Fatal(err)
			}
			if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(DISTINCT c.descendant) FROM plugin_grant g JOIN authz_cut_closure c ON c.ancestor=g.scope WHERE g.subject='principal:sample' AND g.role='reader'`).Scan(&visible); err != nil {
				t.Fatal(err)
			}
			if err := s.DB.QueryRowContext(ctx, `SELECT MAX(depth) FROM bead`).Scan(&maxBeadDepth); err != nil {
				t.Fatal(err)
			}
			if err := s.DB.QueryRowContext(ctx, `SELECT MAX(depth) FROM authz_closure`).Scan(&maxClosureDepth); err != nil {
				t.Fatal(err)
			}
			if grants != tc.grants {
				t.Errorf("grant rows = %d, want %d", grants, tc.grants)
			}
			if visible != tc.visible {
				t.Errorf("visible rows = %d, want %d", visible, tc.visible)
			}
			if maxBeadDepth != MaxGraphDepth || maxClosureDepth != MaxGraphDepth {
				t.Errorf("depth maxima bead=%d closure=%d, want %d/%d", maxBeadDepth, maxClosureDepth, MaxGraphDepth, MaxGraphDepth)
			}
		})
	}
}

func fractionName(fraction float64) string {
	switch fraction {
	case 1:
		return "100-percent"
	case .01:
		return "1-percent"
	case .001:
		return "0.1-percent"
	default:
		return "fraction"
	}
}
