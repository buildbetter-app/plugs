package authz

import (
	"context"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// TestMeasuredCandidateCardinality is deliberately small: it proves that the
// benchmark's candidate evidence is read from the seeded SQL rows rather than
// being a fixed page-size or requested-fraction label. Plan A's materialized
// scope contains only the granted row; Plan B's ordered walk must pass the
// hidden rows before it reaches that row.
func TestMeasuredCandidateCardinality(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLite(filepath.Join(t.TempDir(), "measurement.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	for id := 1; id <= 4; id++ {
		if _, err := s.DB.ExecContext(ctx, `INSERT INTO bead(id,parent_id,depth,project_id,sort_bucket,sort_rank,sort_created) VALUES (?,?,?,?,?,?,?)`, id, 0, 0, 1, 0, id, id); err != nil {
			t.Fatal(err)
		}
		if _, err := s.DB.ExecContext(ctx, `INSERT INTO authz_cut_closure(descendant,ancestor,depth) VALUES (?,?,?)`, id, id, 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.DB.ExecContext(ctx, `INSERT INTO plugin_grant(subject,scope,role) VALUES (?,?,?)`, "principal:sample", 4, "reader"); err != nil {
		t.Fatal(err)
	}

	pageA, err := s.measurePage(ctx, PlanA, "principal:sample", 1, false)
	if err != nil {
		t.Fatal(err)
	}
	if pageA.visible != 1 || pageA.qualified != 1 || pageA.returned != 1 || pageA.candidate != 1 {
		t.Fatalf("Plan A page measurement = %+v, want one visible/qualified/returned/candidate row", pageA)
	}
	if pageA.method != "measured-candidate-count: visible ready rows" {
		t.Fatalf("Plan A candidate method = %q", pageA.method)
	}

	pageB, err := s.measurePage(ctx, PlanB, "principal:sample", 1, false)
	if err != nil {
		t.Fatal(err)
	}
	if pageB.visible != 1 || pageB.qualified != 1 || pageB.returned != 1 || pageB.candidate != 4 {
		t.Fatalf("Plan B page measurement = %+v, want one visible/qualified/returned and four walked rows", pageB)
	}
	if pageB.method != "measured-candidate-count: ready rows through returned key" {
		t.Fatalf("Plan B candidate method = %q", pageB.method)
	}

	countA, err := s.measureCount(ctx, PlanA, "principal:sample")
	if err != nil {
		t.Fatal(err)
	}
	countB, err := s.measureCount(ctx, PlanB, "principal:sample")
	if err != nil {
		t.Fatal(err)
	}
	if countA.candidate != 1 || countB.candidate != 4 {
		t.Fatalf("grouped-count candidate cardinalities A=%d B=%d, want 1/4", countA.candidate, countB.candidate)
	}
}
