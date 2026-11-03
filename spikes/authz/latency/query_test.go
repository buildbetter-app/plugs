package authz

import (
	"context"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestClaimNextSQLiteLocksRechecksAndCommits(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLite(filepath.Join(t.TempDir(), "claim.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`INSERT INTO bead(id,parent_id,depth,project_id,assignee,sort_bucket,sort_rank,sort_created) VALUES (1,0,0,1,0,0,1,1)`,
		`INSERT INTO bead(id,parent_id,depth,project_id,assignee,sort_bucket,sort_rank,sort_created) VALUES (2,1,1,1,0,0,2,2)`,
		`INSERT INTO authz_closure(descendant,ancestor,depth) VALUES (1,1,0)`,
		`INSERT INTO authz_closure(descendant,ancestor,depth) VALUES (2,2,0)`,
		`INSERT INTO authz_closure(descendant,ancestor,depth) VALUES (2,1,1)`,
		`INSERT INTO authz_cut_closure(descendant,ancestor,depth) VALUES (1,1,0)`,
		`INSERT INTO authz_cut_closure(descendant,ancestor,depth) VALUES (2,2,0)`,
		`INSERT INTO authz_cut_closure(descendant,ancestor,depth) VALUES (2,1,1)`,
		`INSERT INTO plugin_grant(subject,scope,role) VALUES ('principal:sample',1,'reader')`,
	} {
		if _, err := s.DB.ExecContext(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	first, err := s.claimNextEvidence(ctx, PlanA, "principal:sample")
	if err != nil {
		t.Fatal(err)
	}
	if first.LockMode != "BEGIN IMMEDIATE" || !first.LockAcquired || !first.RecheckPassed || first.RowsAffected != 1 || !first.Committed {
		t.Fatalf("first claim evidence = %+v, want immediate lock, recheck, one update, and commit", first)
	}
	var holder string
	if err := s.DB.QueryRowContext(ctx, `SELECT claimed_by FROM bead WHERE id=1`).Scan(&holder); err != nil {
		t.Fatal(err)
	}
	if holder != "principal:sample" {
		t.Fatalf("committed holder = %q, want principal:sample", holder)
	}

	second, err := s.claimNextEvidence(ctx, PlanB, "principal:sample")
	if err != nil {
		t.Fatal(err)
	}
	if second.CandidateID != 2 || !second.RecheckPassed || !second.Committed {
		t.Fatalf("second claim evidence = %+v, want the next unclaimed candidate and commit", second)
	}
	var claimed int
	if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM bead WHERE claimed_by='principal:sample'`).Scan(&claimed); err != nil {
		t.Fatal(err)
	}
	if claimed != 2 {
		t.Fatalf("claimed rows = %d, want 2", claimed)
	}

	last, err := s.claimNextEvidence(ctx, PlanA, "principal:sample")
	if err != nil {
		t.Fatal(err)
	}
	if last.CandidateID != 0 || !last.Committed {
		t.Fatalf("empty claim evidence = %+v, want a committed no-op", last)
	}
}

func TestBenchmarkQ5UsesFreshCommittedSamples(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLite(filepath.Join(t.TempDir(), "benchmark.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	// Keep this fixture small while leaving enough candidates for both plans'
	// three warmups and one measured sample. Benchmark resets claims outside
	// each timed invocation, so both plans exercise a committed claim on a
	// fresh unclaimed row.
	for id := 1; id <= 16; id++ {
		parent, depth := 0, 0
		if id > 1 {
			parent, depth = 1, 1
		}
		if _, err := s.DB.ExecContext(ctx, `INSERT INTO bead(id,parent_id,depth,restricted,project_id,assignee,ready,sort_bucket,sort_rank,sort_created) VALUES(?,?,?,?,?,?,?,?,?,?)`, id, parent, depth, 0, 1, 0, 1, id%3, id, id); err != nil {
			t.Fatal(err)
		}
		if _, err := s.DB.ExecContext(ctx, `INSERT INTO authz_closure(descendant,ancestor,depth) VALUES(?,?,?)`, id, id, 0); err != nil {
			t.Fatal(err)
		}
		if id > 1 {
			if _, err := s.DB.ExecContext(ctx, `INSERT INTO authz_closure(descendant,ancestor,depth) VALUES(?,?,?)`, id, 1, 1); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.DB.ExecContext(ctx, `INSERT INTO authz_cut_closure(descendant,ancestor,depth) VALUES(?,?,?)`, id, id, 0); err != nil {
			t.Fatal(err)
		}
		if id > 1 {
			if _, err := s.DB.ExecContext(ctx, `INSERT INTO authz_cut_closure(descendant,ancestor,depth) VALUES(?,?,?)`, id, 1, 1); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := s.DB.ExecContext(ctx, `INSERT INTO plugin_grant(subject,scope,role) VALUES(?,?,?)`, "principal:sample", 1, "reader"); err != nil {
		t.Fatal(err)
	}

	cases, err := s.Benchmark(ctx, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		if c.Query != Q5Claim {
			continue
		}
		if c.Claim == nil {
			t.Fatalf("%s Q5 claim evidence is nil", c.Plan)
		}
		if c.Claim.WarmupSamples != 3 || c.Claim.MeasuredSamples != 1 || c.Claim.ResetSamples != 4 {
			t.Fatalf("%s Q5 sample accounting = %+v, want 3 warmups, 1 measured, 4 resets", c.Plan, c.Claim)
		}
		if c.Claim.FreshCandidateCount != 1 || c.Claim.CommittedSampleCount != 1 || c.Claim.RecheckSampleCount != 1 || len(c.Claim.CandidateIDs) != 1 {
			t.Fatalf("%s Q5 evidence = %+v, want one fresh committed recheck", c.Plan, c.Claim)
		}
	}
}
