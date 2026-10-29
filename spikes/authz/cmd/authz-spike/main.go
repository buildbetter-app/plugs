package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/buildbetter-app/plugs/spikes/authz"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

type Output struct {
	GeneratedAt      string                `json:"generated_at"`
	N                int                   `json:"beads"`
	Depth            int                   `json:"depth"`
	PageP95TargetMS  float64               `json:"page_p95_target_ms"`
	CountP95TargetMS float64               `json:"count_p95_target_ms"`
	Cases            []authz.BenchCase     `json:"cases"`
	Closure          []authz.ClosureResult `json:"closure"`
}

type benchmarkContract struct {
	Schema    string          `json:"schema"`
	Module    string          `json:"module"`
	Matrix    matrixContract  `json:"matrix"`
	Dataset   datasetContract `json:"dataset"`
	TargetsMS targetsContract `json:"targets_ms"`
}

type matrixContract struct {
	Engines             []string          `json:"engines"`
	VisibilityFractions []float64         `json:"visibility_fractions"`
	Plans               []authz.Plan      `json:"plans"`
	Queries             []authz.QueryKind `json:"queries"`
	Warmups             int               `json:"warmups"`
	MeasuredIterations  int               `json:"measured_iterations"`
}

type datasetContract struct {
	Beads                     int `json:"beads"`
	MaximumGraphDepth         int `json:"maximum_graph_depth"`
	ClosureRewriteDescendants int `json:"closure_rewrite_descendants"`
	NormalWriteClosureLimit   int `json:"normal_write_closure_limit"`
}

type targetsContract struct {
	PageP95         float64 `json:"page_p95"`
	GroupedCountP95 float64 `json:"grouped_count_p95"`
}

func supportedContract() benchmarkContract {
	plans := authz.SupportedPlans()
	queries := authz.SupportedQueries()
	return benchmarkContract{
		Schema: "plug-authz-benchmark/v1",
		Module: "spikes/authz",
		Matrix: matrixContract{
			Engines:             authz.SupportedEngines(),
			VisibilityFractions: authz.SupportedVisibilityFractions(),
			Plans:               plans,
			Queries:             queries,
			Warmups:             authz.WarmupSamples,
			MeasuredIterations:  authz.DefaultMeasuredIterations,
		},
		Dataset: datasetContract{
			Beads:                     authz.BeadCount,
			MaximumGraphDepth:         authz.MaxGraphDepth,
			ClosureRewriteDescendants: authz.ReparentSize,
			NormalWriteClosureLimit:   authz.ClosureLimit,
		},
		TargetsMS: targetsContract{
			PageP95:         authz.PageP95TargetMS,
			GroupedCountP95: authz.CountP95TargetMS,
		},
	}
}

func main() {
	engine := flag.String("engine", "all", "sqlite, postgres, or all")
	dsn := flag.String("postgres-dsn", os.Getenv("AUTHZ_POSTGRES_DSN"), "Postgres DSN")
	iterations := flag.Int("iterations", authz.DefaultMeasuredIterations, "measured iterations after three warmups")
	output := flag.String("output", "", "optional JSON output path")
	printContract := flag.Bool("print-contract", false, "print the executable benchmark contract and exit")
	flag.Parse()
	if *printContract {
		b, err := json.Marshal(supportedContract())
		if err != nil {
			fatal(err)
		}
		fmt.Println(string(b))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	out := Output{GeneratedAt: time.Now().UTC().Format(time.RFC3339), N: authz.BeadCount, Depth: authz.MaxGraphDepth, PageP95TargetMS: authz.PageP95TargetMS, CountP95TargetMS: authz.CountP95TargetMS}
	run := func(name string, s *authz.Store) error {
		defer s.Close()
		for i, f := range authz.SupportedVisibilityFractions() {
			if i == 0 {
				if err := s.Reset(ctx); err != nil {
					return err
				}
				if err := s.Seed(ctx, f); err != nil {
					return err
				}
			} else if err := s.SetVisibleFraction(ctx, f); err != nil {
				return err
			}
			cases, err := s.Benchmark(ctx, f, *iterations)
			if err != nil {
				return err
			}
			out.Cases = append(out.Cases, cases...)
			fmt.Printf("%s fraction=%.3f\n", name, f)
			for _, c := range cases {
				fmt.Printf("  %s %s %-25s p50=%8.3fms p95=%8.3fms visible=%d qualified=%d returned=%d candidates=%d (%s)\n", c.Plan, c.Engine, c.Query, c.Stats.P50, c.Stats.P95, c.VisibleRows, c.QualifiedRows, c.ReturnedRows, c.CandidateRows, c.CandidateRowsMethod)
				if c.Claim != nil {
					fmt.Printf("    claim candidate=%d lock=%s acquired=%t recheck=%t affected=%d committed=%t\n", c.Claim.CandidateID, c.Claim.LockMode, c.Claim.LockAcquired, c.Claim.RecheckPassed, c.Claim.RowsAffected, c.Claim.Committed)
					fmt.Printf("    claim-samples measured=%d resets=%d fresh=%d committed=%d rechecked=%d\n", c.Claim.MeasuredSamples, c.Claim.ResetSamples, c.Claim.FreshCandidateCount, c.Claim.CommittedSampleCount, c.Claim.RecheckSampleCount)
				}
			}
		}
		// The graph/closures are unchanged by fraction sweeps; restore only the
		// grant rows before the closure migration checks instead of rebuilding
		// another 100,000-row closure fixture.
		if err := s.SetVisibleFraction(ctx, 1); err != nil {
			return err
		}
		if err := s.RefuseNormalClosureWrite(ctx, 2); err != nil {
			fmt.Printf("%s normal closure: %v\n", name, err)
		} else {
			return fmt.Errorf("normal closure unexpectedly accepted")
		}
		// 50011 is a shallow child of root 1, outside epic 2's subtree.  Using
		// an existing descendant here would create a cycle and make the closure
		// rebuild fail with a duplicate (descendant, ancestor) key.
		r, err := s.RewriteClosureMigration(ctx, 2, 50011, false)
		if err != nil {
			return err
		}
		fmt.Printf("%s %s: %.3fms rows=%d\n", name, r.Operation, r.DurationMS, r.Rows)
		out.Closure = append(out.Closure, r)
		r, err = s.RewriteClosureMigration(ctx, 2, 50011, true)
		if err != nil {
			return err
		}
		fmt.Printf("%s %s: %.3fms rows=%d\n", name, r.Operation, r.DurationMS, r.Rows)
		out.Closure = append(out.Closure, r)
		return nil
	}
	if *engine == "sqlite" || *engine == "all" {
		s, err := authz.OpenSQLite("file:authz-spike?mode=memory&cache=shared")
		if err != nil {
			fatal(err)
		}
		if err = run("sqlite", s); err != nil {
			fatal(err)
		}
	}
	if *engine == "postgres" || *engine == "all" {
		if *dsn == "" {
			fatal(fmt.Errorf("-postgres-dsn or AUTHZ_POSTGRES_DSN is required for postgres"))
		}
		s, err := authz.OpenPostgres(ctx, *dsn)
		if err != nil {
			fatal(err)
		}
		if err = run("postgres", s); err != nil {
			fatal(err)
		}
	}
	b, _ := json.MarshalIndent(out, "", "  ")
	if *output != "" {
		if err := os.WriteFile(*output, b, 0644); err != nil {
			fatal(err)
		}
	}
	fmt.Println(string(b))
}
func fatal(err error) { fmt.Fprintln(os.Stderr, "authz-spike:", err); os.Exit(1) }
