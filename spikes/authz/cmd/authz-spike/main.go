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

func main() {
	engine := flag.String("engine", "all", "sqlite, postgres, or all")
	dsn := flag.String("postgres-dsn", os.Getenv("AUTHZ_POSTGRES_DSN"), "Postgres DSN")
	iterations := flag.Int("iterations", 10, "measured iterations after three warmups")
	output := flag.String("output", "", "optional JSON output path")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	out := Output{GeneratedAt: time.Now().UTC().Format(time.RFC3339), N: authz.BeadCount, Depth: 10, PageP95TargetMS: 50, CountP95TargetMS: authz.CountP95TargetMS}
	run := func(name string, s *authz.Store) error {
		defer s.Close()
		for _, f := range []float64{1, .01, .001} {
			if err := s.Reset(ctx); err != nil {
				return err
			}
			if err := s.Seed(ctx, f); err != nil {
				return err
			}
			cases, err := s.Benchmark(ctx, f, *iterations)
			if err != nil {
				return err
			}
			out.Cases = append(out.Cases, cases...)
			fmt.Printf("%s fraction=%.3f\n", name, f)
			for _, c := range cases {
				fmt.Printf("  %s %s %-25s p50=%8.3fms p95=%8.3fms rows=%d\n", c.Plan, c.Engine, c.Query, c.Stats.P50, c.Stats.P95, c.RowsTouched)
			}
		}
		if err := s.Reset(ctx); err != nil {
			return err
		}
		if err := s.Seed(ctx, 1); err != nil {
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
