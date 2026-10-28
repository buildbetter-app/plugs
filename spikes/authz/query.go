package authz

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"
)

type QueryKind string

const (
	Q4Ready QueryKind = "Q4 bead.ready"
	Q5Claim QueryKind = "Q5 bead.claimNext"
	Q6Count QueryKind = "Q6 view.run grouped counts"
)

type Plan string

const (
	PlanA Plan = "A scope-first"
	PlanB Plan = "B index-walk"
)

type BenchCase struct {
	Engine      Dialect
	Plan        Plan
	Query       QueryKind
	Fraction    float64
	Stats       Stats
	RowsTouched int
}
type Stats struct {
	P50, P95, Min, Max float64
	Samples            int
}

func (s *Store) queryPage(ctx context.Context, plan Plan, subject string, limit int) (*sql.Rows, error) {
	var q string
	if plan == PlanA {
		q = `WITH visible AS MATERIALIZED (
			SELECT c.descendant FROM plugin_grant g JOIN authz_cut_closure c ON c.ancestor = g.scope
			WHERE g.subject = ? AND g.role = 'reader' GROUP BY c.descendant
		)
		SELECT b.id,b.sort_bucket,b.sort_rank,b.sort_created FROM bead b JOIN visible v ON v.descendant=b.id
		WHERE b.ready=1 AND (b.sort_bucket,b.sort_rank,b.sort_created,b.id)>(?,?,?,?)
		ORDER BY b.sort_bucket,b.sort_rank,b.sort_created,b.id LIMIT ?`
	} else {
		q = `SELECT b.id,b.sort_bucket,b.sort_rank,b.sort_created FROM bead b
		WHERE b.ready=1 AND (b.sort_bucket,b.sort_rank,b.sort_created,b.id)>(?,?,?,?)
		AND EXISTS (SELECT 1 FROM authz_cut_closure c JOIN plugin_grant g ON g.scope=c.ancestor
			WHERE c.descendant=b.id AND g.subject=? AND g.role='reader')
		ORDER BY b.sort_bucket,b.sort_rank,b.sort_created,b.id LIMIT ?`
	}
	q = s.bind(q)
	if plan == PlanA {
		return s.DB.QueryContext(ctx, q, subject, -1, 0, 0, 0, limit)
	}
	return s.DB.QueryContext(ctx, q, -1, 0, 0, 0, subject, limit)
}

func (s *Store) queryCount(ctx context.Context, plan Plan, subject string) (*sql.Rows, error) {
	var q string
	if plan == PlanA {
		q = `WITH visible AS MATERIALIZED (
			SELECT c.descendant FROM plugin_grant g JOIN authz_cut_closure c ON c.ancestor=g.scope
			WHERE g.subject=? AND g.role='reader' GROUP BY c.descendant
		)
		SELECT b.project_id,COUNT(*) FROM bead b JOIN visible v ON v.descendant=b.id
		WHERE b.ready=1 GROUP BY b.project_id ORDER BY b.project_id`
	} else {
		q = `SELECT b.project_id,COUNT(*) FROM bead b WHERE b.ready=1
		AND EXISTS (SELECT 1 FROM authz_cut_closure c JOIN plugin_grant g ON g.scope=c.ancestor
			WHERE c.descendant=b.id AND g.subject=? AND g.role='reader')
		GROUP BY b.project_id ORDER BY b.project_id`
	}
	return s.DB.QueryContext(ctx, s.bind(q), subject)
}

func (s *Store) claimNext(ctx context.Context, plan Plan, subject string) error {
	// The candidate query is Q4 with LIMIT 1. PostgreSQL locks that candidate;
	// SQLite's write transaction provides the equivalent serializable lock.
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var q string
	var args []any
	if plan == PlanA {
		q = `WITH visible AS MATERIALIZED (
			SELECT c.descendant FROM plugin_grant g JOIN authz_cut_closure c ON c.ancestor=g.scope
			WHERE g.subject=? AND g.role='reader' GROUP BY c.descendant
		)
		SELECT b.id FROM bead b JOIN visible v ON v.descendant=b.id
		WHERE b.ready=1 AND (b.sort_bucket,b.sort_rank,b.sort_created,b.id)>(?,?,?,?)
		ORDER BY b.sort_bucket,b.sort_rank,b.sort_created,b.id LIMIT 1`
		args = []any{subject, -1, 0, 0, 0}
	} else {
		q = `SELECT b.id FROM bead b WHERE b.ready=1 AND (b.sort_bucket,b.sort_rank,b.sort_created,b.id)>(?,?,?,?)
		AND EXISTS (SELECT 1 FROM authz_cut_closure c JOIN plugin_grant g ON g.scope=c.ancestor
			WHERE c.descendant=b.id AND g.subject=? AND g.role='reader')
		ORDER BY b.sort_bucket,b.sort_rank,b.sort_created,b.id LIMIT 1`
		args = []any{-1, 0, 0, 0, subject}
	}
	if s.Dialect == DialectPostgres {
		q += ` FOR UPDATE`
	}
	var id int
	err = tx.QueryRowContext(ctx, s.bind(q), args...).Scan(&id)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	// Re-check the permission on the locked candidate before claiming it.
	var ok int
	err = tx.QueryRowContext(ctx, s.bind(`SELECT 1 FROM bead b WHERE b.id=? AND b.ready=1 AND EXISTS (SELECT 1 FROM authz_cut_closure c JOIN plugin_grant g ON g.scope=c.ancestor WHERE c.descendant=b.id AND g.subject=? AND g.role='reader')`), id, subject).Scan(&ok)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, s.bind(`UPDATE bead SET claimed_by=? WHERE id=?`), subject, id)
	return err
}

func drain(rows *sql.Rows, cols ...any) error {
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(cols...); err != nil {
			return err
		}
	}
	return rows.Err()
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1)*p + 0.5)
	if i < 0 {
		i = 0
	}
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}
func summarize(samples []float64) Stats {
	if len(samples) == 0 {
		return Stats{}
	}
	x := append([]float64(nil), samples...)
	sort.Float64s(x)
	return Stats{P50: percentile(x, .50), P95: percentile(x, .95), Min: x[0], Max: x[len(x)-1], Samples: len(x)}
}

// Benchmark runs warmed-up SQL cases. Durations are milliseconds, and every
// row is fully consumed so timings include the complete result set.
func (s *Store) Benchmark(ctx context.Context, fraction float64, iterations int) ([]BenchCase, error) {
	cases := make([]BenchCase, 0, 6)
	subject := "principal:sample"
	for _, plan := range []Plan{PlanA, PlanB} {
		for _, kind := range []QueryKind{Q4Ready, Q5Claim, Q6Count} {
			vals := make([]float64, 0, iterations)
			for i := 0; i < iterations+3; i++ {
				start := time.Now()
				var err error
				switch kind {
				case Q4Ready:
					rows, e := s.queryPage(ctx, plan, subject, 50)
					err = e
					if err == nil {
						var id, bucket, rank, created int
						err = drain(rows, &id, &bucket, &rank, &created)
					}
				case Q5Claim:
					err = s.claimNext(ctx, plan, subject)
				case Q6Count:
					rows, e := s.queryCount(ctx, plan, subject)
					err = e
					if err == nil {
						var project, count int
						err = drain(rows, &project, &count)
					}
				}
				if err != nil {
					return nil, fmt.Errorf("%s plan %s: %w", kind, plan, err)
				}
				if i >= 3 {
					vals = append(vals, float64(time.Since(start).Microseconds())/1000)
				}
			}
			rowsTouched := int(float64(BeadCount)*fraction + 0.5)
			if kind == Q4Ready || kind == Q5Claim {
				rowsTouched = 50
			}
			if kind == Q6Count {
				rowsTouched = int(float64(BeadCount)*fraction + 0.5)
			}
			cases = append(cases, BenchCase{Engine: s.Dialect, Plan: plan, Query: kind, Fraction: fraction, Stats: summarize(vals), RowsTouched: rowsTouched})
		}
	}
	return cases, nil
}
