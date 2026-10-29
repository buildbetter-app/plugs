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

// SupportedPlans and SupportedQueries are the executable matrix dimensions.
// They are exported so the command's contract printer and the benchmark loop
// share the same values that the runner validates against the manifest.
func SupportedPlans() []Plan {
	return []Plan{PlanA, PlanB}
}

func SupportedQueries() []QueryKind {
	return []QueryKind{Q4Ready, Q5Claim, Q6Count}
}

type BenchCase struct {
	Engine              Dialect
	Plan                Plan
	Query               QueryKind
	Fraction            float64
	Stats               Stats
	VisibleRows         int
	QualifiedRows       int
	ReturnedRows        int
	CandidateRows       int
	CandidateRowsMethod string
	Claim               *ClaimEvidence `json:",omitempty"`
}

// ClaimEvidence records the transaction facts that make Q5 measurable.  The
// benchmark uses this instead of inferring a claim from its elapsed time:
// SQLite takes a write lock before selecting the candidate, while Postgres
// locks the selected row with FOR UPDATE.  Committed is set only after the
// transaction's commit succeeds.
type ClaimEvidence struct {
	CandidateID   int
	LockMode      string
	LockAcquired  bool
	RecheckPassed bool
	RowsAffected  int64
	Committed     bool
	// Benchmark fills these aggregate fields after all warmups and measured
	// samples finish. CandidateIDs contains the measured candidate IDs. Claims
	// are reset between samples, so an ID may repeat while each invocation
	// still starts with an unclaimed row.
	WarmupSamples        int   `json:"warmup_samples"`
	MeasuredSamples      int   `json:"measured_samples"`
	ResetSamples         int   `json:"reset_samples"`
	FreshCandidateCount  int   `json:"fresh_candidate_count"`
	CommittedSampleCount int   `json:"committed_sample_count"`
	RecheckSampleCount   int   `json:"recheck_sample_count"`
	CandidateIDs         []int `json:"candidate_ids"`
}

type claimRunner interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}
type Stats struct {
	P50, P95, Min, Max float64
	Samples            int
}

func (s *Store) queryPage(ctx context.Context, plan Plan, subject string, limit int) (*sql.Rows, error) {
	return s.queryPageMode(ctx, plan, subject, limit, false)
}

func (s *Store) queryClaimPage(ctx context.Context, plan Plan, subject string, limit int) (*sql.Rows, error) {
	return s.queryPageMode(ctx, plan, subject, limit, true)
}

func (s *Store) queryPageMode(ctx context.Context, plan Plan, subject string, limit int, unclaimedOnly bool) (*sql.Rows, error) {
	var q string
	ready := "b.ready=1"
	if unclaimedOnly {
		ready += " AND b.claimed_by IS NULL"
	}
	if plan == PlanA {
		q = fmt.Sprintf(`WITH visible AS MATERIALIZED (
			SELECT c.descendant FROM plugin_grant g JOIN authz_cut_closure c ON c.ancestor = g.scope
			WHERE g.subject = ? AND g.role = 'reader' GROUP BY c.descendant
		)
		SELECT b.id,b.sort_bucket,b.sort_rank,b.sort_created FROM bead b JOIN visible v ON v.descendant=b.id
		WHERE %s AND (b.sort_bucket,b.sort_rank,b.sort_created,b.id)>(?,?,?,?)
		ORDER BY b.sort_bucket,b.sort_rank,b.sort_created,b.id LIMIT ?`, ready)
	} else {
		q = fmt.Sprintf(`SELECT b.id,b.sort_bucket,b.sort_rank,b.sort_created FROM bead b
		WHERE %s AND (b.sort_bucket,b.sort_rank,b.sort_created,b.id)>(?,?,?,?)
		AND EXISTS (SELECT 1 FROM authz_cut_closure c JOIN plugin_grant g ON g.scope=c.ancestor
			WHERE c.descendant=b.id AND g.subject=? AND g.role='reader')
		ORDER BY b.sort_bucket,b.sort_rank,b.sort_created,b.id LIMIT ?`, ready)
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
	_, err := s.claimNextEvidence(ctx, plan, subject)
	return err
}

// claimNextEvidence performs Q5 as one committed claim transaction. SQLite's
// default BEGIN is deferred, so it does not establish a writer lock before
// the candidate read; use BEGIN IMMEDIATE on a pinned connection instead.
// Postgres uses a normal transaction and FOR UPDATE on the candidate row.
func (s *Store) claimNextEvidence(ctx context.Context, plan Plan, subject string) (evidence ClaimEvidence, err error) {
	evidence.LockMode = "FOR UPDATE"
	var runner claimRunner
	var finish func(bool) error
	if s.Dialect == DialectSQLite {
		conn, connErr := s.DB.Conn(ctx)
		if connErr != nil {
			return evidence, connErr
		}
		// Keep every statement on this connection: database/sql does not expose
		// an *sql.Tx for a transaction started with BEGIN IMMEDIATE.
		if _, connErr = conn.ExecContext(ctx, "BEGIN IMMEDIATE"); connErr != nil {
			_ = conn.Close()
			return evidence, connErr
		}
		evidence.LockMode = "BEGIN IMMEDIATE"
		evidence.LockAcquired = true
		runner = conn
		finish = func(commit bool) error {
			statement := "ROLLBACK"
			if commit {
				statement = "COMMIT"
			}
			_, finishErr := conn.ExecContext(ctx, statement)
			closeErr := conn.Close()
			if finishErr != nil {
				return finishErr
			}
			return closeErr
		}
	} else {
		tx, txErr := s.DB.BeginTx(ctx, nil)
		if txErr != nil {
			return evidence, txErr
		}
		runner = tx
		finish = func(commit bool) error {
			if commit {
				return tx.Commit()
			}
			return tx.Rollback()
		}
	}
	finished := false
	defer func() {
		if !finished {
			_ = finish(false)
		}
	}()

	var q string
	var args []any
	if plan == PlanA {
		q = `WITH visible AS MATERIALIZED (
			SELECT c.descendant FROM plugin_grant g JOIN authz_cut_closure c ON c.ancestor=g.scope
			WHERE g.subject=? AND g.role='reader' GROUP BY c.descendant
		)
		SELECT b.id FROM bead b JOIN visible v ON v.descendant=b.id
		WHERE b.ready=1 AND b.claimed_by IS NULL AND (b.sort_bucket,b.sort_rank,b.sort_created,b.id)>(?,?,?,?)
		ORDER BY b.sort_bucket,b.sort_rank,b.sort_created,b.id LIMIT 1`
		args = []any{subject, -1, 0, 0, 0}
	} else {
		q = `SELECT b.id FROM bead b WHERE b.ready=1 AND b.claimed_by IS NULL AND (b.sort_bucket,b.sort_rank,b.sort_created,b.id)>(?,?,?,?)
		AND EXISTS (SELECT 1 FROM authz_cut_closure c JOIN plugin_grant g ON g.scope=c.ancestor
			WHERE c.descendant=b.id AND g.subject=? AND g.role='reader')
		ORDER BY b.sort_bucket,b.sort_rank,b.sort_created,b.id LIMIT 1`
		args = []any{-1, 0, 0, 0, subject}
	}
	if s.Dialect == DialectPostgres {
		q += ` FOR UPDATE`
	}
	var id int
	err = runner.QueryRowContext(ctx, s.bind(q), args...).Scan(&id)
	if err == sql.ErrNoRows {
		if err = finish(true); err != nil {
			return evidence, err
		}
		finished = true
		evidence.Committed = true
		return evidence, nil
	}
	if err != nil {
		return evidence, err
	}
	evidence.CandidateID = id
	if s.Dialect == DialectPostgres {
		evidence.LockAcquired = true
	}
	// Re-check the permission on the locked candidate before claiming it.
	var ok int
	err = runner.QueryRowContext(ctx, s.bind(`SELECT 1 FROM bead b WHERE b.id=? AND b.ready=1 AND b.claimed_by IS NULL AND EXISTS (SELECT 1 FROM authz_cut_closure c JOIN plugin_grant g ON g.scope=c.ancestor WHERE c.descendant=b.id AND g.subject=? AND g.role='reader')`), id, subject).Scan(&ok)
	if err != nil {
		return evidence, err
	}
	evidence.RecheckPassed = ok == 1
	result, err := runner.ExecContext(ctx, s.bind(`UPDATE bead SET claimed_by=? WHERE id=? AND claimed_by IS NULL`), subject, id)
	if err != nil {
		return evidence, err
	}
	if evidence.RowsAffected, err = result.RowsAffected(); err != nil {
		return evidence, err
	}
	if evidence.RowsAffected != 1 {
		return evidence, fmt.Errorf("claim candidate %d was changed before update", id)
	}
	if err = finish(true); err != nil {
		return evidence, err
	}
	finished = true
	evidence.Committed = true
	return evidence, nil
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

// queryMeasurement records cardinalities from the result and authorization
// predicate itself. These values are measured with SQL after the warmup
// query; they are not estimates derived from the requested fraction. The
// candidate count is reported as semantic cardinality evidence. It is not
// called an executor counter because database/sql does not expose portable
// engine-level row-loop counters for both SQLite and Postgres.
type queryMeasurement struct {
	visible   int
	qualified int
	returned  int
	candidate int
	method    string
}

type sortKey struct {
	bucket, rank, created, id int
}

func (s *Store) countVisible(ctx context.Context, subject string) (int, error) {
	q := `SELECT COUNT(DISTINCT c.descendant)
		FROM plugin_grant g JOIN authz_cut_closure c ON c.ancestor=g.scope
		WHERE g.subject=? AND g.role='reader'`
	var n int64
	if err := s.DB.QueryRowContext(ctx, s.bind(q), subject).Scan(&n); err != nil {
		return 0, err
	}
	return checkedCount(n)
}

func (s *Store) countQualified(ctx context.Context, subject string, unclaimedOnly bool) (int, error) {
	ready := "b.ready=1"
	if unclaimedOnly {
		ready += " AND b.claimed_by IS NULL"
	}
	q := fmt.Sprintf(`WITH visible AS MATERIALIZED (
			SELECT c.descendant FROM plugin_grant g JOIN authz_cut_closure c ON c.ancestor=g.scope
			WHERE g.subject=? AND g.role='reader' GROUP BY c.descendant
		)
		SELECT COUNT(*) FROM bead b JOIN visible v ON v.descendant=b.id
		WHERE %s AND (b.sort_bucket,b.sort_rank,b.sort_created,b.id)>(?,?,?,?)`, ready)
	var n int64
	if err := s.DB.QueryRowContext(ctx, s.bind(q), subject, -1, 0, 0, 0).Scan(&n); err != nil {
		return 0, err
	}
	return checkedCount(n)
}

func (s *Store) countReady(ctx context.Context) (int, error) {
	var n int64
	if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM bead WHERE ready=1`).Scan(&n); err != nil {
		return 0, err
	}
	return checkedCount(n)
}

// resetClaims prepares an independent Q5 sample.  A claim is intentionally
// committed by claimNextEvidence; clearing the prior sample here keeps the
// next timed invocation from measuring a progressively exhausted queue.  The
// setup statement is outside the timed section.
func (s *Store) resetClaims(ctx context.Context) error {
	_, err := s.DB.ExecContext(ctx, `UPDATE bead SET claimed_by=NULL WHERE claimed_by IS NOT NULL`)
	return err
}

func (s *Store) resetClaim(ctx context.Context, id int) error {
	_, err := s.DB.ExecContext(ctx, s.bind(`UPDATE bead SET claimed_by=NULL WHERE id=?`), id)
	return err
}

// countIndexWalkCandidates measures the rows an ordered index walk must visit
// before it can return the page.  The count includes hidden ready rows before
// the last returned key, which is the work Plan B's EXISTS predicate has to
// perform.  If there is no result, the walk reaches the end of the ready set.
func (s *Store) countIndexWalkCandidates(ctx context.Context, last *sortKey, unclaimedOnly bool) (int, error) {
	ready := "ready=1"
	if unclaimedOnly {
		ready += " AND claimed_by IS NULL"
	}
	q := fmt.Sprintf(`SELECT COUNT(*) FROM bead WHERE %s
		AND (sort_bucket,sort_rank,sort_created,id)>(?,?,?,?)`, ready)
	args := []any{-1, 0, 0, 0}
	if last != nil {
		q += ` AND (sort_bucket,sort_rank,sort_created,id)<=(?,?,?,?)`
		args = append(args, last.bucket, last.rank, last.created, last.id)
	}
	var n int64
	if err := s.DB.QueryRowContext(ctx, s.bind(q), args...).Scan(&n); err != nil {
		return 0, err
	}
	return checkedCount(n)
}

func checkedCount(n int64) (int, error) {
	if n < 0 || n > int64(^uint(0)>>1) {
		return 0, fmt.Errorf("row count %d does not fit in int", n)
	}
	return int(n), nil
}

func (s *Store) measurePage(ctx context.Context, plan Plan, subject string, limit int, unclaimedOnly bool) (queryMeasurement, error) {
	visible, err := s.countVisible(ctx, subject)
	if err != nil {
		return queryMeasurement{}, err
	}
	qualified, err := s.countQualified(ctx, subject, unclaimedOnly)
	if err != nil {
		return queryMeasurement{}, err
	}
	var rows *sql.Rows
	if unclaimedOnly {
		rows, err = s.queryClaimPage(ctx, plan, subject, limit)
	} else {
		rows, err = s.queryPage(ctx, plan, subject, limit)
	}
	if err != nil {
		return queryMeasurement{}, err
	}
	defer rows.Close()
	measurement := queryMeasurement{visible: visible, qualified: qualified}
	var last sortKey
	for rows.Next() {
		if err := rows.Scan(&last.id, &last.bucket, &last.rank, &last.created); err != nil {
			return queryMeasurement{}, err
		}
		measurement.returned++
	}
	if err := rows.Err(); err != nil {
		return queryMeasurement{}, err
	}
	if plan == PlanA {
		// The MATERIALIZED visibility CTE is consumed and sorted before LIMIT.
		measurement.candidate = qualified
		measurement.method = "measured-candidate-count: visible ready rows"
	} else {
		var end *sortKey
		if measurement.returned != 0 {
			end = &last
		}
		measurement.candidate, err = s.countIndexWalkCandidates(ctx, end, unclaimedOnly)
		if err != nil {
			return queryMeasurement{}, err
		}
		measurement.method = "measured-candidate-count: ready rows through returned key"
	}
	return measurement, nil
}

func (s *Store) measureCount(ctx context.Context, plan Plan, subject string) (queryMeasurement, error) {
	visible, err := s.countVisible(ctx, subject)
	if err != nil {
		return queryMeasurement{}, err
	}
	qualified, err := s.countQualified(ctx, subject, false)
	if err != nil {
		return queryMeasurement{}, err
	}
	rows, err := s.queryCount(ctx, plan, subject)
	if err != nil {
		return queryMeasurement{}, err
	}
	defer rows.Close()
	measurement := queryMeasurement{visible: visible, qualified: qualified, candidate: qualified, method: "measured-candidate-count: visible ready rows"}
	for rows.Next() {
		var project, count int
		if err := rows.Scan(&project, &count); err != nil {
			return queryMeasurement{}, err
		}
		measurement.returned++
	}
	if err := rows.Err(); err != nil {
		return queryMeasurement{}, err
	}
	if plan == PlanB {
		// Grouped counts cannot stop after the first match: Plan B probes the
		// authorization predicate for every ready bead.
		measurement.candidate, err = s.countReady(ctx)
		if err != nil {
			return queryMeasurement{}, err
		}
		measurement.method = "measured-candidate-count: all ready rows"
	}
	return measurement, nil
}

// Benchmark runs warmed-up SQL cases. Durations are milliseconds, and every
// row is fully consumed so timings include the complete result set.
func (s *Store) Benchmark(ctx context.Context, fraction float64, iterations int) ([]BenchCase, error) {
	cases := make([]BenchCase, 0, 6)
	subject := "principal:sample"
	for _, plan := range SupportedPlans() {
		for _, kind := range SupportedQueries() {
			if kind == Q5Claim {
				if err := s.resetClaims(ctx); err != nil {
					return nil, fmt.Errorf("reset claims before plan %s: %w", plan, err)
				}
			}
			var measurement queryMeasurement
			var err error
			switch kind {
			case Q4Ready:
				measurement, err = s.measurePage(ctx, plan, subject, 50, false)
			case Q5Claim:
				measurement, err = s.measurePage(ctx, plan, subject, 1, true)
			case Q6Count:
				measurement, err = s.measureCount(ctx, plan, subject)
			}
			if err != nil {
				return nil, fmt.Errorf("measure %s plan %s: %w", kind, plan, err)
			}
			if kind == Q5Claim {
				// Every warmup and measured sample commits a real claim. Check
				// capacity before starting so a later sample cannot time the
				// committed no-op path after the visible candidates are exhausted.
				available, countErr := s.countQualified(ctx, subject, true)
				if countErr != nil {
					return nil, fmt.Errorf("count Q5 candidates plan %s: %w", plan, countErr)
				}
				want := iterations + WarmupSamples
				if available < want {
					return nil, fmt.Errorf("Q5 plan %s has %d claimable candidates, need %d for %d warmups and %d measured samples", plan, available, want, WarmupSamples, iterations)
				}
			}
			vals := make([]float64, 0, iterations)
			var claimEvidence *ClaimEvidence
			measuredCandidateIDs := make([]int, 0, iterations)
			resetSamples, committedSamples, recheckSamples := 0, 0, 0
			previousCandidate := 0
			for i := 0; i < iterations+WarmupSamples; i++ {
				if kind == Q5Claim {
					if previousCandidate != 0 {
						if err := s.resetClaim(ctx, previousCandidate); err != nil {
							return nil, fmt.Errorf("reset claim %d for sample %d plan %s: %w", previousCandidate, i, plan, err)
						}
					}
					resetSamples++
				}
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
					var evidence ClaimEvidence
					evidence, err = s.claimNextEvidence(ctx, plan, subject)
					if err == nil {
						if evidence.CandidateID == 0 {
							err = fmt.Errorf("no fresh unclaimed candidate")
						} else if !evidence.LockAcquired || !evidence.RecheckPassed || evidence.RowsAffected != 1 || !evidence.Committed {
							err = fmt.Errorf("candidate %d claim evidence is not a committed lock/recheck/update: %+v", evidence.CandidateID, evidence)
						} else {
							previousCandidate = evidence.CandidateID
							if i >= WarmupSamples && evidence.Committed {
								committedSamples++
							}
							if i >= WarmupSamples && evidence.RecheckPassed {
								recheckSamples++
							}
							if i >= WarmupSamples {
								measuredCandidateIDs = append(measuredCandidateIDs, evidence.CandidateID)
								if claimEvidence == nil {
									first := evidence
									claimEvidence = &first
								}
							}
						}
					}
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
				if i >= WarmupSamples {
					vals = append(vals, float64(time.Since(start).Microseconds())/1000)
				}
			}
			if kind == Q5Claim {
				if claimEvidence == nil && iterations > 0 {
					return nil, fmt.Errorf("Q5 plan %s produced no measured claim evidence", plan)
				}
				if claimEvidence != nil {
					claimEvidence.WarmupSamples = WarmupSamples
					claimEvidence.MeasuredSamples = iterations
					claimEvidence.ResetSamples = resetSamples
					claimEvidence.FreshCandidateCount = len(measuredCandidateIDs)
					claimEvidence.CommittedSampleCount = committedSamples
					claimEvidence.RecheckSampleCount = recheckSamples
					claimEvidence.CandidateIDs = measuredCandidateIDs
					if claimEvidence.FreshCandidateCount != iterations || claimEvidence.CommittedSampleCount != iterations || claimEvidence.RecheckSampleCount != iterations {
						return nil, fmt.Errorf("Q5 plan %s measured %d fresh committed rechecks, want %d", plan, claimEvidence.FreshCandidateCount, iterations)
					}
				}
			}
			cases = append(cases, BenchCase{
				Engine:              s.Dialect,
				Plan:                plan,
				Query:               kind,
				Fraction:            fraction,
				Stats:               summarize(vals),
				VisibleRows:         measurement.visible,
				QualifiedRows:       measurement.qualified,
				ReturnedRows:        measurement.returned,
				CandidateRows:       measurement.candidate,
				CandidateRowsMethod: measurement.method,
				Claim:               claimEvidence,
			})
		}
	}
	return cases, nil
}
