package authz

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type ClosureOperation string

const (
	ClosureReparent   ClosureOperation = "50,000-descendant reparent"
	ClosureRestricted ClosureOperation = "restricted toggle"
)

type ClosureResult struct {
	Operation      ClosureOperation
	DurationMS     float64
	Rows           int
	MigrationClass bool
	Refusal        string
}

// RefuseNormalClosureWrite implements P56's normal-write boundary. It reads
// the current closure cardinality and refuses before changing any row.
func (s *Store) RefuseNormalClosureWrite(ctx context.Context, epic int) error {
	var n int
	if err := s.DB.QueryRowContext(ctx, s.bind(`SELECT COUNT(*) FROM authz_closure WHERE ancestor=?`), epic).Scan(&n); err != nil {
		return err
	}
	if n > ClosureLimit {
		return fmt.Errorf("closure_too_large: %d rows exceeds normal-write limit %d", n, ClosureLimit)
	}
	return nil
}

// RewriteClosureMigration performs a closure rewrite in one transaction. A
// temporary target table makes the complete descendant set part of the same
// transaction and avoids constructing a 50k-element IN list.
func (s *Store) RewriteClosureMigration(ctx context.Context, epic, newParent int, toggleRestricted bool) (ClosureResult, error) {
	result := ClosureResult{Operation: ClosureReparent, MigrationClass: true}
	if toggleRestricted {
		result.Operation = ClosureRestricted
	}
	// Measure the complete migration-class transaction, including BEGIN,
	// target selection/setup, closure rebuild, and COMMIT. Starting the clock
	// after target preparation would report only the final rewrite suffix.
	start := time.Now()
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `DROP TABLE IF EXISTS rewrite_target`); err != nil {
		return result, err
	}
	if _, err = tx.ExecContext(ctx, `CREATE TEMP TABLE rewrite_target(id INTEGER PRIMARY KEY)`); err != nil {
		return result, err
	}
	if _, err = tx.ExecContext(ctx, s.bind(`INSERT INTO rewrite_target(id) SELECT descendant FROM authz_closure WHERE ancestor=?`), epic); err != nil {
		return result, err
	}
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM rewrite_target`).Scan(&result.Rows); err != nil {
		return result, err
	}
	var inTarget int
	if err = tx.QueryRowContext(ctx, s.bind(`SELECT COUNT(*) FROM rewrite_target WHERE id=?`), newParent).Scan(&inTarget); err != nil {
		return result, err
	}
	if inTarget != 0 {
		return result, fmt.Errorf("closure_reparent_cycle: new parent %d is a descendant of %d", newParent, epic)
	}
	if toggleRestricted {
		if _, err = tx.ExecContext(ctx, s.bind(`UPDATE bead SET restricted=? WHERE id=?`), 1, epic); err != nil {
			return result, err
		}
	} else {
		if _, err = tx.ExecContext(ctx, s.bind(`UPDATE bead SET parent_id=? WHERE id=?`), newParent, epic); err != nil {
			return result, err
		}
	}
	if err = rebuildTable(ctx, tx, s.Dialect, "authz_closure", false); err != nil {
		return result, err
	}
	if err = rebuildTable(ctx, tx, s.Dialect, "authz_cut_closure", true); err != nil {
		return result, err
	}
	if err = tx.Commit(); err != nil {
		return result, err
	}
	s.bumpAuthorizationRevision()
	result.DurationMS = float64(time.Since(start).Microseconds()) / 1000
	return result, nil
}

func rebuildTable(ctx context.Context, tx *sql.Tx, d Dialect, table string, stopAtRestricted bool) error {
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE descendant IN (SELECT id FROM rewrite_target)`, table)); err != nil {
		return err
	}
	// For a cut closure, include the nearest restricted ancestor and stop
	// walking above it. Carrying the stop bit in the recursive CTE avoids the
	// common mistake of filtering restricted ancestors out of the result.
	var q string
	if stopAtRestricted {
		q = fmt.Sprintf(`WITH RECURSIVE walk(descendant,ancestor,depth,stopped) AS (
			SELECT r.id,r.id,0,b.restricted FROM rewrite_target r JOIN bead b ON b.id=r.id
			UNION ALL
			SELECT w.descendant,p.id,w.depth+1,p.restricted
			FROM walk w JOIN bead b ON b.id=w.ancestor
			JOIN bead p ON p.id=b.parent_id
			WHERE b.parent_id<>0 AND w.stopped=0
		) INSERT INTO %s(descendant,ancestor,depth) SELECT descendant,ancestor,depth FROM walk`, table)
	} else {
		q = fmt.Sprintf(`WITH RECURSIVE walk(descendant,ancestor,depth) AS (
			SELECT id,id,0 FROM rewrite_target
			UNION ALL
			SELECT w.descendant,b.parent_id,w.depth+1 FROM walk w JOIN bead b ON b.id=w.ancestor
			WHERE b.parent_id<>0
		) INSERT INTO %s(descendant,ancestor,depth) SELECT descendant,ancestor,depth FROM walk`, table)
	}
	_, err := tx.ExecContext(ctx, q)
	return err
}
