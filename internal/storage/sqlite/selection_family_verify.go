package sqlite

import (
	"context"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// The encoded LockRecord range is ["0:", "0;"). It is only a source-kind
// discriminator; SQL never parses or decodes the opaque lock key after it.
// A complete derived-source fact set and a known live nonbead record source
// are independent envelopes. A record-source row without any live registered
// owner is a genuine orphan.
func verifySQLiteSelectionOwnedOrphans(ctx context.Context, q queryer, incarnation, at string, family driver.SelectionProjectionFamily) error {
	var bad int
	if family.Kind == "" {
		err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM selection_bead_history b
			WHERE b.incarnation=? AND b.branch=? AND b.start_revision<=?
			AND (b.end_revision IS NULL OR b.end_revision>?)
			AND NOT EXISTS(SELECT 1 FROM records r JOIN objects o ON o.key=r.key
				WHERE r.key=b.record_key AND o.kind='bead'))`,
			incarnation, revKey(0), at, at).Scan(&bad)
		if err != nil {
			return err
		}
	} else {
		err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM selection_fact_sets f
			WHERE f.incarnation=? AND f.branch=? AND f.row_table=? AND f.kind=?
			AND f.source_key>='0:' AND f.source_key<'0;'
			AND f.start_revision<=? AND (f.end_revision IS NULL OR f.end_revision>?)
			AND NOT EXISTS(SELECT 1 FROM records r JOIN objects o ON o.key=r.key
				WHERE r.key=f.source_key))`,
			incarnation, revKey(0), string(family.Table), string(family.Kind), at, at).Scan(&bad)
		if err != nil {
			return err
		}
	}
	if bad != 0 {
		return fmt.Errorf("%w: SQLite selection %s/%s orphan source-built row", driver.ErrSelectionUnavailable, family.Table, family.Kind)
	}
	return nil
}

// The final census is bidirectional: every live source has exactly one
// complete row and no physical row is attributed to a deleted source. It is
// checked under the pinned Head before the final fenced marker transaction.
func verifySQLiteSelectionFamilyCensus(ctx context.Context, q queryer, incarnation, at string, family driver.SelectionProjectionFamily) error {
	if err := verifySQLiteSelectionOwnedOrphans(ctx, q, incarnation, at, family); err != nil {
		return err
	}
	var bad int
	if family.Kind == "" {
		err := q.QueryRowContext(ctx, `WITH live AS (
			SELECT r.key FROM records r JOIN objects o ON o.key=r.key WHERE o.kind='bead'),
			active AS (SELECT record_key FROM selection_bead_history WHERE incarnation=? AND branch=?
				AND start_revision<=? AND (end_revision IS NULL OR end_revision>?))
			SELECT EXISTS(SELECT 1 FROM live l WHERE (SELECT COUNT(*) FROM active a WHERE a.record_key=l.key)<>1
				UNION ALL SELECT 1 FROM active a WHERE NOT EXISTS(SELECT 1 FROM live l WHERE l.key=a.record_key))`,
			incarnation, revKey(0), at, at).Scan(&bad)
		if err != nil {
			return err
		}
	} else {
		// Ancestor closures are one logical source envelope spread across one
		// physical row per depth (or an equivalent multi-fact row).  The other
		// families require exactly one active row per source.
		countCheck := "<>1"
		if family.Kind == driver.SelectionAncestorFact {
			countCheck = "=0"
		}
		err := q.QueryRowContext(ctx, `WITH live AS (
			SELECT r.key FROM records r JOIN objects o ON o.key=r.key WHERE o.kind='bead'),
			active AS (SELECT f.source_key FROM selection_fact_sets f
				JOIN objects o ON o.key=f.source_key AND o.kind='bead'
				WHERE f.incarnation=? AND f.branch=? AND f.row_table=? AND f.kind=?
				AND f.source_key>='0:' AND f.source_key<'0;'
				AND f.start_revision<=? AND (f.end_revision IS NULL OR f.end_revision>?))
			SELECT EXISTS(SELECT 1 FROM live l WHERE (SELECT COUNT(*) FROM active a WHERE a.source_key=l.key)`+countCheck+`
				UNION ALL SELECT 1 FROM active a WHERE NOT EXISTS(SELECT 1 FROM live l WHERE l.key=a.source_key))`,
			incarnation, revKey(0), string(family.Table), string(family.Kind), at, at).Scan(&bad)
		if err != nil {
			return err
		}
	}
	if bad != 0 {
		return fmt.Errorf("%w: SQLite selection %s/%s source census incomplete", driver.ErrSelectionUnavailable, family.Table, family.Kind)
	}
	return nil
}

// A producer can update one ancestor's Parent without rewriting a descendant
// bead. Check the final typed closure against the final typed Parent relation,
// including self, every step, live targets, uniqueness and the root terminus.
// This is scalar SQL; storage never decodes source bodies or opaque row keys.
func verifySQLiteSelectionParentChains(ctx context.Context, q queryer, incarnation, at string) (bool, error) {
	var bad int
	err := q.QueryRowContext(ctx, `WITH
		beads AS (
			SELECT record_key,parent_key FROM selection_bead_history WHERE incarnation=? AND branch=?
			AND start_revision<=? AND (end_revision IS NULL OR end_revision>?)),
		sets AS (
			SELECT s.row_key,s.start_revision,s.source_key FROM selection_fact_sets s
			JOIN objects o ON o.key=s.source_key AND o.kind='bead'
			WHERE s.incarnation=? AND s.branch=? AND s.row_table=? AND s.kind='ancestor'
			AND s.source_key>='0:' AND s.source_key<'0;'
			AND s.start_revision<=? AND (s.end_revision IS NULL OR s.end_revision>?)),
		facts AS (
			SELECT s.row_key,s.start_revision,s.source_key,f.ref_key,f.target_key,f.depth
			FROM sets s JOIN selection_facts f ON f.incarnation=? AND f.branch=?
			AND f.row_key=s.row_key AND f.start_revision=s.start_revision AND f.kind='ancestor'),
		bad_rows AS (
			-- A source may own several physical rows, one for each closure
			-- depth, but it must own at least one complete row.
			SELECT 1 FROM beads b
				WHERE NOT EXISTS(SELECT 1 FROM sets s WHERE s.source_key=b.record_key)
			UNION ALL SELECT 1 FROM sets s
				WHERE NOT EXISTS(SELECT 1 FROM beads b WHERE b.record_key=s.source_key)
			-- Every physical row is one complete typed envelope.
			UNION ALL SELECT 1 FROM sets s
				WHERE (SELECT COUNT(*) FROM facts f WHERE f.row_key=s.row_key
					AND f.start_revision=s.start_revision)=0
			-- The self row is unique for every source, and every source/depth
			-- pair is unique across its physical row set.
			UNION ALL SELECT 1 FROM beads b
				WHERE (SELECT COUNT(*) FROM facts f WHERE f.source_key=b.record_key
					AND f.depth=0 AND f.ref_key=b.record_key AND f.target_key=b.record_key)<>1
			UNION ALL SELECT 1 FROM facts f
				GROUP BY f.source_key,f.depth HAVING COUNT(*)<>1
			-- Every typed target is live and points back to its source.
			UNION ALL SELECT 1 FROM facts f
				LEFT JOIN beads target ON target.record_key=f.target_key
				WHERE f.depth<0 OR f.ref_key IS NULL OR f.ref_key<>f.source_key
				OR target.record_key IS NULL
			-- Every non-root depth follows the current Parent image of the
			-- preceding target, across row boundaries.
			UNION ALL SELECT 1 FROM facts f
				LEFT JOIN facts prev ON prev.source_key=f.source_key AND prev.depth=f.depth-1
				LEFT JOIN beads previous_target ON previous_target.record_key=prev.target_key
				WHERE f.depth>0 AND (prev.source_key IS NULL OR previous_target.parent_key IS NULL
					OR previous_target.parent_key<>f.target_key)
			-- The last target must be a root; a non-root target requires a
			-- subsequent depth in the same source's closure.
			UNION ALL SELECT 1 FROM facts f
				JOIN beads target ON target.record_key=f.target_key
				WHERE NOT EXISTS(SELECT 1 FROM facts later WHERE later.source_key=f.source_key
					AND later.depth=f.depth+1) AND target.parent_key IS NOT NULL
		)
		SELECT EXISTS(SELECT 1 FROM bad_rows)`,
		incarnation, revKey(0), at, at,
		incarnation, revKey(0), string(driver.DerivedAncestorClosure), at, at,
		incarnation, revKey(0)).Scan(&bad)
	if err != nil {
		return false, err
	}
	return bad == 0, nil
}

// verifySQLiteSelectionParentChainsAffected proves only the current Parent
// components touched by a commit.  The prior ready interval already certified
// every component outside this recursive closure.  Seeds include changed bead
// sources and typed ancestor sources; expanding through hierarchy_source_current
// also catches descendants whose stored closure was not rewritten after a
// parent move.  The checks inside each affected component are the same closed
// typed proof as verifySQLiteSelectionParentChains.
func verifySQLiteSelectionParentChainsAffected(ctx context.Context, q queryer, incarnation, at string, seeds []string) (bool, error) {
	if len(seeds) == 0 {
		return true, nil
	}
	values := make([]string, len(seeds))
	args := make([]any, 0, len(seeds)+40)
	for i, seed := range seeds {
		values[i] = "(?)"
		args = append(args, seed)
	}
	// Keep the scope values as ordinary predicates instead of a correlated
	// one-row CTE. SQLite can then use the existing (incarnation,branch,key)
	// indexes for every affected lookup. Descendants are expanded only from the
	// original seeds; a second upward walk adds their required ancestors without
	// walking down again from a discovered parent into unrelated siblings.
	args = append(args, incarnation, revKey(0), incarnation, revKey(0), incarnation, revKey(0),
		incarnation, revKey(0), string(driver.DerivedAncestorClosure), string(driver.SelectionAncestorFact), at, at,
		incarnation, revKey(0), string(driver.SelectionAncestorFact),
		incarnation, revKey(0), at, at,
		incarnation, revKey(0), at, at,
		incarnation, revKey(0), at, at)
	var bad int
	err := q.QueryRowContext(ctx, `WITH RECURSIVE
		seed(record_key) AS (VALUES `+strings.Join(values, ",")+`),
		descendants(record_key) AS MATERIALIZED (
			SELECT record_key FROM seed
			UNION
			SELECT h.child_key FROM hierarchy_source_current h
			JOIN descendants d ON d.record_key=h.parent_key
			WHERE h.incarnation=? AND h.branch=?
		),
		ancestors(record_key) AS MATERIALIZED (
			SELECT record_key FROM descendants
			UNION
			SELECT h.parent_key FROM hierarchy_source_current h
			JOIN ancestors a ON a.record_key=h.child_key
			WHERE h.incarnation=? AND h.branch=? AND h.parent_key IS NOT NULL
		),
		affected(record_key) AS MATERIALIZED (
			SELECT record_key FROM ancestors
		),
		beads(record_key,parent_key) AS MATERIALIZED (
			SELECT h.child_key AS record_key,h.parent_key
			FROM hierarchy_source_current h
			JOIN affected a ON a.record_key=h.child_key
			WHERE h.incarnation=? AND h.branch=?
		),
		sets(row_key,start_revision,source_key) AS MATERIALIZED (
			SELECT f.row_key,f.start_revision,f.source_key
			FROM selection_fact_sets f
			JOIN affected a ON a.record_key=f.source_key
			WHERE f.incarnation=? AND f.branch=?
			AND f.row_table=? AND f.kind=? AND f.start_revision<=?
			AND (f.end_revision IS NULL OR f.end_revision>?)
		),
		facts(row_key,start_revision,source_key,ref_key,target_key,depth) AS MATERIALIZED (
			SELECT s.row_key,s.start_revision,s.source_key,f.ref_key,f.target_key,f.depth
			FROM sets s CROSS JOIN selection_facts f
			WHERE f.incarnation=? AND f.branch=? AND f.row_key=s.row_key
			AND f.start_revision=s.start_revision AND f.kind=?
		),
		bad_rows AS (
			SELECT 1 FROM beads b
				WHERE (SELECT COUNT(*) FROM selection_bead_history h
					WHERE h.incarnation=? AND h.branch=?
					AND h.record_key=b.record_key AND h.start_revision<=?
					AND (h.end_revision IS NULL OR h.end_revision>?))<>1
			UNION ALL SELECT 1 FROM beads b
				WHERE EXISTS(SELECT 1 FROM selection_bead_history h
					WHERE h.incarnation=? AND h.branch=?
					AND h.record_key=b.record_key AND h.start_revision<=?
					AND (h.end_revision IS NULL OR h.end_revision>?)
					AND h.parent_key IS NOT b.parent_key)
			UNION ALL SELECT 1 FROM selection_bead_history h
				JOIN affected a ON a.record_key=h.record_key
				WHERE h.incarnation=? AND h.branch=?
				AND h.start_revision<=? AND (h.end_revision IS NULL OR h.end_revision>?)
				AND NOT EXISTS(SELECT 1 FROM beads b WHERE b.record_key=h.record_key)
			UNION ALL SELECT 1 FROM beads b
				WHERE NOT EXISTS(SELECT 1 FROM sets f WHERE f.source_key=b.record_key)
			UNION ALL SELECT 1 FROM sets f
				WHERE NOT EXISTS(SELECT 1 FROM beads b WHERE b.record_key=f.source_key)
			UNION ALL SELECT 1 FROM sets f
				WHERE (SELECT COUNT(*) FROM facts x WHERE x.row_key=f.row_key
					AND x.start_revision=f.start_revision)=0
			UNION ALL SELECT 1 FROM beads b
				WHERE (SELECT COUNT(*) FROM facts x WHERE x.source_key=b.record_key
					AND x.depth=0 AND x.ref_key=b.record_key AND x.target_key=b.record_key)<>1
			UNION ALL SELECT 1 FROM facts x
				GROUP BY x.source_key,x.depth HAVING COUNT(*)<>1
			UNION ALL SELECT 1 FROM facts x
				LEFT JOIN beads target ON target.record_key=x.target_key
				WHERE x.depth<0 OR x.ref_key IS NULL OR x.ref_key<>x.source_key OR target.record_key IS NULL
			UNION ALL SELECT 1 FROM facts x
				LEFT JOIN facts previous ON previous.source_key=x.source_key AND previous.depth=x.depth-1
				LEFT JOIN beads previous_target ON previous_target.record_key=previous.target_key
				WHERE x.depth>0 AND (previous.source_key IS NULL OR previous_target.parent_key IS NULL
					OR previous_target.parent_key<>x.target_key)
			UNION ALL SELECT 1 FROM facts x
				JOIN beads target ON target.record_key=x.target_key
				WHERE NOT EXISTS(SELECT 1 FROM facts later WHERE later.source_key=x.source_key
					AND later.depth=x.depth+1) AND target.parent_key IS NOT NULL
		)
		SELECT EXISTS(SELECT 1 FROM bad_rows)`,
		args...).Scan(&bad)
	if err != nil {
		return false, err
	}
	return bad == 0, nil
}
