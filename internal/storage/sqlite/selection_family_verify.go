package sqlite

import (
	"context"
	"fmt"

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
		err := q.QueryRowContext(ctx, `WITH live AS (
			SELECT r.key FROM records r JOIN objects o ON o.key=r.key WHERE o.kind='bead'),
			active AS (SELECT f.source_key FROM selection_fact_sets f
				JOIN objects o ON o.key=f.source_key AND o.kind='bead'
				WHERE f.incarnation=? AND f.branch=? AND f.row_table=? AND f.kind=?
				AND f.source_key>='0:' AND f.source_key<'0;'
				AND f.start_revision<=? AND (f.end_revision IS NULL OR f.end_revision>?))
			SELECT EXISTS(SELECT 1 FROM live l WHERE (SELECT COUNT(*) FROM active a WHERE a.source_key=l.key)<>1
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
			AND f.row_key=s.row_key AND f.start_revision=s.start_revision AND f.kind='ancestor')
		SELECT EXISTS(
			SELECT 1 FROM beads b WHERE (SELECT COUNT(*) FROM sets s WHERE s.source_key=b.record_key)<>1
			UNION ALL SELECT 1 FROM sets s WHERE NOT EXISTS(SELECT 1 FROM beads b WHERE b.record_key=s.source_key)
			UNION ALL SELECT 1 FROM sets s WHERE (SELECT COUNT(*) FROM facts f WHERE f.row_key=s.row_key
				AND f.start_revision=s.start_revision AND f.depth=0 AND f.ref_key=s.source_key
				AND f.target_key=s.source_key)<>1
			UNION ALL SELECT 1 FROM facts f GROUP BY f.row_key,f.start_revision,f.depth HAVING COUNT(*)<>1
			UNION ALL SELECT 1 FROM facts f LEFT JOIN beads target ON target.record_key=f.target_key
				LEFT JOIN facts prev ON prev.row_key=f.row_key AND prev.start_revision=f.start_revision AND prev.depth=f.depth-1
				LEFT JOIN beads previous_target ON previous_target.record_key=prev.target_key
				WHERE f.depth<0 OR f.ref_key IS NULL OR f.ref_key<>f.source_key OR target.record_key IS NULL
				OR (f.depth>0 AND (prev.row_key IS NULL OR previous_target.parent_key IS NULL
					OR previous_target.parent_key<>f.target_key))
				OR (NOT EXISTS(SELECT 1 FROM facts later WHERE later.row_key=f.row_key
					AND later.start_revision=f.start_revision AND later.depth=f.depth+1)
					AND target.parent_key IS NOT NULL)
		)`,
		incarnation, revKey(0), at, at,
		incarnation, revKey(0), string(driver.DerivedAncestorClosure), at, at,
		incarnation, revKey(0)).Scan(&bad)
	if err != nil {
		return false, err
	}
	return bad == 0, nil
}
