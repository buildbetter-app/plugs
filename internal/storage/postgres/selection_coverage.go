package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func selectionFamilies() []driver.SelectionProjectionFamily {
	return []driver.SelectionProjectionFamily{
		{Table: driver.DerivedSortKey, Version: driver.SelectionIndexVersion},
		{Table: driver.DerivedAttribute, Kind: driver.SelectionLabelFact, Version: driver.SelectionIndexVersion},
		{Table: driver.DerivedReferenceIndex, Kind: driver.SelectionLinkFact, Version: driver.SelectionIndexVersion},
		{Table: driver.DerivedAncestorClosure, Kind: driver.SelectionAncestorFact, Version: driver.SelectionIndexVersion},
	}
}
func isSelectionUnavailable(err error) bool                 { return errors.Is(err, driver.ErrSelectionUnavailable) }
func selectionBeadFamily() driver.SelectionProjectionFamily { return selectionFamilies()[0] }
func selectionCoreFamily(table, kind string) (driver.SelectionProjectionFamily, bool) {
	for _, f := range selectionFamilies() {
		if string(f.Table) == table && string(f.Kind) == kind {
			return f, true
		}
	}
	return driver.SelectionProjectionFamily{}, false
}

func (r *reader) requireSelectionCoverage(ctx context.Context, family driver.SelectionProjectionFamily, at uint64) error {
	if err := family.Validate(); err != nil {
		return err
	}
	var first, through string
	err := r.sql.QueryRowContext(ctx, `SELECT built_from::text,certified_through::text FROM plug_postgres.selection_coverage WHERE incarnation=$1 AND branch=0 AND family_table=$2 AND fact_kind=$3 AND format=$4 AND ready AND built_from<=$5 AND certified_through>=$5 ORDER BY start_revision DESC LIMIT 1`, r.meta.Incarnation, string(family.Table), string(family.Kind), family.Version, decimal(at)).Scan(&first, &through)
	if errors.Is(err, sql.ErrNoRows) {
		return selectionUnsupported("family revision range unavailable")
	}
	if err != nil {
		return err
	}
	return (driver.SelectionCoverage{Family: family, Incarnation: r.meta.Incarnation, Branch: model.Natural64(0), First: model.Natural(first), Through: model.Natural(through)}).Require(family, r.meta.Incarnation, model.Natural64(0), model.Natural64(at))
}
func (r *reader) selectionCoverage(ctx context.Context, at uint64) (bool, error) {
	err := r.requireSelectionCoverage(ctx, selectionBeadFamily(), at)
	if errors.Is(err, driver.ErrSelectionUnavailable) {
		return false, nil
	}
	return err == nil, err
}
func (c *selectionCompiler) familyCoverageSQL(family driver.SelectionProjectionFamily, inc string, at uint64) string {
	rev := c.bind(decimal(at)) + "::numeric"
	return `EXISTS(SELECT 1 FROM plug_postgres.selection_coverage v WHERE v.incarnation=` + inc + ` AND v.branch=0 AND v.family_table=` + c.bind(string(family.Table)) + ` AND v.fact_kind=` + c.bind(string(family.Kind)) + ` AND v.format=` + c.bind(family.Version) + ` AND v.ready AND v.built_from<=` + rev + ` AND v.certified_through>=` + rev + `)`
}

// This proof is evaluated during publication/startup, never inferred from a
// selector's candidates. It reads only typed scalars and source headers. In a
// writer overlay, live identities and headers use the same precommit tokens as
// its typed annotations. No record body or opaque key is decoded by SQL.
func (r *reader) compileSelectionFamilyValidity(ctx context.Context, at uint64, overlay bool) (string, []any, error) {
	c := &selectionCompiler{reader: r, planes: map[string]selectionPlane{}, refs: map[string]string{}}
	if err := c.relation(r, "proof", at, overlay); err != nil {
		return "", nil, err
	}
	inc := c.bind(r.meta.Incarnation)
	shadow := [][]byte{}
	headers := []selectionSourceHeader{}
	live := []selectionSourceHeader{}
	if overlay && r.writer != nil && at == r.at {
		t := r.writer
		keys := [][]byte{}
		for id := range t.records {
			keys = append(keys, []byte(id))
		}
		for id := range t.derived {
			keys = append(keys, []byte(id))
		}
		for id := range t.bumps {
			keys = append(keys, []byte(id))
		}
		states, err := r.selectionSourceStates(ctx, keys, at, true)
		if err != nil {
			return "", nil, err
		}
		for id, h := range states {
			h.Key = selectionHex(id)
			h.Digest = selectionHex(h.Digest)
			if record, ok := t.records[id]; ok {
				if record != nil {
					h.Kind = record.Kind
				} else {
					h.Kind = t.identities[id].Kind
				}
			} else if _, ok := t.bumps[id]; ok {
				h.Kind = ""
			}
			headers = append(headers, h)
		}
		for id, record := range t.records {
			shadow = append(shadow, []byte(id))
			if record != nil && record.Kind == "bead" {
				h := states[id]
				h.Key = selectionHex(id)
				h.Digest = selectionHex(h.Digest)
				live = append(live, h)
			}
		}
	}
	headerShadow := [][]byte{}
	for _, h := range headers {
		headerShadow = append(headerShadow, []byte(selectionUnhex(h.Key)))
	}
	hs := `SELECT key,revision,present,digest,kind FROM plug_postgres.selection_sources h WHERE h.incarnation=` + inc + ` AND ` + c.interval("h", at)
	if len(headerShadow) > 0 {
		hs += ` AND NOT(h.key=ANY(` + c.bind(headerShadow) + `::bytea[]))`
	}
	if len(headers) > 0 {
		hs += ` UNION ALL SELECT decode(key,'hex'),revision::numeric,present,decode(digest,'hex'),kind FROM jsonb_to_recordset(` + c.bind(selectionJSON(headers)) + `::jsonb) AS h(key text,revision text,present boolean,digest text,kind text)`
	}
	ls := `SELECT key,revision FROM plug_postgres.records WHERE kind='bead' AND NOT deleted`
	if len(shadow) > 0 {
		ls += ` AND NOT(key=ANY(` + c.bind(shadow) + `::bytea[]))`
	}
	if len(live) > 0 {
		ls += ` UNION ALL SELECT decode(key,'hex'),revision::numeric FROM jsonb_to_recordset(` + c.bind(selectionJSON(live)) + `::jsonb) AS h(key text,revision text)`
	}
	// Inline the header relation so each source-key equality can use the
	// selection_sources primary key. Materializing all headers makes nested
	// joins repeatedly scan the complete domain, including unrelated sources.
	// The interval and staged-header union remain the same statement snapshot.
	// Live-source freshness joins likewise need the records primary key rather
	// than a full materialized live-domain rescan for every projection row.
	c.ctes = append(c.ctes, `proof_headers AS NOT MATERIALIZED (`+hs+`)`, `proof_live AS NOT MATERIALIZED (`+ls+`)`)
	// The relation includes ALL current closure rows, including untouched rows
	// after an ancestor move. CutClosure and derived-source domains are separate.
	c.ctes = append(c.ctes, `proof_beads AS MATERIALIZED (SELECT bead_key,parent_key,source_key FROM proof_rows WHERE family_table='sort_key' AND source_kind='record' AND bead_key IS NOT NULL)`,
		`proof_ancestors AS MATERIALIZED (SELECT r.source_key,f.ref_key,f.target_key,f.depth FROM proof_rows r JOIN proof_facts f ON f.row_key=r.row_key LEFT JOIN proof_headers h ON h.key=r.source_key WHERE (h.kind='bead' OR h.key IS NULL AND r.source_present) AND r.family_table='ancestor_closure' AND r.source_kind='record' AND r.fact_kind='ancestor')`)
	// Count the complete relation once, rather than running an aggregate for
	// each live source in every family while publication holds the lease fence.
	// Each family uses a full equality join so its materialized census cannot
	// be rescanned by a nested loop for every live source. Missing groups still
	// count as zero. Orphan groups already fail the independent freshness proof
	// below; retaining them here prevents reduction back to a left join. All
	// untouched rows and freshness/identity/parent-chain proofs remain.
	c.ctes = append(c.ctes, `proof_census AS MATERIALIZED (SELECT r.source_key,r.family_table,coalesce(r.fact_kind,'') fact_kind,count(*) n FROM proof_rows r LEFT JOIN proof_headers h ON h.key=r.source_key WHERE (h.kind='bead' OR h.key IS NULL AND r.source_present) AND r.source_kind='record' GROUP BY r.source_key,r.family_table,coalesce(r.fact_kind,''))`)
	chain := selectionParentChainSQL
	branches := []string{}
	for _, family := range selectionFamilies() {
		table, kind := c.bind(string(family.Table)), c.bind(string(family.Kind))
		rows := `SELECT r.* FROM proof_rows r LEFT JOIN proof_headers h ON h.key=r.source_key WHERE (h.kind='bead' OR h.key IS NULL AND r.source_present) AND source_kind='record' AND family_table=` + table + ` AND coalesce(fact_kind,'')=` + kind
		countCheck := `coalesce(census.n,0)=0`
		if family.Kind == "" {
			countCheck = `coalesce(census.n,0)<>1`
		}
		valid := `NOT EXISTS(SELECT 1 FROM proof_live l FULL JOIN (SELECT source_key,n FROM proof_census WHERE family_table=` + table + ` AND fact_kind=` + kind + `) census ON census.source_key=l.key WHERE l.key IS NULL OR ` + countCheck + `) AND NOT EXISTS(SELECT 1 FROM (` + rows + `) x LEFT JOIN proof_live l ON l.key=x.source_key LEFT JOIN proof_headers h ON h.key=x.source_key WHERE l.key IS NULL OR h.key IS NULL OR h.kind<>'bead' OR NOT h.present OR NOT x.source_present OR h.revision IS DISTINCT FROM x.source_revision OR l.revision IS DISTINCT FROM x.source_revision OR h.digest IS DISTINCT FROM x.source_digest)`
		if family.Kind == "" {
			valid += ` AND NOT EXISTS(SELECT 1 FROM (` + rows + `) x WHERE x.bead_key IS DISTINCT FROM x.source_key)`
		}
		if family.Table == driver.DerivedAncestorClosure {
			valid += ` AND (` + chain + `)`
		}
		branches = append(branches, `SELECT `+table+`::text,`+kind+`::text,(`+valid+`)`)
	}
	return `WITH ` + strings.Join(c.ctes, ",\n") + ` ` + strings.Join(branches, ` UNION ALL `), c.args, nil
}

func (r *reader) selectionFamilyValidity(ctx context.Context, at uint64, overlay bool) (map[driver.SelectionProjectionFamily]bool, error) {
	query, args, err := r.compileSelectionFamilyValidity(ctx, at, overlay)
	if err != nil {
		return nil, err
	}
	rows, err := r.sql.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[driver.SelectionProjectionFamily]bool{}
	for rows.Next() {
		var table, kind string
		var valid bool
		if err = rows.Scan(&table, &kind, &valid); err != nil {
			return nil, err
		}
		f, ok := selectionCoreFamily(table, kind)
		if !ok {
			return nil, selectionUnsupported("unexpected family proof")
		}
		result[f] = valid
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if len(result) != len(selectionFamilies()) {
		return nil, selectionUnsupported("incomplete family proof")
	}
	// Parent columns are authoritative only when the complete bead projection
	// binds every current source. A stale sort row cannot prove a new closure.
	ancestor := selectionFamilies()[3]
	result[ancestor] = result[ancestor] && result[selectionBeadFamily()]
	return result, nil
}

// Scalar equivalent of driver.ValidateSelectionParentChains. Every conjunct
// is consumed in the family proof, including on an empty candidate request.
const selectionParentChainSQL = `
 NOT EXISTS(SELECT 1 FROM proof_beads GROUP BY bead_key HAVING count(*)<>1) AND
 NOT EXISTS(SELECT 1 FROM proof_beads GROUP BY source_key HAVING count(*)<>1) AND
 NOT EXISTS(SELECT 1 FROM proof_ancestors a LEFT JOIN proof_beads owner ON owner.bead_key=a.ref_key WHERE owner.bead_key IS NULL OR a.source_key IS DISTINCT FROM owner.source_key) AND
 NOT EXISTS(SELECT 1 FROM proof_ancestors a LEFT JOIN proof_beads target ON target.bead_key=a.target_key WHERE target.bead_key IS NULL) AND
 NOT EXISTS(SELECT 1 FROM proof_ancestors GROUP BY ref_key,depth HAVING count(*)<>1) AND
 NOT EXISTS(SELECT 1 FROM proof_ancestors GROUP BY ref_key,target_key HAVING count(*)<>1) AND
 NOT EXISTS(SELECT 1 FROM proof_beads b WHERE NOT EXISTS(SELECT 1 FROM proof_ancestors a WHERE a.ref_key=b.bead_key AND a.depth=0 AND a.target_key=b.bead_key)) AND
 NOT EXISTS(SELECT 1 FROM proof_ancestors a WHERE a.depth<0 OR (a.depth=0 AND a.target_key IS DISTINCT FROM a.ref_key) OR (a.depth>0 AND NOT EXISTS(SELECT 1 FROM proof_ancestors previous WHERE previous.ref_key=a.ref_key AND previous.depth=a.depth-1))) AND
 NOT EXISTS(SELECT 1 FROM proof_ancestors a LEFT JOIN proof_beads target ON target.bead_key=a.target_key LEFT JOIN proof_ancestors next ON next.ref_key=a.ref_key AND next.depth=a.depth+1 WHERE next.target_key IS DISTINCT FROM target.parent_key)`

func (t *transaction) writeSelectionCoverage(ctx context.Context, revision model.Revision, incarnation string) error {
	view := &reader{sql: t.sql, meta: t.meta, at: mustRevision(revision)}
	view.meta.Incarnation = incarnation
	view.meta.Head = revision
	valid, err := view.selectionFamilyValidity(ctx, view.at, false)
	if err != nil {
		return err
	}
	affected := false
	for id, record := range t.records {
		affected = affected || record != nil && record.Kind == "bead" || t.identities[id].Kind == "bead"
	}
	for _, state := range t.derived {
		if !state.Present || state.Derived != nil && state.Derived.Index != nil {
			continue
		}
		for _, family := range selectionFamilies() {
			if state.Key.Derived != nil && state.Key.Derived.Table == family.Table {
				valid[family] = false
			}
		}
	}
	for _, family := range selectionFamilies() {
		if !valid[family] || t.restore != nil {
			continue
		}
		result, err := t.sql.ExecContext(ctx, `UPDATE plug_postgres.selection_coverage SET certified_through=$6 WHERE incarnation=$1 AND branch=0 AND family_table=$2 AND fact_kind=$3 AND format=$4 AND ready AND certified_through=$5`, incarnation, string(family.Table), string(family.Kind), family.Version, t.meta.Head.String(), revision.String())
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 && affected && t.selectionVirgin {
			_, err = t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.selection_coverage(incarnation,branch,family_table,fact_kind,format,start_revision,built_from,certified_through,ready) SELECT $1,0,$2,$3,$4,$5,$5,$5,true WHERE NOT EXISTS(SELECT 1 FROM plug_postgres.selection_coverage WHERE incarnation=$1 AND branch=0 AND family_table=$2 AND fact_kind=$3 AND format=$4)`, incarnation, string(family.Table), string(family.Kind), family.Version, revision.String())
			if err != nil {
				return err
			}
		}
	}
	if affected || t.restore != nil {
		_, err = t.sql.ExecContext(ctx, `UPDATE plug_postgres.meta SET selection_virgin=false WHERE singleton`)
	}
	return err
}
