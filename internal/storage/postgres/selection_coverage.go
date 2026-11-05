package postgres

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const selectionAllFamilies uint8 = 15

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
	return r.compileSelectionValidityForSources(ctx, at, overlay, nil, "")
}

// This complete-four-family entry point requires the complete predecessor
// certificate and impacted closure. Nil retains the full audit path.
func (r *reader) compileSelectionValidityForSources(ctx context.Context, at uint64, overlay bool, sources [][]byte, generation string) (string, []any, error) {
	return r.compileSelectionValiditySubset(ctx, at, overlay, sources, generation, selectionAllFamilies, selectionAllFamilies, nil)
}

// A partial predecessor supplies only its explicitly proven families. Every
// other family is still audited over the complete current relation in the
// same statement. A positive ancestor proof also requires the bead proof.
func (r *reader) compileSelectionCarriedValidity(ctx context.Context, at uint64, overlay bool, sources [][]byte, generation string, proven uint8) (string, []any, error) {
	if proven == 0 {
		return r.compileSelectionFamilyValidity(ctx, at, overlay)
	}
	query, args, err := r.compileSelectionValiditySubset(ctx, at, overlay, sources, generation, proven, proven, nil)
	if err != nil || proven == selectionAllFamilies {
		return query, args, err
	}
	fallback, args, err := r.compileSelectionValiditySubset(ctx, at, overlay, nil, generation, selectionAllFamilies&^proven, proven, args)
	if err != nil {
		return "", nil, err
	}
	return "SELECT * FROM (" + query + ") carried UNION ALL SELECT * FROM (" + fallback + ") audited", args, nil
}

func (r *reader) compileSelectionValiditySubset(ctx context.Context, at uint64, overlay bool, sources [][]byte, generation string, families, certified uint8, initialArgs []any) (string, []any, error) {
	c := &selectionCompiler{reader: r, planes: map[string]selectionPlane{}, refs: map[string]string{}, args: initialArgs}
	if err := c.relationForSources(r, "proof", at, overlay, sources); err != nil {
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
	censusLive := "proof_live"
	if sources != nil {
		// Only the complete live-source census needs a domain restriction.
		// Freshness/header lookups are already anchored to restricted proof_rows
		// by source-key equality. Repeating ANY(sources) inside their primary-key
		// probes makes PostgreSQL preprocess the whole array on every rescan.
		// Scope the live census, including its staged UNION arm, separately;
		// missing envelopes still fail for every affected old/new live source.
		c.ctes = append(c.ctes, `proof_scoped_live AS NOT MATERIALIZED (SELECT * FROM proof_live WHERE key=ANY(`+c.bind(sources)+`::bytea[]))`)
		censusLive = "proof_scoped_live"
	}
	// Mixed table statistics can otherwise turn the equality into a join
	// filter and materialize every header inside a nested loop. Keep the key
	// restriction inside a lateral lookup boundary, including for full audits.
	// OFFSET 0 preserves all matching historical/staged headers and duplicates.
	headerJoin := func(key string) string {
		return ` LEFT JOIN LATERAL (SELECT * FROM proof_headers WHERE key=` + key + ` OFFSET 0) h ON true`
	}
	// The relation includes ALL current closure rows, including untouched rows
	// after an ancestor move. CutClosure and derived-source domains are separate.
	c.ctes = append(c.ctes, `proof_beads AS MATERIALIZED (SELECT bead_key,parent_key,source_key FROM proof_rows WHERE family_table='sort_key' AND source_kind='record' AND bead_key IS NOT NULL)`,
		`proof_ancestors AS MATERIALIZED (SELECT r.source_key,f.ref_key,f.target_key,f.depth FROM proof_rows r JOIN proof_facts f ON f.row_key=r.row_key`+headerJoin("r.source_key")+` WHERE (h.kind='bead' OR h.key IS NULL AND r.source_present) AND r.family_table='ancestor_closure' AND r.source_kind='record' AND r.fact_kind='ancestor')`,
		selectionParentChainRelationSQL)
	// Count the complete relation once, rather than running an aggregate for
	// each live source in every family while publication holds the lease fence.
	// Each family uses a full equality join so its materialized census cannot
	// be rescanned by a nested loop for every live source. Missing groups still
	// count as zero. Orphan groups already fail the independent freshness proof
	// below; retaining them here prevents reduction back to a left join. All
	// untouched rows and freshness/identity/parent-chain proofs remain.
	censusFamilies := []string{}
	for i, family := range selectionFamilies() {
		if families&(1<<i) != 0 {
			censusFamilies = append(censusFamilies, `(r.family_table=`+c.bind(string(family.Table))+` AND coalesce(r.fact_kind,'')=`+c.bind(string(family.Kind))+`)`)
		}
	}
	c.ctes = append(c.ctes, `proof_census AS MATERIALIZED (SELECT r.source_key,r.family_table,coalesce(r.fact_kind,'') fact_kind,count(*) n FROM proof_rows r`+headerJoin("r.source_key")+` WHERE (h.kind='bead' OR h.key IS NULL AND r.source_present) AND r.source_kind='record' AND (`+strings.Join(censusFamilies, ` OR `)+`) GROUP BY r.source_key,r.family_table,coalesce(r.fact_kind,''))`)
	chain := selectionParentChainSQL
	if sources != nil {
		// Impacted chains still compare every target with its current parent,
		// including untouched ancestors. Only owners are restricted. Loading
		// just impacted beads would incorrectly reject an unaffected root or
		// accidentally stop checking edges which cross the scope boundary.
		if err := c.relation(r, "target", at, overlay); err != nil {
			return "", nil, err
		}
		c.ctes = append(c.ctes, `proof_target_beads AS MATERIALIZED (SELECT b.bead_key,b.parent_key FROM (SELECT DISTINCT target_key FROM proof_ancestors) needed JOIN LATERAL (SELECT bead_key,parent_key FROM target_rows WHERE family_table='sort_key' AND source_kind='record' AND bead_key=needed.target_key OFFSET 0) b ON true)`)
		chain = strings.ReplaceAll(chain, `SELECT bead_key FROM proof_beads)`, `SELECT bead_key FROM proof_target_beads)`)
		chain = strings.ReplaceAll(chain, `SELECT bead_key,parent_key FROM proof_beads)`, `SELECT bead_key,parent_key FROM proof_target_beads)`)
	}
	branches := []string{}
	for i, family := range selectionFamilies() {
		if families&(1<<i) == 0 {
			continue
		}
		table, kind := c.bind(string(family.Table)), c.bind(string(family.Kind))
		rows := `SELECT r.* FROM proof_rows r` + headerJoin("r.source_key") + ` WHERE (h.kind='bead' OR h.key IS NULL AND r.source_present) AND source_kind='record' AND family_table=` + table + ` AND coalesce(fact_kind,'')=` + kind
		countCheck := `coalesce(census.n,0)=0`
		if family.Kind == "" {
			countCheck = `coalesce(census.n,0)<>1`
		}
		valid := `NOT EXISTS(SELECT 1 FROM ` + censusLive + ` l FULL JOIN (SELECT source_key,n FROM proof_census WHERE family_table=` + table + ` AND fact_kind=` + kind + `) census ON census.source_key=l.key WHERE l.key IS NULL OR ` + countCheck + `) AND NOT EXISTS(SELECT 1 FROM (` + rows + `) x LEFT JOIN proof_live l ON l.key=x.source_key` + headerJoin("x.source_key") + ` WHERE l.key IS NULL OR h.key IS NULL OR h.kind<>'bead' OR NOT h.present OR NOT x.source_present OR h.revision IS DISTINCT FROM x.source_revision OR l.revision IS DISTINCT FROM x.source_revision OR h.digest IS DISTINCT FROM x.source_digest)`
		if family.Kind == "" {
			valid += ` AND NOT EXISTS(SELECT 1 FROM (` + rows + `) x WHERE x.bead_key IS DISTINCT FROM x.source_key)`
		}
		if family.Table == driver.DerivedAncestorClosure {
			valid += ` AND (` + chain + `)`
		}
		if families != selectionAllFamilies && sources == nil {
			// An entirely unbuilt family cannot cover a nonempty live domain.
			// Refuse it with indexed existence probes before constructing the
			// complete census. If any envelopes exist, retain the exact full
			// fallback: ordinary repairs can still make this family valid.
			valid = `CASE WHEN NOT EXISTS(SELECT 1 FROM (` + rows + `) any_row) AND EXISTS(SELECT 1 FROM proof_live) THEN false ELSE (` + valid + `) END`
		}
		if generation != "" {
			// Writers before Head use READ COMMITTED. Recheck the certificate
			// inside THIS statement, so a concurrent raw mutation cannot turn a
			// prior token observation into permission to skip untouched rows.
			valid = `(` + selectionFamilyCertificateSQL(c, r.meta.Head, certified) + ` AND EXISTS(SELECT 1 FROM plug_postgres.selection_mutation_fence WHERE singleton AND generation=` + c.bind(generation) + `::numeric)) AND (` + valid + `)`
		}
		branches = append(branches, `SELECT `+table+`::text,`+kind+`::text,(`+valid+`)`)
	}
	return `WITH ` + strings.Join(c.ctes, ",\n") + ` ` + strings.Join(branches, ` UNION ALL `), c.args, nil
}

func (r *reader) selectionFamilyValidity(ctx context.Context, at uint64, overlay bool) (map[driver.SelectionProjectionFamily]bool, error) {
	query, args, err := r.compileSelectionFamilyValidity(ctx, at, overlay)
	return r.readSelectionValidity(ctx, query, args, err)
}

func (r *reader) readSelectionValidity(ctx context.Context, query string, args []any, err error) (map[driver.SelectionProjectionFamily]bool, error) {
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

// Order each complete chain once. Independent uniqueness and depth checks below
// make adjacent rows equivalent to equality lookups at depth-1 and depth+1.
const selectionParentChainRelationSQL = `proof_chain AS MATERIALIZED (
 SELECT ref_key,target_key,depth,
 row_number() OVER parent_chain - 1 expected_depth,lead(target_key) OVER parent_chain next_target_key
 FROM proof_ancestors
 WINDOW parent_chain AS (PARTITION BY ref_key ORDER BY depth))`

// Scalar equivalent of driver.ValidateSelectionParentChains. Set differences
// avoid rescanning an unindexed materialized relation for every chain row.
// EXCEPT's NULL equality preserves IS DISTINCT FROM for nullable root parents.
// Every conjunct is consumed, including on an empty candidate request.
const selectionParentChainSQL = `
 NOT EXISTS(SELECT 1 FROM proof_beads GROUP BY bead_key HAVING count(*)<>1) AND
 NOT EXISTS(SELECT 1 FROM proof_beads GROUP BY source_key HAVING count(*)<>1) AND
 NOT EXISTS(SELECT ref_key,source_key FROM proof_ancestors EXCEPT SELECT bead_key,source_key FROM proof_beads) AND
 NOT EXISTS(SELECT target_key FROM proof_ancestors EXCEPT SELECT bead_key FROM proof_beads) AND
 NOT EXISTS(SELECT 1 FROM proof_ancestors GROUP BY ref_key,depth HAVING count(*)<>1) AND
 NOT EXISTS(SELECT 1 FROM proof_ancestors GROUP BY ref_key,target_key HAVING count(*)<>1) AND
 NOT EXISTS(SELECT bead_key FROM proof_beads EXCEPT SELECT ref_key FROM proof_ancestors WHERE depth=0 AND target_key=ref_key) AND
 NOT EXISTS(SELECT 1 FROM proof_chain WHERE depth IS DISTINCT FROM expected_depth OR (depth=0 AND target_key IS DISTINCT FROM ref_key)) AND
 NOT EXISTS(SELECT target_key,next_target_key FROM proof_chain EXCEPT SELECT bead_key,parent_key FROM proof_beads)`

func selectionCertificateSQL(c *selectionCompiler, head model.Revision) string {
	return selectionFamilyCertificateSQL(c, head, selectionAllFamilies)
}

func selectionFamilyCertificateSQL(c *selectionCompiler, head model.Revision, families uint8) string {
	return `EXISTS(SELECT 1 FROM plug_postgres.selection_mutation_fence WHERE singleton AND NOT untracked AND certified_generation=generation AND incarnation=` + c.bind(c.reader.meta.Incarnation) + ` AND head=` + c.bind(head.String()) + `::numeric AND format=` + c.bind(driver.SelectionIndexVersion) + ` AND (proven_families & ` + c.bind(int(families)) + `::integer)=` + c.bind(int(families)) + `::integer) AND NOT EXISTS(SELECT 1 FROM plug_postgres.selection_mutation_keys)`
}

func (r *reader) selectionFamilyCertificate(ctx context.Context) (uint8, string, error) {
	c := &selectionCompiler{reader: r}
	var families uint8
	var generation string
	err := r.sql.QueryRowContext(ctx, `SELECT CASE WHEN `+selectionFamilyCertificateSQL(c, r.meta.Head, 0)+` THEN proven_families ELSE 0 END,generation::text FROM plug_postgres.selection_mutation_fence WHERE singleton`, c.args...).Scan(&families, &generation)
	return families, generation, err
}

func (r *reader) selectionCertificateClean(ctx context.Context) (bool, error) {
	clean, _, err := r.selectionCertificate(ctx)
	return clean, err
}

func (r *reader) selectionCertificate(ctx context.Context) (bool, string, error) {
	c := &selectionCompiler{reader: r}
	var clean bool
	var generation string
	err := r.sql.QueryRowContext(ctx, `SELECT `+selectionCertificateSQL(c, r.meta.Head)+`,generation::text FROM plug_postgres.selection_mutation_fence WHERE singleton`, c.args...).Scan(&clean, &generation)
	return clean, generation, err
}

func (r *reader) selectionProofGeneration(ctx context.Context) (string, error) {
	var generation string
	err := r.sql.QueryRowContext(ctx, `SELECT generation::text FROM plug_postgres.selection_mutation_fence WHERE singleton`).Scan(&generation)
	return generation, err
}

// Only the known coverage statements may follow the completed proof. A data
// side effect during marker publication must abort, even if it changed a key
// already in the impacted domain. No post-proof mutation can be certified.
func (r *reader) requireSelectionProofGeneration(ctx context.Context, generation string, statements int) error {
	var unchanged bool
	err := r.sql.QueryRowContext(ctx, `SELECT generation=$1::numeric+$2::numeric FROM plug_postgres.selection_mutation_fence WHERE singleton`, generation, statements).Scan(&unchanged)
	if err == nil && !unchanged {
		return selectionUnsupported("mutation after family proof")
	}
	return err
}

func selectionSourceUnion(groups ...[][]byte) [][]byte {
	keys := map[string]bool{}
	for _, group := range groups {
		for _, key := range group {
			keys[string(key)] = true
		}
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	result := make([][]byte, 0, len(ordered)) // non-nil empty scope means no mutation
	for _, key := range ordered {
		result = append(result, []byte(key))
	}
	return result
}

// Include the old owner of removed/replaced rows, not merely the proposed
// annotation's owner. No key is parsed and no source body is reconstructed.
func (t *transaction) selectionMutationRoots(ctx context.Context) ([][]byte, error) {
	roots, rows := [][]byte{}, [][]byte{}
	for id := range t.records {
		roots = append(roots, []byte(id))
	}
	for id := range t.bumps {
		roots = append(roots, []byte(id))
	}
	for id, state := range t.derived {
		roots = append(roots, []byte(id))
		rows = append(rows, []byte(id))
		if state.Present && state.Derived != nil && state.Derived.Index != nil {
			key, err := keyID(state.Derived.Index.Source.Key)
			if err != nil {
				return nil, err
			}
			roots = append(roots, []byte(key))
		}
	}
	if len(rows) > 0 {
		old, err := t.sql.QueryContext(ctx, `SELECT source_key FROM plug_postgres.selection_rows WHERE incarnation=$1 AND row_key=ANY($2::bytea[]) AND start_revision<=$3 AND (end_revision IS NULL OR end_revision>$3)`, t.meta.Incarnation, rows, t.meta.Head.String())
		if err != nil {
			return nil, err
		}
		for old.Next() {
			var source []byte
			if err = old.Scan(&source); err != nil {
				break
			}
			roots = append(roots, source)
		}
		if err == nil {
			err = old.Err()
		}
		_ = old.Close()
		if err != nil {
			return nil, err
		}
	}
	return selectionSourceUnion(roots), nil
}

func (r *reader) compileSelectionImpacted(at uint64, overlay bool, roots [][]byte) (string, []any, error) {
	c := &selectionCompiler{reader: r}
	inc, domain := c.bind(r.meta.Incarnation), c.bind(roots)
	interval := c.interval("s", at)
	shadow := [][]byte{}
	staged := []struct{ Source, Target string }{}
	if overlay && r.writer != nil && at == r.at {
		for id, state := range r.writer.derived {
			shadow = append(shadow, []byte(id))
			if !state.Present || state.Derived == nil || state.Derived.Index == nil || state.Key.Derived.Table != driver.DerivedAncestorClosure {
				continue
			}
			x := state.Derived.Index
			if x.Source.Key.Kind != driver.LockRecord || x.Facts == nil || x.Facts.Kind != driver.SelectionAncestorFact {
				continue
			}
			source, err := keyID(x.Source.Key)
			if err != nil {
				return "", nil, err
			}
			for _, fact := range x.Facts.Rows {
				if fact.Target != nil {
					staged = append(staged, struct{ Source, Target string }{selectionHex(source), selectionHex(selectionRefKey(*fact.Target))})
				}
			}
		}
	}
	exclude := ""
	if len(shadow) > 0 {
		exclude = ` AND NOT(s.row_key=ANY(` + c.bind(shadow) + `::bytea[]))`
	}
	// Resolve each fact's complete physical owner identity before testing its
	// family, interval or staged shadow. Those predicates inside the lookup
	// let a family index scan unrelated rows and apply row_key as a filter.
	// OFFSET 0 keeps them outside the primary-key-only lookup boundary.
	owner := `SELECT p.source_key,p.row_key,p.start_revision,p.end_revision,p.layout,p.family_table,p.source_kind,p.fact_kind FROM plug_postgres.selection_rows p WHERE p.incarnation=f.incarnation AND p.row_key=f.row_key AND p.start_revision=f.start_revision OFFSET 0`
	query := `WITH roots AS MATERIALIZED (SELECT unnest(` + domain + `::bytea[]) key) SELECT DISTINCT source_key FROM (SELECT hit.source_key FROM roots changed JOIN LATERAL (SELECT s.source_key FROM plug_postgres.selection_facts f JOIN LATERAL (` + owner + `) s ON true WHERE f.incarnation=` + inc + ` AND f.target_key=changed.key AND s.layout=2 AND s.family_table='ancestor_closure' AND s.source_kind='record' AND s.fact_kind='ancestor' AND ` + interval + exclude + ` OFFSET 0) hit ON true`
	if len(staged) > 0 {
		query += ` UNION ALL SELECT decode(p."Source",'hex') FROM jsonb_to_recordset(` + c.bind(selectionJSON(staged)) + `::jsonb) AS p("Source" text,"Target" text) JOIN roots changed ON changed.key=decode(p."Target",'hex')`
	}
	query += `) impacted`
	return query, c.args, nil
}

// A complete prior closure supplies ALL old descendants. New descendants
// either were old descendants or cross a changed source: those sources are
// roots too. Union the new closure explicitly, without trusting it to replace
// the old one. Thus A<-B<-C; B->D still checks hidden unchanged C's entire
// chain against current parents. Deletion/recreation and owner moves use both
// domains, including the old owner of every deleted physical envelope.
func (r *reader) selectionImpacted(ctx context.Context, at uint64, overlay bool, roots [][]byte) ([][]byte, error) {
	if len(roots) == 0 {
		return [][]byte{}, nil
	}
	query, args, err := r.compileSelectionImpacted(at, overlay, roots)
	if err != nil {
		return nil, err
	}
	rows, err := r.sql.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	impacted := [][]byte{}
	for rows.Next() {
		var key []byte
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		impacted = append(impacted, key)
	}
	return selectionSourceUnion(roots, impacted), rows.Err()
}

func (t *transaction) prepareSelectionProof(ctx context.Context) error {
	var virginGeneration bool
	// The ordinary Head/lease fences precede this lock. BEFORE STATEMENT
	// mutation triggers serialize raw writers with it until commit/rollback.
	err := t.sql.QueryRowContext(ctx, `SELECT generation=0,CASE WHEN coalesce(NOT untracked AND certified_generation=generation AND incarnation=$1 AND head=$2 AND format=$3,false) AND NOT EXISTS(SELECT 1 FROM plug_postgres.selection_mutation_keys) THEN proven_families ELSE 0 END FROM plug_postgres.selection_mutation_fence WHERE singleton FOR UPDATE`, t.meta.Incarnation, t.meta.Head.String(), driver.SelectionIndexVersion).Scan(&virginGeneration, &t.selectionProofFamilies)
	if err != nil {
		return err
	}
	t.selectionProofCarry = t.selectionProofFamilies != 0 && t.restore == nil
	t.selectionProofSeed = virginGeneration && t.selectionVirgin && t.meta.Head.IsZero() && t.restore == nil
	if !t.selectionProofCarry && !t.selectionProofSeed {
		return nil // legacy, raw mutation, restore: original complete proof
	}
	t.selectionProofRoots, err = t.selectionMutationRoots(ctx)
	if err == nil && t.selectionProofCarry {
		t.selectionProofOld, err = t.reader.selectionFamilyImpacted(ctx, t.at, false, t.selectionProofRoots, t.selectionProofFamilies)
	}
	return err
}

// Only a positively proven ancestor family may supply reverse closure.
// The other families bind their own changed physical owners and do not depend
// on unrelated descendants. Unknown ancestry is always audited in full.
func (r *reader) selectionFamilyImpacted(ctx context.Context, at uint64, overlay bool, roots [][]byte, families uint8) ([][]byte, error) {
	if families&8 == 0 {
		return selectionSourceUnion(roots), nil
	}
	return r.selectionImpacted(ctx, at, overlay, roots)
}

func (r *reader) selectionAdmissionValidity(ctx context.Context, at uint64) (map[driver.SelectionProjectionFamily]bool, error) {
	families, generation, err := r.selectionFamilyCertificate(ctx)
	if err != nil {
		return nil, err
	}
	if families == 0 || r.writer == nil || at != r.at || r.writer.restore != nil {
		return r.selectionFamilyValidity(ctx, at, true)
	}
	roots, err := r.writer.selectionMutationRoots(ctx)
	if err != nil {
		return nil, err
	}
	old, err := r.selectionFamilyImpacted(ctx, at, false, roots, families)
	if err != nil {
		return nil, err
	}
	next, err := r.selectionFamilyImpacted(ctx, at, true, roots, families)
	if err != nil {
		return nil, err
	}
	query, args, err := r.compileSelectionCarriedValidity(ctx, at, true, selectionSourceUnion(old, next), generation, families)
	return r.readSelectionValidity(ctx, query, args, err)
}

// Only a trusted virgin publication, an already fenced predecessor, or the
// complete canonical startup builder may mint this token. Ordinary full thin
// proofs after an out-of-band mutation never launder an invalid token.
func (r *reader) certifySelectionProof(ctx context.Context, valid map[driver.SelectionProjectionFamily]bool) error {
	c := &selectionCompiler{reader: r}
	eligible := []string{}
	for i, family := range selectionFamilies() {
		if valid[family] {
			coverage := c.familyCoverageSQL(family, c.bind(r.meta.Incarnation), mustRevision(r.meta.Head))
			eligible = append(eligible, `(CASE WHEN `+coverage+` THEN `+decimal(uint64(1<<i))+` ELSE 0 END)`)
		}
	}
	if len(eligible) == 0 {
		return nil
	}
	// Each positive bit binds a complete proof of that family and its current
	// ready range. Clearing the journal grants nothing to unproven families:
	// their next admission/publication still uses a complete physical audit.
	mask := `(` + strings.Join(eligible, ` + `) + `)`
	if _, err := r.sql.ExecContext(ctx, `DELETE FROM plug_postgres.selection_mutation_keys WHERE `+mask+`<>0`, c.args...); err != nil {
		return err
	}
	_, err := r.sql.ExecContext(ctx, `UPDATE plug_postgres.selection_mutation_fence SET certified_generation=generation,proven_families=`+mask+`,untracked=false,incarnation=`+c.bind(r.meta.Incarnation)+`,head=`+c.bind(r.meta.Head.String())+`::numeric,format=`+c.bind(driver.SelectionIndexVersion)+` WHERE singleton AND `+mask+`<>0`, c.args...)
	return err
}

// Rank rows are outside the selection source domain only when their logical
// key did not own a typed row at the predecessor snapshot. Check the old
// interval even after publication closed it, including same-commit deletion.
func (t *transaction) disjointReadySortRows(ctx context.Context, incarnation string) (map[string]bool, error) {
	disjoint := map[string]bool{}
	keys := [][]byte{}
	for key, state := range t.derived {
		if !state.Present || state.Derived == nil || state.Derived.Index != nil || state.Key.Derived == nil || state.Key.Derived.Table != driver.DerivedSortKey {
			continue
		}
		id, ready := strings.CutPrefix(state.Key.Derived.Key, "ready/")
		if _, err := driver.NewBeadSentinel(id); !ready || err != nil {
			continue
		}
		disjoint[key] = true
		keys = append(keys, []byte(key))
	}
	if len(keys) == 0 {
		return disjoint, nil
	}
	rows, err := t.sql.QueryContext(ctx, `SELECT row_key FROM plug_postgres.selection_rows
	 WHERE incarnation=$1 AND row_key=ANY($2::bytea[]) AND start_revision<=$3::numeric
	 AND (end_revision IS NULL OR end_revision>$3::numeric)`, incarnation, keys, t.meta.Head.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var key []byte
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		delete(disjoint, string(key))
	}
	return disjoint, rows.Err()
}

func (t *transaction) writeSelectionCoverage(ctx context.Context, revision model.Revision, incarnation string) error {
	view := &reader{sql: t.sql, meta: t.meta, at: mustRevision(revision)}
	view.meta.Incarnation = incarnation
	view.meta.Head = revision
	var valid map[driver.SelectionProjectionFamily]bool
	var err error
	// The generation fence excludes concurrent transactions. Reconcile actual
	// row mutations too: an unexpected helper/trigger side effect cannot be
	// hidden among the generation increments of ordinary publication.
	tracked := false
	if t.selectionProofCarry || t.selectionProofSeed {
		err = t.sql.QueryRowContext(ctx, `SELECT NOT untracked AND NOT EXISTS(SELECT 1 FROM plug_postgres.selection_mutation_keys WHERE NOT(key=ANY($1::bytea[]))) FROM plug_postgres.selection_mutation_fence WHERE singleton`, selectionSourceUnion(t.selectionProofRoots, t.selectionProofOld)).Scan(&tracked)
		if err != nil {
			return err
		}
	}
	if t.selectionProofCarry && tracked {
		var next [][]byte
		next, err = view.selectionFamilyImpacted(ctx, view.at, false, t.selectionProofRoots, t.selectionProofFamilies)
		if err == nil {
			var query string
			var args []any
			query, args, err = view.compileSelectionCarriedValidity(ctx, view.at, false, selectionSourceUnion(t.selectionProofOld, next), "", t.selectionProofFamilies)
			valid, err = view.readSelectionValidity(ctx, query, args, err)
		}
	} else {
		valid, err = view.selectionFamilyValidity(ctx, view.at, false)
	}
	if err != nil {
		return err
	}
	affected := false
	for id, record := range t.records {
		affected = affected || record != nil && record.Kind == "bead" || t.identities[id].Kind == "bead"
	}
	disjoint, err := t.disjointReadySortRows(ctx, incarnation)
	if err != nil {
		return err
	}
	for key, state := range t.derived {
		if !state.Present || state.Derived != nil && state.Derived.Index != nil {
			continue
		}
		// The main writer's ready/<id> rank rows share this table, but do
		// not claim to be selection source images. Exclude only a valid,
		// independently owned key, preserving every false family proof.
		if disjoint[key] {
			continue
		}
		for _, family := range selectionFamilies() {
			if state.Key.Derived != nil && state.Key.Derived.Table == family.Table {
				valid[family] = false
			}
		}
	}
	generation, err := view.selectionProofGeneration(ctx)
	if err != nil {
		return err
	}
	statements := 0
	for _, family := range selectionFamilies() {
		if !valid[family] || t.restore != nil {
			continue
		}
		result, err := t.sql.ExecContext(ctx, `UPDATE plug_postgres.selection_coverage SET certified_through=$6 WHERE incarnation=$1 AND branch=0 AND family_table=$2 AND fact_kind=$3 AND format=$4 AND ready AND certified_through=$5`, incarnation, string(family.Table), string(family.Kind), family.Version, t.meta.Head.String(), revision.String())
		statements++
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		// A trusted first publication can contain only non-bead authority
		// facts. Its complete empty-domain proof needs a ready range before
		// Head advances, so later bead batches can carry those proven bits.
		// A source-free no-op retains the explicit startup availability floor.
		if n == 0 && (affected || t.selectionProofSeed && len(t.selectionProofRoots) != 0) && t.selectionVirgin {
			_, err = t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.selection_coverage(incarnation,branch,family_table,fact_kind,format,start_revision,built_from,certified_through,ready) SELECT $1,0,$2,$3,$4,$5,$5,$5,true WHERE NOT EXISTS(SELECT 1 FROM plug_postgres.selection_coverage WHERE incarnation=$1 AND branch=0 AND family_table=$2 AND fact_kind=$3 AND format=$4)`, incarnation, string(family.Table), string(family.Kind), family.Version, revision.String())
			statements++
			if err != nil {
				return err
			}
		}
	}
	if affected || t.restore != nil {
		_, err = t.sql.ExecContext(ctx, `UPDATE plug_postgres.meta SET selection_virgin=false WHERE singleton`)
	}
	if err == nil {
		err = view.requireSelectionProofGeneration(ctx, generation, statements)
	}
	if err == nil && tracked && (t.selectionProofCarry || t.selectionProofSeed) {
		err = view.certifySelectionProof(ctx, valid)
	}
	return err
}
