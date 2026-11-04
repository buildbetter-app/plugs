package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This operation intentionally does not call pinned Select or its multi-query
// preflight. Meta, complete-domain coverage, every control/fact source and the
// uncapped committed EXISTS share ONE fresh READ COMMITTED statement snapshot.
func (t *transaction) FreshSelectionExists(ctx context.Context, p driver.SelectionPlan) (bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return false, err
	}
	if !t.hasHead {
		return false, fmt.Errorf("%w: fresh existence requires Head", driver.ErrSelectionInvalid)
	}
	if p.Mode != driver.SelectionCountMode || p.Cap != 0 || p.Page != nil || p.GroupBy != "" || len(p.Excluded) != 0 || p.ContentRevision.String() != p.AuthorityRevision.String() {
		return false, fmt.Errorf("%w: fresh existence requires uncapped current count intent", driver.ErrSelectionInvalid)
	}
	if err := p.Validate(); err != nil {
		return false, err
	}
	if p.Incarnation != t.meta.Incarnation {
		return false, driver.ErrIncarnationChanged{From: p.Incarnation, To: t.meta.Incarnation}
	}
	if p.AuthorityRevision.String() != t.meta.Head.String() {
		return false, selectionUnsupported("fresh authority was not rebound to Head")
	}
	query, args, err := t.compileFreshSelection(p)
	if err != nil {
		return false, err
	}
	var incarnation, head, floor string
	var controls, coverage, facts, exists bool
	if err := t.sql.QueryRowContext(ctx, query, args...).Scan(&incarnation, &head, &floor, &controls, &coverage, &facts, &exists); err != nil {
		return false, err
	}
	if incarnation != p.Incarnation {
		return false, driver.ErrIncarnationChanged{From: p.Incarnation, To: incarnation}
	}
	floorValue, err := parseCounter(floor)
	if err != nil {
		return false, err
	}
	at, _ := p.ContentRevision.Uint64()
	if at < floorValue {
		return false, driver.ErrTruncated{Floor: model.Natural64(floorValue)}
	}
	if head != p.AuthorityRevision.String() || !controls || !coverage || !facts {
		return false, selectionUnsupported("fresh head, controls or complete coverage changed")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return exists, nil
}

type selectionExpectedSource struct {
	Key, Revision, Digest string
	Present               bool
}
type selectionExpectedFact struct{ Key, Kind, Table string }

func (t *transaction) compileFreshSelection(p driver.SelectionPlan) (string, []any, error) {
	c, err := t.reader.selectionCompiler(p, true)
	if err != nil {
		return "", nil, err
	}
	from, predicate, err := c.selectionMatch(p)
	if err != nil {
		return "", nil, err
	}
	boundary, err := p.Boundary.Description()
	if err != nil {
		return "", nil, err
	}
	inc := c.bind(p.Incarnation)
	at, _ := p.AuthorityRevision.Uint64()
	rev := c.bind(p.AuthorityRevision.String()) + "::numeric"
	sources := make([]selectionExpectedSource, 0, len(boundary.Sources))
	for _, source := range boundary.Sources {
		key, err := keyID(source.Key)
		if err != nil {
			return "", nil, err
		}
		sources = append(sources, selectionExpectedSource{Key: selectionHex(key), Revision: source.Revision.String(), Present: source.Present, Digest: selectionHex(source.Digest)})
	}
	controls := `NOT EXISTS(SELECT 1 FROM jsonb_to_recordset(` + c.bind(selectionJSON(sources)) + `::jsonb) AS wanted("Key" text,"Revision" text,"Digest" text,"Present" boolean) LEFT JOIN plug_postgres.selection_sources h ON h.incarnation=` + inc + ` AND h.key=decode(wanted."Key",'hex') AND ` + c.interval("h", at) + ` WHERE coalesce(h.revision,0)<>wanted."Revision"::numeric OR coalesce(h.present,false)<>wanted."Present" OR coalesce(h.digest,''::bytea)<>decode(wanted."Digest",'hex'))`
	coverage := []string{c.familyCoverageSQL(selectionBeadFamily(), inc, at)}
	factChecks := []string{}
	checks := []struct {
		expr   driver.SelectionExpr
		domain string
	}{{p.Membership, "content"}, {p.Authorization, "authority"}}
	if boundary.HostExpression != nil {
		checks = append(checks, struct {
			expr   driver.SelectionExpr
			domain string
		}{*boundary.HostExpression, "authority"})
	}
	seen := map[string]bool{}
	for _, check := range checks {
		factChecks = append(factChecks, c.freshFactCheck(check.expr, check.domain, inc, at))
		for _, table := range selectionSourceTables(check.expr) {
			if !seen[table] {
				seen[table] = true
				coverage = append(coverage, `EXISTS(SELECT 1 FROM plug_postgres.`+table+` s WHERE s.incarnation=`+inc+` AND s.branch=0 AND s.format=1 AND s.state='ready' AND s.built_from<=`+rev+` AND s.certified_through>=`+rev+`)`)
			}
		}
	}
	c.ctes = append(c.ctes, `selection_preflight AS MATERIALIZED (SELECT m.incarnation,m.head,m.history_floor,`+controls+` controls,(`+strings.Join(coverage, " AND ")+`) coverage,(`+strings.Join(factChecks, " AND ")+`) facts FROM plug_postgres.meta m)`)
	valid := `p.incarnation=` + inc + ` AND p.head=` + rev + ` AND p.history_floor<=` + rev + ` AND p.controls AND p.coverage AND p.facts`
	query := `WITH ` + strings.Join(c.ctes, ",\n") + ` SELECT p.incarnation,p.head::text,p.history_floor::text,p.controls,p.coverage,p.facts,CASE WHEN ` + valid + ` THEN EXISTS(SELECT 1 FROM ` + from + ` WHERE ` + predicate + `) ELSE FALSE END FROM selection_preflight p`
	return query, c.args, nil
}

// All AST branches contribute required fact domains, including NOT/false arms.
// Header validation is eager even when the uncapped candidate relation is empty.
func (c *selectionCompiler) freshFactCheck(e driver.SelectionExpr, domain, inc string, at uint64) string {
	wanted := []selectionExpectedFact{}
	var visit func(driver.SelectionExpr)
	visit = func(e driver.SelectionExpr) {
		if e.Exists != nil {
			key, _ := keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &e.Exists.Row})
			wanted = append(wanted, selectionExpectedFact{Key: selectionHex(key), Kind: string(e.Exists.Kind), Table: string(e.Exists.Row.Table)})
		}
		for _, child := range e.Args {
			visit(child)
		}
	}
	visit(e)
	if len(wanted) == 0 {
		return "TRUE"
	}
	checks := []string{}
	for _, f := range selectionFamilies()[1:] {
		checks = append(checks, `(x.source_kind='record' AND x.family_table=`+c.bind(string(f.Table))+` AND x.fact_kind=`+c.bind(string(f.Kind))+` AND NOT (`+c.familyCoverageSQL(f, inc, at)+`))`)
	}
	return `NOT EXISTS(SELECT 1 FROM jsonb_to_recordset(` + c.bind(selectionJSON(wanted)) + `::jsonb) AS wanted("Key" text,"Kind" text,"Table" text) LEFT JOIN ` + domain + `_rows x ON x.row_key=decode(wanted."Key",'hex') LEFT JOIN plug_postgres.selection_sources h ON h.incarnation=` + inc + ` AND h.key=x.source_key AND ` + c.interval("h", at) + ` WHERE x.row_key IS NULL OR x.fact_kind IS DISTINCT FROM wanted."Kind" OR x.family_table IS DISTINCT FROM wanted."Table" OR coalesce(h.revision,0)<>x.source_revision OR coalesce(h.present,false)<>x.source_present OR coalesce(h.digest,''::bytea)<>x.source_digest OR (` + strings.Join(checks, " OR ") + `))`
}

var _ driver.FreshSelectionExistsReader = (*transaction)(nil)
