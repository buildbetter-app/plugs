package postgres

import (
	"context"
	"database/sql"
	"errors"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Only source tokens/digests cross this boundary, never record bodies. Every
// publication maintains these headers even when it invalidates acceleration.
func (r *reader) selectionSourceStates(ctx context.Context, keys [][]byte, at uint64, overlay bool) (map[string]selectionSourceHeader, error) {
	state := map[string]selectionSourceHeader{}
	if len(keys) == 0 {
		return state, nil
	}
	rows, err := r.sql.QueryContext(ctx, `SELECT key,revision::text,present,digest,kind FROM plug_postgres.selection_sources WHERE incarnation=$1 AND key=ANY($2::bytea[]) AND start_revision<=$3 AND (end_revision IS NULL OR end_revision>$3)`, r.meta.Incarnation, keys, decimal(at))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var key, digest []byte
		var h selectionSourceHeader
		if err = rows.Scan(&key, &h.Revision, &h.Present, &digest, &h.Kind); err != nil {
			break
		}
		h.Digest = string(digest)
		state[string(key)] = h
	}
	if err == nil {
		err = rows.Err()
	}
	_ = rows.Close()
	if err != nil {
		return nil, err
	}
	if overlay && r.writer != nil && at == r.at {
		t := r.writer
		for _, key := range keys {
			id := string(key)
			h, ok := state[id]
			if !ok {
				h.Revision = "0"
			}
			if record, staged := t.records[id]; staged {
				h.Present, h.Digest = record != nil, ""
				if record != nil {
					h.Digest = driver.SelectionDigest(record.Body)
				}
			}
			if s, staged := t.derived[id]; staged {
				h.Present, h.Digest = s.Present, ""
				if s.Present && s.Derived != nil {
					h.Digest = driver.SelectionDigest(s.Derived.Value)
				}
			}
			if _, staged := t.bumps[id]; staged {
				h.Present, h.Digest = true, ""
			}
			state[id] = h
		}
	}
	return state, nil
}

func (r *reader) selectionRequireSources(ctx context.Context, controls []driver.SelectionSource, at uint64, overlay bool) error {
	keys := make([][]byte, 0, len(controls))
	for _, s := range controls {
		id, err := keyID(s.Key)
		if err != nil {
			return err
		}
		keys = append(keys, []byte(id))
	}
	state, err := r.selectionSourceStates(ctx, keys, at, overlay)
	if err != nil {
		return err
	}
	for i, want := range controls {
		actual, ok := state[string(keys[i])]
		if !ok {
			actual.Revision = "0"
		}
		if actual.Revision != want.Revision.String() || actual.Present != want.Present || actual.Digest != want.Digest {
			return selectionUnsupported("sealed authority source changed")
		}
	}
	return nil
}

// Source domains are independent. Traverse every AST branch, but never make
// a Parent-only predicate depend on Links or a source-free predicate on either.
func selectionSourceTables(e driver.SelectionExpr) []string {
	parent, links := false, false
	var visit func(driver.SelectionExpr)
	visit = func(e driver.SelectionExpr) {
		parent = parent || e.Op == driver.SelectionChildrenOp
		links = links || e.Op == driver.SelectionIncomingOp
		for _, child := range e.Args {
			visit(child)
		}
	}
	visit(e)
	tables := []string{}
	if parent {
		tables = append(tables, "hierarchy_index_coverage")
	}
	if links {
		tables = append(tables, "graph_link_index_coverage")
	}
	return tables
}

func (r *reader) selectionRequireGraphDomains(ctx context.Context, e driver.SelectionExpr, at uint64) error {
	for _, table := range selectionSourceTables(e) {
		var format int
		var first, through, state string
		err := r.sql.QueryRowContext(ctx, `SELECT format,built_from::text,certified_through::text,state FROM plug_postgres.`+table+` WHERE incarnation=$1 AND branch=0`, r.meta.Incarnation).Scan(&format, &first, &through, &state)
		if errors.Is(err, sql.ErrNoRows) {
			return selectionUnsupported("referenced graph domain unavailable")
		}
		if err != nil {
			return err
		}
		if format != 1 || state != "ready" {
			return selectionUnsupported("referenced graph domain unavailable")
		}
		from, err := parseCounter(first)
		if err != nil {
			return err
		}
		to, err := parseCounter(through)
		if err != nil {
			return err
		}
		if at < from || at > to {
			return selectionUnsupported("referenced graph revision unavailable")
		}
	}
	return nil
}

// Even empty/dead source predicates validate the entire current declaration
// overlay. Typed annotations cannot stand in for the independent graph domains.
func (r *reader) selectionRequireDeclarations(e driver.SelectionExpr, at uint64, overlay bool) error {
	if e.Sources != nil && overlay && r.writer != nil && at == r.at {
		for id, record := range r.writer.records {
			if record == nil || record.Kind != "bead" {
				continue
			}
			if declaration, ok := r.writer.sourceOverlay.Declarations[id]; !ok || e.Op == driver.SelectionIncomingOp && !declaration.Graph {
				return selectionUnsupported("staged graph declaration unavailable")
			}
		}
	}
	for _, child := range e.Args {
		if err := r.selectionRequireDeclarations(child, at, overlay); err != nil {
			return err
		}
	}
	return nil
}

// Reuse exact temporal parent/link metadata. No edge copy, source-body join,
// payload lookahead, sourceindex header-budget increase or per-resource query.
func (c *selectionCompiler) sourceExpression(e driver.SelectionExpr, alias, domain string) string {
	plane := c.planes[domain]
	refs := make([][]byte, 0, len(e.Sources.Refs))
	for _, ref := range e.Sources.Refs {
		refs = append(refs, []byte(selectionRefKey(ref)))
	}
	if len(refs) == 0 || e.Op == driver.SelectionIncomingOp && len(e.Sources.Kinds) == 0 {
		return "FALSE"
	}
	refSet, kindSet := map[string]bool{}, map[string]bool{}
	for _, ref := range refs {
		refSet[string(ref)] = true
	}
	for _, kind := range e.Sources.Kinds {
		kindSet[string(kind)] = true
	}
	match := map[string]bool{}
	shadow := [][]byte{}
	if plane.overlay && c.reader.writer != nil && plane.at == c.reader.at {
		t := c.reader.writer
		for id, record := range t.records {
			shadow = append(shadow, []byte(id))
			if record == nil || record.Kind != "bead" {
				continue
			}
			d := t.sourceOverlay.Declarations[id]
			if e.Op == driver.SelectionChildrenOp && refSet[d.ParentKey] {
				match[id] = true
			}
			if e.Op == driver.SelectionIncomingOp {
				for _, link := range d.Links {
					if refSet[selectionRefKey(link.To)] && kindSet[link.Kind] {
						match[id] = true
						break
					}
				}
			}
		}
	}
	key := alias + ".bead_key"
	scope := c.bind(c.reader.meta.Incarnation)
	var predicate string
	if e.Op == driver.SelectionChildrenOp {
		predicate = `EXISTS(SELECT 1 FROM plug_postgres.hierarchy_source_history h WHERE h.incarnation=` + scope + ` AND h.branch=0 AND h.child_key=` + key + ` AND h.parent_key=ANY(` + c.bind(refs) + `::bytea[]) AND ` + c.interval("h", plane.at) + `)`
	} else {
		kinds := make([][]byte, 0, len(e.Sources.Kinds))
		for _, kind := range e.Sources.Kinds {
			kinds = append(kinds, []byte(kind))
		}
		predicate = `EXISTS(SELECT 1 FROM plug_postgres.graph_link_history l JOIN plug_postgres.graph_link_source_history h ON h.incarnation=l.incarnation AND h.branch=l.branch AND h.source_key=l.source_key AND h.start_revision=l.start_revision WHERE l.incarnation=` + scope + ` AND l.branch=0 AND l.source_key=` + key + ` AND l.target_key=ANY(` + c.bind(refs) + `::bytea[]) AND l.link_kind=ANY(` + c.bind(kinds) + `::bytea[]) AND ` + c.interval("h", plane.at) + `)`
	}
	if len(shadow) > 0 {
		predicate = "(" + predicate + " AND NOT(" + key + "=ANY(" + c.bind(shadow) + "::bytea[])))"
	}
	if len(match) > 0 {
		keys := make([][]byte, 0, len(match))
		for id := range match {
			keys = append(keys, []byte(id))
		}
		predicate = "(" + predicate + " OR " + key + "=ANY(" + c.bind(keys) + "::bytea[]))"
	}
	// Current authority is optional for historical candidates. A staged match
	// relation must still yield false, rather than SQL NULL, for a deleted key.
	return "coalesce(" + predicate + ",FALSE)"
}
