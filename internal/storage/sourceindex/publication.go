package sourceindex

import (
	"context"
	"fmt"
	"strings"
)

func (d Dialect) insertRows(ctx context.Context, q SQL, table string, columns []string, rows [][]any) error {
	for first := 0; first < len(rows); first += publicationRows {
		end := min(first+publicationRows, len(rows))
		b := bindings{d: d}
		values := make([]string, 0, end-first)
		for _, row := range rows[first:end] {
			if len(row) != len(columns) {
				return fmt.Errorf("source index publication column mismatch")
			}
			parameters := make([]string, len(row))
			for i, value := range row {
				parameters[i] = b.add(value)
			}
			values = append(values, "("+strings.Join(parameters, ",")+")")
		}
		if _, err := q.ExecContext(ctx, "INSERT INTO "+d.table(table)+" ("+strings.Join(columns, ",")+") VALUES "+strings.Join(values, ","), b.args...); err != nil {
			return err
		}
	}
	return nil
}

func (p Plan) closeRows(ctx context.Context, q SQL, current, history, column string, keys []string) error {
	for first := 0; first < len(keys); first += publicationRows {
		end := min(first+publicationRows, len(keys))
		b := bindings{d: p.Dialect}
		at := b.add(p.Dialect.revision(p.At))
		scope := b.scope(p.Incarnation)
		parameters := make([]string, 0, end-first)
		for _, id := range keys[first:end] {
			parameters = append(parameters, b.add(p.Dialect.key(id)))
		}
		if _, err := q.ExecContext(ctx, "UPDATE "+p.Dialect.table(history)+" SET end_revision="+at+" WHERE "+scope+" AND end_revision IS NULL AND "+column+" IN ("+strings.Join(parameters, ",")+")", b.args...); err != nil {
			return err
		}
		b = bindings{d: p.Dialect}
		scope = b.scope(p.Incarnation)
		parameters = parameters[:0]
		for _, id := range keys[first:end] {
			parameters = append(parameters, b.add(p.Dialect.key(id)))
		}
		if _, err := q.ExecContext(ctx, "DELETE FROM "+p.Dialect.table(current)+" WHERE "+scope+" AND "+column+" IN ("+strings.Join(parameters, ",")+")", b.args...); err != nil {
			return err
		}
	}
	return nil
}

func (p Plan) marker(ctx context.Context, q SQL, name string, coverage Coverage) error {
	b := bindings{d: p.Dialect}
	values := []any{p.Incarnation, p.Dialect.revision(0), 1, p.Dialect.revision(coverage.First), p.Dialect.revision(p.At), "ready"}
	args := make([]string, len(values))
	for i, value := range values {
		args[i] = b.add(value)
	}
	_, err := q.ExecContext(ctx, "INSERT INTO "+p.Dialect.table(name)+" (incarnation,branch,format,built_from,certified_through,state) VALUES ("+strings.Join(args, ",")+") ON CONFLICT(incarnation,branch) DO UPDATE SET format=excluded.format,built_from=excluded.built_from,certified_through=excluded.certified_through,state=excluded.state", b.args...)
	return err
}

func (p Plan) Publish(ctx context.Context, q SQL) error {
	keys := sortedRecordKeys(p.Records)
	d := p.Dialect
	branch, at := d.revision(0), d.revision(p.At)
	if p.ParentActive {
		if !p.empty {
			if err := p.closeRows(ctx, q, "hierarchy_source_current", "hierarchy_source_history", "child_key", keys); err != nil {
				return err
			}
		}
		current, history := make([][]any, 0), make([][]any, 0)
		for _, id := range keys {
			r := p.Records[id]
			declaration, typed := p.Declarations[id]
			if r == nil || !typed {
				continue
			}
			var parent any
			if declaration.ParentKey != "" {
				parent = d.key(declaration.ParentKey)
			}
			// Ref is the resolved ID's raw bytes, not JSON. Branch zero is scoped
			// separately, so opaque admitted identity bytes round-trip exactly.
			revision := at
			if p.backfill {
				value, err := r.Revision.Uint64()
				if err != nil || value > p.At {
					return fmt.Errorf("invalid backfill source revision")
				}
				revision = d.revision(value)
			}
			current = append(current, []any{p.Incarnation, branch, d.key(id), parent, []byte(r.Ref.ID), r.Kind, revision, r.Body})
			history = append(history, []any{p.Incarnation, branch, d.key(id), parent, at, nil, []byte(r.Ref.ID), r.Kind, revision, r.Body})
		}
		if err := d.insertRows(ctx, q, "hierarchy_source_current", []string{"incarnation", "branch", "child_key", "parent_key", "ref", "kind", "revision", "body"}, current); err != nil {
			return err
		}
		if err := d.insertRows(ctx, q, "hierarchy_source_history", []string{"incarnation", "branch", "child_key", "parent_key", "start_revision", "end_revision", "ref", "kind", "revision", "body"}, history); err != nil {
			return err
		}
		if !p.backfill {
			if err := p.marker(ctx, q, "hierarchy_index_coverage", p.Parent); err != nil {
				return err
			}
		}
	}
	if p.LinksActive {
		if !p.empty {
			if err := p.closeRows(ctx, q, "graph_link_source_current", "graph_link_source_history", "source_key", keys); err != nil {
				return err
			}
		}
		current, history, facts, historyFacts := make([][]any, 0), make([][]any, 0), make([][]any, 0), make([][]any, 0)
		for _, id := range keys {
			r := p.Records[id]
			declaration, typed := p.Declarations[id]
			if r == nil || !typed || !declaration.Graph {
				continue
			}
			revision := at
			if p.backfill {
				value, err := r.Revision.Uint64()
				if err != nil || value > p.At {
					return fmt.Errorf("invalid backfill source revision")
				}
				revision = d.revision(value)
			}
			current = append(current, []any{p.Incarnation, branch, d.key(id), []byte(r.Ref.ID), r.Kind, revision, r.Body})
			history = append(history, []any{p.Incarnation, branch, d.key(id), at, nil, []byte(r.Ref.ID), r.Kind, revision, r.Body})
			seen := make(map[Bucket]bool, len(declaration.Links))
			for _, link := range declaration.Links {
				bucket := Bucket{link.Kind, key(link.To)}
				if seen[bucket] {
					continue
				}
				seen[bucket] = true
				facts = append(facts, []any{p.Incarnation, branch, d.key(id), []byte(link.Kind), d.key(bucket.Target)})
				historyFacts = append(historyFacts, []any{p.Incarnation, branch, d.key(id), at, []byte(link.Kind), d.key(bucket.Target)})
			}
		}
		if err := d.insertRows(ctx, q, "graph_link_source_current", []string{"incarnation", "branch", "source_key", "ref", "kind", "revision", "body"}, current); err != nil {
			return err
		}
		if err := d.insertRows(ctx, q, "graph_link_source_history", []string{"incarnation", "branch", "source_key", "start_revision", "end_revision", "ref", "kind", "revision", "body"}, history); err != nil {
			return err
		}
		if err := d.insertRows(ctx, q, "graph_link_current", []string{"incarnation", "branch", "source_key", "link_kind", "target_key"}, facts); err != nil {
			return err
		}
		if err := d.insertRows(ctx, q, "graph_link_history", []string{"incarnation", "branch", "source_key", "start_revision", "link_kind", "target_key"}, historyFacts); err != nil {
			return err
		}
		if !p.backfill {
			if err := p.marker(ctx, q, "graph_link_index_coverage", p.Links); err != nil {
				return err
			}
		}
	}
	return nil
}
