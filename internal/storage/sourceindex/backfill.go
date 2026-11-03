package sourceindex

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Backfill must run in one caller-owned graph/Head-fenced SQL transaction. The
// only materialized source images/declarations belong to one bounded key page.
// Unknown history is discarded only in the unavailable index domain, never in
// records/versions or in a ready domain. Markers publish after the final page.
func (d Dialect) Backfill(ctx context.Context, q SQL, meta driver.Meta, decode driver.GraphSourceDecoder) error {
	if decode == nil {
		return fmt.Errorf("source index backfill requires a core decoder")
	}
	coverage, err := d.ReadCoverage(ctx, q, meta.Incarnation)
	if err != nil {
		return err
	}
	head, err := meta.Head.Uint64()
	if err != nil {
		return err
	}
	// Do not occupy a fresh native-restore target with index-only state. Its
	// first ordinary or native publication proves and activates coverage itself.
	if head == 0 {
		return nil
	}
	for _, domain := range []Coverage{coverage.Parent, coverage.Links} {
		if domain.Ready {
			if err := domain.Require(head); err != nil || domain.Through != head {
				return fmt.Errorf("%w: gapped source index backfill", driver.ErrUnsupported)
			}
		}
	}
	p := Plan{Dialect: d, Incarnation: meta.Incarnation, At: head, Parent: Coverage{First: head}, Links: Coverage{First: head}, ParentActive: !coverage.Parent.Ready, LinksActive: !coverage.Links.Ready, backfill: true}
	if !p.ParentActive && !p.LinksActive {
		return nil
	}
	tables := []string{}
	if p.ParentActive {
		tables = append(tables, "hierarchy_source_current", "hierarchy_source_history")
	}
	if p.LinksActive {
		tables = append(tables, "graph_link_current", "graph_link_history", "graph_link_source_current", "graph_link_source_history")
	}
	for _, table := range tables {
		b := bindings{d: d}
		where := b.scope(meta.Incarnation)
		if _, err := q.ExecContext(ctx, "DELETE FROM "+d.table(table)+" WHERE "+where, b.args...); err != nil {
			return err
		}
	}
	after := ""
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		records, err := d.sourcePage(ctx, q, after)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			break
		}
		p.Records = make(map[string]*driver.Record, len(records))
		p.Declarations = make(map[string]Declaration, len(records))
		for i := range records {
			record := &records[i]
			id := key(record.Ref)
			if id <= after {
				return fmt.Errorf("%w: unordered backfill source identity", driver.ErrUnsupported)
			}
			image := *record
			image.Body = bytes.Clone(record.Body)
			metadata, err := decode(image)
			if err != nil {
				return err
			}
			declaration, err := Declare(*record, metadata.Parent, metadata.Links, true)
			if err != nil {
				return err
			}
			p.Records[id], p.Declarations[id] = record, declaration
			after = id
		}
		if err := p.Publish(ctx, q); err != nil {
			return err
		}
	}
	if p.ParentActive {
		if err := p.marker(ctx, q, "hierarchy_index_coverage", p.Parent); err != nil {
			return err
		}
	}
	if p.LinksActive {
		return p.marker(ctx, q, "graph_link_index_coverage", p.Links)
	}
	return nil
}

// sourcePage decodes only the driver's Record envelope, never document JSON.
func (d Dialect) sourcePage(ctx context.Context, q SQL, after string) ([]driver.Record, error) {
	b := bindings{d: d}
	query := "SELECT r.key,r.data FROM records r JOIN objects o ON o.key=r.key WHERE o.kind='bead' AND r.key>" + b.add(d.key(after)) + " ORDER BY r.key LIMIT 128"
	if d == Postgres {
		b = bindings{d: d}
		query = "SELECT key,ref,kind,CAST(revision AS TEXT),body FROM plug_postgres.records WHERE kind='bead' AND NOT deleted AND key>" + b.add(d.key(after)) + " ORDER BY key LIMIT 128"
	}
	rows, err := q.QueryContext(ctx, query, b.args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]driver.Record, 0, publicationRows)
	for rows.Next() {
		var rawKey, raw, ref []byte
		var record driver.Record
		if d == SQLite {
			err = rows.Scan(&rawKey, &raw)
			if err == nil {
				err = json.Unmarshal(raw, &record)
			}
		} else {
			var revision string
			err = rows.Scan(&rawKey, &ref, &record.Kind, &revision, &record.Body)
			if err == nil {
				record.Ref = model.RefByID(string(ref), model.Natural64(0))
				record.Revision = model.Natural(revision)
				err = record.Revision.Validate()
			}
		}
		if err != nil {
			return nil, err
		}
		if record.Kind != "bead" || key(record.Ref) != string(rawKey) {
			return nil, fmt.Errorf("%w: backfill source envelope mismatch", driver.ErrUnsupported)
		}
		result = append(result, record)
	}
	return result, rows.Err()
}
