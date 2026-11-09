package sourceindex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// BackfillPublication supplies the engine's fenced publication boundary. The
// graph and Head locks must remain held across ALL pages and final activation.
// SQLite uses separate short transactions so lease heartbeats can interleave;
// PostgreSQL can retain one transaction without locking the lease row yet.
type BackfillPublication func(context.Context, func(SQL) error) error

// PrepareBackfill only reads coverage. Unavailable domains remain unavailable
// while their rows are replaced; a ready domain and its history are never touched.
func (d Dialect) PrepareBackfill(ctx context.Context, q SQL, meta driver.Meta) (Plan, error) {
	coverage, err := d.ReadCoverage(ctx, q, meta.Incarnation)
	if err != nil {
		return Plan{}, err
	}
	head, err := meta.Head.Uint64()
	if err != nil {
		return Plan{}, err
	}
	p := Plan{Dialect: d, Incarnation: meta.Incarnation, At: head, Parent: Coverage{First: head}, Links: Coverage{First: head}, backfill: true}
	// Keep fresh native-restore targets durably empty.
	if head == 0 {
		return p, nil
	}
	for _, domain := range []Coverage{coverage.Parent, coverage.Links} {
		if domain.Ready {
			if err := domain.Require(head); err != nil || domain.Through != head {
				return Plan{}, fmt.Errorf("%w: gapped source index backfill", driver.ErrUnsupported)
			}
		}
	}
	p.ParentActive, p.LinksActive = !coverage.Parent.Ready, !coverage.Links.Ready
	return p, nil
}

// ClearBackfill removes only unavailable-domain rows. Bound even restart cleanup
// so a large interrupted build cannot starve SQLite lease renewal on deletion.
// Coverage markers are left unchanged: they still fail closed until activation.
func (p Plan) ClearBackfill(ctx context.Context, publish BackfillPublication, selection ...func(context.Context, BackfillPublication) error) error {
	for _, clear := range selection {
		if err := clear(ctx, publish); err != nil {
			return err
		}
	}
	tables := []string{}
	if p.ParentActive {
		tables = append(tables, "hierarchy_source_current", "hierarchy_source_history")
	}
	if p.LinksActive {
		tables = append(tables, "graph_link_current", "graph_link_history", "graph_link_source_current", "graph_link_source_history")
	}
	rowID := "rowid"
	if p.Dialect == Postgres {
		rowID = "ctid"
	}
	for _, table := range tables {
		for {
			var removed int64
			err := publish(ctx, func(q SQL) error {
				b := bindings{d: p.Dialect}
				where := b.scope(p.Incarnation)
				target := p.Dialect.table(table)
				result, err := q.ExecContext(ctx, "DELETE FROM "+target+" WHERE "+rowID+" IN (SELECT "+rowID+" FROM "+target+" WHERE "+where+" LIMIT 128)", b.args...)
				if err == nil {
					removed, err = result.RowsAffected()
				}
				return err
			})
			if err != nil {
				return err
			}
			if removed == 0 {
				break
			}
		}
	}
	return nil
}

// BuildBackfill decodes only one key page at a time, OUTSIDE publication locks.
// Pages never contain coverage markers, so readers cannot observe partial proof.
func (p Plan) BuildBackfill(ctx context.Context, sources SQL, decode driver.GraphSourceDecoder, publish BackfillPublication, pages ...BackfillPage) error {
	if decode == nil {
		return fmt.Errorf("source index backfill requires a core decoder")
	}
	if !p.ParentActive && !p.LinksActive && len(pages) == 0 {
		return nil
	}
	after := ""
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		records, err := p.Dialect.sourcePage(ctx, sources, after)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return nil
		}
		p.Records = make(map[string]*driver.Record, len(records))
		p.Declarations = make(map[string]Declaration, len(records))
		metadata := make([]driver.GraphSourceMetadata, len(records))
		for i := range records {
			if err := ctx.Err(); err != nil {
				return err
			}
			record := &records[i]
			id := key(record.Ref)
			if id <= after {
				return fmt.Errorf("%w: unordered backfill source identity", driver.ErrUnsupported)
			}
			image := *record
			image.Body = bytes.Clone(record.Body)
			decoded, err := decode(image)
			if err != nil {
				return err
			}
			metadata[i] = decoded
			declaration, err := Declare(*record, decoded.Parent, decoded.Links, true)
			if err != nil {
				return err
			}
			p.Records[id], p.Declarations[id] = record, declaration
			after = id
		}
		if err := publish(ctx, func(q SQL) error {
			if err := p.Publish(ctx, q); err != nil {
				return err
			}
			for _, page := range pages {
				if err := page(ctx, q, records, metadata); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
}

// ActivateBackfill certifies both newly built domains in ONE final publication,
// only after all sources were decoded and published under graph/Head locks.
func (p Plan) ActivateBackfill(ctx context.Context, q SQL, selection ...func(context.Context, SQL) error) error {
	if p.ParentActive {
		if err := p.marker(ctx, q, "hierarchy_index_coverage", p.Parent); err != nil {
			return err
		}
	}
	if p.LinksActive {
		if err := p.marker(ctx, q, "graph_link_index_coverage", p.Links); err != nil {
			return err
		}
	}
	for _, activate := range selection {
		if err := activate(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

// Backfill uses one caller-owned transaction when the engine can renew its lease
// independently. SQLite instead uses the same plan with short page publications.
func (d Dialect) Backfill(ctx context.Context, q SQL, meta driver.Meta, decode driver.GraphSourceDecoder, selection ...BackfillSelection) error {
	if decode == nil {
		return fmt.Errorf("source index backfill requires a core decoder")
	}
	p, err := d.PrepareBackfill(ctx, q, meta)
	if err != nil {
		return err
	}
	publish := func(_ context.Context, fn func(SQL) error) error { return fn(q) }
	clears := []func(context.Context, BackfillPublication) error{}
	pages := []BackfillPage{}
	activations := []func(context.Context, SQL) error{}
	isolated := make([]BackfillSelection, 0, len(selection))
	for _, hooks := range selection {
		hooks = hooks.Independent()
		isolated = append(isolated, hooks)
		clears, pages, activations = append(clears, hooks.Clear), append(pages, hooks.Page), append(activations, hooks.Activate)
	}
	if err := p.ClearBackfill(ctx, publish, clears...); err != nil {
		return err
	}
	if err := p.BuildBackfill(ctx, q, decode, publish, pages...); err != nil {
		return err
	}
	if err := p.ActivateBackfill(ctx, q, activations...); err != nil {
		return err
	}
	var refused error
	for _, hooks := range isolated {
		refused = errors.Join(refused, hooks.Refusal())
	}
	if refused != nil {
		return NewSelectionBackfillRefused(refused)
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
				err = json.Unmarshal(ref, &record.Ref)
				if err == nil {
					record.Revision = model.Natural(revision)
					err = record.Revision.Validate()
				}
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
