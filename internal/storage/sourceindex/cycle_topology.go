package sourceindex

import (
	"context"
	"fmt"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"strings"
)

// CycleTopology reads raw local facts, never source document bodies. The SQL
// transaction is the caller's existing repeatable pinned reader transaction.
func (d Dialect) CycleTopology(ctx context.Context, q SQL, meta driver.Meta, at uint64, cursor *driver.Cursor, limit int) (driver.Page[driver.CycleTopologyEdge], error) {
	empty := driver.Page[driver.CycleTopologyEdge]{}
	revision := model.Natural64(at)
	offset, err := driver.CycleTopologyOffset(cursor, meta.Incarnation, revision, limit)
	if err != nil {
		return empty, err
	}
	if offset > uint64(1<<63-1) {
		return empty, driver.ErrUnsupported
	}
	coverage, err := d.ReadCoverage(ctx, q, meta.Incarnation)
	if err != nil {
		return empty, err
	}
	if err = coverage.Links.Require(at); err != nil {
		return empty, err
	}
	b := bindings{d: d}
	where := "e.incarnation=" + b.add(meta.Incarnation) + " AND e.branch=" + b.add(d.revision(0))
	table := d.table("graph_link_current")
	if revision.String() != meta.Head.String() {
		table = d.table("graph_link_history")
		where += " AND e.start_revision<=" + b.add(d.revision(at)) + " AND EXISTS (SELECT 1 FROM " + d.table("graph_link_source_history") + " s WHERE s.incarnation=e.incarnation AND s.branch=e.branch AND s.source_key=e.source_key AND s.start_revision=e.start_revision AND (s.end_revision IS NULL OR s.end_revision>" + b.add(d.revision(at)) + "))"
	}
	query := "SELECT e.source_key,e.link_kind,e.target_key FROM " + table + " e WHERE " + where + " ORDER BY e.source_key,e.link_kind,e.target_key LIMIT " + b.add(limit+1) + " OFFSET " + b.add(int64(offset))
	rows, err := q.QueryContext(ctx, query, b.args...)
	if err != nil {
		return empty, err
	}
	page := driver.Page[driver.CycleTopologyEdge]{Items: []driver.CycleTopologyEdge{}, AsOf: revision}
	// Both encodings are common driver lock keys; only exact canonical local
	// branch-zero record keys are accepted. IDs can themselves contain colons.
	ref := func(raw []byte) (model.Ref, error) {
		key := string(raw)
		if !strings.HasPrefix(key, "0:id:") || !strings.HasSuffix(key, ":0") {
			return model.Ref{}, fmt.Errorf("%w: invalid cycle topology key", driver.ErrUnsupported)
		}
		value := model.RefByID(strings.TrimSuffix(strings.TrimPrefix(key, "0:id:"), ":0"), model.Natural64(0))
		encoded, e := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &value})
		if e != nil || encoded != key {
			return model.Ref{}, fmt.Errorf("%w: invalid cycle topology reference", driver.ErrUnsupported)
		}
		return value, nil
	}
	for rows.Next() {
		var from, to, kind []byte
		if err = rows.Scan(&from, &kind, &to); err != nil {
			break
		}
		edge := driver.CycleTopologyEdge{Kind: string(kind)}
		edge.From, err = ref(from)
		if err != nil {
			break
		}
		edge.To, err = ref(to)
		if err != nil {
			break
		}
		if edge.Kind == "" {
			err = driver.ErrUnsupported
			break
		}
		page.Items = append(page.Items, edge)
	}
	if err == nil {
		err = rows.Err()
	}
	closeErr := rows.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return empty, err
	}
	if len(page.Items) > limit {
		page.Items = page.Items[:limit]
		page.NextCursor = driver.CycleTopologyCursor(meta.Incarnation, revision, offset+uint64(limit))
	}
	return page, nil
}
