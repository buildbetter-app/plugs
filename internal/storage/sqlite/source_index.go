package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sourceindex"
)

func (t *transaction) clearSourceOverlay() {
	t.sourceOverlay = sourceindex.Overlay{}
	t.sourceCoverage = sourceindex.CoveragePair{}
	t.sourceCoverageValid = false
	t.sourceIdentities = nil
}

func (t *transaction) currentSourceCoverage(ctx context.Context) (sourceindex.CoveragePair, error) {
	if t.sourceCoverageValid && t.sourceCoverageIncarnation == t.meta.Incarnation && t.sourceCoverageHead == t.meta.Head {
		return t.sourceCoverage, nil
	}
	coverage, err := sourceindex.SQLite.ReadCoverage(ctx, t.tx, t.meta.Incarnation)
	if err != nil {
		return sourceindex.CoveragePair{}, err
	}
	for _, c := range []sourceindex.Coverage{coverage.Parent, coverage.Links} {
		if c.Ready {
			if err = c.Require(mustUint(t.meta.Head)); err != nil {
				return sourceindex.CoveragePair{}, err
			}
		}
	}
	t.sourceCoverage = coverage
	t.sourceCoverageHead = t.meta.Head
	t.sourceCoverageIncarnation = t.meta.Incarnation
	t.sourceCoverageValid = true
	return coverage, nil
}

func (t *transaction) PutHierarchyRecord(ctx context.Context, record driver.Record, parent *model.Ref) error {
	return t.putTypedSource(ctx, record, parent, nil, false)
}

func (t *transaction) PutGraphRecord(ctx context.Context, record driver.Record, parent *model.Ref, links []driver.SourceLink) error {
	return t.putTypedSource(ctx, record, parent, links, true)
}

func (t *transaction) putTypedSource(ctx context.Context, record driver.Record, parent *model.Ref, links []driver.SourceLink, graph bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	declaration, err := sourceindex.Declare(record, parent, links, graph)
	if err != nil {
		return err
	}
	return t.putSourceRecord(ctx, record, &declaration)
}

// Validate both the ordinary record and the optional complete declaration
// before replacing any component of the previous staged image.
func (t *transaction) putSourceRecord(ctx context.Context, record driver.Record, declaration *sourceindex.Declaration) error {
	id, err := recordKey(record.Ref)
	if err != nil {
		return err
	}
	if err = t.writable(driver.LockKey{Kind: driver.LockRecord, Ref: &record.Ref}); err != nil {
		return err
	}
	if record.Kind == "" || !json.Valid(record.Body) {
		return fmt.Errorf("sqlite: kind and valid JSON body required")
	}
	var kind string
	err = t.tx.QueryRowContext(ctx, `SELECT kind FROM objects WHERE key=?`, id).Scan(&kind)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil && kind != record.Kind {
		return fmt.Errorf("%w: changing record kind", driver.ErrUnsupported)
	}
	if old, ok := t.records[id]; ok && old != nil && old.Kind != record.Kind {
		return fmt.Errorf("%w: changing staged kind", driver.ErrUnsupported)
	}
	state, err := t.observe(ctx, driver.LockKey{Kind: driver.LockRecord, Ref: &record.Ref})
	if err != nil {
		return err
	}
	if record.Kind == "bead" {
		coverage, err := t.currentSourceCoverage(ctx)
		if err != nil {
			return err
		}
		if declaration == nil && (coverage.Parent.Ready || coverage.Links.Ready) || declaration != nil && !declaration.Graph && coverage.Links.Ready {
			return fmt.Errorf("%w: certified source requires complete typed metadata", driver.ErrUnsupported)
		}
		if declaration != nil {
			if err = t.writable(driver.GraphBlockingSentinelLock); err != nil {
				return err
			}
		}
	}
	record = copyRecord(record)
	record.Ref.Branch = model.Natural64(0)
	record.Revision = state.Revision
	if t.sourceIdentities == nil {
		t.sourceIdentities = make(map[string]driver.Record)
	}
	t.sourceIdentities[id] = driver.Record{Ref: record.Ref, Kind: record.Kind}
	t.records[id] = &record
	t.sourceOverlay.Replace(id, declaration)
	t.selectionSourceStaged(id)
	return nil
}

func (t *transaction) prepareSourcePublication(ctx context.Context, c *sql.Conn, current driver.Meta, revision model.Revision) (sourceindex.Plan, error) {
	next := current
	next.Head = revision
	if t.restore != nil {
		next = *t.restore
	}
	empty := false
	if current.Head.IsZero() {
		err := restoreEmpty(ctx, c)
		if err == nil {
			empty = true
		} else if !errors.Is(err, driver.ErrRestoreTargetNotEmpty) {
			return sourceindex.Plan{}, err
		}
	}
	_, bumped := t.bumps[keyID(driver.GraphBlockingSentinelLock)]
	return sourceindex.SQLite.Prepare(ctx, c, current, next, empty, t.records, t.sourceIdentities, &t.sourceOverlay, func() error { return t.writable(driver.GraphBlockingSentinelLock) }, bumped)
}

func (r *reader) HierarchyChildren(ctx context.Context, parents []model.Ref, limit int) ([]driver.Record, bool, error) {
	return r.HierarchyChildrenExcluding(ctx, parents, nil, limit)
}

func (r *reader) HierarchyChildrenExcluding(ctx context.Context, parents, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return nil, false, err
	}
	return r.selectIndexedSources(ctx, sourceindex.Request{Refs: parents, Excluded: excluded, Limit: limit}, nil)
}

func (r *reader) InverseSourceLinks(ctx context.Context, targets []model.Ref, kinds []driver.IncomingLinkKind, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return nil, false, err
	}
	return r.selectIndexedSources(ctx, sourceindex.Request{Refs: targets, Kinds: kinds, Excluded: excluded, Limit: limit, Inverse: true}, nil)
}

func (r *reader) selectIndexedSources(ctx context.Context, request sourceindex.Request, work *sourceindex.Work) ([]driver.Record, bool, error) {
	// Every SQLite reader, including a pre-Head writer, retains one WAL
	// snapshot for both key selection and exact source-image materialization.
	request.Historical = r.at != mustUint(r.meta.Head)
	var overlay *sourceindex.Overlay
	var records map[string]*driver.Record
	if r.owner != nil {
		overlay, records = &r.owner.sourceOverlay, r.owner.records
	}
	return sourceindex.SQLite.Select(ctx, r.tx, r.meta, r.at, request, overlay, records, work)
}

var _ driver.GraphSourceWriter = (*transaction)(nil)
var _ driver.HierarchySourceWriter = (*transaction)(nil)
var _ driver.HierarchyChildrenExcludingReader = (*reader)(nil)
var _ driver.InverseSourceLinksReader = (*reader)(nil)
