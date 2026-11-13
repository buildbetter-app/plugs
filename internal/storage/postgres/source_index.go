package postgres

import (
	"context"
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
}

func (t *transaction) currentSourceCoverage(ctx context.Context) (sourceindex.CoveragePair, error) {
	if t.sourceCoverageValid && t.sourceCoverageIncarnation == t.meta.Incarnation && t.sourceCoverageHead.String() == t.meta.Head.String() {
		return t.sourceCoverage, nil
	}
	coverage, err := sourceindex.Postgres.ReadCoverage(ctx, t.sql, t.meta.Incarnation)
	if err == nil {
		at, e := t.meta.Head.Uint64()
		if e != nil {
			return sourceindex.CoveragePair{}, e
		}
		for _, c := range []sourceindex.Coverage{coverage.Parent, coverage.Links} {
			if c.Ready {
				if e = c.Require(at); e != nil {
					return sourceindex.CoveragePair{}, e
				}
			}
		}
		t.sourceCoverage = coverage
		t.sourceCoverageHead = t.meta.Head
		t.sourceCoverageIncarnation = t.meta.Incarnation
		t.sourceCoverageValid = true
	}
	return coverage, err
}
func (t *transaction) guardSourcePut(ctx context.Context, declaration *sourceindex.Declaration) error {
	coverage, err := t.currentSourceCoverage(ctx)
	if err != nil {
		return err
	}
	if declaration == nil && (coverage.Parent.Ready || coverage.Links.Ready) || declaration != nil && !declaration.Graph && coverage.Links.Ready {
		return fmt.Errorf("%w: certified source requires complete typed metadata", driver.ErrUnsupported)
	}
	if declaration != nil {
		return t.writable(driver.GraphBlockingSentinelLock)
	}
	return nil
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
func (t *transaction) prepareSourcePublication(ctx context.Context, revision model.Revision) (sourceindex.Plan, error) {
	next := t.meta
	next.Head = revision
	if t.restore != nil {
		next = *t.restore
	}
	empty := false
	if t.meta.Head.IsZero() {
		err := t.restoreEmpty(ctx)
		if err == nil {
			empty = true
		} else if !errors.Is(err, driver.ErrRestoreTargetNotEmpty) {
			return sourceindex.Plan{}, err
		}
	}
	graphKey, _ := keyID(driver.GraphBlockingSentinelLock)
	_, bumped := t.bumps[graphKey]
	return sourceindex.Postgres.Prepare(ctx, t.sql, t.meta, next, empty, t.records, t.identities, &t.sourceOverlay, func() error { return t.writable(driver.GraphBlockingSentinelLock) }, bumped)
}

func (t *transaction) prepareImportedSources(ctx context.Context, meta driver.Meta) (sourceindex.Plan, error) {
	graphKey, _ := keyID(driver.GraphBlockingSentinelLock)
	_, bumped := t.bumps[graphKey]
	return sourceindex.Postgres.PrepareImport(ctx, t.sql, t.meta, meta, t.records, t.identities, &t.sourceOverlay, func() error { return t.writable(driver.GraphBlockingSentinelLock) }, bumped)
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
	var overlay *sourceindex.Overlay
	var records map[string]*driver.Record
	// Ordinary readers pin a repeatable-read snapshot. A pre-Head writer is
	// READ COMMITTED, so every selected Body must use its exact historical PK.
	request.Historical = r.at != mustRevision(r.meta.Head) || r.writer != nil && !r.writer.hasHead
	if r.writer != nil {
		overlay = &r.writer.sourceOverlay
		records = r.writer.records
	}
	return sourceindex.Postgres.Select(ctx, r.sql, r.meta, r.at, request, overlay, records, work)
}
func mustRevision(revision model.Revision) uint64 { value, _ := revision.Uint64(); return value }

var _ driver.HierarchySourceWriter = (*transaction)(nil)
var _ driver.GraphSourceWriter = (*transaction)(nil)
var _ driver.HierarchyChildrenExcludingReader = (*reader)(nil)
var _ driver.InverseSourceLinksReader = (*reader)(nil)
