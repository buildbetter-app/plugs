package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

var _ port.HierarchySnapshotReader = (*snapshotReader)(nil)

// InverseSourceLinks observes complete source images for the closed incoming
// relation domain. It never broadens an empty kind vector into all links and
// never substitutes a generic Query when indexed coverage is unavailable.
func (r *snapshotReader) InverseSourceLinks(ctx context.Context, targets []model.Ref, kinds []driver.IncomingLinkKind, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := r.budget(0, 1); err != nil {
		return nil, false, err
	}
	if _, err := r.observe(ctx, []driver.LockKey{driver.GraphBlockingSentinelLock}); err != nil {
		return nil, false, err
	}
	limit, err := driver.NormalizeInverseSourceLinksRequest(targets, kinds, excluded, limit)
	if err != nil {
		return nil, false, err
	}
	reader, ok := r.reader.(driver.InverseSourceLinksReader)
	if !ok {
		return nil, false, driver.ErrUnsupported
	}
	records, more, err := reader.InverseSourceLinks(ctx, targets, kinds, excluded, limit)
	if err != nil {
		return nil, false, err
	}
	if len(records) > limit || more && len(records) != limit {
		return nil, false, fmt.Errorf("driver returned an invalid inverse source-link page")
	}
	targetSet, kindSet, excludedSet := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, target := range targets {
		targetSet[target.ID] = true
	}
	for _, kind := range kinds {
		kindSet[string(kind)] = true
	}
	for _, ref := range excluded {
		excludedSet[ref.ID] = true
	}
	refs := make([]model.Ref, len(records))
	previous := ""
	for i, record := range records {
		if driver.ValidateHierarchyRef(record.Ref) != nil || record.Kind != "bead" || excludedSet[record.Ref.ID] {
			return nil, false, failure("ontology_guard_failed", "invalid inverse hierarchy source image")
		}
		if i > 0 && recordLockKey(record.Ref) <= previous {
			return nil, false, failure("ontology_guard_failed", "duplicate or unordered inverse hierarchy sources")
		}
		previous = recordLockKey(record.Ref)
		doc, decodeErr := DecodeDocument(record.Body)
		if decodeErr != nil || doc.ID != record.Ref.ID {
			return nil, false, failure("ontology_guard_failed", "invalid inverse hierarchy source document")
		}
		matched := false
		for _, link := range doc.Links {
			if link.TargetKind != model.LinkTargetExternal && targetSet[link.To.ID] && kindSet[link.Kind] {
				matched = true
				break
			}
		}
		if !matched {
			return nil, false, failure("ontology_guard_failed", "inverse source-link index differs from exact source body")
		}
		refs[i] = record.Ref
	}
	observed, err := r.Get(ctx, refs)
	if err != nil {
		return nil, false, err
	}
	if len(observed) != len(records) {
		return nil, false, fmt.Errorf("inverse hierarchy source differs from source observations")
	}
	for i, record := range records {
		actual := observed[i]
		if recordLockKey(actual.Ref) != recordLockKey(record.Ref) || actual.Kind != record.Kind || actual.Revision.String() != record.Revision.String() || !bytes.Equal(actual.Body, record.Body) {
			return nil, false, fmt.Errorf("inverse hierarchy source differs from its source observation")
		}
	}
	return observed, more, nil
}

// Observe membership even for an empty request, zero-row probe, More or
// unavailable coverage. Every returned image also retains its exact source
// observation; the indexed result cannot substitute for that record witness.
func (r *snapshotReader) HierarchyChildren(ctx context.Context, parents []model.Ref, limit int) ([]driver.Record, bool, error) {
	return r.hierarchyChildren(ctx, parents, nil, limit)
}

func (r *snapshotReader) HierarchyChildrenExcluding(ctx context.Context, parents, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	return r.hierarchyChildren(ctx, parents, excluded, limit)
}

func (r *snapshotReader) hierarchyChildren(ctx context.Context, parents, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := r.budget(0, 1); err != nil {
		return nil, false, err
	}
	if _, err := r.observe(ctx, []driver.LockKey{driver.GraphBlockingSentinelLock}); err != nil {
		return nil, false, err
	}
	limit, err := driver.NormalizeHierarchyExcludingRequest(parents, excluded, limit)
	if err != nil {
		return nil, false, err
	}
	var records []driver.Record
	var more bool
	if len(excluded) != 0 {
		reader, ok := r.reader.(driver.HierarchyChildrenExcludingReader)
		if !ok {
			return nil, false, driver.ErrUnsupported
		}
		records, more, err = reader.HierarchyChildrenExcluding(ctx, parents, excluded, limit)
	} else {
		reader, ok := r.reader.(driver.HierarchyChildrenReader)
		if !ok {
			return nil, false, driver.ErrUnsupported
		}
		records, more, err = reader.HierarchyChildren(ctx, parents, limit)
	}
	if err != nil {
		return nil, false, err
	}
	if len(records) > limit || more && len(records) != limit {
		return nil, false, fmt.Errorf("driver returned an invalid hierarchy child page")
	}
	refs := make([]model.Ref, len(records))
	selected, omitted := map[string]bool{}, map[string]bool{}
	for _, parent := range parents {
		selected[parent.ID] = true
	}
	for _, ref := range excluded {
		omitted[ref.ID] = true
	}
	previous := ""
	for i, record := range records {
		if driver.ValidateHierarchyRef(record.Ref) != nil || record.Kind != "bead" {
			return nil, false, failure("ontology_guard_failed", "invalid hierarchy child image")
		}
		doc, err := DecodeDocument(record.Body)
		if err != nil || doc.ID != record.Ref.ID || doc.Parent == nil || !selected[doc.Parent.ID] || omitted[doc.ID] || driver.ValidateHierarchyRecord(record, doc.Parent) != nil {
			return nil, false, failure("ontology_guard_failed", "hierarchy index differs from canonical Parent")
		}
		key := recordLockKey(record.Ref)
		if i > 0 && key <= previous {
			return nil, false, failure("ontology_guard_failed", "duplicate or unordered hierarchy children")
		}
		previous, refs[i] = key, record.Ref
	}
	observed, err := r.Get(ctx, refs)
	if err != nil {
		return nil, false, err
	}
	if len(observed) != len(records) {
		return nil, false, fmt.Errorf("driver hierarchy children differ from source observations")
	}
	for i, record := range records {
		actual := observed[i]
		if recordLockKey(actual.Ref) != recordLockKey(record.Ref) || actual.Kind != record.Kind || actual.Revision.String() != record.Revision.String() || !bytes.Equal(actual.Body, record.Body) {
			return nil, false, fmt.Errorf("driver hierarchy child differs from its source observation")
		}
	}
	return observed, more, nil
}

// HierarchyClosureRows enumerates only the named individual descendant
// prefixes. All sentinels, including absent groups, join the read set before
// enumeration. Exact row observations must agree with the enumerated bytes.
func (r *snapshotReader) HierarchyClosureRows(ctx context.Context, descendants []model.Ref) ([]driver.DerivedRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefixes := make([]string, 0, len(descendants))
	sentinels := map[string]driver.LockKey{}
	selected := map[string]bool{}
	for _, ref := range descendants {
		if err := driver.ValidateHierarchyRef(ref); err != nil {
			return nil, err
		}
		if selected[ref.ID] {
			continue
		}
		selected[ref.ID] = true
		prefix, err := AncestorPrefix(ref.ID)
		if err != nil {
			return nil, err
		}
		prefixes = append(prefixes, prefix)
		sentinel, _ := driver.NewClosureSentinel(ref.ID)
		key := driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}
		encoded, _ := driver.EncodeLockKey(key)
		sentinels[encoded] = key
	}
	if err := r.budget(len(sentinels), 0); err != nil {
		return nil, err
	}
	if _, err := r.observe(ctx, sortedKeys(sentinels)); err != nil {
		return nil, err
	}
	if len(prefixes) == 0 {
		return []driver.DerivedRow{}, nil
	}
	sort.Strings(prefixes)
	var rows []driver.DerivedRow
	if reader, ok := r.reader.(driver.DerivedPrefixReader); ok {
		if err := r.budget(0, 1); err != nil {
			return nil, err
		}
		var err error
		rows, err = reader.DerivedRowsForPrefixes(ctx, driver.DerivedAncestorClosure, prefixes)
		if err != nil {
			return nil, err
		}
	} else {
		if err := r.budget(0, len(prefixes)); err != nil {
			return nil, err
		}
		for _, prefix := range prefixes {
			group, err := r.reader.DerivedRows(ctx, driver.DerivedAncestorClosure, prefix)
			if err != nil {
				return nil, err
			}
			rows = append(rows, group...)
		}
	}
	keys := make([]driver.LockKey, len(rows))
	for i, row := range rows {
		descendant, _, _, err := DecodeAncestorRow(row)
		if err != nil || !selected[descendant] || i > 0 && row.Key <= rows[i-1].Key {
			return nil, failure("ontology_guard_failed", "malformed selected ancestor closure")
		}
		keys[i] = ancestorLock(row.Key)
	}
	states, err := r.Observe(ctx, keys)
	if err != nil {
		return nil, err
	}
	result := make([]driver.DerivedRow, len(rows))
	for i, state := range states {
		if !state.Present || state.Derived == nil || state.Derived.Key != rows[i].Key || !bytes.Equal(state.Derived.Value, rows[i].Value) {
			return nil, fmt.Errorf("driver closure enumeration differs from its snapshot observations")
		}
		result[i] = driver.DerivedRow{Key: rows[i].Key, Value: bytes.Clone(rows[i].Value)}
	}
	return result, nil
}
