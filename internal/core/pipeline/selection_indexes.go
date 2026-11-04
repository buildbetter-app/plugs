package pipeline

import (
	"bytes"
	"context"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// refreshableLegacyAncestorSet permits annotation of a cold legacy closure only
// after core proves its complete logical chain against the actual old sources.
// Every target is observed before the Writer's lock census/Head. Bad optional
// bytes keep the ordinary write supported, with unavailable annotations. A
// non-nil stale annotation is never repaired by this path.
func (p *prepared) refreshableLegacyAncestorSet(ctx context.Context, source driver.Record, rows []driver.DerivedRow) (bool, error) {
	if source.Kind != "bead" || len(source.Body) == 0 || len(rows) == 0 || len(rows) > maxClosureRows {
		return false, nil
	}
	legacy := false
	for _, row := range rows {
		if row.Index == nil {
			legacy = true
		} else if !canRefreshAncestorProjection(row, source, p.snapshot.Snapshot()) {
			return false, nil
		}
	}
	if !legacy {
		return false, nil
	}
	chain := make([]model.Ref, len(rows))
	seenDepth, seenTarget := map[uint64]bool{}, map[string]bool{}
	for _, row := range rows {
		descendant, target, depth, err := DecodeAncestorRow(row)
		if err != nil || descendant != source.Ref.ID || depth >= uint64(len(rows)) || seenDepth[depth] || seenTarget[target] {
			return false, nil
		}
		chain[depth] = model.RefByID(target, model.Natural64(0))
		seenDepth[depth], seenTarget[target] = true, true
	}
	targets, err := p.snapshot.Get(ctx, chain)
	if err != nil {
		return false, ctx.Err()
	}
	if len(targets) != len(chain) {
		return false, nil
	}
	expected := source.Ref
	for depth, target := range targets {
		if !driver.SelectionSameRef(chain[depth], expected) || !driver.SelectionSameRef(target.Ref, expected) || target.Kind != "bead" {
			return false, nil
		}
		if depth == 0 && (target.Revision.String() != source.Revision.String() || !bytes.Equal(target.Body, source.Body)) {
			return false, nil
		}
		doc, _, err := decodeGraphSource(target)
		if err != nil || (doc.Parent == nil) != (depth+1 == len(chain)) {
			return false, nil
		}
		if doc.Parent != nil {
			expected = *doc.Parent
		}
	}
	return true, nil
}

func sourceSelectionFamilies() []driver.SelectionProjectionFamily {
	return []driver.SelectionProjectionFamily{
		{Table: driver.DerivedSortKey, Version: driver.SelectionIndexVersion},
		{Table: driver.DerivedAttribute, Kind: driver.SelectionLabelFact, Version: driver.SelectionIndexVersion},
		{Table: driver.DerivedReferenceIndex, Kind: driver.SelectionLinkFact, Version: driver.SelectionIndexVersion},
		{Table: driver.DerivedAncestorClosure, Kind: driver.SelectionAncestorFact, Version: driver.SelectionIndexVersion},
	}
}

// sourceSelectionDecoder decodes each body once. The separate projection
// callback interprets only the current logical closure Key/Value, not JSON.
func sourceSelectionDecoder(incarnation string) (driver.GraphSourceDecoder, driver.GraphSourceProjection) {
	decode := func(record driver.Record) (driver.GraphSourceMetadata, error) {
		plan, err := PrepareSourceRecord(record)
		if err != nil || plan.doc == nil {
			return driver.GraphSourceMetadata{}, fmt.Errorf("%w: invalid core source document during index activation", driver.ErrUnsupported)
		}
		rows, err := plan.SelectionRows(incarnation, record.Revision)
		if err != nil {
			return driver.GraphSourceMetadata{}, err
		}
		metadata := driver.GraphSourceMetadata{Parent: plan.doc.Parent, Links: plan.links}
		for i, row := range rows {
			metadata.Selection = append(metadata.Selection, driver.DerivedSelectionRow{Table: plan.keys[i].Table, Row: row})
		}
		return metadata, nil
	}
	projection := driver.GraphSourceProjection{
		Families: sourceSelectionFamilies(), MaxPageRows: maxClosureRows,
		Inputs: func(record driver.Record) ([]driver.DerivedRowRange, error) {
			prefix, err := AncestorPrefix(record.Ref.ID)
			if err != nil {
				return nil, err
			}
			return []driver.DerivedRowRange{{Table: driver.DerivedAncestorClosure, Prefix: []byte(prefix), Max: maxClosureRows}}, nil
		},
		Decode: func(record driver.Record, current []driver.DerivedRow) ([]driver.DerivedSelectionRow, error) {
			if len(current) == 0 || len(current) > maxClosureRows {
				return nil, fmt.Errorf("%w: missing or oversized current closure", driver.ErrSelectionUnavailable)
			}
			depths := make([]bool, len(current))
			rows := make([]driver.DerivedSelectionRow, 0, len(current))
			for _, row := range current {
				descendant, ancestor, depth, err := DecodeAncestorRow(row)
				if err != nil || descendant != record.Ref.ID || depth >= uint64(len(depths)) || depths[depth] {
					return nil, fmt.Errorf("%w: incomplete or inconsistent current closure", driver.ErrSelectionUnavailable)
				}
				depths[depth] = true
				ref, target := record.Ref, model.RefByID(ancestor, model.Natural64(0))
				copy := driver.DerivedRow{Key: row.Key, Value: bytes.Clone(row.Value)}
				copy.Index = &driver.SelectionIndex{Version: driver.SelectionIndexVersion, Row: driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: row.Key}, Incarnation: incarnation, Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(copy.Value), Source: driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Revision: record.Revision, Present: true, Digest: driver.SelectionDigest(record.Body)}, Facts: &driver.SelectionFacts{Kind: driver.SelectionAncestorFact, Rows: []driver.SelectionFact{{Kind: driver.SelectionAncestorFact, Ref: &ref, Target: &target, Depth: depth}}}}
				if err := copy.Index.Validate(driver.DerivedAncestorClosure, copy); err != nil {
					return nil, fmt.Errorf("%w: invalid current closure projection", driver.ErrSelectionUnavailable)
				}
				rows = append(rows, driver.DerivedSelectionRow{Table: driver.DerivedAncestorClosure, Row: copy})
			}
			// DecodeAncestorRow permits depth zero only on the exact self row.
			// Unique depths in [0,len) therefore prove self plus contiguity.
			return rows, nil
		},
	}
	return decode, projection
}
