package restore

import (
	"context"
	"fmt"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// nativeAncestorPlan is calculated from the immutable native head image before
// the restore transaction opens. The plan contains the complete canonical
// closure and the exact derived-row locks needed to publish it atomically with
// the records. It deliberately does not expose pipeline.Writer state or repair
// an already-populated store.
type nativeAncestorPlan struct {
	rows      []driver.DerivedRow
	locks     []driver.LockRequest
	sentinels []driver.Sentinel
}

func buildNativeAncestorPlan(ctx context.Context, records []driver.Record, maxClosureBytes int64) (*nativeAncestorPlan, error) {
	documents := make(map[string]pipeline.Document)
	parents := make(map[string]string)
	for _, record := range records {
		if record.Kind != "bead" {
			continue
		}
		if !record.Ref.IsID() || !record.Ref.Branch.IsZero() {
			return nil, fmt.Errorf("native bead reference must be a branch-zero ID")
		}
		doc, err := pipeline.DecodeDocument(record.Body)
		if err != nil {
			return nil, fmt.Errorf("invalid native bead document %s: %w", record.Ref.ID, err)
		}
		if doc.ID != record.Ref.ID {
			return nil, fmt.Errorf("native bead body ID differs from record reference")
		}
		if _, exists := documents[doc.ID]; exists {
			return nil, fmt.Errorf("duplicate native bead document %s", doc.ID)
		}
		documents[doc.ID] = pipeline.Document{ID: doc.ID, Parent: doc.Parent, Links: doc.Links}
		if doc.Parent != nil {
			parents[doc.ID] = doc.Parent.ID
		}
	}
	// Typed Parent/Links coverage proves membership only. Certify the complete
	// semantic graph, including unrelated components and opaque link targets,
	// before any restore publication can carry the global-validity read fact.
	if err := pipeline.ValidateGraphDocuments(ctx, documents); err != nil {
		return nil, err
	}
	for id, parent := range parents {
		if _, exists := documents[parent]; !exists {
			return nil, fmt.Errorf("native bead %s refers to missing parent %s", id, parent)
		}
	}

	rowsByKey := make(map[string]driver.DerivedRow)
	closureSentinels := make(map[driver.Sentinel]bool, len(documents))
	var closureBytes int64
	for id := range documents {
		seen := make(map[string]bool)
		for current, depth := id, uint64(0); current != ""; depth++ {
			if seen[current] {
				return nil, fmt.Errorf("native bead parent graph contains a cycle")
			}
			seen[current] = true
			if maxClosureBytes != 0 {
				// Reserve the encoded row/key and the map, sorted slices,
				// lock requests, and transaction staging that reuse them.
				closureBytes += int64(8*(len(id)+len(current)) + 1024)
				if closureBytes > maxClosureBytes {
					return nil, fmt.Errorf("native ancestor closure exceeds buffer bound")
				}
			}
			row, err := pipeline.AncestorRow(id, current, depth)
			if err != nil {
				return nil, fmt.Errorf("native ancestor row %s/%s: %w", id, current, err)
			}
			if prior, exists := rowsByKey[row.Key]; exists && string(prior.Value) != string(row.Value) {
				return nil, fmt.Errorf("native ancestor row has conflicting depths")
			}
			rowsByKey[row.Key] = row
			current = parents[current]
		}
		sentinel, err := driver.NewClosureSentinel(id)
		if err != nil {
			return nil, err
		}
		closureSentinels[sentinel] = true
	}

	rows := make([]driver.DerivedRow, 0, len(rowsByKey))
	for _, row := range rowsByKey {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
	locks := make([]driver.LockRequest, 0, len(rows))
	for _, row := range rows {
		key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: row.Key}}
		locks = append(locks, driver.LockRequest{Key: key, Mode: driver.LockUpdate})
	}
	sentinels := make([]driver.Sentinel, 0, len(closureSentinels))
	for sentinel := range closureSentinels {
		sentinels = append(sentinels, sentinel)
	}
	sort.Slice(sentinels, func(i, j int) bool { return sentinels[i] < sentinels[j] })
	return &nativeAncestorPlan{rows: rows, locks: locks, sentinels: sentinels}, nil
}

func nativeAncestorPlanForArchive(ctx context.Context, records []driver.Record, native, streamed bool) (*nativeAncestorPlan, error) {
	if !native {
		return nil, nil
	}
	var maxClosureBytes int64
	if streamed {
		maxClosureBytes = 16 << 20
	}
	return buildNativeAncestorPlan(ctx, records, maxClosureBytes)
}
