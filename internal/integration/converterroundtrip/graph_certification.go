package converterroundtrip

import (
	"context"
	"fmt"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// GraphCertification describes only the imported current graph, never the
// source's historical graph. Provenance remains in the existing import binding.
type GraphCertification struct {
	State       string         `json:"state"`
	Reason      string         `json:"reason,omitempty"`
	Incarnation string         `json:"incarnation,omitempty"`
	From        model.Revision `json:"from"`
	Through     model.Revision `json:"through"`
}

// Preflight before staging anything. In particular, PutSourceRecord's optional
// parent-only fallback is insufficient for a semantic graph certificate.
func prepareImportGraph(ctx context.Context, tx driver.WriteTx, records []driver.Record, owners ...*dumpformat.Archive) (bool, error) {
	if _, ok := tx.(driver.GraphSourceWriter); !ok {
		return false, nil
	}
	if len(owners) != 0 {
		work, release := owners[0].NativeDeltaAccountingScope()
		defer release()
		var bytes uint64
		for _, record := range records {
			if record.Kind == "bead" {
				bytes += uint64(len(record.Body))*8 + 1024
			}
		}
		if err := work.ChargeNativeDeltaBytes("import_graph_validation", bytes); err != nil {
			return false, err
		}
	}
	docs := make(map[string]pipeline.Document)
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if record.Kind != "bead" {
			continue
		}
		if driver.ValidateHierarchyRef(record.Ref) != nil {
			return false, fmt.Errorf("invalid imported graph reference")
		}
		doc, err := pipeline.DecodeDocument(record.Body)
		if err != nil || doc.ID != record.Ref.ID {
			return false, fmt.Errorf("imported graph body/reference mismatch: %s", record.Ref.ID)
		}
		if _, exists := docs[doc.ID]; exists {
			return false, fmt.Errorf("duplicate imported graph item: %s", doc.ID)
		}
		docs[doc.ID] = doc
	}
	if err := pipeline.ValidateGraphDocuments(ctx, docs); err != nil {
		return false, err
	}
	for _, doc := range docs {
		if doc.Parent != nil {
			if _, ok := docs[doc.Parent.ID]; !ok {
				return false, fmt.Errorf("imported graph parent outside admitted image: %s", doc.Parent.ID)
			}
		}
	}
	// An empty import has no typed source declaration to activate coverage.
	return len(docs) != 0, nil
}

func putImportRecord(ctx context.Context, tx driver.WriteTx, record driver.Record, certify bool) error {
	if certify {
		return pipeline.PutSourceRecord(ctx, tx, record)
	}
	return tx.Put(ctx, record)
}

func importGraphReport(certified bool, meta driver.Meta) *GraphCertification {
	r := &GraphCertification{State: "uncertified", Reason: "complete typed graph publication unavailable or empty graph", From: model.Natural64(0), Through: model.Natural64(0)}
	if certified {
		r.State, r.Reason, r.Incarnation = "certified", "", meta.Incarnation
		r.From, r.Through = meta.Head, meta.Head
	}
	return r
}

func recoveredImportGraphReport(ctx context.Context, reader driver.Reader, head model.Revision) *GraphCertification {
	view := reader.Snapshot()
	if view.At == nil || *view.At != head {
		return nil // no claim about the original certificate from a later snapshot
	}
	states, err := reader.Observe(ctx, []driver.LockKey{pipeline.GraphValidityLockKey(), driver.GraphBlockingSentinelLock})
	if err != nil {
		return &GraphCertification{State: "unknown", Reason: "certificate observation unavailable", From: model.Natural64(0), Through: model.Natural64(0)}
	}
	certified := len(states) == 2
	for _, state := range states {
		certified = certified && state.Present && state.Revision == head
	}
	if certified {
		parents, parentOK := reader.(driver.HierarchyChildrenReader)
		links, linksOK := reader.(driver.InverseSourceLinksReader)
		if !parentOK || !linksOK {
			certified = false
		} else {
			_, _, parentErr := parents.HierarchyChildren(ctx, nil, 0)
			_, _, linkErr := links.InverseSourceLinks(ctx, nil, nil, nil, 0)
			certified = parentErr == nil && linkErr == nil
		}
	}
	return importGraphReport(certified, driver.Meta{Incarnation: view.Incarnation, Head: head})
}

// The only added locks are the semantic fact and graph sentinel, sorted with
// the import's existing complete lock set and acquired before Head.
func importGraphLocks(requests []driver.LockRequest) []driver.LockRequest {
	keys := make(map[string]driver.LockRequest, len(requests)+2)
	for _, request := range requests {
		key, _ := driver.EncodeLockKey(request.Key)
		keys[key] = request
	}
	for _, key := range []driver.LockKey{pipeline.GraphValidityLockKey(), driver.GraphBlockingSentinelLock} {
		encoded, _ := driver.EncodeLockKey(key)
		keys[encoded] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	result := make([]driver.LockRequest, 0, len(ordered))
	for _, key := range ordered {
		result = append(result, keys[key])
	}
	return result
}
