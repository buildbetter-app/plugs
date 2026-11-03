package pipeline

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// PutSourceRecord publishes canonical Parent metadata from the exact bead Body
// passed to storage. Callers still own graph validation, locks and publication;
// this adapter does not repair hierarchy state or certify a populated store.
// Drivers without the optional typed writer retain ordinary unindexed writes.
func PutSourceRecord(ctx context.Context, tx driver.WriteTx, record driver.Record) error {
	if record.Kind != "bead" {
		return tx.Put(ctx, record)
	}
	if err := driver.ValidateHierarchyRef(record.Ref); err != nil {
		return failure("ontology_guard_failed", "invalid hierarchy source reference")
	}
	doc, err := DecodeDocument(record.Body)
	if err != nil || doc.ID != record.Ref.ID {
		return failure("ontology_guard_failed", "invalid hierarchy source document")
	}
	if err := driver.ValidateHierarchyRecord(record, doc.Parent); err != nil {
		return failure("ontology_guard_failed", "invalid hierarchy source parent")
	}
	links := make([]driver.SourceLink, len(doc.Links))
	for i, link := range doc.Links {
		if !validDependencyKind(link.Kind) || driver.ValidateHierarchyRef(link.To) != nil {
			return failure("ontology_guard_failed", "invalid hierarchy source link")
		}
		links[i] = driver.SourceLink{Kind: link.Kind, To: link.To}
	}
	if writer, ok := tx.(driver.GraphSourceWriter); ok {
		return writer.PutGraphRecord(ctx, record, doc.Parent, links)
	}
	if writer, ok := tx.(driver.HierarchySourceWriter); ok {
		return writer.PutHierarchyRecord(ctx, record, doc.Parent)
	}
	return tx.Put(ctx, record)
}
