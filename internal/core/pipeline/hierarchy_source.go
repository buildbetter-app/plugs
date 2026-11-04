package pipeline

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// PutSourceRecord publishes canonical Parent metadata from the exact bead Body
// passed to storage. Callers still own graph validation, locks and publication;
// this adapter does not repair hierarchy state or certify a populated store.
// Drivers without the optional typed writer retain ordinary unindexed writes.
// This graph-only adapter does not certify selection. Owners maintaining
// selection use PrepareSourceRecord and its pre-Head lock census; a graph-only
// rewrite leaves any old source-bound selection annotation unavailable.
func PutSourceRecord(ctx context.Context, tx driver.WriteTx, record driver.Record) error {
	if record.Kind != "bead" {
		return tx.Put(ctx, record)
	}
	if err := driver.ValidateHierarchyRef(record.Ref); err != nil {
		return failure("ontology_guard_failed", "invalid hierarchy source reference")
	}
	doc, links, err := decodeGraphSource(record)
	if err != nil {
		return err
	}
	return putDecodedGraphSource(ctx, tx, record, doc, links)
}

func decodeGraphSource(record driver.Record) (Document, []driver.SourceLink, error) {
	doc, err := DecodeDocument(record.Body)
	if err != nil {
		return Document{}, nil, failure("ontology_guard_failed", "invalid hierarchy source document")
	}
	return graphSourceMetadata(record, doc)
}

func graphSourceMetadata(record driver.Record, doc Document) (Document, []driver.SourceLink, error) {
	if doc.ID != record.Ref.ID {
		return Document{}, nil, failure("ontology_guard_failed", "invalid hierarchy source document")
	}
	if err := driver.ValidateHierarchyRecord(record, doc.Parent); err != nil {
		return Document{}, nil, failure("ontology_guard_failed", "invalid hierarchy source parent")
	}
	links := make([]driver.SourceLink, len(doc.Links))
	for i, link := range doc.Links {
		if !validDependencyKind(link.Kind) || driver.ValidateHierarchyRef(link.To) != nil {
			return Document{}, nil, failure("ontology_guard_failed", "invalid hierarchy source link")
		}
		links[i] = driver.SourceLink{Kind: link.Kind, To: link.To}
	}
	return doc, links, nil
}

func putDecodedGraphSource(ctx context.Context, tx driver.WriteTx, record driver.Record, doc Document, links []driver.SourceLink) error {
	if writer, ok := tx.(driver.GraphSourceWriter); ok {
		return writer.PutGraphRecord(ctx, record, doc.Parent, links)
	}
	if writer, ok := tx.(driver.HierarchySourceWriter); ok {
		return writer.PutHierarchyRecord(ctx, record, doc.Parent)
	}
	return tx.Put(ctx, record)
}
