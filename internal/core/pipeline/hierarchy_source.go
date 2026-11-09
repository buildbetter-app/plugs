package pipeline

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/model"

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
	links, err := localSourceLinks(doc.Links)
	if err != nil {
		return err
	}
	if writer, ok := tx.(driver.GraphSourceWriter); ok {
		return writer.PutGraphRecord(ctx, record, doc.Parent, links)
	}
	if writer, ok := tx.(driver.HierarchySourceWriter); ok {
		return writer.PutHierarchyRecord(ctx, record, doc.Parent)
	}
	return tx.Put(ctx, record)
}

// External edges remain in the exact source body, outside local inverse facts.
func localSourceLinks(links []Link) ([]driver.SourceLink, error) {
	local := make([]driver.SourceLink, 0, len(links))
	for _, link := range links {
		if !validDependencyKind(link.Kind) || model.ValidateLinkTarget(link.TargetKind, link.To) != nil {
			return nil, failure("ontology_guard_failed", "invalid hierarchy source link")
		}
		if link.TargetKind == model.LinkTargetExternal {
			continue
		}
		if driver.ValidateHierarchyRef(link.To) != nil {
			return nil, failure("ontology_guard_failed", "invalid hierarchy source link")
		}
		local = append(local, driver.SourceLink{Kind: link.Kind, To: link.To})
	}
	return local, nil
}
