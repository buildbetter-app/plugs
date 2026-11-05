package pipeline

import (
	"context"
	"errors"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

var hierarchyIncomingKinds = []driver.IncomingLinkKind{
	driver.IncomingLinkParentChild,
	driver.IncomingLinkBlocks,
	driver.IncomingLinkConditionalBlocks,
	driver.IncomingLinkWaitsFor,
}

// observeIndexedGraphIncoming retains the selected inverse-link facts in the
// same pinned read set as canonical children. A true result means the changed
// hierarchy sources have no scheduling or legacy parent-child links, so this
// hierarchy-only write preserves the dependency graph outside the selected
// sources. This local observation is NOT proof of global validity: skipping
// complete revalidation also requires a separate, current semantic certificate.
func (p *prepared) observeIndexedGraphIncoming(ctx context.Context) (bool, error) {
	s := p.hierarchySources
	if s == nil || len(p.hierarchyEdits) == 0 {
		return false, nil
	}
	for _, change := range p.proposal.Changes {
		switch change.Op.Kind {
		case model.OpDelete, model.OpLink, model.OpUnlink:
			// General deletion needs every custom/association incoming kind.
			return false, nil
		}
	}
	for _, row := range s.stored {
		if len(row.Links) != 0 {
			return false, nil
		}
	}
	for _, row := range s.current {
		if len(row.Links) != 0 {
			return false, nil
		}
	}
	for _, pending := range p.writes {
		if pending.deleted || len(pending.doc.Links) != 0 {
			return false, nil
		}
		if len(pending.before) != 0 {
			before, err := DecodeDocument(pending.before)
			if err != nil {
				return false, err
			}
			if len(before.Links) != 0 {
				return false, nil
			}
		}
	}
	ids := make(map[string]bool, len(s.records)+len(s.current))
	for id := range s.records {
		ids[id] = true
	}
	for id := range s.current {
		ids[id] = true
	}
	refs := hierarchyRefs(ids)
	if len(refs) == 0 {
		// Coverage is required even for an empty fact domain.
		_, _, err := s.snapshot.InverseSourceLinks(ctx, nil, hierarchyIncomingKinds, nil, 0)
		return unsupportedInverse(err)
	}
	for start := 0; start < len(refs); start += driver.SourceLinkMaxTargets {
		end := start + driver.SourceLinkMaxTargets
		if end > len(refs) {
			end = len(refs)
		}
		rows, more, err := s.snapshot.InverseSourceLinks(ctx, refs[start:end], hierarchyIncomingKinds, nil, 0)
		if errors.Is(err, driver.ErrUnsupported) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if len(rows) != 0 || more {
			return false, nil
		}
	}
	return true, nil
}

func unsupportedInverse(err error) (bool, error) {
	if errors.Is(err, driver.ErrUnsupported) {
		return false, nil
	}
	return false, err
}
