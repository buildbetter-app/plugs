package events

import (
	"context"
	"fmt"

	core "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (m HistoryMaterializer) materializeDescriptor(ctx context.Context, set model.Changeset, index int) (model.DurableEvent, error) {
	var event model.DurableEvent
	if index < 0 || index >= len(set.Events) {
		return event, core.ErrCursorInvalid
	}
	// Reader checks the whole plan once. Check this entry/anchor as well for
	// direct materializer callers, without revalidating N entries N times.
	d := model.CloneEventDescriptors(set.Events[index : index+1])[0]
	if err := d.Validate(); err != nil {
		return event, err
	}
	i, _ := d.Index.Uint64()
	anchor, _ := d.ChangeIndex.Uint64()
	if i != uint64(index) || anchor >= uint64(len(set.Changes)) {
		return event, core.ErrCursorInvalid
	}
	change := set.Changes[anchor]
	for _, path := range change.ChangedPaths {
		if !validJSONPointer(path) {
			return event, fmt.Errorf("committed changed path is not a JSON Pointer: %w", driver.ErrUnsupported)
		}
	}
	event = model.DurableEvent{Type: d.Type, Resource: d.Resource.Kind + "/" + d.Resource.Ref.ID, Changed: []string{}, Principal: set.Principal, Chain: d.Provenance.Chain, SetBy: d.Provenance.SetBy, Cause: d.Provenance.Cause, Derived: set.Kind == model.ChangesetDerived, Details: d.Details}
	if d.Type == model.EventDependencyConditionUnmet {
		// The unchanged dependent is only a source pin in Details. Notices
		// forbid before/after envelopes and never imply a graph mutation.
		if _, err := m.pinnedImage(ctx, set.Incarnation, d.Details.Dependent, "bead"); err != nil {
			return event, err
		}
		for _, pin := range []model.EventImage{d.Details.BlockerBefore, d.Details.BlockerAfter} {
			if _, err := m.pinnedImage(ctx, set.Incarnation, pin, "bead"); err != nil {
				return event, err
			}
		}
		if _, err := m.pinnedImage(ctx, set.Incarnation, model.EventImage{Ref: d.Details.Ontology.Ref, Revision: d.Details.Ontology.Revision}, "ontology"); err != nil {
			return event, err
		}
		return event, nil
	}
	if change.Ref.ID != d.Resource.Ref.ID || !change.Ref.IsID() || !change.Ref.Branch.IsZero() {
		return event, fmt.Errorf("descriptor changed-resource mismatch")
	}
	var before, after *driver.Record
	var err error
	if !change.FromRevision.IsZero() {
		before, err = m.pinnedImage(ctx, set.Incarnation, model.EventImage{Ref: change.Ref, Revision: change.FromRevision}, d.Resource.Kind)
		if err != nil {
			return event, err
		}
	}
	if change.Op != model.OpDelete {
		after, err = m.pinnedImage(ctx, set.Incarnation, model.EventImage{Ref: change.Ref, Revision: set.Revision}, d.Resource.Kind)
		if err != nil {
			return event, err
		}
	}
	event.Before, err = body(before)
	if err != nil {
		return event, err
	}
	event.After, err = body(after)
	event.Changed = append([]string{}, change.ChangedPaths...)
	return event, err
}

func (m HistoryMaterializer) pinnedImage(ctx context.Context, incarnation string, pin model.EventImage, kind string) (*driver.Record, error) {
	record, err := m.image(ctx, incarnation, pin.Ref, pin.Revision)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, core.ErrNoticeUnavailable
	}
	cmp, err := core.CompareNatural(record.Revision, pin.Revision)
	if err != nil || cmp != 0 || record.Kind != kind || record.Ref.ID != pin.Ref.ID || !record.Ref.IsID() || !record.Ref.Branch.IsZero() {
		return nil, fmt.Errorf("committed event source pin mismatch")
	}
	return record, nil
}
