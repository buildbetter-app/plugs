package events

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

type Reader struct {
	Log           Log
	Materializer  Materializer
	Authorization Authorization
}

type ReadRequest struct {
	After     Position
	Principal model.PrincipalRef
	Filter    Filter
	Limit     int
	// WholeChangesets is for projections, which may not acknowledge a prefix.
	WholeChangesets bool
	Changesets      int
	OneChangeset    bool
}

type Group struct {
	Revision model.Revision
	Time     time.Time
	Events   []model.Event
	End      Position
}
type Page struct {
	Groups   []Group
	Position Position
	Boundary string
}

func (p Page) Events() []model.Event {
	result := []model.Event{}
	for _, group := range p.Groups {
		result = append(result, group.Events...)
	}
	return result
}

func (r *Reader) Read(ctx context.Context, req ReadRequest) (Page, error) {
	page := Page{Position: req.After}
	if r.Log == nil || r.Materializer == nil || r.Authorization == nil {
		return page, fmt.Errorf("event reader requires log, materializer and authorization")
	}
	if req.Limit < 1 || req.Limit > 1000 {
		return page, fmt.Errorf("event limit must be 1..1000")
	}
	if req.Changesets == 0 {
		req.Changesets = 64
	}
	if req.Changesets < 1 || req.Changesets > 1000 {
		return page, fmt.Errorf("changeset limit must be 1..1000")
	}
	head, err := r.Log.Head(ctx)
	if err != nil {
		return page, err
	}
	if req.After.Incarnation == "" {
		if !req.After.Revision.IsZero() || !req.After.Index.IsZero() {
			return page, ErrCursorInvalid
		}
		req.After.Incarnation = head.Incarnation
		page.Position = req.After
	}
	if err := req.After.Validate(); err != nil {
		return page, err
	}
	if req.After.Incarnation != head.Incarnation {
		return page, &BoundaryError{Code: "incarnation_changed", From: req.After.Incarnation, Head: head, Reason: "failover"}
	}
	cmp, err := CompareNatural(req.After.Revision, head.Floor)
	if err != nil {
		return page, err
	}
	if cmp < 0 {
		return page, &BoundaryError{Code: "cursor_expired", From: req.After.Incarnation, Head: head, Reason: "history_floor"}
	}
	cmp, err = CompareNatural(req.After.Revision, head.Revision)
	if err != nil {
		return page, err
	}
	if cmp > 0 {
		return page, &BoundaryError{Code: "cursor_invalid", From: req.After.Incarnation, Head: head, Reason: "checkpoint"}
	}
	sets, err := r.Log.Scan(ctx, req.After, req.Changesets)
	if err != nil {
		return page, err
	}
	if !req.After.Revision.IsZero() && (len(sets) == 0 || !equalNatural(sets[0].Revision, req.After.Revision)) {
		return page, ErrCursorInvalid
	}
	previous := req.After.Revision
	visible := 0
	for si, set := range sets {
		if err := ctx.Err(); err != nil {
			return page, err
		}
		cmp, err = CompareNatural(set.Revision, previous)
		if err != nil || set.Incarnation != head.Incarnation || set.Revision.IsZero() || cmp < 0 || (si > 0 && cmp == 0) {
			return page, fmt.Errorf("invalid committed changeset order")
		}
		previous = set.Revision
		start := 0
		if equalNatural(set.Revision, req.After.Revision) {
			index, err := req.After.Index.Uint64()
			if err != nil || (len(set.Changes) > 0 && index >= uint64(len(set.Changes))) || (len(set.Changes) == 0 && index != 0) {
				return page, ErrCursorInvalid
			}
			start = int(index) + 1
		}
		group := Group{Revision: set.Revision, Time: set.Time, Events: []model.Event{}, End: page.Position}
		// Empty changesets are durable receipts. Their scan marker is index 0;
		// they advance delivery without inventing a graph event.
		if len(set.Changes) == 0 {
			group.End = Position{Incarnation: set.Incarnation, Revision: set.Revision}
			page.Position = group.End
		}
		boundary := ""
		for i := start; i < len(set.Changes); i++ {
			durable, err := r.Materializer.Materialize(ctx, set, i)
			if err != nil {
				return page, err
			}
			position := Position{Incarnation: set.Incarnation, Revision: set.Revision, Index: model.Natural64(uint64(i))}
			durable.EventPosition = eventPosition(position)
			if _, err = json.Marshal(model.Event{Durable: &durable}); err != nil {
				return page, fmt.Errorf("invalid materialized event: %w", err)
			}
			visibility, err := r.Authorization.Read(ctx, req.Principal, durable)
			if err != nil {
				return page, err
			}
			var event model.Event
			if visibility.Readable {
				durable.Before, durable.After = visibility.Before, visibility.After
				event = model.Event{Category: model.EventDurable, Durable: &durable}
			} else if visibility.PreviouslyVisible {
				event = model.Event{Category: model.EventControl, Control: &model.ControlEvent{EventPosition: eventPosition(position), Type: model.EventVisibilityLost, Resource: durable.Resource, Details: model.VisibilityLostDetails{Reason: "authorization_changed"}}}
			}
			if event.Durable != nil || event.Control != nil {
				if req.Filter.accepts(event) {
					group.Events = append(group.Events, event)
					visible++
				}
			}
			group.End = position
			page.Position = position
			switch durable.Type {
			case model.EventOntologyApplied:
				boundary = "ontology_changed"
			case model.EventPluginApplied:
				boundary = "policy_changed"
			case model.EventViewSaved:
				boundary = "view_changed"
			case model.EventViewDeleted:
				boundary = "view_deleted"
			}
			if visible >= req.Limit && !req.WholeChangesets {
				page.Groups = append(page.Groups, group)
				page.Boundary = boundary
				return page, nil
			}
		}
		page.Groups = append(page.Groups, group)
		if req.OneChangeset && !samePosition(group.End, req.After) {
			page.Boundary = boundary
			return page, nil
		}
		if boundary != "" {
			page.Boundary = boundary
			return page, nil
		}
		if visible >= req.Limit {
			return page, nil
		}
	}
	return page, nil
}
