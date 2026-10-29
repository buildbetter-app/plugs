package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
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
	scanLimit := req.Changesets
	if !req.After.Revision.IsZero() && scanLimit == 1 {
		// Rereading the acknowledged revision consumes a scan slot. Keep
		// room for its successor so a one-changeset page can advance.
		scanLimit++
	}
	sets, err := r.Log.Scan(ctx, req.After, scanLimit)
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
		if err := set.ValidateEvents(); err != nil {
			return page, fmt.Errorf("invalid committed event plan: %w", err)
		}
		count := len(set.Changes)
		if set.Events != nil {
			count = len(set.Events)
		}
		start := 0
		if equalNatural(set.Revision, req.After.Revision) {
			index, err := req.After.Index.Uint64()
			if err != nil || (count > 0 && index >= uint64(count)) || (count == 0 && index != 0) {
				return page, ErrCursorInvalid
			}
			start = int(index) + 1
		}
		group := Group{Revision: set.Revision, Time: set.Time, Events: []model.Event{}, End: page.Position}
		// Empty changesets are durable receipts. Their scan marker is index 0;
		// they advance delivery without inventing a graph event.
		if count == 0 {
			group.End = Position{Incarnation: set.Incarnation, Revision: set.Revision}
			page.Position = group.End
		}
		boundary := ""
		for i := start; i < count; i++ {
			position := Position{Incarnation: set.Incarnation, Revision: set.Revision, Index: model.Natural64(uint64(i))}
			notice := set.Events != nil && set.Events[i].Type == model.EventDependencyConditionUnmet
			if notice {
				allowed, err := r.noticeReadable(ctx, req.Principal, set.Events[i])
				if err != nil {
					return page, err
				}
				if !allowed {
					group.End, page.Position = position, position
					continue
				}
			}
			durable, err := r.Materializer.Materialize(ctx, set, i)
			if notice && errors.Is(err, ErrNoticeUnavailable) {
				group.End, page.Position = position, position
				continue
			}
			if err != nil {
				return page, err
			}
			if set.Events != nil {
				d := set.Events[i]
				if durable.Type != d.Type || durable.Resource != d.Resource.Kind+"/"+d.Resource.Ref.ID ||
					!reflect.DeepEqual(durable.Details, d.Details) || !reflect.DeepEqual(durable.SetBy, d.Provenance.SetBy) ||
					!reflect.DeepEqual(durable.Chain, d.Provenance.Chain) || !reflect.DeepEqual(durable.Cause, d.Provenance.Cause) ||
					!reflect.DeepEqual(durable.Principal, set.Principal) || durable.Derived != (set.Kind == model.ChangesetDerived) || (notice && len(durable.Changed) != 0) {
					return page, fmt.Errorf("materialized event differs from committed descriptor")
				}
			} else if durable.Type == model.EventDependencyConditionUnmet {
				return page, fmt.Errorf("condition notice requires a committed descriptor")
			}
			durable.EventPosition = eventPosition(position)
			protected, err := cloneDurable(durable)
			if err != nil {
				return page, fmt.Errorf("invalid materialized event: %w", err)
			}
			visibility, err := r.Authorization.Read(ctx, req.Principal, protected)
			if err != nil {
				return page, err
			}
			var event model.Event
			if notice && (visibility.Redacted || !reflect.DeepEqual(visibility.Before, durable.Before) || !reflect.DeepEqual(visibility.After, durable.After)) {
				visibility.Readable = false
			}
			if visibility.Readable {
				durable.Before, durable.After = visibility.Before, visibility.After
				durable, err = cloneDurable(durable)
				if err != nil {
					return page, fmt.Errorf("invalid authorized event: %w", err)
				}
				event = model.Event{Category: model.EventDurable, Durable: &durable}
			} else if !notice && visibility.PreviouslyVisible {
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

func (r *Reader) noticeReadable(ctx context.Context, principal model.PrincipalRef, descriptor model.EventDescriptor) (bool, error) {
	authorization, ok := r.Authorization.(ResourceAuthorization)
	if !ok {
		return false, ErrResourceAuthorization
	}
	resources, err := descriptor.RequiredResources()
	if err != nil {
		return false, err
	}
	for _, resource := range resources {
		visibility, err := authorization.ReadResource(ctx, principal, resource)
		if err != nil {
			return false, err
		}
		if !visibility.Readable || visibility.Redacted {
			return false, nil
		}
	}
	return true, nil
}

// The authorization port and returned pages each receive owned JSON values.
// Typed details, provenance maps/chains and images must never alias log data or
// a policy callback's mutable arguments/results. The model codec keeps numbers
// exact and validates the closed event envelope on both sides of masking.
func cloneDurable(event model.DurableEvent) (model.DurableEvent, error) {
	raw, err := json.Marshal(model.Event{Durable: &event})
	if err != nil {
		return model.DurableEvent{}, err
	}
	var copied model.Event
	if err := json.Unmarshal(raw, &copied); err != nil {
		return model.DurableEvent{}, err
	}
	return *copied.Durable, nil
}
