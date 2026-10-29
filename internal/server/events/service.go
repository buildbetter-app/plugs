package events

import (
	"context"
	"errors"

	core "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
)

// Service is transport independent: an RPC handler can pull, and a separate
// subscription transport can poll after its authorized snapshot. It does not
// expose a network listener or claim to implement the snapshot/watch registry.
type Service struct {
	Reader  *core.Reader
	Cursors *CursorCodec
}
type Request struct {
	Principal      model.PrincipalRef
	Filter         core.Filter
	After          string
	Limit          int
	Ontology       model.Revision
	PolicyVersion  string
	SubscriptionID string
}
type Page struct {
	Events []model.Event `json:"events"`
	Cursor string        `json:"cursor"`
	Reset  bool          `json:"-"`
}

func (s Service) List(ctx context.Context, req Request) (Page, error) {
	return s.read(ctx, "event.list", req)
}
func (s Service) Poll(ctx context.Context, req Request) (Page, error) {
	return s.read(ctx, "event.subscribe", req)
}
func (s Service) read(ctx context.Context, method string, req Request) (Page, error) {
	var result Page
	binding, err := BindingFor(method, req.Filter, req.Principal, req.Ontology, req.PolicyVersion)
	if err != nil {
		return result, err
	}
	var after core.Position
	if req.After != "" {
		after, err = s.Cursors.Decode(req.After, binding)
		if err != nil {
			return result, err
		}
	}
	page, err := s.Reader.Read(ctx, core.ReadRequest{After: after, Principal: req.Principal, Filter: req.Filter, Limit: req.Limit})
	if err != nil {
		var boundary *core.BoundaryError
		if method != "event.subscribe" || !errors.As(err, &boundary) {
			return result, err
		}
		// A gap is never suppressed by resource or event-type filters. Its
		// cursor is a reopen marker; the caller must take a fresh snapshot.
		position := core.Position{Incarnation: boundary.Head.Incarnation, Revision: boundary.Head.Revision}
		if !position.Revision.IsZero() {
			sets, scanErr := s.Reader.Log.Scan(ctx, position, 1)
			if scanErr != nil {
				return result, scanErr
			}
			if len(sets) != 1 {
				return result, core.ErrCursorInvalid
			}
			if len(sets[0].Changes) > 0 {
				position.Index = model.Natural64(uint64(len(sets[0].Changes) - 1))
			}
		}
		cursor, encodeErr := s.Cursors.Encode(position, binding)
		if encodeErr != nil {
			return result, encodeErr
		}
		control := model.ControlEvent{EventPosition: model.EventPosition{Cursor: cursor, Incarnation: position.Incarnation, Revision: position.Revision, Index: position.Index}, Type: model.EventGap, Details: model.EventGapDetails{FromIncarnation: boundary.From, ToIncarnation: position.Incarnation, Floor: boundary.Head.Floor, Reason: boundary.Reason}}
		return Page{Events: []model.Event{{Category: model.EventControl, Control: &control}}, Cursor: cursor, Reset: true}, nil
	}
	result.Events = page.Events()
	for i := range result.Events {
		position := core.PositionOf(result.Events[i])
		cursor, err := s.Cursors.Encode(position, binding)
		if err != nil {
			return Page{}, err
		}
		if result.Events[i].Durable != nil {
			result.Events[i].Durable.Cursor = cursor
		} else {
			result.Events[i].Control.Cursor = cursor
		}
	}
	result.Cursor, err = s.Cursors.Encode(page.Position, binding)
	if err != nil {
		return Page{}, err
	}
	if method == "event.subscribe" && page.Boundary != "" {
		position := page.Position
		reset := model.ControlEvent{EventPosition: model.EventPosition{Cursor: result.Cursor, Incarnation: position.Incarnation, Revision: position.Revision, Index: position.Index}, Type: model.EventWatchReset, Details: model.WatchResetDetails{SubscriptionID: req.SubscriptionID, Reason: page.Boundary, SnapshotCursor: result.Cursor}}
		if requestsType(req.Filter, "watch.reset") {
			result.Events = append(result.Events, model.Event{Category: model.EventControl, Control: &reset})
		}
		result.Reset = true
	}
	return result, nil
}

func requestsType(filter core.Filter, kind string) bool {
	if len(filter.Types) == 0 {
		return true
	}
	for _, value := range filter.Types {
		if value == kind {
			return true
		}
	}
	return false
}
