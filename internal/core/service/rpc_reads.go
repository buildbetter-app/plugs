package service

import (
	"context"
	"errors"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// EventListConfigured lets a composition root keep event.list unbound when it
// has not installed the real persisted event reader.
func (s *Service) EventListConfigured() bool { return s != nil && s.events != nil }

func (s *Service) EventList(ctx context.Context, by model.PrincipalRef, req EventListRequest) (EventPage, error) {
	if req.Filter == nil {
		return EventPage{}, failure("invalid_params", "event.list requires a filter")
	}
	if req.Filter.Parent != "" || req.Filter.Priority != nil || req.Filter.Query != "" || len(req.Filter.Types) != 0 || req.Filter.ViewID != "" {
		return EventPage{}, failure("invalid_params", "event.list supports only filter.events and filter.resources")
	}
	if req.Limit < 1 || req.Limit > 1000 {
		return EventPage{}, failure("invalid_params", "limit must be between 1 and 1000")
	}
	if err := s.admit(ctx, by); err != nil {
		return EventPage{}, err
	}
	if s.events == nil {
		return EventPage{}, failure("internal_error", "event reader is not configured")
	}
	page, err := s.events.List(ctx, EventProviderRequest{
		Principal: by,
		Filter: EventFilter{
			Types:     append([]string(nil), req.Filter.Events...),
			Resources: append([]string(nil), req.Filter.Resources...),
		},
		After: req.After,
		Limit: req.Limit,
	})
	if err != nil {
		return EventPage{}, mapEventError(err)
	}
	if page.Events == nil {
		page.Events = []model.Event{}
	}
	return page, nil
}

func mapEventError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, coreevents.ErrCursorInvalid) {
		return failure("cursor_invalid", "event cursor is invalid; restart from a fresh snapshot")
	}
	if errors.Is(err, coreevents.ErrResourceAuthorization) {
		return failure("authz_postfilter_cap", "event resource authorization is unavailable")
	}
	var boundary *coreevents.BoundaryError
	if errors.As(err, &boundary) && boundary != nil {
		details := map[string]any{}
		if boundary.From != "" {
			details["fromIncarnation"] = boundary.From
		}
		if boundary.Head.Incarnation != "" {
			details["toIncarnation"] = boundary.Head.Incarnation
		}
		if boundary.Head.Floor.Validate() == nil {
			details["floor"] = boundary.Head.Floor
		}
		code := model.ErrorCode(boundary.Code)
		if code != "cursor_expired" && code != "incarnation_changed" && code != "cursor_invalid" {
			code = "internal_error"
		}
		return &model.Error{Data: model.ErrorData{Code: code, Details: details}}
	}
	if code, details, ok := driver.ErrorMapping(err); ok {
		return &model.Error{Data: model.ErrorData{Code: code, Details: details}}
	}
	return err
}

// Health is read-only. The driver owns topology and backup facts; this seam
// intentionally does not acquire a transaction or mutate graph state.
func (s *Service) Health(ctx context.Context, by model.PrincipalRef) (driver.StoreHealth, error) {
	if err := s.admit(ctx, by); err != nil {
		return driver.StoreHealth{}, err
	}
	health, err := s.store.Health(ctx)
	if err != nil {
		return driver.StoreHealth{}, mapped(err)
	}
	if health.AuthorizationChain == nil {
		health.AuthorizationChain = []string{}
	}
	return health, nil
}
