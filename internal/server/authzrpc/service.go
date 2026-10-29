package authzrpc

import (
	"context"
	"errors"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// GuardedService keeps the domain service intact while adding the read-side
// authorization boundary that the generic service intentionally does not own.
// Writes remain authorized by the same Checker installed in pipeline.Writer.
type GuardedService struct {
	delegate    rpc.Service
	checker     *Checker
	commitEmpty func(context.Context, model.PrincipalRef, service.ClaimNextRequest, model.WriteMeta) error
}

var _ rpc.Service = (*GuardedService)(nil)

const candidateCap = 20000

func (s *GuardedService) Get(ctx context.Context, by model.PrincipalRef, req service.GetRequest) (service.GetResult, error) {
	allowed, err := s.checkRead(ctx, by, "bead.get", req.ID)
	if err != nil {
		return service.GetResult{}, err
	}
	if !allowed {
		return service.GetResult{}, notFound()
	}
	return s.delegate.Get(ctx, by, req)
}

func (s *GuardedService) Ready(ctx context.Context, by model.PrincipalRef, req service.ReadyRequest) (service.BeadPage, error) {
	if s == nil || s.delegate == nil || s.checker == nil {
		return service.BeadPage{}, &model.Error{Message: "authorization service is unavailable", Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	// Ask the real service for a complete bounded candidate page, then apply
	// the compiled fixed filter before pagination. A truncated candidate set is
	// refused; it never becomes a short page or an uncounted hidden tail.
	full := req
	full.Limit = 1000
	full.Cursor = ""
	full.Position = nil
	page, err := s.delegate.Ready(ctx, by, full)
	if err != nil {
		return service.BeadPage{}, err
	}
	items := append([]service.Bead(nil), page.Items...)
	for page.Next != nil {
		if len(items) >= candidateCap {
			return service.BeadPage{}, postfilterCap()
		}
		position := *page.Next
		full.Position = &position
		page, err = s.delegate.Ready(ctx, by, full)
		if err != nil {
			return service.BeadPage{}, err
		}
		items = append(items, page.Items...)
	}
	if len(items) > candidateCap {
		return service.BeadPage{}, postfilterCap()
	}
	page.Items = items
	page.Next = nil
	return s.filterPage(ctx, by, "bead.ready", req.Limit, req.Position, page, false)
}

func (s *GuardedService) History(ctx context.Context, by model.PrincipalRef, req service.HistoryRequest) (service.BeadPage, error) {
	if s == nil || s.delegate == nil || s.checker == nil {
		return service.BeadPage{}, &model.Error{Message: "authorization service is unavailable", Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	full := req
	full.Limit = 1000
	full.Cursor = ""
	full.Position = nil
	page, err := s.delegate.History(ctx, by, full)
	if err != nil {
		return service.BeadPage{}, err
	}
	items := append([]service.Bead(nil), page.Items...)
	for page.Next != nil {
		if len(items) >= candidateCap {
			return service.BeadPage{}, postfilterCap()
		}
		position := *page.Next
		full.Position = &position
		page, err = s.delegate.History(ctx, by, full)
		if err != nil {
			return service.BeadPage{}, err
		}
		items = append(items, page.Items...)
	}
	if len(items) > candidateCap {
		return service.BeadPage{}, postfilterCap()
	}
	page.Items = items
	page.Next = nil
	return s.filterPage(ctx, by, "history.query", req.Limit, req.Position, page, true)
}

func (s *GuardedService) CreateWithOutcome(ctx context.Context, by model.PrincipalRef, req service.CreateRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.delegate.CreateWithOutcome(ctx, by, req, meta)
}
func (s *GuardedService) UpdateWithOutcome(ctx context.Context, by model.PrincipalRef, req service.UpdateRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.delegate.UpdateWithOutcome(ctx, by, req, meta)
}
func (s *GuardedService) ClaimWithOutcome(ctx context.Context, by model.PrincipalRef, req service.ClaimRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.delegate.ClaimWithOutcome(ctx, by, req, meta)
}

// ClaimNextWithOutcome keeps queue candidates visible to an Agent by applying
// the modify permit to each ready bead. Guarded Ready intentionally uses read
// authorization and therefore cannot be reused for this write operation.
func (s *GuardedService) ClaimNextWithOutcome(ctx context.Context, by model.PrincipalRef, req service.ClaimNextRequest, meta model.WriteMeta) (*port.WriteOutcome, error) {
	if s == nil || s.delegate == nil || s.checker == nil {
		return nil, &model.Error{Message: "authorization service is unavailable", Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	identity, err := service.ClaimNextRequestIdentity(req)
	if err != nil {
		return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: "invalid claimNext selector"}}
	}
	meta.ClaimNextIdentity = identity
	meta.Method = "bead.claimNext"
	if by.Kind != "anonymous" {
		meta.AgentLabel = ""
	} else if meta.AgentLabel == "" {
		meta.AgentLabel = meta.Agent
	}
	if meta.IdempotencyKey != "" {
		principal, err := driver.PrincipalKey(by)
		if err != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params"}}
		}
		reader, _, closeReader, err := s.current(ctx, nil)
		if err != nil {
			return nil, err
		}
		entry, lookupErr := reader.IdempotencyByKey(ctx, principal, meta.AgentLabel, "bead.claimNext", meta.IdempotencyKey)
		if lookupErr != nil {
			closeReader()
			return nil, lookupErr
		}
		closeReader()
		if entry != nil {
			// Let the shared domain replay path perform the request-hash check
			// before current authorization. A selector change must be reported as
			// idempotency_key_reused, while an equal receipt is reauthorized by the
			// installed Writer checker.
			return s.delegate.ClaimNextWithOutcome(ctx, by, req, meta)
		}
	}
	position := (*service.PagePosition)(nil)
	fresh := false
	for {
		page, err := s.delegate.Ready(ctx, by, service.ReadyRequest{Project: req.Project, ViewID: req.ViewID, Filter: req.Filter, Limit: 1000, Position: position})
		if err != nil {
			return nil, err
		}
		for _, bead := range page.Items {
			ref := model.RefByID(bead.ID, model.Natural64(0))
			_, snapshot, closeReader, err := s.current(ctx, &service.AsOf{Revision: page.AsOf.Revision, Incarnation: page.AsOf.Incarnation})
			if err != nil {
				return nil, err
			}
			allowed, decisionErr := s.checker.modifyDecision(ctx, snapshot, by, "bead.claimNext", ref)
			closeReader()
			if decisionErr != nil {
				return nil, decisionErr
			}
			if !allowed {
				continue
			}
			candidateMeta := meta
			candidateMeta.Method = "bead.claimNext"
			out, claimErr := s.delegate.ClaimWithOutcome(ctx, by, service.ClaimRequest{ID: ref}, candidateMeta)
			if claimErr == nil {
				return &out, nil
			}
			if !claimNextRace(claimErr) {
				return nil, claimErr
			}
		}
		if page.Next == nil {
			if !fresh {
				fresh = true
				position = nil
				continue
			}
			if s.commitEmpty == nil {
				return nil, &model.Error{Message: "authorization service is unavailable", Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
			}
			if err := s.commitEmpty(ctx, by, req, meta); err != nil {
				return nil, err
			}
			return nil, nil
		}
		position = page.Next
	}
}

func claimNextRace(err error) bool {
	var value *model.Error
	if !errors.As(err, &value) {
		return false
	}
	switch value.Data.Code {
	case "forbidden", "not_found", "busy", "write_conflict", "precondition_failed", "ontology_guard_failed", "lease_not_held", "lease_expired":
		return true
	default:
		return false
	}
}
func (s *GuardedService) CommentWithOutcome(ctx context.Context, by model.PrincipalRef, req service.CommentRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.delegate.CommentWithOutcome(ctx, by, req, meta)
}
func (s *GuardedService) CloseWithOutcome(ctx context.Context, by model.PrincipalRef, req service.CloseRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.delegate.CloseWithOutcome(ctx, by, req, meta)
}

func (s *GuardedService) checkRead(ctx context.Context, by model.PrincipalRef, method string, ref model.Ref) (bool, error) {
	_, snapshot, closeReader, err := s.current(ctx, nil)
	if err != nil {
		return false, err
	}
	defer closeReader()
	return s.checker.readDecision(ctx, snapshot, by, method, ref)
}

func (s *GuardedService) current(ctx context.Context, asOf *service.AsOf) (driver.Reader, Snapshot, func(), error) {
	if s == nil || s.checker == nil || s.checker.store == nil {
		return nil, nil, func() {}, errors.New("authorization store is unavailable")
	}
	meta, err := s.checker.store.Meta(ctx)
	if err != nil {
		return nil, nil, func() {}, err
	}
	view := driver.View{Incarnation: meta.Incarnation}
	if asOf != nil {
		if err := asOf.Revision.Validate(); err != nil {
			return nil, nil, func() {}, &model.Error{Message: "invalid asOf revision", Data: model.ErrorData{Code: "invalid_params"}}
		}
		view.At = &asOf.Revision
		if asOf.Incarnation != "" {
			view.Incarnation = asOf.Incarnation
		}
	}
	reader, err := s.checker.store.Read(ctx, view)
	if err != nil {
		return nil, nil, func() {}, err
	}
	return reader, reader, func() { _ = reader.Close() }, nil
}

func (s *GuardedService) filterPage(ctx context.Context, by model.PrincipalRef, method string, limit int, position *service.PagePosition, page service.BeadPage, pinned bool) (service.BeadPage, error) {
	var authorizationAt *service.AsOf
	if !pinned {
		authorizationAt = &service.AsOf{Revision: page.AsOf.Revision, Incarnation: page.AsOf.Incarnation}
	}
	_, snapshot, closeReader, err := s.current(ctx, authorizationAt)
	if err != nil {
		return service.BeadPage{}, err
	}
	defer closeReader()
	caller, err := s.checker.callerFor(ctx, by)
	if err != nil {
		return service.BeadPage{}, authzError(err)
	}
	facts, err := s.checker.load(ctx, snapshot, caller)
	if err != nil {
		return service.BeadPage{}, &model.Error{Message: "authorization facts unavailable", Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	// History visibility is current bead read authorization; fixed CP7's
	// history resource does not create a second permission vocabulary.
	resourceKind := authz.Bead
	compiledMethod := method
	if method == "history.query" {
		compiledMethod = "bead.get"
	}
	template, err := s.checker.chain.Compile(authz.ListRequest{Caller: caller, Action: authz.Read, ResourceKind: resourceKind, Method: compiledMethod})
	if err != nil {
		return service.BeadPage{}, authzError(err)
	}
	filtered := make([]service.Bead, 0, len(page.Items))
	for _, bead := range page.Items {
		ok, err := template.Match(ctx, facts, authz.Resource{Kind: authz.Bead, Ref: model.RefByID(bead.ID, model.Natural64(0))})
		if err != nil {
			return service.BeadPage{}, authzError(err)
		}
		if ok {
			filtered = append(filtered, bead)
		}
	}
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 1000 {
		return service.BeadPage{}, &model.Error{Message: "limit outside 1..1000", Data: model.ErrorData{Code: "invalid_params"}}
	}
	offset := 0
	if position != nil {
		if position.Offset < 0 || position.Offset > len(filtered) || position.AsOf.Incarnation != page.AsOf.Incarnation {
			return service.BeadPage{}, &model.Error{Message: "page position does not match authorized snapshot", Data: model.ErrorData{Code: "cursor_invalid"}}
		}
		if pinned && position.AsOf.Revision.String() != page.AsOf.Revision.String() {
			return service.BeadPage{}, &model.Error{Message: "history page does not match requested asOf", Data: model.ErrorData{Code: "cursor_invalid"}}
		}
		offset = position.Offset
	}
	end := offset + limit
	if end > len(filtered) {
		end = len(filtered)
	}
	out := service.BeadPage{Items: append([]service.Bead(nil), filtered[offset:end]...), AsOf: page.AsOf}
	if end < len(filtered) {
		out.Next = &service.PagePosition{Offset: end, AsOf: page.AsOf}
	}
	return out, nil
}

func postfilterCap() error {
	return &model.Error{Message: "authorization post-filter candidate cap exceeded", Data: model.ErrorData{Code: "authz_postfilter_cap"}}
}
