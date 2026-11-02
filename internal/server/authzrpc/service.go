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
	delegate        rpc.Service
	checker         *Checker
	lease           driver.ActiveLease
	commitEmpty     func(context.Context, model.PrincipalRef, service.ClaimNextRequest, model.WriteMeta, port.ClaimNextReadyProbe) (port.WriteOutcome, error)
	claimNextPage   func(context.Context, model.PrincipalRef, service.ReadyRequest) (service.BeadPage, error)
	readyCandidates func(context.Context, service.ReadyRequest, driver.Reader) ([]service.Bead, error)
}

var _ rpc.Service = (*GuardedService)(nil)

// listPostfilterCap remains the adopted cap for ordinary authorized list
// post-filtering (P84). ClaimNext never passes through this list path and is
// intentionally uncapped.
const listPostfilterCap = 20000

// Keep the guarded duplicate claimNext walk bounded when the same permitted
// candidate repeatedly exhausts the Writer's snapshot attempts. The pager
// still visits every distinct candidate; this only limits extra refetch passes.
const claimNextConflictRestartBudget = 1

func (s *GuardedService) Get(ctx context.Context, by model.PrincipalRef, req service.GetRequest) (service.GetResult, error) {
	allowed, err := s.checkRead(ctx, by, "bead.get", req.ID)
	if err != nil {
		return service.GetResult{}, err
	}
	if !allowed {
		return service.GetResult{}, notFound()
	}
	content, _, closeContent, err := s.current(ctx, req.AsOf)
	if err != nil {
		return service.GetResult{}, err
	}
	defer closeContent()
	view := content.Snapshot()
	req.AsOf = &service.AsOf{Revision: *view.At, Incarnation: view.Incarnation}
	result, err := s.delegate.Get(ctx, by, req)
	if err != nil {
		return service.GetResult{}, err
	}
	parents, err := s.checker.AuthorizeParentProjections(ctx, by, content, []model.Ref{req.ID})
	if err != nil {
		return service.GetResult{}, err
	}
	projectBeadParent(&result.Bead, parents.Parents[req.ID.ID])
	if err := s.checker.ValidateReadAuthorityBinding(ctx, by, parents.PolicyBinding); err != nil {
		return service.GetResult{}, err
	}
	return result, nil
}

// AuthorizeReadBatch is a count producer adapter to the existing finite read
// filter. It keeps one fresh current authority snapshot for this call, verifies
// current row existence, and fences the authority before returning decisions.
// It never loads item payload projections or retains decisions across calls.
func (s *GuardedService) AuthorizeReadBatch(ctx context.Context, by model.PrincipalRef, resources []model.ResourceRef) ([]bool, error) {
	refs := make([]model.Ref, len(resources))
	for i, resource := range resources {
		if resource.Kind != "bead" || resource.Ref.Validate() != nil || !resource.Ref.IsID() || !resource.Ref.Branch.IsZero() {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: "batch read requires resolved bead references"}}
		}
		refs[i] = resource.Ref
	}
	current, snapshot, closeReader, err := s.current(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer closeReader()
	caller, err := s.checker.callerFor(ctx, by)
	if err != nil {
		return nil, authzError(err)
	}
	authority, err := s.checker.load(ctx, snapshot, caller)
	if err != nil {
		return nil, &model.Error{Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	// This is the same existing compiled read path as filterPage. Unsupported
	// compilation refuses; it never falls back to an unprotected driver count.
	template, err := s.checker.chain.Compile(authz.ListRequest{Caller: caller, Action: authz.Read, ResourceKind: authz.Bead, Method: "bead.get"})
	if err != nil {
		return nil, authzError(err)
	}
	rows, err := current.Get(ctx, refs)
	if err != nil {
		return nil, err
	}
	present := make(map[string]bool, len(rows))
	for _, row := range rows {
		if row.Kind == "bead" {
			present[row.Ref.ID] = true
		}
	}
	allowed := make([]bool, len(resources))
	for i, ref := range refs {
		if !present[ref.ID] {
			continue
		}
		allowed[i], err = template.Match(ctx, authority, authz.Resource{Kind: authz.Bead, Ref: ref})
		if err != nil {
			return nil, authzError(err)
		}
	}
	binding, err := s.checker.readAuthorityBinding(ctx, caller, authority, snapshot)
	if err != nil {
		return nil, err
	}
	if err := s.checker.ValidateReadAuthorityBinding(ctx, by, binding); err != nil {
		return nil, err
	}
	return allowed, nil
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
		if len(items) >= listPostfilterCap {
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
	if len(items) > listPostfilterCap {
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
		if len(items) >= listPostfilterCap {
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
	if len(items) > listPostfilterCap {
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
func (s *GuardedService) ApplyWithOutcome(ctx context.Context, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.delegate.ApplyWithOutcome(ctx, by, cs, meta)
}
func (s *GuardedService) LinkWithOutcome(ctx context.Context, by model.PrincipalRef, req service.LinkRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.delegate.LinkWithOutcome(ctx, by, req, meta)
}
func (s *GuardedService) UnlinkWithOutcome(ctx context.Context, by model.PrincipalRef, req service.LinkRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.delegate.UnlinkWithOutcome(ctx, by, req, meta)
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
	selectionSeed := service.ClaimNextSelectionSeed(identity, meta.IdempotencyKey)
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
	exhausted := map[string]error{}
	var exhaustedConflict error
	repeatedConflictThisPass := false
	conflictRestarts := 0
	for {
		page, err := s.claimPage(ctx, by, service.ReadyRequest{Project: req.Project, ViewID: req.ViewID, Filter: req.Filter, Limit: 1000, Position: position, ClaimNextSeed: selectionSeed})
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
			if claimNextCandidateConflict(claimErr) {
				if _, seen := exhausted[bead.ID]; seen {
					repeatedConflictThisPass = true
				} else {
					exhausted[bead.ID] = claimErr
					if exhaustedConflict == nil {
						exhaustedConflict = claimErr
					}
				}
			}
		}
		if page.Next == nil {
			if !fresh {
				fresh = true
				position = nil
				repeatedConflictThisPass = false
				continue
			}
			if s.commitEmpty == nil {
				return nil, &model.Error{Message: "authorization service is unavailable", Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
			}
			out, err := s.commitEmpty(ctx, by, req, meta, s.claimNextReadyProbe(by, req))
			if err != nil {
				if errors.Is(err, port.ErrClaimNextReady) {
					if repeatedConflictThisPass {
						if conflictRestarts >= claimNextConflictRestartBudget {
							return nil, exhaustedConflict
						}
						conflictRestarts++
					}
					repeatedConflictThisPass = false
					fresh = false
					position = nil
					continue
				}
				return nil, err
			}
			out.NullResult = true
			return &out, nil
		}
		position = page.Next
	}
}

func (s *GuardedService) claimPage(ctx context.Context, by model.PrincipalRef, req service.ReadyRequest) (service.BeadPage, error) {
	if s.claimNextPage != nil {
		return s.claimNextPage(ctx, by, req)
	}
	return s.delegate.Ready(ctx, by, req)
}

// claimNextReadyProbe applies the same per-candidate modify permit as the live
// walk, but reads through the transaction that already owns the head. An
// unauthorized ready bead therefore does not keep an otherwise empty queue
// from publishing its terminal null receipt.
func (s *GuardedService) claimNextReadyProbe(by model.PrincipalRef, req service.ClaimNextRequest) port.ClaimNextReadyProbe {
	return func(ctx context.Context, reader driver.Reader) (bool, error) {
		if s.readyCandidates == nil {
			return false, &model.Error{Message: "authorization service is unavailable", Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
		}
		items, err := s.readyCandidates(ctx, service.ReadyRequest{Project: req.Project, ViewID: req.ViewID, Filter: req.Filter}, reader)
		if err != nil {
			return false, err
		}
		for _, bead := range items {
			allowed, decisionErr := s.checker.modifyDecision(ctx, reader, by, "bead.claimNext", model.RefByID(bead.ID, model.Natural64(0)))
			if decisionErr != nil {
				return false, decisionErr
			}
			if allowed {
				return true, nil
			}
		}
		return false, nil
	}
}

func claimNextRace(err error) bool {
	var value *model.Error
	if !errors.As(err, &value) {
		return false
	}
	switch value.Data.Code {
	case "forbidden", "not_found", "precondition_failed", "ontology_guard_failed":
		return true
	case "busy":
		// A live lease on this candidate is an ordinary queue race. Store
		// admission timeouts, global lease loss and other busy errors must
		// propagate instead of becoming a false terminal null.
		return value.Message == "bead already has a live claim"
	case "write_conflict":
		// A selected candidate can exhaust the writer's bounded snapshot
		// retries while the queue is contended. Advance to the next candidate;
		// other/global write failures retain their typed error.
		return claimNextCandidateConflict(value)
	default:
		return false
	}
}

func claimNextCandidateConflict(err error) bool {
	var value *model.Error
	if !errors.As(err, &value) || value.Data.Code != "write_conflict" {
		return false
	}
	return value.Message == "snapshot changed during all four write attempts" || value.Data.Hint == "snapshot changed during all four write attempts"
}
func (s *GuardedService) CommentWithOutcome(ctx context.Context, by model.PrincipalRef, req service.CommentRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.delegate.CommentWithOutcome(ctx, by, req, meta)
}
func (s *GuardedService) CloseWithOutcome(ctx context.Context, by model.PrincipalRef, req service.CloseRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.delegate.CloseWithOutcome(ctx, by, req, meta)
}
func (s *GuardedService) ReopenWithOutcome(ctx context.Context, by model.PrincipalRef, req service.ReopenRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.delegate.ReopenWithOutcome(ctx, by, req, meta)
}
func (s *GuardedService) DeferWithOutcome(ctx context.Context, by model.PrincipalRef, req service.DeferRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.delegate.DeferWithOutcome(ctx, by, req, meta)
}
func (s *GuardedService) CancelWithOutcome(ctx context.Context, by model.PrincipalRef, req service.CancelRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.delegate.CancelWithOutcome(ctx, by, req, meta)
}
func (s *GuardedService) FailWithOutcome(ctx context.Context, by model.PrincipalRef, req service.FailRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.delegate.FailWithOutcome(ctx, by, req, meta)
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
	if s == nil || s.checker == nil || s.checker.store == nil || s.lease == nil {
		return nil, nil, func() {}, errors.New("authorization store is unavailable")
	}
	// Authorization and idempotency reads can precede the domain service's
	// lease guard. Fence them before touching storage after lease loss too.
	select {
	case <-s.lease.Lost():
		return nil, nil, func() {}, &model.Error{Data: model.ErrorData{Code: "busy", Retryable: true, Hint: "active storage lease was lost"}}
	default:
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
	// Historical and current content both use current authority. Only content
	// membership and parent relationships remain pinned to the returned page.
	_, snapshot, closeReader, err := s.current(ctx, nil)
	if err != nil {
		return service.BeadPage{}, err
	}
	defer closeReader()
	caller, err := s.checker.callerFor(ctx, by)
	if err != nil {
		return service.BeadPage{}, authzError(err)
	}
	authorization, err := s.checker.load(ctx, snapshot, caller)
	if err != nil {
		return service.BeadPage{}, &model.Error{Message: "authorization facts unavailable", Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	if position != nil && (!position.AuthorizationBound || !authorization.authorizationBound || position.AuthorizationRevision.String() != authorization.authorizationRevision.String()) {
		return service.BeadPage{}, &model.Error{Message: "authorization facts changed since cursor issuance", Data: model.ErrorData{Code: "cursor_invalid"}}
	}
	facts := authorization
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
	content, _, closeContent, err := s.current(ctx, &page.AsOf)
	if err != nil {
		return service.BeadPage{}, err
	}
	defer closeContent()
	refs := make([]model.Ref, len(out.Items))
	for i, bead := range out.Items {
		refs[i] = model.RefByID(bead.ID, model.Natural64(0))
	}
	parents, err := s.checker.parentProjections(ctx, caller, authorization, content, refs)
	if err != nil {
		return service.BeadPage{}, err
	}
	for i := range out.Items {
		projectBeadParent(&out.Items[i], parents[out.Items[i].ID])
	}
	if end < len(filtered) {
		out.Next = &service.PagePosition{
			Offset: end, AsOf: page.AsOf,
			AuthorizationRevision: authorization.authorizationRevision,
			AuthorizationBound:    authorization.authorizationBound,
		}
	}
	binding, err := s.checker.readAuthorityBinding(ctx, caller, authorization, snapshot)
	if err != nil {
		return service.BeadPage{}, err
	}
	if err := s.checker.ValidateReadAuthorityBinding(ctx, by, binding); err != nil {
		return service.BeadPage{}, err
	}
	return out, nil
}

func postfilterCap() error {
	return &model.Error{Message: "authorization post-filter candidate cap exceeded", Data: model.ErrorData{Code: "authz_postfilter_cap"}}
}
