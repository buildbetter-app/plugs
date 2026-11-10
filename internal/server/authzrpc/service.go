package authzrpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
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
	readyCandidates func(context.Context, model.PrincipalRef, service.ReadyRequest, driver.Reader) ([]service.Bead, error)
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
	if source, ok := s.delegate.(interface {
		ResolveSourceAddress(context.Context, model.PrincipalRef, string, model.Ref) (model.Ref, bool, error)
	}); ok {
		resolved, _, err := source.ResolveSourceAddress(ctx, by, req.Project, req.ID)
		if err != nil {
			return service.GetResult{}, err
		}
		req.ID = resolved
	}
	if workflowItemRef(req.ID) {
		return s.getWorkflow(ctx, by, req)
	}
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
		if resource.Kind != "bead" || authz.CanonicalRef(resource.Ref) != nil {
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
	// Match still validates context, facts and caveats for every resource. The
	// four common dependencies join this same immutable authority snapshot once;
	// repeating their SQL observations cannot add another snapshot guarantee.
	facts := &countReadSlice{slice: authority, dependencies: [4]authz.Dependency{
		authz.ContextDependency(s.checker.chain.Namespace),
		authz.ACLDependency(s.checker.chain.Namespace),
		{Kind: "sentinel", Key: "plugins"},
		{Kind: "sentinel", Key: "ontology"},
	}}
	for _, dep := range facts.dependencies {
		if err := authority.Record(ctx, dep); err != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
		}
	}
	allowed := make([]bool, len(resources))
	for i, ref := range refs {
		if !present[ref.ID] {
			continue
		}
		allowed[i], err = template.Match(ctx, facts, authz.Resource{Kind: authz.Bead, Ref: ref})
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

// countReadSlice exists only during one batch against one current snapshot.
// It retains no effective grants or decisions. Every other dependency remains
// a real source read, and the caller's final fresh authority fence is unchanged.
type countReadSlice struct {
	*slice
	dependencies [4]authz.Dependency
}

func (s *countReadSlice) Record(ctx context.Context, dep authz.Dependency) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, recorded := range s.dependencies {
		if dep == recorded {
			return nil
		}
	}
	return s.slice.Record(ctx, dep)
}

// readySnapshotFacts uses the existing batch dependency adapter only within
// this immutable read snapshot. Register every common dependency against the
// real reader first; per-item Match and the final fresh authority fence still
// run. No effective grant, decision or cross-request state is retained.
func readySnapshotFacts(ctx context.Context, authority *slice) (*countReadSlice, error) {
	facts := &countReadSlice{slice: authority, dependencies: [4]authz.Dependency{
		authz.ContextDependency(authority.Namespace()),
		authz.ACLDependency(authority.Namespace()),
		{Kind: "sentinel", Key: "plugins"},
		{Kind: "sentinel", Key: "ontology"},
	}}
	for _, dep := range facts.dependencies {
		if err := authority.Record(ctx, dep); err != nil {
			return nil, &model.Error{Message: "authorization dependency unavailable", Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
		}
	}
	return facts, nil
}

func (s *GuardedService) Ready(ctx context.Context, by model.PrincipalRef, req service.ReadyRequest) (service.BeadPage, error) {
	if s == nil || s.delegate == nil || s.checker == nil {
		return service.BeadPage{}, &model.Error{Message: "authorization service is unavailable", Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	if page, err := s.compiledReady(ctx, by, req); !errors.Is(err, driver.ErrUnsupported) {
		return page, err
	}
	return s.readyFallback(ctx, by, req)
}

func (s *GuardedService) readyFallback(ctx context.Context, by model.PrincipalRef, req service.ReadyRequest) (service.BeadPage, error) {
	readyFilter, compiled, err := s.compiledReadyFilter(ctx, by, req.Project)
	if err != nil {
		return service.BeadPage{}, err
	}
	return s.collectReady(ctx, by, req, readyFilter, compiled)
}

func (s *GuardedService) collectReady(ctx context.Context, by model.PrincipalRef, req service.ReadyRequest, readyFilter service.ReadyAuthorizer, compiled bool) (service.BeadPage, error) {
	// Compiled finite permissions are pushed through the domain service's
	// snapshot-aware filter before pageResult. The 20,000 candidate cap remains
	// only for genuinely check-only predicates that require post-filtering.
	full := req
	full.Limit = 1000
	full.Cursor = ""
	full.Position = nil
	var page service.BeadPage
	filteredDelegate, supportsFiltered := s.delegate.(interface {
		ReadyWithFilter(context.Context, model.PrincipalRef, service.ReadyRequest, service.ReadyAuthorizer) (service.BeadPage, error)
	})
	readPage := func() (service.BeadPage, error) {
		if supportsFiltered {
			// The trusted capability also supplies complete source-order metadata
			// for the bounded check-only path, whose authorizer remains nil.
			return filteredDelegate.ReadyWithFilter(ctx, by, full, readyFilter)
		}
		candidate, err := s.delegate.Ready(ctx, by, full)
		if err == nil {
			for _, item := range candidate.Items {
				if item.Kind != "workflow.item" {
					return service.BeadPage{}, &model.Error{Message: "bound ready source is unavailable", Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
				}
			}
		}
		return candidate, err
	}
	page, err := readPage()
	if err != nil {
		return service.BeadPage{}, err
	}
	items := append([]service.Bead(nil), page.Items...)
	workflowSnapshot := page.WorkflowSnapshot
	durableSnapshot := page.AsOf
	coreMembership := page.ReadyMembership
	for page.Next != nil {
		if !compiled && len(items) >= listPostfilterCap {
			return service.BeadPage{}, postfilterCap()
		}
		position := *page.Next
		full.Position = &position
		page, err = readPage()
		if err != nil {
			return service.BeadPage{}, err
		}
		// Plain-core Ready offsets intentionally remain live. A guarded
		// aggregate cannot sign a sequence spliced across durable snapshots or
		// a same-revision no-write change to the complete core ready order.
		if page.AsOf.Incarnation != durableSnapshot.Incarnation || page.AsOf.Revision.String() != durableSnapshot.Revision.String() || page.ReadyMembership != coreMembership {
			return service.BeadPage{}, &model.Error{Message: "durable ready source changed while collecting candidates; restart paging", Data: model.ErrorData{Code: "cursor_invalid"}}
		}
		if workflowSnapshot != nil && !workflowSnapshot.Equal(page.WorkflowSnapshot) {
			return service.BeadPage{}, &model.Error{Message: "workflow ready membership changed while collecting candidates", Data: model.ErrorData{Code: "cursor_invalid"}}
		}
		workflowSnapshot = page.WorkflowSnapshot
		items = append(items, page.Items...)
	}
	if !compiled && len(items) > listPostfilterCap {
		return service.BeadPage{}, postfilterCap()
	}
	page.Items = items
	page.Next = nil
	page.WorkflowSnapshot = workflowSnapshot
	return s.filterPage(ctx, by, "bead.ready", req.Project, req.Limit, req.Position, page, false)
}

func (s *GuardedService) compiledReadyFilter(ctx context.Context, by model.PrincipalRef, project string) (service.ReadyAuthorizer, bool, error) {
	_, snapshot, closeReader, err := s.current(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	caller, _, template, err := s.readyProjectFilter(ctx, by, snapshot, project, "bead.ready", authz.Read)
	closeReader()
	if errors.Is(err, authz.ErrCheckOnly) || errors.Is(err, driver.ErrUnsupported) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, authzError(err)
	}
	var authorizationRevision model.Revision
	var authorizationBound bool
	var boundIncarnation, boundRevision string
	var facts *countReadSlice
	var boundReader driver.Reader
	return func(ctx context.Context, reader driver.Reader, bead service.Bead) (bool, error) {
		view := reader.Snapshot()
		revision := "0"
		if view.At != nil {
			revision = view.At.String()
		}
		if facts == nil || !sameReadyReader(boundReader, reader) || boundIncarnation != view.Incarnation || boundRevision != revision {
			loaded, err := s.checker.load(ctx, reader, caller)
			if err != nil {
				return false, &model.Error{Message: "authorization facts unavailable", Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
			}
			if authorizationBound && authorizationRevision.String() != loaded.authorizationRevision.String() {
				return false, &model.Error{Message: "authorization facts changed while collecting ready candidates", Data: model.ErrorData{Code: "cursor_invalid"}}
			}
			authorizationRevision = loaded.authorizationRevision
			authorizationBound = true
			facts, err = readySnapshotFacts(ctx, loaded)
			if err != nil {
				return false, err
			}
			boundReader = reader
			boundIncarnation, boundRevision = view.Incarnation, revision
		}
		allowed, err := template.Match(ctx, facts, authz.Resource{Kind: authz.Bead, Ref: model.RefByID(bead.ID, model.Natural64(0))})
		if err != nil {
			return false, authzError(err)
		}
		return allowed, nil
	}, true, nil
}

// A read snapshot owns its reader lifetime, even at an unchanged revision.
// Production readers are pointer instances. Value adapters cannot prove the
// same lifetime, so they conservatively reload rather than reuse source facts.
func sameReadyReader(a, b driver.Reader) bool {
	left, right := reflect.ValueOf(a), reflect.ValueOf(b)
	return left.IsValid() && right.IsValid() && left.Kind() == reflect.Pointer && right.Kind() == reflect.Pointer && left.Type() == right.Type() && left.Pointer() == right.Pointer()
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
	return s.filterPage(ctx, by, "history.query", req.Project, req.Limit, req.Position, page, true)
}

func (s *GuardedService) CreateWithOutcome(ctx context.Context, by model.PrincipalRef, req service.CreateRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.delegate.CreateWithOutcome(ctx, by, req, meta)
}
func (s *GuardedService) UpdateWithOutcome(ctx context.Context, by model.PrincipalRef, req service.UpdateRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	if workflowItemRef(req.ID) && !s.sourceRuntimeInstalled() {
		return s.workflowWrite(ctx, by, meta.Project, func() (port.WriteOutcome, error) {
			return s.delegate.UpdateWithOutcome(ctx, by, req, meta)
		})
	}
	return s.delegate.UpdateWithOutcome(ctx, by, req, meta)
}
func (s *GuardedService) ApplyWithOutcome(ctx context.Context, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.delegate.ApplyWithOutcome(ctx, by, cs, meta)
}
func (s *GuardedService) LinkWithOutcome(ctx context.Context, by model.PrincipalRef, req service.LinkRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	kind, err := req.TargetKind.Normalized()
	if err != nil {
		return port.WriteOutcome{}, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
	}
	if err := model.ValidateLinkTarget(kind, req.To); err != nil {
		return port.WriteOutcome{}, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
	}
	req.TargetKind = kind
	if kind == model.LinkTargetExternal {
		return s.delegate.LinkWithOutcome(ctx, by, req, meta)
	}
	if workflowItemRef(req.From) || workflowItemRef(req.To) {
		return s.workflowWrite(ctx, by, meta.Project, func() (port.WriteOutcome, error) {
			if !workflowItemRef(req.From) {
				if allowed, err := s.checkRead(ctx, by, "bead.get", req.From); err != nil {
					return port.WriteOutcome{}, err
				} else if !allowed {
					return port.WriteOutcome{}, &model.Error{Data: model.ErrorData{Code: "forbidden"}}
				}
			}
			if !workflowItemRef(req.To) {
				if allowed, err := s.checkRead(ctx, by, "bead.get", req.To); err != nil {
					return port.WriteOutcome{}, err
				} else if !allowed {
					return port.WriteOutcome{}, &model.Error{Data: model.ErrorData{Code: "forbidden"}}
				}
			}
			return s.delegate.LinkWithOutcome(ctx, by, req, meta)
		})
	}
	return s.delegate.LinkWithOutcome(ctx, by, req, meta)
}
func (s *GuardedService) UnlinkWithOutcome(ctx context.Context, by model.PrincipalRef, req service.LinkRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	kind, err := req.TargetKind.Normalized()
	if err != nil {
		return port.WriteOutcome{}, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
	}
	if err := model.ValidateLinkTarget(kind, req.To); err != nil {
		return port.WriteOutcome{}, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
	}
	req.TargetKind = kind
	if kind == model.LinkTargetExternal {
		return s.delegate.UnlinkWithOutcome(ctx, by, req, meta)
	}
	if workflowItemRef(req.From) || workflowItemRef(req.To) {
		return s.workflowWrite(ctx, by, meta.Project, func() (port.WriteOutcome, error) {
			if !workflowItemRef(req.From) {
				if allowed, err := s.checkRead(ctx, by, "bead.get", req.From); err != nil {
					return port.WriteOutcome{}, err
				} else if !allowed {
					return port.WriteOutcome{}, &model.Error{Data: model.ErrorData{Code: "forbidden"}}
				}
			}
			if !workflowItemRef(req.To) {
				if allowed, err := s.checkRead(ctx, by, "bead.get", req.To); err != nil {
					return port.WriteOutcome{}, err
				} else if !allowed {
					return port.WriteOutcome{}, &model.Error{Data: model.ErrorData{Code: "forbidden"}}
				}
			}
			return s.delegate.UnlinkWithOutcome(ctx, by, req, meta)
		})
	}
	return s.delegate.UnlinkWithOutcome(ctx, by, req, meta)
}
func (s *GuardedService) ClaimWithOutcome(ctx context.Context, by model.PrincipalRef, req service.ClaimRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	if workflowItemRef(req.ID) && !s.sourceRuntimeInstalled() {
		return s.workflowWrite(ctx, by, meta.Project, func() (port.WriteOutcome, error) {
			return s.delegate.ClaimWithOutcome(ctx, by, req, meta)
		})
	}
	return s.delegate.ClaimWithOutcome(ctx, by, req, meta)
}

func (s *GuardedService) WorkflowInvoke(ctx context.Context, by model.PrincipalRef, req workflowplugin.InvokeRequest, meta model.WriteMeta) (workflowplugin.InvokeResult, error) {
	workflow, ok := s.delegate.(rpc.WorkflowService)
	if !ok {
		return workflowplugin.InvokeResult{}, &model.Error{Data: model.ErrorData{Code: "method_not_found"}}
	}
	if req.Mode != "ephemeral" {
		// Permanent invocations use the ordinary Writer's transactional policy.
		return workflow.WorkflowInvoke(ctx, by, req, meta)
	}
	binding, err := s.workflowModifyAuthority(ctx, by, req.Project)
	if err != nil {
		return workflowplugin.InvokeResult{}, err
	}
	out, err := workflow.WorkflowInvoke(ctx, by, req, meta)
	if err != nil {
		return workflowplugin.InvokeResult{}, err
	}
	if err := s.checker.ValidateReadAuthorityBinding(ctx, by, binding); err != nil {
		return workflowplugin.InvokeResult{}, err
	}
	return out, nil
}

func workflowItemRef(ref model.Ref) bool {
	return ref.IsID() && strings.HasPrefix(ref.ID, workflowplugin.ItemPrefix)
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
	meta.Project = req.Project
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
	if workflow, ok := s.delegate.(rpc.WorkflowClaimNextService); ok {
		out, replayed, replayErr := workflow.ReplayWorkflowClaimNext(ctx, by, req, meta)
		if replayErr != nil {
			return nil, replayErr
		}
		if replayed {
			binding, err := s.workflowModifyAuthority(ctx, by, req.Project)
			if err != nil {
				return nil, err
			}
			if err := s.checker.ValidateReadAuthorityBinding(ctx, by, binding); err != nil {
				return nil, err
			}
			return out, nil
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
			if workflowItemRef(ref) {
				candidateMeta := meta
				candidateMeta.Method = "bead.claimNext"
				out, claimErr := s.ClaimWithOutcome(ctx, by, service.ClaimRequest{ID: ref}, candidateMeta)
				if claimErr == nil {
					return &out, nil
				}
				if !claimNextRace(claimErr) {
					return nil, claimErr
				}
				continue
			}
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
			// The provider may select a workflow item after the final live page.
			// Apply the same current project permit before entering that seam.
			binding, err := s.workflowModifyAuthority(ctx, by, req.Project)
			if err != nil {
				return nil, err
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
			if out.Commit.Revision.IsZero() {
				// Provider results do not advance the core head. Core null
				// commits use the Writer's own transactional policy guard.
				if err := s.checker.ValidateReadAuthorityBinding(ctx, by, binding); err != nil {
					return nil, err
				}
			}
			if len(out.Commit.Results) == 0 {
				out.NullResult = true
			}
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
		items, err := s.readyCandidates(ctx, by, service.ReadyRequest{Project: req.Project, ViewID: req.ViewID, Filter: req.Filter}, reader)
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
	if workflowItemRef(req.ID) && !s.sourceRuntimeInstalled() {
		return s.workflowWrite(ctx, by, meta.Project, func() (port.WriteOutcome, error) {
			return s.delegate.CommentWithOutcome(ctx, by, req, meta)
		})
	}
	return s.delegate.CommentWithOutcome(ctx, by, req, meta)
}
func (s *GuardedService) CloseWithOutcome(ctx context.Context, by model.PrincipalRef, req service.CloseRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	if workflowItemRef(req.ID) && !s.sourceRuntimeInstalled() {
		return s.workflowWrite(ctx, by, meta.Project, func() (port.WriteOutcome, error) {
			return s.delegate.CloseWithOutcome(ctx, by, req, meta)
		})
	}
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

func (s *GuardedService) filterPage(ctx context.Context, by model.PrincipalRef, method, project string, limit int, position *service.PagePosition, page service.BeadPage, pinned bool) (service.BeadPage, error) {
	if method == "bead.ready" && (!page.WorkflowSnapshot.Valid() || (position != nil && (position.Workflow || !position.WorkflowSnapshot.Equal(page.WorkflowSnapshot)))) {
		return service.BeadPage{}, &model.Error{Message: "workflow ready snapshot changed since cursor issuance; restart paging", Data: model.ErrorData{Code: "cursor_invalid"}}
	}
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
	var facts authz.Slice = authorization
	if method == "bead.ready" {
		facts, err = readySnapshotFacts(ctx, authorization)
		if err != nil {
			return service.BeadPage{}, err
		}
	}
	// History visibility is current bead read authorization; fixed CP7's
	// history resource does not create a second permission vocabulary.
	resourceKind := authz.Bead
	compiledMethod := method
	if method == "history.query" {
		compiledMethod = "bead.get"
	}
	template, err := s.checker.chain.Compile(authz.ListRequest{Caller: caller, Action: authz.Read, ResourceKind: resourceKind, Method: compiledMethod})
	checkOnly := method == "bead.ready" && errors.Is(err, authz.ErrCheckOnly)
	if err != nil && !checkOnly {
		return service.BeadPage{}, authzError(err)
	}
	filtered := make([]service.Bead, 0, len(page.Items))
	workflowChecked, workflowAllowed := false, false
	for _, bead := range page.Items {
		if bead.Kind == "workflow.item" {
			if method != "bead.ready" || bead.Project != project {
				continue
			}
			allowed := false
			if s.sourceRuntimeInstalled() {
				// Imported items use their verified per-item source binding and the
				// original Bead method policy, as on the source Get path. Native
				// workflow project authority cannot authorize this separate plane.
				allowed, err = s.sourceRead(ctx, caller, authorization, model.RefByID(bead.ID, model.Natural64(0)), project, method)
			} else {
				if !workflowChecked {
					workflowAllowed, err = s.checker.workflowRead(ctx, caller, authorization, method, project)
					workflowChecked = true
				}
				allowed = workflowAllowed
			}
			if err != nil {
				return service.BeadPage{}, err
			}
			if allowed {
				filtered = append(filtered, bead)
			}
			continue
		}
		resource := authz.Resource{Kind: authz.Bead, Ref: model.RefByID(bead.ID, model.Natural64(0))}
		var ok bool
		if checkOnly {
			// Ready already refused an over-cap unfiltered set atomically. A
			// genuine under-cap check-only predicate must now run the complete
			// installed chain rather than attempt the unavailable compiler again.
			decisions, checkErr := s.checker.chain.Check(ctx, facts, []authz.CheckRequest{{Caller: caller, Action: authz.Read, Resource: resource, Method: compiledMethod}})
			err = checkErr
			if err == nil && len(decisions) != 1 {
				return service.BeadPage{}, &model.Error{Message: "invalid authorization result cardinality", Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
			}
			ok = err == nil && decisions[0].Effect == authz.Allow
		} else {
			ok, err = template.Match(ctx, facts, resource)
		}
		if err != nil {
			return service.BeadPage{}, authzError(err)
		}
		if ok {
			filtered = append(filtered, bead)
		}
	}
	membership := ""
	if method == "bead.ready" {
		// New finite compiled Ready pages use the versioned semantic
		// commitment. Check-only pages and continuations carrying an older
		// unprefixed digest stay on the legacy verifier for compatibility.
		useV2 := !checkOnly && (position == nil || strings.HasPrefix(position.ReadyMembership, "v2:"))
		if useV2 {
			membership, err = readyMembershipV2(filtered)
			if err != nil {
				return service.BeadPage{}, &model.Error{Message: "ready membership commitment failed", Data: model.ErrorData{Code: "internal_error"}}
			}
		} else {
			membership = readyMembershipDigest(filtered)
		}
	}
	if method == "bead.ready" && position != nil && position.ReadyMembership != membership {
		return service.BeadPage{}, &model.Error{Message: "ready membership changed since cursor issuance; restart paging", Data: model.ErrorData{Code: "cursor_invalid"}}
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
	if method == "bead.ready" {
		out.WorkflowSnapshot = page.WorkflowSnapshot
		out.ReadyMembership = membership
	}
	content, _, closeContent, err := s.current(ctx, &page.AsOf)
	if err != nil {
		return service.BeadPage{}, err
	}
	defer closeContent()
	refs := make([]model.Ref, 0, len(out.Items))
	for _, bead := range out.Items {
		if bead.Kind != "workflow.item" {
			refs = append(refs, model.RefByID(bead.ID, model.Natural64(0)))
		}
	}
	parents := map[string]ParentDisclosure{}
	if len(refs) != 0 {
		parents, err = s.checker.parentProjections(ctx, caller, authorization, content, refs)
		if err != nil {
			return service.BeadPage{}, err
		}
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
		if method == "bead.ready" {
			out.Next.WorkflowSnapshot = page.WorkflowSnapshot
			out.Next.ReadyMembership = membership
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

// readyMembershipDigest is the signed binding for a guarded aggregate page.
// It covers the complete authorized order, so a core row leaving Ready or
// changing priority cannot make a later workflow offset skip an item.
func readyMembershipDigest(items []service.Bead) string {
	h := sha256.New()
	for _, item := range items {
		encoded, err := json.Marshal(struct {
			ID            string         `json:"id"`
			Type          string         `json:"type"`
			Project       string         `json:"project"`
			Status        string         `json:"status"`
			Revision      model.Revision `json:"revision"`
			Fields        map[string]any `json:"fields,omitempty"`
			Plane         string         `json:"plane,omitempty"`
			Kind          string         `json:"kind,omitempty"`
			Plugin        string         `json:"plugin,omitempty"`
			WorkflowRunID string         `json:"workflowRunId,omitempty"`
		}{item.ID, item.Type, item.Project, item.Status, item.Revision, item.Fields, item.Plane, item.Kind, item.Plugin, item.WorkflowRunID})
		if err != nil {
			encoded = []byte(fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s", item.ID, item.Type, item.Project, item.Status, item.Revision.String()))
		}
		var size [8]byte
		size[0] = byte(len(encoded) >> 56)
		size[1] = byte(len(encoded) >> 48)
		size[2] = byte(len(encoded) >> 40)
		size[3] = byte(len(encoded) >> 32)
		size[4] = byte(len(encoded) >> 24)
		size[5] = byte(len(encoded) >> 16)
		size[6] = byte(len(encoded) >> 8)
		size[7] = byte(len(encoded))
		_, _ = h.Write(size[:])
		_, _ = h.Write(encoded)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func postfilterCap() error {
	return &model.Error{Message: "authorization post-filter candidate cap exceeded", Data: model.ErrorData{Code: "authz_postfilter_cap"}}
}

// ViewSelection is the chain-only view adapter. An additional viewops
// AuthorizeRead callback remains a separate restriction and disables storage
// acceleration. No function identity or policy subset is used as certification.
func (s *GuardedService) ViewSelection() *viewops.SelectionConfig {
	return &viewops.SelectionConfig{Bind: s.PrepareViewSelection, AuthorizeRead: func(ctx context.Context, by model.PrincipalRef, resource model.ResourceRef, _ string) error {
		if resource.Kind != "bead" {
			return notFound()
		}
		allowed, err := s.checkRead(ctx, by, "bead.get", resource.Ref)
		if err != nil {
			return err
		}
		if !allowed {
			return notFound()
		}
		return nil
	}}
}

// PrepareViewSelection binds CURRENT authority before any content candidate
// reads. W2 owns caller/filter preparation; this adapter only lowers complete
// finite read expressions and records their actual current source witnesses.
func (s *GuardedService) PrepareViewSelection(ctx context.Context, by model.PrincipalRef, inlineBinding string) (*viewops.SelectionAuthority, error) {
	reader, _, closeReader, err := s.current(ctx, nil)
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			closeReader()
		}
	}()
	snapshot := &viewSelectionSnapshot{Reader: reader, sources: map[string]driver.SelectionSource{}}
	bound, err := s.checker.BindAuthorization(ctx, snapshot, by, authz.Read, authz.Bead, "bead.get")
	if err != nil {
		return nil, err
	}
	filter, err := bound.Filter()
	if err != nil && !errors.Is(err, authz.ErrCheckOnly) {
		return nil, err
	}
	caller := bound.request.Request().Caller
	for _, dep := range []authz.Dependency{authz.ContextDependency(s.checker.chain.Namespace), authz.ACLDependency(s.checker.chain.Namespace), {Kind: "sentinel", Key: "plugins"}, {Kind: "sentinel", Key: "ontology"}} {
		if err := bound.slice.Record(ctx, dep); err != nil {
			return nil, authzError(err)
		}
	}
	binding, err := s.checker.readAuthorityBinding(ctx, caller, bound.slice, snapshot)
	if err != nil {
		return nil, err
	}
	out := &viewops.SelectionAuthority{Reader: reader, Binding: binding}
	out.Check = func(ctx context.Context, refs []model.Ref) ([]bool, error) {
		// Check-only preserves the existing per-resource component budget. It
		// cannot turn a large view into one oversized policy invocation.
		decisions := make([]bool, len(refs))
		for i, ref := range refs {
			decision, err := bound.Check(ctx, []model.Ref{ref})
			if err != nil {
				return nil, err
			}
			decisions[i] = decision[0]
		}
		rows, err := reader.Get(ctx, refs)
		if err != nil {
			return nil, err
		}
		present := map[string]bool{}
		for _, row := range rows {
			if row.Kind == "bead" {
				present[row.Ref.ID] = true
			}
		}
		for i, ref := range refs {
			decisions[i] = decisions[i] && present[ref.ID]
		}
		return decisions, nil
	}
	sourceMatches, err := viewSelectionFactsMatch(ctx, snapshot, bound.slice.facts)
	if err != nil {
		return nil, err
	}
	if inlineBinding != "" && filter != nil && finiteViewCaller(caller) && sourceMatches {
		description := filter.Description()
		permit, p := lowerViewFilter(description.Permit, description.Request.Caller, description.Namespace)
		forbid, f := lowerViewFilter(description.Forbid, description.Request.Caller, description.Namespace)
		refs, r := lowerViewCallerRefs(description.Request.Caller)
		args := []driver.SelectionExpr{{Op: driver.SelectionResourceExists}, permit, {Op: driver.SelectionNot, Args: []driver.SelectionExpr{forbid}}, refs}
		// The no-plugin schema admits only authorization=true. It cannot
		// encode this adapter's current-existence check, so retain check_only.
		complete := p && f && r && !description.NoPlugins
		for _, restriction := range description.Restrictions {
			expr, ok := lowerViewFilter(restriction, description.Request.Caller, description.Namespace)
			complete = complete && ok
			args = append(args, expr)
		}
		if complete {
			preparedBinding, err := filter.Binding()
			if err != nil {
				return nil, err
			}
			expression := driver.SelectionExpr{Op: driver.SelectionAll, Args: args}
			view := reader.Snapshot()
			// A valid policy can exceed the storage AST budget. Retain its
			// complete ordinary Check/Fence instead of aborting binding or
			// narrowing the policy to fit. Other boundary errors still fail.
			if viewSelectionExpressionBudget(expression) {
				out.Boundary, err = driver.PrepareSelectionBoundary(driver.SelectionBoundarySpec{Version: 1, PreparedBinding: preparedBinding, Incarnation: view.Incarnation, Branch: view.Branch, AuthorityRevision: *view.At, Sources: snapshot.observations(), Chain: driver.SelectionChainCompiled, Authorization: expression, Host: driver.SelectionHostAbsent, InlineViewBinding: inlineBinding})
				if err != nil {
					return nil, err
				}
			}
		}
	}
	// Rebind reacquires the real caller, facts and verification. Comparing both
	// the complete authority binding and source bytes also fences empty counts.
	out.Fence = func(ctx context.Context) error {
		fresh, _, closeFresh, err := s.current(ctx, nil)
		if err != nil {
			return err
		}
		defer closeFresh()
		rebound, err := bound.Rebind(ctx, fresh)
		if err != nil {
			return err
		}
		now, err := s.checker.readAuthorityBinding(ctx, rebound.request.Request().Caller, rebound.slice, fresh)
		if err != nil {
			return err
		}
		if now != binding {
			return hierarchyUnavailable()
		}
		for _, source := range snapshot.observations() {
			states, err := fresh.Observe(ctx, []driver.LockKey{source.Key})
			if err != nil {
				return err
			}
			if len(states) != 1 || !sameViewSelectionSource(source, states[0]) {
				return hierarchyUnavailable()
			}
		}
		return nil
	}
	keep = true
	return out, nil
}

// This admission check classifies only the closed expression size limits.
// Boundary construction remains responsible for every other validation error.
func viewSelectionExpressionBudget(expression driver.SelectionExpr) bool {
	nodes := 0
	var within func(driver.SelectionExpr, int) bool
	within = func(expr driver.SelectionExpr, depth int) bool {
		nodes++
		if depth > driver.SelectionMaxDepth || nodes > driver.SelectionMaxNodes {
			return false
		}
		for _, child := range expr.Args {
			if !within(child, depth+1) {
				return false
			}
		}
		return true
	}
	return within(expression, 0)
}

func finiteViewCaller(c authz.Caller) bool {
	if !authz.ValidID(c.VerificationID) || c.CapturedEpoch.Validate() != nil {
		return false
	}
	p := c.Principal
	if !authz.ValidID(p.ID) {
		return false
	}
	switch p.Kind {
	case "anonymous", "user", "agent", "service", "plugin", "system":
	default:
		return false
	}
	if p.Sponsor != nil && (p.Kind != "agent" || p.Sponsor.Sponsor != nil || !authz.ValidID(p.Sponsor.ID) || (p.Sponsor.Kind != "user" && p.Sponsor.Kind != "service")) {
		return false
	}
	return true
}

// lowerViewCallerRefs consumes the JSON-normalized prepared caller. It adds
// every principal and sponsor restriction to the full policy conjunction;
// refs never replace a permit, forbid, host restriction or current-source proof.
// Other caveat forms deliberately retain ordinary Check/Fence in this child.
func lowerViewCallerRefs(c authz.Caller) (driver.SelectionExpr, bool) {
	args := []driver.SelectionExpr{}
	for principal := &c.Principal; principal != nil; principal = principal.Sponsor {
		for name, value := range principal.Caveats {
			if name != "refs" {
				return driver.SelectionExpr{}, false
			}
			data, err := json.Marshal(value)
			if err != nil || string(data) == "null" {
				return driver.SelectionExpr{}, false
			}
			var refs []model.Ref
			if json.Unmarshal(data, &refs) != nil || len(refs) > driver.SelectionMaxNodes {
				return driver.SelectionExpr{}, false
			}
			for _, ref := range refs {
				// The authorization Ref domain is wider than SelectionRefs.
				// Preserve ordinary checks for any storage-unrepresentable ID,
				// including one beside a matching canonical reference.
				if authz.CanonicalRef(ref) != nil || driver.ValidateHierarchyRef(ref) != nil {
					return driver.SelectionExpr{}, false
				}
			}
			// An explicitly empty vector is a denying membership predicate.
			// Omitted refs contribute no restriction to the conjunction.
			args = append(args, driver.SelectionExpr{Op: driver.SelectionRefs, Refs: refs})
		}
	}
	return driver.SelectionExpr{Op: driver.SelectionAll, Args: args}, true
}

// No data-dependent simplification may remove an unsupported policy arm.
// TeamInvolvement and Modify remain check_only in full. Caller refs are lowered
// separately and ANDed only after every configured policy arm is complete.
func lowerViewFilter(f authz.FilterExpr, caller authz.Caller, ns authz.Namespace) (driver.SelectionExpr, bool) {
	constant := func(v bool) driver.SelectionExpr {
		if v {
			return driver.SelectionExpr{Op: driver.SelectionTrue}
		}
		return driver.SelectionExpr{Op: driver.SelectionFalse}
	}
	switch f.Op {
	case authz.True:
		return constant(true), true
	case authz.False:
		return constant(false), true
	case authz.PrincipalKind:
		return constant(caller.Principal.Kind == f.Kind), true
	case authz.SponsorKind:
		return constant(caller.Principal.Sponsor != nil && caller.Principal.Sponsor.Kind == f.Kind), true
	case authz.All, authz.Any:
		op := driver.SelectionAll
		if f.Op == authz.Any {
			op = driver.SelectionAny
		}
		args := []driver.SelectionExpr{}
		complete := true
		for _, child := range f.Args {
			e, ok := lowerViewFilter(child, caller, ns)
			args = append(args, e)
			complete = complete && ok
		}
		return driver.SelectionExpr{Op: op, Args: args}, complete
	case authz.GrantExists:
		keys, err := authzfixed.SelectionRows(ns)
		if err != nil {
			return driver.SelectionExpr{}, false
		}
		principal := &caller.Principal
		if f.Subject == authz.SponsorSubject {
			principal = principal.Sponsor
		}
		if principal == nil || principal.Kind != "user" {
			return constant(false), true
		}
		subject := driver.SelectionString(authzfixed.SelectionSubject(authz.SubjectRef{Kind: "user", ID: principal.ID}))
		role := driver.SelectionString(f.Role)
		tests := []driver.SelectionFactTest{{Column: driver.SelectionFactSubject, Value: &subject}, {Column: driver.SelectionFactRole, Value: &role}}
		switch f.Scope {
		case authz.AuthorityBinding:
			scope, err := authzfixed.SelectionScope(ns, authz.Scope{Kind: authz.AuthorityScope, Ref: ns.Authority})
			if err != nil {
				return driver.SelectionExpr{}, false
			}
			v := driver.SelectionString(scope)
			tests = append(tests, driver.SelectionFactTest{Column: driver.SelectionFactScope, Value: &v})
		case authz.ProjectBindingScope:
			v := driver.SelectionString(string(driver.SelectionProjectScope))
			tests = append(tests, driver.SelectionFactTest{Column: driver.SelectionFactScopeKind, Value: &v}, driver.SelectionFactTest{Column: driver.SelectionFactProjectName, Candidate: driver.SelectionProject})
		default:
			return driver.SelectionExpr{}, false
		}
		return driver.SelectionExpr{Op: driver.SelectionExistsOp, Exists: &driver.SelectionExists{Row: keys.Grants, Kind: driver.SelectionScopedGrantFact, Tests: tests}}, true
	case authz.ActiveProject:
		keys, err := authzfixed.SelectionRows(ns)
		if err != nil {
			return driver.SelectionExpr{}, false
		}
		active := driver.SelectionBool(true)
		return driver.SelectionExpr{Op: driver.SelectionExistsOp, Exists: &driver.SelectionExists{Row: keys.Projects, Kind: driver.SelectionProjectFact, Tests: []driver.SelectionFactTest{{Column: driver.SelectionFactName, Candidate: driver.SelectionProject}, {Column: driver.SelectionFactActive, Value: &active}}}}, true
	default:
		return driver.SelectionExpr{}, false
	}
}

// FactsSource sees the real pinned reader through this observer. Misses,
// tombstones and exact raw logical bytes all become source witnesses.
type viewSelectionSnapshot struct {
	driver.Reader
	sources map[string]driver.SelectionSource
}

func (s *viewSelectionSnapshot) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	states, err := s.Reader.Observe(ctx, keys)
	if err != nil {
		return nil, err
	}
	if len(states) != len(keys) {
		return nil, hierarchyUnavailable()
	}
	for i, state := range states {
		key, err := driver.EncodeLockKey(keys[i])
		if err != nil {
			return nil, err
		}
		digest := viewSelectionSourceDigest(state)
		if state.Present && (keys[i].Kind == driver.LockRecord || keys[i].Kind == driver.LockDerived) && digest == "" {
			return nil, hierarchyUnavailable()
		}
		s.sources[key] = driver.SelectionSource{Key: keys[i], Revision: state.Revision, Present: state.Present, Digest: digest}
	}
	return states, nil
}
func (s *viewSelectionSnapshot) Get(ctx context.Context, refs []model.Ref) ([]driver.Record, error) {
	keys := make([]driver.LockKey, len(refs))
	for i := range refs {
		ref := refs[i]
		keys[i] = driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
	}
	if _, err := s.Observe(ctx, keys); err != nil {
		return nil, err
	}
	return s.Reader.Get(ctx, refs)
}
func (s *viewSelectionSnapshot) observations() []driver.SelectionSource {
	keys := make([]string, 0, len(s.sources))
	for key := range s.sources {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]driver.SelectionSource, 0, len(keys))
	for _, key := range keys {
		out = append(out, s.sources[key])
	}
	return out
}
func viewSelectionSourceDigest(state driver.LockState) string {
	if !state.Present {
		return ""
	}
	if state.Record != nil {
		return driver.SelectionDigest(state.Record.Body)
	}
	if state.Derived != nil {
		return driver.SelectionDigest(state.Derived.Value)
	}
	return ""
}
func sameViewSelectionSource(source driver.SelectionSource, state driver.LockState) bool {
	return source.Present == state.Present && source.Revision.String() == state.Revision.String() && source.Digest == viewSelectionSourceDigest(state)
}

func viewSelectionFactsMatch(ctx context.Context, snapshot *viewSelectionSnapshot, facts authzfixed.Facts) (bool, error) {
	keys, err := authzfixed.SelectionRows(facts.Namespace)
	if err != nil {
		return false, err
	}
	states, err := snapshot.Observe(ctx, []driver.LockKey{{Kind: driver.LockDerived, Derived: &keys.Source}})
	if err != nil {
		return false, err
	}
	canonical, err := authzfixed.FactsSourceBytes(facts)
	if err != nil {
		return false, err
	}
	return len(states) == 1 && states[0].Present && states[0].Derived != nil && bytes.Equal(states[0].Derived.Value, canonical), nil
}
