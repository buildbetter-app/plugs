package authzrpc

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type readyOrderDomain interface {
	ReadyOrderPlan(service.ReadyRequest, driver.Reader, bool, []model.Ref) (driver.ReadyOrderPlan, error)
	WorkflowReadyItems(context.Context, model.PrincipalRef, service.ReadyRequest) ([]service.Bead, *service.WorkflowPageSnapshot, error)
}

// readyProjectFilter specializes only a complete policy at a current verified
// project. The selector must separately certify current live source coverage
// for this same project and authority view before any result can be emitted.
func (s *GuardedService) readyProjectFilter(ctx context.Context, by model.PrincipalRef, snapshot Snapshot, project, method string, action authz.Action) (authz.Caller, *slice, authz.ProjectFilter, error) {
	caller, err := s.checker.callerFor(ctx, by)
	if err != nil {
		return authz.Caller{}, nil, authz.ProjectFilter{}, authzError(err)
	}
	authority, err := s.checker.load(ctx, snapshot, caller)
	if err != nil {
		return caller, nil, authz.ProjectFilter{}, hierarchyUnavailable()
	}
	ref, found := authority.projectByName(project)
	if !found {
		return caller, authority, authz.ProjectFilter{}, driver.ErrUnsupported
	}
	template, err := s.checker.chain.Compile(authz.ListRequest{Caller: caller, Action: action, ResourceKind: authz.Bead, Method: method})
	if err != nil {
		// The ordinary path retains its exact policy behavior and individual
		// custom component budgets. No incomplete compiler grants authority.
		return caller, authority, authz.ProjectFilter{}, driver.ErrUnsupported
	}
	filter, err := template.BindProject(ctx, authority, authz.ProjectBinding{Name: project, Ref: ref, Found: true})
	return caller, authority, filter, err
}

func readyMembershipV2(items []service.Bead) (string, error) {
	leaves := make([]driver.ReadyOrderLeaf, 0, len(items))
	workflow := make([][]byte, 0)
	for _, item := range items {
		image := service.ReadyMembershipItem(item)
		if item.Kind == "workflow.item" {
			encoded, err := json.Marshal(image)
			if err != nil {
				return "", err
			}
			workflow = append(workflow, encoded)
			continue
		}
		// Priority is part of the exact order, as well as the semantic leaf.
		leaf, err := service.ReadyMembershipLeaf(item)
		if err != nil {
			return "", err
		}
		leaves = append(leaves, leaf)
	}
	core, err := driver.ReadyCommitmentFromLeaves(leaves)
	if err != nil {
		return "", err
	}
	return driver.ReadyMembershipV2(core, workflow), nil
}

func (s *GuardedService) compiledReady(ctx context.Context, by model.PrincipalRef, req service.ReadyRequest) (service.BeadPage, error) {
	domain, ok := s.delegate.(readyOrderDomain)
	if !ok || req.Position != nil && !strings.HasPrefix(req.Position.ReadyMembership, "v2:") {
		return service.BeadPage{}, driver.ErrUnsupported
	}
	current, snapshot, closeReader, err := s.current(ctx, nil)
	if err != nil {
		return service.BeadPage{}, err
	}
	defer closeReader()
	selector, ok := current.(driver.ReadyOrderReader)
	if !ok {
		return service.BeadPage{}, driver.ErrUnsupported
	}
	caller, authority, filter, err := s.readyProjectFilter(ctx, by, snapshot, req.Project, "bead.ready", authz.Read)
	if err != nil {
		return service.BeadPage{}, err
	}
	refs, _ := filter.References()
	query := req
	query.Position, query.ClaimNextSeed = nil, ""
	plan, err := domain.ReadyOrderPlan(query, current, filter.Uniform(), refs)
	if err != nil {
		return service.BeadPage{}, err
	}
	// The first thin selection proves complete current source coverage and
	// yields the full authorized commitment before any item body is read.
	plan.Offset, plan.Limit = 0, 1
	core, err := selector.SelectReadyOrder(ctx, plan)
	if err != nil {
		return service.BeadPage{}, err
	}
	workflow, workflowSnapshot, err := domain.WorkflowReadyItems(ctx, by, query)
	if err != nil {
		return service.BeadPage{}, err
	}
	if len(workflow) != 0 {
		allowed, err := s.checker.workflowRead(ctx, caller, authority, "bead.ready", req.Project)
		if err != nil {
			return service.BeadPage{}, err
		}
		if !allowed {
			workflow = nil
		}
	}
	workflowJSON := make([][]byte, len(workflow))
	for i, item := range workflow {
		if item.Project != req.Project || item.Kind != "workflow.item" {
			return service.BeadPage{}, hierarchyUnavailable()
		}
		workflowJSON[i], err = json.Marshal(service.ReadyMembershipItem(item))
		if err != nil {
			return service.BeadPage{}, err
		}
	}
	membership := driver.ReadyMembershipV2(core.Commitment, workflowJSON)
	limit := req.Limit
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 1000 || core.Total > uint64(^uint(0)>>1)-uint64(len(workflow)) {
		return service.BeadPage{}, &model.Error{Data: model.ErrorData{Code: "invalid_params"}}
	}
	total, offset := int(core.Total)+len(workflow), 0
	if req.Position != nil {
		p := req.Position
		if !p.AuthorizationBound || !authority.authorizationBound || p.AuthorizationRevision.String() != authority.authorizationRevision.String() || p.AsOf.Incarnation != plan.Incarnation || p.Offset < 0 || p.Offset > total || p.Workflow || !p.WorkflowSnapshot.Equal(workflowSnapshot) || p.ReadyMembership != membership {
			return service.BeadPage{}, &model.Error{Message: "ready membership or authority changed since cursor issuance; restart paging", Data: model.ErrorData{Code: "cursor_invalid"}}
		}
		offset = p.Offset
	}
	end := offset + min(limit, total-offset)
	page := service.BeadPage{Items: []service.Bead{}, AsOf: service.AsOf{Revision: plan.ContentRevision, Incarnation: plan.Incarnation}, ReadyMembership: membership, WorkflowSnapshot: workflowSnapshot}
	rows := []driver.Record{}
	if offset < int(core.Total) {
		plan.Offset, plan.Limit = uint64(offset), min(limit, int(core.Total)-offset)
		selected, err := selector.SelectReadyOrder(ctx, plan)
		if err != nil {
			return service.BeadPage{}, err
		}
		if selected.Total != core.Total || selected.Commitment != core.Commitment {
			return service.BeadPage{}, hierarchyUnavailable()
		}
		page.Items, rows, err = service.HydrateReadyOrder(ctx, current, selected.Keys)
		if err != nil {
			return service.BeadPage{}, err
		}
		for _, item := range page.Items {
			if item.Project != req.Project {
				return service.BeadPage{}, hierarchyUnavailable()
			}
		}
	}
	if end > int(core.Total) {
		page.Items = append(page.Items, workflow[max(0, offset-int(core.Total)):end-int(core.Total)]...)
	}
	parents, err := s.checker.parentProjectionsFromRows(ctx, caller, authority, current, rows)
	if err != nil {
		return service.BeadPage{}, err
	}
	for i := range page.Items {
		projectBeadParent(&page.Items[i], parents[page.Items[i].ID])
	}
	if err := s.projectReadyLinkTargets(ctx, by, page.Items); err != nil {
		return service.BeadPage{}, err
	}
	if end < total {
		page.Next = &service.PagePosition{Offset: end, AsOf: page.AsOf, AuthorizationRevision: authority.authorizationRevision, AuthorizationBound: authority.authorizationBound, ReadyMembership: membership, WorkflowSnapshot: workflowSnapshot}
	}
	// Empty pages and workflow-only tails carry the same final current fence.
	binding, err := s.checker.readAuthorityBinding(ctx, caller, authority, snapshot)
	if err != nil {
		return service.BeadPage{}, err
	}
	if err := s.checker.ValidateReadAuthorityBinding(ctx, by, binding); err != nil {
		return service.BeadPage{}, err
	}
	return page, nil
}

func (s *GuardedService) compiledFreshReady(ctx context.Context, by model.PrincipalRef, req service.ClaimNextRequest, reader driver.Reader) (bool, error) {
	selector, ok := reader.(driver.FreshReadyOrderExistsReader)
	committed, committedOK := reader.(driver.FreshReadyAuthorityReader)
	domain, domainOK := s.delegate.(readyOrderDomain)
	if !ok || !committedOK || !domainOK {
		return false, driver.ErrUnsupported
	}
	snapshot, err := committed.FreshReadyAuthority(ctx)
	if err != nil {
		return false, err
	}
	caller, authority, filter, err := s.readyProjectFilter(ctx, by, snapshot, req.Project, "bead.claimNext", authz.Modify)
	if err != nil {
		return false, err
	}
	refs, _ := filter.References()
	plan, err := domain.ReadyOrderPlan(service.ReadyRequest{Project: req.Project, ViewID: req.ViewID, Filter: req.Filter, Limit: 1}, reader, filter.Uniform(), refs)
	if err != nil {
		return false, err
	}
	binding, err := s.checker.readAuthorityBinding(ctx, caller, authority, snapshot)
	if err != nil {
		return false, err
	}
	found, err := selector.FreshReadyOrderExists(ctx, plan)
	if err != nil {
		return false, err
	}
	// Head already fences committed authority on this transaction. Reload the
	// caller and host facts through that SAME post-Head reader for the final
	// fence: opening another general-pool reader here deadlocks a saturated
	// PostgreSQL writer pool while every other writer waits for our Head.
	currentBinding, err := s.checker.ReadAuthorityBinding(ctx, by, snapshot)
	if err != nil {
		return false, err
	}
	if currentBinding != binding {
		return false, hierarchyUnavailable()
	}
	return found, nil
}

func readyAccelerationUnavailable(err error) bool { return errors.Is(err, driver.ErrUnsupported) }

// The typed-link successor adds projectLinkTargets alongside Bead.Links.
// Frozen main has neither. Discover the same-package hook so composing that
// reviewed successor automatically applies its current endpoint projection to
// emitted Ready items, without importing its unrelated model/protocol changes.
func (s *GuardedService) projectReadyLinkTargets(ctx context.Context, by model.PrincipalRef, items []service.Bead) error {
	if projector, ok := any(s).(interface {
		projectLinkTargets(context.Context, model.PrincipalRef, []service.Bead) error
	}); ok {
		return projector.projectLinkTargets(ctx, by, items)
	}
	return nil
}
