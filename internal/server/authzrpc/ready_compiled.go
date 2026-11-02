package authzrpc

import (
	"context"
	"errors"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Only the complete real Ready port enters this adapter. Legacy delegate
// paging, History, ClaimNext, generic filterPage and all write paths stay put.
func (s *GuardedService) filterReadyPage(ctx context.Context, by model.PrincipalRef, limit int, position *service.PagePosition, page service.BeadPage) (service.BeadPage, error) {
	reader, snapshot, closeReader, err := s.current(ctx, nil)
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
	template, compileErr := s.checker.chain.Compile(authz.ListRequest{Caller: caller, Action: authz.Read, ResourceKind: authz.Bead, Method: "bead.ready"})
	var selected []int
	total, offset := 0, 0
	if errors.Is(compileErr, authz.ErrCheckOnly) {
		// This new P84 arm is chosen solely by the actual Compile sentinel.
		if len(page.Items) > listPostfilterCap {
			return service.BeadPage{}, postfilterCap()
		}
		requests := make([]authz.CheckRequest, len(page.Items))
		for i, bead := range page.Items {
			requests[i] = authz.CheckRequest{Caller: caller, Action: authz.Read, Resource: authz.Resource{Kind: authz.Bead, Ref: model.RefByID(bead.ID, model.Natural64(0))}, Method: "bead.ready"}
		}
		decisions, err := s.checker.chain.Check(ctx, authorization, requests)
		if err != nil {
			return service.BeadPage{}, authzError(err)
		}
		if len(decisions) != len(requests) {
			return service.BeadPage{}, fmt.Errorf("authorization result cardinality")
		}
		for i, d := range decisions {
			switch d.Effect {
			case authz.Allow:
				selected = append(selected, i)
			case authz.Deny, authz.NoDecision:
			default:
				return service.BeadPage{}, fmt.Errorf("invalid authorization decision")
			}
		}
	} else if compileErr != nil {
		return service.BeadPage{}, authzError(compileErr)
	} else if readySQLCompatible(template, page.Items) {
		input := make([]driver.ReadCandidate, len(page.Items))
		for i, bead := range page.Items {
			ref := model.RefByID(bead.ID, model.Natural64(0))
			prepared, err := template.ReplayReadPreparation(ctx, authorization, authz.Resource{Kind: authz.Bead, Ref: ref})
			if err != nil {
				return service.BeadPage{}, authzError(err)
			}
			input[i] = driver.ReadCandidate{Ordinal: i, Ref: ref, Prepared: prepared}
		}
		// Reference Match validates every actual resource before page bounds.
		// Keep that order, including errors on resources beyond the first page.
		limit, offset, err = readySelectionBounds(limit, position, page.AsOf)
		if err != nil {
			return service.BeadPage{}, err
		}
		view := reader.Snapshot()
		request := &driver.ReadSelection{View: view, Candidates: input, Predicate: readyStoragePredicate(template), PrincipalKind: template.Request.Caller.Principal.Kind, Offset: offset, Limit: limit}
		if sponsor := template.Request.Caller.Principal.Sponsor; sponsor != nil {
			request.SponsorKind = sponsor.Kind
		}
		result, err := reader.Query(ctx, driver.Query{AsOf: view.At, ReadSelection: request})
		if errors.Is(err, driver.ErrReadSelectionOffset) {
			return service.BeadPage{}, readySelectionCursorError()
		}
		if err != nil {
			return service.BeadPage{}, authzError(err)
		}
		if result.ReadSelection == nil || result.NextCursor != nil || len(result.Items) != 0 || result.AsOf.String() != view.At.String() {
			return service.BeadPage{}, fmt.Errorf("invalid Ready selection response")
		}
		selected, total = result.ReadSelection.Ordinals, result.ReadSelection.Total
		if err := validateReadySelection(selected, total, offset, limit, len(page.Items)); err != nil {
			return service.BeadPage{}, err
		}
		return s.finishReadySelection(ctx, by, caller, authorization, snapshot, page, selected, total, offset)
	} else {
		// Preserve full reference availability for all valid finite templates
		// outside this first SQL vocabulary, including every project caveat.
		// No postfiltered SQL page or unsupported-node empty success is used.
		// This route is explicitly unqualified for P50, at any cardinality.
		for i, bead := range page.Items {
			ok, err := template.Match(ctx, authorization, authz.Resource{Kind: authz.Bead, Ref: model.RefByID(bead.ID, model.Natural64(0))})
			if err != nil {
				return service.BeadPage{}, authzError(err)
			}
			if ok {
				selected = append(selected, i)
			}
		}
	}
	total = len(selected)
	limit, offset, err = readySelectionBounds(limit, position, page.AsOf)
	if err != nil {
		return service.BeadPage{}, err
	}
	if offset > total {
		return service.BeadPage{}, readySelectionCursorError()
	}
	selected = selected[offset:min(offset+limit, total)]
	return s.finishReadySelection(ctx, by, caller, authorization, snapshot, page, selected, total, offset)
}

func readySelectionBounds(limit int, position *service.PagePosition, asOf service.AsOf) (int, int, error) {
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 1000 {
		return 0, 0, &model.Error{Message: "limit outside 1..1000", Data: model.ErrorData{Code: "invalid_params"}}
	}
	offset := 0
	if position != nil {
		if position.Offset < 0 || position.AsOf.Incarnation != asOf.Incarnation {
			return 0, 0, readySelectionCursorError()
		}
		offset = position.Offset
	}
	return limit, offset, nil
}
func readySelectionCursorError() error {
	return &model.Error{Message: "page position does not match authorized snapshot", Data: model.ErrorData{Code: "cursor_invalid"}}
}
func validateReadySelection(ordinals []int, total, offset, limit, candidates int) error {
	if total < 0 || total > candidates || offset > total || len(ordinals) != min(limit, total-offset) {
		return fmt.Errorf("invalid or partial Ready selection")
	}
	for i, ordinal := range ordinals {
		if ordinal < 0 || ordinal >= candidates || i > 0 && ordinal <= ordinals[i-1] {
			return fmt.Errorf("invalid Ready selection ordinal")
		}
	}
	return nil
}

func (s *GuardedService) finishReadySelection(ctx context.Context, by model.PrincipalRef, caller authz.Caller, authorization *slice, snapshot Snapshot, page service.BeadPage, selected []int, total, offset int) (service.BeadPage, error) {
	out := service.BeadPage{AsOf: page.AsOf}
	if len(selected) != 0 {
		out.Items = make([]service.Bead, len(selected))
	}
	for i, ordinal := range selected {
		out.Items[i] = page.Items[ordinal]
	}
	end := offset + len(selected)
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
	if end < total {
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
