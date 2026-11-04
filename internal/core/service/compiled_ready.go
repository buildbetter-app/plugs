package service

import (
	"context"
	"fmt"
	"reflect"
	"slices"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// ReadyOrderPlan lowers only the exact, maintained Ready domain. Authorization
// is supplied by the host independently from the raw request resource filter.
// Unsupported domains retain the ordinary exact Ready evaluator.
func (s *Service) ReadyOrderPlan(req ReadyRequest, reader driver.Reader, uniform bool, authorized []model.Ref) (driver.ReadyOrderPlan, error) {
	if _, err := pageOptions(req.Project, req.ViewID, req.Cursor, req.Filter, req.Limit); err != nil {
		return driver.ReadyOrderPlan{}, err
	}
	if err := validatePosition(req.Position); err != nil {
		return driver.ReadyOrderPlan{}, err
	}
	view := reader.Snapshot()
	if view.At == nil || !view.Branch.IsZero() {
		return driver.ReadyOrderPlan{}, driver.ErrUnsupported
	}
	plan := driver.ReadyOrderPlan{
		Incarnation: view.Incarnation, ContentRevision: *view.At, AuthorityRevision: *view.At,
		Scope:                driver.ReadyOrderScope{Project: &req.Project},
		UniformAuthorization: uniform, AuthorizedRefs: authorized,
		Seed: req.ClaimNextSeed, Limit: req.Limit,
	}
	if plan.Limit == 0 {
		plan.Limit = 100
	}
	if req.Position != nil {
		if req.Position.AsOf.Incarnation != view.Incarnation {
			return driver.ReadyOrderPlan{}, mapped(driver.ErrIncarnationChanged{From: req.Position.AsOf.Incarnation, To: view.Incarnation})
		}
		plan.Offset = uint64(req.Position.Offset)
	}
	if req.Filter != nil {
		if len(req.Filter.Types) > 1 {
			return driver.ReadyOrderPlan{}, driver.ErrUnsupported
		}
		if len(req.Filter.Types) == 1 {
			plan.Scope.Type = &req.Filter.Types[0]
		}
		if len(req.Filter.Resources) != 0 {
			plan.RawRefs = make([]model.Ref, len(req.Filter.Resources))
			for i, id := range req.Filter.Resources {
				plan.RawRefs[i] = model.RefByID(id, model.Natural64(0))
			}
		}
	}
	var err error
	plan.Eligibility, err = s.ontology.ReadyProjectionBinding(req.Project)
	return plan, err
}

// HydrateReadyOrder fetches emitted item bodies once, in one batch. Lookahead,
// counts, rank and the complete membership commitment remain thin metadata.
func HydrateReadyOrder(ctx context.Context, reader driver.Reader, keys []driver.SelectionKey) ([]Bead, []driver.Record, error) {
	if len(keys) > 1000 {
		return nil, nil, failure("internal_error", "Ready hydration exceeds page size")
	}
	if len(keys) == 0 {
		return []Bead{}, []driver.Record{}, ctx.Err()
	}
	refs := make([]model.Ref, len(keys))
	wanted := make(map[string]int, len(keys))
	for i, key := range keys {
		if driver.ValidateHierarchyRef(key.Ref) != nil || key.Revision.Validate() != nil {
			return nil, nil, failure("internal_error", "invalid Ready selection key")
		}
		if _, duplicate := wanted[key.Ref.ID]; duplicate {
			return nil, nil, failure("internal_error", "duplicate Ready selection key")
		}
		refs[i], wanted[key.Ref.ID] = key.Ref, i
	}
	rows, err := reader.Get(ctx, refs)
	if err != nil {
		return nil, nil, mapped(err)
	}
	items := make([]Bead, len(keys))
	ordered := make([]driver.Record, len(keys))
	seen := make([]bool, len(keys))
	for _, row := range rows {
		i, found := wanted[row.Ref.ID]
		if !found || seen[i] || row.Kind != "bead" || !driver.SelectionSameRef(row.Ref, keys[i].Ref) || row.Revision.String() != keys[i].Revision.String() {
			return nil, nil, failure("internal_error", "Ready source identity changed")
		}
		doc, err := decodeDocument(row.Body)
		if err != nil || doc.ID != row.Ref.ID {
			return nil, nil, failure("internal_error", "invalid Ready source")
		}
		item, err := beadValue(storedBead{record: row, doc: doc})
		if err != nil {
			return nil, nil, err
		}
		leaf, err := ReadyMembershipLeaf(item)
		if err != nil || leaf.Digest != keys[i].MembershipLeaf || !reflect.DeepEqual(leaf.Key.Order, keys[i].Order) {
			return nil, nil, failure("internal_error", "Ready source differs from maintained membership")
		}
		items[i], ordered[i], seen[i] = item, row, true
	}
	for _, found := range seen {
		if !found {
			return nil, nil, failure("internal_error", "Ready source is missing")
		}
	}
	return items, ordered, nil
}

// ReadyMembershipItem keeps the legacy field set shared by maintained leaves
// and the exact fallback. Parent and link disclosure happen after commitment.
func ReadyMembershipItem(item Bead) driver.ReadyMembershipItem {
	return driver.ReadyMembershipItem{ID: item.ID, Type: item.Type, Project: item.Project,
		Status: item.Status, Revision: item.Revision, Fields: item.Fields,
		Plane: item.Plane, Kind: item.Kind, Plugin: item.Plugin, WorkflowRunID: item.WorkflowRunID}
}

func ReadyMembershipLeaf(item Bead) (driver.ReadyOrderLeaf, error) {
	priority, err := beadPriority(item)
	if err != nil {
		return driver.ReadyOrderLeaf{}, err
	}
	return driver.ReadyOrderLeafFor(ReadyMembershipItem(item), int64(priority))
}

// WorkflowReadyItems captures the provider tail once, independently of core
// pagination, so guarded pages can bind the complete authorized sequence.
func (s *Service) WorkflowReadyItems(ctx context.Context, by model.PrincipalRef, req ReadyRequest) ([]Bead, *WorkflowPageSnapshot, error) {
	if err := s.admit(ctx, by); err != nil {
		return nil, nil, err
	}
	if s.workflow == nil {
		return []Bead{}, nil, nil
	}
	snapshot, err := s.workflow.ReadySnapshot(ctx, by, req.Project, workflowTypes(req.Filter), workflowMeta(by, "bead.ready", req.Project, req.Agent))
	if err != nil {
		return nil, nil, err
	}
	binding := &WorkflowPageSnapshot{Revision: model.Revision(snapshot.Revision), ValidUntil: snapshot.ValidUntil}
	if !binding.Valid() {
		return nil, nil, failure("internal_error", "invalid workflow ready snapshot")
	}
	items := make([]Bead, 0, len(snapshot.Items))
	for _, item := range snapshot.Items {
		if req.Filter == nil || len(req.Filter.Resources) == 0 || slices.Contains(req.Filter.Resources, item.ID) {
			items = append(items, workflowBead(item))
		}
	}
	return items, binding, nil
}

func (s *Service) compiledReadyPage(ctx context.Context, req ReadyRequest, reader driver.Reader, restart bool) (BeadPage, error) {
	selector, ok := reader.(driver.ReadyOrderReader)
	if !ok {
		return BeadPage{}, driver.ErrUnsupported
	}
	plan, err := s.ReadyOrderPlan(req, reader, true, nil)
	if err != nil {
		return BeadPage{}, err
	}
	if !restart {
		plan.Seed = ""
	} else if req.Position != nil && req.Position.AsOf.Revision.String() != plan.ContentRevision.String() {
		plan.Offset = 0
	}
	if plan.Offset != 0 {
		// Unguarded live offsets may outlive a shrinking list. Count using thin
		// metadata before clamping; no previous-page body is hydrated.
		countPlan := plan
		countPlan.Offset, countPlan.Limit = 0, 1
		count, err := selector.SelectReadyOrder(ctx, countPlan)
		if err != nil {
			return BeadPage{}, err
		}
		plan.Offset = min(plan.Offset, count.Total)
	}
	selected, err := selector.SelectReadyOrder(ctx, plan)
	if err != nil {
		return BeadPage{}, err
	}
	items, _, err := HydrateReadyOrder(ctx, reader, selected.Keys)
	if err != nil {
		return BeadPage{}, err
	}
	page := BeadPage{Items: items, AsOf: AsOf{Revision: plan.ContentRevision, Incarnation: plan.Incarnation}}
	if selected.More {
		next := plan.Offset + uint64(len(items))
		if next > uint64(^uint(0)>>1) {
			return BeadPage{}, fmt.Errorf("Ready offset overflow")
		}
		page.Next = &PagePosition{Offset: int(next), AsOf: page.AsOf}
	}
	return page, nil
}
