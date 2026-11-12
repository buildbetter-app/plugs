package service

import (
	"context"
	"encoding/json"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// workflowBlockerResolver lazily binds every durable endpoint and waits-for
// predicate to one pinned core graph. Publishing workflow state additionally
// checks that graph's head inside the host checkpoint transaction.
func (s *Service) workflowBlockerResolver(ctx context.Context, by model.PrincipalRef, meta model.WriteMeta) workflowplugin.BlockerResolver {
	return &workflowCoreResolver{service: s, principal: by}
}

// workflowBlockerResolverForGraph binds workflow durable-target checks to the
// exact core reader used to build a combined ready page. The provider may call
// this resolver while holding its checkpoint read lock; it must never open a
// second core reader for the same page.
func (s *Service) workflowBlockerResolverForGraph(ctx context.Context, by model.PrincipalRef, r driver.Reader, graph map[string]storedBead) (*workflowCoreResolver, error) {
	view := r.Snapshot()
	if view.At == nil || view.Incarnation == "" {
		return nil, failure("internal_error", "ready reader has no snapshot binding")
	}
	meta, err := s.store.Meta(ctx)
	if err != nil {
		return nil, mapped(err)
	}
	if meta.Incarnation != view.Incarnation {
		return nil, mapped(driver.ErrIncarnationChanged{From: view.Incarnation, To: meta.Incarnation})
	}
	if meta.Head.String() != view.At.String() {
		return nil, failure("cursor_invalid", "ready source changed while collecting candidates; restart paging")
	}
	return &workflowCoreResolver{
		service:   s,
		principal: by,
		graph:     graph,
		head:      driver.CoreSnapshotExpectation{StoreID: meta.StoreID, Incarnation: view.Incarnation, Head: *view.At},
	}, nil
}

type workflowCoreResolver struct {
	service   *Service
	principal model.PrincipalRef
	graph     map[string]storedBead
	head      driver.CoreSnapshotExpectation
}

func (r *workflowCoreResolver) CheckpointHead() (driver.CoreSnapshotExpectation, bool) {
	return r.head, r.graph != nil
}

func (r *workflowCoreResolver) IsBlocked(ctx context.Context, principal model.PrincipalRef, _ model.WriteMeta, target model.Ref, kind string, metadata *model.LinkMetadata) (bool, error) {
	if r.graph == nil {
		if principal.Kind == "" {
			principal = r.principal
		}
		reader, err := r.service.reader(ctx, principal, nil)
		if err != nil {
			return true, err
		}
		defer reader.Close()
		graph, err := scanBeads(ctx, reader)
		if err != nil {
			return true, err
		}
		snapshot := reader.Snapshot()
		meta, err := r.service.store.Meta(ctx)
		if err != nil {
			return true, mapped(err)
		}
		if snapshot.Incarnation != meta.Incarnation {
			return true, mapped(driver.ErrIncarnationChanged{From: snapshot.Incarnation, To: meta.Incarnation})
		}
		r.head = driver.CoreSnapshotExpectation{StoreID: meta.StoreID, Incarnation: snapshot.Incarnation, Head: *snapshot.At}
		r.graph = graph
	}
	row, ok := r.graph[target.ID]
	if !ok || !target.IsID() || !target.Branch.IsZero() {
		return true, nil
	}
	effective := r.service.ontology.effective(row.doc.Project)
	status := state(row.doc).Status
	satisfied := false
	switch kind {
	case "blocks":
		satisfied = effective.SatisfiesBlocks(status)
	case "conditional-blocks":
		satisfied = effective.SatisfiesConditionalBlocks(status)
	case "parent-child":
		category, known := effective.Category(status)
		satisfied = known && category != "backlog"
		if satisfied {
			var err error
			satisfied, err = r.service.ontology.readyDependencies(row.doc, r.graph)
			if err != nil {
				return false, err
			}
		}
	case "waits-for":
		link := pipeline.Link{To: target, Kind: kind, Metadata: metadata}
		satisfied = r.service.ontology.waitsForSatisfied(link, row, r.graph)
	default:
		return false, nil
	}
	return !satisfied, nil
}

// validateWorkflowEndpoints rejects absent or cross-project ordinary endpoints
// before any provider CAS. Workflow endpoint ownership is checked by Provider.
func (s *Service) validateWorkflowEndpoints(ctx context.Context, by model.PrincipalRef, project string, refs ...model.Ref) error {
	var ordinary []model.Ref
	for _, ref := range refs {
		if workflowRef(ref) {
			continue
		}
		if err := namedRef(ref); err != nil {
			return err
		}
		ordinary = append(ordinary, ref)
	}
	if len(ordinary) == 0 {
		return nil
	}
	reader, err := s.reader(ctx, by, nil)
	if err != nil {
		return err
	}
	defer reader.Close()
	graph, err := scanBeads(ctx, reader)
	if err != nil {
		return err
	}
	for _, ref := range ordinary {
		row, ok := graph[ref.ID]
		if !ok {
			return failure("not_found", "workflow ordinary endpoint was not found")
		}
		if row.doc.Project != project {
			return failure("forbidden", "workflow ordinary endpoint is outside the project")
		}
	}
	return nil
}

func workflowAuxiliaryRefs(parent *model.Ref, links []workflowplugin.RelationSpec) []model.Ref {
	refs := make([]model.Ref, 0, len(links)+1)
	if parent != nil {
		refs = append(refs, *parent)
	}
	for _, link := range links {
		refs = append(refs, link.To)
	}
	return refs
}
func workflowUpdateRefs(fields map[string]any) ([]model.Ref, error) {
	data, err := json.Marshal(fields)
	if err != nil {
		return nil, failure("invalid_params", "invalid workflow fields")
	}
	var aux struct {
		Parent *model.Ref                    `json:"parent"`
		Links  []workflowplugin.RelationSpec `json:"links"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return nil, failure("invalid_params", "invalid workflow auxiliary fields")
	}
	return workflowAuxiliaryRefs(aux.Parent, aux.Links), nil
}

// workflowWriteGuard runs only on a receipt miss, after ordinary pre-image
// authorization. Its checkpoint expectation joins the writer's tracked reads.
func (s *Service) workflowWriteGuard(ctx context.Context, proposal port.ProposedChange) (driver.CheckpointExpectation, error) {
	ids := []string{}
	for _, change := range proposal.Changes {
		if change.Resource.Kind != "bead" || !hasDocument(change.Before) {
			continue
		}
		if proposal.Meta.Method == "bead.claim" || proposal.Meta.Method == "bead.claimNext" || proposal.Meta.Method == "bead.close" || proposal.Meta.Method == "tx.apply" && change.Op.Kind == model.OpClose {
			ids = append(ids, change.Resource.Ref.ID)
		}
	}
	if len(ids) == 0 {
		return driver.CheckpointExpectation{}, nil
	}
	if proposal.Meta.Method == "bead.claim" || proposal.Meta.Method == "bead.claimNext" {
		// The existing guard API carries the Writer's exact base revision.
		// Its ontology graph read already tracks graph/blocking at this base,
		// so both that sentinel and the checkpoint expectation fence publish.
		reader, err := s.reader(ctx, proposal.Principal, &AsOf{Revision: proposal.BaseRevision})
		if err != nil {
			return driver.CheckpointExpectation{}, err
		}
		defer reader.Close()
		graph, err := scanBeads(ctx, reader)
		if err != nil {
			return driver.CheckpointExpectation{}, err
		}
		resolver := &workflowCoreResolver{service: s, principal: proposal.Principal, graph: graph}
		facts, expectation, err := s.workflowProvider().OrdinaryParentSnapshot(ctx, proposal.Principal, sortedIDs(graph), proposal.Meta, resolver)
		if err != nil {
			return driver.CheckpointExpectation{}, err
		}
		lookup := func(id string) (workflowplugin.OrdinaryParentFact, error) { return facts[id], nil }
		for _, id := range ids {
			blocked, err := resolver.mixedParentBlocked(ctx, model.RefByID(id, model.Natural64(0)), lookup)
			if err != nil {
				return driver.CheckpointExpectation{}, err
			}
			if blocked {
				return driver.CheckpointExpectation{}, failure("ontology_guard_failed", "an active workflow parent or dependency is not satisfied")
			}
		}
		return expectation, nil
	}
	blocked, children, expectation, err := s.workflowProvider().OrdinaryCloseSnapshot(ctx, proposal.Principal, ids, proposal.Meta)
	if err != nil {
		return driver.CheckpointExpectation{}, err
	}
	setWorkflowCloseFacts(ctx, children)
	for _, id := range ids {
		if blocked[id] {
			return driver.CheckpointExpectation{}, failure("ontology_guard_failed", "an active workflow dependency is not satisfied")
		}
	}
	return expectation, nil
}

func (r *workflowCoreResolver) IsParentBlocked(ctx context.Context, by model.PrincipalRef, meta model.WriteMeta, parent model.Ref, mixed func(string) (workflowplugin.OrdinaryParentFact, error)) (bool, error) {
	blocked, err := r.IsBlocked(ctx, by, meta, parent, "parent-child", nil)
	if err != nil || blocked {
		return blocked, err
	}
	return r.mixedParentBlocked(ctx, parent, func(id string) (workflowplugin.OrdinaryParentFact, error) {
		if row, exists := r.graph[id]; !exists || meta.Project != "" && row.doc.Project != meta.Project {
			return workflowplugin.OrdinaryParentFact{Blocked: true}, nil
		}
		return mixed(id)
	})
}

func (r *workflowCoreResolver) mixedParentBlocked(ctx context.Context, parent model.Ref, mixed func(string) (workflowplugin.OrdinaryParentFact, error)) (bool, error) {
	seen := map[string]bool{}
	for {
		if err := ctx.Err(); err != nil {
			return true, err
		}
		if !parent.IsID() || !parent.Branch.IsZero() || seen[parent.ID] {
			return true, nil
		}
		seen[parent.ID] = true
		row, exists := r.graph[parent.ID]
		if !exists {
			return true, nil
		}
		next, err := pipeline.HierarchyParent(row.doc)
		if err != nil {
			return true, err
		}
		fact, err := mixed(parent.ID)
		if err != nil {
			return true, err
		}
		if fact.Parent != nil && next != nil && *fact.Parent != *next {
			return true, failure("internal_error", "conflicting persisted mixed hierarchy parents")
		}
		if fact.Blocked {
			return true, nil
		}
		if next == nil {
			return false, nil
		}
		parent = *next
	}
}
