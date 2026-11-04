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

// workflowBlockerResolver is the narrow bridge from the plugin's active
// checkpoint to the ordinary graph and ontology. It reads one complete core
// snapshot for each resolution so a workflow ready/claim decision never
// treats an absent or stale durable endpoint as ready.
func (s *Service) workflowBlockerResolver(ctx context.Context, by model.PrincipalRef, meta model.WriteMeta) workflowplugin.BlockerResolver {
	return workflowplugin.BlockerResolverFunc(func(ctx context.Context, principal model.PrincipalRef, writeMeta model.WriteMeta, target model.Ref, kind string, metadata *model.LinkMetadata) (bool, error) {
		if principal.Kind == "" {
			principal = by
		}
		if writeMeta.Method == "" {
			writeMeta = meta
		}
		reader, err := s.reader(ctx, principal, nil)
		if err != nil {
			return true, err
		}
		defer reader.Close()
		graph, err := scanBeads(ctx, reader)
		if err != nil {
			return true, err
		}
		row, ok := graph[target.ID]
		if !ok || !target.IsID() || !target.Branch.IsZero() {
			return true, nil
		}
		effective := s.ontology.effective(row.doc.Project)
		status := state(row.doc).Status
		satisfied := false
		switch kind {
		case "blocks":
			satisfied = effective.SatisfiesBlocks(status)
		case "conditional-blocks":
			satisfied = effective.SatisfiesConditionalBlocks(status)
		case "waits-for":
			link := pipeline.Link{To: target, Kind: kind, Metadata: metadata}
			satisfied = s.ontology.waitsForSatisfied(link, row, graph)
		default:
			// Non-blocking auxiliary relations do not affect ready membership.
			return false, nil
		}
		return !satisfied, nil
	})
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
		if change.Resource.Kind != "bead" || !hasDocument(change.After) {
			continue
		}
		if proposal.Meta.Method == "bead.claim" || proposal.Meta.Method == "bead.claimNext" || proposal.Meta.Method == "bead.close" || proposal.Meta.Method == "tx.apply" && change.Op.Kind == model.OpClose {
			ids = append(ids, change.Resource.Ref.ID)
		}
	}
	if len(ids) == 0 {
		return driver.CheckpointExpectation{}, nil
	}
	blocked, expectation, err := s.workflow.OrdinaryBlockerSnapshot(ctx, proposal.Principal, ids, proposal.Meta)
	if err != nil {
		return driver.CheckpointExpectation{}, err
	}
	for _, id := range ids {
		if blocked[id] {
			return driver.CheckpointExpectation{}, failure("ontology_guard_failed", "an active workflow dependency is not satisfied")
		}
	}
	return expectation, nil
}
