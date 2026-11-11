package service

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// Direct active links retain their checkpoint-only history semantics. The
// existing detached preparation and head-guarded save fence validation against
// both planes without introducing an ordinary changeset or receipt.
func (s *Service) linkWorkflowChecked(ctx context.Context, by model.PrincipalRef, req LinkRequest, request workflowplugin.LinkRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	if err := s.admit(ctx, by); err != nil {
		return port.WriteOutcome{}, err
	}
	meta.Method = "bead.link"
	var commit model.Commit
	prepared, err := s.workflow.PrepareOperations(ctx, func(staged *workflowplugin.Provider) error {
		before, err := staged.StagedCycleGraph(ctx, by, meta)
		if err != nil {
			return err
		}
		commit, err = staged.Link(ctx, by, request, meta)
		if err != nil {
			return err
		}
		after, err := staged.StagedCycleGraph(ctx, by, meta)
		if err != nil || before.Revision == after.Revision {
			return err
		}
		reader, err := s.store.Read(ctx, driver.View{Incarnation: before.Head.Incarnation, At: &before.Head.Head})
		if err != nil {
			return err
		}
		graph, err := scanBeads(ctx, reader)
		closeErr := reader.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		for _, ref := range []model.Ref{req.From, req.To} {
			if workflowRef(ref) {
				continue
			}
			row, exists := graph[ref.ID]
			if !exists {
				return failure("not_found", "workflow ordinary endpoint was not found")
			}
			if row.doc.Project != meta.Project {
				return failure("forbidden", "workflow ordinary endpoint is outside the project")
			}
		}
		return validateWorkflowCycleLink(ctx, staged, by, meta, graph, model.LinkOp{From: req.From, To: req.To, LinkKind: req.Kind, Metadata: req.Metadata})
	})
	if err != nil {
		return port.WriteOutcome{}, mapped(err)
	}
	if !prepared.Empty() {
		head, ok := prepared.CoreSnapshot()
		if !ok {
			return port.WriteOutcome{}, mapped(driver.ErrUnsupported)
		}
		mutation, err := prepared.Checkpoint(ctx)
		if err != nil {
			return port.WriteOutcome{}, mapped(err)
		}
		store, ok := s.store.(driver.HeadGuardedCheckpointStore)
		if !ok {
			return port.WriteOutcome{}, mapped(driver.ErrUnsupported)
		}
		if _, err := store.SaveCheckpointAtHead(ctx, s.lease, mutation.Key, mutation.Expected, mutation.Payload, head); err != nil {
			return port.WriteOutcome{}, mapped(err)
		}
	}
	if err := s.admit(ctx, by); err != nil {
		return port.WriteOutcome{}, err
	}
	return port.WriteOutcome{Commit: commit}, nil
}

func cycleLinkChanges(cs model.ChangeSet) bool {
	for _, op := range cs.Ops {
		if op.Kind == model.OpLink && op.Link != nil && op.Link.TargetKind != model.LinkTargetExternal {
			return true
		}
	}
	return false
}

// Validate against the ordered core image and the SAME detached checkpoint as
// the mutation. Existing preparation joins its head/namespace expectations to
// publication; this check opens no independent reader and changes no locks.
func validateWorkflowCycleLink(ctx context.Context, staged *workflowplugin.Provider, by model.PrincipalRef, meta model.WriteMeta, core map[string]storedBead, link model.LinkOp) error {
	if link.From == link.To {
		if !workflowRef(link.From) {
			return failure("ontology_guard_failed", "a dependency cannot target itself")
		}
		return failure("invalid_params", "a dependency cannot target itself")
	}
	if link.LinkKind != "blocks" && link.LinkKind != "conditional-blocks" && link.LinkKind != "parent-child" && link.LinkKind != "waits-for" {
		return nil
	}
	snapshot, err := staged.StagedCycleGraph(ctx, by, meta)
	if err != nil {
		return err
	}
	parents := map[string]string{}
	graph := map[string][]string{}
	add := func(from, to, kind string) error {
		if kind == "parent-child" {
			if prior := parents[from]; prior != "" && prior != to {
				return failure("ontology_guard_failed", "conflicting mixed hierarchy parents")
			}
			parents[from] = to
		}
		if kind == "blocks" || kind == "conditional-blocks" || kind == "parent-child" {
			graph[from] = append(graph[from], to)
		}
		return nil
	}
	for id, row := range core {
		parent, err := pipeline.HierarchyParent(row.doc)
		if err != nil {
			return err
		}
		if parent != nil {
			if err := add(id, parent.ID, "parent-child"); err != nil {
				return err
			}
		}
		for _, edge := range row.doc.Links {
			if edge.TargetKind == model.LinkTargetExternal || !edge.To.IsID() || !edge.To.Branch.IsZero() {
				continue
			}
			if err := add(id, edge.To.ID, edge.Kind); err != nil {
				return err
			}
		}
	}
	for _, edge := range snapshot.Edges {
		if err := add(edge.From.ID, edge.To.ID, edge.Kind); err != nil {
			return err
		}
	}
	ancestor := func(child, candidate string) (bool, error) {
		seen := map[string]bool{}
		for parent := parents[child]; parent != ""; parent = parents[parent] {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			if seen[parent] {
				return false, failure("ontology_guard_failed", "mixed hierarchy contains a cycle")
			}
			seen[parent] = true
			if parent == candidate {
				return true, nil
			}
		}
		return false, nil
	}
	if link.LinkKind == "parent-child" {
		if prior := parents[link.From.ID]; prior != "" && prior != link.To.ID {
			return failure("ontology_guard_failed", "conflicting mixed hierarchy parents")
		}
	} else {
		for _, pair := range [][2]string{{link.From.ID, link.To.ID}, {link.To.ID, link.From.ID}} {
			found, err := ancestor(pair[0], pair[1])
			if err != nil {
				return err
			}
			if found {
				return failure("ontology_guard_failed", "blocking link joins an ancestor and descendant")
			}
		}
	}
	if link.LinkKind == "waits-for" {
		return nil
	}
	seen := map[string]bool{}
	queue := []string{link.To.ID}
	for i := 0; i < len(queue); i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		id := queue[i]
		if id == link.From.ID {
			return failure("ontology_guard_failed", "dependency would form a scheduling cycle")
		}
		if !seen[id] {
			seen[id] = true
			queue = append(queue, graph[id]...)
		}
	}
	return nil
}
