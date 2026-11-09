package workflow

import (
	"context"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Validate the complete declared graph in its actual typed address domains.
// Workflow refs contain a slash and cannot enter ordinary hierarchy sentinels.
// No address is rewritten to make it pass an ordinary storage validator.
func validateSourceGraph(ctx context.Context, plan driver.SourcePlanePlanV1, entities map[driver.SourceEntityKey]driver.SourcePlaneEntityV1) error {
	parents := make(map[string]string, len(entities))
	scheduling := make(map[string][]string, len(entities))
	for _, entity := range plan.Entities {
		ref := model.RefByID(entity.TargetID, model.Natural64(0))
		if err := validateRef(ref); err != nil {
			return err
		}
		if entity.TargetKind == "bead" {
			if err := driver.ValidateHierarchyRef(ref); err != nil {
				return err
			}
		} else if !workflowTarget(ref) {
			return sourceRefusal("active graph endpoint lacks a workflow address")
		}
	}
	for _, relation := range plan.Relations {
		if err := ctx.Err(); err != nil {
			return err
		}
		if relation.Kind == "" || len(relation.Kind) > 32 || !utf8.ValidString(relation.Kind) {
			return sourceRefusal("invalid source dependency kind")
		}
		if relation.External != nil {
			if err := model.ValidateLinkTarget(model.LinkTargetExternal, model.RefByID(*relation.External, model.Natural64(0))); err != nil {
				return err
			}
			continue
		}
		from, to := entities[relation.From].TargetID, entities[*relation.To].TargetID
		if relation.Kind == "parent-child" {
			if prior := parents[from]; prior != "" && prior != to {
				return sourceRefusal("conflicting declared typed parents")
			}
			parents[from] = to
		}
		switch relation.Kind {
		case "blocks", "conditional-blocks", "parent-child":
			scheduling[from] = append(scheduling[from], to)
		}
	}
	// These are the scheduling-cycle and ancestor-blocking rules used by the
	// ordinary graph, applied to the full mixed graph with its original refs.
	visiting, done := map[string]bool{}, map[string]bool{}
	var visit func(string) error
	visit = func(id string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if visiting[id] {
			return sourceRefusal("declared scheduling graph contains a cycle")
		}
		if done[id] {
			return nil
		}
		visiting[id] = true
		for _, target := range scheduling[id] {
			if err := visit(target); err != nil {
				return err
			}
		}
		delete(visiting, id)
		done[id] = true
		return nil
	}
	for _, entity := range plan.Entities {
		if err := visit(entity.TargetID); err != nil {
			return err
		}
	}
	ancestor := func(child, candidate string) (bool, error) {
		for parent := parents[child]; parent != ""; parent = parents[parent] {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			if parent == candidate {
				return true, nil
			}
		}
		return false, nil
	}
	for _, relation := range plan.Relations {
		if relation.External != nil || (relation.Kind != "blocks" && relation.Kind != "conditional-blocks" && relation.Kind != "waits-for") {
			continue
		}
		from, to := entities[relation.From].TargetID, entities[*relation.To].TargetID
		forward, err := ancestor(from, to)
		if err != nil {
			return err
		}
		backward, err := ancestor(to, from)
		if err != nil {
			return err
		}
		if forward || backward {
			return sourceRefusal("blocking link joins declared relatives")
		}
	}
	return ctx.Err()
}
