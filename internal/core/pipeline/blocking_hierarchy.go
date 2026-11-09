package pipeline

import "github.com/buildbetter-app/plugs/internal/core/model"

// Scheduling-cycle detection cannot reject a redundant child-to-ancestor
// blocker: that edge points in the same direction as the hierarchy. B06 also
// forbids blocking either way between relatives, independently of cycles.
// Both parent representations participate; opaque/association edges do not.
func checkBlockingHierarchy(docs map[string]Document) error {
	ancestors := make(map[string]map[string]bool)
	var collect func(string) map[string]bool
	collect = func(id string) map[string]bool {
		if known, ok := ancestors[id]; ok {
			return known
		}
		result := make(map[string]bool)
		ancestors[id] = result
		parents := []string{}
		if parent := docs[id].Parent; parent != nil {
			parents = append(parents, parent.ID)
		}
		for _, edge := range docs[id].Links {
			if edge.TargetKind != model.LinkTargetExternal && edge.Kind == "parent-child" {
				parents = append(parents, edge.To.ID)
			}
		}
		for _, parent := range parents {
			result[parent] = true
			for ancestor := range collect(parent) {
				result[ancestor] = true
			}
		}
		return result
	}
	for id, doc := range docs {
		for _, edge := range doc.Links {
			if edge.TargetKind != model.LinkTargetExternal && dependencyIsBlocking(edge.Kind) && (collect(id)[edge.To.ID] || collect(edge.To.ID)[id]) {
				return failure("ontology_guard_failed", "blocking link cannot join an ancestor and descendant")
			}
		}
	}
	return nil
}
