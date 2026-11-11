package converterroundtrip

import (
	"context"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/storage/depid"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

type sourceLinkPair struct{ from, to string }

// projectMixedSourceLinks projects only workflow-owned relations. Unchanged
// admitted edges retain their complete original tuple, lexical tokens and source
// fields. New edges use the established endpoint UUID and declared source planes;
// no source timestamp, actor or origin is manufactured.
func projectMixedSourceLinks(ctx context.Context, edges []reverseconverter.Record, projection sourceCoreProjection, plan driver.SourcePlanePlanV1, initial, current []workflowplugin.RelationView, included, terminal map[string]bool) ([]reverseconverter.Record, error) {
	if sameReverseJSON(initial, current) {
		// Preserve the established unchanged-source projection, including raw
		// origin multiplicity and absent/null opaque slots, byte for byte.
		var result []reverseconverter.Record
		for _, edge := range edges {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			relation := projection.relations[driver.SourceEdgeKey{Project: edge.Project, Table: edge.Source.Table, ID: edge.ID}]
			if terminal[projection.entities[relation.From].TargetID] || relation.To != nil && terminal[projection.entities[*relation.To].TargetID] {
				continue
			}
			result = append(result, edge)
		}
		return result, nil
	}
	entities := map[string]driver.SourcePlaneEntityV1{}
	for _, entity := range plan.Entities {
		if _, duplicate := entities[entity.TargetID]; duplicate {
			return nil, ReverseRefusal{"ambiguous_mixed_source_endpoint"}
		}
		entities[entity.TargetID] = entity
	}
	index := func(relations []workflowplugin.RelationView) (map[sourceLinkPair]workflowplugin.RelationView, error) {
		result := map[sourceLinkPair]workflowplugin.RelationView{}
		for _, relation := range relations {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			pair := sourceLinkPair{relation.From, relation.To.ID}
			if _, duplicate := result[pair]; duplicate {
				return nil, ReverseRefusal{"ambiguous_mixed_source_relation"}
			}
			if entities[pair.from].TargetID == "" || entities[pair.to].TargetID == "" {
				return nil, ReverseRefusal{"unmapped_mixed_source_relation"}
			}
			result[pair] = relation
		}
		return result, nil
	}
	before, err := index(initial)
	if err != nil {
		return nil, err
	}
	after, err := index(current)
	if err != nil {
		return nil, err
	}
	var result []reverseconverter.Record
	consumed := map[sourceLinkPair]int{}
	for _, edge := range edges {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := driver.SourceEdgeKey{Project: edge.Project, Table: edge.Source.Table, ID: edge.ID}
		if projection.inertRelations[key] {
			result = append(result, edge)
			continue
		}
		relation := projection.relations[key]
		from := projection.entities[relation.From]
		toTerminal := relation.To != nil && terminal[projection.entities[*relation.To].TargetID]
		if terminal[from.TargetID] || toTerminal {
			continue
		}
		core, err := projection.coreEdge(reverseSourceRecord(edge))
		if err != nil {
			return nil, err
		}
		if core {
			result = append(result, edge)
			continue
		}
		if !included[from.TargetID] {
			result = append(result, edge)
			continue
		}
		if relation.To == nil {
			return nil, ReverseRefusal{"unmapped_mixed_source_relation"}
		}
		pair := sourceLinkPair{from.TargetID, projection.entities[*relation.To].TargetID}
		live, exists := after[pair]
		if !exists {
			// An explicit unlink removes every retained origin for this pair.
			continue
		}
		original, exists := before[pair]
		if !exists {
			return nil, ReverseRefusal{"missing_initial_mixed_source_relation"}
		}
		if sameReverseJSON(original, live) {
			result = append(result, edge)
		} else {
			if consumed[pair] != 0 {
				return nil, ReverseRefusal{"ambiguous_changed_mixed_source_origin"}
			}
			projected, err := projectMixedSourceEdge(edge, live, entities[pair.from], entities[pair.to])
			if err != nil {
				return nil, err
			}
			result = append(result, projected)
		}
		consumed[pair]++
	}
	for pair := range consumed {
		delete(after, pair)
	}
	pairs := make([]sourceLinkPair, 0, len(after))
	for pair := range after {
		pairs = append(pairs, pair)
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].from != pairs[j].from {
			return pairs[i].from < pairs[j].from
		}
		return pairs[i].to < pairs[j].to
	})
	for _, pair := range pairs {
		if terminal[pair.from] || terminal[pair.to] {
			continue
		}
		from, to := entities[pair.from], entities[pair.to]
		if from.TargetKind != "workflow:item" && to.TargetKind != "workflow:item" {
			return nil, ReverseRefusal{"changed_source_relationships"}
		}
		if _, existed := before[pair]; existed {
			return nil, ReverseRefusal{"missing_mixed_source_edge_origin"}
		}
		edge := reverseconverter.Record{Kind: "edge", ID: depid.New(from.Source.ID, to.Source.ID), Project: from.Source.Project}
		projected, err := projectMixedSourceEdge(edge, after[pair], from, to)
		if err != nil {
			return nil, err
		}
		result = append(result, projected)
	}
	return result, nil
}

func projectMixedSourceEdge(edge reverseconverter.Record, relation workflowplugin.RelationView, from, to driver.SourcePlaneEntityV1) (reverseconverter.Record, error) {
	if from.Source.Project != to.Source.Project || from.Source.Project != edge.Project || from.Source.ID == "" || to.Source.ID == "" {
		return reverseconverter.Record{}, ReverseRefusal{"unmapped_mixed_source_relation"}
	}
	hadOrigin := edge.Source.Table != ""
	edge.Body, edge.Source.Fields = cloneRow(edge.Body), cloneRow(edge.Source.Fields)
	if edge.Body == nil {
		edge.Body = map[string]any{}
	}
	if edge.Source.Fields == nil {
		edge.Source.Fields = map[string]any{"metadata": nil, "thread_id": nil, "created_at": nil, "created_by": nil}
	}
	// Only an unchanged relation may retain the original opaque slots. A
	// changed relation is projected from its current declared metadata.
	for key := range edge.Source.Fields {
		if dumpformat.NativeEdgeField(key) {
			delete(edge.Source.Fields, key)
		}
	}
	if !hadOrigin {
		edge.Source.Fields["metadata"], edge.Source.Fields["thread_id"] = nil, nil
	}
	edge.NativeEdgeTokens = map[string]string{}
	edge.Source.Table = dumpformat.DurableDependencyTable
	if from.Source.Plane == dumpformat.EphemeralEntityTable {
		edge.Source.Table = dumpformat.EphemeralDependencyTable
	}
	arm := "depends_on_issue_id"
	if to.Source.Plane == dumpformat.EphemeralEntityTable {
		arm = dumpformat.EphemeralDependencyTargetField
	}
	edge.Body["from"], edge.Body["to"], edge.Body["target_kind"], edge.Body["type"] = from.Source.ID, to.Source.ID, arm, relation.Kind
	edge.Source.Fields["id"], edge.Source.Fields["issue_id"], edge.Source.Fields["type"] = edge.ID, from.Source.ID, relation.Kind
	for _, name := range []string{"depends_on_issue_id", dumpformat.EphemeralDependencyTargetField, "depends_on_external"} {
		edge.Source.Fields[name] = nil
	}
	edge.Source.Fields[arm] = to.Source.ID
	if relation.Metadata != nil {
		normalized, err := relation.Metadata.ForKind(relation.Kind)
		if err != nil {
			return reverseconverter.Record{}, err
		}
		if relation.Metadata.HasPredicate() {
			edge.Source.Fields["metadata"] = map[string]any{"gate": relation.Metadata.Gate, "also_blocks": relation.Metadata.AlsoBlocks}
			delete(edge.NativeEdgeTokens, "beads.metadata")
		}
		for key, token := range relation.Metadata.NativeExtensions {
			value, err := dumpformat.DecodeNativeEdgeToken(token)
			if err != nil {
				return reverseconverter.Record{}, err
			}
			edge.Source.Fields[strings.TrimPrefix(key, "beads.")] = value
			edge.NativeEdgeTokens[key] = token
		}
		if normalized.HasPredicate() {
			predicate, err := dumpformat.SourceEdgeMetadata(dumpformat.EdgeVersionLocalOpaque, edge.ID, relation.Kind, edge.Source.Fields["metadata"])
			if err != nil || predicate == nil || predicate.Gate != normalized.Gate || predicate.AlsoBlocks != normalized.AlsoBlocks {
				return reverseconverter.Record{}, ReverseRefusal{"mixed_source_link_predicate_not_representable"}
			}
		}
	}
	for _, field := range []string{"metadata", "thread_id"} {
		if value, exists := edge.Source.Fields[field]; exists {
			edge.Body[field] = value
		} else {
			delete(edge.Body, field)
		}
	}
	return edge, nil
}
