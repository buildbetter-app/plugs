package nativeprojection

import (
	"encoding/json"
	"fmt"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type SourceCoreProjection struct {
	accounting     *dumpformat.Archive
	entities       map[driver.SourceEntityKey]driver.SourcePlaneEntityV1
	relations      map[driver.SourceEdgeKey]driver.SourcePlaneRelationV1
	fields         map[driver.SourceEntityKey]map[string]bool
	opaque         map[driver.SourceEntityKey][]string
	inertRelations map[driver.SourceEdgeKey]bool
}

func NewSourceCoreProjection(profile *driver.RestoreImportProfile, archives ...*dumpformat.Archive) (SourceCoreProjection, error) {
	var archive *dumpformat.Archive
	if len(archives) != 0 {
		archive = archives[0]
	}
	projection := SourceCoreProjection{accounting: archive}
	if profile == nil || profile.SourcePlanes == nil {
		return projection, nil
	}
	plan, err := NativeSourcePlan(archive, *profile.SourcePlanes)
	if err != nil {
		return projection, err
	}
	dispositions := make(map[string]map[string]bool, len(plan.Dispositions))
	for _, d := range plan.Dispositions {
		fields := make(map[string]bool, len(d.Fields))
		for _, rule := range d.Fields {
			fields[rule.Source] = rule.Disposition == "exact-map"
		}
		dispositions[d.Key] = fields
	}
	projection.entities = make(map[driver.SourceEntityKey]driver.SourcePlaneEntityV1, len(plan.Entities))
	projection.fields = make(map[driver.SourceEntityKey]map[string]bool, len(plan.Entities))
	projection.opaque = make(map[driver.SourceEntityKey][]string, len(plan.Entities))
	for _, e := range plan.Entities {
		projection.entities[e.Source] = e
		projection.fields[e.Source] = dispositions[e.DispositionKey]
		for _, d := range plan.Dispositions {
			if d.Key == e.DispositionKey {
				for _, rule := range d.Fields {
					if rule.Disposition == "opaque-metadata" {
						projection.opaque[e.Source] = append(projection.opaque[e.Source], rule.Source)
					}
				}
			}
		}
	}
	projection.relations = make(map[driver.SourceEdgeKey]driver.SourcePlaneRelationV1, len(plan.Relations))
	for _, relation := range plan.Relations {
		projection.relations[relation.Source] = relation
	}
	projection.inertRelations = map[driver.SourceEdgeKey]bool{}
	for _, origin := range plan.Origins {
		if origin.Path == "" && origin.Action == "inert-origin" && (origin.Table == dumpformat.DurableDependencyTable || origin.Table == dumpformat.EphemeralDependencyTable) {
			projection.inertRelations[driver.SourceEdgeKey{Project: origin.Project, Table: origin.Table, ID: origin.ID}] = true
		}
	}
	return projection, nil
}

func (p SourceCoreProjection) MapsField(source dumpformat.Record, field string) bool {
	if p.entities == nil {
		return true
	}
	return p.fields[driver.SourceEntityKey{Project: source.Project, Plane: source.Source.Table, ID: source.ID}][field]
}

func (p SourceCoreProjection) CoreEdge(source dumpformat.Record) (bool, error) {
	if p.entities == nil {
		return true, nil
	}
	// Provider preparation verifies each relation against its raw edge. Route
	// using those complete endpoint tuples, including each endpoint's project.
	relation, ok := p.relations[driver.SourceEdgeKey{Project: source.Project, Table: source.Source.Table, ID: source.ID}]
	if !ok {
		// Provider preparation has already matched this explicit inert origin
		// to every original raw field. It is retained data, not a core link.
		if p.inertRelations[driver.SourceEdgeKey{Project: source.Project, Table: source.Source.Table, ID: source.ID}] {
			return false, nil
		}
		return false, fmt.Errorf("%w: core edge lacks its checked source relation", driver.ErrUnsupported)
	}
	from := p.entities[relation.From]
	if from.TargetKind != "bead" {
		return false, nil
	}
	if relation.External != nil {
		return true, nil
	}
	if relation.To == nil {
		return false, driver.ErrUnsupported
	}
	return p.entities[*relation.To].TargetKind == "bead", nil
}

// Internal evidence keys frame all physical components. This never changes
// the source entity ID, raw record ID, tuple, origin bytes or target reference.
func SourcePlaneMirrorID(source dumpformat.Record, joint bool) string {
	if key, scoped := source.SourceKey(); scoped {
		encoded, _ := json.Marshal(key)
		return "source/" + source.Kind + "/" + HashBytes(encoded)
	}
	if !joint {
		return "source/" + source.Kind + "/" + source.ID
	}
	return driver.SourcePlaneMirrorIDV1(source.Kind, source.Project, source.Source.Table, source.ID)
}

// SourceMirrorBody keeps the exact source mirror. Only an explicitly bound
// source-plane import adds its live memory envelope; ordinary restore retains
// the legacy raw schema. Admitted legacy imports use their separate public
// string projection in buildPlan. Source-plane JSON values also remain raw.
func SourceMirrorBody(source dumpformat.Record, joint bool) (map[string]any, error) {
	body := CloneRow(source.Body)
	body["source_id"], body["source_table"], body["source"] = source.ID, source.Source.Table, CloneRow(source.Source.Fields)
	if key, scoped := source.SourceKey(); scoped {
		body["source_identity"] = map[string]any{"store_id": key.StoreID, "project": key.Project}
		body["source_key"] = map[string]any{"store_id": key.StoreID, "project": key.Project, "kind": key.Kind, "table": key.Table, "id": key.ID}
		body["source_project"] = key.Project
	}
	if joint {
		if source.Source.Identity == nil {
			body["source_project"] = source.Project
		}
	}
	if source.Kind == "beads-archive" {
		body["read_only"], body["redactable"] = true, true
	}
	if source.Kind == "memory" && joint {
		content, text := source.Body["value"].(string)
		encoding := "text"
		if !text {
			raw, err := json.Marshal(source.Body["value"])
			if err != nil {
				return nil, fmt.Errorf("source memory %s: %w", source.ID, err)
			}
			content, encoding = string(raw), "json"
		}
		body["id"], body["content"] = SourcePlaneMirrorID(source, joint), content
		body["metadata"] = map[string]any{"beads.memory.value_encoding": encoding, "beads.classification": source.Body["classification"]}
	}
	return body, nil
}
