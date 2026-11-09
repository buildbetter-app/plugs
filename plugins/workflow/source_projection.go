package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func sourceItem(entity driver.SourcePlaneEntityV1, disposition driver.SourcePlaneDispositionV1, fields map[string]json.RawMessage) (itemState, time.Time, error) {
	var empty itemState
	typ, err := sourceText(fields["issue_type"])
	if err != nil {
		return empty, time.Time{}, err
	}
	storageRaw, storagePresent := fields["storage_class"]
	var storage any
	if !storagePresent {
		return empty, time.Time{}, sourceRefusal("source storage_class column absent")
	}
	if err := decodeWorkflowJSON(storageRaw, &storage); err != nil {
		return empty, time.Time{}, err
	}
	ephemeral, err := sourceBool(fields["ephemeral"])
	if err != nil {
		return empty, time.Time{}, err
	}
	noHistory, err := sourceBool(fields["no_history"])
	if err != nil {
		return empty, time.Time{}, err
	}
	if disposition.SourceType != typ || !disposition.MatchesStorageClass(storage, storagePresent) || disposition.Ephemeral != ephemeral || disposition.NoHistory != noHistory || disposition.SinkKind != "workflow:item" || disposition.Schema != SourceStateSchema || disposition.RefusalPolicy != "exact-only" {
		return empty, time.Time{}, sourceRefusal("source type/storage/flags have no exact disposition")
	}
	if disposition.Lifetime != "process" && disposition.Lifetime != "restart" {
		return empty, time.Time{}, sourceRefusal("unsupported active lifetime")
	}
	rules := map[string]driver.SourcePlaneFieldRuleV1{}
	for _, rule := range disposition.Fields {
		rules[rule.Source] = rule
	}
	item := itemState{ID: entity.TargetID, Label: entity.Source.ID, Project: entity.Source.Project, Generation: 1, Fields: map[string]any{}}
	var created time.Time
	mapped := map[string]bool{}
	for name, raw := range fields {
		rule, declared := rules[name]
		if !declared {
			return empty, time.Time{}, sourceRefusal("missing source field disposition: " + name)
		}
		switch rule.Disposition {
		case "origin-only":
			if rule.Target != "" {
				return empty, time.Time{}, sourceRefusal("origin-only field has a live target")
			}
			continue
		case "exact-map":
		case "refuse-required":
			return empty, time.Time{}, sourceRefusal("required source field is unsupported: " + name)
		default:
			return empty, time.Time{}, sourceRefusal("unknown field disposition")
		}
		if mapped[rule.Target] {
			return empty, time.Time{}, sourceRefusal("multiple fields overwrite one active slot")
		}
		mapped[rule.Target] = true
		switch rule.Target {
		case "id":
			if name != "id" {
				return empty, time.Time{}, sourceRefusal("unsupported identity mapping")
			}
		case "title":
			item.Title, err = sourceText(raw)
		case "type":
			if name != "issue_type" {
				return empty, time.Time{}, sourceRefusal("unsupported type mapping")
			}
			item.Type, err = sourceText(raw)
		case "status":
			if name != "status" {
				return empty, time.Time{}, sourceRefusal("unsupported status mapping")
			}
			status, statusErr := sourceText(raw)
			if statusErr != nil {
				return empty, time.Time{}, statusErr
			}
			for _, correspondence := range disposition.Statuses {
				if correspondence.From == status && correspondence.To == status && (status == "open" || status == "closed") {
					item.Status = status
				}
			}
			if item.Status == "" {
				return empty, time.Time{}, sourceRefusal("source status has no supported exact correspondence")
			}
		case "metadata":
			data := bytes.TrimSpace(raw)
			if len(data) != 0 && data[0] == '"' {
				text, textErr := sourceText(raw)
				if textErr != nil {
					return empty, time.Time{}, textErr
				}
				data = []byte(text)
			}
			if string(data) != "null" {
				err = decodeWorkflowJSON(data, &item.Metadata)
				if item.Metadata == nil && err == nil {
					err = sourceRefusal("mapped metadata is not an object")
				}
			}
		case "created_at":
			if name != "created_at" {
				return empty, time.Time{}, sourceRefusal("unsupported creation mapping")
			}
			stamp, stampErr := sourceText(raw)
			if stampErr != nil {
				return empty, time.Time{}, stampErr
			}
			created, err = time.Parse(time.RFC3339Nano, stamp)
			created = created.UTC()
		default:
			if !strings.HasPrefix(rule.Target, "fields.") || strings.TrimPrefix(rule.Target, "fields.") != name {
				return empty, time.Time{}, sourceRefusal("unsupported active field slot: " + rule.Target)
			}
			var value any
			err = decodeWorkflowJSON(raw, &value)
			item.Fields[name] = value
		}
		if err != nil {
			return empty, time.Time{}, err
		}
	}
	for name := range rules {
		if _, present := fields[name]; !present {
			return empty, time.Time{}, sourceRefusal("disposition field absent from physical row")
		}
	}
	if !mapped["id"] || strings.TrimSpace(item.Title) == "" || item.Type == "" || item.Status == "" || created.IsZero() {
		return empty, time.Time{}, sourceRefusal("incomplete active projection")
	}
	return item, created, nil
}

func prepareSourceRelations(ctx context.Context, value *persisted, plan driver.SourcePlanePlanV1, byHash map[string]sourceWireRecord, _ []driver.SourcePlaneRecordV1, entities map[driver.SourceEntityKey]driver.SourcePlaneEntityV1, used map[string]bool) error {
	for _, relation := range plan.Relations {
		if err := ctx.Err(); err != nil {
			return err
		}
		wire, err := sourceMatchedRecord(byHash, relation.Origin)
		if err != nil {
			return err
		}
		if wire.Kind != "edge" || wire.ID != relation.Source.ID || wire.Project != relation.Source.Project || wire.Source.Table != relation.Source.Table {
			return sourceRefusal("physical edge address differs")
		}
		if err := sourceFieldCensus(wire.Source.Fields, relation.Fields); err != nil {
			return err
		}
		fields := wire.Source.Fields
		if len(fields) != 10 {
			return sourceRefusal("incomplete physical dependency row")
		}
		for _, name := range []string{"id", "issue_id", "depends_on_issue_id", dumpformat.EphemeralDependencyTargetField, "depends_on_external", "type", "created_at", "created_by", "metadata", "thread_id"} {
			if _, ok := fields[name]; !ok {
				return sourceRefusal("missing dependency column")
			}
		}
		from, err := sourceText(fields["issue_id"])
		if err != nil || from != relation.From.ID {
			return sourceRefusal("physical edge source differs")
		}
		id, err := sourceText(fields["id"])
		if err != nil || id != relation.Source.ID {
			return sourceRefusal("physical edge identity differs")
		}
		kind, err := sourceText(fields["type"])
		if err != nil || kind != relation.Kind {
			return sourceRefusal("physical edge kind differs")
		}
		arm, target := "", ""
		for _, name := range []string{"depends_on_issue_id", dumpformat.EphemeralDependencyTargetField, "depends_on_external"} {
			if string(fields[name]) == "null" {
				continue
			}
			text, err := sourceText(fields[name])
			if err != nil {
				return err
			}
			if text == "" {
				continue
			}
			if arm != "" {
				return sourceRefusal("multiple physical edge arms")
			}
			arm, target = name, text
		}
		if relation.External != nil {
			if arm != "depends_on_external" || target != *relation.External {
				return sourceRefusal("external arm differs")
			}
		} else {
			plane := dumpformat.DurableEntityTable
			if arm == dumpformat.EphemeralDependencyTargetField {
				plane = dumpformat.EphemeralEntityTable
			} else if arm != "depends_on_issue_id" {
				return sourceRefusal("local arm differs")
			}
			if relation.To == nil || relation.To.ID != target || relation.To.Plane != plane {
				return sourceRefusal("typed target plane differs")
			}
		}
		if relation.PredicateContract != "beads-typed-metadata/1" {
			return sourceRefusal("unknown predicate contract")
		}
		var metadata any
		if err := decodeWorkflowJSON(fields["metadata"], &metadata); err != nil {
			return err
		}
		predicate, err := dumpformat.SourceEdgeMetadata(3, id, kind, metadata)
		if err != nil {
			return err
		}
		canonical, err := json.Marshal(predicate)
		if err != nil {
			return err
		}
		if string(canonical) != string(relation.PredicateJSON) {
			return sourceRefusal("detached predicate differs from physical metadata")
		}
		used[relation.Origin.SHA256] = true
		// External endpoints retain complete inert origins. The canonical
		// workflow relation model contains only local branch-zero references.
		if relation.External != nil {
			continue
		}
		fromEntity, toEntity := entities[relation.From], entities[*relation.To]
		if fromEntity.TargetKind != "workflow:item" && toEntity.TargetKind != "workflow:item" {
			continue
		}
		if fromEntity.TargetID == toEntity.TargetID {
			return sourceRefusal("self relation")
		}
		if kind == "related" {
			kind = "relates-to"
		}
		link := relationState{From: fromEntity.TargetID, To: model.RefByID(toEntity.TargetID, model.Natural64(0)), Kind: kind, Metadata: predicate}
		if fromEntity.TargetKind == "workflow:item" {
			item := value.Items[fromEntity.TargetID]
			if kind == "parent-child" {
				if item.Parent != nil && !sameRef(item.Parent, &link.To) {
					return sourceRefusal("conflicting imported typed parents")
				}
				item.Parent = cloneRef(&link.To)
			}
			item.Links = dedupeRelations(append(item.Links, link))
			value.Items[item.ID] = item
		}
		if toEntity.TargetKind == "workflow:item" {
			item := value.Items[toEntity.TargetID]
			if fromEntity.TargetKind == "workflow:item" && item.Owner != value.Items[fromEntity.TargetID].Owner {
				return sourceRefusal("relation crosses declared owner scope")
			}
			item.Inbound = dedupeRelations(append(item.Inbound, link))
			value.Items[item.ID] = item
		}
	}
	return nil
}

func prepareSourceOrigins(ctx context.Context, value *persisted, plan driver.SourcePlanePlanV1, byHash map[string]sourceWireRecord, _ []driver.SourcePlaneRecordV1, entities map[driver.SourceEntityKey]driver.SourcePlaneEntityV1, used map[string]bool) error {
	counters := map[string]bool{}
	for _, origin := range plan.Origins {
		if err := ctx.Err(); err != nil {
			return err
		}
		if origin.Path != "" {
			continue
		} // Sidecar bytes are verified by the archive owner.
		wire, err := sourceMatchedRecord(byHash, origin.Raw)
		if err != nil {
			return err
		}
		if wire.Source.Table != origin.Table || wire.ID != origin.ID || !wire.matchesProject(origin.Project) {
			return sourceRefusal("origin record address differs")
		}
		if err := sourceFieldCensus(wire.Source.Fields, origin.Fields); err != nil {
			return err
		}
		used[origin.Raw.SHA256] = true
		if origin.Action == "inert-origin" || origin.Action == "origin-only" {
			continue
		}
		if origin.Action != "exact-map" || origin.Owner == nil {
			return sourceRefusal("unsupported origin disposition")
		}
		entity := entities[*origin.Owner]
		ownerField := "issue_id"
		if origin.Table == "child_counters" || origin.Table == dumpformat.EphemeralChildCounterTable {
			ownerField = "parent_id"
		}
		owner, err := sourceText(wire.Source.Fields[ownerField])
		if err != nil || owner != origin.Owner.ID {
			return sourceRefusal("auxiliary physical owner differs")
		}
		plane := dumpformat.DurableEntityTable
		if origin.Table == dumpformat.EphemeralLabelTable || origin.Table == dumpformat.EphemeralCommentTable || origin.Table == dumpformat.EphemeralChildCounterTable {
			plane = dumpformat.EphemeralEntityTable
		}
		if origin.Owner.Plane != plane || origin.Owner.Project != origin.Project {
			return sourceRefusal("auxiliary owner plane/project differs")
		}
		if entity.TargetKind != "workflow:item" {
			continue
		}
		item := value.Items[entity.TargetID]
		switch origin.Table {
		case "labels", dumpformat.EphemeralLabelTable:
			label, err := sourceText(wire.Source.Fields["label"])
			if err != nil {
				return err
			}
			if err := validateLabels([]string{label}); err != nil {
				return err
			}
			item.Labels = append(item.Labels, label)
		case "child_counters", dumpformat.EphemeralChildCounterTable:
			if counters[item.ID] {
				return sourceRefusal("duplicate source counter")
			}
			counters[item.ID] = true
			var counter model.Natural
			if err := json.Unmarshal(wire.Source.Fields["last_child"], &counter); err != nil {
				return err
			}
			n, err := counter.Uint64()
			if err != nil {
				return err
			}
			item.ChildCounter = n
		case "comments", dumpformat.EphemeralCommentTable:
			id, err := sourceText(wire.Source.Fields["id"])
			if err != nil {
				return err
			}
			body, err := sourceText(wire.Source.Fields["text"])
			if err != nil {
				return err
			}
			author, err := sourceText(wire.Source.Fields["author"])
			if err != nil {
				return err
			}
			stamp, err := sourceText(wire.Source.Fields["created_at"])
			if err != nil {
				return err
			}
			when, err := time.Parse(time.RFC3339Nano, stamp)
			if err != nil {
				return err
			}
			item.Comments = append(item.Comments, commentState{Body: body, Time: when.UTC(), Source: &SourceCommentOrigin{Project: origin.Project, Table: origin.Table, ID: id, Author: author}})
		default:
			return sourceRefusal("unsupported live auxiliary table")
		}
		value.Items[item.ID] = item
	}
	for id, item := range value.Items {
		sort.Strings(item.Labels)
		for i := 1; i < len(item.Labels); i++ {
			if item.Labels[i] == item.Labels[i-1] {
				return sourceRefusal("duplicate physical label")
			}
		}
		sort.Slice(item.Comments, func(i, j int) bool {
			a, b := item.Comments[i], item.Comments[j]
			if !a.Time.Equal(b.Time) {
				return a.Time.Before(b.Time)
			}
			return a.Source.ID < b.Source.ID
		})
		value.Items[id] = item
	}
	return nil
}

// ResolveSourceRef implements the declared active-first correspondence. It
// consults the current image; after an active item ends, a declared durable
// collision may become the source-facing address without losing either origin.
func (p *Provider) ResolveSourceRef(ctx context.Context, by model.PrincipalRef, project, sourceID string, meta model.WriteMeta) (model.Ref, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.source == nil {
		return model.Ref{}, false, driver.ErrUnsupported
	}
	value, _, err := p.readState(ctx)
	if err != nil {
		return model.Ref{}, false, err
	}
	if value.Source == nil || value.Source.Schema != SourceStateSchema {
		return model.Ref{}, false, sourceRefusal("missing restored source image")
	}
	for _, entity := range value.Source.Entities {
		if entity.Source.Project != project || entity.Source.ID != sourceID {
			continue
		}
		route := entity.Route
		if item, ok := value.Items[route.ActiveTargetID]; ok {
			if err := p.authorized(item, by, meta); err != nil {
				return model.Ref{}, false, err
			}
			return model.RefByID(item.ID, model.Natural64(0)), true, nil
		}
		if route.DurableTargetID != "" {
			return model.RefByID(route.DurableTargetID, model.Natural64(0)), true, nil
		}
		return model.Ref{}, false, nil
	}
	return model.Ref{}, false, nil
}

// SourceRecords exposes detached physical origins for the trusted reverse
// adapter. Mutable current items are read separately at the same host snapshot.
func (p *Provider) SourceRecords(ctx context.Context) ([]driver.SourcePlaneRecordV1, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.source == nil {
		return nil, driver.ErrUnsupported
	}
	value, _, err := p.readState(ctx)
	if err != nil {
		return nil, err
	}
	if value.Source == nil {
		return nil, sourceRefusal("missing source origins")
	}
	data, err := json.Marshal(value.Source.Records)
	if err != nil {
		return nil, err
	}
	var records []driver.SourcePlaneRecordV1
	if err := decodeWorkflowJSON(data, &records); err != nil {
		return nil, fmt.Errorf("source origins: %w", err)
	}
	return records, nil
}
