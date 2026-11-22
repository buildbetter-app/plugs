package converterroundtrip

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/ontologymigration"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

func reverseSourceRestored(ctx context.Context, r driver.CheckpointInventory, meta driver.Meta, profile driver.RestoreImportProfile, options reverseconverter.Options) (result LiveReverse, reverseErr error) {
	bound, err := profile.Detached()
	if err != nil || bound.SourcePlanes == nil {
		return LiveReverse{}, ReverseRefusal{"invalid_import_binding"}
	}
	plan, err := bound.SourcePlanes.Plan()
	if err != nil || len(plan.Providers) != 1 {
		return LiveReverse{}, ReverseRefusal{"invalid_source_plan"}
	}
	request, err := bound.RequestHash()
	if err != nil {
		return LiveReverse{}, err
	}
	report, logical, err := recoverSourceImportReader(ctx, r, meta, bound, Verification{ArchiveSHA256: bound.ArchiveDigest, DerivedSHA256: bound.DerivedDigest}, plan, request)
	if err != nil {
		return LiveReverse{}, sourceInspectionFailure(err, ReverseRefusal{"source_receipt_archive_or_origin_mismatch"})
	}
	// Recovery transfers an owned archive/collection. Reverse consumes it
	// locally and returns only converted output, on both success and refusal.
	defer func() {
		if report.collection != nil {
			reverseErr = errors.Join(reverseErr, report.Close())
		} else if report.Archive != nil {
			report.Archive.ReleaseNativeDecoded()
		}
	}()
	// The returned reverse result does not retain inspector lookup maps or
	// preparation clones. Keep their reservations local through all exits.
	inspectCtx, releaseInspection := dumpformat.WithNativeDeltaAccountingScope(ctx)
	defer releaseInspection()
	current, err := workflowplugin.InspectSourceReverseImageContext(inspectCtx, logical)
	if err != nil {
		return LiveReverse{}, sourceInspectionFailure(err, ReverseRefusal{"invalid_current_source_image"})
	}
	preparationArchive, releasePreparation := report.Archive.NativeDeltaAccountingScope()
	defer releasePreparation()
	prepared, err := prepareSourceArchive(inspectCtx, preparationArchive, bound, true)
	if err != nil {
		return LiveReverse{}, sourceInspectionFailure(err, ReverseRefusal{"invalid_admitted_source_image"})
	}
	initial, err := workflowplugin.InspectSourceReverseImageContext(inspectCtx, prepared.LogicalImage())
	if err != nil {
		return LiveReverse{}, sourceInspectionFailure(err, ReverseRefusal{"changed_source_origin"})
	}
	if initial.OriginSHA256 != current.OriginSHA256 {
		return LiveReverse{}, ReverseRefusal{"changed_source_origin"}
	}
	dump, err := retainedReverseDump(ctx, r, bound)
	if err != nil {
		return LiveReverse{}, err
	}
	live, err := reverseLiveCensus(ctx, r)
	if err != nil {
		return LiveReverse{}, err
	}
	coreLosses, namespaceLosses, err := reverseLossCensus(r, live, &plan, options)
	if err != nil {
		return LiveReverse{}, err
	}
	if err := projectJointReverseDump(ctx, r, bound, plan, &dump, live, initial, current, options); err != nil {
		return LiveReverse{}, err
	}
	converted, err := reverseconverter.ConvertWithOptions(dump, options)
	attachReverseLossCensus(&converted, err, coreLosses, namespaceLosses)
	if err != nil {
		return LiveReverse{Snapshot: r.Snapshot(), Result: converted}, err
	}
	return LiveReverse{Snapshot: r.Snapshot(), Result: converted}, nil
}

func reverseSourceRecord(row reverseconverter.Record) dumpformat.Record {
	return dumpformat.Record{NativeEdgeTokens: row.NativeEdgeTokens, Kind: row.Kind, ID: row.ID, Project: row.Project, Body: dumpformat.Row(row.Body), Source: dumpformat.Provenance{Table: row.Source.Table, Fields: dumpformat.Row(row.Source.Fields), Identity: row.Source.Identity}}
}

// Immutable mirrors remain evidence, never a source of mutable active bodies.
// Unrepresentable relation/comment/claim changes refuse with no partial output.
func projectJointReverseDump(ctx context.Context, reader driver.Reader, bound driver.RestoreImportProfile, plan driver.SourcePlanePlanV1, dump *reverseconverter.Dump, live map[string]driver.Record, initial, current workflowplugin.SourceReverseImage, options reverseconverter.Options) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	projection, err := newSourceCoreProjection(&bound)
	if err != nil {
		return err
	}
	for _, id := range []string{"restore/archive", importBindingRecordID, driver.SourcePlaneCommitRecordID} {
		delete(live, id) // Already checked against the original receipt on this reader.
	}
	if row, ok := live[driver.OfflineHistoryEvidenceID]; ok {
		if row.Kind != "beads-archive" {
			return ReverseRefusal{"invalid_history_binding"}
		}
		delete(live, driver.OfflineHistoryEvidenceID)
	}
	for _, project := range dump.Manifest.Projects {
		delete(live, importProjectRecordID(project.Prefix))
	}
	// Native publication retains authenticated family/history and ontology
	// records for source readback. They are restore bookkeeping, not reverse
	// source rows.
	if row, ok := live[nativeFamilyReportID]; ok {
		if row.Kind != "instance_config" {
			return ReverseRefusal{"invalid_import_binding"}
		}
		delete(live, nativeFamilyReportID)
	}
	if row, ok := live[ImportedOntologySourceID]; ok {
		if row.Kind != "ontology" {
			return ReverseRefusal{"invalid_import_binding"}
		}
		delete(live, ImportedOntologySourceID)
	}
	for id, row := range live {
		if strings.HasPrefix(id, driver.NativeTableHistoryRecordPrefix) || strings.HasPrefix(id, "source/native-current/") {
			if row.Kind != "beads-archive" {
				return ReverseRefusal{"invalid_import_binding"}
			}
			delete(live, id)
		}
	}
	coreDump := *dump
	coreDump.Records = make(map[string][]reverseconverter.Record, len(dump.Records))
	for kind, rows := range dump.Records {
		coreDump.Records[kind] = rows
	}
	coreDump.Records["edge"] = nil
	for _, edge := range dump.Records["edge"] {
		core, err := projection.coreEdge(reverseSourceRecord(edge))
		if err != nil {
			return err
		}
		if core {
			coreDump.Records["edge"] = append(coreDump.Records["edge"], edge)
		}
	}
	// The pinned default loads outgoing relations only for selected source
	// rows. A retained source can still reference an omitted ephemeral target.
	physicalEntities := map[[2]string]driver.SourcePlaneEntityV1{}
	for _, entity := range plan.Entities {
		key := [2]string{entity.Source.Plane, entity.Source.ID}
		if _, duplicate := physicalEntities[key]; duplicate {
			return ReverseRefusal{"source-plane-contract: ambiguous raw ID across projects"}
		}
		physicalEntities[key] = entity
	}
	originFields := map[driver.SourceEntityKey]map[string]any{}
	for _, row := range dump.Records["bead"] {
		originFields[driver.SourceEntityKey{Project: row.Project, Plane: row.Source.Table, ID: row.ID}] = row.Source.Fields
	}
	for _, row := range dump.Records["beads-archive"] {
		if row.Source.Table == dumpformat.EphemeralEntityTable {
			entity, exists := physicalEntities[[2]string{row.Source.Table, stringValue(row.Source.Fields["id"])}]
			if !exists {
				return ReverseRefusal{"missing_source_entity_origin"}
			}
			originFields[entity.Source] = row.Source.Fields
		}
	}
	included := map[string]bool{}
	for _, entity := range plan.Entities {
		fields := originFields[entity.Source]
		if fields == nil {
			return ReverseRefusal{"missing_source_entity_origin"}
		}
		included[entity.TargetID] = options.All || !reverseEphemeral(fields)
	}
	terminalTargets := map[string]bool{}
	for _, entry := range current.Entities {
		if entry.Terminal && !entry.Present && entry.Item == nil {
			terminalTargets[entry.TargetID] = true
		}
	}
	selectedRelations := func(rows []workflowplugin.RelationView) []workflowplugin.RelationView {
		var result []workflowplugin.RelationView
		for _, row := range rows {
			if included[row.From] && !terminalTargets[row.From] && !terminalTargets[row.To.ID] {
				result = append(result, row)
			}
		}
		return result
	}
	currentEdges, err := projectMixedSourceLinks(ctx, dump.Records["edge"], projection, plan, selectedRelations(initial.Relations), selectedRelations(current.Relations), included, terminalTargets)
	if err != nil {
		return err
	}
	dispositions := map[string]driver.SourcePlaneDispositionV1{}
	for _, disposition := range plan.Dispositions {
		dispositions[disposition.Key] = disposition
	}
	for kind, rows := range dump.Records {
		for i, source := range rows {
			if kind == "bead" {
				entity := projection.entities[driver.SourceEntityKey{Project: source.Project, Plane: source.Source.Table, ID: source.ID}]
				if entity.TargetKind != "bead" {
					return ReverseRefusal{"unsupported_durable_source_sink"}
				}
				row, exists := live[entity.TargetID]
				if !exists || row.Kind != "bead" {
					return ReverseRefusal{"missing_source_record"}
				}
				delete(live, entity.TargetID)
				if !included[entity.TargetID] {
					continue
				}
				projected, err := projectLiveBead(source, row, coreDump)
				if err != nil {
					return err
				}
				for _, field := range []string{"description", "priority", "metadata"} {
					if !projection.mapsField(reverseSourceRecord(source), field) {
						projected.Body[field], projected.Source.Fields[field] = source.Body[field], source.Source.Fields[field]
					}
				}
				if err := projectNativeStatusForReverse(&projected, *dump); err != nil {
					return err
				}
				if err := projectNativeTypeForReverse(&projected, *dump); err != nil {
					return err
				}
				dump.Records[kind][i] = projected
				continue
			}
			mirrorID := sourcePlaneMirrorID(reverseSourceRecord(source), true)
			row, exists := live[mirrorID]
			if !exists || row.Kind != kind {
				return ReverseRefusal{"missing_source_mirror"}
			}
			delete(live, mirrorID)
			want, err := sourceMirrorBody(reverseSourceRecord(source), true)
			if err != nil {
				return ReverseRefusal{"changed_source_mirror"}
			}
			var got map[string]any
			if decodeReverseJSON(row.Body, &got) != nil || !sameReverseJSON(want, got) {
				return ReverseRefusal{"changed_source_mirror"}
			}
		}
	}
	if err := projectEffectiveMemories(ctx, reader, dump, live, true); err != nil {
		return err
	}
	if len(live) != 0 {
		return ReverseRefusal{"unmapped_live_record"}
	}
	dump.Records["edge"] = currentEdges
	for i, source := range dump.Records["beads-archive"] {
		if source.Source.Table != dumpformat.EphemeralEntityTable {
			continue
		}
		key := physicalEntities[[2]string{source.Source.Table, stringValue(source.Source.Fields["id"])}].Source
		entry, exists := current.Entities[key]
		before := initial.Entities[key]
		if !exists || before.Item == nil {
			return ReverseRefusal{"missing_source_entity_binding"}
		}
		if !entry.Present || entry.Item == nil {
			if entry.Terminal && !included[entry.TargetID] {
				// Only the immutable address/flag is needed for the explicit
				// omission report; no retained body enters the JSONL output.
				source.Project = key.Project
				dump.Records["beads-archive"][i] = source
				continue
			}
			return ReverseRefusal{"terminal_source_body_unavailable"}
		}
		entity := projection.entities[key]
		projected, err := projectActiveReverse(source, *before.Item, *entry.Item, dispositions[entity.DispositionKey], !included[entity.TargetID])
		if err != nil {
			return err
		}
		projected.Project = key.Project
		dump.Records["beads-archive"][i] = projected
	}
	// Labels are a current typed set. Replace the physical archived label rows
	// only after their original protected mirrors were checked above.
	var archived []reverseconverter.Record
	for _, row := range dump.Records["beads-archive"] {
		if row.Source.Table != dumpformat.EphemeralLabelTable {
			archived = append(archived, row)
		}
	}
	keys := make([]driver.SourceEntityKey, 0, len(current.Entities))
	for key := range current.Entities {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return driver.CompareSourceEntityKeys(keys[i], keys[j]) < 0 })
	for _, key := range keys {
		entry := current.Entities[key]
		if key.Plane != dumpformat.EphemeralEntityTable || !entry.Present || entry.Item == nil {
			continue
		}
		for _, label := range entry.Item.Labels {
			fields := map[string]any{"issue_id": key.ID, "label": label}
			identity, _ := json.Marshal([]string{key.Project, key.ID, label})
			archived = append(archived, reverseconverter.Record{Kind: "beads-archive", ID: "current-label/" + hashBytes(identity), Project: key.Project, Body: map[string]any{"read_only": true}, Source: reverseconverter.Provenance{Table: dumpformat.EphemeralLabelTable, Fields: fields}})
		}
	}
	dump.Records["beads-archive"] = archived
	for i := range dump.Manifest.Kinds {
		dump.Manifest.Kinds[i].Count = len(dump.Records[dump.Manifest.Kinds[i].Name])
	}
	return nil
}

// Native status catalogs retain source names and categories that the generic
// JSONL reverse converter cannot emit as live Beads workflow values. Preserve
// those exact native values in metadata, then project only the converter-facing
// status/category pair for this authenticated source-plane route.
func projectNativeStatusForReverse(projected *reverseconverter.Record, dump reverseconverter.Dump) error {
	if dump.Manifest.SourcePlaneAddressContract != dumpformat.SourcePlaneAddressContract || dump.Manifest.StatusCategoryContract != dumpformat.StatusCategoryContract {
		return nil
	}
	nativeStatus := stringValue(projected.Source.Fields["status"])
	nativeCategory := stringValue(projected.Body["status_category"])
	status := nativeStatus
	switch nativeCategory {
	case "active":
		projected.Body["status_category"] = "unstarted"
		if !nativeStatusCompatible(nativeCategory, status) {
			status = "open"
		}
	case "wip":
		projected.Body["status_category"] = "started"
		if !nativeStatusCompatible(nativeCategory, status) {
			status = "in_progress"
		}
	case "frozen":
		projected.Body["status_category"] = "backlog"
		if !nativeStatusCompatible(nativeCategory, status) {
			status = "blocked"
		}
	case "done":
		projected.Body["status_category"], projected.Body["result"] = "completed", "success"
		if !nativeStatusCompatible(nativeCategory, status) {
			status = "closed"
		}
	case "unstarted", "started", "backlog", "completed":
		return nil
	default:
		return ReverseRefusal{"unsupported_native_status_category"}
	}
	if nativeStatusCompatible(nativeCategory, nativeStatus) {
		projected.Source.Fields["status"] = status
		return nil
	}
	if err := mergeNativeReverseMetadata(projected, map[string]any{
		"_plug_native_status": nativeStatus, "_plug_native_status_category": nativeCategory,
	}); err != nil {
		return err
	}
	projected.Source.Fields["status"] = status
	return nil
}

// These names are admitted user metadata, so native bookkeeping may only fill
// absent keys. A present null conflicts just like any other unequal value.
func mergeNativeReverseMetadata(projected *reverseconverter.Record, bookkeeping map[string]any) error {
	metadata, err := dumpformat.SourceMetadataObject(projected.Body["metadata"])
	if err != nil {
		return ReverseRefusal{"invalid_source_metadata"}
	}
	for key, value := range bookkeeping {
		if existing, present := metadata[key]; present && !sameReverseJSON(existing, value) {
			return ReverseRefusal{"conflicting_native_metadata"}
		}
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	for key, value := range bookkeeping {
		if _, present := metadata[key]; !present {
			metadata[key] = value
		}
	}
	projected.Body["metadata"] = metadata
	return nil
}

func nativeStatusCompatible(category, status string) bool {
	switch category {
	case "active":
		return status == "open"
	case "wip":
		return status == "in_progress" || status == "hook"+"ed"
	case "frozen":
		return status == "blocked" || status == "deferred" || status == "pinned" || status == "frozen"
	case "done":
		return status == "closed"
	default:
		return false
	}
}

func projectNativeTypeForReverse(projected *reverseconverter.Record, dump reverseconverter.Dump) error {
	if dump.Manifest.SourcePlaneAddressContract != dumpformat.SourcePlaneAddressContract {
		return nil
	}
	nativeType := stringValue(projected.Source.Fields["issue_type"])
	switch nativeType {
	case "task", "bug", "feature", "epic", "chore":
		return nil
	}
	for _, directive := range dump.Manifest.Ontology {
		if directive.Target.Project == projected.Project && directive.Target.Type == nativeType && directive.Kind == ontologymigration.MapType {
			return nil
		}
	}
	if err := mergeNativeReverseMetadata(projected, map[string]any{"_plug_native_type": nativeType}); err != nil {
		return err
	}
	projected.Body["type"] = "issue"
	projected.Body["source_type"] = "task"
	projected.Source.Fields["issue_type"] = "task"
	return nil
}

func projectActiveReverse(source reverseconverter.Record, initial, item workflowplugin.ItemView, disposition driver.SourcePlaneDispositionV1, omitted bool) (reverseconverter.Record, error) {
	if omitted {
		return source, nil
	}
	if item.Claim != nil || item.Blocked != initial.Blocked || item.ChildCounter != initial.ChildCounter {
		return reverseconverter.Record{}, ReverseRefusal{"active_claim_or_unrepresentable_state"}
	}
	if !sameReverseJSON(initial.Comments, item.Comments) {
		return reverseconverter.Record{}, ReverseRefusal{"changed_source_comments"}
	}
	source.Source.Fields = cloneRow(source.Source.Fields)
	fields := source.Source.Fields
	for _, rule := range disposition.Fields {
		if rule.Disposition != "exact-map" {
			continue
		}
		switch rule.Target {
		case "id", "created_at": // Immutable identity/time already bound by origin.
		case "title":
			fields[rule.Source] = item.Title
		case "type":
			fields[rule.Source] = item.Type
		case "status":
			if item.Status != "open" && item.Status != "closed" {
				return reverseconverter.Record{}, ReverseRefusal{"unmapped_active_status"}
			}
			fields[rule.Source] = item.Status
		case "metadata":
			original, err := dumpformat.SourceMetadataObject(fields[rule.Source])
			if err != nil {
				return reverseconverter.Record{}, err
			}
			if !sameReverseJSON(original, item.Metadata) && !(len(original) == 0 && len(item.Metadata) == 0) {
				fields[rule.Source] = cloneRow(item.Metadata)
			}
		default:
			if !strings.HasPrefix(rule.Target, "fields.") {
				return reverseconverter.Record{}, ReverseRefusal{"unsupported_active_field_mapping"}
			}
			name := strings.TrimPrefix(rule.Target, "fields.")
			value, exists := item.Fields[name]
			if !exists {
				return reverseconverter.Record{}, ReverseRefusal{"missing_mapped_active_field"}
			}
			fields[rule.Source] = value
		}
	}
	// A newly populated fields slot needs an explicit physical source mapping.
	for field := range item.Fields {
		mapped := false
		for _, rule := range disposition.Fields {
			mapped = mapped || rule.Disposition == "exact-map" && rule.Target == "fields."+field
		}
		if !mapped {
			return reverseconverter.Record{}, ReverseRefusal{"unmapped_active_field"}
		}
	}
	return source, nil
}

// Admission has already checked the canonical source flag representation.
func reverseEphemeral(fields map[string]any) bool {
	switch value := fields["ephemeral"].(type) {
	case bool:
		return value
	case json.Number:
		return value == "1"
	case string:
		return value == "1" || value == "true"
	}
	return false
}
