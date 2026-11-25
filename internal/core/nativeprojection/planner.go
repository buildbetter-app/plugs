package nativeprojection

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/recordimage"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

var ErrRestoreUnavailable = errors.New("trusted offline restore seam unavailable")

const ImportBindingRecordID = "instance_config/restore-import/binding"

type restoreRecord struct {
	ref  model.Ref
	kind string
	body []byte
}

type restorePlan struct {
	migration          *dumpformat.ImportMigration
	nativeOntology     *dumpformat.NativeStatusOntology
	nativeFamilies     []NativeFamilyReport
	nativeSchemas      []NativeSchemaReport
	nativeHistories    []NativeHistoryReport
	nativeDeltaHistory *driver.NativeDeltaHistoryEvidence
	records            []restoreRecord
	sentinels          []driver.Sentinel
	derived            []DerivedImage
	derivedDigest      string
	blobs              map[string][]byte
	collectionConfig   dumpformat.File
	nativeCapture      *dumpformat.Archive
	allocators         []driver.OfflineAllocator
}

// ImportBindingEvidence is an immutable, transaction-local copy of the full
// admitted import binding. Source allocator declarations are retained as
// dumpformat projects, because their widths are provenance (and may be below
// the live allocator's continuation minimum). Target allocators are the exact
// seeded Beads profiles from which recovery must observe continuation.
type ImportBindingEvidence struct {
	Profile     driver.RestoreImportProfile `json:"profile"`
	RequestHash string                      `json:"request_hash"`
	Projects    []ImportProjectBinding      `json:"projects"`
}

type ImportProjectBinding struct {
	SourceIdentity  *dumpformat.SourceIdentity `json:"sourceIdentity,omitempty"`
	SourceAllocator dumpformat.Project         `json:"sourceAllocator"`
	TargetAllocator driver.OfflineAllocator    `json:"targetAllocator"`
}

type ImportProjectMetadata struct {
	SourceIdentity  *dumpformat.SourceIdentity  `json:"sourceIdentity,omitempty"`
	Project         string                      `json:"project"`
	ImportProfile   driver.RestoreImportProfile `json:"importProfile"`
	Metadata        map[string]json.RawMessage  `json:"metadata"`
	TargetAllocator driver.OfflineAllocator     `json:"targetAllocator"`
}

func ImportProjectRecordID(prefix string) string {
	return "instance_config/restore-import/project/" + prefix
}

type planMode uint8

const (
	beadsAgentStateMetadataKey = "beads.agent_state"
	beadsAwaitIDMetadataKey    = "beads.await_id"
)

const (
	planLegacy planMode = iota
	planImport
	planInspection
)

func buildPlan(archive *dumpformat.Archive) (restorePlan, error) {
	return buildPlanMode(archive, nil, time.Time{}, planLegacy)
}

func buildPlanWithProfile(archive *dumpformat.Archive, profile *driver.RestoreImportProfile, restoreTime time.Time, contexts ...context.Context) (restorePlan, error) {
	mode := planLegacy
	if profile != nil {
		mode = planImport
	}
	return buildPlanMode(archive, profile, restoreTime, mode, NativeOperationContext(contexts))
}

func buildPlanForInspection(archive *dumpformat.Archive) (restorePlan, error) {
	return buildPlanMode(archive, nil, time.Time{}, planInspection)
}

func buildPlanMode(archive *dumpformat.Archive, profile *driver.RestoreImportProfile, restoreTime time.Time, mode planMode, contexts ...context.Context) (restorePlan, error) {
	return buildPlanModeWithCurrent(archive, profile, restoreTime, mode, nil, contexts...)
}

func buildPlanModeWithCurrent(archive *dumpformat.Archive, profile *driver.RestoreImportProfile, restoreTime time.Time, mode planMode, collection *collectionCurrent, contexts ...context.Context) (restorePlan, error) {
	scratch, releaseScratch := archive.NativeDeltaAccountingScope()
	defer releaseScratch()
	manifest, err := scratch.ManifestChecked()
	if err != nil {
		return restorePlan{}, err
	}
	nativeTables := archive.HasNativeSourceHistories()
	migration, _ := archive.ImportMigration()
	if mode == planLegacy && migration != nil {
		// A missing import profile cannot opt a newly strict migration into
		// snapshot semantics. Inspection remains available without admission.
		return restorePlan{}, driver.ErrRestoreHistoryRefused{Mode: driver.RestoreHistoryModeFull, History: manifest.History, Reason: driver.RestoreHistoryReasonUnavailable}
	}
	if mode == planLegacy && manifest.History != "unavailable" {
		return restorePlan{}, fmt.Errorf("%w: unverified source history cannot be restored", ErrRestoreUnavailable)
	}
	comments, err := scratch.RecordsChecked("comment")
	if err != nil {
		return restorePlan{}, err
	}
	if mode == planLegacy && len(comments) != 0 {
		return restorePlan{}, fmt.Errorf("%w: unverified comment attribution", ErrRestoreUnavailable)
	}
	projects := map[string]dumpformat.Project{}
	for _, project := range manifest.Projects {
		// Imported archives preserve the source allocator's declared hash
		// width (the canonical contract admits 1..64). The live target
		// allocator is seeded separately below, so this lower bound must not
		// inherit the legacy restore profile's hash32 compatibility guard.
		if project.IDMode != "hash" || project.Prefix == "" || project.HashLength < 1 {
			return restorePlan{}, fmt.Errorf("%w: allocator continuation is unsupported for %s", ErrRestoreUnavailable, project.Prefix)
		}
		if mode == planLegacy && (project.Prefix != "pl" || project.HashLength != 32 || project.LastID != 0 || len(project.ChildCounters) != 0) {
			return restorePlan{}, fmt.Errorf("%w: allocator continuation is unsupported for %s", ErrRestoreUnavailable, project.Prefix)
		}
		projects[project.Prefix] = project
	}
	if profile != nil && restoreTime.IsZero() {
		return restorePlan{}, fmt.Errorf("%w: import restore time is required", ErrRestoreUnavailable)
	}

	projection, err := NewSourceCoreProjection(profile, scratch)
	if err != nil {
		return restorePlan{}, err
	}
	entities := projection.entities
	beads, err := scratch.RecordsChecked("bead")
	if err != nil {
		return restorePlan{}, err
	}
	if entities != nil {
		var durable []dumpformat.Record
		for _, source := range beads {
			entity, present := entities[driver.SourceEntityKey{Project: source.Project, Plane: source.Source.Table, ID: source.ID}]
			if !present {
				return restorePlan{}, fmt.Errorf("%w: historical source entity lacks an explicit disposition: %s/%s/%s", ErrRestoreUnavailable, source.Project, source.Source.Table, source.ID)
			}
			if entity.TargetKind == "bead" {
				durable = append(durable, source)
			}
		}
		beads = durable
	}
	docs := map[string]recordimage.Document{}
	if len(beads) > 20000 {
		return restorePlan{}, fmt.Errorf("restore graph exceeds local service limit")
	}
	for _, source := range beads {
		project, knownProject := projects[source.Project]
		if !knownProject || !ValidSourceID(source.ID, project, mode != planLegacy) {
			return restorePlan{}, fmt.Errorf("%w: source ID does not match the declared allocator", ErrRestoreUnavailable)
		}
		status, ok := TargetStatus(StringValue(source.Source.Fields["status"]))
		if manifest.StatusCategoryContract == dumpformat.StatusCategoryContract {
			status = StringValue(source.Source.Fields["status"])
			ok = false
			for _, declared := range project.Statuses {
				ok = ok || declared.Name == status
			}
		}
		targetType := StringValue(source.Body["type"])
		if migration != nil {
			target, present := migration.Target(source.Project, source.ID)
			status, targetType, ok = target.Status, target.Type, present
		}
		if !ok {
			return restorePlan{}, fmt.Errorf("unsupported source status for %s", source.ID)
		}
		// The admitted issue schema (including strict-union type aliases) has
		// title/description plus the kernel priority field. Preserve every other
		// source field in metadata so restored beads remain writable by the ordinary pipeline instead of
		// presenting unsupported source columns as typed Plug fields.
		fields := map[string]any{"status": status}
		metadata := map[string]json.RawMessage{}
		for _, key := range []string{"title", "description", "priority"} {
			if value := source.Body[key]; value != nil && projection.MapsField(source, key) {
				fields[key] = value
			}
		}
		var rawMetadata map[string]any
		if projection.MapsField(source, "metadata") {
			if err := scratch.ChargeNativeDeltaJSON("portable_metadata_object", source.Body["metadata"]); err != nil {
				return restorePlan{}, err
			}
			rawMetadata, err = dumpformat.SourceMetadataObject(source.Body["metadata"])
			if err != nil {
				return restorePlan{}, fmt.Errorf("bead %s metadata: %w", source.ID, err)
			}
		}
		if raw := rawMetadata; raw != nil {
			for key, value := range raw {
				if strings.HasPrefix(key, "beads.") || strings.HasPrefix(key, "plug.restore.") {
					return restorePlan{}, fmt.Errorf("reserved restore metadata key %q", key)
				}
				encoded, err := MarshalNativePayload(scratch, "portable_document_metadata", value)
				if err != nil {
					return restorePlan{}, fmt.Errorf("bead %s metadata: %w", source.ID, err)
				}
				metadata[key] = encoded
			}
		}
		for _, field := range dumpformat.OpaqueIssueFields() {
			value, present := source.Source.Fields[field]
			if !present {
				continue
			}
			encoded, err := MarshalNativePayload(scratch, "portable_document_metadata", value)
			if err != nil {
				return restorePlan{}, fmt.Errorf("bead %s opaque field %s: %w", source.ID, field, err)
			}
			// Opaque Beads columns are stored as reserved metadata. Plug never
			// interprets these values; the importer owns their meaning.
			metadata["beads."+field] = encoded
		}
		// An explicit rehearsal plan can retain every otherwise unmodeled
		// native column under the reserved opaque namespace. This does not
		// interpret actor names or grant source identities any authority.
		for _, field := range projection.opaque[driver.SourceEntityKey{Project: source.Project, Plane: source.Source.Table, ID: source.ID}] {
			value, present := source.Source.Fields[field]
			if !present {
				return restorePlan{}, fmt.Errorf("%w: family=%s field=%s row_count=1", driver.ErrUnsupported, source.Source.Table, field)
			}
			encoded, err := MarshalNativePayload(scratch, "portable_document_metadata", value)
			if err != nil {
				return restorePlan{}, err
			}
			metadata["beads."+field] = encoded
		}
		for key, value := range source.Body {
			switch key {
			case "title", "description", "priority", "metadata", "labels":
				continue
			}
			encoded, err := MarshalNativePayload(scratch, "portable_document_metadata", value)
			if err != nil {
				return restorePlan{}, fmt.Errorf("bead %s source field %s: %w", source.ID, key, err)
			}
			metadata["beads.source."+key] = encoded
		}
		sourceStatus, err := MarshalNativePayload(scratch, "portable_document_metadata", StringValue(source.Source.Fields["status"]))
		if err != nil {
			return restorePlan{}, fmt.Errorf("bead %s source status: %w", source.ID, err)
		}
		metadata["beads.source.status"] = sourceStatus
		sourceFields, err := MarshalNativePayload(scratch, "portable_document_metadata", source.Source.Fields)
		if err != nil {
			return restorePlan{}, fmt.Errorf("bead %s source fields: %w", source.ID, err)
		}
		metadata["beads.source_fields"] = sourceFields
		if key, scoped := source.SourceKey(); scoped {
			metadata["beads.source_identity"], err = MarshalNativePayload(scratch, "portable_document_metadata", source.Source.Identity)
			if err != nil {
				return restorePlan{}, err
			}
			metadata["beads.source_key"], err = MarshalNativePayload(scratch, "portable_document_metadata", key)
			if err != nil {
				return restorePlan{}, err
			}
		}
		if project, ok := projects[source.Project]; ok {
			encoded, err := MarshalNativePayload(scratch, "portable_document_metadata", project)
			if err != nil {
				return restorePlan{}, err
			}
			if profile == nil {
				// Legacy restore keeps its historical per-bead mirror. Imported
				// source/target allocator provenance is authoritative only in the
				// protected project records below.
				metadata["plug.restore.allocator"] = encoded
			}
		}
		labels, labelsOK := ExactLabelSet(source.Body["labels"])
		if !labelsOK {
			return restorePlan{}, fmt.Errorf("%w: source labels are not writable for %s", ErrRestoreUnavailable, source.ID)
		}
		if len(labels) != 0 {
			fields["labels"] = append([]string(nil), labels...)
		}
		doc := recordimage.Document{Labels: labels, ID: source.ID, Type: targetType, Project: source.Project, Fields: fields, Metadata: metadata}
		// The admitted source row is the creation authority. Its exact value
		// remains in reserved source metadata above; absence stays unknown.
		// Restore time, version time and incomplete history cannot fill it in.
		if projection.MapsField(source, "created_at") {
			createdAt, err := SourceCreationTime(source)
			if err != nil {
				return restorePlan{}, err
			}
			doc.CreatedAt = createdAt
		}
		if projection.MapsField(source, "closed_at") {
			closedAt, err := SourceCloseTime(source)
			if err != nil {
				return restorePlan{}, err
			}
			doc.ClosedAt = closedAt
		}
		docs[source.ID] = doc
	}

	if profile != nil {
		for _, source := range comments {
			issueID := StringValue(source.Body["issue_id"])
			doc, ok := docs[issueID]
			if !ok && entities != nil && entities[driver.SourceEntityKey{Project: source.Project, Plane: dumpformat.DurableEntityTable, ID: issueID}].TargetKind == "workflow:item" {
				continue
			}
			if !ok {
				return restorePlan{}, fmt.Errorf("comment %s has no restored bead", source.ID)
			}
			author, ok := source.Body["author"].(string)
			if !ok {
				return restorePlan{}, fmt.Errorf("comment %s author is not text", source.ID)
			}
			createdAt := StringValue(source.Body["created_at"])
			sourceTime, err := time.Parse(time.RFC3339Nano, createdAt)
			if err != nil {
				return restorePlan{}, fmt.Errorf("comment %s timestamp: %w", source.ID, err)
			}
			comment, err := recordimage.NewImportedComment(*profile, StringValue(source.Body["text"]), author, sourceTime, restoreTime)
			if err != nil {
				return restorePlan{}, fmt.Errorf("comment %s: %w", source.ID, err)
			}
			doc.Comments = append(doc.Comments, comment)
			docs[issueID] = doc
		}
	}

	edges, err := scratch.RecordsChecked("edge")
	if err != nil {
		return restorePlan{}, err
	}
	for _, source := range edges {
		coreEdge, err := projection.CoreEdge(source)
		if err != nil {
			return restorePlan{}, err
		}
		if !coreEdge {
			continue
		}
		var targetKind model.LinkTargetKind
		switch StringValue(source.Body["target_kind"]) {
		case "depends_on_issue_id":
		case "depends_on_external":
			targetKind = model.LinkTargetExternal
		default:
			return restorePlan{}, fmt.Errorf("%w: unsupported typed relation %s: %w", ErrRestoreUnavailable, source.ID, &dumpformat.UnresolvedDependencyError{
				EdgeID: source.ID, Project: source.Project, Source: StringValue(source.Body["from"]), Target: StringValue(source.Body["to"]), TargetKind: StringValue(source.Body["target_kind"]),
			})
		}
		from, to := StringValue(source.Body["from"]), StringValue(source.Body["to"])
		doc, ok := docs[from]
		if _, targetExists := docs[to]; !ok || targetKind != model.LinkTargetExternal && !targetExists {
			return restorePlan{}, fmt.Errorf("%w: edge %s has no restored endpoint: %w", ErrRestoreUnavailable, source.ID, &dumpformat.UnresolvedDependencyError{
				EdgeID: source.ID, Project: source.Project, Source: from, Target: to, TargetKind: StringValue(source.Body["target_kind"]),
			})
		}
		kind := StringValue(source.Body["type"])
		targetKindName := StringValue(source.Body["target_kind"])
		switch kind {
		case "blocks":
		case "waits-for", "conditional-blocks", "tracks":
			if targetKind != model.LinkTargetExternal && (!dumpformat.IsVersionedLocalKnownEdgeType(manifest.EdgeVersion(), targetKindName, kind) || mode == planLegacy) {
				return restorePlan{}, fmt.Errorf("%w: relation type %s requires the versioned import contract", ErrRestoreUnavailable, kind)
			}
		case "related":
			kind = "relates-to"
		case "parent-child":
			if targetKind == model.LinkTargetExternal {
				break
			}
			// The source edge points from child to parent. A Plug parent is
			// core-owned hierarchy, not a second generic link. Keep the source
			// edge mirror below for exact reverse provenance.
			if mode == planLegacy {
				return restorePlan{}, fmt.Errorf("%w: relation type %s", ErrRestoreUnavailable, kind)
			}
			if doc.Parent != nil {
				return restorePlan{}, fmt.Errorf("%w: bead %s has multiple parent edges", ErrRestoreUnavailable, from)
			}
			parent := model.RefByID(to, model.Natural64(0))
			doc.Parent = &parent
			metadata, err := dumpformat.SourceLinkMetadata(manifest.EdgeVersion(), source, kind)
			if err != nil {
				return restorePlan{}, fmt.Errorf("%w: parent metadata: %v", ErrRestoreUnavailable, err)
			}
			doc.ParentMetadata = metadata
			docs[from] = doc
			continue
		default:
			if !dumpformat.IsVersionedLocalKnownEdgeType(manifest.EdgeVersion(), targetKindName, kind) &&
				!dumpformat.IsLocalOpaqueEdgeType(manifest.EdgeVersion(), targetKindName, kind) &&
				(targetKind != model.LinkTargetExternal || kind == "" || len(kind) > 32 || !utf8.ValidString(kind)) {
				return restorePlan{}, fmt.Errorf("%w: relation type %s", ErrRestoreUnavailable, kind)
			}
			if targetKind != model.LinkTargetExternal && mode == planLegacy {
				return restorePlan{}, fmt.Errorf("%w: relation type %s requires the versioned import contract", ErrRestoreUnavailable, kind)
			}
		}
		link, err := ImportedVersionedSourceLink(source, kind, targetKind, manifest.EdgeVersion())
		if err != nil {
			return restorePlan{}, fmt.Errorf("%w: relation %s: %v", ErrRestoreUnavailable, source.ID, err)
		}
		doc.Links = append(doc.Links, link)
		docs[from] = doc
	}

	plan := restorePlan{migration: migration}
	if profile != nil {
		for _, project := range manifest.Projects {
			allocator, err := ImportedAllocator(archive, project)
			if err != nil {
				return restorePlan{}, err
			}
			plan.allocators = append(plan.allocators, allocator)
		}
	}
	seen := map[string]bool{}
	add := func(ref model.Ref, kind string, body any) error {
		if err := ref.Validate(); err != nil || !ref.IsID() || ref.ID == "" || !ref.Branch.IsZero() {
			return fmt.Errorf("invalid restore record identity %q", ref.ID)
		}
		key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
		if err != nil {
			return err
		}
		if seen[key] {
			return fmt.Errorf("duplicate restore record identity %q", ref.ID)
		}
		encoded, err := MarshalNativePayload(archive, "restore_record_bodies", body)
		if err != nil {
			return err
		}
		seen[key] = true
		plan.records = append(plan.records, restoreRecord{ref: ref, kind: kind, body: encoded})
		return nil
	}

	for _, source := range beads {
		doc := docs[source.ID]
		if err := add(model.RefByID(source.ID, model.Natural64(0)), "bead", doc); err != nil {
			return restorePlan{}, err
		}
	}
	for _, kind := range []string{"edge", "comment", "memory", "config", "beads-archive"} {
		sources, err := scratch.RecordsChecked(kind)
		if err != nil {
			return restorePlan{}, err
		}
		for _, source := range sources {
			if source.Source.Table == "leases" && nativeTables && mode == planImport {
				keep, err := NativeLeaseUnexpired(source.Source.Fields, restoreTime)
				if err != nil {
					return restorePlan{}, err
				}
				if !keep {
					continue
				}
			}
			if profile != nil && entities == nil && kind == "memory" {
				// Preserve main's legacy public-memory contract. Source-plane plans
				// retain their separately bound mirror identity and raw envelope.
				if value, ok := source.Body["value"].(string); source.Body["classification"] == "public" && ok {
					key := strings.TrimSpace(StringValue(source.Body["key"]))
					if key == "" {
						return restorePlan{}, fmt.Errorf("imported memory %s has an empty public key", source.ID)
					}
					ref := model.RefByID(SourcePlaneMirrorID(source, entities != nil), model.Natural64(0))
					body := recordimage.MemoryDocument{ID: ref.ID, Key: key, Content: value, Metadata: map[string]any{
						"plug.restore.source_id": source.ID, "plug.restore.source_table": source.Source.Table,
						"plug.restore.source": CloneRow(source.Source.Fields), "plug.restore.archive_sha256": archive.SHA256(),
					}}
					if entities != nil {
						body.Metadata["plug.restore.source_project"] = source.Project
					}
					if err := add(ref, kind, body); err != nil {
						return restorePlan{}, err
					}
				} else {
					body := CloneRow(source.Body)
					body["source_id"], body["source_table"], body["source"] = source.ID, source.Source.Table, CloneRow(source.Source.Fields)
					if err := add(model.RefByID("source/memory/"+source.ID, model.Natural64(0)), kind, body); err != nil {
						return restorePlan{}, err
					}
				}
				continue
			}
			body, err := SourceMirrorBody(source, entities != nil)
			if err != nil {
				return restorePlan{}, err
			}
			if err := add(model.RefByID(SourcePlaneMirrorID(source, entities != nil), model.Natural64(0)), kind, body); err != nil {
				return restorePlan{}, err
			}
		}
	}
	if nativeTables && mode == planImport {
		if profile.SourcePlanes == nil || profile.HistoryMode != driver.RestoreHistoryModeFull && !(archive.HasNativeDelta() && profile.NativeDeltaHistory != nil) {
			return restorePlan{}, fmt.Errorf("%w: native table publication requires bound full source-plane restore", driver.ErrUnsupported)
		}
		var current []driver.Record
		var families []NativeFamilyReport
		if collection != nil {
			current, families = collection.records, collection.families
		} else {
			current, families, err = NativeCurrentRecords(archive, restoreTime, NativeOperationContext(contexts))
		}
		if err != nil {
			return restorePlan{}, err
		}
		for _, row := range current {
			if err := add(row.Ref, row.Kind, json.RawMessage(row.Body)); err != nil {
				return restorePlan{}, err
			}
		}
		plan.nativeFamilies = families
		plan.nativeSchemas, err = NativeSchemaAccounting(archive, *profile, docs, plan.allocators)
		if err != nil {
			return restorePlan{}, err
		}
		plan.nativeHistories, err = NativeHistoryAccounting(archive)
		if err != nil {
			return restorePlan{}, err
		}
		if profile.NativeDeltaHistory != nil {
			plan.nativeDeltaHistory, err = NativeDeltaHistoryEvidence(archive, *profile)
			if err != nil {
				return restorePlan{}, err
			}
			if profile.HistoryMode == driver.RestoreHistoryModeSnapshot {
				if err := add(model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0)), "beads-archive", NativeSnapshotHistoryEvidence(plan.nativeDeltaHistory)); err != nil {
					return restorePlan{}, err
				}
			}
		}
		accounting := NativeFamilyEvidence{Contract: manifest.SourceFamilyContract, RestoreTime: restoreTime.UTC().Format(time.RFC3339Nano), Families: families, Schemas: plan.nativeSchemas, Histories: plan.nativeHistories, NativeDeltaHistory: plan.nativeDeltaHistory}
		if err := add(model.RefByID(NativeFamilyReportID, model.Natural64(0)), "instance_config", accounting); err != nil {
			return restorePlan{}, err
		}
	}

	if source, native := archive.NativeStatusOntology(); native {
		plan.nativeOntology = &source
		if err := add(model.RefByID(ImportedOntologySourceID, model.Natural64(0)), "ontology", source); err != nil {
			return restorePlan{}, err
		}
	}
	if err := rebuildDerived(&plan, docs, archive); err != nil {
		return restorePlan{}, err
	}
	if err := retainArchive(&plan, archive, add); err != nil {
		return restorePlan{}, err
	}
	if profile != nil {
		requestHash, err := profile.RequestHash()
		if err != nil {
			return restorePlan{}, err
		}
		binding := ImportBindingEvidence{Profile: *profile, RequestHash: requestHash}
		for i, project := range manifest.Projects {
			sourceRaw, err := MarshalNativePayload(scratch, "portable_document_metadata", project)
			if err != nil {
				return restorePlan{}, err
			}
			targetAllocator := plan.allocators[i].Clone()
			var identity *dumpformat.SourceIdentity
			for _, origin := range archive.SourceIdentities() {
				if origin.Project == project.Prefix {
					copy := origin
					identity = &copy
				}
			}
			// Single-source native archives retain the original store/project
			// in their validated history scope, without rewriting each record
			// into the merged-source identity encoding.
			nativeMetadata, err := archive.NativeSourceHistoryMetadata()
			if err != nil {
				return restorePlan{}, err
			}
			for _, scope := range nativeMetadata {
				for _, prefix := range scope.Projects {
					if prefix != project.Prefix {
						continue
					}
					origin := dumpformat.SourceIdentity{StoreID: scope.StoreID, Project: prefix}
					if identity != nil && *identity != origin {
						return restorePlan{}, fmt.Errorf("%w: conflicting native source/project identity", driver.ErrUnsupported)
					}
					identity = &origin
				}
			}
			binding.Projects = append(binding.Projects, ImportProjectBinding{
				SourceIdentity:  identity,
				SourceAllocator: project,
				TargetAllocator: targetAllocator,
			})
			metadata := ImportProjectMetadata{
				SourceIdentity: identity,
				Project:        project.Prefix,
				ImportProfile:  *profile,
				Metadata: map[string]json.RawMessage{
					"plug.restore.sourceAllocator": sourceRaw,
				},
				TargetAllocator: targetAllocator,
			}
			if err := add(model.RefByID(ImportProjectRecordID(project.Prefix), model.Natural64(0)), "instance_config", metadata); err != nil {
				return restorePlan{}, err
			}
		}
		if err := add(model.RefByID(ImportBindingRecordID, model.Natural64(0)), "instance_config", binding); err != nil {
			return restorePlan{}, err
		}
	}

	// Every bead write needs its exact predicate bump. The graph sentinel is
	// always included so the restore transaction is serialized with blocking
	// relation maintenance, even when this particular archive has no block edge.
	plan.sentinels = append(plan.sentinels, driver.SentinelGraphBlocking)
	for _, source := range beads {
		sentinel, err := driver.NewBeadSentinel(source.ID)
		if err != nil {
			return restorePlan{}, err
		}
		plan.sentinels = append(plan.sentinels, sentinel)
	}
	sort.Slice(plan.records, func(i, j int) bool {
		return plan.records[i].ref.ID < plan.records[j].ref.ID
	})
	sort.Slice(plan.sentinels, func(i, j int) bool { return string(plan.sentinels[i]) < string(plan.sentinels[j]) })
	return plan, nil
}

func TargetStatus(source string) (string, bool) {
	switch source {
	case "open":
		return "todo", true
	case "in_progress", "hooked":
		return "in_progress", true
	case "blocked", "deferred", "pinned", "frozen":
		return "backlog", true
	case "closed":
		return "done", true
	default:
		return "", false
	}
}

func ValidSourceID(id string, project dumpformat.Project, imported bool) bool {
	if imported {
		return dumpformat.ValidSourceID(id, project)
	}
	prefix := project.Prefix + "-"
	if !strings.HasPrefix(id, prefix) || strings.ContainsRune(id, '\x00') {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(id, prefix), ".")
	if len(parts) == 0 || parts[0] == "" || len(parts[0]) < project.HashLength {
		return false
	}
	if len(parts) != 1 {
		return false
	}
	identity, err := hex.DecodeString(parts[0])
	return err == nil && len(identity) == 16 && hex.EncodeToString(identity) == parts[0]
}

func StringValue(value any) string {
	text, _ := value.(string)
	return text
}

func CloneRow(input map[string]any) map[string]any {
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = CloneValue(value)
	}
	return output
}

func CloneValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		return CloneRow(value)
	case []any:
		output := make([]any, len(value))
		for i, item := range value {
			output[i] = CloneValue(item)
		}
		return output
	default:
		return value
	}
}

// SourceCreationTime accepts only the admitted row's own creation field.
// It has no clock or history input and never substitutes the canonical mirror.
func SourceCreationTime(source dumpformat.Record) (*time.Time, error) {
	raw := source.Source.Fields["created_at"]
	if raw == nil {
		return nil, nil
	}
	stamp, ok := raw.(string)
	createdAt, err := time.Parse(time.RFC3339Nano, stamp)
	if !ok || err != nil || createdAt.IsZero() {
		return nil, fmt.Errorf("bead %s has invalid source creation instant", source.ID)
	}
	createdAt = createdAt.UTC()
	return &createdAt, nil
}

// SourceCloseTime accepts only the admitted row's own close field. A missing
// value preserves an unknown close instant; no restore or version clock is
// evidence of when a bead was closed.
func SourceCloseTime(source dumpformat.Record) (*time.Time, error) {
	raw := source.Source.Fields["closed_at"]
	if raw == nil {
		return nil, nil
	}
	stamp, ok := raw.(string)
	closedAt, err := time.Parse(time.RFC3339Nano, stamp)
	if !ok || err != nil || closedAt.IsZero() {
		return nil, fmt.Errorf("bead %s has invalid source close instant", source.ID)
	}
	closedAt = closedAt.UTC()
	return &closedAt, nil
}

// ImportedSourceLink preserves the typed endpoint before any local lookup.
func ImportedSourceLink(body map[string]any, kind string, targetKind model.LinkTargetKind) (recordimage.Link, error) {
	return ImportedVersionedSourceLink(dumpformat.Record{Body: body}, kind, targetKind, 1)
}

func ImportedVersionedSourceLink(source dumpformat.Record, kind string, targetKind model.LinkTargetKind, edgeVersion int) (recordimage.Link, error) {
	link := recordimage.Link{To: model.RefByID(StringValue(source.Body["to"]), model.Natural64(0)), Kind: kind, TargetKind: targetKind}
	if err := model.ValidateLinkTarget(targetKind, link.To); err != nil {
		return recordimage.Link{}, err
	}
	metadata, err := dumpformat.SourceLinkMetadata(edgeVersion, source, kind)
	if err != nil {
		return recordimage.Link{}, err
	}
	link.Metadata = metadata
	return link, nil
}
