package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// BindSourceImport verifies a host's complete explicit mapping against the
// admitted archive. Only byte commitments and the rebuilt core digest may be
// filled here. Runs, principals, dispositions, targets and lifetime are inputs.
// Empty image/digest fields request calculation; nonempty fields must match.
func BindSourceImport(ctx context.Context, input io.Reader, profile driver.RestoreImportProfile, plan driver.SourcePlanePlanV1) (driver.RestoreImportProfile, error) {
	if err := ctx.Err(); err != nil {
		return driver.RestoreImportProfile{}, err
	}
	if profile.SourcePlanes != nil {
		return driver.RestoreImportProfile{}, driver.ErrConflict
	}
	archive, err := dumpformat.Read(input)
	if err != nil {
		return driver.RestoreImportProfile{}, err
	}
	if len(plan.Providers) != 1 {
		return driver.RestoreImportProfile{}, driver.ErrUnsupported
	}
	plan.Providers = append([]driver.SourcePlaneProviderV1(nil), plan.Providers...)
	wantImage, wantSize := plan.Providers[0].LogicalImageSHA256, plan.Providers[0].LogicalImageSize
	if wantImage == "" && wantSize != 0 {
		return driver.RestoreImportProfile{}, driver.ErrConflict
	}
	if wantImage == "" {
		plan.Providers[0].LogicalImageSHA256 = strings.Repeat("0", 64)
	}
	binding, err := driver.NewSourcePlaneImportBindingV1(plan)
	if err != nil {
		return driver.RestoreImportProfile{}, err
	}
	wantDerived := profile.DerivedDigest
	if wantDerived == "" {
		profile.DerivedDigest = strings.Repeat("0", 64)
	}
	profile.SourcePlanes = &binding
	profile, err = profile.Detached()
	if err != nil {
		return driver.RestoreImportProfile{}, err
	}
	prepared, err := prepareSourceArchive(ctx, archive, profile, false)
	if err != nil {
		return driver.RestoreImportProfile{}, err
	}
	image := prepared.LogicalImage()
	if wantImage != "" && (wantImage != prepared.SHA256() || wantSize != uint64(len(image))) {
		return driver.RestoreImportProfile{}, driver.ErrConflict
	}
	plan.Providers[0].LogicalImageSHA256, plan.Providers[0].LogicalImageSize = prepared.SHA256(), uint64(len(image))
	binding, err = driver.NewSourcePlaneImportBindingV1(plan)
	if err != nil {
		return driver.RestoreImportProfile{}, err
	}
	profile.SourcePlanes = &binding
	core, err := buildPlanWithProfile(archive, &profile, time.Now().UTC())
	if err != nil {
		return driver.RestoreImportProfile{}, err
	}
	if wantDerived != "" && wantDerived != core.derivedDigest {
		return driver.RestoreImportProfile{}, driver.ErrConflict
	}
	profile.DerivedDigest = core.derivedDigest
	if profile.HistoryMode != driver.RestoreHistoryModeSnapshot {
		when := time.Now().UTC()
		if profile.HistoryTime != "" {
			when, err = time.Parse(time.RFC3339Nano, profile.HistoryTime)
			if err != nil {
				return driver.RestoreImportProfile{}, err
			}
		}
		return BindHistoryImport(ctx, bytes.NewReader(archive.TarBytes()), profile, when)
	}
	return profile.Detached()
}

func prepareSourceArchive(ctx context.Context, archive *dumpformat.Archive, profile driver.RestoreImportProfile, final bool) (workflowplugin.PreparedSourceRestore, error) {
	var empty workflowplugin.PreparedSourceRestore
	if archive == nil || archive.Manifest().SourcePlaneAddressContract != driver.SourcePlaneAddressContractV1 || profile.SourcePlanes == nil {
		return empty, fmt.Errorf("%w: joint restore adapter requires the source plane archive and complete plan", driver.ErrUnsupported)
	}
	plan, err := profile.SourcePlanes.Plan()
	if err != nil {
		return empty, err
	}
	if plan.ArchiveSHA256 != archive.SHA256() || plan.SourceStoreID != archive.Manifest().StoreID {
		return empty, fmt.Errorf("%w: joint plan/archive correspondence", driver.ErrUnsupported)
	}
	switch profile.HistoryMode {
	case driver.RestoreHistoryModeSnapshot:
		if profile.HistoryDigest != "" || profile.HistoryTime != "" {
			return empty, driver.ErrUnsupported
		}
	case driver.RestoreHistoryModeFull:
		if err := validateArchiveHistoryPolicy(profile.HistoryMode, archive); err != nil {
			return empty, err
		}
		if final && (profile.HistoryDigest == "" || profile.HistoryTime == "") {
			return empty, driver.ErrRestoreHistoryRefused{Mode: profile.HistoryMode, History: archive.Manifest().History, Reason: driver.RestoreHistoryReasonUnmappable}
		}
	default:
		return empty, driver.ErrRestoreHistoryRefused{Mode: profile.HistoryMode, History: archive.Manifest().History, Reason: driver.RestoreHistoryReasonNotYetSupported}
	}
	if err := validateSourceCoreMappings(archive, plan); err != nil {
		return empty, err
	}
	wantOrigins, err := archive.SourcePlaneSidecarOrigins()
	if err != nil {
		return empty, err
	}
	var actualOrigins []driver.SourcePlaneOriginV1
	for _, origin := range plan.Origins {
		if origin.Path != "" {
			actualOrigins = append(actualOrigins, origin)
		}
	}
	if !reflect.DeepEqual(actualOrigins, wantOrigins) {
		return empty, fmt.Errorf("%w: sidecar dispositions differ from captured bytes", driver.ErrUnsupported)
	}
	records, err := archive.SourcePlaneRecords()
	if err != nil {
		return empty, err
	}
	prepared, err := (&workflowplugin.Provider{}).PrepareSourceRestore(ctx, plan, records)
	if err != nil {
		return empty, err
	}
	if final && (len(plan.Providers) != 1 || plan.Providers[0].LogicalImageSHA256 != prepared.SHA256() || plan.Providers[0].LogicalImageSize != uint64(len(prepared.LogicalImage()))) {
		return empty, driver.ErrConflict
	}
	return prepared, nil
}

type sourceCoreProjection struct {
	entities  map[driver.SourceEntityKey]driver.SourcePlaneEntityV1
	relations map[driver.SourceEdgeKey]driver.SourcePlaneRelationV1
	fields    map[driver.SourceEntityKey]map[string]bool
}

func newSourceCoreProjection(profile *driver.RestoreImportProfile) (sourceCoreProjection, error) {
	var projection sourceCoreProjection
	if profile == nil || profile.SourcePlanes == nil {
		return projection, nil
	}
	plan, err := profile.SourcePlanes.Plan()
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
	for _, e := range plan.Entities {
		projection.entities[e.Source] = e
		projection.fields[e.Source] = dispositions[e.DispositionKey]
	}
	projection.relations = make(map[driver.SourceEdgeKey]driver.SourcePlaneRelationV1, len(plan.Relations))
	for _, relation := range plan.Relations {
		projection.relations[relation.Source] = relation
	}
	return projection, nil
}

func (p sourceCoreProjection) mapsField(source dumpformat.Record, field string) bool {
	if p.entities == nil {
		return true
	}
	return p.fields[driver.SourceEntityKey{Project: source.Project, Plane: source.Source.Table, ID: source.ID}][field]
}

func (p sourceCoreProjection) coreEdge(source dumpformat.Record) (bool, error) {
	if p.entities == nil {
		return true, nil
	}
	// Provider preparation verifies each relation against its raw edge. Route
	// using those complete endpoint tuples, including each endpoint's project.
	relation, ok := p.relations[driver.SourceEdgeKey{Project: source.Project, Table: source.Source.Table, ID: source.ID}]
	if !ok {
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
func sourcePlaneMirrorID(source dumpformat.Record, joint bool) string {
	if key, scoped := source.SourceKey(); scoped {
		encoded, _ := json.Marshal(key)
		return "source/" + source.Kind + "/" + hashBytes(encoded)
	}
	if !joint {
		return "source/" + source.Kind + "/" + source.ID
	}
	key, _ := json.Marshal([]string{source.Kind, source.Project, source.Source.Table, source.ID})
	return "source/" + source.Kind + "/" + hashBytes(key)
}

// sourceMirrorBody keeps the exact source mirror while giving imported memories
// the canonical live memory envelope consumed by memory.get/list and agent.brief.
// Non-text JSON values retain their exact JSON representation in content; the
// original value and provenance remain present for lossless reverse/readback.
func sourceMirrorBody(source dumpformat.Record, joint bool) (map[string]any, error) {
	body := cloneRow(source.Body)
	body["source_id"], body["source_table"], body["source"] = source.ID, source.Source.Table, cloneRow(source.Source.Fields)
	if key, scoped := source.SourceKey(); scoped {
		body["source_identity"], body["source_key"] = *source.Source.Identity, key
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
	if source.Kind == "memory" {
		content, text := source.Body["value"].(string)
		encoding := "text"
		if !text {
			raw, err := json.Marshal(source.Body["value"])
			if err != nil {
				return nil, fmt.Errorf("source memory %s: %w", source.ID, err)
			}
			content, encoding = string(raw), "json"
		}
		body["id"], body["content"] = sourcePlaneMirrorID(source, joint), content
		body["metadata"] = map[string]any{"beads.memory.value_encoding": encoding, "beads.classification": source.Body["classification"]}
	}
	return body, nil
}

func validateSourceCoreMappings(archive *dumpformat.Archive, plan driver.SourcePlanePlanV1) error {
	dispositions := map[string]driver.SourcePlaneDispositionV1{}
	for _, d := range plan.Dispositions {
		dispositions[d.Key] = d
	}
	entities := map[driver.SourceEntityKey]driver.SourcePlaneEntityV1{}
	for _, e := range plan.Entities {
		entities[e.Source] = e
		if e.TargetKind == "bead" && (e.Source.Plane != dumpformat.DurableEntityTable || e.TargetID != e.Source.ID) {
			return fmt.Errorf("%w: unsupported durable source identity/schema", driver.ErrUnsupported)
		}
	}
	migration, _ := archive.ImportMigration()
	for _, source := range archive.Records("bead") {
		e, ok := entities[driver.SourceEntityKey{Project: source.Project, Plane: source.Source.Table, ID: source.ID}]
		if !ok {
			return fmt.Errorf("%w: core source entity missing from joint plan", driver.ErrUnsupported)
		}
		if e.TargetKind != "bead" {
			continue
		}
		d := dispositions[e.DispositionKey]
		storage, storagePresent := source.Source.Fields["storage_class"]
		ephemeral, ephemeralOK := sourceSourceFlag(source.Source.Fields["ephemeral"])
		noHistory, noHistoryOK := sourceSourceFlag(source.Source.Fields["no_history"])
		if !d.MatchesStorageClass(storage, storagePresent) || !ephemeralOK || !noHistoryOK || d.SourceType != stringValue(source.Source.Fields["issue_type"]) || d.Ephemeral != ephemeral || d.NoHistory != noHistory || d.SinkKind != "bead" || d.Schema != "bead/1" || d.Lifetime != "core" || d.RefusalPolicy != "exact-only" {
			return fmt.Errorf("%w: core type/storage/flags lack exact disposition", driver.ErrUnsupported)
		}
		status, known := targetStatus(stringValue(source.Source.Fields["status"]))
		if archive.Manifest().StatusCategoryContract == dumpformat.StatusCategoryContract {
			status, known = stringValue(source.Source.Fields["status"]), false
			for _, project := range archive.Manifest().Projects {
				if project.Prefix == source.Project {
					for _, declaration := range project.Statuses {
						known = known || declaration.Name == status
					}
				}
			}
			for _, pair := range d.Statuses {
				if pair.From != pair.To {
					return fmt.Errorf("%w: native status disposition must preserve its exact name", driver.ErrUnsupported)
				}
			}
		}
		if migration != nil {
			target, ok := migration.Target(source.Project, source.ID)
			status, known = target.Status, ok
		}
		matched := false
		for _, pair := range d.Statuses {
			matched = matched || pair.From == stringValue(source.Source.Fields["status"]) && pair.To == status
		}
		if !known || !matched || len(d.Fields) != len(source.Source.Fields) {
			return fmt.Errorf("%w: incomplete core field/status disposition", driver.ErrUnsupported)
		}
		mapped := map[string]bool{}
		for _, rule := range d.Fields {
			if _, present := source.Source.Fields[rule.Source]; !present {
				return driver.ErrUnsupported
			}
			if rule.Disposition == "refuse-required" {
				return fmt.Errorf("%w: core field explicitly refused: %s", driver.ErrUnsupported, rule.Source)
			}
			if rule.Disposition == "origin-only" && rule.Target == "" {
				continue
			}
			if rule.Disposition != "exact-map" || mapped[rule.Target] {
				return driver.ErrUnsupported
			}
			mapped[rule.Target] = true
			want := rule.Source
			if rule.Source == "issue_type" {
				want = "type"
			}
			switch rule.Source {
			case "id", "title", "description", "priority", "status", "issue_type", "metadata", "created_at":
			default:
				return driver.ErrUnsupported
			}
			if rule.Target != want {
				return driver.ErrUnsupported
			}
		}
		for _, required := range []string{"id", "title", "type", "status"} {
			if !mapped[required] {
				return fmt.Errorf("%w: required core field lacks exact mapping: %s", driver.ErrUnsupported, required)
			}
		}
	}
	return nil
}

// Initial checkpoint construction is data-only under the already held Head.
// It uses the driver's new incarnation, never the abandoned empty-target one.
func prepareSourcePublication(ctx context.Context, capability driver.WorkflowCheckpointStore, lease driver.ActiveLease, tx driver.WriteTx, profile driver.RestoreImportProfile, restored driver.Meta, logical []byte) (driver.SourcePlaneCommitBindingV1, error) {
	plan, err := profile.SourcePlanes.Plan()
	if err != nil {
		return driver.SourcePlaneCommitBindingV1{}, err
	}
	host, ok := pluginhost.NewDurableState(capability, lease, "workflow", "default").(driver.SourcePlaneCheckpointPreparer)
	if !ok {
		return driver.SourcePlaneCommitBindingV1{}, driver.ErrUnsupported
	}
	prepared, err := host.PrepareRestoreCheckpoint(ctx, restored, profile, plan.Providers[0], logical)
	if err != nil {
		return driver.SourcePlaneCommitBindingV1{}, err
	}
	writer, ok := tx.(driver.CheckpointWriteTx)
	if !ok {
		return driver.SourcePlaneCommitBindingV1{}, driver.ErrUnsupported
	}
	mutation := prepared.Mutation
	if mutation.StoreID != restored.StoreID || mutation.Incarnation != restored.Incarnation || mutation.Key != plan.Providers[0].Namespace || mutation.Expected != 0 {
		return driver.SourcePlaneCommitBindingV1{}, driver.ErrConflict
	}
	if err = writer.StageCheckpoint(ctx, mutation.Key, mutation.Expected, mutation.Payload); err != nil {
		return driver.SourcePlaneCommitBindingV1{}, err
	}
	request, err := profile.RequestHash()
	if err != nil {
		return driver.SourcePlaneCommitBindingV1{}, err
	}
	binding := driver.SourcePlaneCommitBindingV1{Version: driver.SourcePlaneCommitVersionV1, JointPlanDigest: profile.SourcePlanes.JointPlanDigest, RequestHash: request, StoreID: restored.StoreID, Incarnation: restored.Incarnation, Revision: restored.Head, Providers: []driver.SourcePlaneProviderCommitV1{prepared.Binding}}
	if err = tx.(driver.SourcePlaneRestoreTx).BindSourcePlaneImport(ctx, binding); err != nil {
		return driver.SourcePlaneCommitBindingV1{}, err
	}
	return binding, nil
}

func sourceSourceFlag(value any) (bool, bool) {
	switch value {
	case true, json.Number("1"):
		return true, true
	case false, json.Number("0"):
		return false, true
	}
	return false, false
}
