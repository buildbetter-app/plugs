package b01readyorder_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

const b01CaptureProject = "clockready"

func b01CaptureHash(raw []byte) string { return fmt.Sprintf("%x", sha256.Sum256(raw)) }

func b01CaptureDecode(t *testing.T, raw []byte, out any) {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(out); err != nil {
		t.Fatal(err)
	}
}

func b01CaptureCommitment(raw []byte, presence string) driver.SourcePlaneBytesV1 {
	return driver.SourcePlaneBytesV1{Presence: presence, Size: uint64(len(raw)), SHA256: b01CaptureHash(raw)}
}

// Every physical field has a lossless commitment, including fields retained
// only as source origins. No Plug response contributes a source expectation.
func b01CaptureFields(t *testing.T, raw []byte) []driver.SourcePlaneFieldV1 {
	t.Helper()
	var wire struct {
		Source struct {
			Fields map[string]json.RawMessage `json:"fields"`
		} `json:"source"`
	}
	b01CaptureDecode(t, raw, &wire)
	var fields []driver.SourcePlaneFieldV1
	for name, value := range wire.Source.Fields {
		presence := "null"
		switch bytes.TrimSpace(value)[0] {
		case '"':
			presence = "text"
			var text string
			b01CaptureDecode(t, value, &text)
			value = []byte(text)
		case '{':
			presence = "object"
		case '[':
			presence = "array"
		case 't', 'f':
			presence = "boolean"
		case 'n':
		default:
			presence = "number"
		}
		fields = append(fields, driver.SourcePlaneFieldV1{Name: name, Value: b01CaptureCommitment(value, presence)})
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i].Name < fields[j].Name })
	return fields
}

func b01CaptureText(t *testing.T, row dumpformat.Row, name string) string {
	t.Helper()
	s, ok := row[name].(string)
	if !ok {
		t.Fatalf("native %s is not text: %T", name, row[name])
	}
	return s
}

func b01CaptureBool(t *testing.T, value any) bool {
	t.Helper()
	switch value {
	case true, json.Number("1"):
		return true
	case false, json.Number("0"):
		return false
	default:
		t.Fatalf("native flag is not an exact boolean/0/1: %v", value)
		return false
	}
}

func b01CaptureStatus(t *testing.T, archive *dumpformat.Archive, raw string) string {
	t.Helper()
	if archive.Manifest().StatusCategoryContract == dumpformat.StatusCategoryContract {
		return raw
	}
	switch raw {
	case "open":
		return "todo"
	case "in_progress":
		return "in_progress"
	case "closed":
		return "done"
	default:
		t.Fatalf("no recorded durable status correspondence for %q", raw)
		return ""
	}
}

// This is card-owned host intent at the existing import API. It introduces no
// importer or runtime service. Each genuine active native item remains a workflow:item, with
// the selected store's actual process/restart lifetime and explicit run.
func b01CapturePlan(t *testing.T, p *publicAcceptance, archive *dumpformat.Archive) driver.SourcePlanePlanV1 {
	t.Helper()
	meta := p.meta(t)
	capability, err := driver.RequireWorkflowCheckpointStore(p.store, p.lease)
	if err != nil {
		t.Fatal(err)
	}
	principal := jsonBytes(t, anonymous)
	plan := driver.SourcePlanePlanV1{
		Version: driver.SourcePlanePlanVersionV1, AddressContract: driver.SourcePlaneAddressContractV1,
		SourceStoreID: archive.Manifest().StoreID, ArchiveSHA256: archive.SHA256(),
		Target:     driver.SourcePlaneTargetV1{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, RequireEmpty: true, Lifetime: capability.WorkflowCheckpointLifetime(), RuntimeContract: driver.SourcePlaneRuntimeContractV1},
		Principals: []driver.SourcePlanePrincipalV1{{Key: "owner", Source: "B01 disposable public capture host", Authority: "trusted-explicit/1", PrincipalJSON: principal}},
		Providers:  []driver.SourcePlaneProviderV1{{Alias: "workflow", Instance: "default", Namespace: readyWorkflowCheckpoint, Schema: workflowplugin.SourceStateSchema, Codec: "workflow/json/1", RuntimeContract: driver.SourcePlaneRuntimeContractV1, Lifetime: capability.WorkflowCheckpointLifetime(), OriginAdapter: "beads-workflow-origins/1"}},
	}
	raw, err := archive.SourcePlaneRecords()
	if err != nil {
		t.Fatal(err)
	}
	dispositions := map[string]driver.SourcePlaneDispositionV1{}
	run := driver.SourcePlaneRunV1{Project: b01CaptureProject, Key: "capture", TargetID: "workflow:run/b01-public-capture", Template: "B01 authentic active native rows", Schema: workflowplugin.SourceStateSchema, Mode: "ephemeral", State: "active", PrincipalKey: "owner"}
	for _, record := range raw {
		if record.Table != dumpformat.DurableEntityTable && record.Table != dumpformat.EphemeralEntityTable {
			continue
		}
		var row dumpformat.Record
		b01CaptureDecode(t, record.Raw, &row)
		key := driver.SourceEntityKey{Project: b01CaptureProject, Plane: record.Table, ID: b01CaptureText(t, row.Source.Fields, "id")}
		active := record.Table == dumpformat.EphemeralEntityTable
		storage, present := row.Source.Fields["storage_class"]
		if !present {
			t.Fatal("native storage_class column absent")
		}
		storageClass := ""
		if storage != nil {
			var ok bool
			storageClass, ok = storage.(string)
			if !ok || storageClass == "" {
				t.Fatal("native storage_class must be SQL NULL or nonempty text")
			}
		}
		d := driver.SourcePlaneDispositionV1{SourceType: b01CaptureText(t, row.Source.Fields, "issue_type"), StorageClass: storageClass, Ephemeral: b01CaptureBool(t, row.Source.Fields["ephemeral"]), NoHistory: b01CaptureBool(t, row.Source.Fields["no_history"]), SinkKind: "bead", Schema: "bead/1", Lifetime: "core", RefusalPolicy: "exact-only"}
		if active {
			d.SinkKind, d.Schema, d.Lifetime = "workflow:item", workflowplugin.SourceStateSchema, string(capability.WorkflowCheckpointLifetime())
		}
		census := b01CaptureFields(t, record.Raw)
		for _, field := range census {
			rule := driver.SourcePlaneFieldRuleV1{Source: field.Name, Disposition: "origin-only"}
			switch field.Name {
			case "id", "title", "status", "metadata", "created_at":
				rule.Target, rule.Disposition = field.Name, "exact-map"
			case "issue_type":
				rule.Target, rule.Disposition = "type", "exact-map"
			case "priority", "description":
				rule.Target, rule.Disposition = field.Name, "exact-map"
				if active {
					rule.Target = "fields." + field.Name
				}
			}
			d.Fields = append(d.Fields, rule)
		}
		// Classification and complete field slots select a shared disposition;
		// status correspondences are then explicitly unioned from source rows.
		d.Key = "class-" + b01CaptureHash(jsonBytes(t, d))[:20]
		status := b01CaptureText(t, row.Source.Fields, "status")
		targetStatus := status
		if !active {
			targetStatus = b01CaptureStatus(t, archive, status)
		}
		if existing, ok := dispositions[d.Key]; ok {
			d = existing
		}
		pair := driver.SourcePlaneStringPairV1{From: status, To: targetStatus}
		found := false
		for _, existing := range d.Statuses {
			found = found || existing == pair
		}
		if !found {
			d.Statuses = append(d.Statuses, pair)
		}
		sort.Slice(d.Statuses, func(i, j int) bool { return d.Statuses[i].From < d.Statuses[j].From })
		dispositions[d.Key] = d
		e := driver.SourcePlaneEntityV1{Source: key, TargetKind: "bead", TargetID: key.ID, DispositionKey: d.Key, PrincipalKey: "owner", Origin: b01CaptureCommitment(record.Raw, "object"), Fields: census, Route: driver.SourcePlaneRouteV1{Project: key.Project, SourceID: key.ID, DurableTargetID: key.ID, Preference: "active-first"}}
		if active {
			activeID := "workflow:item/" + b01CaptureHash([]byte(key.Project + "/" + key.Plane + "/" + key.ID))[:32]
			e.TargetKind, e.TargetID, e.RunKey = "workflow:item", activeID, run.Key
			e.Route.ActiveTargetID, e.Route.DurableTargetID = e.TargetID, ""
			run.Members = append(run.Members, key)
		}
		plan.Entities = append(plan.Entities, e)
	}
	if len(run.Members) == 0 {
		t.Fatal("complete B01 capture has no genuine active items")
	}
	sort.Slice(run.Members, func(i, j int) bool { return driver.CompareSourceEntityKeys(run.Members[i], run.Members[j]) < 0 })
	plan.Runs = []driver.SourcePlaneRunV1{run}
	for _, d := range dispositions {
		plan.Dispositions = append(plan.Dispositions, d)
	}
	sort.Slice(plan.Dispositions, func(i, j int) bool { return plan.Dispositions[i].Key < plan.Dispositions[j].Key })
	for _, record := range raw {
		if record.Table == dumpformat.DurableEntityTable || record.Table == dumpformat.EphemeralEntityTable {
			continue
		}
		var row dumpformat.Record
		b01CaptureDecode(t, record.Raw, &row)
		origin, fields := b01CaptureCommitment(record.Raw, "object"), b01CaptureFields(t, record.Raw)
		if record.Kind == "edge" {
			fromPlane, toPlane := dumpformat.DurableEntityTable, dumpformat.DurableEntityTable
			if record.Table == dumpformat.EphemeralDependencyTable {
				fromPlane = dumpformat.EphemeralEntityTable
			}
			if row.Body["target_kind"] == dumpformat.EphemeralDependencyTargetField {
				toPlane = dumpformat.EphemeralEntityTable
			}
			from := driver.SourceEntityKey{Project: b01CaptureProject, Plane: fromPlane, ID: b01CaptureText(t, row.Source.Fields, "issue_id")}
			to := driver.SourceEntityKey{Project: b01CaptureProject, Plane: toPlane, ID: b01CaptureText(t, row.Body, "to")}
			kind := b01CaptureText(t, row.Body, "type")
			predicate, err := dumpformat.SourceEdgeMetadata(3, record.ID, kind, row.Source.Fields["metadata"])
			if err != nil {
				t.Fatal(err)
			}
			plan.Relations = append(plan.Relations, driver.SourcePlaneRelationV1{Source: driver.SourceEdgeKey{Project: b01CaptureProject, Table: record.Table, ID: record.ID}, From: from, To: &to, Kind: kind, PredicateContract: "beads-typed-metadata/1", PredicateJSON: jsonBytes(t, predicate), Origin: origin, Fields: fields})
			continue
		}
		o := driver.SourcePlaneOriginV1{Project: row.Project, Table: record.Table, ID: record.ID, Action: "inert-origin", Raw: origin, Fields: fields, Parsed: driver.SourcePlaneBytesV1{Presence: "absent"}}
		if row.Source.Identity != nil {
			o.Project = row.Source.Identity.Project
		}
		switch record.Table {
		case dumpformat.EphemeralLabelTable, dumpformat.EphemeralCommentTable, dumpformat.EphemeralChildCounterTable:
			ownerField := "issue_id"
			if record.Table == dumpformat.EphemeralChildCounterTable {
				ownerField = "parent_id"
			}
			o.Owner = &driver.SourceEntityKey{Project: b01CaptureProject, Plane: dumpformat.EphemeralEntityTable, ID: b01CaptureText(t, row.Source.Fields, ownerField)}
			o.Project, o.Action = b01CaptureProject, "exact-map"
		}
		plan.Origins = append(plan.Origins, o)
	}
	sidecars, err := archive.SourcePlaneSidecarOrigins()
	if err != nil {
		t.Fatal(err)
	}
	plan.Origins = append(plan.Origins, sidecars...)
	sort.Slice(plan.Entities, func(i, j int) bool {
		return driver.CompareSourceEntityKeys(plan.Entities[i].Source, plan.Entities[j].Source) < 0
	})
	sort.Slice(plan.Relations, func(i, j int) bool {
		return driver.CompareSourceEdgeKeys(plan.Relations[i].Source, plan.Relations[j].Source) < 0
	})
	sort.Slice(plan.Origins, func(i, j int) bool {
		a, b := plan.Origins[i], plan.Origins[j]
		left, right := []string{a.Project, a.Table, a.ID, a.Path, a.Layer}, []string{b.Project, b.Table, b.ID, b.Path, b.Layer}
		for k := range left {
			if left[k] != right[k] {
				return left[k] < right[k]
			}
		}
		return false
	})
	return plan
}

func b01MountCapturedWorkflow(t *testing.T, p *publicAcceptance, profile driver.RestoreImportProfile, report converterroundtrip.Report) *workflowplugin.Provider {
	t.Helper()
	if report.SourcePlanes == nil || len(report.SourcePlanes.Providers) != 1 || report.Receipt == nil {
		t.Fatal("genuine capture import lacks its joint receipt/provider binding")
	}
	meta, err := p.store.Meta(p.ctx)
	if err != nil {
		t.Fatal("read captured store metadata for durable ontology", err)
	}
	at := meta.Head
	reader, err := p.store.Read(p.ctx, driver.View{Incarnation: meta.Incarnation, At: &at})
	if err != nil {
		t.Fatal("read captured durable ontology snapshot", err)
	}
	ontologyRef := model.RefByID(converterroundtrip.ImportedOntologySourceID, model.Natural64(0))
	rows, readErr := reader.Get(p.ctx, []model.Ref{ontologyRef})
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || len(rows) != 1 || rows[0].Kind != "ontology" || rows[0].Ref.ID != ontologyRef.ID || rows[0].Ref.Branch.String() != ontologyRef.Branch.String() || rows[0].Revision.Validate() != nil {
		t.Fatalf("captured durable ontology contract is absent or malformed: rows=%v read=%v close=%v", rows, readErr, closeErr)
	}
	installed, err := service.DecodeOntologySource(rows[0].Body)
	if err != nil {
		t.Fatal("decode captured durable ontology contract", err)
	}
	snapshot, problems := ontology.Compile(installed.Base, installed.Overlays, installed.Plugins)
	if len(problems) != 0 {
		t.Fatalf("compile captured durable ontology contract: %+v", problems)
	}
	if _, ok := snapshot.Effective(b01CaptureProject).Status("open"); !ok {
		t.Fatal("captured durable ontology contract omits native task/open status")
	}
	p.onto = service.NewOntology(snapshot.Effective)
	writer := pipeline.New(p.store, p.lease, pipeline.WithOntology(p.onto), pipeline.WithClock(p.clock.Now))
	p.svc = service.New(p.store, p.lease, writer, service.WithOntology(p.onto), service.WithClock(p.clock.Now))
	capability, err := driver.RequireWorkflowCheckpointStore(p.store, p.lease)
	if err != nil {
		t.Fatal(err)
	}
	state, err := pluginhost.NewSourceRuntimeState(p.ctx, capability, p.lease, report.SourcePlanes.Providers[0])
	if err != nil {
		t.Fatal(err)
	}
	marker, ok := state.(interface{ Persistent() bool })
	wantLifetime := driver.CheckpointLifetimeRestart
	if p.engine == "memory" {
		wantLifetime = driver.CheckpointLifetimeProcess
	}
	if !ok || marker.Persistent() != (p.engine != "memory") || capability.WorkflowCheckpointLifetime() != wantLifetime {
		t.Fatal("capture import changed actual memory/SQL lifetime metadata")
	}
	provider, err := workflowplugin.NewSourceRuntime(state, report.SourcePlanes.Providers[0])
	if err != nil {
		t.Fatal(err)
	}
	provider = provider.WithClock(p.clock.Now)
	if err := p.svc.InstallSourceWorkflow(p.ctx, provider, profile, *report.SourcePlanes, *report.Receipt); err != nil {
		t.Fatal(err)
	}
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, p.svc); err != nil {
		t.Fatal(err)
	}
	cursorKey := sha256.Sum256([]byte("b01-ready-captured-stable-key"))
	p.server, err = rpc.New(registry, rpc.Config{DefaultProject: b01CaptureProject, Incarnation: p.meta(t).Incarnation, CursorKey: cursorKey[:]})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("B01_CAPTURE_RUNTIME engine=%s lifetime=%s persistent=%t source=%s", p.engine, wantLifetime, marker.Persistent(), profile.ArchiveDigest)
	return provider
}

func b01CaptureProfile(archive *dumpformat.Archive, when time.Time) driver.RestoreImportProfile {
	return driver.RestoreImportProfile{
		Admission:     driver.RestoreImportAdmission{Mode: "anonymous-local", Actor: model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, Authority: "trusted-human-presence", AdmissionDigest: b01CaptureHash([]byte("B01 public native ready capture host admission/1"))},
		SourceStoreID: driver.StoreID(archive.Manifest().StoreID), Method: "offline.restore.import", IdempotencyKey: fixtureKey("b01-captured-import"), ArchiveDigest: archive.SHA256(),
		HistoryMode: driver.RestoreHistoryModeSnapshot, HistoryTime: when.UTC().Format(time.RFC3339Nano), NativeDeltaHistory: &driver.NativeDeltaHistoryPolicy{Format: driver.NativeDeltaHistoryPolicyFormat},
	}
}

// The disposable host and its public workflow use the anonymous principal.
// Its explicit local admission must not claim an authenticated restore-admin
// actor. Structural binding here does not establish host authentication.
func TestB01CapturedImportAdmissionContract(t *testing.T) {
	in := b01ReadCapturedReady(t, b01CompleteReadyFixture, b01CompleteReadyFixtureSHA256)
	p := newPublicAcceptance(t, "memory", false)
	plan := b01CapturePlan(t, p, in.Archive)
	profile := b01CaptureProfile(in.Archive, p.clock.Now())
	want := driver.RestoreImportAdmission{
		Mode: "anonymous-local", Actor: model.PrincipalRef{Kind: "anonymous", ID: "anonymous"},
		Authority: "trusted-human-presence", AdmissionDigest: b01CaptureHash([]byte("B01 public native ready capture host admission/1")),
	}
	before := p.state(t)
	t.Run("authentic-capture-local-anonymous-binding", func(t *testing.T) {
		bound, err := converterroundtrip.BindNativeCaptureSetImport(p.ctx, in.Archive, profile, plan)
		if err != nil || !reflect.DeepEqual(bound.Admission, want) || bound.SourcePlanes == nil || bound.DerivedDigest == "" || bound.ArchiveDigest != profile.ArchiveDigest {
			t.Fatal("authentic capture lost its explicit local admission or checked source binding", err)
		}
		if request, err := bound.RequestHash(); err != nil || len(request) != 64 || bound.AgentLabel() != "" {
			t.Fatal("checked local admission lost request/scope binding", err)
		}
	})
	for _, negative := range []struct {
		name   string
		mutate func(*driver.RestoreImportAdmission)
		want   string
	}{
		{"original-authenticated-anonymous-mismatch", func(a *driver.RestoreImportAdmission) { a.Mode, a.Authority = "authenticated", "restore-admin" }, "import admission mode/actor/authority mismatch"},
		{"local-restore-admin-authority", func(a *driver.RestoreImportAdmission) { a.Authority = "restore-admin" }, "import admission mode/actor/authority mismatch"},
		{"local-user-actor", func(a *driver.RestoreImportAdmission) {
			a.Actor = model.PrincipalRef{Kind: "user", ID: "restore-admin"}
		}, "import admission mode/actor/authority mismatch"},
		{"authenticated-local-authority", func(a *driver.RestoreImportAdmission) {
			a.Mode = "authenticated"
			a.Actor = model.PrincipalRef{Kind: "user", ID: "restore-admin"}
		}, "import admission mode/actor/authority mismatch"},
		{"unknown-mode", func(a *driver.RestoreImportAdmission) { a.Mode = "unknown" }, "import admission mode/actor/authority mismatch"},
		{"missing-authority", func(a *driver.RestoreImportAdmission) { a.Authority = "" }, "import admission mode/actor/authority mismatch"},
		{"invalid-local-scope", func(a *driver.RestoreImportAdmission) { a.Scope = " noncanonical-scope" }, "import admission scope is not the normalized G4 agent label"},
		{"missing-admission-digest", func(a *driver.RestoreImportAdmission) { a.AdmissionDigest = "" }, "invalid prepared import binding"},
	} {
		t.Run(negative.name, func(t *testing.T) {
			bad := profile
			negative.mutate(&bad.Admission)
			bound, err := converterroundtrip.BindNativeCaptureSetImport(p.ctx, in.Archive, bad, plan)
			if err == nil || err.Error() != negative.want || !reflect.DeepEqual(bound, driver.RestoreImportProfile{}) {
				t.Fatal("invalid admission did not refuse atomically", bound.Admission, err)
			}
			if !reflect.DeepEqual(before, p.state(t)) || !reflect.DeepEqual(profile.Admission, want) {
				t.Fatal("refused admission mutated target state or the original host request")
			}
		})
	}
	if !reflect.DeepEqual(before, p.state(t)) {
		t.Fatal("admission preparation wrote the public target before import")
	}
}
