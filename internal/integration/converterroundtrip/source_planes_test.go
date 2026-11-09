package converterroundtrip

import (
	"archive/tar"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// Disposable admitted input, not a native producer recording. The complete
// physical active row collides with a durable ID; cross-plane edges share an
// ID containing NUL, so neither a naked key nor delimiter framing can work.
func jointSourceFixture(t *testing.T) []byte {
	t.Helper()
	base := admittedFixtureBytes(t)
	return rewriteData(t, func(members map[string][]byte) {
		r := tar.NewReader(bytes.NewReader(base))
		for {
			h, err := r.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			members[h.Name], err = io.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}
		}
		var m dumpformat.Manifest
		decode(t, members["manifest.json"], &m)
		m.SourcePlaneAddressContract = dumpformat.SourcePlaneAddressContract
		rows := map[string][]dumpformat.Record{}
		for _, kind := range m.Kinds {
			for _, line := range bytes.Split(bytes.TrimSpace(members[kind.File]), []byte{'\n'}) {
				if len(line) == 0 {
					continue
				}
				var row dumpformat.Record
				decode(t, line, &row)
				rows[kind.Name] = append(rows[kind.Name], row)
			}
		}
		for i := range rows["bead"] {
			fields := rows["bead"][i].Source.Fields
			fields["storage_class"] = "versioned"
			fields["ephemeral"], fields["no_history"] = json.Number("0"), json.Number("0")
			if rows["bead"][i].ID == "demo-7" {
				for field, value := range (dumpformat.Row{"description": "explicit durable description", "priority": json.Number("3"), "metadata": ` {"durable_only":9007199254740993,"decimal":1.0,"null":null} `, "created_at": "2026-01-01T00:00:00Z"}) {
					fields[field], rows["bead"][i].Body[field] = value, value
				}
			}
		}
		// The declared empty project now has an explicit durable endpoint. A
		// cross-project both-bead edge must belong to core exactly once.
		other := rows["bead"][0]
		other.ID, other.Project = "empty-1", "empty"
		other.Source.Fields = cloneRow(other.Source.Fields)
		other.Source.Fields["id"] = other.ID
		other.Body = cloneRow(other.Body)
		rows["bead"] = append(rows["bead"], other)
		for i := range m.Projects {
			if m.Projects[i].Prefix == "empty" {
				m.Projects[i].LastID = 1
			}
		}
		crossFields := dumpformat.Row{"id": "cross-project-block", "issue_id": "demo-7", "depends_on_issue_id": "empty-1", dumpformat.EphemeralDependencyTargetField: nil, "depends_on_external": nil, "type": "blocks", "created_at": "2026-11-01T00:00:00Z", "created_by": "raw author", "metadata": `{}`, "thread_id": " raw cross-project thread "}
		rows["edge"] = append(rows["edge"], dumpformat.Record{Kind: "edge", ID: "cross-project-block", Project: "demo", Source: dumpformat.Provenance{Table: dumpformat.DurableDependencyTable, Fields: crossFields}, Body: dumpformat.Row{"from": "demo-7", "to": "empty-1", "target_kind": "depends_on_issue_id", "type": "blocks", "metadata": crossFields["metadata"], "thread_id": crossFields["thread_id"]}})
		fields := cloneRow(rows["bead"][0].Source.Fields)
		fields["id"], fields["title"], fields["status"] = "demo-7", "Exact active collision", "open"
		fields["ephemeral"], fields["no_history"], fields["storage_class"] = json.Number("1"), json.Number("1"), "ephemeral"
		fields["is_blocked"], fields["row_lock"] = nil, nil
		fields["metadata"] = ` {"wide":9007199254740993,"n":1.0,"null":null} `
		addArchive := func(table string, fields dumpformat.Row) {
			rows["beads-archive"] = append(rows["beads-archive"], dumpformat.Record{Kind: "beads-archive", ID: table + "/" + digestBytes(encode(t, fields)), Source: dumpformat.Provenance{Table: table, Fields: fields}, Body: dumpformat.Row{"read_only": true, "redactable": true, "history_verified": false, "attribution_verified": false}})
		}
		addArchive(dumpformat.EphemeralEntityTable, fields)
		addArchive(dumpformat.EphemeralCommentTable, dumpformat.Row{"id": "raw-active-comment", "issue_id": "demo-7", "author": "unverified source author", "text": "raw active comment", "created_at": "2026-11-01T00:00:00Z"})
		addArchive(dumpformat.EphemeralLabelTable, dumpformat.Row{"issue_id": "demo-7", "label": "active-label"})
		addArchive(dumpformat.EphemeralChildCounterTable, dumpformat.Row{"parent_id": "demo-7", "last_child": json.Number("7")})
		// Preserve the original durable blocking edge, then add two inert
		// related edges, with independent physical identities and raw metadata.
		for _, table := range []string{dumpformat.DurableDependencyTable, dumpformat.EphemeralDependencyTable} {
			arm, target := "depends_on_issue_id", "demo-8"
			if table == dumpformat.DurableDependencyTable {
				arm, target = dumpformat.EphemeralDependencyTargetField, "demo-7"
			}
			fields := dumpformat.Row{"id": "equal\x00edge", "issue_id": "demo-7", "depends_on_issue_id": nil, dumpformat.EphemeralDependencyTargetField: nil, "depends_on_external": nil, "type": "related", "created_at": "2026-11-01T00:00:00Z", "created_by": "raw author", "metadata": ` {"wide":9007199254740993} `, "thread_id": " raw thread "}
			fields[arm] = target
			rows["edge"] = append(rows["edge"], dumpformat.Record{Kind: "edge", ID: "equal\x00edge", Project: "demo", Source: dumpformat.Provenance{Table: table, Fields: fields}, Body: dumpformat.Row{"from": "demo-7", "to": target, "target_kind": arm, "type": "related", "metadata": fields["metadata"], "thread_id": fields["thread_id"]}})
		}
		counts := map[string]int{}
		for i, kind := range m.Kinds {
			sort.Slice(rows[kind.Name], func(i, j int) bool {
				a, b := rows[kind.Name][i], rows[kind.Name][j]
				if kind.Name == "edge" {
					return driver.CompareSourceEdgeKeys(driver.SourceEdgeKey{Project: a.Project, Table: a.Source.Table, ID: a.ID}, driver.SourceEdgeKey{Project: b.Project, Table: b.Source.Table, ID: b.ID}) < 0
				}
				if a.Project != b.Project {
					return a.Project < b.Project
				}
				return a.ID < b.ID
			})
			var data []byte
			for _, row := range rows[kind.Name] {
				data = append(data, encode(t, row)...)
				counts[row.Source.Table]++
				if row.Kind == "bead" {
					counts["labels"] += len(row.Body["labels"].([]any))
				}
			}
			members[kind.File] = data
			m.Kinds[i].Count = len(rows[kind.Name])
			if kind.Name == "edge" {
				m.Kinds[i].Version = dumpformat.EdgeVersionLocalOpaque
			}
		}
		for i, d := range m.Sources {
			if count, ok := counts[d.Family]; ok {
				m.Sources[i].Rows = count
			}
			switch d.Family {
			case "issues.row_lock", "issues.is_blocked":
				m.Sources[i].Rows = counts[dumpformat.DurableEntityTable]
			case dumpformat.EphemeralEntityTable, dumpformat.EphemeralDependencyTable, dumpformat.EphemeralCommentTable, dumpformat.EphemeralLabelTable, dumpformat.EphemeralEventTable, dumpformat.EphemeralChildCounterTable, "custom_statuses", "custom_types":
				m.Sources[i].Action = "archive"
			}
		}
		members["manifest.json"] = encode(t, m)
	})
}

func jointCommitment(data []byte, presence string) driver.SourcePlaneBytesV1 {
	return driver.SourcePlaneBytesV1{Presence: presence, Size: uint64(len(data)), SHA256: digestBytes(data)}
}

func jointSourceProject(t *testing.T, id string, projects []dumpformat.Project) string {
	t.Helper()
	for _, project := range projects {
		if strings.HasPrefix(id, project.Prefix+"-") {
			return project.Prefix
		}
	}
	t.Fatalf("fixture source ID %q lacks its declared project", id)
	return ""
}

func jointFields(t *testing.T, record driver.SourcePlaneRecordV1) []driver.SourcePlaneFieldV1 {
	t.Helper()
	var wire struct {
		Source struct {
			Fields map[string]json.RawMessage `json:"fields"`
		} `json:"source"`
	}
	decode(t, record.Raw, &wire)
	var result []driver.SourcePlaneFieldV1
	for key, raw := range wire.Source.Fields {
		presence := "null"
		switch bytes.TrimSpace(raw)[0] {
		case '{':
			presence = "object"
		case '[':
			presence = "array"
		case '"':
			presence = "text"
			var text string
			if err := json.Unmarshal(raw, &text); err != nil {
				t.Fatal(err)
			}
			raw = []byte(text)
		case 't', 'f':
			presence = "boolean"
		case 'n':
		default:
			presence = "number"
		}
		result = append(result, driver.SourcePlaneFieldV1{Name: key, Value: jointCommitment(raw, presence)})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

func jointProfile(t *testing.T, data []byte, store driver.Store, lease driver.ActiveLease) driver.RestoreImportProfile {
	t.Helper()
	return jointProfileWithPlan(t, data, store, lease, nil)
}

func jointProfileWithPlan(t *testing.T, data []byte, store driver.Store, lease driver.ActiveLease, adjust func(*driver.SourcePlanePlanV1)) driver.RestoreImportProfile {
	t.Helper()
	p, plan := jointUnboundProfileWithPlan(t, data, store, lease, adjust)
	p, err := BindSourceImport(context.Background(), bytes.NewReader(data), p, plan)
	if err != nil {
		t.Fatal("complete explicit preparation", err)
	}
	return p
}

func jointUnboundProfileWithPlan(t *testing.T, data []byte, store driver.Store, lease driver.ActiveLease, adjust func(*driver.SourcePlanePlanV1)) (driver.RestoreImportProfile, driver.SourcePlanePlanV1) {
	t.Helper()
	ctx := context.Background()
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal("joint wire precondition", err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cap, err := driver.RequireWorkflowCheckpointStore(store, lease)
	if err != nil {
		t.Fatal(err)
	}
	actor := model.PrincipalRef{Kind: "user", ID: "restore-admin"}
	principal, _ := json.Marshal(actor)
	active := driver.SourceEntityKey{Project: "demo", Plane: dumpformat.EphemeralEntityTable, ID: "demo-7"}
	plan := driver.SourcePlanePlanV1{Version: driver.SourcePlanePlanVersionV1, AddressContract: driver.SourcePlaneAddressContractV1, SourceStoreID: archive.Manifest().StoreID, ArchiveSHA256: archive.SHA256(), Target: driver.SourcePlaneTargetV1{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, RequireEmpty: true, Lifetime: cap.WorkflowCheckpointLifetime(), RuntimeContract: driver.SourcePlaneRuntimeContractV1}, Principals: []driver.SourcePlanePrincipalV1{{Key: "owner", Source: "explicit fixture owner", Authority: "trusted-explicit/1", PrincipalJSON: principal}}, Runs: []driver.SourcePlaneRunV1{{Project: "demo", Key: "explicit", TargetID: "workflow:run/restored", Template: "explicit fixture", Schema: workflowplugin.SourceStateSchema, Mode: "ephemeral", State: "active", PrincipalKey: "owner", Members: []driver.SourceEntityKey{active}}}, Providers: []driver.SourcePlaneProviderV1{{Alias: "workflow", Instance: "default", Namespace: "plugin/workflow/default/durable", Schema: workflowplugin.SourceStateSchema, Codec: "workflow/json/1", RuntimeContract: driver.SourcePlaneRuntimeContractV1, Lifetime: cap.WorkflowCheckpointLifetime(), OriginAdapter: "beads-workflow-origins/1"}}}
	plan.Dispositions = []driver.SourcePlaneDispositionV1{{Key: "active", SourceType: "task", StorageClass: "ephemeral", Ephemeral: true, NoHistory: true, SinkKind: "workflow:item", Schema: workflowplugin.SourceStateSchema, Lifetime: string(cap.WorkflowCheckpointLifetime()), Statuses: []driver.SourcePlaneStringPairV1{{From: "open", To: "open"}}, RefusalPolicy: "exact-only"}, {Key: "durable", SourceType: "task", StorageClass: "versioned", SinkKind: "bead", Schema: "bead/1", Lifetime: "core", Statuses: []driver.SourcePlaneStringPairV1{{From: "closed", To: "done"}, {From: "in_progress", To: "in_progress"}, {From: "open", To: "todo"}}, RefusalPolicy: "exact-only"}}
	raw, err := archive.SourcePlaneRecords()
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range raw {
		census := jointFields(t, record)
		origin := jointCommitment(record.Raw, "object")
		var row dumpformat.Record
		decode(t, record.Raw, &row)
		if record.Table == dumpformat.DurableEntityTable || record.Table == dumpformat.EphemeralEntityTable {
			key := driver.SourceEntityKey{Plane: record.Table, ID: stringValue(row.Source.Fields["id"])}
			key.Project = jointSourceProject(t, key.ID, archive.Manifest().Projects)
			route := driver.SourcePlaneRouteV1{Project: key.Project, SourceID: key.ID, DurableTargetID: key.ID, Preference: "active-first"}
			e := driver.SourcePlaneEntityV1{Source: key, TargetKind: "bead", TargetID: key.ID, DispositionKey: "durable", PrincipalKey: "owner", Route: route, Origin: origin, Fields: census}
			index := 1
			if key.ID == "demo-7" {
				e.Route.ActiveTargetID = "workflow:item/restored"
			}
			if record.Table == dumpformat.EphemeralEntityTable {
				index = 0
				e.TargetKind = "workflow:item"
				e.TargetID = "workflow:item/restored"
				e.DispositionKey = "active"
				e.RunKey = "explicit"
			}
			if plan.Dispositions[index].Fields == nil {
				for _, field := range census {
					rule := driver.SourcePlaneFieldRuleV1{Source: field.Name, Disposition: "origin-only"}
					switch field.Name {
					case "id", "title", "status", "metadata", "created_at":
						rule.Target, rule.Disposition = field.Name, "exact-map"
					case "issue_type":
						rule.Target, rule.Disposition = "type", "exact-map"
					case "description", "priority":
						if index == 1 {
							rule.Target, rule.Disposition = field.Name, "exact-map"
						}
					}
					plan.Dispositions[index].Fields = append(plan.Dispositions[index].Fields, rule)
				}
			}
			plan.Entities = append(plan.Entities, e)
			continue
		}
		if record.Kind == "edge" {
			plane := dumpformat.DurableEntityTable
			if record.Table == dumpformat.EphemeralDependencyTable {
				plane = dumpformat.EphemeralEntityTable
			}
			from := driver.SourceEntityKey{Project: record.Project, Plane: plane, ID: stringValue(row.Source.Fields["issue_id"])}
			plane = dumpformat.DurableEntityTable
			if row.Body["target_kind"] == dumpformat.EphemeralDependencyTargetField {
				plane = dumpformat.EphemeralEntityTable
			}
			to := driver.SourceEntityKey{Plane: plane, ID: stringValue(row.Body["to"])}
			to.Project = jointSourceProject(t, to.ID, archive.Manifest().Projects)
			predicate, err := dumpformat.SourceEdgeMetadata(3, record.ID, stringValue(row.Body["type"]), row.Source.Fields["metadata"])
			if err != nil {
				t.Fatal(err)
			}
			pred, _ := json.Marshal(predicate)
			plan.Relations = append(plan.Relations, driver.SourcePlaneRelationV1{Source: driver.SourceEdgeKey{Project: record.Project, Table: record.Table, ID: record.ID}, From: from, To: &to, Kind: stringValue(row.Body["type"]), PredicateContract: "beads-typed-metadata/1", PredicateJSON: pred, Origin: origin, Fields: census})
			continue
		}
		o := driver.SourcePlaneOriginV1{Project: record.Project, Table: record.Table, ID: record.ID, Action: "inert-origin", Raw: origin, Fields: census, Parsed: driver.SourcePlaneBytesV1{Presence: "absent"}}
		switch record.Table {
		case dumpformat.EphemeralLabelTable, dumpformat.EphemeralCommentTable, dumpformat.EphemeralChildCounterTable:
			o.Project = "demo"
			o.Owner = &active
			o.Action = "exact-map"
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
	if adjust != nil {
		adjust(&plan)
	}
	p := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: actor, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: driver.StoreID(archive.Manifest().StoreID), Method: "offline.restore.import", IdempotencyKey: strings.Repeat("b", 32), ArchiveDigest: archive.SHA256(), HistoryMode: driver.RestoreHistoryModeSnapshot}
	return p, plan
}

func TestJointSourceImportPublishesAndRecoversAcrossThreeStores(t *testing.T) {
	data := jointSourceFixture(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			store, lease, reopen := openIdentityTarget(t, engine)
			before := reverseState(t, store)
			p := jointProfile(t, data, store, lease)
			report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, p)
			if err != nil {
				t.Fatal(err)
			}
			if report.SourcePlanes == nil || report.Receipt == nil || report.Restored.Incarnation == before.Meta.Incarnation || report.Restored.StoreID != p.SourceStoreID || report.Restored.Head.String() != "1" {
				t.Fatal("missing joint publication", report)
			}
			requireJointCrossProjectCoreEdge(t, store, report)
			binding := report.SourcePlanes.Providers[0]
			cap, err := driver.RequireWorkflowCheckpointStore(store, lease)
			if err != nil {
				t.Fatal(err)
			}
			cp, err := cap.LoadCheckpoint(ctx, binding.Provider.Namespace)
			if err != nil || driver.ValidateSourcePlaneInitialCheckpoint(cp, binding) != nil {
				t.Fatal("initial checkpoint mismatch", err)
			}
			state, err := pluginhost.NewSourceRuntimeState(ctx, cap, lease, binding)
			if err != nil {
				t.Fatal(err)
			}
			provider, err := workflowplugin.NewSourceRuntime(state, binding)
			if err != nil {
				t.Fatal(err)
			}
			if state.(interface{ Persistent() bool }).Persistent() != (engine != "memory") {
				t.Fatal("false lifetime promise")
			}
			ref, found, err := provider.ResolveSourceRef(ctx, p.Admission.Actor, "demo", "demo-7", model.WriteMeta{})
			if err != nil || !found || ref.ID != "workflow:item/restored" {
				t.Fatal("active-first route", ref, found, err)
			}
			original, err := provider.SourceRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			want, err := report.Archive.SourcePlaneRecords()
			if err != nil {
				t.Fatal(err)
			}
			sort.Slice(want, func(i, j int) bool { return hashBytes(want[i].Raw) < hashBytes(want[j].Raw) })
			sort.Slice(original, func(i, j int) bool { return hashBytes(original[i].Raw) < hashBytes(original[j].Raw) })
			if !reflect.DeepEqual(original, want) {
				t.Fatal("raw provider origins differ from admitted bytes")
			}
			core := savedLabelImport(t, store, report.Archive, p, "")
			pinned, err := cap.ReadSourcePlaneSnapshot(ctx, driver.View{Incarnation: report.Restored.Incarnation}, []string{binding.Provider.Namespace})
			if err != nil {
				t.Fatal(err)
			}
			defer pinned.Close()
			pinnedCore, err := pinned.Get(ctx, []model.Ref{model.RefByID("demo-7", model.Natural64(0))})
			if err != nil || len(pinnedCore) != 1 {
				t.Fatal("pinned durable collision", err)
			}
			_, err = provider.Update(ctx, p.Admission.Actor, ref, map[string]any{"title": "legitimate later update"}, model.WriteMeta{Project: "demo", Method: "bead.update", IdempotencyKey: "joint-later-active-update"})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(core, savedLabelImport(t, store, report.Archive, p, "")) {
				t.Fatal("active update touched core/history/blobs/original receipt")
			}
			later, err := cap.LoadCheckpoint(ctx, binding.Provider.Namespace)
			if err != nil || later.Generation != cp.Generation+1 {
				t.Fatal("missing later generation", err)
			}
			pinnedCP, err := pinned.SourcePlaneCheckpoint(binding.Provider.Namespace)
			pinnedAgain, readErr := pinned.Get(ctx, []model.Ref{model.RefByID("demo-7", model.Natural64(0))})
			if err != nil || readErr != nil || !reflect.DeepEqual(pinnedCP, cp) || !reflect.DeepEqual(pinnedAgain, pinnedCore) || pinned.Snapshot().At.String() != "1" {
				t.Fatal("provider mutation changed a pinned joint observation", err, readErr)
			}
			if err := pinned.Close(); err != nil {
				t.Fatal(err)
			}
			check := Verification{ArchiveSHA256: p.ArchiveDigest, DerivedSHA256: p.DerivedDigest}
			for _, retry := range []driver.RestoreImportProfile{p, p} {
				recovered, err := RecoverImport(ctx, store, retry, check)
				if err != nil || !reflect.DeepEqual(recovered.Receipt, report.Receipt) || !reflect.DeepEqual(recovered.SourcePlanes, report.SourcePlanes) {
					t.Fatal("original joint receipt recovery", err)
				}
			}
			changed := p
			changed.Admission.AdmissionDigest = strings.Repeat("f", 64)
			if _, err := RecoverImport(ctx, store, changed, check); !errors.Is(err, driver.ErrConflict) {
				t.Fatal("changed profile recovered", err)
			}
			changed = p
			differentPlan, err := p.SourcePlanes.Plan()
			if err != nil {
				t.Fatal(err)
			}
			differentPlan.Principals[0].Source = "different declared source attribution"
			differentBinding, err := driver.NewSourcePlaneImportBindingV1(differentPlan)
			if err != nil {
				t.Fatal(err)
			}
			changed.SourcePlanes = &differentBinding
			if _, err := RecoverImport(ctx, store, changed, check); !errors.Is(err, driver.ErrConflict) {
				t.Fatal("different exact plan recovered original receipt", err)
			}
			afterRecovery, err := cap.LoadCheckpoint(ctx, binding.Provider.Namespace)
			if err != nil || !reflect.DeepEqual(later, afterRecovery) {
				t.Fatal("recovery republished provider", err)
			}
			store, lease = reopen()
			recovered, err := RecoverImport(ctx, store, p, check)
			if err != nil || !reflect.DeepEqual(report.Receipt, recovered.Receipt) {
				t.Fatal("reopened recovery", err)
			}
			cap, err = driver.RequireWorkflowCheckpointStore(store, lease)
			if err != nil {
				t.Fatal(err)
			}
			state, err = pluginhost.NewSourceRuntimeState(ctx, cap, lease, binding)
			if err != nil {
				t.Fatal(err)
			}
			provider, err = workflowplugin.NewSourceRuntime(state, binding)
			if err != nil {
				t.Fatal(err)
			}
			item, err := provider.Get(ctx, p.Admission.Actor, ref, model.WriteMeta{Project: "demo"})
			if err != nil || item.Title != "legitimate later update" {
				t.Fatal("reopened live image", item, err)
			}
		})
	}
}

type incompleteSourceStore struct {
	driver.Store
	driver.WorkflowCheckpointStore
	fault string
}

func (s *incompleteSourceStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return &incompleteSourceTx{WriteTx: tx, fault: s.fault}, nil
}

type incompleteSourceTx struct {
	driver.WriteTx
	fault string
}

func (t *incompleteSourceTx) PrepareSourcePlaneImport(ctx context.Context, p driver.RestoreImportProfile) (driver.Meta, error) {
	return t.WriteTx.(driver.SourcePlaneRestoreTx).PrepareSourcePlaneImport(ctx, p)
}
func (t *incompleteSourceTx) BindSourcePlaneImport(ctx context.Context, b driver.SourcePlaneCommitBindingV1) error {
	if t.fault == "missing-binding" {
		return nil
	}
	return t.WriteTx.(driver.SourcePlaneRestoreTx).BindSourcePlaneImport(ctx, b)
}
func (t *incompleteSourceTx) RestoreAllocators(ctx context.Context, a []driver.OfflineAllocator) error {
	return t.WriteTx.(driver.AllocatorContinuation).RestoreAllocators(ctx, a)
}
func (t *incompleteSourceTx) StageCheckpoint(ctx context.Context, key string, expected uint64, payload []byte) error {
	if t.fault == "missing-checkpoint" {
		return nil
	}
	if t.fault == "changed-envelope" {
		payload = append(bytes.Clone(payload), ' ')
	}
	return t.WriteTx.(driver.CheckpointWriteTx).StageCheckpoint(ctx, key, expected, payload)
}
func (t *incompleteSourceTx) Commit(ctx context.Context, h driver.ChangesetHeader) (model.Changeset, error) {
	if t.fault == "missing-receipt" {
		var results []model.OpResult
		for _, result := range h.Response.Results {
			if result.Ref.ID != driver.SourcePlaneCommitRecordID {
				results = append(results, result)
			}
		}
		h.Response.Results = results
	}
	return t.WriteTx.Commit(ctx, h)
}

// Includes every application table, locks, leases, blobs, versions, caches,
// checkpoint generations and receipts. The PostgreSQL placeholder lock census
// is established explicitly before the baseline; nothing is excluded later.
func jointPhysicalSQL(t *testing.T, db *sql.DB, engine string) map[string][]string {
	t.Helper()
	ctx := context.Background()
	catalog := `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`
	prefix := ""
	if engine == "postgres" {
		catalog = `SELECT tablename FROM pg_tables WHERE schemaname='plug_postgres' ORDER BY tablename`
		prefix = "plug_postgres."
	}
	names, err := db.QueryContext(ctx, catalog)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for names.Next() {
		var table string
		if err := names.Scan(&table); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, table)
	}
	if err := names.Err(); err != nil {
		t.Fatal(err)
	}
	_ = names.Close()
	image := map[string][]string{}
	for _, table := range tables {
		rows, err := db.QueryContext(ctx, `SELECT * FROM `+prefix+`"`+strings.ReplaceAll(table, `"`, `""`)+`"`)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		image[table] = nil
		for rows.Next() {
			values := make([]any, len(columns))
			ptrs := make([]any, len(columns))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(values)
			if err != nil {
				t.Fatal(err)
			}
			image[table] = append(image[table], string(data))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()
		sort.Strings(image[table])
	}
	return image
}

func TestJointSourceImportRefusesIncompletePublicationWithoutWrites(t *testing.T) {
	data := jointSourceFixture(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			var dsn string
			store, lease, _ := openIdentityTarget(t, engine, func(value string) { dsn = value })
			p := jointProfile(t, data, store, lease)
			cap, err := driver.RequireWorkflowCheckpointStore(store, lease)
			if err != nil {
				t.Fatal(err)
			}
			archive, err := dumpformat.Read(bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			core, err := buildPlanWithProfile(archive, &p, time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC))
			if err != nil {
				t.Fatal(err)
			}
			core.records = append(core.records, restoreRecord{ref: model.RefByID(driver.SourcePlaneCommitRecordID, model.Natural64(0)), kind: "instance_config"})
			prime, err := store.Write(ctx, lease)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := prime.Lock(ctx, lockRequests(core)); err != nil {
				t.Fatal(err)
			}
			if err := prime.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			_ = prime.Close()
			var db *sql.DB
			if engine != "memory" {
				dialect := "sqlite"
				if engine == "postgres" {
					dialect = "pgx"
				}
				db, err = sql.Open(dialect, dsn)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
			}
			before := reverseState(t, store)
			beforeCP, err := cap.LoadCheckpoint(ctx, "plugin/workflow/default/durable")
			if err != nil {
				t.Fatal(err)
			}
			var physical map[string][]string
			if db != nil {
				physical = jointPhysicalSQL(t, db, engine)
			}
			for _, fault := range []string{"missing-binding", "missing-checkpoint", "changed-envelope", "missing-receipt"} {
				t.Run(fault, func(t *testing.T) {
					broken := &incompleteSourceStore{Store: store, WorkflowCheckpointStore: cap, fault: fault}
					if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), broken, lease, p); !errors.Is(err, driver.ErrUnsupported) {
						t.Fatal("incomplete joint publication accepted or failed at wrong boundary", err)
					}
					cp, err := cap.LoadCheckpoint(ctx, "plugin/workflow/default/durable")
					if err != nil || !reflect.DeepEqual(beforeCP, cp) || !reflect.DeepEqual(before, reverseState(t, store)) {
						t.Fatal("joint refusal published a participant", err)
					}
					if db != nil && !reflect.DeepEqual(physical, jointPhysicalSQL(t, db, engine)) {
						t.Fatal("joint refusal changed complete physical SQL image")
					}
				})
			}
			if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, p); err != nil {
				t.Fatal("valid joint control after refusals", err)
			}
		})
	}
}

func requireJointCrossProjectCoreEdge(t *testing.T, store driver.Store, report Report) {
	t.Helper()
	ctx := context.Background()
	r, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Get(ctx, []model.Ref{model.RefByID("demo-7", model.Natural64(0)), model.RefByID("empty-1", model.Natural64(0))})
	if err != nil || len(rows) != 2 {
		t.Fatal("cross-project endpoint census", err)
	}
	owner := false
	for _, row := range rows {
		doc, err := pipeline.DecodeDocument(row.Body)
		if err != nil {
			t.Fatal(err)
		}
		if doc.ID == "demo-7" {
			owner = true
			if doc.Project != "demo" || len(doc.Links) != 1 || doc.Links[0].Kind != "blocks" || doc.Links[0].To.ID != "empty-1" || doc.Links[0].TargetKind == model.LinkTargetExternal {
				t.Fatalf("cross-project both-bead edge was lost or duplicated: %+v", doc.Links)
			}
		} else if doc.Project != "empty" || doc.Fields["status"] != "todo" {
			t.Fatal("cross-project target identity/status changed", doc)
		}
	}
	states, err := r.DerivedRows(ctx, driver.DerivedReadyState, "")
	if err != nil {
		t.Fatal(err)
	}
	blocked := false
	for _, row := range states {
		if row.Key == "demo-7" {
			var state struct{ Ready, Blocked bool }
			if err := json.Unmarshal(row.Value, &state); err != nil || state.Ready || !state.Blocked {
				t.Fatal("cross-project edge has no live blocking effect", string(row.Value), err)
			}
			blocked = true
		}
	}
	if !owner || !blocked {
		t.Fatal("cross-project owner/readiness missing")
	}
	for _, edge := range report.Archive.Records("edge") {
		if edge.ID != "cross-project-block" {
			continue
		}
		mirror, err := r.Get(ctx, []model.Ref{model.RefByID(sourcePlaneMirrorID(edge, true), model.Natural64(0))})
		if err != nil || len(mirror) != 1 {
			t.Fatal("cross-project exact raw mirror missing", err)
		}
		var body dumpformat.Row
		decode(t, mirror[0].Body, &body)
		if !reflect.DeepEqual(body["source"], map[string]any(edge.Source.Fields)) || body["source_project"] != edge.Project || body["source_id"] != edge.ID || body["source_table"] != edge.Source.Table {
			t.Fatal("cross-project raw identity or TEN physical fields changed")
		}
		return
	}
	t.Fatal("cross-project source edge absent from admitted baseline")
}

type jointTargetAccess struct {
	driver.Store
	metaReads, writes int
}

func (s *jointTargetAccess) Meta(ctx context.Context) (driver.Meta, error) {
	s.metaReads++
	return s.Store.Meta(ctx)
}
func (s *jointTargetAccess) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	s.writes++
	return s.Store.Write(ctx, lease)
}

// Refused imports start from an empty target, so expected source blobs and
// the original import receipt must remain absent. Capture the same logical
// surfaces as the post-import census without assuming publication occurred.
func savedEmptyJointTarget(t *testing.T, store driver.Store, archive *dumpformat.Archive, p driver.RestoreImportProfile) labelImportState {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Query(ctx, driver.Query{Limit: 1000})
	if err != nil || rows.NextCursor != nil || len(rows.Items) == 1000 {
		t.Fatal("incomplete pre-import record census", err)
	}
	state := labelImportState{Meta: meta, Records: rows.Items, Versions: map[string][]driver.Version{}, Derived: map[driver.DerivedTable][]driver.DerivedRow{}, Blobs: map[string][]byte{}}
	for _, row := range rows.Items {
		versions, err := r.Versions(ctx, row.Ref, driver.PageReq{Limit: 1000})
		if err != nil || versions.NextCursor != nil || len(versions.Items) == 1000 {
			t.Fatal("incomplete pre-import version census", row.Ref.ID, err)
		}
		state.Versions[row.Ref.ID] = versions.Items
	}
	var cursor *driver.Cursor
	if !meta.HistoryFloor.IsZero() {
		cursor = &driver.Cursor{Incarnation: meta.Incarnation, Revision: meta.HistoryFloor}
	}
	state.Log, err = r.Changesets(ctx, cursor, 1000)
	if err != nil || len(state.Log) == 1000 {
		t.Fatal("incomplete pre-import changeset census", err)
	}
	for _, table := range []driver.DerivedTable{driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins, driver.DerivedReadyState, driver.DerivedReferenceIndex, driver.DerivedSortKey} {
		state.Derived[table], err = r.DerivedRows(ctx, table, "")
		if err != nil {
			t.Fatal(err)
		}
	}
	hashes := map[string]bool{archive.SHA256(): true}
	for _, file := range archive.Manifest().Files {
		hashes[file.SHA256] = true
	}
	for hash := range hashes {
		data, err := r.Blob(ctx, driver.Hash(hash))
		if !errors.Is(err, driver.ErrNotFound) || len(data) != 0 {
			t.Fatal("source blob present before import", hash, err)
		}
		// A present key with nil bytes explicitly records expected absence.
		state.Blobs[hash] = nil
	}
	principal, err := driver.PrincipalKey(p.Admission.Actor)
	if err != nil {
		t.Fatal(err)
	}
	state.ImportReceipt, err = r.IdempotencyByKey(ctx, principal, p.AgentLabel(), p.Method, p.IdempotencyKey)
	if err != nil || state.ImportReceipt != nil {
		t.Fatal("original import receipt present before publication", err)
	}
	return state
}

func TestJointSourceCoreFieldDispositionsAndCommitmentsAcrossThreeStores(t *testing.T) {
	data := jointSourceFixture(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			for _, mode := range []string{"exact-map", "origin-only"} {
				t.Run(mode, func(t *testing.T) {
					ctx := context.Background()
					var dsn string
					store, lease, _ := openIdentityTarget(t, engine, func(value string) { dsn = value })
					p := jointProfile(t, data, store, lease)
					cap, err := driver.RequireWorkflowCheckpointStore(store, lease)
					if err != nil {
						t.Fatal(err)
					}
					archive, err := dumpformat.Read(bytes.NewReader(data))
					if err != nil {
						t.Fatal(err)
					}
					before := savedEmptyJointTarget(t, store, archive, p)
					checkpoint, err := cap.LoadCheckpoint(ctx, "plugin/workflow/default/durable")
					if err != nil {
						t.Fatal(err)
					}
					var db *sql.DB
					var physical map[string][]string
					if engine != "memory" {
						dialect := "sqlite"
						if engine == "postgres" {
							dialect = "pgx"
						}
						db, err = sql.Open(dialect, dsn)
						if err != nil {
							t.Fatal(err)
						}
						defer db.Close()
						physical = jointPhysicalSQL(t, db, engine)
					}
					// Every mutation starts from a valid complete plan. Refusals
					// must identify the changed field/commitment before target access.
					for _, field := range []string{"description", "priority", "metadata", "created_at", "title", "bad-text-commitment"} {
						t.Run(field, func(t *testing.T) {
							plan, err := p.SourcePlanes.Plan()
							if err != nil {
								t.Fatal(err)
							}
							reason := "core field explicitly refused: " + field
							if field == "bad-text-commitment" {
								reason = "source byte commitment differs"
								for i := range plan.Entities {
									for j := range plan.Entities[i].Fields {
										if plan.Entities[i].Fields[j].Name == "id" {
											plan.Entities[i].Fields[j].Value.SHA256 = strings.Repeat("f", 64)
										}
									}
								}
							} else {
								for i := range plan.Dispositions {
									if plan.Dispositions[i].SinkKind != "bead" {
										continue
									}
									for j := range plan.Dispositions[i].Fields {
										rule := &plan.Dispositions[i].Fields[j]
										if rule.Source == field {
											rule.Disposition, rule.Target = "refuse-required", ""
											if field == "title" {
												rule.Disposition = "origin-only"
												reason = "required core field lacks exact mapping: title"
											}
										}
									}
								}
							}
							binding, err := driver.NewSourcePlaneImportBindingV1(plan)
							if err != nil {
								t.Fatal("negative control lost valid structural baseline", err)
							}
							bad := p
							bad.SourcePlanes = &binding
							target := &jointTargetAccess{Store: store}
							if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), target, lease, bad); !errors.Is(err, driver.ErrUnsupported) || !strings.Contains(err.Error(), reason) {
								t.Fatal("changed plan accepted or refused for unrelated reason", err)
							}
							afterCP, err := cap.LoadCheckpoint(ctx, "plugin/workflow/default/durable")
							if err != nil || target.metaReads != 0 || target.writes != 0 || !reflect.DeepEqual(before, savedEmptyJointTarget(t, store, archive, p)) || !reflect.DeepEqual(checkpoint, afterCP) {
								t.Fatal("plan refusal accessed target or published state", err)
							}
							if db != nil && !reflect.DeepEqual(physical, jointPhysicalSQL(t, db, engine)) {
								t.Fatal("plan refusal changed full physical SQL census")
							}
						})
					}
					if mode == "origin-only" {
						plan, err := p.SourcePlanes.Plan()
						if err != nil {
							t.Fatal(err)
						}
						for i := range plan.Dispositions {
							if plan.Dispositions[i].SinkKind != "bead" {
								continue
							}
							for j := range plan.Dispositions[i].Fields {
								rule := &plan.Dispositions[i].Fields[j]
								switch rule.Source {
								case "description", "priority", "metadata", "created_at":
									rule.Disposition, rule.Target = "origin-only", ""
								}
							}
						}
						plan.Providers[0].LogicalImageSHA256, plan.Providers[0].LogicalImageSize = "", 0
						p.SourcePlanes, p.DerivedDigest = nil, ""
						p, err = BindSourceImport(ctx, bytes.NewReader(data), p, plan)
						if err != nil {
							t.Fatal("origin-only complete preparation", err)
						}
					}
					report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, p)
					if err != nil {
						t.Fatal("valid publication after same-target refusals", err)
					}
					requireJointCrossProjectCoreEdge(t, store, report)
					r, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation})
					if err != nil {
						t.Fatal(err)
					}
					defer r.Close()
					rows, err := r.Get(ctx, []model.Ref{model.RefByID("demo-7", model.Natural64(0))})
					if err != nil || len(rows) != 1 {
						t.Fatal("durable field publication missing", err)
					}
					doc, err := pipeline.DecodeDocument(rows[0].Body)
					if err != nil {
						t.Fatal(err)
					}
					if mode == "origin-only" {
						_, description := doc.Fields["description"]
						_, priority := doc.Fields["priority"]
						_, metadata := doc.Metadata["durable_only"]
						if description || priority || metadata || doc.CreatedAt != nil {
							t.Fatal("origin-only durable value acquired a live slot", doc)
						}
					} else if doc.Fields["description"] != "explicit durable description" || string(encode(t, doc.Fields["priority"])) != "3\n" || string(doc.Metadata["durable_only"]) != "9007199254740993" || doc.CreatedAt == nil || !doc.CreatedAt.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
						t.Fatal("exact-map durable values were lost", doc)
					}
					var raw dumpformat.Row
					decode(t, doc.Metadata["beads.source_fields"], &raw)
					for _, source := range report.Archive.Records("bead") {
						if source.ID == "demo-7" && !reflect.DeepEqual(raw, source.Source.Fields) {
							t.Fatal("disposition changed retained inert physical fields")
						}
					}
					saved, err := r.Blob(ctx, driver.Hash(archive.SHA256()))
					if err != nil || !bytes.Equal(saved, data) {
						t.Fatal("disposition changed retained raw archive", err)
					}
				})
			}
		})
	}
}
