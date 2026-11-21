package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// Declared synthetic full histories exercise publication. They are deliberately
// constructed from the older public fixture and are not current-native proof.
// Both projects use the same custom name, commit IDs and global config key.
func nativeHistoryFixture(t *testing.T, project, category string) []byte {
	t.Helper()
	members := tarMembers(t, projectHistoryArchive(t, project, 0))
	var m dumpformat.Manifest
	decode(t, members["manifest.json"], &m)
	m.SourcePlaneAddressContract = dumpformat.SourcePlaneAddressContract
	m.StatusCategoryContract = dumpformat.StatusCategoryContract
	m.Verification["ontology"] = dumpformat.NativeStatusOntologyVerification
	// Keep the external builtin catalog as explicit fixture data, with every
	// native name/category visible and unchanged.
	catalog, err := os.ReadFile("testdata/native-built-in-statuses.json")
	if err != nil {
		t.Fatal(err)
	}
	decode(t, catalog, &m.Projects[0].Statuses)
	m.Projects[0].Statuses = append(m.Projects[0].Statuses, dumpformat.ProjectStatus{Name: "shared", Category: category})
	value := "shared:" + category
	config := dumpformat.Record{Kind: "config", ID: "config/status.custom", Body: dumpformat.Row{"key": "config/status.custom", "value": value, "classification": "public"}, Source: dumpformat.Provenance{Table: "config", Fields: dumpformat.Row{"key": "status.custom", "value": value}}}
	convert := func(row *dumpformat.Record) {
		if row.Kind != "bead" {
			return
		}
		row.Source.Fields["status"], row.Source.Fields["storage_class"] = "shared", "versioned"
		row.Source.Fields["ephemeral"], row.Source.Fields["no_history"] = json.Number("0"), json.Number("0")
		row.Body["status_category"] = category
		delete(row.Body, "result")
		if category == "done" {
			row.Body["result"] = "success"
		}
	}
	var beads bytes.Buffer
	for _, line := range bytes.Split(bytes.TrimSpace(members["records/bead.ndjson"]), []byte{'\n'}) {
		var row dumpformat.Record
		decode(t, line, &row)
		convert(&row)
		beads.Write(encode(t, row))
	}
	members["records/bead.ndjson"] = beads.Bytes()
	members["records/config.ndjson"] = encode(t, config)
	m.Kinds = append(m.Kinds, dumpformat.Kind{Name: "config", Version: 1, File: "records/config.ndjson", Count: 1})
	sort.Slice(m.Kinds, func(i, j int) bool { return m.Kinds[i].Name < m.Kinds[j].Name })
	for i := range m.Sources {
		if m.Sources[i].Family == "config" {
			m.Sources[i].Rows = 1
		}
		// The source-plane marker archives these families even when empty.
		// Keep the declared disposition consistent with that explicit contract.
		switch m.Sources[i].Family {
		case dumpformat.EphemeralEntityTable, dumpformat.EphemeralDependencyTable,
			dumpformat.EphemeralLabelTable, dumpformat.EphemeralCommentTable,
			dumpformat.EphemeralEventTable, dumpformat.EphemeralChildCounterTable,
			"custom_statuses", "custom_types":
			m.Sources[i].Action = "archive"
		}
	}
	var h dumpformat.SourceHistory
	decode(t, members[dumpformat.SourceHistoryMember], &h)
	for i := range h.Range.Frames {
		for j := range h.Range.Frames[i].Records {
			convert(&h.Range.Frames[i].Records[j])
		}
		h.Range.Frames[i].Records = append(h.Range.Frames[i].Records, config)
	}
	members[dumpformat.SourceHistoryMember] = encode(t, h)
	for path, raw := range members {
		if path != "manifest.json" {
			m.Files[path] = dumpformat.File{SHA256: hashBytes(raw), Size: int64(len(raw))}
		}
	}
	members["manifest.json"] = encode(t, m)
	return writeTarMembers(t, members)
}

func nativeHistoryPlan(t *testing.T, data []byte, store driver.Store, lease driver.ActiveLease) driver.SourcePlanePlanV1 {
	t.Helper()
	a, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal("declared native history", err)
	}
	return nativeHistoryPlanFromArchive(t, a, store, lease)
}

func nativeHistoryPlanFromArchive(t *testing.T, a *dumpformat.Archive, store driver.Store, lease driver.ActiveLease) driver.SourcePlanePlanV1 {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cap, err := driver.RequireWorkflowCheckpointStore(store, lease)
	if err != nil {
		t.Fatal(err)
	}
	principal, _ := json.Marshal(model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})
	plan := driver.SourcePlanePlanV1{Version: driver.SourcePlanePlanVersionV1, AddressContract: driver.SourcePlaneAddressContractV1, SourceStoreID: a.Manifest().StoreID, ArchiveSHA256: a.SHA256(), Target: driver.SourcePlaneTargetV1{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, RequireEmpty: true, Lifetime: cap.WorkflowCheckpointLifetime(), RuntimeContract: driver.SourcePlaneRuntimeContractV1}, Principals: []driver.SourcePlanePrincipalV1{{Key: "owner", Source: "explicit public fixture", Authority: "trusted-explicit/1", PrincipalJSON: principal}}, Providers: []driver.SourcePlaneProviderV1{{Alias: "workflow", Instance: "default", Namespace: "plugin/workflow/default/durable", Schema: workflowplugin.SourceStateSchema, Codec: "workflow/json/1", RuntimeContract: driver.SourcePlaneRuntimeContractV1, Lifetime: cap.WorkflowCheckpointLifetime(), OriginAdapter: "beads-workflow-origins/1"}}}
	disposition := driver.SourcePlaneDispositionV1{Key: "durable", SourceType: "task", StorageClass: "versioned", SinkKind: "bead", Schema: "bead/1", Lifetime: "core", Statuses: []driver.SourcePlaneStringPairV1{{From: "shared", To: "shared"}}, RefusalPolicy: "exact-only"}
	work, release := a.NativeDeltaAccountingScope()
	defer release()
	raw, err := work.SourcePlaneRecords()
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range raw {
		var row dumpformat.Record
		decode(t, record.Raw, &row)
		origin, census := jointCommitment(record.Raw, "object"), jointFields(t, record)
		if row.Kind == "bead" {
			key := driver.SourceEntityKey{Project: row.Project, Plane: row.Source.Table, ID: row.ID}
			plan.Entities = append(plan.Entities, driver.SourcePlaneEntityV1{Source: key, TargetKind: "bead", TargetID: row.ID, DispositionKey: "durable", PrincipalKey: "owner", Route: driver.SourcePlaneRouteV1{Project: row.Project, SourceID: row.ID, DurableTargetID: row.ID, Preference: "active-first"}, Origin: origin, Fields: census})
			if disposition.Fields == nil {
				for _, field := range census {
					rule := driver.SourcePlaneFieldRuleV1{Source: field.Name, Disposition: "origin-only"}
					switch field.Name {
					case "id", "title", "status", "metadata", "created_at", "priority", "description", "closed_at":
						rule.Target, rule.Disposition = field.Name, "exact-map"
					case "issue_type":
						rule.Target, rule.Disposition = "type", "exact-map"
					}
					disposition.Fields = append(disposition.Fields, rule)
				}
			}
		} else {
			project := row.Project
			if row.Source.Identity != nil {
				project = row.Source.Identity.Project
			}
			plan.Origins = append(plan.Origins, driver.SourcePlaneOriginV1{Project: project, Table: row.Source.Table, ID: row.ID, Action: "inert-origin", Raw: origin, Fields: census, Parsed: driver.SourcePlaneBytesV1{Presence: "absent"}})
		}
	}
	plan.Dispositions = []driver.SourcePlaneDispositionV1{disposition}
	sidecars, err := a.SourcePlaneSidecarOrigins()
	if err != nil {
		t.Fatal(err)
	}
	plan.Origins = append(plan.Origins, sidecars...)
	sort.Slice(plan.Entities, func(i, j int) bool {
		return driver.CompareSourceEntityKeys(plan.Entities[i].Source, plan.Entities[j].Source) < 0
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

func mergedNativeHistory(t *testing.T) []byte {
	t.Helper()
	left, right := nativeHistoryFixture(t, "pl", "active"), nativeHistoryFixture(t, "qa", "done")
	data, err := dumpformat.Merge(bytes.NewReader(left), bytes.NewReader(right))
	if err != nil {
		t.Fatal("synthetic native merge", err)
	}
	return data
}

func TestFullSourceHistoryPublishesOriginalProjectCatalogsAndRecovers(t *testing.T) {
	data := mergedNativeHistory(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			store, lease, cleanup := openIdentityTarget(t, engine)
			defer cleanup()
			plan := nativeHistoryPlan(t, data, store, lease)
			profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: driver.StoreID(plan.SourceStoreID), Method: "offline.restore.import", IdempotencyKey: strings.Repeat("b", 32), ArchiveDigest: plan.ArchiveSHA256, HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: time.Now().UTC().Format(time.RFC3339Nano)}
			var err error
			profile, err = BindSourceImport(ctx, bytes.NewReader(data), profile, plan)
			if err != nil {
				t.Fatal("bind full joined history", err)
			}
			report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
			if err != nil {
				t.Fatal("publish full joined history", err)
			}
			if report.Restored.Head != model.Natural64(15) || !report.Restored.HistoryFloor.IsZero() || report.SourcePlanes == nil || report.Receipt == nil {
				t.Fatal("joined full metadata/receipt", report)
			}
			_, recovered, found, err := RecoverBoundSourceImport(ctx, store)
			if err != nil || !found || !reflect.DeepEqual(recovered.Receipt, report.Receipt) {
				t.Fatal("joined full original receipt recovery", err)
			}
			r, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			rows, err := r.Get(ctx, []model.Ref{model.RefByID(ImportedOntologySourceID, model.Natural64(0))})
			if err != nil || len(rows) != 1 || rows[0].Kind != "ontology" {
				t.Fatal("protected installed catalog", err)
			}
			var installed dumpformat.NativeStatusOntology
			if err := json.Unmarshal(rows[0].Body, &installed); err != nil {
				t.Fatal(err)
			}
			if installed.Overlays["pl"].Statuses["shared"].Category != "active" || installed.Overlays["qa"].Statuses["shared"].Category != "done" {
				t.Fatal("project catalog categories were aliased")
			}
			assertProjectHistoryReadback(t, store, report, profile)
		})
	}
}

func assertProjectHistoryReadback(t *testing.T, store driver.Store, report Report, profile driver.RestoreImportProfile) {
	t.Helper()
	ctx := context.Background()
	r, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := r.Get(ctx, []model.Ref{model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0))})
	if err != nil || len(rows) != 1 {
		t.Fatal("history evidence", err)
	}
	var evidence driver.HistoryImportEvidence
	if err := json.Unmarshal(rows[0].Body, &evidence); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	history, _ := report.Archive.SourceHistory()
	for _, scope := range history.ProjectHistories {
		count := 0
		for _, mapping := range evidence.Mapping {
			if mapping.Project != scope.Project {
				continue
			}
			count++
			if mapping.SourceStoreID != scope.SourceStoreID || mapping.SourceSequence != uint64(count) || !reflect.DeepEqual(mapping.CommitIDs, []string{scope.Range.Frames[count-1].CommitID}) {
				t.Fatalf("original source revision lost: %+v", mapping)
			}
			at := mapping.Revision
			pinned, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &at})
			if err != nil {
				t.Fatal(err)
			}
			image, err := report.Archive.HistoryImageForProject(scope.Project, mapping.SourceSequence)
			if err != nil {
				t.Fatal(err)
			}
			when, _ := time.Parse(time.RFC3339Nano, profile.HistoryTime)
			want, err := buildPlanWithProfile(image, &profile, when)
			if err != nil {
				t.Fatal(err)
			}
			for _, expected := range portableHistoryRecords(want) {
				got, err := pinned.Get(ctx, []model.Ref{expected.Ref})
				if err != nil || len(got) != 1 || got[0].Kind != expected.Kind || !bytes.Equal(got[0].Body, expected.Body) {
					t.Fatalf("project %s source sequence %d record %s differs: %v", scope.Project, mapping.SourceSequence, expected.Ref.ID, err)
				}
			}
			if err := pinned.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if count != len(scope.Range.Frames) {
			t.Fatal("source frames lost", scope.Project, count)
		}
	}
}

func TestFullSourceHistoryRefusedPublicationConservesCoreAndNamespace(t *testing.T) {
	data := mergedNativeHistory(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			store, lease, cleanup := openIdentityTarget(t, engine)
			defer cleanup()
			plan := nativeHistoryPlan(t, data, store, lease)
			p := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: driver.StoreID(plan.SourceStoreID), Method: "offline.restore.import", IdempotencyKey: strings.Repeat("b", 32), ArchiveDigest: plan.ArchiveSHA256, HistoryMode: driver.RestoreHistoryModeFull}
			var err error
			p, err = BindSourceImport(ctx, bytes.NewReader(data), p, plan)
			if err != nil {
				t.Fatal(err)
			}
			cap, err := driver.RequireWorkflowCheckpointStore(store, lease)
			if err != nil {
				t.Fatal(err)
			}
			before := reverseState(t, store)
			beforeCP, err := cap.LoadCheckpoint(ctx, plan.Providers[0].Namespace)
			if err != nil {
				t.Fatal(err)
			}
			for _, fault := range []string{"missing-binding", "missing-checkpoint", "changed-envelope", "missing-receipt"} {
				t.Run(fault, func(t *testing.T) {
					broken := &incompleteSourceStore{Store: store, WorkflowCheckpointStore: cap, fault: fault}
					if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), broken, lease, p); !errors.Is(err, driver.ErrUnsupported) {
						t.Fatal("full joined incomplete publication accepted or wrong refusal", err)
					}
					cp, err := cap.LoadCheckpoint(ctx, plan.Providers[0].Namespace)
					if err != nil || !reflect.DeepEqual(before, reverseState(t, store)) || !reflect.DeepEqual(cp, beforeCP) {
						t.Fatal("refused full history published core or namespace", err)
					}
				})
			}
			if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, p); err != nil {
				t.Fatal("valid full control after refusals", err)
			}
		})
	}
}

func TestStrictSourceHistoryRefusalRetainsOriginalNamedGap(t *testing.T) {
	members := tarMembers(t, nativeHistoryFixture(t, "pl", "active"))
	var history dumpformat.SourceHistory
	decode(t, members[dumpformat.SourceHistoryMember], &history)
	// An unverified capture retains the current audit table exactly. The
	// portable fixture's independently rewritten event payloads are not that
	// census and cannot stand in for the archived current SQL cells.
	history.Events = []dumpformat.Row{}
	for _, line := range bytes.Split(bytes.TrimSpace(members["records/beads-archive.ndjson"]), []byte{'\n'}) {
		var record dumpformat.Record
		decode(t, line, &record)
		if record.Source.Table == "events" {
			history.Events = append(history.Events, record.Source.Fields)
		}
	}
	history.Range = dumpformat.SourceHistoryRange{Status: "unverified", Reason: "head-image-missing-families:config,issues", Baseline: []dumpformat.Record{}, BaselineEvents: []dumpformat.Row{}, Frames: []dumpformat.SourceHistoryFrame{}, Gaps: []dumpformat.HistoryGap{}}
	members[dumpformat.SourceHistoryMember] = encode(t, history)
	var manifest dumpformat.Manifest
	decode(t, members["manifest.json"], &manifest)
	manifest.History = "available-unverified"
	raw := members[dumpformat.SourceHistoryMember]
	manifest.Files[dumpformat.SourceHistoryMember] = dumpformat.File{SHA256: hashBytes(raw), Size: int64(len(raw))}
	members["manifest.json"] = encode(t, manifest)
	a, err := dumpformat.Read(bytes.NewReader(writeTarMembers(t, members)))
	if err != nil {
		t.Fatal("declared unverified source", err)
	}
	err = validateArchiveHistoryPolicy(driver.RestoreHistoryModeFull, a)
	var policy driver.ErrRestoreHistoryRefused
	var gap dumpformat.IncompleteHistoryError
	if !errors.As(err, &policy) || policy.Reason != driver.RestoreHistoryReasonAvailableUnverified || !errors.As(err, &gap) || gap.StoreID != manifest.StoreID || gap.Project != "pl" || gap.Reason != history.Range.Reason {
		t.Fatal("strict policy lost its original named source gap", err)
	}
	if err := validateArchiveHistoryPolicy(driver.RestoreHistoryModeSnapshot, a); err != nil {
		t.Fatal("explicit snapshot policy changed", err)
	}
}
