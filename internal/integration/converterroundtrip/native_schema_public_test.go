package converterroundtrip

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// Original public native outputs from producer d7f83c6f, R42737229912.
// The enclosing fixture retains both archives and independent typed evidence
// byte for byte. These receiving tests do not inherit the producer's result.
func actualOwnVersionArchive(t *testing.T, version int) (*dumpformat.Archive, []byte) {
	t.Helper()
	fixture, err := os.ReadFile("testdata/public-own-version-d7f83c6f.tar")
	if err != nil {
		t.Fatal(err)
	}
	if hashBytes(fixture) != "de2a032098a8b17aa3e06ac7fa445ab18bb13427a59606e4a4d66959650e62c5" {
		t.Fatal("public fixture bytes changed")
	}
	members := tarMembers(t, fixture)
	name := fmt.Sprintf("public-actual-v%d", version)
	data, evidence := members[name+".tar"], members[name+"-physical-evidence.json"]
	want := map[int][2]string{
		66: {"8c5617da814314fe210024573420f15659fea5b99fb54df9790fbb9efd5b51fc", "6c346cecdcb2a2ad900f9cff2fd3c12bc2391eadbb146ec34e0597b95fb589ad"},
		72: {"fb350f96445ea220792eec406271f462bd8481e556478cc3384b405735afe109", "64b9a1710cd41a0c0d3a6a43df7b10e9fac735e682342e48d99ccb09c030f082"},
	}[version]
	if hashBytes(data) != want[0] || hashBytes(evidence) != want[1] {
		t.Fatal("original public archive or typed evidence changed")
	}
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal("actual unmigrated source", err)
	}
	var physical struct {
		Working struct {
			HeadCommitID    string `json:"head_commit_id"`
			WorkingRootHash string `json:"working_root_hash"`
			ReadOnlyScope   bool   `json:"read_only_scope"`
			Complete        bool
			Tables          []struct {
				Name    string
				Columns []struct {
					Name    string
					SQLType string `json:"sql_type"`
				}
				Rows [][]struct{ Kind, Value string }
			}
		}
		Images []struct {
			CommitID      string `json:"commit_id"`
			ReadOnlyScope bool   `json:"read_only_scope"`
		}
	}
	// Wire names containing underscores have explicit tags in the binding view.
	var binding struct {
		Format        string   `json:"format"`
		StoreID       string   `json:"store_id"`
		ArchiveSHA256 string   `json:"archive_sha256"`
		SQLDefaults   string   `json:"sql_defaults"`
		Projects      []string `json:"projects"`
	}
	decode(t, evidence, &binding)
	decode(t, evidence, &physical)
	scopes := archive.NativeSourceHistories()
	if len(scopes) != 1 {
		t.Fatal("actual public source scope changed")
	}
	scope := scopes[0]
	ignored, tables := 26, 30
	if version == 72 {
		ignored, tables = 28, 33
	}
	if archive.Manifest().MainVersion != version || archive.Manifest().IgnoredVersion != ignored || binding.Format != "beads-physical-evidence/1" || binding.StoreID != scope.StoreID || binding.ArchiveSHA256 != archive.SHA256() || !reflect.DeepEqual(binding.Projects, scope.Projects) || binding.SQLDefaults != "not-captured" || !physical.Working.Complete || !physical.Working.ReadOnlyScope || physical.Working.WorkingRootHash == "" || physical.Working.HeadCommitID != scope.Head || len(physical.Working.Tables) != tables+3 || len(scope.Current) != tables || len(scope.Images) != 9 || len(physical.Images) != 9 || len(scope.Gaps) != 0 || !scope.Complete {
		t.Fatal("actual profile, physical binding or native history changed")
	}
	for i, image := range scope.Images {
		if physical.Images[i].CommitID != image.CommitID || !physical.Images[i].ReadOnlyScope {
			t.Fatal("original typed image identity changed")
		}
	}
	for _, table := range physical.Working.Tables {
		if table.Name != dumpformat.DurableEntityTable && table.Name != dumpformat.EphemeralEntityTable {
			continue
		}
		if len(table.Rows) != 1 || len(table.Rows[0]) != len(table.Columns) {
			t.Fatal("actual physical entity census changed")
		}
		for i, column := range table.Columns {
			cell := table.Rows[0][i]
			if column.SQLType == "" {
				t.Fatal("typed source lost SQL type")
			}
			switch column.Name {
			case "storage_class":
				if cell.Kind != "sql-null" {
					t.Fatal("actual SQL NULL storage policy changed")
				}
			case "description":
				if cell.Kind != "string" || cell.Value != "" {
					t.Fatal("actual empty string conflated with SQL NULL")
				}
			case "timeout_ns":
				if cell.Kind != "int64" || cell.Value != "9007199254740993" {
					t.Fatal("actual physical integer changed")
				}
			case "metadata":
				var actual any
				if cell.Kind != "string" || decodeNativeSchemaFixture([]byte(cell.Value), &actual) != nil || !bytes.Equal(encode(t, actual), encode(t, scope.Current[table.Name].Rows[0]["metadata"])) {
					t.Fatal("native stored JSON oracle differs from archive")
				}
			}
		}
	}
	return archive, data
}

func actualOwnVersionProfile(t *testing.T, archive *dumpformat.Archive, data []byte, store driver.Store, lease driver.ActiveLease) driver.RestoreImportProfile {
	t.Helper()
	plan := nativeHistoryPlan(t, data, store, lease)
	plan.Dispositions[0].StorageClass = "" // Present SQL NULL, authenticated policy.
	plan.Dispositions[0].Statuses = []driver.SourcePlaneStringPairV1{{From: "open", To: "open"}}
	// Identical observed type/storage/flags cannot select conflicting sinks.
	// This producer archives the active-plane row; retain that explicit origin
	// and every native cell instead of inventing a workflow mapping.
	conflicting := plan
	duplicate := plan.Dispositions[0]
	duplicate.Key, duplicate.SinkKind, duplicate.Schema = "conflicting", "workflow:item", workflowplugin.SourceStateSchema
	duplicate.Lifetime = string(plan.Target.Lifetime)
	conflicting.Dispositions = []driver.SourcePlaneDispositionV1{duplicate, plan.Dispositions[0]}
	before := reverseState(t, store)
	if _, err := driver.NewSourcePlaneImportBindingV1(conflicting); !errors.Is(err, driver.ErrUnsupported) || !strings.Contains(err.Error(), "duplicate observed disposition tuple") || !reflect.DeepEqual(before, reverseState(t, store)) {
		t.Fatal("conflicting observed dispositions must refuse unchanged", err)
	}
	profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: driver.StoreID(plan.SourceStoreID), Method: "offline.restore.import", IdempotencyKey: strings.Repeat("b", 32), ArchiveDigest: plan.ArchiveSHA256, HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: time.Date(2026, 11, 10, 2, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)}
	profile, err := BindSourceImport(context.Background(), bytes.NewReader(data), profile, plan)
	if err != nil {
		t.Fatal("bind actual own-version source", err)
	}
	return profile
}

func mustSourceRecords(t *testing.T, archive *dumpformat.Archive) []driver.SourcePlaneRecordV1 {
	t.Helper()
	rows, err := archive.SourcePlaneRecords()
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func assertActualOwnVersionReceiving(t *testing.T, store driver.Store, profile driver.RestoreImportProfile, report Report, data []byte) {
	t.Helper()
	ctx := context.Background()
	if report.Archive == nil || report.Receipt == nil || report.SourcePlanes == nil || !bytes.Equal(report.Archive.TarBytes(), data) {
		t.Fatal("actual committed source/receipt missing")
	}
	archive := report.Archive
	scope := archive.NativeSourceHistories()[0]
	identity, err := nativeTableIdentity(scope)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := profile.SourcePlanes.Plan()
	if err != nil {
		t.Fatal(err)
	}
	capability, ok := store.(driver.SourcePlaneSnapshotStore)
	if !ok {
		t.Fatal("actual driver lacks atomic core/namespace snapshot")
	}
	r, err := capability.ReadSourcePlaneSnapshot(ctx, driver.View{Incarnation: report.Restored.Incarnation}, []string{plan.Providers[0].Namespace})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	_, logical, err := recoverSourceImportReader(ctx, r, report.Restored, profile, Verification{ArchiveSHA256: profile.ArchiveDigest, DerivedSHA256: profile.DerivedDigest}, plan, report.RequestHash)
	if err != nil {
		t.Fatal("actual committed source-plane recovery", err)
	}
	current, err := workflowplugin.InspectSourceReverseImage(logical)
	if err != nil {
		t.Fatal("actual current provider readback", err)
	}
	if len(current.Entities) != 0 {
		t.Fatal("archived source row became an invented live target")
	}
	var raw struct {
		Source struct{ Records []driver.SourcePlaneRecordV1 }
	}
	decode(t, logical, &raw)
	wantRecords := mustSourceRecords(t, archive)
	sort.Slice(wantRecords, func(i, j int) bool { return hashBytes(wantRecords[i].Raw) < hashBytes(wantRecords[j].Raw) })
	sort.Slice(raw.Source.Records, func(i, j int) bool {
		return hashBytes(raw.Source.Records[i].Raw) < hashBytes(raw.Source.Records[j].Raw)
	})
	if !reflect.DeepEqual(raw.Source.Records, wantRecords) {
		t.Fatal("actual complete original source records changed")
	}
	rows, err := r.Get(ctx, []model.Ref{model.RefByID(NativeFamilyReportID, model.Natural64(0)), model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0)), model.RefByID("public-a1b", model.Natural64(0)), model.RefByID(importProjectRecordID(identity.Project), model.Natural64(0))})
	if err != nil || len(rows) != 4 {
		t.Fatal("actual committed evidence/core/default readback missing", err)
	}
	var history driver.HistoryImportEvidence
	var stored nativeFamilyEvidence
	var doc pipeline.Document
	var project importProjectMetadata
	for _, row := range rows {
		switch row.Ref.ID {
		case NativeFamilyReportID:
			decode(t, row.Body, &stored)
			if archive.Manifest().MainVersion == 72 && bytes.Contains(row.Body, []byte(`"source_schemas"`)) {
				t.Fatal("legacy v72 receipt schema changed")
			}
		case driver.OfflineHistoryEvidenceID:
			decode(t, row.Body, &history)
		case "public-a1b":
			decode(t, row.Body, &doc)
		default:
			decode(t, row.Body, &project)
		}
	}
	if !reflect.DeepEqual(stored.Schemas, report.NativeSchemas) || !reflect.DeepEqual(stored.Families, report.NativeFamilies) || len(stored.Families) != len(scope.Current) || history.Source != driver.NativeTableHistorySource || !history.Complete || project.SourceIdentity == nil || *project.SourceIdentity != identity || !reflect.DeepEqual(project.TargetAllocator, nativeReceivingExpectedHashAllocator(t, archive, identity.Project)) {
		t.Fatal("actual stored schema/default/source/history accounting differs")
	}
	var fields dumpformat.Row
	decode(t, doc.Metadata["beads.source_fields"], &fields)
	if !bytes.Equal(encode(t, fields), encode(t, archive.Records("bead")[0].Source.Fields)) || doc.Fields["description"] != "" || doc.Fields["title"] != scope.Current[dumpformat.DurableEntityTable].Rows[0]["title"] {
		t.Fatal("actual durable source fields or SQL NULL/empty boundary changed")
	}
	storage, present := fields["storage_class"]
	if !present || storage != nil || string(doc.Metadata["n"]) != "9007199254740992" || string(doc.Metadata["beads.timeout_ns"]) != "9007199254740993" {
		t.Fatal("source SQL NULL, native-stored JSON and exact physical integer conflated")
	}
	if archive.Manifest().MainVersion == 66 {
		if len(stored.Schemas) != 1 {
			t.Fatal("actual v66 schema report missing")
		}
		schema := stored.Schemas[0]
		wantProfile, _ := dumpformat.NativeSchemaForVersion(66, 26)
		if schema.Source != identity || !reflect.DeepEqual(schema.Profile, wantProfile) || schema.SchemaDefaultCount != 0 || len(schema.PlugDefaults) != 1 || len(schema.TargetOmissions) != 2 {
			t.Fatal("actual absent columns, omissions and defaults conflated")
		}
		var allocator driver.OfflineAllocator
		decode(t, schema.PlugDefaults[0].Value, &allocator)
		if schema.PlugDefaults[0].Count != 1 || schema.PlugDefaults[0].TargetKind != "instance_config" || schema.PlugDefaults[0].Field != "target_allocator" || !reflect.DeepEqual(allocator, project.TargetAllocator) {
			t.Fatal("reported initializer differs from actual committed value")
		}
		wantOmissions := map[string]bool{"current_revision": true, "participation_generation": true}
		for _, omission := range schema.TargetOmissions {
			if omission.Family != dumpformat.DurableEntityTable || omission.Count != 1 || omission.TargetKind != "bead" || omission.Disposition != "absent-source-column-no-target-slot" || !wantOmissions[omission.Column] {
				t.Fatal("actual omission tuples differ from admitted/live targets", omission)
			}
			delete(wantOmissions, omission.Column)
			if _, exists := doc.Fields[omission.Column]; exists {
				t.Fatal("unmodeled core field invented")
			}
		}
		if len(wantOmissions) != 0 {
			t.Fatal("actual target omission missing")
		}
	} else if report.NativeSchemas != nil || stored.Schemas != nil {
		t.Fatal("legacy v72 schema encoding changed")
	}
	for table, value := range scope.Current {
		historyClass := ""
		for _, family := range scope.Sources {
			if family.Family == table {
				historyClass = family.History
			}
		}
		assertNativeTableReadback(t, r, identity, table, historyClass, value)
		matches := 0
		for _, family := range report.NativeFamilies {
			if family.Source == identity && family.Family == table {
				matches++
				if family.SourceRows != len(value.Rows) || family.ImportedRows != len(value.Rows) || family.ExpiredRows != 0 {
					t.Fatal("actual current row counts differ", family)
				}
			}
		}
		if matches != 1 {
			t.Fatal("actual family count missing or duplicated")
		}
	}
	for _, image := range scope.Images {
		var revision model.Revision
		matches := 0
		for _, mapping := range history.Mapping {
			if mapping.SourceStoreID == identity.StoreID && mapping.Project == identity.Project && mapping.SourceSequence == image.Sequence {
				revision = mapping.Revision
				matches++
				if !reflect.DeepEqual(mapping.CommitIDs, []string{image.CommitID}) {
					t.Fatal("actual commit identity changed")
				}
			}
		}
		if matches != 1 || revision == "" {
			t.Fatal("actual commit image missing or duplicated")
		}
		pinned, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &revision})
		if err != nil {
			t.Fatal(err)
		}
		assertNativeMissingImageTables(t, pinned, identity, scope, image)
		for table, value := range image.Tables {
			assertNativeTableReadback(t, pinned, identity, table, "retained-native-commit", value)
		}
		if err := pinned.Close(); err != nil {
			t.Fatal(err)
		}
	}
	retained, err := r.Blob(ctx, driver.Hash(archive.SHA256()))
	if err != nil || !bytes.Equal(retained, data) {
		t.Fatal("actual source archive blob changed", err)
	}
}

func TestActualNativeOwnVersionReceivingAcrossThreeDrivers(t *testing.T) {
	for _, version := range []int{66, 72} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			archive, data := actualOwnVersionArchive(t, version)
			original := bytes.Clone(data)
			for _, engine := range []string{"memory", "sqlite", "postgres"} {
				t.Run(engine, func(t *testing.T) {
					ctx := context.Background()
					store, lease, reopen := openIdentityTarget(t, engine)
					profile := actualOwnVersionProfile(t, archive, data, store, lease)
					report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
					if err != nil {
						t.Fatal("actual receiving", err)
					}
					assertActualOwnVersionReceiving(t, store, profile, report, data)
					capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
					if err != nil {
						t.Fatal(err)
					}
					before := reverseState(t, store)
					namespace := report.SourcePlanes.Providers[0].Provider.Namespace
					checkpoint, err := capability.LoadCheckpoint(ctx, namespace)
					if err != nil {
						t.Fatal(err)
					}
					newKey := profile
					newKey.IdempotencyKey = profile.IdempotencyKey + "-new"
					if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, newKey); !errors.Is(err, ErrTargetNotEmpty) {
						t.Fatal("populated target import must refuse", err)
					}
					after, err := capability.LoadCheckpoint(ctx, namespace)
					if err != nil || !reflect.DeepEqual(checkpoint, after) || !reflect.DeepEqual(before, reverseState(t, store)) {
						t.Fatal("repeat refusal changed core or checkpoint", err)
					}
					store, lease = reopen()
					_, recovered, found, err := RecoverBoundSourceImport(ctx, store)
					if err != nil || !found || !reflect.DeepEqual(report.Receipt, recovered.Receipt) || !reflect.DeepEqual(report.NativeSchemas, recovered.NativeSchemas) || !reflect.DeepEqual(report.NativeFamilies, recovered.NativeFamilies) || !reflect.DeepEqual(report.SourcePlanes, recovered.SourcePlanes) {
						t.Fatal("actual receipt/accounting changed on reopen", err)
					}
					assertActualOwnVersionReceiving(t, store, profile, recovered, data)
					if !bytes.Equal(original, data) {
						t.Fatal("receiving changed original input")
					}
					if out := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); out != "" {
						receipt := struct {
							Contract      string `json:"contract"`
							Engine        string `json:"engine"`
							ArchiveSHA256 string `json:"archive_sha256"`
							Report        Report `json:"committed_report"`
						}{"public-own-version-receiving/1", engine, archive.SHA256(), recovered}
						if err := os.WriteFile(filepath.Join(out, fmt.Sprintf("public-receiving-v%d-%s.json", version, engine)), encode(t, receipt), 0o600); err != nil {
							t.Fatal(err)
						}
					}
				})
			}
		})
	}
}
