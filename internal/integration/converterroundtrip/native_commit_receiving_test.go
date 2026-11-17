package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

func TestActualNativeMultiVersionReceivingAcrossThreeDrivers(t *testing.T) {
	_, input := actualMultiVersionArchive(t)
	exerciseNativeCommitReceivingLibrary(t, input)
}

func TestNativeReceivingExecutableActualMultiVersion(t *testing.T) {
	_, input := actualMultiVersionArchive(t)
	exerciseNativeCommitReceivingExecutable(t, input)
}

// Recount from the authenticated input and committed clock rather than from
// another reported total. Every source/family tuple must occur exactly once.
func assertNativeCommitFamilyReports(t *testing.T, archive *dumpformat.Archive, when time.Time, reports []NativeFamilyReport) int {
	t.Helper()
	type familyKey struct {
		Source dumpformat.SourceIdentity
		Family string
	}
	want := map[familyKey]NativeFamilyReport{}
	importedRows := 0
	for _, scope := range archive.NativeSourceHistories() {
		identity, err := nativeTableIdentity(scope)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		for _, source := range scope.Sources {
			if source.History == "" {
				continue
			}
			table, present := scope.Current[source.Family]
			key := familyKey{identity, source.Family}
			if !present || seen[source.Family] || source.Rows != len(table.Rows) {
				t.Fatal("original current family census missing or duplicated")
			}
			if _, duplicate := want[key]; duplicate {
				t.Fatal("original source/family tuple duplicated")
			}
			seen[source.Family] = true
			account := NativeFamilyReport{Source: identity, Family: source.Family, History: source.History, SourceRows: len(table.Rows), ImportedRows: len(table.Rows)}
			if source.Family == "leases" {
				for _, row := range table.Rows {
					text, ok := row["lease_expires_at"].(string)
					expires, err := time.Parse(time.RFC3339Nano, text)
					if !ok || err != nil {
						t.Fatal("original lease lacks an exact timestamp", err)
					}
					if !expires.After(when) {
						account.ImportedRows--
						account.ExpiredRows++
					}
				}
			}
			want[key] = account
			importedRows += account.ImportedRows
		}
		if len(seen) != len(scope.Current) {
			t.Fatal("original current family has no explicit disposition")
		}
	}
	if len(reports) != len(want) {
		t.Fatal("current source/family report cardinality differs")
	}
	for _, account := range reports {
		key := familyKey{account.Source, account.Family}
		expected, present := want[key]
		if !present || account != expected {
			t.Fatal("current source/imported/expired count differs or tuple duplicated", account)
		}
		delete(want, key)
	}
	if len(want) != 0 {
		t.Fatal("current source/family report missing")
	}
	return importedRows
}

func nativeCommitOpaqueCounts(t *testing.T, archive *dumpformat.Archive, plan driver.SourcePlanePlanV1) map[string]int {
	t.Helper()
	dispositions := map[string]driver.SourcePlaneDispositionV1{}
	entities := map[driver.SourceEntityKey]driver.SourcePlaneEntityV1{}
	for _, disposition := range plan.Dispositions {
		if _, duplicate := dispositions[disposition.Key]; duplicate {
			t.Fatal("duplicate constructed disposition")
		}
		dispositions[disposition.Key] = disposition
	}
	for _, entity := range plan.Entities {
		if _, duplicate := entities[entity.Source]; duplicate {
			t.Fatal("duplicate constructed source entity")
		}
		entities[entity.Source] = entity
	}
	counts := map[string]int{}
	for _, source := range archive.Records("bead") {
		entity, present := entities[driver.SourceEntityKey{Project: source.Project, Plane: source.Source.Table, ID: source.ID}]
		if !present || entity.TargetKind != "bead" || entity.TargetID != source.ID {
			t.Fatal("ordinary source identity missing or remapped")
		}
		disposition, present := dispositions[entity.DispositionKey]
		if !present {
			t.Fatal("ordinary source disposition missing")
		}
		seen := map[string]bool{}
		for _, rule := range disposition.Fields {
			if rule.Disposition != "opaque-metadata" {
				continue
			}
			_, present := source.Source.Fields[rule.Source]
			if !present || seen[rule.Source] || rule.Target != "beads."+rule.Source {
				t.Fatal("opaque field binding missing, duplicated or renamed")
			}
			seen[rule.Source] = true
			counts[source.Project]++
		}
	}
	return counts
}

// assertNativeCommitReceiving compares the complete supplied original source
// after actual publication and recovery. It does not assign schema versions
// to unannotated images or use a fixed-version fixture as migration evidence.
func assertNativeCommitReceiving(t *testing.T, store driver.Store, profile driver.RestoreImportProfile, report Report, input []byte) {
	t.Helper()
	assertNativeHistoryReceiving(t, store, profile, report, input, true)
}

// Complete fixture callers retain their explicit completeness requirement.
// Boundary callers must instead retain and read back their exact descriptor.
func assertNativeHistoryReceiving(t *testing.T, store driver.Store, profile driver.RestoreImportProfile, report Report, input []byte, complete bool) {
	t.Helper()
	ctx := context.Background()
	archive, err := dumpformat.Read(bytes.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if report.Archive == nil || report.Receipt == nil || report.SourcePlanes == nil || !bytes.Equal(report.Archive.TarBytes(), input) || !reflect.DeepEqual(report.Archive.NativeSourceHistories(), archive.NativeSourceHistories()) {
		t.Fatal("recovered archive or per-image schema annotations changed")
	}
	assertNativeReceivingHistoryReport(t, archive, report, complete)
	plan, err := profile.SourcePlanes.Plan()
	if err != nil {
		t.Fatal(err)
	}
	dispositions := map[string]driver.SourcePlaneDispositionV1{}
	entities := map[driver.SourceEntityKey]driver.SourcePlaneEntityV1{}
	for _, disposition := range plan.Dispositions {
		dispositions[disposition.Key] = disposition
	}
	for _, entity := range plan.Entities {
		entities[entity.Source] = entity
	}
	namespaces := []string{}
	for _, provider := range plan.Providers {
		namespaces = append(namespaces, provider.Namespace)
	}
	capability, ok := store.(driver.SourcePlaneSnapshotStore)
	if !ok {
		t.Fatal("missing atomic core/source snapshot")
	}
	reader, err := capability.ReadSourcePlaneSnapshot(ctx, driver.View{Incarnation: report.Restored.Incarnation}, namespaces)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	retained, err := reader.Blob(ctx, driver.Hash(archive.SHA256()))
	if err != nil || !bytes.Equal(retained, input) {
		t.Fatal("committed original archive bytes changed", err)
	}
	_, logical, err := recoverSourceImportReader(ctx, reader, report.Restored, profile, Verification{ArchiveSHA256: profile.ArchiveDigest, DerivedSHA256: profile.DerivedDigest}, plan, report.RequestHash)
	if err != nil {
		t.Fatal("committed source-plane recovery", err)
	}
	var provider struct {
		Source struct{ Records []driver.SourcePlaneRecordV1 }
	}
	decode(t, logical, &provider)
	wantRecords := mustSourceRecords(t, archive)
	sort.Slice(wantRecords, func(i, j int) bool { return hashBytes(wantRecords[i].Raw) < hashBytes(wantRecords[j].Raw) })
	sort.Slice(provider.Source.Records, func(i, j int) bool {
		return hashBytes(provider.Source.Records[i].Raw) < hashBytes(provider.Source.Records[j].Raw)
	})
	if !reflect.DeepEqual(wantRecords, provider.Source.Records) {
		t.Fatal("complete current source records or raw field commitments changed")
	}
	rows, err := reader.Get(ctx, []model.Ref{model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0)), model.RefByID(NativeFamilyReportID, model.Natural64(0))})
	if err != nil || len(rows) != 2 {
		t.Fatal("committed native history/accounting missing", err)
	}
	var history driver.HistoryImportEvidence
	var accounting nativeFamilyEvidence
	for _, row := range rows {
		if row.Ref.ID == driver.OfflineHistoryEvidenceID {
			decode(t, row.Body, &history)
		} else {
			decode(t, row.Body, &accounting)
		}
	}
	if history.Source != driver.NativeTableHistorySource || history.Mode != driver.RestoreHistoryModeFull || history.Complete != complete || len(history.Gaps) != 0 || accounting.RestoreTime != profile.HistoryTime || !reflect.DeepEqual(accounting.Families, report.NativeFamilies) || !reflect.DeepEqual(accounting.Schemas, report.NativeSchemas) || !reflect.DeepEqual(accounting.Histories, report.NativeHistories) {
		t.Fatal("native receipt/accounting differs from committed readback")
	}
	assertNativeHistoryCoverageReadback(t, store, archive, report, history)
	when, err := time.Parse(time.RFC3339Nano, profile.HistoryTime)
	if err != nil {
		t.Fatal(err)
	}
	assertNativeCommitFamilyReports(t, archive, when, report.NativeFamilies)
	wantOpaque := nativeCommitOpaqueCounts(t, archive, plan)
	wantSchemas := map[dumpformat.SourceIdentity]dumpformat.NativeSchemaProfile{}
	familyCount, imageCount := 0, 0
	for _, scope := range archive.NativeSourceHistories() {
		familyCount += len(scope.Current)
		imageCount += len(scope.Images)
		identity, err := nativeTableIdentity(scope)
		if err != nil {
			t.Fatal(err)
		}
		if scope.SchemaProfile == nil {
			t.Fatal("current source schema profile missing")
		}
		if _, duplicate := wantSchemas[identity]; duplicate {
			t.Fatal("current source identity duplicated")
		}
		wantSchemas[identity] = *scope.SchemaProfile
		for _, image := range scope.Images {
			if image.SchemaProfile == nil {
				t.Fatal("genuine per-commit annotation missing")
			}
			var revision model.Revision
			matches := 0
			for _, mapping := range history.Mapping {
				if mapping.SourceStoreID == scope.StoreID && mapping.Project == identity.Project && mapping.SourceSequence == image.Sequence {
					matches++
					revision = mapping.Revision
					if !reflect.DeepEqual(mapping.CommitIDs, []string{image.CommitID}) || len(mapping.EventIDs) != 0 {
						t.Fatal("source commit mapping gained event assignments or changed identity")
					}
				}
			}
			if matches != 1 || revision == "" {
				t.Fatal("original per-source image mapping missing or duplicated")
			}
			pinned, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &revision})
			if err != nil {
				t.Fatal(err)
			}
			assertNativeMissingImageTables(t, pinned, identity, scope, image)
			for name, value := range image.Tables {
				assertNativeTableReadback(t, pinned, identity, name, "retained-native-commit", value)
			}
			for _, record := range archive.Records("bead") {
				rows, err := pinned.Get(ctx, []model.Ref{model.RefByID(record.ID, model.Natural64(0))})
				if err != nil || len(rows) != 0 {
					t.Fatal("current business row was grafted into native history", err)
				}
			}
			if err := pinned.Close(); err != nil {
				t.Fatal(err)
			}
		}
		for _, family := range scope.Sources {
			if family.History == "" {
				continue
			}
			value := scope.Current[family.Family]
			if family.Family == "leases" {
				kept := []dumpformat.Row{}
				for _, row := range value.Rows {
					keep, err := nativeLeaseUnexpired(row, when)
					if err != nil {
						t.Fatal(err)
					}
					if keep {
						kept = append(kept, row)
					}
				}
				value.Rows = kept
			}
			assertNativeTableReadback(t, reader, identity, family.Family, family.History, value)
		}
	}
	if len(history.Mapping) != imageCount || len(report.NativeFamilies) != familyCount || len(report.NativeSchemas) != len(archive.NativeSourceHistories()) {
		t.Fatal("current family or source schema report census incomplete")
	}
	opaqueByProject := map[string]int{}
	for _, source := range archive.Records("bead") {
		rows, err := reader.Get(ctx, []model.Ref{model.RefByID(source.ID, model.Natural64(0))})
		if err != nil || len(rows) != 1 {
			t.Fatal("admitted ordinary entity missing", err)
		}
		var doc pipeline.Document
		decode(t, rows[0].Body, &doc)
		var fields dumpformat.Row
		decode(t, doc.Metadata["beads.source_fields"], &fields)
		if !bytes.Equal(encode(t, fields), encode(t, source.Source.Fields)) {
			t.Fatal("original ordinary entity fields or SQL NULL/empty boundary changed")
		}
		entity := entities[driver.SourceEntityKey{Project: source.Project, Plane: source.Source.Table, ID: source.ID}]
		if entity.TargetKind != "bead" || entity.TargetID != source.ID {
			t.Fatal("explicit ordinary source identity was remapped")
		}
		for _, rule := range dispositions[entity.DispositionKey].Fields {
			if rule.Disposition != "opaque-metadata" {
				continue
			}
			want := bytes.TrimSpace(encode(t, source.Source.Fields[rule.Source]))
			if rule.Target != "beads."+rule.Source || !bytes.Equal(doc.Metadata[rule.Target], want) {
				t.Fatal("reserved opaque source metadata changed", rule.Source)
			}
			opaqueByProject[source.Project]++
		}
	}
	for _, schema := range report.NativeSchemas {
		want, present := wantSchemas[schema.Source]
		if !present || !reflect.DeepEqual(schema.Profile, want) || schema.Profile.MainVersion != 66 || schema.Profile.IgnoredVersion != 26 || len(schema.Profile.Absences) != 7 || schema.SchemaDefaultCount != 0 || len(schema.PlugDefaults) != 1 || schema.PlugDefaults[0].Count != 1 || schema.PlugDefaults[0].TargetKind != "instance_config" || schema.PlugDefaults[0].Field != "target_allocator" || schema.OpaqueMetadataFields == 0 || schema.OpaqueMetadataFields != opaqueByProject[schema.Source.Project] || schema.OpaqueMetadataFields != wantOpaque[schema.Source.Project] {
			t.Fatal("current v66 source absence/default accounting differs")
		}
		delete(wantSchemas, schema.Source)
		wantAllocator := nativeReceivingExpectedHashAllocator(t, archive, schema.Source.Project)
		allocator, err := json.Marshal(wantAllocator)
		if err != nil || !bytes.Equal(schema.PlugDefaults[0].Value, allocator) {
			t.Fatal("actual committed allocator initializer differs", err)
		}
		rows, err := reader.Get(ctx, []model.Ref{model.RefByID(importProjectRecordID(schema.Source.Project), model.Natural64(0))})
		if err != nil || len(rows) != 1 {
			t.Fatal("committed original project/default missing", err)
		}
		var project importProjectMetadata
		decode(t, rows[0].Body, &project)
		if project.SourceIdentity == nil || *project.SourceIdentity != schema.Source || !reflect.DeepEqual(project.TargetAllocator, wantAllocator) {
			t.Fatal("reported initializer differs from actual committed project")
		}
		entities := 0
		for _, source := range archive.Records("bead") {
			if source.Project == schema.Source.Project {
				entities++
			}
		}
		seen := map[string]bool{}
		for _, omission := range schema.TargetOmissions {
			key := omission.Family + "/" + omission.Column + "/" + omission.TargetKind
			if seen[key] || omission.Family != dumpformat.DurableEntityTable || omission.TargetKind != "bead" || omission.Column != "current_revision" && omission.Column != "participation_generation" || omission.Disposition != "absent-source-column-no-target-slot" || omission.Count != entities {
				t.Fatal("counted current omissions changed or were duplicated")
			}
			seen[key] = true
		}
		if len(seen) != 2 {
			t.Fatal("current durable omission set incomplete")
		}
	}
	if len(wantSchemas) != 0 {
		t.Fatal("current source schema report missing")
	}
}

func nativeCommitReceivingPolicy(t *testing.T, input []byte, engine string) []byte {
	t.Helper()
	var policy map[string]json.RawMessage
	decode(t, receivingPolicyFor(t, input, engine), &policy)
	policy["unmapped_fields"] = encode(t, "opaque-metadata")
	return encode(t, policy)
}

// This preparation uses the same executable's explicit policy constructor.
// Target identity is filled only after opening the actual disposable driver;
// no fixture-specific disposition or source-schema mapping is inferred here.
func nativeCommitReceivingProfile(t *testing.T, input []byte, request receivingRequest, store driver.Store, lease driver.ActiveLease) driver.RestoreImportProfile {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
	if err != nil {
		t.Fatal(err)
	}
	plan := request.SourcePlan
	if plan.Target.StoreID != "" || plan.Target.Incarnation != "" || plan.Target.Lifetime != capability.WorkflowCheckpointLifetime() {
		t.Fatal("constructor target identity/lifetime differs from actual driver")
	}
	plan.Target.StoreID, plan.Target.Incarnation = string(meta.StoreID), meta.Incarnation
	profile := driver.RestoreImportProfile{Admission: request.Admission, SourceStoreID: driver.StoreID(request.SourceStoreID), Method: "offline.restore.import", IdempotencyKey: request.IdempotencyKey, ArchiveDigest: request.ArchiveSHA256, HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: time.Now().UTC().Format(time.RFC3339Nano)}
	bound, err := BindSourceImport(ctx, bytes.NewReader(input), profile, plan)
	if err != nil {
		t.Fatal("bind genuine multi-version source with explicit constructed policy", err)
	}
	return bound
}

func exerciseNativeCommitReceivingLibrary(t *testing.T, input []byte) {
	t.Helper()
	exerciseNativeHistoryReceivingLibrary(t, input, "multiversion", true)
}

func exerciseNativeHistoryReceivingLibrary(t *testing.T, input []byte, label string, complete bool) {
	t.Helper()
	binary, binaryHash := receivingBinary(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			policy := nativeCommitReceivingPolicy(t, input, engine)
			request := prepareReceivingRequest(t, binary, binaryHash, engine, input, policy, label+"-library-"+engine)
			store, lease, reopen := openIdentityTarget(t, engine)
			profile := nativeCommitReceivingProfile(t, input, request, store, lease)
			report, err := RestoreImportWithLease(ctx, bytes.NewReader(input), store, lease, profile)
			if err != nil {
				t.Fatal("genuine multi-version library receiving", err)
			}
			assertNativeHistoryReceiving(t, store, profile, report, input, complete)
			capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := profile.SourcePlanes.Plan()
			if err != nil || len(plan.Providers) != 1 {
				t.Fatal("expected explicit single public provider", err)
			}
			namespace := plan.Providers[0].Namespace
			before := reverseState(t, store)
			checkpoint, err := capability.LoadCheckpoint(ctx, namespace)
			if err != nil {
				t.Fatal(err)
			}
			newKey := profile
			newKey.IdempotencyKey = profile.IdempotencyKey + "-new"
			if _, err := RestoreImportWithLease(ctx, bytes.NewReader(input), store, lease, newKey); !errors.Is(err, ErrTargetNotEmpty) {
				t.Fatal("populated genuine multi-version destination must refuse", err)
			}
			after, err := capability.LoadCheckpoint(ctx, namespace)
			if err != nil || !reflect.DeepEqual(before, reverseState(t, store)) || !reflect.DeepEqual(checkpoint, after) {
				t.Fatal("populated refusal changed core or provider checkpoint", err)
			}
			store, lease = reopen()
			bound, recovered, found, err := RecoverBoundSourceImport(ctx, store)
			if err != nil || !found || bound.IdempotencyKey != profile.IdempotencyKey || bound.HistoryTime != profile.HistoryTime || !reflect.DeepEqual(bound.Admission, profile.Admission) || !reflect.DeepEqual(report.Receipt, recovered.Receipt) || !reflect.DeepEqual(report.NativeFamilies, recovered.NativeFamilies) || !reflect.DeepEqual(report.NativeSchemas, recovered.NativeSchemas) || !reflect.DeepEqual(report.NativeHistories, recovered.NativeHistories) || !reflect.DeepEqual(report.SourcePlanes, recovered.SourcePlanes) {
				t.Fatal("genuine multi-version destination recovery differs", err)
			}
			assertNativeHistoryReceiving(t, store, bound, recovered, input, complete)
			if out := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); out != "" {
				lifetime := "driver-reopened"
				if engine == "memory" {
					lifetime = "same-process-memory"
				}
				receipt := struct {
					Format        string `json:"format"`
					Engine        string `json:"engine"`
					ArchiveSHA256 string `json:"archive_sha256"`
					PolicySHA256  string `json:"policy_sha256"`
					Reopen        string `json:"reopen"`
					Report        Report `json:"committed_report"`
				}{"public-" + label + "-receiving/1", engine, hashBytes(input), hashBytes(policy), lifetime, recovered}
				if err := os.WriteFile(filepath.Join(out, fmt.Sprintf("public-%s-library-%s.json", label, engine)), encode(t, receipt), 0o600); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func inspectNativeCommitReceivingTarget(t *testing.T, engine, target string, input []byte, expected Report) []byte {
	t.Helper()
	return inspectNativeHistoryReceivingTarget(t, engine, target, input, expected, true)
}

func inspectNativeHistoryReceivingTarget(t *testing.T, engine, target string, input []byte, expected Report, complete bool) []byte {
	t.Helper()
	ctx := context.Background()
	var store driver.Store
	var err error
	if engine == "sqlite" {
		store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: target})
	} else if engine == "postgres" {
		store, err = postgres.Open(ctx, driver.Config{DSN: target})
	} else {
		t.Fatal("durable external inspection requires a persistent driver")
	}
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	lease, err := store.Lease(ctx, "public-multiversion-receiving-verifier", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	profile, recovered, found, err := RecoverBoundSourceImport(ctx, store)
	if err != nil || !found || !reflect.DeepEqual(expected.Receipt, recovered.Receipt) || !reflect.DeepEqual(expected.NativeFamilies, recovered.NativeFamilies) || !reflect.DeepEqual(expected.NativeSchemas, recovered.NativeSchemas) || !reflect.DeepEqual(expected.NativeHistories, recovered.NativeHistories) || !reflect.DeepEqual(expected.SourcePlanes, recovered.SourcePlanes) {
		t.Fatal("same-ELF multi-version receipt differs from external recovery", err)
	}
	assertNativeHistoryReceiving(t, store, profile, recovered, input, complete)
	plan, err := profile.SourcePlanes.Plan()
	if err != nil || len(plan.Providers) != 1 {
		t.Fatal("expected explicit single public provider", err)
	}
	capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := capability.LoadCheckpoint(ctx, plan.Providers[0].Namespace)
	if err != nil {
		t.Fatal(err)
	}
	return encode(t, struct{ Core, Checkpoint any }{reverseState(t, store), checkpoint})
}

func exerciseNativeCommitReceivingExecutable(t *testing.T, input []byte) {
	t.Helper()
	exerciseNativeHistoryReceivingExecutable(t, input, "multiversion", true)
}

func exerciseNativeHistoryReceivingExecutable(t *testing.T, input []byte, label string, complete bool) {
	t.Helper()
	binary, binaryHash := receivingBinary(t)
	archive, err := dumpformat.Read(bytes.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			target := ""
			if engine == "sqlite" {
				target = filepath.Join(t.TempDir(), "multiversion-receiving.db")
			} else if engine == "postgres" {
				target = identityPostgresDSN(t)
			}
			policy := nativeCommitReceivingPolicy(t, input, engine)
			request := prepareReceivingRequest(t, binary, binaryHash, engine, input, policy, label+"-elf-"+engine)
			stdout, stderr, err := executeReceiving(t, binary, binaryHash, engine, target, input, request, label+"-elf-"+engine, policy)
			if err != nil {
				t.Fatalf("genuine multi-version same-ELF receiving failed: %v %s", err, stderr)
			}
			var result receivingReceipt
			if err := decodeNativeSchemaFixture(stdout, &result); err != nil {
				t.Fatal("multi-version executable receipt", err)
			}
			if result.Format != "plug-native-receiving-receipt/1" || result.BinarySHA256 != binaryHash || result.PolicySHA256 != hashBytes(policy) || result.ArchiveSHA256 != hashBytes(input) || result.RequestSHA256 != hashBytes(encode(t, request)) || result.Driver != engine || result.HistoryMode != "full" || !result.Verification.ArchiveBlob || result.Report.Receipt == nil || result.CrossProjectIDCollisions != 0 {
				t.Fatal("same-ELF multi-version input/request/committed receipt binding differs")
			}
			assertNativeReceivingHistoryReport(t, archive, result.Report, complete)
			when, err := time.Parse(time.RFC3339Nano, result.RestoreTime)
			if err != nil {
				t.Fatal(err)
			}
			currentRows := assertNativeCommitFamilyReports(t, archive, when, result.Report.NativeFamilies)
			families, images, tables, cellsRows := 0, 0, 0, 0
			for _, scope := range archive.NativeSourceHistories() {
				families += len(scope.Current)
				for _, image := range scope.Images {
					if image.SchemaProfile == nil {
						t.Fatal("genuine input image lacks per-commit schema facts")
					}
					images++
					for _, table := range image.Tables {
						tables++
						cellsRows += len(table.Rows)
					}
				}
			}
			v := result.Verification
			if v.CurrentFamilies != families || v.CurrentRows != currentRows || v.HistoryImages != images || v.HistoryTables != tables || v.HistoryRows != cellsRows || len(result.Report.NativeFamilies) != families || len(result.Report.NativeSchemas) != len(archive.NativeSourceHistories()) {
				t.Fatal("same-ELF multi-version current/all-image conservation census differs")
			}
			opaque := nativeCommitOpaqueCounts(t, archive, request.SourcePlan)
			totalOpaque := 0
			for _, count := range opaque {
				totalOpaque += count
			}
			if totalOpaque == 0 || v.OpaqueMetadataFields != totalOpaque {
				t.Fatal("same-ELF opaque metadata verification count differs")
			}
			for _, schema := range result.Report.NativeSchemas {
				if schema.Profile.MainVersion != 66 || schema.Profile.IgnoredVersion != 26 || len(schema.Profile.Absences) != 7 || schema.SchemaDefaultCount != 0 || len(schema.PlugDefaults) != 1 || schema.PlugDefaults[0].Count != 1 || len(schema.TargetOmissions) != 2 || schema.OpaqueMetadataFields != opaque[schema.Source.Project] {
					t.Fatal("current initializer/default/omission accounting changed with historical schemas")
				}
			}
			if engine == "memory" {
				if v.Reopen != "same-process-memory" {
					t.Fatal("memory multi-version lifetime mislabeled")
				}
				return
			}
			if v.Reopen != "driver-reopened" {
				t.Fatal("persistent multi-version receiving lacks reopen")
			}
			before := inspectNativeHistoryReceivingTarget(t, engine, target, input, result.Report, complete)
			stdout, stderr, err = executeReceiving(t, binary, binaryHash, engine, target, input, request, label+"-elf-"+engine+"-populated", policy)
			// The disposable executable binds a fresh restore clock on each
			// invocation. Reusing the key therefore carries a different full
			// request hash and must take the idempotency conflict path before
			// the populated-target guard.
			if err == nil || len(stdout) != 0 || !strings.Contains(string(stderr), "transaction conflict") {
				t.Fatal("populated multi-version destination must refuse the changed request without receipt", err, string(stderr))
			}
			after := inspectNativeHistoryReceivingTarget(t, engine, target, input, result.Report, complete)
			if !bytes.Equal(before, after) {
				t.Fatal("same-ELF populated refusal changed core/checkpoint")
			}
		})
	}
}
