package converterroundtrip

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/nativeprojection"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/historyverify"
)

const nativeJoinedPhysicalPath = "../../core/dumpformat/testdata/native-joined-public-ca26683b"

func joinedPhysicalBinding(t *testing.T) dumpformat.NativeJoinedBinding {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(nativeJoinedPhysicalPath, "creation-record.json"))
	if err != nil || hashBytes(raw) != "2e0e02826753f83f9bf72c621a2863ea7312c16af8875bb685495fa7fdc9c0e6" {
		t.Fatal("public physical creation record", err)
	}
	var evidence struct {
		Pair []struct {
			Database string                         `json:"database"`
			Project  string                         `json:"rehearsal_project"`
			Files    []dumpformat.NativeCaptureFile `json:"ordered_file_ledger"`
		}
	}
	decode(t, raw, &evidence)
	b := dumpformat.NativeJoinedBinding{Format: dumpformat.NativeJoinedInputsFormatV2, PolicySHA256: strings.Repeat("a", 64), ProfileSHA256: strings.Repeat("b", 64)}
	for _, source := range evidence.Pair {
		raw, err := os.ReadFile(filepath.Join(nativeJoinedPhysicalPath, source.Database, "capture-set.json"))
		if err != nil {
			t.Fatal(err)
		}
		var root dumpformat.NativeCaptureSet
		decode(t, raw, &root)
		b.Sources = append(b.Sources, dumpformat.NativeJoinedInput{RehearsalProject: source.Project, Database: source.Database, SourceStoreID: root.StoreID, SourceProject: root.Projects[0], ArchiveSHA256: hashBytes(raw), Files: source.Files})
	}
	return b
}

func assertJoinedPhysicalHistories(t *testing.T, reports []NativeHistoryReport, binding dumpformat.NativeJoinedBinding) {
	t.Helper()
	if len(reports) != 2 {
		t.Fatal("complete physical histories omitted", reports)
	}
	want := map[dumpformat.SourceIdentity]bool{}
	for _, source := range binding.Sources {
		want[dumpformat.SourceIdentity{StoreID: source.SourceStoreID, Project: source.SourceProject}] = true
	}
	for _, report := range reports {
		if !want[report.Source] || !report.Complete || report.VerifiedCommitCount != 97 || report.Coverage != nil {
			t.Fatal("complete physical history identity/count/coverage differs", report)
		}
		delete(want, report.Source)
	}
	if len(want) != 0 {
		t.Fatal("complete physical source history omitted", want)
	}
}

func assertJoinedPhysicalPlanHistories(t *testing.T, image nativeprojection.Snapshot, binding dumpformat.NativeJoinedBinding) {
	t.Helper()
	assertJoinedPhysicalHistories(t, image.NativeHistories, binding)
	n := 0
	for _, row := range image.Records {
		if row.Ref.ID != nativeprojection.NativeFamilyReportID {
			continue
		}
		n++
		var evidence nativeprojection.NativeFamilyEvidence
		if err := json.Unmarshal(row.Body, &evidence); err != nil {
			t.Fatal(err)
		}
		assertJoinedPhysicalHistories(t, evidence.Histories, binding)
	}
	if n != 1 {
		t.Fatal("actual publication must retain one complete history evidence row", n)
	}
}

// This explicit test intent is independent of the product constructor. The
// frozen physical pair contains epic/task durable rows with present SQL NULL
// storage, exact named statuses and flags. Other rows remain inert origins.
func physicalCollectionSourcePlan(t *testing.T, a *dumpformat.Archive, store driver.Store, lease driver.ActiveLease) driver.SourcePlanePlanV1 {
	t.Helper()
	plan := nativeHistoryPlanFromArchive(t, a, store, lease)
	types := map[string]string{}
	for _, row := range a.Records("bead") {
		if row.Source.Fields["storage_class"] != nil || row.Source.Fields["ephemeral"] != json.Number("0") || row.Source.Fields["no_history"] != json.Number("0") {
			t.Fatal("physical test intent no longer matches exact source flags")
		}
		types[row.ID] = row.Source.Fields["issue_type"].(string)
	}
	base := plan.Dispositions[0]
	base.StorageClass = ""
	statuses := map[string]bool{}
	for _, project := range a.Manifest().Projects {
		for _, status := range project.Statuses {
			statuses[status.Name] = true
		}
	}
	base.Statuses = nil
	for name := range statuses {
		base.Statuses = append(base.Statuses, driver.SourcePlaneStringPairV1{From: name, To: name})
	}
	sort.Slice(base.Statuses, func(i, j int) bool { return base.Statuses[i].From < base.Statuses[j].From })
	for i, rule := range base.Fields {
		if rule.Disposition == "origin-only" {
			base.Fields[i].Disposition, base.Fields[i].Target = "opaque-metadata", "beads."+rule.Source
		}
	}
	plan.Dispositions = nil
	for _, typ := range []string{"epic", "task"} {
		d := base
		d.Key, d.SourceType = typ, typ
		plan.Dispositions = append(plan.Dispositions, d)
	}
	for i := range plan.Entities {
		plan.Entities[i].DispositionKey = types[plan.Entities[i].Source.ID]
	}
	return plan
}

func TestNativeCollectionOneActualPlanAndFinalProfileSealPhysicalPair(t *testing.T) {
	budget := dumpformat.NewNativeDeltaBudget(0)
	ctx := dumpformat.WithNativeDeltaBudget(context.Background(), budget)
	store, lease, cleanup := openIdentityTarget(t, "memory")
	defer cleanup()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fixture := joinedPhysicalBinding(t)
	raw := encode(t, fixture)
	// The collection commitment is canonical JSON without an envelope/newline.
	raw, err = json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	when := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "physical-collection-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("c", 64)}, SourceStoreID: driver.StoreID("merge-" + hashBytes(raw)), Method: "offline.restore.import", IdempotencyKey: "physical-native-collection-one-plan", ArchiveDigest: hashBytes(raw), HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: when.Format(time.RFC3339Nano)}
	target := driver.SourcePlaneTargetV1{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, RequireEmpty: true, Lifetime: driver.CheckpointLifetimeProcess, RuntimeContract: driver.SourcePlaneRuntimeContractV1}
	provider := driver.SourcePlaneProviderV1{Alias: "workflow", Instance: "default", Namespace: "plugin/workflow/default/durable", Schema: "workflow/source-state/1", Codec: "workflow/json/1", RuntimeContract: driver.SourcePlaneRuntimeContractV1, Lifetime: driver.CheckpointLifetimeProcess, OriginAdapter: "beads-workflow-origins/1"}
	c, err := historyverify.NewNativeCollection(ctx, fixture, historyverify.NativeCollectionAdmission{Profile: profile, Target: target, Provider: provider}, when)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, database := range []string{"ops", "sv2"} {
		if err = c.Append(ctx, func(ctx context.Context, options dumpformat.NativeCaptureSetOptions) (*dumpformat.Archive, error) {
			return dumpformat.ReadNativeCaptureSetDirectory(ctx, joinedPhysicalDirectory(t, database), options)
		}); err != nil {
			t.Fatal(err)
		}
	}
	a, err := c.FinishCurrent()
	if err != nil {
		t.Fatal(err)
	}
	plan := physicalCollectionSourcePlan(t, a, store, lease)
	plan.Providers[0].LogicalImageSHA256 = strings.Repeat("0", 64)
	binding, err := driver.NewSourcePlaneImportBindingV1(plan)
	if err != nil {
		t.Fatal(err)
	}
	profile.SourcePlanes, profile.DerivedDigest = &binding, strings.Repeat("0", 64)
	prepared, err := prepareSourceArchive(a.WithNativeDeltaAccounting(ctx), a, profile, false)
	if err != nil {
		t.Fatal("actual builtin provider from authenticated current", err)
	}
	plan.Providers[0].LogicalImageSHA256, plan.Providers[0].LogicalImageSize = prepared.SHA256(), uint64(len(prepared.LogicalImage()))
	binding, err = driver.NewSourcePlaneImportBindingV1(plan)
	if err != nil {
		t.Fatal(err)
	}
	profile.SourcePlanes, profile.DerivedDigest = &binding, ""
	stream, actual, profile, err := c.Bind(ctx, profile)
	if err != nil {
		t.Fatal("one final actual planner image", err)
	}
	image, err := actual.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	assertJoinedPhysicalPlanHistories(t, image, fixture)
	image = nativeprojection.Snapshot{}
	request := driver.OfflineHistoryStreamRequest{Current: meta, Profile: profile, Header: stream.Header(), When: when, Lifetime: target.Lifetime, Limits: historyverify.NativeArchiveLimits()}
	for _, attack := range []string{"derived", "actor", "key", "clock", "provider-image"} {
		t.Run(attack, func(t *testing.T) {
			candidate := request
			switch attack {
			case "derived":
				candidate.Profile.DerivedDigest = strings.Repeat("e", 64)
			case "actor":
				candidate.Profile.Admission.Actor.ID = "late-actor"
			case "key":
				candidate.Profile.IdempotencyKey = "late-different-collection-key"
			case "clock":
				candidate.When = candidate.When.Add(time.Second)
			case "provider-image":
				changed, e := profile.SourcePlanes.Plan()
				if e != nil {
					t.Fatal(e)
				}
				changed.Providers[0].LogicalImageSHA256 = strings.Repeat("f", 64)
				b, e := driver.NewSourcePlaneImportBindingV1(changed)
				if e != nil {
					t.Fatal(e)
				}
				candidate.Profile.SourcePlanes = &b
			}
			_, err := stream.Seal(ctx, candidate, actual)
			var refusal historyverify.ErrCheckpoint
			if !errors.As(err, &refusal) || refusal.Reason != "archive, transcript, clock or receiving plan binding differs" {
				t.Fatal("late final profile substituted or unrelated refusal", err)
			}
		})
	}
	if _, err = stream.Seal(ctx, request, actual); err != nil {
		t.Fatal("whole final Plan seal", err)
	}
	if request.Header.Head != 194 || request.Header.NativeSourceCount != 2 || actual.DerivedSHA256() != profile.DerivedDigest {
		t.Fatal("final range/digest differs")
	}
	counts := [2]int{}
	for {
		frame, err := stream.Next(ctx, request.Limits)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if commit := frame.Commit; commit != nil {
			i := 0
			if commit.Sequence > 97 {
				i = 1
			}
			counts[i]++
			if commit.SourceStoreID != fixture.Sources[i].SourceStoreID || commit.Project != fixture.Sources[i].SourceProject || commit.SourceSequence != uint64(counts[i]) {
				t.Fatal("sealed source order/provenance differs")
			}
		}
	}
	if counts != [2]int{97, 97} {
		t.Fatal("sealed physical commits omitted", counts)
	}
	if after, err := store.Meta(ctx); err != nil || after != meta {
		t.Fatal("planner/verifier wrote target", err)
	}
	actual = nativeprojection.Plan{}
	prepared = preparedSourceImage{}
	a = nil
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	if usage := budget.Usage(); usage.CurrentAccountedDecodedBytes != 0 || usage.PeakAccountedDecodedBytes > dumpformat.NativeDeltaDefaultDecodedLimit {
		t.Fatal("final collection lifetime/cap differs", usage)
	}
}

func TestNativeCollectionCompleteHistorySummariesAdmitBeforeResult(t *testing.T) {
	budget := dumpformat.NewNativeDeltaBudget(0)
	ctx := dumpformat.WithNativeDeltaBudget(context.Background(), budget)
	store, lease, cleanup := openIdentityTarget(t, "memory")
	defer cleanup()
	l := newJoinedOriginalLedger(t)
	issuer, _, _ := physicalCollectionIntent(t, ctx, l, store, lease)
	defer issuer.Close()
	owner := issuer.Owner()
	metadata, err := owner.NativeSourceHistoryMetadata()
	if err != nil || len(metadata) != 2 {
		t.Fatal("physical source metadata", err)
	}
	metadataSize, err := dumpformat.NativeDeltaJSONSize(metadata)
	if err != nil {
		t.Fatal(err)
	}
	first := NativeHistoryReport{Source: dumpformat.SourceIdentity{StoreID: l.binding.Sources[0].SourceStoreID, Project: l.binding.Sources[0].SourceProject}, Complete: true, VerifiedCommitCount: 97}
	reportSize, err := dumpformat.NativeDeltaJSONSize(first)
	if err != nil {
		t.Fatal(err)
	}
	// Permit the reader's detached metadata and its encoding, but leave one
	// byte less than the first summary needs. No result slice may be returned.
	usage := budget.Usage()
	spare := 2*metadataSize + reportSize - 1
	if usage.LimitBytes-usage.CurrentAccountedDecodedBytes <= spare {
		t.Fatal("unexpected physical fixture reservation size", usage)
	}
	fill := budget.Scope()
	defer fill.Release()
	if err := fill.ChargeBytes("test_history_summary_boundary", usage.LimitBytes-usage.CurrentAccountedDecodedBytes-spare); err != nil {
		t.Fatal(err)
	}
	reports, err := nativeprojection.NativeHistoryAccounting(owner)
	var limit *dumpformat.NativeDeltaDecodedLimitError
	if reports != nil || !errors.As(err, &limit) || limit.Phase != "collection_native_history_reports" {
		t.Fatal("history summary was not refused at its own admission", reports, err)
	}
	metadata, owner = nil, nil
	fill.Release()
	if err := issuer.Close(); err != nil {
		t.Fatal(err)
	}
	usage = budget.Usage()
	if usage.CurrentAccountedDecodedBytes != 0 || len(usage.CurrentCategories) != 0 || len(usage.CurrentCharges) != 0 {
		t.Fatal("history admission refusal retained reservations", usage)
	}
}

// Bazel runfiles may be symlinks. The directory reader's production no-symlink
// guard stays intact; tests stage byte-identical originals as regular files.
func joinedPhysicalDirectory(t *testing.T, database string) string {
	t.Helper()
	directory := t.TempDir()
	for _, source := range joinedPhysicalBinding(t).Sources {
		if source.Database != database {
			continue
		}
		for _, file := range source.Files {
			raw, err := os.ReadFile(filepath.Join(nativeJoinedPhysicalPath, database, file.Name))
			if err != nil || uint64(len(raw)) != file.Bytes || hashBytes(raw) != file.SHA256 {
				t.Fatal("pinned physical fixture bytes differ", file.Name, err)
			}
			path := filepath.Join(directory, file.Name)
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
		}
		return directory
	}
	t.Fatal("unknown pinned physical source")
	return ""
}
