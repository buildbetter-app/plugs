package historyverify

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const physicalPairPath = "../../core/dumpformat/testdata/native-joined-public-ca26683b"

func nativeCollectionPhysicalBinding(t *testing.T) dumpformat.NativeJoinedBinding {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(physicalPairPath, "creation-record.json"))
	if err != nil || nativeSHA(raw) != "2e0e02826753f83f9bf72c621a2863ea7312c16af8875bb685495fa7fdc9c0e6" {
		t.Fatal("physical pair creation record", err)
	}
	var record struct {
		Pair []struct {
			Database         string                         `json:"database"`
			RehearsalProject string                         `json:"rehearsal_project"`
			Files            []dumpformat.NativeCaptureFile `json:"ordered_file_ledger"`
		}
	}
	if err = json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	binding := dumpformat.NativeJoinedBinding{Format: dumpformat.NativeJoinedInputsFormatV2, PolicySHA256: strings.Repeat("a", 64), ProfileSHA256: strings.Repeat("b", 64)}
	for _, source := range record.Pair {
		raw, err := os.ReadFile(filepath.Join(physicalPairPath, source.Database, "capture-set.json"))
		if err != nil {
			t.Fatal(err)
		}
		var root dumpformat.NativeCaptureSet
		if err = json.Unmarshal(raw, &root); err != nil {
			t.Fatal(err)
		}
		binding.Sources = append(binding.Sources, dumpformat.NativeJoinedInput{RehearsalProject: source.RehearsalProject, Database: source.Database, SourceStoreID: root.StoreID, SourceProject: root.Projects[0], ArchiveSHA256: nativeSHA(raw), Files: source.Files})
	}
	if len(binding.Sources) != 2 {
		t.Fatal("physical pair is incomplete")
	}
	return binding
}

func newPhysicalNativeCollection(t *testing.T, limit uint64) (*NativeCollection, *dumpformat.NativeDeltaBudget, context.Context) {
	t.Helper()
	binding := nativeCollectionPhysicalBinding(t)
	raw, err := json.Marshal(binding)
	if err != nil {
		t.Fatal(err)
	}
	budget := dumpformat.NewNativeDeltaBudget(limit)
	ctx := dumpformat.WithNativeDeltaBudget(context.Background(), budget)
	when := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "fixture-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("c", 64)}, SourceStoreID: driver.StoreID("merge-" + nativeSHA(raw)), Method: "offline.restore.import", IdempotencyKey: "physical-native-collection-fixture", ArchiveDigest: nativeSHA(raw), HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: when.Format(time.RFC3339Nano)}
	target := driver.SourcePlaneTargetV1{StoreID: "fixture-target", Incarnation: "fixture-incarnation", RequireEmpty: true, Lifetime: driver.CheckpointLifetimeProcess, RuntimeContract: driver.SourcePlaneRuntimeContractV1}
	provider := driver.SourcePlaneProviderV1{Alias: "workflow", Instance: "default", Namespace: "plugin/workflow/default/durable", Schema: "workflow/source-state/1", Codec: "workflow/json/1", RuntimeContract: driver.SourcePlaneRuntimeContractV1, Lifetime: driver.CheckpointLifetimeProcess, OriginAdapter: "beads-workflow-origins/1"}
	c, err := NewNativeCollection(ctx, binding, NativeCollectionAdmission{Profile: profile, Target: target, Provider: provider}, when)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	return c, budget, ctx
}

func appendPhysicalNative(t *testing.T, c *NativeCollection, ctx context.Context, database string) {
	t.Helper()
	if err := c.Append(ctx, func(ctx context.Context, options dumpformat.NativeCaptureSetOptions) (*dumpformat.Archive, error) {
		return dumpformat.ReadNativeCaptureSetDirectory(ctx, nativeCollectionPhysicalDirectory(t, database), options)
	}); err != nil {
		t.Fatal("append exact physical source", database, err)
	}
}

func TestNativeCollectionPhysicalSourcesSpoolInOrdinalOrderAndReleasePreparations(t *testing.T) {
	c, budget, ctx := newPhysicalNativeCollection(t, 0)
	for _, database := range []string{"ops", "sv2"} {
		appendPhysicalNative(t, c, ctx, database)
		usage := budget.Usage()
		for _, category := range []string{"receiving_staged_originals", "receiving_staged_changes", "receiving_staged_checkpoints", "retained_commit_proofs", "head_tar"} {
			if usage.CurrentCategories[category] != 0 {
				t.Fatal("source-owned history escaped sequential release", category, usage.CurrentCategories[category])
			}
		}
	}
	a, err := c.FinishCurrent()
	if err != nil {
		t.Fatal(err)
	}
	if !a.HasNativeCaptureCollection() || a.HasNativeDelta() || c.commits != 194 || c.collisions != 0 || len(c.stream.files) != 6 {
		t.Fatal("incomplete physical collection")
	}
	if _, err := c.stream.file.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	counts := [2]uint64{}
	ordinal := 0
	for {
		frame, release, err := c.stream.readFrame(ctx, c.stream.file, NativeArchiveLimits())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if commit := frame.Commit; commit != nil {
			if commit.SourceStoreID != c.binding.Sources[ordinal].SourceStoreID {
				ordinal++
				if ordinal != 1 || commit.SourceStoreID != c.binding.Sources[ordinal].SourceStoreID {
					t.Fatal("source blocks were interleaved")
				}
			}
			counts[ordinal]++
			if commit.Project != c.binding.Sources[ordinal].SourceProject || commit.SourceSequence != counts[ordinal] || commit.Sequence != counts[0]+counts[1] {
				t.Fatal("original/local/global sequence differs", commit)
			}
		}
		if frame.End != nil {
			t.Fatal("independent source transcript was prematurely sealed")
		}
		release()
	}
	if counts != [2]uint64{97, 97} {
		t.Fatal("physical commit census differs", counts)
	}
	if _, err := c.stream.Next(ctx, NativeArchiveLimits()); err == nil {
		t.Fatal("unsealed collection acquired consumer authority")
	}
	dir := c.dir
	a = nil
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("private original/transcript spool escaped close", err)
	}
	usage := budget.Usage()
	if usage.CurrentAccountedDecodedBytes != 0 || usage.PeakAccountedDecodedBytes > dumpformat.NativeDeltaDefaultDecodedLimit {
		t.Fatal("collection cleanup/cap differs", usage)
	}
	t.Logf("physical_native_collection_usage=%+v", usage)
}

func TestNativeCollectionSecondSourceFailureDropsFirstSourceAndPrivateSpool(t *testing.T) {
	c, budget, ctx := newPhysicalNativeCollection(t, 0)
	appendPhysicalNative(t, c, ctx, "ops")
	want := errors.New("second original close refusal")
	if err := c.Append(ctx, func(context.Context, dumpformat.NativeCaptureSetOptions) (*dumpformat.Archive, error) {
		return nil, want
	}); !errors.Is(err, want) {
		t.Fatal("second-source refusal lost", err)
	}
	if _, err := c.FinishCurrent(); err == nil {
		t.Fatal("failed pair yielded current image")
	}
	dir := c.dir
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) || budget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("failed pair retained resources", err, budget.Usage())
	}
}

func TestNativeCollectionCorruptSecondPhysicalOriginalCannotYieldPlan(t *testing.T) {
	c, budget, ctx := newPhysicalNativeCollection(t, 0)
	appendPhysicalNative(t, c, ctx, "ops")
	directory := t.TempDir()
	for _, file := range c.binding.Sources[1].Files {
		raw, err := os.ReadFile(filepath.Join(physicalPairPath, "sv2", file.Name))
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(file.Name, "parts/") {
			raw[len(raw)-1] ^= 1
		}
		path := filepath.Join(directory, file.Name)
		if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	err := c.Append(ctx, func(ctx context.Context, options dumpformat.NativeCaptureSetOptions) (*dumpformat.Archive, error) {
		return dumpformat.ReadNativeCaptureSetDirectory(ctx, directory, options)
	})
	if err == nil || !c.failed || c.next != 1 {
		t.Fatal("changed original was admitted", err)
	}
	if _, err = c.FinishCurrent(); err == nil {
		t.Fatal("corrupt second source yielded a current Plan candidate")
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	if budget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("corrupt-source abort leaked decoded owner")
	}
}

func TestNativeCollectionCancellationPermanentlyClosesSourceAdmission(t *testing.T) {
	c, budget, ctx := newPhysicalNativeCollection(t, 0)
	appendPhysicalNative(t, c, ctx, "ops")
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	called := false
	open := func(context.Context, dumpformat.NativeCaptureSetOptions) (*dumpformat.Archive, error) {
		called = true
		return nil, errors.New("must not open")
	}
	if err := c.Append(cancelled, open); !errors.Is(err, context.Canceled) || called {
		t.Fatal("cancellation opened the next source", err)
	}
	if err := c.Append(ctx, open); err == nil || called {
		t.Fatal("cancelled collection resumed admission")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if budget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("cancelled collection leaked decoded owner")
	}
}

func TestNativeCollectionSharedLimitRefusesBeforeRetainingSource(t *testing.T) {
	c, budget, ctx := newPhysicalNativeCollection(t, 1<<20)
	err := c.Append(ctx, func(ctx context.Context, options dumpformat.NativeCaptureSetOptions) (*dumpformat.Archive, error) {
		return dumpformat.ReadNativeCaptureSetDirectory(ctx, nativeCollectionPhysicalDirectory(t, "ops"), options)
	})
	var limit *dumpformat.NativeDeltaDecodedLimitError
	if !errors.As(err, &limit) || limit.LimitBytes != 1<<20 || !c.failed || c.next != 0 {
		t.Fatal("shared owner did not refuse source admission", err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	usage := budget.Usage()
	if usage.CurrentAccountedDecodedBytes != 0 || usage.PeakAccountedDecodedBytes > 1<<20 {
		t.Fatal("limit refusal leaked/exceeded shared ceiling", usage)
	}
}

func TestNativeCollectionFinalAdmissionSubstitutionRefusesBeforePlanSeal(t *testing.T) {
	c, budget, ctx := newPhysicalNativeCollection(t, 0)
	appendPhysicalNative(t, c, ctx, "ops")
	appendPhysicalNative(t, c, ctx, "sv2")
	if _, err := c.FinishCurrent(); err != nil {
		t.Fatal(err)
	}
	profile := c.admission.Profile
	provider := c.admission.Provider
	provider.LogicalImageSHA256 = strings.Repeat("d", 64)
	plan := driver.SourcePlanePlanV1{Version: driver.SourcePlanePlanVersionV1, AddressContract: driver.SourcePlaneAddressContractV1, SourceStoreID: string(profile.SourceStoreID), ArchiveSHA256: profile.ArchiveDigest, Target: c.admission.Target, Providers: []driver.SourcePlaneProviderV1{provider}}
	binding, err := driver.NewSourcePlaneImportBindingV1(plan)
	if err != nil {
		t.Fatal("negative fixture's canonical source plan", err)
	}
	profile.SourcePlanes = &binding
	profile.Admission.Actor.ID = "late-different-actor"
	_, actual, _, err := c.Bind(ctx, profile)
	var refusal ErrCheckpoint
	if !errors.As(err, &refusal) || refusal.Reason != "collection fixed admission/provider differs" || actual.Built() || c.stream.sealed {
		t.Fatal("late admission reached authoritative plan/seal", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if budget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("refused final bind leaked owner")
	}
}

func TestNativeCollectionBindingEncodingRequiresSeparateLiveReservation(t *testing.T) {
	c, budget, _ := newPhysicalNativeCollection(t, 0)
	binding := nativeCollectionPhysicalBinding(t)
	owner := c.Owner()
	if err := owner.ChargeNativeDeltaJSON("test_retained_binding", binding); err != nil {
		t.Fatal(err)
	}
	n, err := dumpformat.NativeDeltaJSONSize(binding)
	if err != nil || n == 0 {
		t.Fatal("binding size", err)
	}
	before := budget.Usage().CurrentAccountedDecodedBytes
	block := budget.Scope()
	if err = block.ChargeBytes("test_other_live_payloads", budget.Usage().LimitBytes-before-n+1); err != nil {
		t.Fatal(err)
	}
	detached, err := detachNativeCollectionBinding(owner, binding)
	var limit *dumpformat.NativeDeltaDecodedLimitError
	if !errors.As(err, &limit) || limit.Phase != "collection_issuer_encoding" || len(detached.Sources) != 0 {
		t.Fatal("encoding allocated without its own admission", err)
	}
	block.Release()
	if budget.Usage().CurrentAccountedDecodedBytes != before {
		t.Fatal("refused encoding retained a reservation")
	}
	detached, err = detachNativeCollectionBinding(owner, binding)
	if err != nil || len(detached.Sources) != 2 {
		t.Fatal("admitted binding clone", err)
	}
	binding.Sources[0].Files[0].Name = "changed-after-detach"
	if detached.Sources[0].Files[0].Name != "capture-set.json" || budget.Usage().CurrentCategories["collection_issuer_encoding"] != 0 || budget.Usage().CurrentAccountedDecodedBytes != before {
		t.Fatal("encoding scratch retained or detached binding aliases input")
	}
	detached, binding = dumpformat.NativeJoinedBinding{}, dumpformat.NativeJoinedBinding{}
	if err = c.Close(); err != nil || budget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("binding owner cleanup", err, budget.Usage())
	}
}

// Bazel runfiles may be symlinks. The directory reader's production no-symlink
// guard stays intact; tests stage byte-identical originals as regular files.
func nativeCollectionPhysicalDirectory(t *testing.T, database string) string {
	t.Helper()
	directory := t.TempDir()
	for _, source := range nativeCollectionPhysicalBinding(t).Sources {
		if source.Database != database {
			continue
		}
		for _, file := range source.Files {
			raw, err := os.ReadFile(filepath.Join(physicalPairPath, database, file.Name))
			if err != nil || uint64(len(raw)) != file.Bytes || nativeSHA(raw) != file.SHA256 {
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
