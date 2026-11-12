package historyverify

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func nativeVerifierHashProfile(t *testing.T, archive *dumpformat.Archive) (driver.RestoreImportProfile, time.Time) {
	t.Helper()
	when := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	plan := driver.SourcePlanePlanV1{
		Version: driver.SourcePlanePlanVersionV1, AddressContract: driver.SourcePlaneAddressContractV1,
		SourceStoreID: archive.Manifest().StoreID, ArchiveSHA256: archive.SHA256(),
		Target:    driver.SourcePlaneTargetV1{StoreID: "target", Incarnation: "incarnation", RequireEmpty: true, Lifetime: driver.CheckpointLifetimeProcess, RuntimeContract: driver.SourcePlaneRuntimeContractV1},
		Providers: []driver.SourcePlaneProviderV1{{Alias: "workflow", Instance: "default", Namespace: "plugin/workflow/default/durable", Schema: "workflow/source-state/1", Codec: "workflow/json/1", RuntimeContract: driver.SourcePlaneRuntimeContractV1, Lifetime: driver.CheckpointLifetimeProcess, LogicalImageSize: 2, LogicalImageSHA256: nativeSHA([]byte(`{}`)), OriginAdapter: "beads-workflow-origins/1"}},
	}
	binding, err := driver.NewSourcePlaneImportBindingV1(plan)
	if err != nil {
		t.Fatal(err)
	}
	return driver.RestoreImportProfile{
		Admission:     driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)},
		SourceStoreID: driver.StoreID(plan.SourceStoreID), Method: "offline.restore.import", IdempotencyKey: "native-profile-hash-accounting",
		ArchiveDigest: archive.SHA256(), DerivedDigest: strings.Repeat("c", 64), HistoryTime: when.Format(time.RFC3339Nano), SourcePlanes: &binding,
	}, when
}

func sameNativeHashReservations(t *testing.T, before, after dumpformat.NativeDeltaDecodedUsage) {
	t.Helper()
	if before.CurrentAccountedDecodedBytes != after.CurrentAccountedDecodedBytes || !reflect.DeepEqual(before.CurrentCategories, after.CurrentCategories) || !reflect.DeepEqual(before.CurrentCharges, after.CurrentCharges) {
		t.Fatal("temporary verifier profile candidates survived their lifetime", before.CurrentAccountedDecodedBytes, after.CurrentAccountedDecodedBytes, before.CurrentCategories, after.CurrentCategories)
	}
}

func TestNativeVerifierPreparationHashReleasesTemporaryProfileCandidates(t *testing.T) {
	archive := nativeVerifierArchive(t)
	profile, _ := nativeVerifierHashProfile(t, archive)
	want, err := profile.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	before, _ := archive.NativeDecodedUsage()
	for range 4 {
		got, err := nativePreparationHash(archive, profile)
		if err != nil || got != want {
			t.Fatal("accounted verifier hash differs from the complete request", got, want, err)
		}
		after, _ := archive.NativeDecodedUsage()
		sameNativeHashReservations(t, before, after)
	}
	after, _ := archive.NativeDecodedUsage()
	if after.CumulativeCategories["restore_profile_hash_base64"]-before.CumulativeCategories["restore_profile_hash_base64"] != 4*1024 {
		t.Fatal("actual fixed hash buffer was not admitted for each call", after)
	}
}

func TestNativeVerifierPreparationReleasesProfileBeforeTransferAndOnRefusal(t *testing.T) {
	archive := nativeVerifierArchive(t)
	if _, err := archive.PrepareNativeDelta(context.Background()); err != nil {
		t.Fatal(err)
	}
	profile, when := nativeVerifierHashProfile(t, archive)
	before, _ := archive.NativeDecodedUsage()
	stream, err := PrepareNativeArchive(context.Background(), archive, profile, when)
	if err != nil {
		t.Fatal(err)
	}
	live, _ := archive.NativeDecodedUsage()
	if live.CurrentCategories["verifier_preparation_hash"] != before.CurrentCategories["verifier_preparation_hash"]+64 {
		t.Fatal("stream does not retain exactly its preparation hash", live)
	}
	for phase, n := range live.CurrentCategories {
		profilePhase := strings.HasPrefix(phase, "profile_") || strings.HasPrefix(phase, "restore_profile_hash_") || strings.HasPrefix(phase, "source_plan_")
		if profilePhase && n != before.CurrentCategories[phase] {
			t.Fatal("detached profile or hash scratch escaped into the stream", phase, n, before.CurrentCategories[phase])
		}
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	after, _ := archive.NativeDecodedUsage()
	sameNativeHashReservations(t, before, after)

	profile.HistoryDigest = strings.Repeat("b", 64)
	stream, err = PrepareNativeArchive(context.Background(), archive, profile, when)
	var refusal ErrCheckpoint
	if stream != nil || !errors.As(err, &refusal) || refusal.Reason != "archive transcript digest differs" {
		t.Fatal("actual transcript mismatch was not reached", stream, err)
	}
	after, _ = archive.NativeDecodedUsage()
	sameNativeHashReservations(t, before, after)
}
