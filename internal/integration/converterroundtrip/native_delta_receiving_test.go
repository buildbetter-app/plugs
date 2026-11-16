package converterroundtrip

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This exact public producer capture was independently reviewed at 0e4d5da4.
// Import/readback here is a separate receiving proof. The fixture contains no
// executable, same-source physical writer ports or preserved native store copy.
func publicNativeDeltaArchive(t *testing.T) *dumpformat.Archive {
	t.Helper()
	raw, err := os.ReadFile("testdata/public-native-delta-small-0e4d5da4.tar.gz")
	if err != nil {
		t.Fatal(err)
	}
	if hashBytes(raw) != "66e2a1410cb6de0532e8bd453f06353d3fde8159ed9d6ffe4b0ed17f20bf6ef8" {
		t.Fatal("public capture bytes changed")
	}
	compressed, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer compressed.Close()
	files := map[string][]byte{}
	tarReader := tar.NewReader(compressed)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Typeflag != tar.TypeReg || !strings.HasPrefix(header.Name, "capture-set/") {
			continue
		}
		value, err := io.ReadAll(tarReader)
		if err != nil {
			t.Fatal(err)
		}
		files[strings.TrimPrefix(header.Name, "capture-set/")] = value
	}
	if len(files) != 3 {
		t.Fatal("public source file census changed")
	}
	archive, err := dumpformat.ReadNativeCaptureSet(context.Background(), bytes.NewReader(files["capture-set.json"]), bytes.NewReader(files["head.tar"]), func(_ context.Context, n uint64) (io.ReadCloser, error) {
		raw, ok := files[fmt.Sprintf("parts/%020d.part", n)]
		if !ok {
			return nil, os.ErrNotExist
		}
		return io.NopCloser(bytes.NewReader(raw)), nil
	}, dumpformat.NativeCaptureSetOptions{})
	if err != nil {
		t.Fatal("authenticate genuine public delta capture", err)
	}
	if archive.SHA256() != "fd910c93ebad8da226c7ff6cc84f49c46f87aafe2c3530a4a27bf48280e4c67d" {
		t.Fatal("public source identity changed")
	}
	return archive
}

func TestNativeDeltaRowReceivingEveryMappingAndReopenedStore(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			archive := publicNativeDeltaArchive(t)
			defer archive.ReleaseNativeDecoded()
			defer func() {
				usage, _ := archive.NativeDecodedUsage()
				receipt := map[string]any{
					"format":                 "native-row-receiver-rehearsal-memory/1",
					"driver":                 engine,
					"decoded_scope":          "receiver/converter-controlled payloads including optional workflow hooks",
					"decoded":                usage,
					"process_memory":         nativeTestProcessMemory(),
					"uncharged_sites":        nativeRehearsalRSSSites(),
					"authority":              "orchestrator62/a64ec1ed-1b97-47df-b8d7-4ca840564277",
					"required_before_switch": "lowest-layer streaming OfflineHistory with in-driver accounting; no rehearsal switch credit",
				}
				raw, err := json.Marshal(receipt)
				if err != nil {
					t.Error("memory receipt", err)
					return
				}
				t.Logf("native_receiving_memory_receipt=%s", raw)
			}()
			ctx := archive.WithNativeDeltaAccounting(context.Background())
			baseline, _ := archive.NativeDecodedUsage()
			store, lease, reopen := openIdentityTarget(t, engine)
			// The WithLease entry point leaves renewal with its caller. This
			// public conversion can span LeaseTTL. Replace the helper's initial
			// lease with the existing caller-owned renewal mechanism, preserving
			// its TTL, fencing and cancellation behavior.
			if err := lease.Release(ctx); err != nil {
				t.Fatal("release initial public receiving lease", err)
			}
			op, lease, release, err := HoldRestoreLease(ctx, store, "native-receive")
			if err != nil {
				t.Fatal("hold public receiving lease", err)
			}
			release = sync.OnceValue(release)
			defer func() {
				if err := release(); err != nil {
					t.Error("release renewed public receiving lease", err)
				}
			}()
			// The host-plan helper consumes detached raw source records. Its
			// temporary copies end when the plan is built, independently of the
			// original archive and the converter's own operation scopes.
			plan := func() driver.SourcePlanePlanV1 {
				work, release := archive.NativeDeltaAccountingScope()
				defer release()
				return nativeHistoryPlanFromArchive(t, work, store, lease)
			}()
			// This genuine fixture declares SQL NULL, rather than the separate
			// synthetic helper's versioned storage class. Keep that distinction
			// explicit in host intent and verify every source row against it.
			plan.Dispositions[0].StorageClass = ""
			for _, row := range archive.Records("bead") {
				storage, present := row.Source.Fields["storage_class"]
				if !plan.Dispositions[0].MatchesStorageClass(storage, present) {
					t.Fatal("public fixture host storage intent differs from original")
				}
			}
			statuses := map[string]bool{}
			for _, project := range archive.Manifest().Projects {
				for _, status := range project.Statuses {
					statuses[status.Name] = true
				}
			}
			plan.Dispositions[0].Statuses = nil
			for name := range statuses {
				plan.Dispositions[0].Statuses = append(plan.Dispositions[0].Statuses, driver.SourcePlaneStringPairV1{From: name, To: name})
			}
			sort.Slice(plan.Dispositions[0].Statuses, func(i, j int) bool {
				return plan.Dispositions[0].Statuses[i].From < plan.Dispositions[0].Statuses[j].From
			})
			profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: driver.StoreID(archive.Manifest().StoreID), Method: "offline.restore.import", IdempotencyKey: strings.Repeat("b", 32), ArchiveDigest: archive.SHA256(), HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: nativeFixtureTime.Format("2006-01-02T15:04:05.999999999Z07:00")}
			profile, err = BindNativeCaptureSetImport(op, archive, profile, plan)
			if err != nil {
				t.Fatal("bind row-granular public capture", err)
			}
			// The bound profile remains live through receiving and readback;
			// source/cache/scratch cleanup must conserve everything else.
			baseline = receivingUsageWithProfile(t, baseline, profile)
			report, err := RestoreNativeCaptureSetImportWithLease(op, archive, store, lease, profile)
			if err != nil {
				t.Fatal("receive row-granular public capture", err)
			}
			if held, _ := archive.NativeDecodedUsage(); held.CurrentAccountedDecodedBytes != baseline.CurrentAccountedDecodedBytes || !reflect.DeepEqual(held.CurrentCategories, baseline.CurrentCategories) || !reflect.DeepEqual(held.CurrentCharges, baseline.CurrentCharges) {
				t.Fatal("completed conversion retained temporary allocations", baseline, held)
			}
			if err := VerifyNativeCaptureSetReadback(op, store, archive, report); err != nil {
				t.Fatal("every source-mapped row/meta/old table byte oracle", err)
			}
			counts := map[string]int{}
			for _, family := range report.NativeFamilies {
				counts[family.Family] = family.ImportedRows
			}
			for name, want := range map[string]int{"dolt_schemas": 3, "federation_peers": 1, "interactions": 1, "routes": 1} {
				if counts[name] != want {
					t.Fatal("opaque evidence rows changed", name, counts[name])
				}
			}
			proof, ok := archive.NativeDeltaVerification()
			if !ok || proof.Totals.Commits != 32 || proof.Usage.LimitBytes != 2147483648 || proof.Usage.Accounting != dumpformat.NativeDeltaDecodedAccounting {
				t.Fatal("source/accounting domain changed")
			}
			// Drain and release before closing the store. Recovery keeps the
			// original accounting context because release cancels only op.
			if err := release(); err != nil {
				t.Fatal("release public receiving lease before reopen", err)
			}
			store, lease = reopen()
			_ = lease
			_, recovered, found, err := RecoverBoundSourceImport(ctx, store)
			if recovered.Archive != nil {
				defer recovered.Archive.ReleaseNativeDecoded()
			}
			if err != nil || !found || !reflect.DeepEqual(recovered.Receipt, report.Receipt) || !reflect.DeepEqual(recovered.NativeFamilies, report.NativeFamilies) {
				t.Fatal("retained source/census/receipt lost on reopen", err)
			}
			if err := VerifyNativeCaptureSetReadback(ctx, store, archive, recovered); err != nil {
				t.Fatal("reopened every mapped native table bytes", err)
			}
			// RecoverBoundSourceImport has closed the source snapshot before it
			// returns. Read back through the returned archive itself, rather than
			// the separately opened public fixture above: its authenticated
			// preparation must outlive that snapshot until the caller releases it.
			if recovered.Archive == nil {
				t.Fatal("recovery did not retain the source archive")
			}
			if err := VerifyNativeCaptureSetReadback(ctx, store, recovered.Archive, recovered); err != nil {
				t.Fatal("returned retained archive readback after source snapshot close", err)
			}
			recovered.Archive.ReleaseNativeDecoded()
			usage, _ := archive.NativeDecodedUsage()
			if usage.CurrentAccountedDecodedBytes != baseline.CurrentAccountedDecodedBytes || !reflect.DeepEqual(usage.CurrentCategories, baseline.CurrentCategories) || !reflect.DeepEqual(usage.CurrentCharges, baseline.CurrentCharges) {
				t.Fatal("recovery/readback ownership leaked", baseline, usage)
			}
			t.Logf("native_receiving_decoded_usage=%+v test_process_memory=%v", usage, nativeTestProcessMemory())
		})
	}
}

// These are measured Linux Go-test process values, separate from decoded
// payload accounting. The process includes the test harness and store engine.
func nativeTestProcessMemory() map[string]any {
	out := map[string]any{"scope": "go-test process; harness and engine included"}
	raw, err := os.ReadFile("/proc/self/status")
	if err != nil {
		out["measurement"] = "NOT_MEASURED"
		return out
	}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 && (fields[0] == "VmRSS:" || fields[0] == "VmHWM:") {
			value, err := strconv.ParseUint(fields[1], 10, 64)
			if err == nil {
				out[strings.TrimSuffix(fields[0], ":")+"_bytes"] = value * 1024
			}
		}
	}
	return out
}

// This inventory is part of the versioned rehearsal receipt, not an assertion
// of in-driver accounting. The source-bound delivery records the exact blobs
// and allocation sites. Receiver/converter/workflow-owned candidates still
// reserve against the unchanged 2 GiB budget.
func nativeRehearsalRSSSites() []map[string]string {
	return []map[string]string{
		{"site": "driver.RestoreImportProfile.Detached/RequestHash", "state": "uncharged, driver-internal, RSS-covered", "domain": "source-plan detach/decode and profile JSON/decoded copies"},
		{"site": "driver.SourcePlaneImportBindingV1.Detached/Plan", "state": "uncharged, driver-internal, RSS-covered", "domain": "canonical-plan clone and repeated decode/canonical validation"},
		{"site": "driver.buildOfflineHistory", "state": "uncharged, driver-internal, RSS-covered", "domain": "detached history JSON/decoded request, state map, row body, version body/patch and final plan copies"},
		{"site": "driver.prepareOfflineHistoryBinding/PreparedOfflineHistory.Finalize", "state": "uncharged, driver-internal, RSS-covered", "domain": "profile/history detach, retained prepared history, repeated complete plan build"},
		{"site": "driver.prepareSourcePlaneProfile/SourcePlaneImportPublication.Validate", "state": "uncharged, driver-internal, RSS-covered", "domain": "detached profile/plan, binding JSON and publication validation candidates"},
		{"site": "memory.transaction.CommitOfflineHistory", "state": "uncharged, driver-internal, RSS-covered", "domain": "state fork, record/blob copies, version/index/receipt publication"},
		{"site": "sqlite.transaction.CommitOfflineHistory", "state": "uncharged, driver-internal, RSS-covered", "domain": "record/version/allocator/changeset/receipt JSON buffers and SQLite state"},
		{"site": "postgres.transaction.CommitOfflineHistory", "state": "uncharged, driver-internal, RSS-covered", "domain": "plan and row/version/receipt publication buffers"},
		{"site": "dumpformat.SourceEdgeMetadata/InspectSourceEdgeMetadata/sourceJSONValue", "state": "uncharged, core-internal, RSS-covered under the named rehearsal ruling", "domain": "cloned value/envelope projection, JSON buffers, validation/shape/decoded trees and typed predicate"},
	}
}
