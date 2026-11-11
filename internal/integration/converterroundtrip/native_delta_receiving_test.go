package converterroundtrip

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
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
			ctx := archive.WithNativeDeltaAccounting(context.Background())
			baseline, _ := archive.NativeDecodedUsage()
			store, lease, reopen := openIdentityTarget(t, engine)
			plan := nativeHistoryPlanFromArchive(t, archive, store, lease)
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
			profile, err := BindNativeCaptureSetImport(ctx, archive, profile, plan)
			if err != nil {
				t.Fatal("bind row-granular public capture", err)
			}
			report, err := RestoreNativeCaptureSetImportWithLease(ctx, archive, store, lease, profile)
			if err != nil {
				t.Fatal("receive row-granular public capture", err)
			}
			if held, _ := archive.NativeDecodedUsage(); held.CurrentAccountedDecodedBytes != baseline.CurrentAccountedDecodedBytes {
				t.Fatal("completed conversion retained temporary allocations", baseline, held)
			}
			if err := VerifyNativeCaptureSetReadback(ctx, store, archive, report); err != nil {
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
			store, lease = reopen()
			_ = lease
			_, recovered, found, err := RecoverBoundSourceImport(ctx, store)
			if err != nil || !found || !reflect.DeepEqual(recovered.Receipt, report.Receipt) || !reflect.DeepEqual(recovered.NativeFamilies, report.NativeFamilies) {
				t.Fatal("retained source/census/receipt lost on reopen", err)
			}
			if err := VerifyNativeCaptureSetReadback(ctx, store, archive, recovered); err != nil {
				t.Fatal("reopened every mapped native table bytes", err)
			}
			if recovered.Archive != nil {
				recovered.Archive.ReleaseNativeDecoded()
			}
			usage, _ := archive.NativeDecodedUsage()
			if usage.CurrentAccountedDecodedBytes != baseline.CurrentAccountedDecodedBytes {
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
