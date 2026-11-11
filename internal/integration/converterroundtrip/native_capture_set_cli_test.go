package converterroundtrip

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

func nativeCLISetDirectory(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("testdata/public-native-delta-small-0e4d5da4.tar.gz")
	if err != nil || hashBytes(raw) != "66e2a1410cb6de0532e8bd453f06353d3fde8159ed9d6ffe4b0ed17f20bf6ef8" {
		t.Fatal("public capture fixture differs", err)
	}
	gz, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	dir := t.TempDir()
	reader := tar.NewReader(gz)
	files := 0
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Typeflag != tar.TypeReg || !strings.HasPrefix(header.Name, "capture-set/") {
			continue
		}
		name := strings.TrimPrefix(header.Name, "capture-set/")
		if name != "capture-set.json" && name != "head.tar" && name != "parts/00000000000000000001.part" {
			t.Fatal("unexpected public capture file", name)
		}
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		value, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, value, 0600); err != nil {
			t.Fatal(err)
		}
		files++
	}
	if files != 3 {
		t.Fatal("capture directory census differs")
	}
	return dir
}

func nativeCLISetRequest(t *testing.T, archive *dumpformat.Archive, engine string) receivingRequest {
	t.Helper()
	ctx := context.Background()
	store := memory.New()
	defer store.Close()
	lease, err := store.Lease(ctx, "public-capture-cli-planner", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	work, release := archive.NativeDeltaAccountingScope()
	defer release()
	plan := nativeHistoryPlanFromArchive(t, work, store, lease)
	plan.Dispositions[0].StorageClass = ""
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
	lifetime := driver.CheckpointLifetimeProcess
	if engine != "memory" {
		lifetime = driver.CheckpointLifetimeRestart
	}
	plan.Target.StoreID, plan.Target.Incarnation, plan.Target.Lifetime = "", "", lifetime
	for i := range plan.Providers {
		plan.Providers[i].Lifetime = lifetime
	}
	return receivingRequest{Format: "plug-native-receiving-request/1", ArchiveSHA256: archive.SHA256(), SourceStoreID: archive.Manifest().StoreID, Projects: []string{archive.Manifest().Projects[0].Prefix}, Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, IdempotencyKey: strings.Repeat("b", 32), SourcePlan: plan}
}

func nativeCLISetExecute(t *testing.T, binary string, args []string, name string) ([]byte, []byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	if out := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); out != "" {
		dir := filepath.Join(out, name)
		if e := os.Mkdir(dir, 0700); e != nil {
			t.Fatal(e)
		}
		for name, data := range map[string][]byte{"argv.json": encode(t, append([]string{binary}, args...)), "stdout.json": stdout.Bytes(), "stderr.txt": stderr.Bytes()} {
			if e := os.WriteFile(filepath.Join(dir, name), data, 0600); e != nil {
				t.Fatal(e)
			}
		}
	}
	return stdout.Bytes(), stderr.Bytes(), err
}

func TestNativeCaptureSetCLIImportsAndReopensEveryDriver(t *testing.T) {
	binary, binaryHash := receivingBinary(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			archive := publicNativeDeltaArchive(t)
			defer archive.ReleaseNativeDecoded()
			capture := nativeCLISetDirectory(t)
			request := nativeCLISetRequest(t, archive, engine)
			raw := encode(t, request)
			path := filepath.Join(t.TempDir(), "request.json")
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			target := ""
			if engine == "sqlite" {
				target = filepath.Join(t.TempDir(), "received.db")
			} else if engine == "postgres" {
				target = identityPostgresDSN(t)
			}
			args := []string{"--capture-set", capture, "--driver", engine, "--target", target, "--request", path, "--expected-binary-sha256", binaryHash}
			stdout, stderr, err := nativeCLISetExecute(t, binary, args, "capture-cli-"+engine)
			if err != nil {
				t.Fatal("capture CLI refused", err, string(stderr))
			}
			var result struct {
				receivingReceipt
				CaptureVerification struct {
					MappedCommits     int    `json:"mapped_commits"`
					ImportedReadback  bool   `json:"imported_byte_readback"`
					ReopenedReadback  bool   `json:"reopened_byte_readback"`
					CompleteSnapshots bool   `json:"complete_snapshots"`
					Reopen            string `json:"reopen"`
				} `json:"capture_set_verification"`
				Measurements struct {
					Decoded      dumpformat.NativeDeltaDecodedUsage `json:"decoded"`
					AfterRelease dumpformat.NativeDeltaDecodedUsage `json:"after_release"`
					Process      struct {
						RSS   uint64 `json:"rss_bytes"`
						HWM   uint64 `json:"hwm_bytes"`
						Error string `json:"error"`
					} `json:"process_memory"`
				} `json:"capture_set_measurements"`
			}
			if err := json.Unmarshal(stdout, &result); err != nil {
				t.Fatal(err)
			}
			if result.Format != "plug-native-receiving-receipt/1" || result.BinarySHA256 != binaryHash || result.RequestSHA256 != hashBytes(raw) || result.ArchiveSHA256 != archive.SHA256() || result.Driver != engine || result.Report.Receipt == nil || result.CaptureVerification.MappedCommits != 32 || !result.CaptureVerification.ImportedReadback || !result.CaptureVerification.ReopenedReadback || !result.CaptureVerification.CompleteSnapshots {
				t.Fatal("capture CLI bindings or full commit census differ")
			}
			m := result.Measurements
			if m.Decoded.LimitBytes != 2147483648 || m.Decoded.LimitOrigin != "default" || m.Decoded.PeakAccountedDecodedBytes == 0 || m.Decoded.PeakAccountedDecodedBytes > m.Decoded.LimitBytes || m.Decoded.PeakRetainedBatchBytes == 0 || m.AfterRelease.CurrentAccountedDecodedBytes != 0 || m.Process.Error != "" || m.Process.RSS == 0 || m.Process.HWM < m.Process.RSS {
				t.Fatal("capture CLI decoded/batch/process measurements differ")
			}
			wantReopen := "driver-reopened"
			if engine == "memory" {
				wantReopen = "same-process-memory-lease-reacquired"
			}
			if result.CaptureVerification.Reopen != wantReopen {
				t.Fatal("capture CLI reopen scope differs")
			}
			if engine == "memory" {
				return
			}
			var store driver.Store
			if engine == "sqlite" {
				store, err = (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: target})
			} else {
				store, err = postgres.Open(context.Background(), driver.Config{DSN: target})
			}
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			_, recovered, found, err := RecoverBoundSourceImport(context.Background(), store)
			if recovered.Archive != nil {
				defer recovered.Archive.ReleaseNativeDecoded()
			}
			if err != nil || !found || !reflect.DeepEqual(recovered.Receipt, result.Report.Receipt) {
				t.Fatal("independent connection recovery differs", err)
			}
			if err := VerifyNativeCaptureSetReadback(context.Background(), store, archive, recovered); err != nil {
				t.Fatal("independent connection table bytes differ", err)
			}
		})
	}
}

func TestNativeCaptureSetCLIRefusesCorruptPartBeforeDestination(t *testing.T) {
	binary, binaryHash := receivingBinary(t)
	archive := publicNativeDeltaArchive(t)
	defer archive.ReleaseNativeDecoded()
	capture := nativeCLISetDirectory(t)
	part := filepath.Join(capture, "parts", "00000000000000000001.part")
	raw, err := os.ReadFile(part)
	if err != nil || len(raw) == 0 {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 1
	if err := os.WriteFile(part, raw, 0600); err != nil {
		t.Fatal(err)
	}
	requestPath := filepath.Join(t.TempDir(), "request.json")
	if err := os.WriteFile(requestPath, encode(t, nativeCLISetRequest(t, archive, "sqlite")), 0600); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "refused.db")
	stdout, stderr, err := nativeCLISetExecute(t, binary, []string{"--capture-set", capture, "--driver", "sqlite", "--target", target, "--request", requestPath, "--expected-binary-sha256", binaryHash}, "capture-cli-corrupt")
	if err == nil || len(stdout) != 0 {
		t.Fatal("corrupt capture produced a success receipt")
	}
	var refusal struct {
		Format  string `json:"format"`
		Phase   string `json:"phase"`
		Metrics struct {
			Process struct {
				RSS uint64 `json:"rss_bytes"`
			} `json:"process_memory"`
		} `json:"measurements"`
	}
	first, _, _ := bytes.Cut(stderr, []byte{'\n'})
	if err := json.Unmarshal(first, &refusal); err != nil || refusal.Format != "plug-native-receiving-refusal/1" || refusal.Phase != "authenticate" || refusal.Metrics.Process.RSS == 0 {
		t.Fatal("refusal omitted phase or process measurements", err, string(stderr))
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatal("corrupt capture created destination", err)
	}
}
