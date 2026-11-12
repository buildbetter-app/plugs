package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

func TestNativeDeltaCaptureCLIMemoryPartial(t *testing.T) {
	runNativeDeltaCaptureCLIHistoryMode(t, "memory", "partial", 2)
}

func TestNativeDeltaCaptureCLIMemorySnapshot(t *testing.T) {
	runNativeDeltaCaptureCLIHistoryMode(t, "memory", "snapshot", 0)
}

func TestNativeDeltaCaptureCLISqlitePartial(t *testing.T) {
	runNativeDeltaCaptureCLIHistoryMode(t, "sqlite", "partial", 17) // Populated authenticated predecessor.
}

func TestNativeDeltaCaptureCLISqliteSnapshot(t *testing.T) {
	runNativeDeltaCaptureCLIHistoryMode(t, "sqlite", "snapshot", 0)
}

func TestNativeDeltaCaptureCLIPostgresPartial(t *testing.T) {
	runNativeDeltaCaptureCLIHistoryMode(t, "postgres", "partial", 2)
}

func TestNativeDeltaCaptureCLIPostgresSnapshot(t *testing.T) {
	runNativeDeltaCaptureCLIHistoryMode(t, "postgres", "snapshot", 0)
}

func TestNativeDeltaCaptureCLIMemoryPartialFirstSequence(t *testing.T) {
	runNativeDeltaCaptureCLIHistoryMode(t, "memory", "partial", 1)
}

func TestNativeDeltaCaptureCLISqlitePartialFirstSequence(t *testing.T) {
	runNativeDeltaCaptureCLIHistoryMode(t, "sqlite", "partial", 1)
}

func TestNativeDeltaCaptureCLIPostgresPartialFirstSequence(t *testing.T) {
	runNativeDeltaCaptureCLIHistoryMode(t, "postgres", "partial", 1)
}

func runNativeDeltaCaptureCLIHistoryMode(t *testing.T, engine, mode string, first uint64) {
	t.Helper()
	binary, binaryHash := receivingBinary(t)
	archive := publicNativeDeltaArchive(t)
	defer archive.ReleaseNativeDecoded()
	capture := nativeCLISetDirectory(t)
	request := nativeCLISetRequest(t, archive, engine)
	var wire map[string]any
	decode(t, encode(t, request), &wire)
	wire["restore_mode"] = mode
	if mode == "partial" {
		wire["restore_first_sequence"] = first
	}
	raw := encode(t, wire)
	requestPath := filepath.Join(t.TempDir(), "request.json")
	if err := os.WriteFile(requestPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	target := ""
	if engine == "sqlite" {
		target = filepath.Join(t.TempDir(), "restored.db")
	}
	if engine == "postgres" {
		target = identityPostgresDSN(t)
	}
	stdout, stderr, err := nativeCLISetExecute(t, binary, []string{"--capture-set", capture, "--driver", engine, "--target", target, "--request", requestPath, "--expected-binary-sha256", binaryHash}, "native-mode-"+engine+"-"+mode)
	if err != nil {
		t.Fatalf("actual mode CLI: %v: %s", err, stderr)
	}
	var result struct {
		Format        string `json:"format"`
		HistoryMode   string `json:"history_mode"`
		RequestSHA256 string `json:"request_sha256"`
		Report        Report `json:"committed_report"`
		Verification  struct {
			Mapped   int  `json:"mapped_commits"`
			Imported bool `json:"imported_byte_readback"`
			Reopened bool `json:"reopened_byte_readback"`
		} `json:"capture_set_verification"`
	}
	decode(t, stdout, &result)
	if result.Format != "plug-native-receiving-receipt/1" || result.HistoryMode != mode || result.RequestSHA256 != hashBytes(raw) || !result.Verification.Imported || !result.Verification.Reopened {
		t.Fatal("actual mode/request/readbacks differ")
	}
	e := result.Report.NativeDeltaHistory
	if e == nil || e.Format != driver.NativeDeltaHistoryEvidenceFormat || e.Mode != driver.RestoreHistoryMode(mode) || e.ArchiveSHA256 != archive.SHA256() || e.HeadSequence != 32 || result.Report.Receipt == nil {
		t.Fatal("typed source provenance missing")
	}
	if mode == "partial" {
		baseline, head, floor := uint64(0), uint64(33), model.Natural64(0)
		var gap *driver.SourceHistoryGap
		if first > 1 {
			baseline, head, floor = first-1, 35-first, model.Natural64(1)
			gap = &driver.SourceHistoryGap{First: 1, Last: first - 1, Reason: "operator-selected-floor"}
		}
		if e.FirstSequence != first || e.BaselineSequence != baseline || !reflect.DeepEqual(e.OmittedPrefix, gap) || result.Verification.Mapped != int(33-first) || result.Report.Restored.HistoryFloor != floor || result.Report.Restored.Head != model.Natural64(head) {
			t.Fatal("partial source floor/gap/mappings differ", e)
		}
		if first == 1 && (e.BaselineCommitID != "" || e.BaselineRootSHA256 != "") {
			t.Fatal("first source sequence fabricated a predecessor", e)
		}
	} else if e.FirstSequence != 0 || e.BaselineSequence != 0 || e.OmittedPrefix != nil || result.Verification.Mapped != 0 || result.Report.Restored.Head != model.Natural64(1) || result.Report.Restored.HistoryFloor != model.Natural64(1) {
		t.Fatal("snapshot fabricated source history", e)
	}
	t.Logf("native_delta_mode_receipt engine=%s mode=%s request=%s archive=%s first=%d head=%d mapped=%d floor=%s target_head=%s current_root=%s", engine, mode, result.RequestSHA256, e.ArchiveSHA256, e.FirstSequence, e.HeadSequence, result.Verification.Mapped, result.Report.Restored.HistoryFloor, result.Report.Restored.Head, e.CurrentRootSHA256)
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
	profile, recovered, found, err := RecoverBoundSourceImport(context.Background(), store)
	if recovered.Archive != nil {
		defer recovered.Archive.ReleaseNativeDecoded()
	}
	if err != nil || !found || profile.NativeDeltaHistory == nil || profile.HistoryMode != driver.RestoreHistoryMode(mode) || !reflect.DeepEqual(recovered.NativeDeltaHistory, e) || !reflect.DeepEqual(recovered.Receipt, result.Report.Receipt) || recovered.Restored.HistoryFloor != result.Report.Restored.HistoryFloor || recovered.Restored.Head != result.Report.Restored.Head {
		t.Fatal("mode lost across independent connection recovery", err)
	}
	if err := VerifyNativeCaptureSetReadback(context.Background(), store, archive, recovered); err != nil {
		t.Fatal("mode tables differ after reopen", err)
	}
	assertDetachedReceivingPlan(t, archive, profile)
}

func TestNativeDeltaCaptureCLIRefusesInvalidFloorBeforeTarget(t *testing.T) {
	binary, binaryHash := receivingBinary(t)
	archive := publicNativeDeltaArchive(t)
	defer archive.ReleaseNativeDecoded()
	for _, tc := range []struct {
		mode   string
		name   string
		first  any
		reason driver.RestoreHistoryReason
	}{
		{"partial", "missing", nil, driver.RestoreHistoryReasonUnmappable},
		{"partial", "zero", 0, driver.RestoreHistoryReasonUnmappable},
		{"partial", "null", json.RawMessage("null"), driver.RestoreHistoryReasonUnmappable},
		{"partial", "negative", -1, driver.RestoreHistoryReasonUnmappable},
		{"partial", "fractional", json.RawMessage("2.5"), driver.RestoreHistoryReasonUnmappable},
		{"partial", "beyond-head", 33, driver.RestoreHistoryReasonNonContiguous},
		{"snapshot", "zero", 0, driver.RestoreHistoryReasonUnmappable},
		{"snapshot", "nonzero", 2, driver.RestoreHistoryReasonUnmappable},
		{"full", "zero", 0, driver.RestoreHistoryReasonUnmappable},
		{"full", "nonzero", 2, driver.RestoreHistoryReasonUnmappable},
	} {
		t.Run(tc.mode+"/"+tc.name, func(t *testing.T) {
			var request map[string]any
			decode(t, encode(t, nativeCLISetRequest(t, archive, "sqlite")), &request)
			request["restore_mode"] = tc.mode
			if tc.first != nil {
				request["restore_first_sequence"] = tc.first
			}
			requestPath := filepath.Join(t.TempDir(), "request.json")
			if err := os.WriteFile(requestPath, encode(t, request), 0600); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(t.TempDir(), "refused.db")
			stdout, stderr, err := nativeCLISetExecute(t, binary, []string{"--capture-set", nativeCLISetDirectory(t), "--driver", "sqlite", "--target", target, "--request", requestPath, "--expected-binary-sha256", binaryHash}, "native-mode-refusal-"+tc.mode+"-"+tc.name)
			if err == nil || len(stdout) != 0 {
				t.Fatal("invalid explicit floor succeeded")
			}
			first, _, _ := bytes.Cut(stderr, []byte{'\n'})
			var refusal struct {
				Phase   string                          `json:"phase"`
				History driver.ErrRestoreHistoryRefused `json:"history_refusal"`
			}
			if err := json.Unmarshal(first, &refusal); err != nil || refusal.Phase != "request" || refusal.History.Mode != driver.RestoreHistoryMode(tc.mode) || refusal.History.Reason != tc.reason {
				t.Fatal("typed invalid floor refusal differs", err, string(stderr))
			}
			if _, err := os.Stat(target); !os.IsNotExist(err) {
				t.Fatal("invalid floor created target", err)
			}
		})
	}
}
