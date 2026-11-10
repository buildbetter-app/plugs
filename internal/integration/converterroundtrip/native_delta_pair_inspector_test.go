package converterroundtrip

// Source-only companion for the existing ordinary pair test. The receiver
// owner integrates this file with its normal test/data/BUILD source step.
// No Test entrypoint or new gate is introduced by this supplied helper.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type retainedPairFile struct {
	SHA256 string `json:"sha256"`
	Bytes  uint64 `json:"bytes"`
}

type retainedPairExpectation struct {
	Format           string                            `json:"format"`
	Ledger           retainedPairFile                  `json:"ledger"`
	Counts           map[string]uint64                 `json:"counts"`
	RestoreTime      string                            `json:"restore_time"`
	CommitCount      uint64                            `json:"commit_count"`
	HistoryCoverage  *dumpformat.NativeHistoryCoverage `json:"history_coverage"`
	CaptureSetSHA256 string                            `json:"capture_set_sha256"`
	SourceStoreID    string                            `json:"source_store_id"`
	Projects         []string                          `json:"projects"`
	ProducerHead     string                            `json:"producer_head"`
	ReceiverHead     string                            `json:"receiver_head"`
	CaptureFiles     map[string]retainedPairFile       `json:"capture_files"`
	Persistent       string                            `json:"persistent_readback"`
	Publication      bool                              `json:"publication_authorized"`
}

type retainedPairEvent struct {
	Type          string           `json:"type"`
	Kind          string           `json:"kind"`
	Sequence      uint64           `json:"sequence"`
	CommitID      string           `json:"commit_id"`
	SchemaID      string           `json:"schema_id,omitempty"`
	RootSHA256    string           `json:"root_sha256,omitempty"`
	Family        string           `json:"family,omitempty"`
	RefID         string           `json:"ref_id,omitempty"`
	Present       bool             `json:"present,omitempty"`
	BodyFile      string           `json:"body_file,omitempty"`
	Body          retainedPairFile `json:"body,omitempty"`
	History       string           `json:"history,omitempty"`
	SourceRows    uint64           `json:"source_rows,omitempty"`
	ImportedRows  uint64           `json:"imported_rows,omitempty"`
	TableSchemaID string           `json:"table_schema_id,omitempty"`
	TableSHA256   string           `json:"table_sha256,omitempty"`
}

func retainedPairGuard(t *testing.T) {
	t.Helper()
	for _, path := range []string{"/tmp/plug-swarm/PAUSE", "/tmp/plug-swarm/STOP"} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatal("retained native pair stopped")
		}
	}
}

func retainedPairHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func retainedPairDecode(t *testing.T, raw []byte, target any) {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		t.Fatal("retained pair expectation schema differs")
	}
	if d.Decode(new(any)) != io.EOF {
		t.Fatal("retained pair expectation has trailing bytes")
	}
}

func retainedPairBoundBytes(t *testing.T, path string, binding retainedPairFile) []byte {
	t.Helper()
	retainedPairGuard(t)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o444 || uint64(info.Size()) != binding.Bytes {
		t.Fatal("retained pair file type, mode or length differs")
	}
	raw, err := os.ReadFile(path)
	after, statErr := os.Lstat(path)
	if err != nil || statErr != nil || !os.SameFile(info, after) || !info.ModTime().Equal(after.ModTime()) ||
		uint64(len(raw)) != binding.Bytes || retainedPairHash(raw) != binding.SHA256 {
		t.Fatal("retained pair file binding differs")
	}
	return raw
}

// inspectRetainedNativePair runs AFTER the Python harness and direct CLI wait.
// The ordinary caller supplies the hash/size of the just-frozen independent
// expectations manifest; that file transitively binds every ledger/body file.
// It preserves the closed original target and opens a byte-exact writable copy
// through the supported SQLite driver, with no write/restore/import operation.
// This is not a reverse converter execution or an independent business mapper.
func inspectRetainedNativePair(t *testing.T, caseDir string, expectationBinding retainedPairFile) {
	t.Helper()
	retainedPairGuard(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()
	expectedDir := filepath.Join(caseDir, "expected")
	var expected retainedPairExpectation
	retainedPairDecode(t, retainedPairBoundBytes(t, filepath.Join(expectedDir, "manifest.json"), expectationBinding), &expected)
	if expected.Format != "native-delta-independent-readback-expectations/1" || expected.Publication ||
		expected.CaptureSetSHA256 != "f35639353f2e314e7f72b03190bb513ca7811417549f4b6716805862e6adb947" ||
		expected.ProducerHead != "1a75f75feae282890c4b9b8769c54e8b6a23c2f4" ||
		expected.ReceiverHead != "26e90d85a0be080538b34b97e17849e4ced61d8b" ||
		len(expected.Projects) != 1 || expected.CommitCount != expected.Counts["historical_states"] {
		t.Fatal("retained pair source identity differs")
	}
	var targetBinding retainedPairFile
	targetBindingRaw, err := os.ReadFile(filepath.Join(caseDir, "retained-target.json"))
	if err != nil {
		t.Fatal(err)
	}
	retainedPairDecode(t, targetBindingRaw, &targetBinding)
	originalTarget := filepath.Join(caseDir, "retained.sqlite")
	targetBytes := retainedPairBoundBytes(t, originalTarget, targetBinding)
	copyDir := filepath.Join(caseDir, "inspection-copy")
	if err := os.Mkdir(copyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	copyPath := filepath.Join(copyDir, "inspected.sqlite")
	if err := os.WriteFile(copyPath, targetBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	// Target clone memory is released before historical table inspection.
	targetBytes = nil
	store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: copyPath})
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	profile, report, found, err := RecoverBoundSourceImport(ctx, store)
	if err != nil || !found || !report.Recovered || report.Receipt == nil || profile.HistoryTime != expected.RestoreTime ||
		profile.ArchiveDigest != expected.CaptureSetSHA256 || string(profile.SourceStoreID) != expected.SourceStoreID {
		t.Fatal("persistent recovered admission, receipt or source binding differs")
	}
	// Compare the original constructor's explicit policy to recovered binding;
	// this checks persistence of the selected policy, not host authentication.
	var constructor struct {
		Format         string                        `json:"format"`
		PolicySHA256   string                        `json:"policy_sha256"`
		ArchiveSHA256  string                        `json:"archive_sha256"`
		SourceStoreID  string                        `json:"source_store_id"`
		Projects       []string                      `json:"projects"`
		Admission      driver.RestoreImportAdmission `json:"admission"`
		IdempotencyKey string                        `json:"idempotency_key"`
		SourcePlan     driver.SourcePlanePlanV1      `json:"source_plan"`
	}
	requestRaw, err := os.ReadFile(filepath.Join(caseDir, "constructor.stdout"))
	if err != nil {
		t.Fatal(err)
	}
	retainedPairDecode(t, requestRaw, &constructor)
	if profile.SourcePlanes == nil {
		t.Fatal("persistent source plane binding missing")
	}
	plan, err := profile.SourcePlanes.Plan()
	if err != nil {
		t.Fatal(err)
	}
	constructor.SourcePlan.Target.StoreID = plan.Target.StoreID
	constructor.SourcePlan.Target.Incarnation = plan.Target.Incarnation
	if !reflect.DeepEqual(plan, constructor.SourcePlan) || !reflect.DeepEqual(profile.Admission, constructor.Admission) ||
		profile.IdempotencyKey != constructor.IdempotencyKey || profile.HistoryMode != driver.RestoreHistoryModeFull {
		t.Fatal("persistent request or policy selection differs")
	}
	meta, err := store.Meta(ctx)
	if err != nil || meta.Incarnation != report.Restored.Incarnation || meta.Head != report.Restored.Head {
		t.Fatal("persistent restored target metadata differs")
	}
	current, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	evidenceRows, err := current.Get(ctx, []model.Ref{model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0))})
	if err != nil || len(evidenceRows) != 1 {
		t.Fatal("persistent source mapping missing")
	}
	var evidence driver.HistoryImportEvidence
	retainedPairDecode(t, evidenceRows[0].Body, &evidence)
	if evidence.Source != driver.NativeTableHistorySource || evidence.Mode != driver.RestoreHistoryModeFull ||
		evidence.Complete != (expected.HistoryCoverage == nil) || evidence.First != 1 || evidence.Head != expected.CommitCount ||
		!evidence.Floor.IsZero() || len(evidence.Gaps) != 0 || len(evidence.Mapping) != int(expected.CommitCount) {
		t.Fatal("persistent history coverage differs")
	}
	var boundaries []driver.NativeHistoryBoundary
	if expected.HistoryCoverage != nil {
		boundaries = []driver.NativeHistoryBoundary{{SourceStoreID: expected.SourceStoreID,
			Project: expected.Projects[0], VerifiedCommitCount: expected.CommitCount,
			BoundarySHA256: dumpformat.NativeHistoryCoverageSHA256(*expected.HistoryCoverage)}}
	}
	if !reflect.DeepEqual(evidence.NativeBoundaries, boundaries) {
		t.Fatal("persistent history frontier differs")
	}
	mappings := map[uint64]driver.SourceImportRevision{}
	for _, mapping := range evidence.Mapping {
		if _, exists := mappings[mapping.SourceSequence]; exists || mapping.SourceStoreID != expected.SourceStoreID ||
			mapping.Project != expected.Projects[0] || len(mapping.CommitIDs) != 1 || mapping.Revision == "" {
			t.Fatal("persistent source mapping duplicate or identity differs")
		}
		mappings[mapping.SourceSequence] = mapping
	}
	// Every original head/part/root blob is byte-bound by source hash and size.
	// This proves retained evidence for reverse/undo; no reverse tool is invoked.
	for _, binding := range expected.CaptureFiles {
		retainedPairGuard(t)
		raw, err := current.Blob(ctx, driver.Hash(binding.SHA256))
		if err != nil || uint64(len(raw)) != binding.Bytes || retainedPairHash(raw) != binding.SHA256 {
			t.Fatal("persistent original native capture blob differs")
		}
	}
	ledgerPath := filepath.Join(expectedDir, "readback.ndjson")
	// Check whole-file binding without retaining a historical image array.
	ledgerFile, err := os.Open(ledgerPath)
	if err != nil {
		t.Fatal(err)
	}
	defer ledgerFile.Close()
	h := sha256.New()
	n, err := io.Copy(h, ledgerFile)
	if err != nil || uint64(n) != expected.Ledger.Bytes || hex.EncodeToString(h.Sum(nil)) != expected.Ledger.SHA256 {
		t.Fatal("independent expectation ledger differs")
	}
	if _, err := ledgerFile.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	proofPath := filepath.Join(caseDir, "persistent-readback.ndjson")
	proof, err := os.OpenFile(proofPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Close()
	proofEncoder := json.NewEncoder(proof)
	scanner := bufio.NewScanner(ledgerFile)
	scanner.Buffer(make([]byte, 4096), 1<<20) // One small generated ledger event; no native total cap.
	var pinned driver.Reader
	defer func() {
		if pinned != nil {
			_ = pinned.Close()
		}
	}()
	states, tables, rows, currentTables, currentSourceRows, currentImportedRows := uint64(0), uint64(0), uint64(0), uint64(0), uint64(0), uint64(0)
	families := map[string]NativeFamilyReport{}
	for _, family := range report.NativeFamilies {
		if _, exists := families[family.Family]; exists || family.Source.StoreID != expected.SourceStoreID || family.Source.Project != expected.Projects[0] {
			t.Fatal("persistent current family report duplicate or source differs")
		}
		families[family.Family] = family
	}
	var active retainedPairEvent
	for scanner.Scan() {
		retainedPairGuard(t)
		var event retainedPairEvent
		retainedPairDecode(t, scanner.Bytes(), &event)
		if event.Type == "state" {
			active = event
			if pinned != nil {
				if err := pinned.Close(); err != nil {
					t.Fatal(err)
				}
				pinned = nil
			}
			at := meta.Head
			if event.Kind == "commit" {
				mapping, exists := mappings[event.Sequence]
				if !exists || mapping.CommitIDs[0] != event.CommitID {
					t.Fatal("persistent per-commit source mapping differs")
				}
				delete(mappings, event.Sequence)
				at = mapping.Revision
				states++
			} else if event.Kind != "current" {
				t.Fatal("unexpected independent state kind")
			}
			pinned, err = store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &at})
			if err != nil {
				t.Fatal(err)
			}
		} else if event.Type == "table" && pinned != nil {
			if event.Kind != active.Kind || event.Sequence != active.Sequence || event.CommitID != active.CommitID {
				t.Fatal("independent table is attached to a different state")
			}
			tuple, err := json.Marshal([]string{expected.SourceStoreID, expected.Projects[0], event.Family})
			if err != nil {
				t.Fatal(err)
			}
			prefix := "source/native-current/"
			if event.Kind == "commit" {
				prefix = driver.NativeTableHistoryRecordPrefix
			}
			if event.RefID != prefix+retainedPairHash(tuple) {
				t.Fatal("independent table identity differs")
			}
			got, err := pinned.Get(ctx, []model.Ref{model.RefByID(event.RefID, model.Natural64(0))})
			if err != nil || (!event.Present && len(got) != 0) || (event.Present && len(got) != 1) {
				t.Fatal("persistent table presence or explicit absence differs")
			}
			if event.Present {
				if filepath.Base(event.BodyFile) != event.BodyFile {
					t.Fatal("independent body path differs")
				}
				body := retainedPairBoundBytes(t, filepath.Join(expectedDir, event.BodyFile), event.Body)
				if got[0].Kind != "beads-archive" || !bytes.Equal(body, got[0].Body) {
					t.Fatal("persistent normalized columns, cells, order or multiplicity differs")
				}
				if event.Kind == "commit" {
					tables++
					rows += event.SourceRows
				} else {
					family, exists := families[event.Family]
					if !exists || family.History != event.History || uint64(family.SourceRows) != event.SourceRows ||
						uint64(family.ImportedRows) != event.ImportedRows || uint64(family.ExpiredRows) != event.SourceRows-event.ImportedRows {
						t.Fatal("persistent current family source/imported/expired counts differ")
					}
					delete(families, event.Family)
					currentTables++
					currentSourceRows += event.SourceRows
					currentImportedRows += event.ImportedRows
				}
			}
		} else {
			t.Fatal("independent readback ledger state order differs")
		}
		if err := proofEncoder.Encode(struct {
			Expected retainedPairEvent `json:"independent_expected"`
			Matched  bool              `json:"persistent_match"`
		}{event, true}); err != nil {
			t.Fatal(err)
		}
	}
	if scanner.Err() != nil || len(mappings) != 0 || len(families) != 0 || states != expected.Counts["historical_states"] ||
		tables != expected.Counts["historical_tables"] || rows != expected.Counts["historical_rows"] ||
		currentTables != expected.Counts["current_tables"] || currentSourceRows != expected.Counts["current_source_rows"] ||
		currentImportedRows != expected.Counts["current_imported_rows"] {
		t.Fatal("persistent readback EOF census differs")
	}
	if err := proof.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := proof.Chmod(0o444); err != nil {
		t.Fatal(err)
	}
	if pinned != nil {
		if err := pinned.Close(); err != nil {
			t.Fatal(err)
		}
		pinned = nil
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	afterMeta, err := store.Meta(ctx)
	if err != nil || !reflect.DeepEqual(meta, afterMeta) {
		t.Fatal("persistent inspection changed logical target metadata")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	// Ordinary driver Open may initialize WAL/schema representation. Preserve
	// the clone and any sidecars that actually survive Close as evidence.
	entries, err := os.ReadDir(copyDir)
	if err != nil {
		t.Fatal(err)
	}
	cloneFiles := map[string]retainedPairFile{}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			t.Fatal("persistent inspection clone has a nonregular member")
		}
		path := filepath.Join(copyDir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		cloneFiles[entry.Name()] = retainedPairFile{SHA256: retainedPairHash(data), Bytes: uint64(len(data))}
		if err := os.Chmod(path, 0o444); err != nil {
			t.Fatal(err)
		}
	}
	_ = retainedPairBoundBytes(t, originalTarget, targetBinding)
	result := struct {
		Format              string                      `json:"format"`
		ExpectedManifest    retainedPairFile            `json:"expected_manifest"`
		OriginalTarget      retainedPairFile            `json:"original_target"`
		InspectionClone     map[string]retainedPairFile `json:"inspection_clone"`
		MetaBefore          driver.Meta                 `json:"meta_before"`
		MetaAfter           driver.Meta                 `json:"meta_after"`
		States              uint64                      `json:"historical_states"`
		Tables              uint64                      `json:"historical_tables"`
		Rows                uint64                      `json:"historical_rows"`
		OriginalCapture     string                      `json:"original_capture"`
		ReadAPIs            string                      `json:"read_apis"`
		ReverseExecution    string                      `json:"reverse_execution"`
		PublicationApproved bool                        `json:"publication_authorized"`
	}{"native-delta-independent-persistent-readback/1", expectationBinding, targetBinding,
		cloneFiles, meta, afterMeta, states, tables, rows, "ALL_ROOT_HEAD_PART_BLOBS_MATCHED",
		"RECOVERY_AND_READS_ONLY_ON_COPY; DRIVER_OPEN_MAY_WRITE_SETUP", "NOT_RUN", false}
	resultBytes, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(caseDir, "persistent-readback-result.json"), append(resultBytes, '\n'), 0o444); err != nil {
		t.Fatal(err)
	}
	t.Logf("retained native pair persistent inspection matched %d commits/%d tables/%d rows; original target preserved; publication=false", states, tables, rows)
}
