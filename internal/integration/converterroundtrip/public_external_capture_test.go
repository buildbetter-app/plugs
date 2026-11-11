package converterroundtrip

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// External public-proof glue only. Production, original tests and fixtures are
// exact 9a1b878438d625f451d35da7570eb1e845f386d5 bytes. The build root exports
// this actual Bazel test ELF without opening or importing a capture set.
func TestPublicNativeExternalHarnessBuild(t *testing.T) {
	publicProofGuard(t)
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	in, err := os.Open(self)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := os.OpenFile(filepath.Join(os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), "public-native-row-proof-test"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0555)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(out, h), in)
	if err != nil {
		t.Fatal(err)
	}
	if err := out.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("public_external_harness_elf_sha256=%s bytes=%d production_head=9a1b878438d625f451d35da7570eb1e845f386d5", hex.EncodeToString(h.Sum(nil)), n)
}

type publicProofRequest struct {
	Format        string                    `json:"format"`
	Capture       string                    `json:"capture"`
	CaptureSHA256 string                    `json:"capture_sha256"`
	Source        dumpformat.SourceIdentity `json:"source_identity"`
	ProducerHead  string                    `json:"producer_head"`
	Oracle        string                    `json:"oracle"`
	LedgerSHA256  string                    `json:"ledger_sha256"`
	RestoreTime   string                    `json:"restore_time"`
	JournalTable  string                    `json:"journal_table"`
	Commits       int                       `json:"commits"`
}

type publicProofTable struct {
	Kind           string            `json:"kind"`
	Sequence       uint64            `json:"sequence"`
	CommitID       string            `json:"commit_id"`
	Family         string            `json:"family"`
	History        string            `json:"history"`
	File           string            `json:"file"`
	SHA256         string            `json:"sha256"`
	Bytes          int               `json:"bytes"`
	SourceRows     int               `json:"source_rows"`
	ImportedRows   int               `json:"imported_rows"`
	SchemaID       string            `json:"schema_id"`
	PhysicalSHA256 string            `json:"physical_sha256"`
	ConfigPolicy   map[string]string `json:"config_policy"`
}

func publicProofGuard(t *testing.T) {
	t.Helper()
	for _, p := range []string{"/tmp/plug-swarm/PAUSE", "/tmp/plug-swarm/STOP"} {
		if _, err := os.Lstat(p); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("public proof stopped")
		}
	}
}

func publicProofHashFile(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func publicProofWrite(t *testing.T, path string, raw []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0444)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func publicProofJSON(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	publicProofWrite(t, path, append(raw, '\n'))
}

func publicProofError(t *testing.T, stage string, err error) {
	t.Helper()
	if err == nil {
		return
	}
	var typed *dumpformat.NativeDeltaDecodedLimitError
	if errors.As(err, &typed) {
		raw, marshalErr := json.Marshal(typed)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		t.Logf("public_external_typed_refusal stage=%s error=%s", stage, raw)
	}
	t.Fatalf("public external %s: %v", stage, err)
}

func publicProofLedger(t *testing.T, req publicProofRequest) []publicProofTable {
	t.Helper()
	path := filepath.Join(req.Oracle, "tables.ndjson")
	if publicProofHashFile(t, path) != req.LedgerSHA256 {
		t.Fatal("independent ledger binding differs")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 4096), 1<<20)
	var out []publicProofTable
	for s.Scan() {
		var item publicProofTable
		d := json.NewDecoder(bytes.NewReader(s.Bytes()))
		d.DisallowUnknownFields()
		if err := d.Decode(&item); err != nil {
			t.Fatal(err)
		}
		if item.File == "" || filepath.Base(item.File) != item.File || item.History == "" || item.Kind != "commit" && item.Kind != "head-checkpoint" && item.Kind != "current" {
			t.Fatal("invalid independent table declaration")
		}
		out = append(out, item)
	}
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
	if len(out) == 0 {
		t.Fatal("empty independent table ledger")
	}
	return out
}

func TestPublicNativeExternalCaptureMatrix(t *testing.T) {
	publicProofGuard(t)
	engine := os.Getenv("PLUG_PUBLIC_PROOF_DRIVER")
	if engine != "memory" && engine != "sqlite" && engine != "postgres" {
		t.Fatal("one explicit public driver required")
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	elfSHA := publicProofHashFile(t, self)
	if len(os.Getenv("PLUG_PUBLIC_PROOF_ELF_SHA256")) != 64 || elfSHA != os.Getenv("PLUG_PUBLIC_PROOF_ELF_SHA256") {
		t.Fatal("actual ELF binding differs")
	}
	requestPath := os.Getenv("PLUG_PUBLIC_PROOF_REQUEST")
	if publicProofHashFile(t, requestPath) != os.Getenv("PLUG_PUBLIC_PROOF_REQUEST_SHA256") {
		t.Fatal("request binding differs")
	}
	raw, err := os.ReadFile(requestPath)
	if err != nil {
		t.Fatal(err)
	}
	var req publicProofRequest
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&req); err != nil {
		t.Fatal(err)
	}
	if req.Format != "public-native-capture-proof/1" || len(req.CaptureSHA256) != 64 || req.Commits < 1 || req.Source.StoreID == "" || req.Source.Project == "" || req.JournalTable == "" {
		t.Fatal("public request identity missing")
	}
	when, err := time.Parse(time.RFC3339Nano, req.RestoreTime)
	if err != nil {
		t.Fatal(err)
	}
	output := os.Getenv("PLUG_PUBLIC_PROOF_OUTPUT")
	if !filepath.IsAbs(output) {
		t.Fatal("absolute owned public output required")
	}
	if err := os.Mkdir(output, 0700); err != nil {
		t.Fatal("once-only output claim", err)
	}
	publicProofJSON(t, filepath.Join(output, "claimed-request.json"), req)
	ledger := publicProofLedger(t, req)
	archive, err := dumpformat.ReadNativeCaptureSetDirectory(context.Background(), req.Capture, dumpformat.NativeCaptureSetOptions{})
	publicProofError(t, "authenticate", err)
	defer archive.ReleaseNativeDecoded()
	if archive.SHA256() != req.CaptureSHA256 || archive.Manifest().StoreID != req.Source.StoreID || len(archive.Manifest().Projects) != 1 || archive.Manifest().Projects[0].Prefix != req.Source.Project {
		t.Fatal("genuine capture identity differs")
	}
	baseline, ok := archive.NativeDecodedUsage()
	if !ok || baseline.LimitBytes != 2147483648 || baseline.LimitOrigin != "default" {
		t.Fatal("default decoded budget differs")
	}
	ctx := archive.WithNativeDeltaAccounting(context.Background())
	defer func() {
		usage, _ := archive.NativeDecodedUsage()
		publicProofJSON(t, filepath.Join(output, "final-measurements.json"), map[string]any{"driver": engine, "elf_sha256": elfSHA, "capture_sha256": req.CaptureSHA256, "baseline": baseline, "decoded": usage, "process_memory": nativeTestProcessMemory(), "uncharged_sites": nativeRehearsalRSSSites(), "scope": "receiver/converter-controlled payloads; harness and engine included in process memory; full accounting required before switch"})
	}()
	store, lease, reopen := openIdentityTarget(t, engine)
	publicProofError(t, "release-initial-lease", lease.Release(ctx))
	op, lease, release, err := HoldRestoreLease(ctx, store, "native-receive")
	publicProofError(t, "renewed-lease", err)
	release = sync.OnceValue(release)
	defer func() { publicProofError(t, "release", release()) }()
	plan := func() driver.SourcePlanePlanV1 {
		work, done := archive.NativeDeltaAccountingScope()
		defer done()
		return nativeHistoryPlanFromArchive(t, work, store, lease)
	}()
	plan.Dispositions[0].StorageClass = ""
	for _, row := range archive.Records("bead") {
		storage, present := row.Source.Fields["storage_class"]
		if !plan.Dispositions[0].MatchesStorageClass(storage, present) {
			t.Fatal("fixture storage disposition differs")
		}
	}
	statuses := map[string]bool{}
	for _, p := range archive.Manifest().Projects {
		for _, status := range p.Statuses {
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
	profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: driver.StoreID(req.Source.StoreID), Method: "offline.restore.import", IdempotencyKey: strings.Repeat("b", 32), ArchiveDigest: archive.SHA256(), HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: when.Format(time.RFC3339Nano)}
	publicProofJSON(t, filepath.Join(output, "host-plan.json"), plan)
	profile, err = BindNativeCaptureSetImport(op, archive, profile, plan)
	publicProofError(t, "bind", err)
	publicProofJSON(t, filepath.Join(output, "bound-profile.json"), profile)
	report, err := RestoreNativeCaptureSetImportWithLease(op, archive, store, lease, profile)
	publicProofError(t, "import", err)
	publicProofError(t, "production-readback", VerifyNativeCaptureSetReadback(op, store, archive, report))
	publicProofIndependentReadback(t, ctx, store, report, req, ledger, output, "imported")
	publicProofJSON(t, filepath.Join(output, "committed-report.json"), report)
	publicProofError(t, "release-before-reopen", release())
	store, _ = reopen()
	_, recovered, found, err := RecoverBoundSourceImport(ctx, store)
	if recovered.Archive != nil {
		defer recovered.Archive.ReleaseNativeDecoded()
	}
	publicProofError(t, "recover", err)
	if !found || !reflect.DeepEqual(recovered.Receipt, report.Receipt) || !reflect.DeepEqual(recovered.NativeFamilies, report.NativeFamilies) || !reflect.DeepEqual(recovered.NativeSchemas, report.NativeSchemas) || !reflect.DeepEqual(recovered.NativeHistories, report.NativeHistories) || !reflect.DeepEqual(recovered.SourcePlanes, report.SourcePlanes) {
		t.Fatal("reopened receipt or source reports differ")
	}
	publicProofError(t, "reopened-production-readback", VerifyNativeCaptureSetReadback(ctx, store, archive, recovered))
	publicProofIndependentReadback(t, ctx, store, recovered, req, ledger, output, "reopened")
	publicProofJSON(t, filepath.Join(output, "recovered-report.json"), recovered)
	if recovered.Archive != nil {
		recovered.Archive.ReleaseNativeDecoded()
	}
	usage, _ := archive.NativeDecodedUsage()
	if usage.CurrentAccountedDecodedBytes != baseline.CurrentAccountedDecodedBytes || !reflect.DeepEqual(usage.CurrentCategories, baseline.CurrentCategories) || !reflect.DeepEqual(usage.CurrentCharges, baseline.CurrentCharges) {
		t.Fatal("conversion or recovery temporary ownership leak")
	}
	proof, ok := archive.NativeDeltaVerification()
	if !ok || proof.Totals.Commits != uint64(req.Commits) {
		t.Fatal("source commit count differs")
	}
	publicProofJSON(t, filepath.Join(output, "SUCCESS.json"), map[string]any{"driver": engine, "capture_sha256": req.CaptureSHA256, "elf_sha256": elfSHA, "commits": req.Commits, "reopen": map[bool]string{true: "same-process-memory-lease-reacquired", false: "driver-connection-closed-and-reopened"}[engine == "memory"], "per_table_exact_bytes": true, "full_accounting_or_switch_credit": false})
}

// This extra oracle never lowers expected records through the receiver codec.
// Expected old-table envelopes, counts and physical/schema commitments came
// from a separately bound disk-backed semantic replay of the original capture.
func publicProofIndependentReadback(t *testing.T, ctx context.Context, store driver.Store, report Report, req publicProofRequest, ledger []publicProofTable, output, phase string) {
	t.Helper()
	at := report.Restored.Head
	r, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &at})
	publicProofError(t, "mapping-read", err)
	items, err := r.Get(ctx, []model.Ref{model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0))})
	publicProofError(t, "mapping-get", err)
	publicProofError(t, "mapping-close", r.Close())
	var evidence driver.HistoryImportEvidence
	if len(items) != 1 || json.Unmarshal(items[0].Body, &evidence) != nil || evidence.Source != driver.NativeTableHistorySource || len(evidence.Mapping) != req.Commits {
		t.Fatal("complete source mapping differs")
	}
	mapping := map[uint64]driver.SourceImportRevision{}
	for _, m := range evidence.Mapping {
		if m.SourceStoreID != req.Source.StoreID || m.Project != req.Source.Project || m.SourceSequence == 0 || mapping[m.SourceSequence].SourceSequence != 0 {
			t.Fatal("source mapping repetition or identity differs")
		}
		mapping[m.SourceSequence] = m
	}
	byState := map[uint64][]publicProofTable{}
	for _, entry := range ledger {
		n := entry.Sequence
		if entry.Kind != "commit" {
			n = 0
		}
		byState[n] = append(byState[n], entry)
	}
	if len(byState) != req.Commits+1 {
		t.Fatal("independent state census differs")
	}
	dir := filepath.Join(output, phase)
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for seq := uint64(0); seq <= uint64(req.Commits); seq++ {
		publicProofGuard(t)
		entries := byState[seq]
		if len(entries) == 0 {
			t.Fatal("missing independent state")
		}
		at := report.Restored.Head
		if seq != 0 {
			m, ok := mapping[seq]
			if !ok || len(m.EventIDs) != 0 || len(m.CommitIDs) != 1 || m.CommitIDs[0] != entries[0].CommitID {
				t.Fatal("exact original commit mapping differs")
			}
			at = m.Revision
		}
		reader, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &at})
		publicProofError(t, "snapshot-read", err)
		groups := map[nativeRowGroup][]driver.Record{}
		seen := map[string]bool{}
		cursors := map[string]bool{}
		var cursor *driver.Cursor
		for {
			page, err := reader.Query(ctx, driver.Query{AsOf: &at, Limit: 256, Cursor: cursor})
			publicProofError(t, "complete-query", err)
			if page.AsOf.String() != at.String() {
				t.Fatal("query snapshot changed")
			}
			for _, record := range page.Items {
				key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &record.Ref})
				if err != nil || seen[key] {
					t.Fatal("duplicate or invalid query Ref")
				}
				seen[key] = true
				if !strings.HasPrefix(record.Ref.ID, driver.NativeTableHistoryRecordPrefix) && !strings.HasPrefix(record.Ref.ID, "source/native-current/") {
					continue
				}
				meta, row, err := dumpformat.DecodeNativeRowRecord(record)
				publicProofError(t, "namespace-first-decode", err)
				var group nativeRowGroup
				if meta != nil {
					group = nativeRowGroup{meta.Source, meta.Table, meta.History}
				} else {
					group = nativeRowGroup{row.Source, row.Table, row.History}
				}
				groups[group] = append(groups[group], record)
			}
			if page.NextCursor == nil {
				break
			}
			if len(page.Items) == 0 || page.NextCursor.Incarnation != report.Restored.Incarnation || page.NextCursor.Revision.String() != at.String() || page.NextCursor.Method != "query" {
				t.Fatal("query cursor binding differs")
			}
			encoded, err := json.Marshal(page.NextCursor)
			if err != nil || cursors[string(encoded)] {
				t.Fatal("query cursor repeated")
			}
			cursors[string(encoded)] = true
			cursor = page.NextCursor
		}
		publicProofError(t, "snapshot-close", reader.Close())
		for _, entry := range entries {
			group := nativeRowGroup{req.Source, entry.Family, entry.History}
			records, exists := groups[group]
			if !exists {
				t.Fatal("source table row/meta missing", entry.Kind, seq, entry.Family)
			}
			value, meta, err := dumpformat.ReconstructNativeRowTable(req.Source, entry.Family, entry.History, records, entry.ConfigPolicy)
			publicProofError(t, "reconstruct-actual", err)
			if meta.IdentityKind != dumpformat.NativeRowD3Physical || meta.SchemaID != entry.SchemaID || int(meta.RowCount) != entry.ImportedRows || len(records) != entry.ImportedRows+1 {
				t.Fatal("row/meta identity schema or census differs", entry.Family)
			}
			if entry.SourceRows == entry.ImportedRows && (meta.PhysicalTableSHA256 == nil || *meta.PhysicalTableSHA256 != entry.PhysicalSHA256) {
				t.Fatal("source physical table commitment differs", entry.Family)
			}
			actual, err := dumpformat.NativeTableBodyBytes(req.Source, entry.Family, entry.History, value)
			publicProofError(t, "old-table-serialization", err)
			want, err := os.ReadFile(filepath.Join(req.Oracle, entry.File))
			publicProofError(t, "independent-old-table-read", err)
			if len(want) != entry.Bytes || hashBytes(want) != entry.SHA256 || !bytes.Equal(actual, want) {
				t.Fatal("independent exact old table bytes differ", entry.Kind, seq, entry.Family)
			}
			name := fmt.Sprintf("%s-%020d-%s", entry.Kind, entry.Sequence, entry.Family)
			publicProofWrite(t, filepath.Join(dir, name+".table.json"), actual)
			publicProofJSON(t, filepath.Join(dir, name+".records.json"), records)
			publicProofJSON(t, filepath.Join(dir, name+".proof.json"), map[string]any{"kind": entry.Kind, "sequence": entry.Sequence, "commit_id": entry.CommitID, "family": entry.Family, "history": entry.History, "source_rows": entry.SourceRows, "actual_rows": meta.RowCount, "actual_meta": 1, "expected_bytes": entry.Bytes, "expected_sha256": entry.SHA256, "actual_sha256": hashBytes(actual), "bytes_equal": true, "meta": meta})
			if entry.Family == req.JournalTable {
				t.Logf("public_native_journal phase=%s kind=%s sequence=%d source_rows=%d actual_rows=%d actual_meta=1 bytes=%d sha256=%s exact=true", phase, entry.Kind, entry.Sequence, entry.SourceRows, meta.RowCount, len(actual), hashBytes(actual))
			}
			delete(groups, group)
		}
		if len(groups) != 0 {
			t.Fatal("complete snapshot has extra native row/meta groups", seq, len(groups))
		}
	}
}
