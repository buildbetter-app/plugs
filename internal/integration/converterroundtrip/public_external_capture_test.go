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
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// External public-proof glue adopted from reviewed b3c6fe72. Consumer callbacks
// compile into this actual converterroundtrip test ELF. The export receipt pins
// its composed product parent without opening or importing a capture set.
func TestPublicNativeExternalHarnessBuild(t *testing.T) {
	publicProofGuard(t)
	publicProofExternalRoot(t, "PLUG_PUBLIC_PROOF_EXPORT")
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
	t.Logf("public_external_harness_elf_sha256=%s bytes=%d source_binding=export-receipt", hex.EncodeToString(h.Sum(nil)), n)
}

type publicProofRequest struct {
	Format               string                    `json:"format"`
	Capture              string                    `json:"capture"`
	CaptureSHA256        string                    `json:"capture_sha256"`
	Source               dumpformat.SourceIdentity `json:"source_identity"`
	ProducerHead         string                    `json:"producer_head"`
	Oracle               string                    `json:"oracle"`
	LedgerSHA256         string                    `json:"ledger_sha256"`
	StatesSHA256         string                    `json:"states_sha256"`
	RestoreTime          string                    `json:"restore_time"`
	RestoreMode          string                    `json:"restore_mode,omitempty"`
	RestoreFirstSequence json.RawMessage           `json:"restore_first_sequence,omitempty"`
	JournalTable         string                    `json:"journal_table"`
	Commits              int                       `json:"commits"`
}

// The request wire keeps /1 compatibility: absence is the original Full
// behavior. Only the three explicit policy names are accepted; no case is
// relabeled to bypass an unavailable receiver mode.
func (r publicProofRequest) historyMode() (driver.RestoreHistoryMode, error) {
	switch r.RestoreMode {
	case "", "full", "partial", "snapshot":
		return driver.ParseRestoreHistoryMode(r.RestoreMode)
	default:
		return "", driver.ErrRestoreHistoryMode{Value: r.RestoreMode}
	}
}

// A raw field distinguishes absence from supplied zero/null for Full/Snapshot.
// A Partial floor is a source ordinal, never a target revision or local floor.
func (r publicProofRequest) historySelection() (driver.RestoreHistoryMode, *driver.NativeDeltaHistoryPolicy, error) {
	mode, err := r.historyMode()
	if err != nil {
		return "", nil, err
	}
	if mode != driver.RestoreHistoryModePartial && len(r.RestoreFirstSequence) != 0 {
		return mode, nil, driver.ErrRestoreHistoryRefused{Mode: mode, Reason: driver.RestoreHistoryReasonUnmappable}
	}
	if mode == driver.RestoreHistoryModeFull {
		return mode, nil, nil
	}
	var first uint64
	if mode == driver.RestoreHistoryModePartial && (len(r.RestoreFirstSequence) == 0 || json.Unmarshal(r.RestoreFirstSequence, &first) != nil || first == 0) {
		return mode, nil, driver.ErrRestoreHistoryRefused{Mode: mode, Reason: driver.RestoreHistoryReasonUnmappable}
	}
	policy := &driver.NativeDeltaHistoryPolicy{Format: driver.NativeDeltaHistoryPolicyFormat, FirstSequence: first}
	return mode, policy, policy.Validate(mode)
}

func publicProofCaptureSelection(archive *dumpformat.Archive, mode driver.RestoreHistoryMode, policy *driver.NativeDeltaHistoryPolicy) error {
	if mode == driver.RestoreHistoryModePartial {
		if policy == nil || policy.Validate(mode) != nil {
			return driver.ErrRestoreHistoryRefused{Mode: mode, History: archive.Manifest().History, Reason: driver.RestoreHistoryReasonUnmappable}
		}
		if _, ok := archive.NativeDeltaHistoryPoint(policy.FirstSequence); !ok {
			return driver.ErrRestoreHistoryRefused{Mode: mode, History: archive.Manifest().History, Reason: driver.RestoreHistoryReasonNonContiguous}
		}
	}
	return nil
}

// Keep typed policy refusal before lower-level binding and require the
// explicit operator opt-in supplied by the actual native mode receiver layer.
func publicProofBindCapture(ctx context.Context, archive *dumpformat.Archive, profile driver.RestoreImportProfile, plan driver.SourcePlanePlanV1) (driver.RestoreImportProfile, error) {
	if err := validateArchiveHistoryPolicy(profile.HistoryMode, archive); err != nil {
		return driver.RestoreImportProfile{}, err
	}
	if profile.NativeDeltaHistory != nil {
		if err := profile.NativeDeltaHistory.Validate(profile.HistoryMode); err != nil {
			return driver.RestoreImportProfile{}, err
		}
	} else if profile.HistoryMode != driver.RestoreHistoryModeFull {
		return driver.RestoreImportProfile{}, driver.ErrRestoreHistoryRefused{Mode: profile.HistoryMode, History: archive.Manifest().History, Reason: driver.RestoreHistoryReasonUnmappable}
	}
	if err := publicProofCaptureSelection(archive, profile.HistoryMode, profile.NativeDeltaHistory); err != nil {
		return driver.RestoreImportProfile{}, err
	}
	return BindNativeCaptureSetImport(ctx, archive, profile, plan)
}

// The callback runs synchronously after import, reopen/recovery and both
// readback oracles. The store is the actually reopened target (the same store
// for memory), never a new target or a DSN. The context retains decoded
// accounting and a renewed lease for this callback's lifetime. The callback
// owns its operation clock and assertions; it must not close the store,
// release the lease, retain either after return, or register asynchronous use.
// Report.Archive is live until the callback returns. Returning an error or
// failing t prevents the outer success receipt.
type publicProofContinuation func(t *testing.T, ctx context.Context, store driver.Store, lease driver.ActiveLease, profile driver.RestoreImportProfile, report Report) error

// The adapter supplies explicit host intent for actual source entities before
// binding. It borrows the admitted archive synchronously and cannot replace
// the authenticated source or the selected empty target. It owns no lifecycle.
type publicProofHostPlanAdapter func(*testing.T, context.Context, *dumpformat.Archive, driver.SourcePlanePlanV1) (driver.SourcePlanePlanV1, error)

func publicProofAdaptHostPlan(t *testing.T, ctx context.Context, archive *dumpformat.Archive, plan driver.SourcePlanePlanV1, adapter publicProofHostPlanAdapter) (driver.SourcePlanePlanV1, error) {
	t.Helper()
	if adapter == nil {
		return plan, nil
	}
	source, digest, target := plan.SourceStoreID, plan.ArchiveSHA256, plan.Target
	adapted, err := adapter(t, ctx, archive, plan)
	if err != nil {
		return driver.SourcePlanePlanV1{}, err
	}
	if adapted.SourceStoreID != source || adapted.ArchiveSHA256 != digest || adapted.Target != target {
		return driver.SourcePlanePlanV1{}, driver.ErrConflict
	}
	return adapted, nil
}

func publicProofRunContinuation(t *testing.T, ctx context.Context, store driver.Store, lease driver.ActiveLease, profile driver.RestoreImportProfile, report Report, continuation publicProofContinuation) (err error) {
	t.Helper()
	if continuation == nil {
		return nil
	}
	publicProofGuard(t)
	if err := lease.Release(ctx); err != nil {
		return err
	}
	op, active, release, err := HoldRestoreLease(ctx, store, "native-receive-continuation")
	if err != nil {
		return err
	}
	defer func() {
		cleanupErr := release()
		if cleanupErr != nil {
			t.Error("release continuation lease", cleanupErr)
		}
		err = errors.Join(err, cleanupErr)
	}()
	if err := op.Err(); err != nil {
		return context.Cause(op)
	}
	err = continuation(t, op, store, active, profile, report)
	if err == nil && op.Err() != nil {
		err = context.Cause(op)
	}
	return err
}

// The optional assertion is read-only and receives the target after the shared
// owner reopens it a second time. The imported report remains provenance for
// the original import; current observations use the reopened store's head.
// Neither callback owns close, release, reopen or the retained archive.
func publicProofRunContinuationPhases(t *testing.T, ctx context.Context, store driver.Store, lease driver.ActiveLease, profile driver.RestoreImportProfile, report Report, writer, assertion publicProofContinuation, reopen func() (driver.Store, driver.ActiveLease)) (driver.Store, driver.ActiveLease, error) {
	t.Helper()
	if assertion != nil {
		if writer == nil {
			return store, lease, errors.New("post-reopen assertion requires a writer callback")
		}
		if reopen == nil {
			return store, lease, errors.New("post-reopen assertion requires the shared reopen owner")
		}
		if report.Archive == nil {
			return store, lease, errors.New("post-reopen assertion requires the retained archive")
		}
	}
	if err := publicProofRunContinuation(t, ctx, store, lease, profile, report, writer); err != nil {
		return store, lease, err
	}
	if t.Failed() {
		return store, lease, errors.New("public continuation failed")
	}
	if assertion == nil {
		return store, lease, nil
	}
	// The writer's helper has returned only after draining its operation and
	// releasing its renewed lease. No callback receives this reopen closure.
	publicProofGuard(t)
	if ctx.Err() != nil {
		return store, lease, context.Cause(ctx)
	}
	before, err := store.Meta(ctx)
	if err != nil {
		return store, lease, err
	}
	store, lease = reopen()
	after, err := store.Meta(ctx)
	if err != nil {
		return store, lease, err
	}
	if !reflect.DeepEqual(before, after) {
		return store, lease, errors.New("post-continuation reopen changed target identity or committed head")
	}
	if err := publicProofRunContinuation(t, ctx, store, lease, profile, report, assertion); err != nil {
		return store, lease, err
	}
	if t.Failed() {
		return store, lease, errors.New("post-reopen assertion failed")
	}
	observed, err := store.Meta(ctx)
	if err != nil {
		return store, lease, err
	}
	if !reflect.DeepEqual(after, observed) {
		return store, lease, errors.New("post-reopen assertion changed committed target metadata")
	}
	return store, lease, nil
}

type publicProofState struct {
	Kind               string `json:"kind"`
	Sequence           uint64 `json:"sequence"`
	CommitID           string `json:"commit_id"`
	TableCount         int    `json:"table_count"`
	PhysicalRootSHA256 string `json:"physical_root_sha256"`
}

type publicProofStateLedger struct {
	Commits       map[uint64]publicProofState
	Head, Current publicProofState
}

func publicProofStates(t *testing.T, req publicProofRequest, ledger []publicProofTable) publicProofStateLedger {
	t.Helper()
	path := filepath.Join(req.Oracle, "states.ndjson")
	if len(req.StatesSHA256) != 64 || publicProofHashFile(t, path) != req.StatesSHA256 {
		t.Fatal("complete state ledger binding differs")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	states := map[string]publicProofState{}
	mapping := map[uint64]publicProofState{}
	for s.Scan() {
		var state publicProofState
		d := json.NewDecoder(bytes.NewReader(s.Bytes()))
		d.DisallowUnknownFields()
		if err := d.Decode(&state); err != nil {
			t.Fatal(err)
		}
		if state.Kind != "commit" && state.Kind != "head-checkpoint" && state.Kind != "current" || state.TableCount < 0 || state.CommitID == "" {
			t.Fatal("invalid complete state")
		}
		if state.Kind == "commit" && (state.Sequence < 1 || state.Sequence > uint64(req.Commits)) || state.Kind != "commit" && state.Sequence != 0 {
			t.Fatal("invalid state sequence")
		}
		key := fmt.Sprintf("%s/%d", state.Kind, state.Sequence)
		if _, ok := states[key]; ok {
			t.Fatal("duplicate independent state")
		}
		states[key] = state
		if state.Kind == "commit" {
			mapping[state.Sequence] = state
		}
	}
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
	if len(states) != req.Commits+2 || len(mapping) != req.Commits {
		t.Fatal("complete independent frame census differs")
	}
	for key, state := range states {
		digests := map[string]dumpformat.NativeDeltaTableDigest{}
		for _, entry := range ledger {
			if fmt.Sprintf("%s/%d", entry.Kind, entry.Sequence) != key {
				continue
			}
			if entry.CommitID != state.CommitID || entry.SourceRows < 0 {
				t.Fatal("table frame identity differs")
			}
			if _, ok := digests[entry.Family]; ok {
				t.Fatal("duplicate independent table")
			}
			digests[entry.Family] = dumpformat.NativeDeltaTableDigest{SHA256: entry.PhysicalSHA256, Rows: uint64(entry.SourceRows)}
		}
		root, err := dumpformat.NativeDeltaRootSHA256(digests)
		if err != nil || root != state.PhysicalRootSHA256 || len(digests) != state.TableCount {
			t.Fatal("independent complete frame root or table count differs", key)
		}
	}
	for _, entry := range ledger {
		if _, ok := states[fmt.Sprintf("%s/%d", entry.Kind, entry.Sequence)]; !ok {
			t.Fatal("table outside complete state census")
		}
	}
	if states["head-checkpoint/0"].CommitID != mapping[uint64(req.Commits)].CommitID || states["current/0"].CommitID != mapping[uint64(req.Commits)].CommitID {
		t.Fatal("HEAD/current source identity differs")
	}
	return publicProofStateLedger{Commits: mapping, Head: states["head-checkpoint/0"], Current: states["current/0"]}
}

// Host intent is explicit per observed tuple. It preserves the existing
// exact field/status mapping and refuses missing or ambiguous tuple fields.
func publicProofHostIntent(t *testing.T, archive *dumpformat.Archive, plan driver.SourcePlanePlanV1) driver.SourcePlanePlanV1 {
	t.Helper()
	template := plan.Dispositions[0]
	statuses := map[string]bool{}
	for _, p := range archive.Manifest().Projects {
		for _, status := range p.Statuses {
			statuses[status.Name] = true
		}
	}
	template.Statuses = nil
	for name := range statuses {
		template.Statuses = append(template.Statuses, driver.SourcePlaneStringPairV1{From: name, To: name})
	}
	sort.Slice(template.Statuses, func(i, j int) bool { return template.Statuses[i].From < template.Statuses[j].From })
	flag := func(fields dumpformat.Row, name string) bool {
		v, present := fields[name]
		if !present {
			t.Fatal("missing source tuple flag", name)
		}
		switch v := v.(type) {
		case bool:
			return v
		case json.Number:
			if v == "0" {
				return false
			}
			if v == "1" {
				return true
			}
		}
		t.Fatal("invalid source tuple flag", name)
		return false
	}
	byTuple := map[string]driver.SourcePlaneDispositionV1{}
	byEntity := map[driver.SourceEntityKey]string{}
	for _, row := range archive.Records("bead") {
		fields := row.Source.Fields
		sourceType, ok := fields["issue_type"].(string)
		if !ok || sourceType == "" {
			t.Fatal("missing source tuple type")
		}
		storage, present := fields["storage_class"]
		if !present {
			t.Fatal("missing source tuple storage")
		}
		storageText := ""
		if storage != nil {
			var ok bool
			storageText, ok = storage.(string)
			if !ok || storageText == "" {
				t.Fatal("invalid source tuple storage")
			}
		}
		ephemeral, noHistory := flag(fields, "ephemeral"), flag(fields, "no_history")
		tuple, err := json.Marshal(struct {
			Type                 string
			Storage              any
			Ephemeral, NoHistory bool
		}{sourceType, storage, ephemeral, noHistory})
		if err != nil {
			t.Fatal(err)
		}
		key := "tuple-" + hashBytes(tuple)
		disposition := template
		disposition.Key = key
		disposition.SourceType = sourceType
		disposition.StorageClass = storageText
		disposition.Ephemeral = ephemeral
		disposition.NoHistory = noHistory
		if !disposition.MatchesStorageClass(storage, true) {
			t.Fatal("exact source storage intent differs")
		}
		byTuple[key] = disposition
		entity := driver.SourceEntityKey{Project: row.Project, Plane: row.Source.Table, ID: row.ID}
		if _, exists := byEntity[entity]; exists {
			t.Fatal("repeated source entity")
		}
		byEntity[entity] = key
	}
	if len(byEntity) != len(plan.Entities) {
		t.Fatal("complete host entity census differs")
	}
	for i := range plan.Entities {
		key, ok := byEntity[plan.Entities[i].Source]
		if !ok {
			t.Fatal("host entity tuple missing")
		}
		plan.Entities[i].DispositionKey = key
		delete(byEntity, plan.Entities[i].Source)
	}
	if len(byEntity) != 0 {
		t.Fatal("unbound source entities")
	}
	plan.Dispositions = nil
	for _, disposition := range byTuple {
		plan.Dispositions = append(plan.Dispositions, disposition)
	}
	sort.Slice(plan.Dispositions, func(i, j int) bool { return plan.Dispositions[i].Key < plan.Dispositions[j].Key })
	return plan
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

// Only the standalone external roots are opt-in. Shared controls and the
// callable import/continuation helper remain available to ordinary tests.
func publicProofExternalRoot(t *testing.T, flag string) {
	t.Helper()
	switch value := os.Getenv(flag); value {
	case "":
		t.Skipf("explicit external public proof root requires %s=1", flag)
	case "1":
	default:
		t.Fatalf("invalid external public proof opt-in %s=%q; require 1 or unset", flag, value)
	}
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
	publicProofLogLeaseFence(t, stage, err)
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
	if code, details, ok := driver.ErrorMapping(err); ok {
		raw, marshalErr := json.Marshal(map[string]any{"code": code, "details": details})
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

// publicProofPhaseClock observes the existing harness boundaries only. The final
// boundary runs before testing cleanup; direct wait4 supplies total process time.
// A boundary is timing evidence and never an import or equality verdict.
func publicProofPhaseClock(t *testing.T) func(string) {
	t.Helper()
	start := time.Now()
	previous := start
	phase := "request-and-ledger"
	t.Logf("public_external_phase_begin phase=%s elapsed_seconds=0", phase)
	return func(next string) {
		t.Helper()
		now := time.Now()
		t.Logf("public_external_phase_end phase=%s elapsed_seconds=%.9f cumulative_seconds=%.9f next=%s failed_at_observation=%t", phase, now.Sub(previous).Seconds(), now.Sub(start).Seconds(), next, t.Failed())
		if next != "test-return" {
			t.Logf("public_external_phase_begin phase=%s elapsed_seconds=%.9f", next, now.Sub(start).Seconds())
		}
		phase, previous = next, now
	}
}

func TestPublicNativeExternalCaptureMatrix(t *testing.T) {
	publicProofGuard(t)
	publicProofExternalRoot(t, "PLUG_PUBLIC_PROOF_RUN")
	if os.Getenv(b15NativePacketEnv) == "1" {
		b15RunNativeCaptureMatrix(t)
		return
	}
	runPublicNativeExternalCapture(t, nil)
}

// Shared test-only entry point for a consumer in this package. No optional
// assertion preserves the original lifecycle. One assertion runs after the
// writer drains and the shared owner performs a second reopen, before cleanup.
func runPublicNativeExternalCapture(t *testing.T, continuation publicProofContinuation, afterReopen ...publicProofContinuation) {
	t.Helper()
	runPublicNativeExternalCaptureWithHostPlan(t, nil, continuation, afterReopen...)
}

// Consumers may supply their exact source disposition through this same
// import/readback/reopen lifecycle. The original entry point keeps its default
// durable mapping and inert origins. Binding still checks the complete plan.
func runPublicNativeExternalCaptureWithHostPlan(t *testing.T, adapter publicProofHostPlanAdapter, continuation publicProofContinuation, afterReopen ...publicProofContinuation) {
	t.Helper()
	phase := publicProofPhaseClock(t)
	defer phase("test-return")
	publicProofGuard(t)
	if len(afterReopen) > 1 {
		t.Fatal("at most one post-reopen assertion is supported")
	}
	var assertion publicProofContinuation
	if len(afterReopen) == 1 {
		assertion = afterReopen[0]
	}
	if assertion != nil && continuation == nil {
		t.Fatal("post-reopen assertion requires a writer callback")
	}
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
	mode, selection, err := req.historySelection()
	publicProofError(t, "restore-mode", err)
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
	phase("admission")
	archive, err := dumpformat.ReadNativeCaptureSetDirectory(context.Background(), req.Capture, dumpformat.NativeCaptureSetOptions{})
	publicProofError(t, "authenticate", err)
	defer archive.ReleaseNativeDecoded()
	if archive.SHA256() != req.CaptureSHA256 || archive.Manifest().StoreID != req.Source.StoreID || len(archive.Manifest().Projects) != 1 || archive.Manifest().Projects[0].Prefix != req.Source.Project {
		t.Fatal("genuine capture identity differs")
	}
	publicProofError(t, "source-floor", publicProofCaptureSelection(archive, mode, selection))
	baseline, ok := archive.NativeDecodedUsage()
	if !ok || baseline.LimitBytes != 2147483648 || baseline.LimitOrigin != "default" {
		t.Fatal("default decoded budget differs")
	}
	ctx := archive.WithNativeDeltaAccounting(context.Background())
	defer func() {
		usage, _ := archive.NativeDecodedUsage()
		publicProofJSON(t, filepath.Join(output, "final-measurements.json"), map[string]any{"driver": engine, "elf_sha256": elfSHA, "capture_sha256": req.CaptureSHA256, "baseline": baseline, "decoded": usage, "process_memory": nativeTestProcessMemory(), "uncharged_sites": nativeRehearsalRSSSites(), "scope": "receiver/converter-controlled payloads; harness and engine included in process memory; full accounting required before switch"})
	}()
	phase("target-and-lease")
	store, lease, reopen := openIdentityTarget(t, engine)
	publicProofError(t, "release-initial-lease", lease.Release(ctx))
	op, lease, release, err := publicProofHoldObservedLease(t, ctx, store, "native-receive", output)
	publicProofError(t, "renewed-lease", err)
	release = sync.OnceValue(release)
	defer func() { publicProofError(t, "release", release()) }()
	phase("host-plan")
	plan := func() driver.SourcePlanePlanV1 {
		work, done := archive.NativeDeltaAccountingScope()
		defer done()
		return nativeHistoryPlanFromArchive(t, work, store, lease)
	}()
	plan = publicProofHostIntent(t, archive, plan)
	plan, err = publicProofAdaptHostPlan(t, op, archive, plan, adapter)
	publicProofError(t, "host-plan", err)
	profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: driver.StoreID(req.Source.StoreID), Method: "offline.restore.import", IdempotencyKey: strings.Repeat("b", 32), ArchiveDigest: archive.SHA256(), HistoryMode: mode, HistoryTime: when.UTC().Format(time.RFC3339Nano), NativeDeltaHistory: selection}
	publicProofJSON(t, filepath.Join(output, "host-plan.json"), plan)
	phase("bind")
	profile, err = publicProofBindCapture(op, archive, profile, plan)
	publicProofError(t, "bind", err)
	// Preserve the source-only baseline and independently add only the two
	// caller-owned profile outputs. Cache, staging and recovery scratch must
	// still disappear exactly; taking a new usage snapshot here would hide it.
	expectedFinal := receivingUsageWithProfile(t, baseline, profile)
	publicProofJSON(t, filepath.Join(output, "bound-profile.json"), profile)
	phase("import-with-automatic-recovery")
	report, err := RestoreNativeCaptureSetImportWithLease(op, archive, store, lease, profile)
	publicProofError(t, "import", err)
	phase("production-readback")
	publicProofError(t, "production-readback", VerifyNativeCaptureSetReadback(op, store, archive, report))
	phase("independent-imported-readback")
	publicProofIndependentReadback(t, ctx, store, report, req, ledger, output, "imported")
	publicProofJSON(t, filepath.Join(output, "committed-report.json"), report)
	phase("release-and-reopen")
	publicProofError(t, "release-before-reopen", release())
	store, lease = reopen()
	phase("explicit-recovery")
	recoveredProfile, recovered, found, err := RecoverBoundSourceImport(ctx, store)
	if recovered.Archive != nil {
		defer recovered.Archive.ReleaseNativeDecoded()
	}
	publicProofError(t, "recover", err)
	if !found || !reflect.DeepEqual(recovered.Receipt, report.Receipt) || !reflect.DeepEqual(recovered.NativeFamilies, report.NativeFamilies) || !reflect.DeepEqual(recovered.NativeSchemas, report.NativeSchemas) || !reflect.DeepEqual(recovered.NativeHistories, report.NativeHistories) || !reflect.DeepEqual(recovered.SourcePlanes, report.SourcePlanes) {
		t.Fatal("reopened receipt or source reports differ")
	}
	if recoveredProfile.HistoryMode != profile.HistoryMode || recoveredProfile.HistoryTime != profile.HistoryTime || !reflect.DeepEqual(recoveredProfile.NativeDeltaHistory, profile.NativeDeltaHistory) || !reflect.DeepEqual(recovered.NativeDeltaHistory, report.NativeDeltaHistory) {
		t.Fatal("reopened selected mode, source floor or provenance differs")
	}
	phase("reopened-production-readback")
	publicProofError(t, "reopened-production-readback", VerifyNativeCaptureSetReadback(ctx, store, archive, recovered))
	phase("independent-reopened-readback")
	publicProofIndependentReadback(t, ctx, store, recovered, req, ledger, output, "reopened")
	publicProofJSON(t, filepath.Join(output, "recovered-report.json"), recovered)
	store, lease, err = publicProofRunContinuationPhases(t, ctx, store, lease, profile, recovered, continuation, assertion, reopen)
	publicProofError(t, "continuation", err)
	if t.Failed() {
		t.Fatal("public continuation failed")
	}
	if continuation != nil {
		publicProofGuard(t)
		publicProofJSON(t, filepath.Join(output, "continuation.json"), map[string]any{"restore_mode": mode, "completed": true, "target": recovered.Restored, "phase": "after-reopened-readback-before-cleanup"})
	}
	if assertion != nil {
		current, err := store.Meta(ctx)
		publicProofError(t, "post-continuation-meta", err)
		publicProofJSON(t, filepath.Join(output, "post-continuation-reopen.json"), map[string]any{"driver": engine, "restore_mode": mode, "completed": true, "imported_target": recovered.Restored, "current_target": current, "lifecycle": map[bool]string{true: "same-store-memory-lease-refresh", false: "driver-connection-closed-and-reopened"}[engine == "memory"], "phase": "after-writer-scope-release-and-second-reopen-before-cleanup"})
	}
	phase("final-accounting-and-proof")
	if recovered.Archive != nil {
		recovered.Archive.ReleaseNativeDecoded()
	}
	usage, _ := archive.NativeDecodedUsage()
	if usage.CurrentAccountedDecodedBytes != expectedFinal.CurrentAccountedDecodedBytes || !reflect.DeepEqual(usage.CurrentCategories, expectedFinal.CurrentCategories) || !reflect.DeepEqual(usage.CurrentCharges, expectedFinal.CurrentCharges) {
		t.Fatal("conversion or recovery temporary ownership leak")
	}
	proof, ok := archive.NativeDeltaVerification()
	if !ok || proof.Totals.Commits != uint64(req.Commits) {
		t.Fatal("source commit count differs")
	}
	publicProofJSON(t, filepath.Join(output, "pre-release-measurements.json"), map[string]any{"baseline": baseline, "expected": expectedFinal, "decoded": usage, "scope": "source baseline plus exactly the live bound-profile outputs"})
	runtime.KeepAlive(profile)
	archive.ReleaseNativeDecoded()
	released, _ := archive.NativeDecodedUsage()
	if released.CurrentAccountedDecodedBytes != 0 || len(released.CurrentCategories) != 0 || len(released.CurrentCharges) != 0 {
		t.Fatal("final archive release left decoded ownership")
	}
	publicProofJSON(t, filepath.Join(output, "post-release-measurements.json"), map[string]any{"decoded": released, "scope": "after explicit archive release; bytes, categories and counts zero"})
	success := map[string]any{"driver": engine, "capture_sha256": req.CaptureSHA256, "elf_sha256": elfSHA, "commits": req.Commits, "reopen": map[bool]string{true: "same-process-memory-lease-reacquired", false: "driver-connection-closed-and-reopened"}[engine == "memory"], "per_table_exact_bytes": true, "restore_mode": mode, "continuation_completed": continuation != nil, "ledger_after_release_bytes": released.CurrentAccountedDecodedBytes, "ledger_after_release_categories": len(released.CurrentCategories), "ledger_after_release_counts": len(released.CurrentCharges), "full_accounting_or_switch_credit": false}
	if assertion != nil {
		success["post_continuation_reopen_asserted"] = true
	}
	if selection != nil {
		success["native_delta_history"] = recovered.NativeDeltaHistory
	}
	publicProofJSON(t, filepath.Join(output, "SUCCESS.json"), success)
}

// These checks qualify only the shared mode/lifecycle seam. They spend no
// fresh fixture or external large/1200 proof case.
func TestPublicNativeExternalExplicitHostPlan(t *testing.T) {
	archive := publicNativeDeltaArchive(t)
	defer archive.ReleaseNativeDecoded()
	ctx := archive.WithNativeDeltaAccounting(context.Background())
	store, lease, _ := openIdentityTarget(t, "memory")
	plan := publicProofHostIntent(t, archive, nativeHistoryPlanFromArchive(t, archive, store, lease))
	before := reverseState(t, store)
	profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: driver.StoreID(archive.Manifest().StoreID), Method: "offline.restore.import", IdempotencyKey: strings.Repeat("b", 32), ArchiveDigest: archive.SHA256(), HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: nativeFixtureTime.Format(time.RFC3339Nano)}
	original, err := publicProofBindCapture(ctx, archive, profile, plan)
	publicProofError(t, "original-host-binding", err)
	adapted, err := publicProofAdaptHostPlan(t, ctx, archive, plan, func(t *testing.T, ctx context.Context, source *dumpformat.Archive, value driver.SourcePlanePlanV1) (driver.SourcePlanePlanV1, error) {
		if source != archive || ctx.Err() != nil {
			t.Fatal("host adapter lost admitted source or live context")
		}
		value.Principals = append([]driver.SourcePlanePrincipalV1(nil), value.Principals...)
		value.Principals[0].PrincipalJSON = []byte(`{"kind":"user","id":"explicit-source-owner"}`)
		return value, nil
	})
	publicProofError(t, "explicit-host-plan", err)
	bound, err := publicProofBindCapture(ctx, archive, profile, adapted)
	publicProofError(t, "explicit-host-binding", err)
	originalHash, err := original.RequestHash()
	publicProofError(t, "original-request-hash", err)
	adaptedHash, err := bound.RequestHash()
	publicProofError(t, "adapted-request-hash", err)
	if originalHash == adaptedHash || !reflect.DeepEqual(plan.Origins, adapted.Origins) || !reflect.DeepEqual(plan.Entities, adapted.Entities) {
		t.Fatal("explicit principal intent was unbound or changed unselected source dispositions")
	}
	want := errors.New("host intent unavailable")
	_, err = publicProofAdaptHostPlan(t, ctx, archive, plan, func(*testing.T, context.Context, *dumpformat.Archive, driver.SourcePlanePlanV1) (driver.SourcePlanePlanV1, error) {
		return driver.SourcePlanePlanV1{}, want
	})
	if !errors.Is(err, want) {
		t.Fatal("host intent error was swallowed", err)
	}
	for _, field := range []string{"source", "archive", "target"} {
		t.Run("reject-"+field, func(t *testing.T) {
			_, err := publicProofAdaptHostPlan(t, ctx, archive, plan, func(_ *testing.T, _ context.Context, _ *dumpformat.Archive, value driver.SourcePlanePlanV1) (driver.SourcePlanePlanV1, error) {
				switch field {
				case "source":
					value.SourceStoreID += "-foreign"
				case "archive":
					value.ArchiveSHA256 = strings.Repeat("0", 64)
				case "target":
					value.Target.Incarnation += "-foreign"
				}
				return value, nil
			})
			if !errors.Is(err, driver.ErrConflict) {
				t.Fatal("host adapter replaced an authenticated envelope", err)
			}
		})
	}
	if !reflect.DeepEqual(before, reverseState(t, store)) {
		t.Fatal("host plan binding or refusal changed target state")
	}
}

func TestPublicNativeExternalRestoreModes(t *testing.T) {
	for _, test := range []struct {
		name, raw string
		want      driver.RestoreHistoryMode
	}{
		{"absent", `{}`, driver.RestoreHistoryModeFull},
		{"empty", `{"restore_mode":""}`, driver.RestoreHistoryModeFull},
		{"full", `{"restore_mode":"full"}`, driver.RestoreHistoryModeFull},
		{"partial", `{"restore_mode":"partial"}`, driver.RestoreHistoryModePartial},
		{"snapshot", `{"restore_mode":"snapshot"}`, driver.RestoreHistoryModeSnapshot},
		{"unknown", `{"restore_mode":"unknown"}`, ""},
		{"alias", `{"restore_mode":"head"}`, ""},
		{"case", `{"restore_mode":"Full"}`, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			var req publicProofRequest
			if err := json.Unmarshal([]byte(test.raw), &req); err != nil {
				t.Fatal(err)
			}
			mode, err := req.historyMode()
			if test.want != "" {
				if err != nil || mode != test.want {
					t.Fatal("mode lost or substituted", mode, err)
				}
			} else {
				var invalid driver.ErrRestoreHistoryMode
				if !errors.As(err, &invalid) || invalid.Value != req.RestoreMode {
					t.Fatal("invalid mode lost typed identity", err)
				}
			}
		})
	}
	archive := publicNativeDeltaArchive(t)
	defer archive.ReleaseNativeDecoded()
	ctx := archive.WithNativeDeltaAccounting(context.Background())
	store, lease, _ := openIdentityTarget(t, "memory")
	plan := publicProofHostIntent(t, archive, nativeHistoryPlanFromArchive(t, archive, store, lease))
	before := reverseState(t, store)
	for _, selected := range []string{"", "full", "partial", "snapshot"} {
		name := selected
		if name == "" {
			name = "default"
		}
		t.Run("bound-"+name, func(t *testing.T) {
			mode, err := (publicProofRequest{RestoreMode: selected}).historyMode()
			if err != nil {
				t.Fatal(err)
			}
			profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: driver.StoreID(archive.Manifest().StoreID), Method: "offline.restore.import", IdempotencyKey: strings.Repeat("b", 32), ArchiveDigest: archive.SHA256(), HistoryMode: mode, HistoryTime: nativeFixtureTime.Format(time.RFC3339Nano)}
			bound, err := publicProofBindCapture(ctx, archive, profile, plan)
			if mode == driver.RestoreHistoryModeFull {
				if err != nil || bound.HistoryMode != mode || bound.NativeDeltaHistory != nil {
					t.Fatal("original full binding changed", err)
				}
			} else {
				var refused driver.ErrRestoreHistoryRefused
				code, details, ok := driver.ErrorMapping(err)
				if !errors.As(err, &refused) || refused.Mode != mode || refused.Reason != driver.RestoreHistoryReasonUnmappable || !errors.Is(err, driver.ErrUnsupported) || !ok || code != "restore_violation" || details["mode"] != string(mode) || details["reason"] != "unmappable" {
					t.Fatal("native mode refusal lost exact type/code/details", err, code, details)
				}
			}
			if !reflect.DeepEqual(before, reverseState(t, store)) {
				t.Fatal("mode binding mutated the target")
			}
		})
	}
}

func TestPublicNativeExternalHistorySelection(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		mode      driver.RestoreHistoryMode
		first     uint64
	}{
		{"absent", `{}`, driver.RestoreHistoryModeFull, 0},
		{"empty", `{"restore_mode":""}`, driver.RestoreHistoryModeFull, 0},
		{"full", `{"restore_mode":"full"}`, driver.RestoreHistoryModeFull, 0},
		{"snapshot", `{"restore_mode":"snapshot"}`, driver.RestoreHistoryModeSnapshot, 0},
		{"partial-first", `{"restore_mode":"partial","restore_first_sequence":1}`, driver.RestoreHistoryModePartial, 1},
		{"partial-second", `{"restore_mode":"partial","restore_first_sequence":2}`, driver.RestoreHistoryModePartial, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var req publicProofRequest
			if err := json.Unmarshal([]byte(tc.raw), &req); err != nil {
				t.Fatal(err)
			}
			mode, policy, err := req.historySelection()
			if err != nil || mode != tc.mode {
				t.Fatal("selected mode lost or substituted", mode, err)
			}
			if mode == driver.RestoreHistoryModeFull {
				if policy != nil {
					t.Fatal("Full acquired a policy field")
				}
			} else if policy == nil || policy.Format != driver.NativeDeltaHistoryPolicyFormat || policy.FirstSequence != tc.first {
				t.Fatal("explicit operator source selection differs", policy)
			}
		})
	}
	for _, tc := range []struct{ name, raw string }{
		{"missing", `{"restore_mode":"partial"}`},
		{"zero", `{"restore_mode":"partial","restore_first_sequence":0}`},
		{"null", `{"restore_mode":"partial","restore_first_sequence":null}`},
		{"negative", `{"restore_mode":"partial","restore_first_sequence":-1}`},
		{"fractional", `{"restore_mode":"partial","restore_first_sequence":2.5}`},
		{"exponent", `{"restore_mode":"partial","restore_first_sequence":2e0}`},
		{"text", `{"restore_mode":"partial","restore_first_sequence":"2"}`},
		{"boolean", `{"restore_mode":"partial","restore_first_sequence":true}`},
		{"overflow", `{"restore_mode":"partial","restore_first_sequence":18446744073709551616}`},
		{"default-zero", `{"restore_first_sequence":0}`},
		{"full-zero", `{"restore_mode":"full","restore_first_sequence":0}`},
		{"full-null", `{"restore_mode":"full","restore_first_sequence":null}`},
		{"full-positive", `{"restore_mode":"full","restore_first_sequence":2}`},
		{"snapshot-zero", `{"restore_mode":"snapshot","restore_first_sequence":0}`},
		{"snapshot-null", `{"restore_mode":"snapshot","restore_first_sequence":null}`},
		{"snapshot-positive", `{"restore_mode":"snapshot","restore_first_sequence":2}`},
	} {
		t.Run("refused-"+tc.name, func(t *testing.T) {
			var req publicProofRequest
			if err := json.Unmarshal([]byte(tc.raw), &req); err != nil {
				t.Fatal(err)
			}
			mode, policy, err := req.historySelection()
			var refusal driver.ErrRestoreHistoryRefused
			code, details, ok := driver.ErrorMapping(err)
			if policy != nil || !errors.As(err, &refusal) || refusal.Mode != mode || refusal.Reason != driver.RestoreHistoryReasonUnmappable || !ok || code != "restore_violation" || details["mode"] != string(mode) || details["reason"] != "unmappable" {
				t.Fatal("floor refusal lost exact mode/type/code/reason", policy, err, code, details)
			}
		})
	}
}

func TestPublicNativeExternalSelectedModeBinding(t *testing.T) {
	archive := publicNativeDeltaArchive(t)
	defer archive.ReleaseNativeDecoded()
	ctx := archive.WithNativeDeltaAccounting(context.Background())
	store, lease, _ := openIdentityTarget(t, "memory")
	plan := publicProofHostIntent(t, archive, nativeHistoryPlanFromArchive(t, archive, store, lease))
	before := reverseState(t, store)
	hashes := map[string]bool{}
	for _, tc := range []struct{ name, raw string }{
		{"snapshot", `{"restore_mode":"snapshot"}`},
		{"partial-first", `{"restore_mode":"partial","restore_first_sequence":1}`},
		{"partial-second", `{"restore_mode":"partial","restore_first_sequence":2}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var req publicProofRequest
			if err := json.Unmarshal([]byte(tc.raw), &req); err != nil {
				t.Fatal(err)
			}
			mode, policy, err := req.historySelection()
			if err != nil {
				t.Fatal(err)
			}
			profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: driver.StoreID(archive.Manifest().StoreID), Method: "offline.restore.import", IdempotencyKey: strings.Repeat("b", 32), ArchiveDigest: archive.SHA256(), HistoryMode: mode, HistoryTime: nativeFixtureTime.In(time.FixedZone("source-clock", 2*60*60)).Format(time.RFC3339Nano), NativeDeltaHistory: policy}
			bound, err := publicProofBindCapture(ctx, archive, profile, plan)
			if err != nil || bound.HistoryMode != mode || !reflect.DeepEqual(bound.NativeDeltaHistory, policy) || bound.ArchiveDigest != archive.SHA256() || bound.HistoryTime != nativeFixtureTime.UTC().Format(time.RFC3339Nano) || (bound.HistoryDigest == "") != (mode == driver.RestoreHistoryModeSnapshot) {
				t.Fatal("bound selected mode/floor/capture/clock differs", err)
			}
			hash, err := bound.RequestHash()
			if err != nil || hash == "" || hashes[hash] {
				t.Fatal("operator selection not independently hash bound", err)
			}
			hashes[hash] = true
			if !reflect.DeepEqual(before, reverseState(t, store)) {
				t.Fatal("selected binding mutated the target")
			}
		})
	}
	// Synthetic reports exercise the actual readback oracle with literal local
	// floors. Authenticated fixture points supply scalar source provenance only;
	// these controls do not import an archive or create independent fresh proof.
	work, release := archive.NativeDeltaAccountingScope()
	defer release()
	metadata, err := work.NativeSourceHistoryMetadata()
	if err != nil || len(metadata) != 1 || len(metadata[0].Projects) != 1 || metadata[0].ImageCount < 2 {
		t.Fatal("oracle control needs its authenticated source scope", err)
	}
	scope := metadata[0]
	states := publicProofStateLedger{Commits: map[uint64]publicProofState{}}
	for seq := uint64(1); seq <= uint64(scope.ImageCount); seq++ {
		point, ok := archive.NativeDeltaHistoryPoint(seq)
		if !ok {
			t.Fatal("oracle control lost an authenticated source point", seq)
		}
		states.Commits[seq] = publicProofState{Kind: "commit", Sequence: seq, CommitID: point.CommitID, PhysicalRootSHA256: point.RootSHA256}
	}
	headRoot, currentRoot, ok := archive.NativeDeltaCheckpointRoots()
	if !ok {
		t.Fatal("oracle control lost authenticated checkpoint roots")
	}
	states.Head = publicProofState{Kind: "head-checkpoint", CommitID: states.Commits[uint64(scope.ImageCount)].CommitID, PhysicalRootSHA256: headRoot}
	states.Current = publicProofState{Kind: "current", CommitID: states.Head.CommitID, PhysicalRootSHA256: currentRoot}
	for _, tc := range []struct {
		name         string
		first, floor uint64
	}{
		{"oracle-partial-first", 1, 0},
		{"oracle-partial-with-predecessor", 2, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := publicProofRequest{CaptureSHA256: archive.SHA256(), Source: dumpformat.SourceIdentity{StoreID: scope.StoreID, Project: scope.Projects[0]}, Commits: scope.ImageCount, RestoreMode: "partial", RestoreFirstSequence: json.RawMessage(fmt.Sprint(tc.first))}
			mode, policy, err := req.historySelection()
			if err != nil {
				t.Fatal(err)
			}
			first := states.Commits[tc.first]
			provenance := &driver.NativeDeltaHistoryEvidence{Format: driver.NativeDeltaHistoryEvidenceFormat, Mode: mode, ArchiveSHA256: req.CaptureSHA256, SourceStoreID: req.Source.StoreID, Project: req.Source.Project, FirstSequence: tc.first, FirstCommitID: first.CommitID, FirstRootSHA256: first.PhysicalRootSHA256, HeadSequence: uint64(req.Commits), HeadCommitID: states.Head.CommitID, HeadRootSHA256: headRoot, CurrentRootSHA256: currentRoot}
			evidence := driver.HistoryImportEvidence{Source: driver.NativeTableHistorySource, Mode: mode, Complete: scope.Complete && tc.first == 1, First: tc.first, Head: uint64(req.Commits), Floor: model.Natural64(tc.floor), Gaps: []driver.SourceHistoryGap{}, NativeDeltaHistory: provenance}
			if tc.first > 1 {
				baseline := states.Commits[tc.first-1]
				provenance.BaselineSequence, provenance.BaselineCommitID, provenance.BaselineRootSHA256 = tc.first-1, baseline.CommitID, baseline.PhysicalRootSHA256
				provenance.OmittedPrefix = &driver.SourceHistoryGap{First: 1, Last: tc.first - 1, Reason: "operator-selected-floor"}
				evidence.Gaps = []driver.SourceHistoryGap{*provenance.OmittedPrefix}
			}
			if scope.HistoryCoverage != nil {
				provenance.UnavailablePrefixSHA256 = dumpformat.NativeHistoryCoverageSHA256(*scope.HistoryCoverage)
				evidence.NativeBoundaries = []driver.NativeHistoryBoundary{{SourceStoreID: req.Source.StoreID, Project: req.Source.Project, VerifiedCommitCount: uint64(req.Commits) - tc.first + 1, BoundarySHA256: provenance.UnavailablePrefixSHA256}}
			}
			report := Report{Archive: archive, NativeDeltaHistory: provenance}
			got := publicProofSelectedSnapshots(t, report, req, states, evidence, policy, mode)
			want := []uint64{0}
			for seq := uint64(1); seq <= uint64(req.Commits); seq++ {
				want = append(want, seq)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatal("oracle lost selected suffix or unmapped predecessor", got, want)
			}
		})
	}
	t.Run("beyond-authenticated-head", func(t *testing.T) {
		proof, ok := archive.NativeDeltaVerification()
		if !ok {
			t.Fatal("authenticated source proof missing")
		}
		policy := &driver.NativeDeltaHistoryPolicy{Format: driver.NativeDeltaHistoryPolicyFormat, FirstSequence: proof.Totals.Commits + 1}
		err := publicProofCaptureSelection(archive, driver.RestoreHistoryModePartial, policy)
		var refusal driver.ErrRestoreHistoryRefused
		code, details, ok := driver.ErrorMapping(err)
		if !errors.As(err, &refusal) || refusal.Mode != driver.RestoreHistoryModePartial || refusal.Reason != driver.RestoreHistoryReasonNonContiguous || !ok || code != "restore_violation" || details["reason"] != "non-contiguous" || !reflect.DeepEqual(before, reverseState(t, store)) {
			t.Fatal("source ordinal above actual authenticated head was not typed-refused", err, code, details)
		}
	})
}

func TestPublicNativeExternalContinuationLifecycle(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, outcome := range []string{"success", "error", "nil"} {
			t.Run(engine+"/"+outcome, func(t *testing.T) {
				ctx := context.Background()
				store, _, reopen := openIdentityTarget(t, engine)
				before, err := store.Meta(ctx)
				if err != nil {
					t.Fatal(err)
				}
				store, lease := reopen()
				profile := driver.RestoreImportProfile{HistoryMode: driver.RestoreHistoryModeFull}
				report := Report{Restored: before, Recovered: true}
				callbackErr := errors.New("continuation rejected")
				calls := 0
				var callbackCtx context.Context
				var callbackLease driver.ActiveLease
				var continuation publicProofContinuation
				if outcome != "nil" {
					continuation = func(t *testing.T, op context.Context, target driver.Store, active driver.ActiveLease, bound driver.RestoreImportProfile, recovered Report) error {
						calls++
						callbackCtx, callbackLease = op, active
						if target != store || !reflect.DeepEqual(bound, profile) || !reflect.DeepEqual(recovered, report) || op.Err() != nil {
							t.Fatal("callback lost reopened target, context or bound evidence")
						}
						meta, err := target.Meta(op)
						if err != nil || !reflect.DeepEqual(meta, before) {
							t.Fatal("callback target changed before continuation", err)
						}
						tx, err := target.Write(op, active)
						if err != nil {
							t.Fatal("callback lease cannot open target transaction", err)
						}
						if err := tx.Rollback(op); err != nil {
							t.Fatal(err)
						}
						if err := tx.Close(); err != nil {
							t.Fatal(err)
						}
						if outcome == "error" {
							return callbackErr
						}
						return nil
					}
				}
				err = publicProofRunContinuation(t, ctx, store, lease, profile, report, continuation)
				if outcome == "nil" {
					if calls != 0 || err != nil || lease.Renew(ctx) != nil {
						t.Fatal("nil continuation changed the original lease", calls, err)
					}
				} else {
					if calls != 1 || (outcome == "error") != errors.Is(err, callbackErr) || outcome == "success" && err != nil || callbackCtx.Err() == nil {
						t.Fatal("callback count, error or drained context differs", calls, err)
					}
					select {
					case <-callbackLease.Lost():
					default:
						t.Fatal("callback lease retained after return")
					}
				}
				if _, err := store.Meta(ctx); err != nil {
					t.Fatal("target cleaned up before caller returned", err)
				}
			})
		}
	}
}

func TestPublicNativeExternalPostContinuationReopen(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, outcome := range []string{"success", "writer-error", "assertion-error", "cancelled", "nil-assertion", "nil-writer"} {
			t.Run(engine+"/"+outcome, func(t *testing.T) {
				archive := publicNativeDeltaArchive(t)
				defer archive.ReleaseNativeDecoded()
				baseline, _ := archive.NativeDecodedUsage()
				ctx, cancel := context.WithCancel(archive.WithNativeDeltaAccounting(context.Background()))
				defer cancel()
				store, _, reopen := openIdentityTarget(t, engine)
				store, lease := reopen()
				initial := store
				meta, err := store.Meta(ctx)
				if err != nil {
					t.Fatal(err)
				}
				profile := driver.RestoreImportProfile{HistoryMode: driver.RestoreHistoryModeFull}
				report := Report{Restored: meta, Recovered: true, Archive: archive}
				ref := model.RefByID("public-continuation-persisted", model.Natural64(0))
				callbackErr := errors.New("post-continuation control rejected")
				writers, assertions, reopens := 0, 0, 0
				var saved []driver.Record
				var writerCtx, assertionCtx context.Context
				var writerLease, assertionLease driver.ActiveLease
				read := func(op context.Context, target driver.Store) []driver.Record {
					current, err := target.Meta(op)
					if err != nil {
						t.Fatal(err)
					}
					r, err := target.Read(op, driver.View{Incarnation: current.Incarnation, At: &current.Head})
					if err != nil {
						t.Fatal(err)
					}
					rows, err := r.Get(op, []model.Ref{ref})
					closeErr := r.Close()
					if err != nil || closeErr != nil {
						t.Fatal(err, closeErr)
					}
					return rows
				}
				writer := publicProofContinuation(func(t *testing.T, op context.Context, target driver.Store, active driver.ActiveLease, bound driver.RestoreImportProfile, recovered Report) error {
					writers++
					writerCtx, writerLease = op, active
					if target != initial || bound.HistoryMode != profile.HistoryMode || recovered.Archive != archive {
						t.Fatal("writer lost the shared target or archive")
					}
					if outcome == "writer-error" {
						return callbackErr
					}
					current, err := target.Meta(op)
					if err != nil {
						t.Fatal(err)
					}
					tx, err := target.Write(op, active)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Close()
					key := driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
					if _, err := tx.Lock(op, []driver.LockRequest{{Key: key, Mode: driver.LockUpdate}}); err != nil {
						t.Fatal(err)
					}
					if _, err := tx.Head(op); err != nil {
						t.Fatal(err)
					}
					if err := tx.Put(op, driver.Record{Ref: ref, Kind: "config", Body: []byte(`{"persisted":true}`)}); err != nil {
						t.Fatal(err)
					}
					if _, err := tx.Commit(op, driver.ChangesetHeader{Incarnation: current.Incarnation, Kind: model.ChangesetWrite, Principal: restorePrincipal, LockKeys: []driver.LockKey{key}, Changes: []model.ChangeRecord{{Ref: ref, Op: model.OpCreate, ChangedPaths: []string{"/"}}}}); err != nil {
						t.Fatal(err)
					}
					saved = read(op, target)
					if len(saved) != 1 {
						t.Fatal("writer did not commit exactly one observation")
					}
					if outcome == "cancelled" {
						cancel()
					}
					return nil
				})
				assertion := publicProofContinuation(func(t *testing.T, op context.Context, target driver.Store, active driver.ActiveLease, bound driver.RestoreImportProfile, recovered Report) error {
					assertions++
					assertionCtx, assertionLease = op, active
					if writers != 1 || reopens != 1 || (target == initial) != (engine == "memory") || writerCtx.Err() == nil || op.Err() != nil || recovered.Archive != archive || !reflect.DeepEqual(bound, profile) {
						t.Fatal("post-reopen assertion lost ordering, identity or live handles")
					}
					if err := active.Renew(op); err != nil {
						t.Fatal("assertion lease is inactive", err)
					}
					if !reflect.DeepEqual(read(op, target), saved) {
						t.Fatal("committed observation changed across actual reopen")
					}
					if usage, ok := recovered.Archive.NativeDecodedUsage(); !ok || usage.CurrentAccountedDecodedBytes != baseline.CurrentAccountedDecodedBytes || !reflect.DeepEqual(usage.CurrentCharges, baseline.CurrentCharges) {
						t.Fatal("accounted archive lifetime ended before assertion")
					}
					if outcome == "assertion-error" {
						return callbackErr
					}
					return nil
				})
				if outcome == "nil-assertion" || outcome == "nil-writer" {
					assertion = nil
				}
				if outcome == "nil-writer" {
					writer = nil
				}
				ownedReopen := func() (driver.Store, driver.ActiveLease) {
					reopens++
					if writers != 1 || writerCtx.Err() == nil {
						t.Fatal("shared owner reopened before writer scope drained")
					}
					select {
					case <-writerLease.Lost():
					default:
						t.Fatal("shared owner reopened before writer lease released")
					}
					return reopen()
				}
				store, _, err = publicProofRunContinuationPhases(t, ctx, store, lease, profile, report, writer, assertion, ownedReopen)
				wantAfter := outcome == "success" || outcome == "assertion-error"
				if writers != map[bool]int{true: 0, false: 1}[outcome == "nil-writer"] || assertions != map[bool]int{true: 1, false: 0}[wantAfter] || reopens != assertions {
					t.Fatal("writer/assertion/reopen count differs", writers, assertions, reopens)
				}
				if (outcome == "writer-error" || outcome == "assertion-error") != errors.Is(err, callbackErr) || outcome == "cancelled" && !errors.Is(err, context.Canceled) || (outcome == "success" || outcome == "nil-assertion" || outcome == "nil-writer") && err != nil {
					t.Fatal("phase error was lost", err)
				}
				for _, ended := range []struct {
					ctx   context.Context
					lease driver.ActiveLease
				}{{writerCtx, writerLease}, {assertionCtx, assertionLease}} {
					if ended.ctx == nil {
						continue
					}
					if ended.ctx.Err() == nil {
						t.Fatal("callback context retained after return")
					}
					select {
					case <-ended.lease.Lost():
					default:
						t.Fatal("callback lease retained after return")
					}
				}
				if _, err := store.Meta(context.Background()); err != nil {
					t.Fatal("shared target cleaned up before return", err)
				}
			})
		}
	}
	for _, missing := range []string{"writer", "reopen", "archive"} {
		t.Run("missing-"+missing, func(t *testing.T) {
			called := false
			callback := publicProofContinuation(func(*testing.T, context.Context, driver.Store, driver.ActiveLease, driver.RestoreImportProfile, Report) error {
				called = true
				return nil
			})
			writer := callback
			reopen := func() (driver.Store, driver.ActiveLease) { called = true; return nil, nil }
			if missing == "writer" {
				writer = nil
			}
			if missing == "reopen" {
				reopen = nil
			}
			_, _, err := publicProofRunContinuationPhases(t, context.Background(), nil, nil, driver.RestoreImportProfile{}, Report{}, writer, callback, reopen)
			if err == nil || called {
				t.Fatal("incomplete assertion configuration reached a handle", err)
			}
		})
	}
}

// This extra oracle never lowers expected records through the receiver codec.
// Expected old-table envelopes, counts and physical/schema commitments came
// from a separately bound disk-backed semantic replay of the original capture.
func publicProofIndependentReadback(t *testing.T, ctx context.Context, store driver.Store, report Report, req publicProofRequest, ledger []publicProofTable, output, phase string) {
	t.Helper()
	mode, selection, err := req.historySelection()
	publicProofError(t, "readback-selection", err)
	states := publicProofStates(t, req, ledger)
	at := report.Restored.Head
	r, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &at})
	publicProofError(t, "mapping-read", err)
	items, err := r.Get(ctx, []model.Ref{model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0)), model.RefByID(importBindingRecordID, model.Natural64(0))})
	publicProofError(t, "mapping-get", err)
	publicProofError(t, "mapping-close", r.Close())
	var evidence driver.HistoryImportEvidence
	var binding importBindingEvidence
	seenEvidence := map[string]bool{}
	for _, item := range items {
		if seenEvidence[item.Ref.ID] {
			t.Fatal("duplicate stored history/binding evidence")
		}
		seenEvidence[item.Ref.ID] = true
		switch item.Ref.ID {
		case driver.OfflineHistoryEvidenceID:
			publicProofError(t, "history-evidence-decode", json.Unmarshal(item.Body, &evidence))
		case importBindingRecordID:
			publicProofError(t, "binding-evidence-decode", json.Unmarshal(item.Body, &binding))
		default:
			t.Fatal("unexpected stored history/binding evidence")
		}
	}
	when, err := time.Parse(time.RFC3339Nano, req.RestoreTime)
	publicProofError(t, "readback-restore-time", err)
	hash, err := binding.Profile.RequestHash()
	publicProofError(t, "stored-profile-hash", err)
	if len(items) != 2 || evidence.Source != driver.NativeTableHistorySource || evidence.Mode != mode || binding.Profile.HistoryMode != mode || !reflect.DeepEqual(binding.Profile.NativeDeltaHistory, selection) || binding.Profile.ArchiveDigest != req.CaptureSHA256 || string(binding.Profile.SourceStoreID) != req.Source.StoreID || binding.Profile.HistoryTime != when.UTC().Format(time.RFC3339Nano) || hash != binding.RequestHash || hash != report.RequestHash {
		t.Fatal("stored selected mode/floor/capture/clock binding differs")
	}
	snapshots := publicProofSelectedSnapshots(t, report, req, states, evidence, selection, mode)
	first, expectedMappings := uint64(1), req.Commits
	if mode == driver.RestoreHistoryModeSnapshot {
		expectedMappings = 0
	} else if selection != nil {
		first = selection.FirstSequence
		expectedMappings = req.Commits - int(first) + 1
	}
	if len(evidence.Mapping) != expectedMappings {
		t.Fatal("selected source mapping census differs")
	}
	mapping := map[uint64]driver.SourceImportRevision{}
	for _, m := range evidence.Mapping {
		if m.SourceStoreID != req.Source.StoreID || m.Project != req.Source.Project || m.SourceSequence < first || m.SourceSequence > uint64(req.Commits) || mapping[m.SourceSequence].SourceSequence != 0 {
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
	dir := filepath.Join(output, phase)
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for _, seq := range snapshots {
		publicProofGuard(t)
		entries := byState[seq]
		at := report.Restored.Head
		if seq != 0 {
			if mode == driver.RestoreHistoryModePartial && first > 1 && seq == first-1 {
				// The authenticated predecessor is a local baseline, never a
				// claimed source mapping in the selected suffix.
				at = evidence.Floor
			} else {
				m, ok := mapping[seq]
				if !ok || len(m.EventIDs) != 0 || len(m.CommitIDs) != 1 || m.CommitIDs[0] != states.Commits[seq].CommitID {
					t.Fatal("exact original commit mapping differs")
				}
				at = m.Revision
			}
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
		receipt := map[string]any{"sequence": seq, "expected_tables": len(entries), "actual_groups_after_complete_check": len(groups), "complete_unfiltered_query": true, "empty_source_commit": seq != 0 && states.Commits[seq].TableCount == 0}
		if selection != nil {
			receipt["restore_mode"] = mode
			receipt["authenticated_baseline_without_mapping"] = seq != 0 && mode == driver.RestoreHistoryModePartial && first > 1 && seq == first-1
		}
		publicProofJSON(t, filepath.Join(dir, fmt.Sprintf("snapshot-%020d.json", seq)), receipt)
	}
}

// The independent state ledger, not the receiver's evidence builder, supplies
// selected commit IDs and roots. Source coverage remains a separate digest;
// an operator-selected known interval never invents pre-capture ordinals.
func publicProofSelectedSnapshots(t *testing.T, report Report, req publicProofRequest, states publicProofStateLedger, evidence driver.HistoryImportEvidence, selection *driver.NativeDeltaHistoryPolicy, mode driver.RestoreHistoryMode) []uint64 {
	t.Helper()
	first := uint64(1)
	snapshots := []uint64{0}
	if selection == nil {
		if mode != driver.RestoreHistoryModeFull || evidence.NativeDeltaHistory != nil || report.NativeDeltaHistory != nil {
			t.Fatal("Full acquired selected-mode provenance")
		}
	} else {
		want := &driver.NativeDeltaHistoryEvidence{Format: driver.NativeDeltaHistoryEvidenceFormat, Mode: mode, ArchiveSHA256: req.CaptureSHA256, SourceStoreID: req.Source.StoreID, Project: req.Source.Project, HeadSequence: uint64(req.Commits), HeadCommitID: states.Head.CommitID, HeadRootSHA256: states.Head.PhysicalRootSHA256, CurrentRootSHA256: states.Current.PhysicalRootSHA256}
		if report.Archive == nil {
			t.Fatal("selected provenance lost authenticated source archive")
		}
		work, release := report.Archive.NativeDeltaAccountingScope()
		defer release()
		metadata, err := work.NativeSourceHistoryMetadata()
		if err != nil || len(metadata) != 1 || metadata[0].StoreID != req.Source.StoreID || len(metadata[0].Projects) != 1 || metadata[0].Projects[0] != req.Source.Project || metadata[0].ImageCount != req.Commits {
			t.Fatal("selected provenance source identity differs", err)
		}
		if metadata[0].HistoryCoverage != nil {
			want.UnavailablePrefixSHA256 = dumpformat.NativeHistoryCoverageSHA256(*metadata[0].HistoryCoverage)
		}
		if mode == driver.RestoreHistoryModePartial {
			first = selection.FirstSequence
			state, ok := states.Commits[first]
			if !ok || first == 0 || first > uint64(req.Commits) {
				t.Fatal("chosen source ordinal outside independent states")
			}
			want.FirstSequence, want.FirstCommitID, want.FirstRootSHA256 = first, state.CommitID, state.PhysicalRootSHA256
			gaps := []driver.SourceHistoryGap{}
			floor := model.Natural64(0)
			if first > 1 {
				floor = model.Natural64(1)
				baseline := states.Commits[first-1]
				want.BaselineSequence, want.BaselineCommitID, want.BaselineRootSHA256 = first-1, baseline.CommitID, baseline.PhysicalRootSHA256
				want.OmittedPrefix = &driver.SourceHistoryGap{First: 1, Last: first - 1, Reason: "operator-selected-floor"}
				gaps = append(gaps, *want.OmittedPrefix)
				snapshots = append(snapshots, first-1)
			}
			boundaries := []driver.NativeHistoryBoundary(nil)
			if want.UnavailablePrefixSHA256 != "" {
				boundaries = []driver.NativeHistoryBoundary{{SourceStoreID: req.Source.StoreID, Project: req.Source.Project, VerifiedCommitCount: uint64(req.Commits) - first + 1, BoundarySHA256: want.UnavailablePrefixSHA256}}
			}
			if evidence.First != first || evidence.Head != uint64(req.Commits) || evidence.Floor != floor || evidence.Complete != (metadata[0].Complete && first == 1) || !reflect.DeepEqual(evidence.Gaps, gaps) || !reflect.DeepEqual(evidence.NativeBoundaries, boundaries) {
				t.Fatal("selected suffix/baseline and independent prefix descriptors differ")
			}
		} else if mode == driver.RestoreHistoryModeSnapshot {
			expected := driver.HistoryImportEvidence{Source: driver.NativeTableHistorySource, Mode: mode, Complete: false, Floor: model.Natural64(1), Gaps: []driver.SourceHistoryGap{}, Mapping: []driver.SourceImportRevision{}, NativeDeltaHistory: want}
			if !reflect.DeepEqual(evidence, expected) {
				t.Fatal("snapshot claims source mappings or fabricated history")
			}
		} else {
			t.Fatal("selected provenance has an unsupported mode")
		}
		if !reflect.DeepEqual(evidence.NativeDeltaHistory, want) || !reflect.DeepEqual(report.NativeDeltaHistory, want) {
			t.Fatal("selected source IDs/roots/floor/gap provenance differs")
		}
		if mode == driver.RestoreHistoryModeSnapshot {
			return snapshots
		}
	}
	for seq := first; seq <= uint64(req.Commits); seq++ {
		snapshots = append(snapshots, seq)
	}
	return snapshots
}
