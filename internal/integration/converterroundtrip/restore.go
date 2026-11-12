// Package converterroundtrip contains the bounded, offline Beads-to-Plug
// restore seam. The archive validator owns the source trust boundary; this
// package only lowers its already validated records into an empty Plug store.
package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/nativeprojection"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

var (
	// ErrRestoreUnavailable is retained for source compatibility with the
	// parser-only checkpoint. Unsupported semantics and absent verification
	// refuse before opening a write transaction.
	ErrRestoreUnavailable = nativeprojection.ErrRestoreUnavailable
	ErrTargetNotEmpty     = driver.ErrRestoreTargetNotEmpty
)

// Report retains the complete parsed source evidence and the target metadata
// observed before the restore transaction. Target is intentionally the
// pre-restore snapshot so refusal reports cannot be mistaken for a commit.
type Report struct {
	Archive         *dumpformat.Archive
	Target          driver.Meta
	Restored        driver.Meta
	IdempotencyKey  string
	RequestHash     string
	Recovered       bool
	SourcePlanes    *driver.SourcePlaneCommitBindingV1
	Receipt         *driver.IdempotencyRecord
	NativeFamilies  []NativeFamilyReport
	NativeSchemas   []NativeSchemaReport  `json:",omitempty"`
	NativeHistories []NativeHistoryReport `json:",omitempty"`
}

// Verification is a trusted offline comparison, bound to the exact tar bytes.
// The Beads exporter has no derived certificate. Callers must independently
// verify this bounded profile's derived digest; omission fails before writes.
// This does not assert source history, A3, authorization or migration parity.
type Verification struct {
	ArchiveSHA256 string
	DerivedSHA256 string
}

// receivingEvidence is private to a single authenticated receiving operation.
// A successful commit checks its durable receipt against these same validated
// inputs. Restart recovery constructs its own source from the fixed reader.
type receivingEvidence struct {
	archive     *dumpformat.Archive
	plan        *restorePlan
	history     *driver.OfflineHistory
	logical     []byte
	requestHash string
}

// Restore validates the complete archive and restores it into an empty store.
// The wrapper obtains the store lease itself; callers that already own the
// lease (including integration tests) can use RestoreWithLease.
func Restore(ctx context.Context, input io.Reader, target driver.Store, verification ...Verification) (Report, error) {
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	archive, err := dumpformat.ReadWithNativeDeltaAccounting(ctx, input)
	if err != nil {
		return Report{}, err
	}
	return restoreArchive(ctx, archive, target, nil, verification)
}

// RestoreWithLease is the same restore operation for a caller that already
// holds the target's active lease. It is deliberately not a second restore
// implementation: both entry points run the same bounded transaction.
func RestoreWithLease(ctx context.Context, input io.Reader, target driver.Store, lease driver.ActiveLease, verification ...Verification) (Report, error) {
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	archive, err := dumpformat.ReadWithNativeDeltaAccounting(ctx, input)
	if err != nil {
		return Report{}, err
	}
	return restoreArchive(ctx, archive, target, lease, verification)
}

// RestoreImport is the trusted-host entry point for a Beads plug-dump/1
// import. The caller must obtain RestoreImportProfile from the host admission
// seam; a PrincipalRef copied from the archive is not an admission. The
// profile binds the exact archive and derived image digests before this
// function opens a write transaction.
func RestoreImport(ctx context.Context, input io.Reader, target driver.Store, profile driver.RestoreImportProfile, verification ...Verification) (Report, error) {
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	bound, err := profile.Detached()
	if err != nil {
		return Report{}, err
	}
	archive, err := dumpformat.ReadWithNativeDeltaAccounting(ctx, input)
	if err != nil {
		return Report{}, historyReadError(bound.HistoryMode, err)
	}
	return restoreArchiveWithProfile(ctx, archive, target, nil, &bound, verification)
}

// RestoreImportWithLease is RestoreImport for a caller that already owns the
// target lease. It shares the same preparation, allocator and commit path.
// The caller remains responsible for renewal through the entire operation.
func RestoreImportWithLease(ctx context.Context, input io.Reader, target driver.Store, lease driver.ActiveLease, profile driver.RestoreImportProfile, verification ...Verification) (Report, error) {
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	bound, err := profile.Detached()
	if err != nil {
		return Report{}, err
	}
	archive, err := dumpformat.ReadWithNativeDeltaAccounting(ctx, input)
	if err != nil {
		return Report{}, historyReadError(bound.HistoryMode, err)
	}
	return restoreArchiveWithProfile(ctx, archive, target, lease, &bound, verification)
}

// ImportInspection is the read-only source binding needed by a trusted host
// before it admits an import. It contains no actor, authority, or credential
// and never opens or writes a target store.
type ImportInspection struct {
	SourceStoreID driver.StoreID
	ArchiveSHA256 string
	DerivedSHA256 string
}

// InspectImport validates a plug-dump/1 archive and computes the exact
// deterministic derived image digest used by RestoreImport. Callers must still
// perform the mode-specific host admission and construct the complete profile;
// this helper is not an authority check.
func InspectImport(ctx context.Context, input io.Reader) (ImportInspection, error) {
	if err := ctx.Err(); err != nil {
		return ImportInspection{}, err
	}
	archive, err := dumpformat.ReadWithNativeDeltaAccounting(ctx, input)
	if err != nil {
		return ImportInspection{}, err
	}
	plan, err := buildPlanForInspection(archive)
	if err != nil {
		return ImportInspection{}, err
	}
	return ImportInspection{SourceStoreID: driver.StoreID(archive.Manifest().StoreID), ArchiveSHA256: archive.SHA256(), DerivedSHA256: plan.derivedDigest}, nil
}

func restoreArchive(ctx context.Context, archive *dumpformat.Archive, target driver.Store, supplied driver.ActiveLease, checks []Verification) (Report, error) {
	return restoreArchiveWithProfile(ctx, archive, target, supplied, nil, checks)
}

func restoreArchiveWithProfile(ctx context.Context, archive *dumpformat.Archive, target driver.Store, supplied driver.ActiveLease, profile *driver.RestoreImportProfile, checks []Verification) (result Report, restoreErr error) {
	if archive == nil || target == nil {
		return Report{Archive: archive}, fmt.Errorf("restore requires an archive and target store")
	}
	// Conversion plans and their lowered batches live through transaction
	// cleanup, then cease to be retained. The report keeps the original archive.
	originalArchive := archive
	defer originalArchive.ClearNativeDeltaPreparation()
	archive, releasePlan := archive.NativeDeltaAccountingScope()
	defer func() {
		releasePlan()
		if result.Archive != nil && result.Archive.SHA256() == originalArchive.SHA256() {
			result.Archive = originalArchive
		}
	}()
	ctx = archive.WithNativeDeltaAccounting(ctx)
	if archive.Manifest().SourcePlaneAddressContract != "" && (profile == nil || profile.SourcePlanes == nil) {
		return Report{Archive: archive}, fmt.Errorf("%w: source plane archive requires the joint restore adapter", driver.ErrUnsupported)
	}
	var sourceLogical []byte
	if profile != nil && profile.SourcePlanes != nil {
		prepared, prepareErr := prepareSourceArchive(ctx, archive, *profile, true)
		if prepareErr != nil {
			return Report{Archive: archive}, prepareErr
		}
		sourceLogical = prepared.LogicalImage()
	}
	meta, err := target.Meta(ctx)
	report := Report{Archive: archive, Target: meta}
	if err != nil {
		return report, err
	}
	// The historical Restore/RestoreWithLease wrappers predate the trusted
	// host admission profile and retain their non-strict current-image behavior.
	// Strict migrations require the explicit import surface, which defaults to
	// full history and must name snapshot to flatten an incomplete archive.
	if profile != nil {
		mode, modeErr := driver.ParseRestoreHistoryMode(string(profile.HistoryMode))
		if modeErr != nil {
			return report, modeErr
		}
		profile.HistoryMode = mode
		if policyErr := validateArchiveHistoryPolicy(mode, archive); policyErr != nil {
			return report, policyErr
		}
	}
	if !meta.Head.IsZero() {
		return report, ErrTargetNotEmpty
	}

	// Complete semantic validation and digest comparison precede lease/write
	// acquisition. A source archive alone is never proof of usable live state.
	var restoreTime time.Time
	if profile != nil {
		if profile.SourceStoreID != driver.StoreID(archive.Manifest().StoreID) {
			return report, fmt.Errorf("%w: import source store does not match archive", ErrRestoreUnavailable)
		}
		restoreTime = time.Now().UTC()
		if profile.HistoryMode != driver.RestoreHistoryModeSnapshot {
			if profile.HistoryTime == "" || profile.HistoryDigest == "" {
				return report, driver.ErrRestoreHistoryRefused{Mode: profile.HistoryMode, History: archive.Manifest().History, Reason: driver.RestoreHistoryReasonUnmappable}
			}
			restoreTime, err = time.Parse(time.RFC3339Nano, profile.HistoryTime)
			if err != nil {
				return report, err
			}
		}
	}
	plan, err := buildPlanWithProfile(archive, profile, restoreTime, ctx)
	if err != nil {
		return report, err
	}
	report.NativeFamilies = append([]NativeFamilyReport(nil), plan.nativeFamilies...)
	report.NativeSchemas = plan.nativeSchemas
	report.NativeHistories = plan.nativeHistories
	if profile != nil && len(checks) == 0 {
		checks = []Verification{{ArchiveSHA256: profile.ArchiveDigest, DerivedSHA256: profile.DerivedDigest}}
	}
	if len(checks) != 1 || checks[0].ArchiveSHA256 != archive.SHA256() || checks[0].DerivedSHA256 != plan.derivedDigest || profile != nil && (checks[0].ArchiveSHA256 != profile.ArchiveDigest || checks[0].DerivedSHA256 != profile.DerivedDigest) {
		return report, fmt.Errorf("%w: exact archive and rebuilt derived digest verification required", ErrRestoreUnavailable)
	}
	if archive.HasNativeDelta() && profile != nil && profile.HistoryMode == driver.RestoreHistoryModeFull {
		return restoreNativeReceivingStream(ctx, archive, target, supplied, *profile, plan.leaf, sourceLogical)
	}
	var history *driver.OfflineHistory
	var replay driver.OfflineHistoryPlan
	lockPlan := plan
	if profile != nil && profile.HistoryMode != driver.RestoreHistoryModeSnapshot {
		historyArchive, releaseHistory := archive.NativeDeltaAccountingScope()
		defer releaseHistory()
		lowered, lowerErr := lowerSourceHistory(historyArchive, *profile, restoreTime, ctx)
		if lowerErr != nil {
			return report, lowerErr
		}
		digest, digestErr := nativeHistoryDigest(historyArchive, lowered)
		if digestErr != nil || digest != profile.HistoryDigest {
			return report, driver.ErrRestoreHistoryRefused{Mode: profile.HistoryMode, History: archive.Manifest().History, Reason: driver.RestoreHistoryReasonUnmappable}
		}
		tail := make([]driver.Record, 0, len(plan.records))
		for _, r := range plan.records {
			tail = append(tail, driver.Record{Ref: r.ref, Kind: r.kind, Body: r.body})
		}
		buildHistory := driver.BuildOfflineHistory
		if sourceLogical != nil {
			buildHistory = driver.BuildSourcePlaneHistory
		}
		replay, err = buildHistory(meta, *profile, lowered, restoreTime, tail)
		if err != nil {
			return report, err
		}
		history = &lowered
		lockPlan = historyLockPlan(plan, lowered)
	}
	if profile != nil {
		report.IdempotencyKey = profile.IdempotencyKey
		report.RequestHash, err = profile.RequestHash()
		if err != nil {
			return report, err
		}
	} else {
		report.IdempotencyKey, report.RequestHash = recoveryScope(checks[0])
	}

	lease := supplied
	if lease == nil {
		var release func() error
		ctx, lease, release, err = HoldRestoreLease(ctx, target, "plug-converter-restore")
		if err != nil {
			return report, err
		}
		defer func() { restoreErr = errors.Join(restoreErr, release()) }()
	}
	var sourceCapability driver.WorkflowCheckpointStore
	if sourceLogical != nil {
		sourceCapability, err = driver.RequireWorkflowCheckpointStore(target, lease)
		if err != nil {
			return report, err
		}
		prepareProfile := driver.PrepareSourcePlaneImportProfile
		if history != nil {
			prepareProfile = driver.PrepareSourcePlaneHistoryProfile
		}
		if _, err = prepareProfile(*profile, meta, sourceCapability.WorkflowCheckpointLifetime()); err != nil {
			return report, err
		}
		// The final evidence has a new-incarnation-dependent body, but its complete
		// lock is acquired with the rest of the core before Head.
		lockPlan.records = append(append([]restoreRecord(nil), lockPlan.records...), restoreRecord{ref: model.RefByID(driver.SourcePlaneCommitRecordID, model.Natural64(0)), kind: "instance_config"})
	}
	requests := lockRequests(lockPlan)
	tx, err := target.Write(ctx, lease)
	if err != nil {
		return report, err
	}
	defer tx.Close()
	if _, err = tx.Lock(ctx, requests); err != nil {
		return report, err
	}
	// Head is the global writer fence, acquired last. Empty-target validation
	// happens AFTER it; same-lease writers cannot slip a write between the check
	// and publication. The driver repeats the full check at atomic commit.
	if _, err = tx.Head(ctx); err != nil {
		return report, err
	}
	var restored driver.Meta
	if history != nil {
		historyCapability, historyOK := tx.(driver.OfflineHistoryTx)
		if !historyOK {
			return report, driver.ErrRestoreHistoryRefused{Mode: profile.HistoryMode, History: archive.Manifest().History, Reason: driver.RestoreHistoryReasonNotYetSupported}
		}
		if sourceLogical != nil {
			joint, ok := tx.(driver.SourcePlaneHistoryRestoreTx)
			if !ok {
				return report, driver.ErrUnsupported
			}
			restored, err = joint.PrepareSourcePlaneHistory(ctx, *profile, *history, restoreTime)
		} else {
			restored, err = historyCapability.PrepareOfflineHistory(ctx, *profile, *history, restoreTime)
		}
		if err == nil {
			continuation, continuationOK := tx.(driver.AllocatorContinuation)
			if !continuationOK {
				return report, driver.ErrUnsupported
			}
			err = continuation.RestoreAllocators(ctx, plan.allocators)
		}
	} else if sourceLogical != nil {
		capability, ok := tx.(driver.SourcePlaneRestoreTx)
		if !ok {
			return report, driver.ErrUnsupported
		}
		restored, err = capability.PrepareSourcePlaneImport(ctx, *profile)
		if err == nil {
			continuation, ok := tx.(driver.AllocatorContinuation)
			if !ok {
				return report, driver.ErrUnsupported
			}
			err = continuation.RestoreAllocators(ctx, plan.allocators)
		}
	} else if profile == nil {
		capability, ok := tx.(driver.OfflineRestoreTx)
		if !ok {
			return report, driver.ErrUnsupported
		}
		restored, err = capability.PrepareOfflineRestore(ctx, driver.StoreID(archive.Manifest().StoreID))
	} else {
		importCapability, importOK := tx.(driver.RestoreImportTx)
		if !importOK {
			return report, driver.ErrUnsupported
		}
		restored, err = importCapability.PrepareRestoreImport(ctx, *profile)
		if err == nil {
			continuation, continuationOK := tx.(driver.AllocatorContinuation)
			if !continuationOK {
				return report, driver.ErrUnsupported
			}
			err = continuation.RestoreAllocators(ctx, plan.allocators)
		}
	}
	if err != nil {
		return report, err
	}

	if sourceLogical != nil {
		binding, bindErr := prepareSourcePublication(ctx, sourceCapability, lease, tx, *profile, restored, sourceLogical)
		if bindErr != nil {
			return report, bindErr
		}
		body, encodeErr := json.Marshal(binding)
		if encodeErr != nil {
			return report, encodeErr
		}
		plan.records = append(plan.records, restoreRecord{ref: model.RefByID(driver.SourcePlaneCommitRecordID, model.Natural64(0)), kind: "instance_config", body: body})
		report.SourcePlanes = &binding
	}
	changes := make([]model.ChangeRecord, 0, len(plan.records))
	results := make([]model.OpResult, 0, len(plan.records))
	for _, record := range plan.records {
		if err = tx.Put(ctx, driver.Record{Ref: record.ref, Kind: record.kind, Body: record.body}); err != nil {
			return report, err
		}
		changes = append(changes, model.ChangeRecord{Ref: record.ref, Op: model.OpCreate, FromRevision: model.Natural64(0), ChangedPaths: []string{"/"}})
		revision := model.Natural64(1)
		if history != nil {
			revision = restored.Head
			for _, final := range replay.Records {
				if final.Ref == record.ref {
					revision = final.Revision
					break
				}
			}
		}
		results = append(results, model.OpResult{Ref: record.ref, Revision: revision, Record: append([]byte(nil), record.body...)})
	}
	for _, item := range plan.derived {
		if err = tx.PutDerived(ctx, item.Table, nil, []driver.DerivedRow{{Key: item.Key, Value: item.Value}}); err != nil {
			return report, err
		}
	}
	for hash, data := range plan.blobs {
		actual, putErr := tx.PutBlob(ctx, data)
		if putErr != nil {
			return report, putErr
		}
		if string(actual) != hash {
			return report, fmt.Errorf("restore blob digest mismatch")
		}
	}
	if plan.nativeCapture != nil {
		prepared, err := plan.nativeCapture.PrepareNativeDelta(ctx)
		if err != nil {
			return report, err
		}
		if err := prepared.Visit(ctx, dumpformat.NativeDeltaVisitor{File: func(_ string, data []byte) error {
			actual, err := tx.PutBlob(ctx, data)
			if err != nil {
				return err
			}
			if string(actual) != hashBytes(data) {
				return fmt.Errorf("native capture blob digest differs")
			}
			return nil
		}}); err != nil {
			return report, err
		}
	}
	if err = tx.BumpSentinels(ctx, lockPlan.sentinels); err != nil {
		return report, err
	}
	principal := restorePrincipal
	method := "offline.restore"
	agentLabel := ""
	commitTime := time.Now().UTC()
	if profile != nil {
		principal = profile.Admission.Actor
		method = profile.Method
		agentLabel = profile.AgentLabel()
		commitTime = restoreTime
	}
	commitFn := tx.Commit
	if history != nil {
		commitFn = tx.(driver.OfflineHistoryTx).CommitOfflineHistory
	}
	commit, err := commitFn(ctx, driver.ChangesetHeader{
		Incarnation: restored.Incarnation, Kind: model.ChangesetRestore,
		Time: commitTime, Principal: principal, Chain: []string{},
		Method: method, AgentLabel: agentLabel, IdempotencyKey: report.IdempotencyKey, RequestHash: report.RequestHash,
		Response: model.Commit{Revision: restored.Head, Results: results, Warnings: []model.Warning{}},
		Changes:  changes, LockKeys: lockKeys(requests),
	})
	if err != nil {
		var uncertain driver.ErrIndeterminate
		if !errors.As(err, &uncertain) || uncertain.IdempotencyKey != report.IdempotencyKey {
			return report, err
		}
		// Resolve only a matching committed receipt, never replay the restore.
		recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		var recovered Report
		var lookupErr error
		if profile != nil {
			recovered, lookupErr = RecoverImport(recoveryCtx, target, *profile, checks[0])
		} else {
			recovered, lookupErr = Recover(recoveryCtx, target, checks[0])
		}
		cancel()
		if lookupErr != nil {
			return report, err
		}
		report.Restored, report.Recovered = recovered.Restored, true
		report.SourcePlanes, report.Receipt = recovered.SourcePlanes, recovered.Receipt
		report.NativeFamilies = recovered.NativeFamilies
		report.NativeSchemas = recovered.NativeSchemas
		report.NativeHistories = recovered.NativeHistories
		return report, nil
	}
	if commit.Kind != model.ChangesetRestore || commit.Incarnation != restored.Incarnation || commit.Revision != restored.Head || history == nil && len(commit.Changes) != len(plan.records) {
		return report, fmt.Errorf("restore commit did not retain its complete changeset")
	}
	report.Restored = restored
	if sourceLogical != nil {
		evidence := &receivingEvidence{archive: archive, plan: &plan, history: history, logical: sourceLogical, requestHash: report.RequestHash}
		recovered, recoverErr := recoverSourceImport(ctx, target, *profile, checks[0], evidence)
		if recoverErr != nil {
			return report, recoverErr
		}
		report.Receipt = recovered.Receipt
	}
	return report, nil
}

var restorePrincipal = model.PrincipalRef{Kind: "system", ID: "plug-converter"}

const importBindingRecordID = nativeprojection.ImportBindingRecordID

func recoveryScope(check Verification) (string, string) {
	return "restore/" + check.ArchiveSHA256, hashBytes([]byte("beads-head/1\x00" + check.ArchiveSHA256 + "\x00" + check.DerivedSHA256))
}

// Recover is read-only and can run after restart or lease takeover. A normal
// Restore retry still refuses a populated target, including a prior restore.
func Recover(ctx context.Context, target driver.Store, check Verification) (Report, error) {
	if !validDigest(check.ArchiveSHA256) || !validDigest(check.DerivedSHA256) {
		return Report{}, driver.ErrConflict
	}
	key, request := recoveryScope(check)
	meta, err := target.Meta(ctx)
	if err != nil {
		return Report{}, err
	}
	r, err := target.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		return Report{}, err
	}
	defer r.Close()
	principal, _ := driver.PrincipalKey(restorePrincipal)
	receipt, err := r.IdempotencyByKey(ctx, principal, "", "offline.restore", key)
	if err != nil {
		return Report{}, err
	}
	if receipt == nil || receipt.RequestHash != request || receipt.ResponsePending || receipt.Changeset.Kind != model.ChangesetRestore || receipt.Changeset.Incarnation != meta.Incarnation || receipt.Changeset.Revision.String() != "1" || receipt.Commit.Revision.String() != "1" || receipt.Changeset.IdempotencyKey != key {
		return Report{}, driver.ErrConflict
	}
	// Bind recovery to the durable archive marker as well as the receipt.
	rows, err := r.Get(ctx, []model.Ref{model.RefByID("restore/archive", model.Natural64(0))})
	if err != nil || len(rows) != 1 {
		return Report{}, driver.ErrConflict
	}
	var evidence archiveEvidence
	if err = json.Unmarshal(rows[0].Body, &evidence); err != nil || evidence.ArchiveSHA256 != check.ArchiveSHA256 || evidence.DerivedSHA256 != check.DerivedSHA256 {
		return Report{}, driver.ErrConflict
	}
	return Report{Restored: meta, IdempotencyKey: key, RequestHash: request, Recovered: true}, nil
}

// RecoverImport is the read-only uncertainty path for an admitted import. It
// looks up the exact actor/scope/method/key binding and verifies the retained
// archive marker; it never replays records or reseeds an allocator.
func RecoverImport(ctx context.Context, target driver.Store, profile driver.RestoreImportProfile, check Verification) (Report, error) {
	if profile.SourcePlanes != nil {
		return recoverSourceImport(ctx, target, profile, check)
	}
	bound, err := profile.Detached()
	if err != nil || !validDigest(check.ArchiveSHA256) || !validDigest(check.DerivedSHA256) || check.ArchiveSHA256 != bound.ArchiveDigest || check.DerivedSHA256 != bound.DerivedDigest {
		return Report{}, driver.ErrConflict
	}
	request, err := bound.RequestHash()
	if err != nil {
		return Report{}, err
	}
	meta, err := target.Meta(ctx)
	if err != nil {
		return Report{}, err
	}
	r, err := target.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		return Report{}, err
	}
	defer r.Close()
	return recoverImportReader(ctx, r, meta, bound, check, request)
}

func recoverImportReader(ctx context.Context, r driver.Reader, meta driver.Meta, bound driver.RestoreImportProfile, check Verification, request string, prepared ...*receivingEvidence) (Report, error) {
	principal, err := driver.PrincipalKey(bound.Admission.Actor)
	if err != nil {
		return Report{}, err
	}
	receipt, err := r.IdempotencyByKey(ctx, principal, bound.AgentLabel(), bound.Method, bound.IdempotencyKey)
	if err != nil {
		return Report{}, err
	}
	if receipt == nil || receipt.RequestHash != request || receipt.ResponsePending || receipt.Changeset.Kind != model.ChangesetRestore || receipt.Changeset.Incarnation != meta.Incarnation || receipt.Changeset.Revision != receipt.Commit.Revision || receipt.Changeset.IdempotencyKey != bound.IdempotencyKey || bound.HistoryDigest == "" && receipt.Commit.Revision.String() != "1" {
		return Report{}, driver.ErrConflict
	}
	var evidence archiveEvidence
	var evidenceRecord []byte
	var binding importBindingEvidence
	projectRecords := map[string]importProjectMetadata{}
	for _, result := range receipt.Commit.Results {
		if len(result.Record) == 0 {
			continue
		}
		switch {
		case result.Ref.ID == "restore/archive":
			if evidenceRecord != nil {
				return Report{}, driver.ErrConflict
			}
			evidenceRecord = result.Record
			if err = json.Unmarshal(result.Record, &evidence); err != nil {
				return Report{}, driver.ErrConflict
			}
		case result.Ref.ID == importBindingRecordID:
			if err = json.Unmarshal(result.Record, &binding); err != nil {
				return Report{}, driver.ErrConflict
			}
		case strings.HasPrefix(result.Ref.ID, "instance_config/restore-import/project/"):
			var metadata importProjectMetadata
			if err = json.Unmarshal(result.Record, &metadata); err != nil {
				return Report{}, driver.ErrConflict
			}
			projectRecords[result.Ref.ID] = metadata
		}
	}
	if evidence.ArchiveSHA256 != check.ArchiveSHA256 || evidence.DerivedSHA256 != check.DerivedSHA256 || !reflect.DeepEqual(binding.Profile, bound) || binding.RequestHash != request || len(projectRecords) != len(binding.Projects) {
		return Report{}, driver.ErrConflict
	}
	if evidence.NativeCaptureFiles != nil {
		// The immutable receipt is the authority for the admitted ceiling and
		// complete raw-file ledger. Compare the pinned current marker before
		// allocating or reading any capture part, including on clean recovery.
		rows, err := r.Get(ctx, []model.Ref{model.RefByID("restore/archive", model.Natural64(0))})
		if err != nil || len(rows) != 1 || rows[0].Kind != "restore-archive" || !bytes.Equal(rows[0].Body, evidenceRecord) {
			return Report{}, driver.ErrConflict
		}
	}
	for _, project := range binding.Projects {
		metadata, ok := projectRecords[importProjectRecordID(project.SourceAllocator.Prefix)]
		var source dumpformat.Project
		rawSource, sourceOK := metadata.Metadata["plug.restore.sourceAllocator"]
		if sourceOK {
			sourceOK = json.Unmarshal(rawSource, &source) == nil
		}
		if !ok || !reflect.DeepEqual(metadata.SourceIdentity, project.SourceIdentity) || metadata.Project != project.SourceAllocator.Prefix || !reflect.DeepEqual(metadata.ImportProfile, bound) || !sourceOK || !reflect.DeepEqual(source, project.SourceAllocator) || !reflect.DeepEqual(metadata.TargetAllocator, project.TargetAllocator) {
			return Report{}, driver.ErrConflict
		}
	}
	projectRefs := make([]model.Ref, 0, len(binding.Projects)+1)
	projectRefs = append(projectRefs, model.RefByID(importBindingRecordID, model.Natural64(0)))
	for _, project := range binding.Projects {
		projectRefs = append(projectRefs, model.RefByID(importProjectRecordID(project.SourceAllocator.Prefix), model.Natural64(0)))
	}
	protectedRows, err := r.Get(ctx, projectRefs)
	if err != nil || len(protectedRows) != len(projectRefs) {
		return Report{}, driver.ErrConflict
	}
	for _, row := range protectedRows {
		var receiptRecord json.RawMessage
		for _, result := range receipt.Commit.Results {
			if result.Ref.ID == row.Ref.ID {
				receiptRecord = result.Record
				break
			}
		}
		if len(receiptRecord) == 0 || !bytes.Equal(receiptRecord, row.Body) {
			return Report{}, driver.ErrConflict
		}
	}
	if err = compareImportBinding(ctx, r, evidence, binding); err != nil {
		return Report{}, nativeDeltaRecoveryError(err)
	}
	var source *receivingEvidence
	if len(prepared) != 0 {
		source = prepared[0]
		if source == nil || source.requestHash != request || source.archive != nil && source.archive.SHA256() != bound.ArchiveDigest {
			return Report{}, driver.ErrConflict
		}
	}
	if (source == nil || source.archive == nil) && (bound.HistoryDigest != "" || bound.SourcePlanes != nil) {
		raw, readErr := r.Blob(ctx, driver.Hash(evidence.ArchiveSHA256))
		if readErr != nil || hashBytes(raw) != bound.ArchiveDigest {
			return Report{}, driver.ErrConflict
		}
		archive, readErr := readRetainedImportArchive(ctx, r, raw, evidence)
		if readErr != nil {
			return Report{}, nativeDeltaRecoveryError(readErr)
		}
		if source == nil {
			defer archive.ReleaseNativeDecoded()
			source = &receivingEvidence{archive: archive, requestHash: request}
		} else {
			source.archive = archive
		}
	}
	if bound.HistoryDigest != "" {
		if err = validateHistoryRecoveryArchive(ctx, r, bound, evidence, receipt, source.archive, source.history); err != nil {
			return Report{}, nativeDeltaRecoveryError(err)
		}
	}
	var families []NativeFamilyReport
	var schemas []NativeSchemaReport
	var histories []NativeHistoryReport
	if source != nil && bound.SourcePlanes != nil && bound.HistoryMode == driver.RestoreHistoryModeFull {
		families, schemas, histories, err = recoverNativeFamiliesArchive(ctx, r, bound, receipt, source.archive, source.plan)
	}
	if err != nil {
		return Report{}, nativeDeltaRecoveryError(err)
	}
	return Report{Restored: meta, IdempotencyKey: bound.IdempotencyKey, RequestHash: request, Recovered: true, Receipt: receipt, NativeFamilies: families, NativeSchemas: schemas, NativeHistories: histories}, nil
}

type restoreRecord struct {
	ref  model.Ref
	kind string
	body []byte
}

// restorePlan is the receiver's mutable publication view of a detached leaf
// plan. Publication appends target-incarnation-dependent records only here.
type restorePlan struct {
	leaf            nativeprojection.Plan
	nativeFamilies  []NativeFamilyReport
	nativeSchemas   []NativeSchemaReport
	nativeHistories []NativeHistoryReport
	records         []restoreRecord
	sentinels       []driver.Sentinel
	derived         []derivedImage
	derivedDigest   string
	blobs           map[string][]byte
	nativeCapture   *dumpformat.Archive
	allocators      []driver.OfflineAllocator
}

type importBindingEvidence = nativeprojection.ImportBindingEvidence
type importProjectBinding = nativeprojection.ImportProjectBinding
type importProjectMetadata = nativeprojection.ImportProjectMetadata

func importProjectRecordID(prefix string) string {
	return nativeprojection.ImportProjectRecordID(prefix)
}

func compareImportBinding(ctx context.Context, r driver.Reader, evidence archiveEvidence, binding importBindingEvidence) error {
	manifestFile, ok := evidence.Members["manifest.json"]
	if !ok {
		return driver.ErrConflict
	}
	manifestRaw, err := r.Blob(ctx, driver.Hash(manifestFile.SHA256))
	if err != nil {
		return err
	}
	var manifest dumpformat.Manifest
	if err = json.Unmarshal(manifestRaw, &manifest); err != nil {
		return err
	}
	if binding.Profile.HistoryDigest == "" {
		if err = driver.ValidateRestoreHistoryPolicy(binding.Profile.HistoryMode, manifest.History); err != nil {
			return err
		}
	} else if binding.Profile.HistoryTime == "" || binding.Profile.HistoryMode == driver.RestoreHistoryModeFull && manifest.History != "verified" && manifest.SourceFamilyContract != "beads-source-families/1" || binding.Profile.HistoryMode == driver.RestoreHistoryModePartial && manifest.History != "verified" && manifest.History != "available-unverified" {
		return driver.ErrConflict
	}
	if len(binding.Projects) != len(manifest.Projects) {
		return driver.ErrConflict
	}
	allocators, ok := r.(driver.AllocatorReader)
	if !ok {
		return driver.ErrUnsupported
	}
	for _, source := range manifest.Projects {
		var found *importProjectBinding
		for i := range binding.Projects {
			if binding.Projects[i].SourceAllocator.Prefix == source.Prefix {
				found = &binding.Projects[i]
				break
			}
		}
		if found == nil || !reflect.DeepEqual(found.SourceAllocator, source) || found.TargetAllocator.Prefix != source.Prefix {
			return driver.ErrConflict
		}
		actual, err := allocators.LoadAllocator(ctx, found.TargetAllocator.Prefix)
		if err != nil || !allocatorContinues(found.TargetAllocator, actual) {
			return driver.ErrConflict
		}
	}
	return nil
}

func buildPlan(archive *dumpformat.Archive) (restorePlan, error) {
	plan, err := nativeprojection.Build(archive)
	return receivePlan(archive, plan, err)
}

func buildPlanWithProfile(archive *dumpformat.Archive, profile *driver.RestoreImportProfile, restoreTime time.Time, contexts ...context.Context) (restorePlan, error) {
	plan, err := nativeprojection.BuildWithProfile(archive, profile, restoreTime, contexts...)
	return receivePlan(archive, plan, err)
}

func buildPlanForInspection(archive *dumpformat.Archive) (restorePlan, error) {
	plan, err := nativeprojection.BuildForInspection(archive)
	return receivePlan(archive, plan, err)
}

func receivePlan(archive *dumpformat.Archive, plan nativeprojection.Plan, err error) (restorePlan, error) {
	if err != nil {
		return restorePlan{}, err
	}
	image, err := plan.Snapshot()
	if err != nil {
		return restorePlan{}, err
	}
	view := restorePlan{leaf: plan, nativeFamilies: image.NativeFamilies, nativeSchemas: image.NativeSchemas, nativeHistories: image.NativeHistories, sentinels: image.Sentinels, derived: image.Derived, derivedDigest: image.DerivedDigest, blobs: image.Blobs, allocators: image.Allocators}
	for _, record := range image.Records {
		view.records = append(view.records, restoreRecord{ref: record.Ref, kind: record.Kind, body: record.Body})
	}
	if plan.HasNativeCapture() {
		view.nativeCapture = archive
	}
	return view, nil
}

func lockRequests(plan restorePlan) []driver.LockRequest {
	keys := map[string]driver.LockKey{}
	for _, allocator := range plan.allocators {
		key, err := driver.AllocatorLock(allocator.Prefix)
		if err != nil {
			continue
		}
		encoded, _ := driver.EncodeLockKey(key)
		keys[encoded] = key
	}
	for _, record := range plan.records {
		ref := record.ref
		key := driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
		encoded, _ := driver.EncodeLockKey(key)
		keys[encoded] = key
	}
	for _, item := range plan.derived {
		key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: item.Table, Key: item.Key}}
		encoded, _ := driver.EncodeLockKey(key)
		keys[encoded] = key
	}
	for _, sentinel := range plan.sentinels {
		key := driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}
		encoded, _ := driver.EncodeLockKey(key)
		keys[encoded] = key
	}
	ordered := make([]string, 0, len(keys))
	for encoded := range keys {
		ordered = append(ordered, encoded)
	}
	sort.Strings(ordered)
	requests := make([]driver.LockRequest, 0, len(ordered))
	for _, encoded := range ordered {
		requests = append(requests, driver.LockRequest{Key: keys[encoded], Mode: driver.LockUpdate})
	}
	return requests
}

func lockKeys(requests []driver.LockRequest) []driver.LockKey {
	keys := make([]driver.LockKey, len(requests))
	for i, request := range requests {
		keys[i] = request.Key
	}
	return keys
}
