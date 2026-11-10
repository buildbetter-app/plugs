// Package converterroundtrip contains the bounded, offline Beads-to-Plug
// restore seam. The archive validator owns the source trust boundary; this
// package only lowers its already validated records into an empty Plug store.
package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

var (
	// ErrRestoreUnavailable is retained for source compatibility with the
	// parser-only checkpoint. Unsupported semantics and absent verification
	// refuse before opening a write transaction.
	ErrRestoreUnavailable = errors.New("trusted offline restore seam unavailable")
	ErrTargetNotEmpty     = driver.ErrRestoreTargetNotEmpty
)

// Report retains the complete parsed source evidence and the target metadata
// observed before the restore transaction. Target is intentionally the
// pre-restore snapshot so refusal reports cannot be mistaken for a commit.
type Report struct {
	Archive        *dumpformat.Archive
	Target         driver.Meta
	Restored       driver.Meta
	IdempotencyKey string
	RequestHash    string
	Recovered      bool
	SourcePlanes   *driver.SourcePlaneCommitBindingV1
	Receipt        *driver.IdempotencyRecord
	NativeFamilies []NativeFamilyReport
}

// Verification is a trusted offline comparison, bound to the exact tar bytes.
// The Beads exporter has no derived certificate. Callers must independently
// verify this bounded profile's derived digest; omission fails before writes.
// This does not assert source history, A3, authorization or migration parity.
type Verification struct {
	ArchiveSHA256 string
	DerivedSHA256 string
}

// Restore validates the complete archive and restores it into an empty store.
// The wrapper obtains the store lease itself; callers that already own the
// lease (including integration tests) can use RestoreWithLease.
func Restore(ctx context.Context, input io.Reader, target driver.Store, verification ...Verification) (Report, error) {
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	archive, err := dumpformat.Read(input)
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
	archive, err := dumpformat.Read(input)
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
	archive, err := dumpformat.Read(input)
	if err != nil {
		return Report{}, historyReadError(bound.HistoryMode, err)
	}
	return restoreArchiveWithProfile(ctx, archive, target, nil, &bound, verification)
}

// RestoreImportWithLease is RestoreImport for a caller that already owns the
// target lease. It shares the same preparation, allocator and commit path.
func RestoreImportWithLease(ctx context.Context, input io.Reader, target driver.Store, lease driver.ActiveLease, profile driver.RestoreImportProfile, verification ...Verification) (Report, error) {
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	bound, err := profile.Detached()
	if err != nil {
		return Report{}, err
	}
	archive, err := dumpformat.Read(input)
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
	archive, err := dumpformat.Read(input)
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

func restoreArchiveWithProfile(ctx context.Context, archive *dumpformat.Archive, target driver.Store, supplied driver.ActiveLease, profile *driver.RestoreImportProfile, checks []Verification) (Report, error) {
	if archive == nil || target == nil {
		return Report{Archive: archive}, fmt.Errorf("restore requires an archive and target store")
	}
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
	plan, err := buildPlanWithProfile(archive, profile, restoreTime)
	if err != nil {
		return report, err
	}
	report.NativeFamilies = append([]NativeFamilyReport(nil), plan.nativeFamilies...)
	if profile != nil && len(checks) == 0 {
		checks = []Verification{{ArchiveSHA256: profile.ArchiveDigest, DerivedSHA256: profile.DerivedDigest}}
	}
	if len(checks) != 1 || checks[0].ArchiveSHA256 != archive.SHA256() || checks[0].DerivedSHA256 != plan.derivedDigest || profile != nil && (checks[0].ArchiveSHA256 != profile.ArchiveDigest || checks[0].DerivedSHA256 != profile.DerivedDigest) {
		return report, fmt.Errorf("%w: exact archive and rebuilt derived digest verification required", ErrRestoreUnavailable)
	}
	var history *driver.OfflineHistory
	var replay driver.OfflineHistoryPlan
	lockPlan := plan
	if profile != nil && profile.HistoryMode != driver.RestoreHistoryModeSnapshot {
		lowered, lowerErr := lowerSourceHistory(archive, *profile, restoreTime)
		if lowerErr != nil {
			return report, lowerErr
		}
		digest, digestErr := driver.OfflineHistoryDigest(lowered)
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
		lease, err = target.Lease(ctx, "plug-converter-restore", driver.LeaseTTL)
		if err != nil {
			return report, err
		}
		defer func() { _ = lease.Release(context.WithoutCancel(ctx)) }()
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
		return report, nil
	}
	if commit.Kind != model.ChangesetRestore || commit.Incarnation != restored.Incarnation || commit.Revision != restored.Head || history == nil && len(commit.Changes) != len(plan.records) {
		return report, fmt.Errorf("restore commit did not retain its complete changeset")
	}
	report.Restored = restored
	if sourceLogical != nil {
		recovered, recoverErr := recoverSourceImport(ctx, target, *profile, checks[0])
		if recoverErr != nil {
			return report, recoverErr
		}
		report.Receipt = recovered.Receipt
	}
	return report, nil
}

var restorePrincipal = model.PrincipalRef{Kind: "system", ID: "plug-converter"}

const importBindingRecordID = "instance_config/restore-import/binding"

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

func recoverImportReader(ctx context.Context, r driver.Reader, meta driver.Meta, bound driver.RestoreImportProfile, check Verification, request string) (Report, error) {
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
	var binding importBindingEvidence
	projectRecords := map[string]importProjectMetadata{}
	for _, result := range receipt.Commit.Results {
		if len(result.Record) == 0 {
			continue
		}
		switch {
		case result.Ref.ID == "restore/archive":
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
		return Report{}, driver.ErrConflict
	}
	if bound.HistoryDigest != "" {
		if err = validateHistoryRecovery(ctx, r, bound, evidence, receipt); err != nil {
			return Report{}, driver.ErrConflict
		}
	}
	families, err := recoverNativeFamilies(ctx, r, bound, evidence, receipt)
	if err != nil {
		return Report{}, driver.ErrConflict
	}
	return Report{Restored: meta, IdempotencyKey: bound.IdempotencyKey, RequestHash: request, Recovered: true, Receipt: receipt, NativeFamilies: families}, nil
}

type restoreRecord struct {
	ref  model.Ref
	kind string
	body []byte
}

type restorePlan struct {
	migration      *dumpformat.ImportMigration
	nativeOntology *dumpformat.NativeStatusOntology
	nativeFamilies []NativeFamilyReport
	records        []restoreRecord
	sentinels      []driver.Sentinel
	derived        []derivedImage
	derivedDigest  string
	blobs          map[string][]byte
	allocators     []driver.OfflineAllocator
}

// importBindingEvidence is an immutable, transaction-local copy of the full
// admitted import binding. Source allocator declarations are retained as
// dumpformat projects, because their widths are provenance (and may be below
// the live allocator's continuation minimum). Target allocators are the exact
// seeded Beads profiles from which recovery must observe continuation.
type importBindingEvidence struct {
	Profile     driver.RestoreImportProfile `json:"profile"`
	RequestHash string                      `json:"request_hash"`
	Projects    []importProjectBinding      `json:"projects"`
}

type importProjectBinding struct {
	SourceIdentity  *dumpformat.SourceIdentity `json:"sourceIdentity,omitempty"`
	SourceAllocator dumpformat.Project         `json:"sourceAllocator"`
	TargetAllocator driver.OfflineAllocator    `json:"targetAllocator"`
}

type importProjectMetadata struct {
	SourceIdentity  *dumpformat.SourceIdentity  `json:"sourceIdentity,omitempty"`
	Project         string                      `json:"project"`
	ImportProfile   driver.RestoreImportProfile `json:"importProfile"`
	Metadata        map[string]json.RawMessage  `json:"metadata"`
	TargetAllocator driver.OfflineAllocator     `json:"targetAllocator"`
}

func importProjectRecordID(prefix string) string {
	return "instance_config/restore-import/project/" + prefix
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

type planMode uint8

const (
	beadsAgentStateMetadataKey = "beads.agent_state"
	beadsAwaitIDMetadataKey    = "beads.await_id"
)

const (
	planLegacy planMode = iota
	planImport
	planInspection
)

func buildPlan(archive *dumpformat.Archive) (restorePlan, error) {
	return buildPlanMode(archive, nil, time.Time{}, planLegacy)
}

func buildPlanWithProfile(archive *dumpformat.Archive, profile *driver.RestoreImportProfile, restoreTime time.Time) (restorePlan, error) {
	mode := planLegacy
	if profile != nil {
		mode = planImport
	}
	return buildPlanMode(archive, profile, restoreTime, mode)
}

func buildPlanForInspection(archive *dumpformat.Archive) (restorePlan, error) {
	return buildPlanMode(archive, nil, time.Time{}, planInspection)
}

func buildPlanMode(archive *dumpformat.Archive, profile *driver.RestoreImportProfile, restoreTime time.Time, mode planMode) (restorePlan, error) {
	manifest := archive.Manifest()
	nativeTables := len(archive.NativeSourceHistories()) != 0
	migration, _ := archive.ImportMigration()
	if mode == planLegacy && migration != nil {
		// A missing import profile cannot opt a newly strict migration into
		// snapshot semantics. Inspection remains available without admission.
		return restorePlan{}, driver.ErrRestoreHistoryRefused{Mode: driver.RestoreHistoryModeFull, History: manifest.History, Reason: driver.RestoreHistoryReasonUnavailable}
	}
	if mode == planLegacy && manifest.History != "unavailable" {
		return restorePlan{}, fmt.Errorf("%w: unverified source history cannot be restored", ErrRestoreUnavailable)
	}
	if mode == planLegacy && len(archive.Records("comment")) != 0 {
		return restorePlan{}, fmt.Errorf("%w: unverified comment attribution", ErrRestoreUnavailable)
	}
	projects := map[string]dumpformat.Project{}
	for _, project := range manifest.Projects {
		// Imported archives preserve the source allocator's declared hash
		// width (the canonical contract admits 1..64). The live target
		// allocator is seeded separately below, so this lower bound must not
		// inherit the legacy restore profile's hash32 compatibility guard.
		if project.IDMode != "hash" || project.Prefix == "" || project.HashLength < 1 {
			return restorePlan{}, fmt.Errorf("%w: allocator continuation is unsupported for %s", ErrRestoreUnavailable, project.Prefix)
		}
		if mode == planLegacy && (project.Prefix != "pl" || project.HashLength != 32 || project.LastID != 0 || len(project.ChildCounters) != 0) {
			return restorePlan{}, fmt.Errorf("%w: allocator continuation is unsupported for %s", ErrRestoreUnavailable, project.Prefix)
		}
		projects[project.Prefix] = project
	}
	if profile != nil && restoreTime.IsZero() {
		return restorePlan{}, fmt.Errorf("%w: import restore time is required", ErrRestoreUnavailable)
	}

	projection, err := newSourceCoreProjection(profile)
	if err != nil {
		return restorePlan{}, err
	}
	entities := projection.entities
	beads := archive.Records("bead")
	if entities != nil {
		var durable []dumpformat.Record
		for _, source := range beads {
			entity, present := entities[driver.SourceEntityKey{Project: source.Project, Plane: source.Source.Table, ID: source.ID}]
			if !present {
				return restorePlan{}, fmt.Errorf("%w: historical source entity lacks an explicit disposition: %s/%s/%s", ErrRestoreUnavailable, source.Project, source.Source.Table, source.ID)
			}
			if entity.TargetKind == "bead" {
				durable = append(durable, source)
			}
		}
		beads = durable
	}
	docs := map[string]pipeline.Document{}
	if len(beads) > 20000 {
		return restorePlan{}, fmt.Errorf("restore graph exceeds local service limit")
	}
	for _, source := range beads {
		project, knownProject := projects[source.Project]
		if !knownProject || !validSourceID(source.ID, project, mode != planLegacy) {
			return restorePlan{}, fmt.Errorf("%w: source ID does not match the declared allocator", ErrRestoreUnavailable)
		}
		status, ok := targetStatus(stringValue(source.Source.Fields["status"]))
		if manifest.StatusCategoryContract == dumpformat.StatusCategoryContract {
			status = stringValue(source.Source.Fields["status"])
			ok = false
			for _, declared := range project.Statuses {
				ok = ok || declared.Name == status
			}
		}
		targetType := stringValue(source.Body["type"])
		if migration != nil {
			target, present := migration.Target(source.Project, source.ID)
			status, targetType, ok = target.Status, target.Type, present
		}
		if !ok {
			return restorePlan{}, fmt.Errorf("unsupported source status for %s", source.ID)
		}
		// The admitted issue schema (including strict-union type aliases) has
		// title/description plus the kernel priority field. Preserve every other
		// source field in metadata so restored beads remain writable by the ordinary pipeline instead of
		// presenting unsupported source columns as typed Plug fields.
		fields := map[string]any{"status": status}
		metadata := map[string]json.RawMessage{}
		for _, key := range []string{"title", "description", "priority"} {
			if value := source.Body[key]; value != nil && projection.mapsField(source, key) {
				fields[key] = value
			}
		}
		var rawMetadata map[string]any
		if projection.mapsField(source, "metadata") {
			rawMetadata, err = dumpformat.SourceMetadataObject(source.Body["metadata"])
			if err != nil {
				return restorePlan{}, fmt.Errorf("bead %s metadata: %w", source.ID, err)
			}
		}
		if raw := rawMetadata; raw != nil {
			for key, value := range raw {
				if strings.HasPrefix(key, "beads.") || strings.HasPrefix(key, "plug.restore.") {
					return restorePlan{}, fmt.Errorf("reserved restore metadata key %q", key)
				}
				encoded, err := json.Marshal(value)
				if err != nil {
					return restorePlan{}, fmt.Errorf("bead %s metadata: %w", source.ID, err)
				}
				metadata[key] = encoded
			}
		}
		for _, field := range dumpformat.OpaqueIssueFields() {
			value, present := source.Source.Fields[field]
			if !present {
				continue
			}
			encoded, err := json.Marshal(value)
			if err != nil {
				return restorePlan{}, fmt.Errorf("bead %s opaque field %s: %w", source.ID, field, err)
			}
			// Opaque Beads columns are stored as reserved metadata. Plug never
			// interprets these values; the importer owns their meaning.
			metadata["beads."+field] = encoded
		}
		for key, value := range source.Body {
			switch key {
			case "title", "description", "priority", "metadata", "labels":
				continue
			}
			encoded, err := json.Marshal(value)
			if err != nil {
				return restorePlan{}, fmt.Errorf("bead %s source field %s: %w", source.ID, key, err)
			}
			metadata["beads.source."+key] = encoded
		}
		sourceStatus, err := json.Marshal(stringValue(source.Source.Fields["status"]))
		if err != nil {
			return restorePlan{}, fmt.Errorf("bead %s source status: %w", source.ID, err)
		}
		metadata["beads.source.status"] = sourceStatus
		sourceFields, err := json.Marshal(source.Source.Fields)
		if err != nil {
			return restorePlan{}, fmt.Errorf("bead %s source fields: %w", source.ID, err)
		}
		metadata["beads.source_fields"] = sourceFields
		if key, scoped := source.SourceKey(); scoped {
			metadata["beads.source_identity"], err = json.Marshal(source.Source.Identity)
			if err != nil {
				return restorePlan{}, err
			}
			metadata["beads.source_key"], err = json.Marshal(key)
			if err != nil {
				return restorePlan{}, err
			}
		}
		if project, ok := projects[source.Project]; ok {
			encoded, err := json.Marshal(project)
			if err != nil {
				return restorePlan{}, err
			}
			if profile == nil {
				// Legacy restore keeps its historical per-bead mirror. Imported
				// source/target allocator provenance is authoritative only in the
				// protected project records below.
				metadata["plug.restore.allocator"] = encoded
			}
		}
		labels, labelsOK := exactLabelSet(source.Body["labels"])
		if !labelsOK {
			return restorePlan{}, fmt.Errorf("%w: source labels are not writable for %s", ErrRestoreUnavailable, source.ID)
		}
		if len(labels) != 0 {
			fields["labels"] = append([]string(nil), labels...)
		}
		doc := pipeline.Document{Labels: labels, ID: source.ID, Type: targetType, Project: source.Project, Fields: fields, Metadata: metadata}
		// The admitted source row is the creation authority. Its exact value
		// remains in reserved source metadata above; absence stays unknown.
		// Restore time, version time and incomplete history cannot fill it in.
		if projection.mapsField(source, "created_at") {
			createdAt, err := sourceCreationTime(source)
			if err != nil {
				return restorePlan{}, err
			}
			doc.CreatedAt = createdAt
		}
		docs[source.ID] = doc
	}

	if profile != nil {
		for _, source := range archive.Records("comment") {
			issueID := stringValue(source.Body["issue_id"])
			doc, ok := docs[issueID]
			if !ok && entities != nil && entities[driver.SourceEntityKey{Project: source.Project, Plane: dumpformat.DurableEntityTable, ID: issueID}].TargetKind == "workflow:item" {
				continue
			}
			if !ok {
				return restorePlan{}, fmt.Errorf("comment %s has no restored bead", source.ID)
			}
			author, ok := source.Body["author"].(string)
			if !ok {
				return restorePlan{}, fmt.Errorf("comment %s author is not text", source.ID)
			}
			createdAt := stringValue(source.Body["created_at"])
			sourceTime, err := time.Parse(time.RFC3339Nano, createdAt)
			if err != nil {
				return restorePlan{}, fmt.Errorf("comment %s timestamp: %w", source.ID, err)
			}
			comment, err := pipeline.NewImportedComment(*profile, stringValue(source.Body["text"]), author, sourceTime, restoreTime)
			if err != nil {
				return restorePlan{}, fmt.Errorf("comment %s: %w", source.ID, err)
			}
			doc.Comments = append(doc.Comments, comment)
			docs[issueID] = doc
		}
	}

	for _, source := range archive.Records("edge") {
		coreEdge, err := projection.coreEdge(source)
		if err != nil {
			return restorePlan{}, err
		}
		if !coreEdge {
			continue
		}
		var targetKind model.LinkTargetKind
		switch stringValue(source.Body["target_kind"]) {
		case "depends_on_issue_id":
		case "depends_on_external":
			targetKind = model.LinkTargetExternal
		default:
			return restorePlan{}, fmt.Errorf("%w: unsupported typed relation %s: %w", ErrRestoreUnavailable, source.ID, &dumpformat.UnresolvedDependencyError{
				EdgeID: source.ID, Project: source.Project, Source: stringValue(source.Body["from"]), Target: stringValue(source.Body["to"]), TargetKind: stringValue(source.Body["target_kind"]),
			})
		}
		from, to := stringValue(source.Body["from"]), stringValue(source.Body["to"])
		doc, ok := docs[from]
		if _, targetExists := docs[to]; !ok || targetKind != model.LinkTargetExternal && !targetExists {
			return restorePlan{}, fmt.Errorf("%w: edge %s has no restored endpoint: %w", ErrRestoreUnavailable, source.ID, &dumpformat.UnresolvedDependencyError{
				EdgeID: source.ID, Project: source.Project, Source: from, Target: to, TargetKind: stringValue(source.Body["target_kind"]),
			})
		}
		kind := stringValue(source.Body["type"])
		targetKindName := stringValue(source.Body["target_kind"])
		switch kind {
		case "blocks":
		case "waits-for", "conditional-blocks", "tracks":
			if targetKind != model.LinkTargetExternal && (!dumpformat.IsVersionedLocalKnownEdgeType(manifest.EdgeVersion(), targetKindName, kind) || mode == planLegacy) {
				return restorePlan{}, fmt.Errorf("%w: relation type %s requires the versioned import contract", ErrRestoreUnavailable, kind)
			}
		case "related":
			kind = "relates-to"
		case "parent-child":
			if targetKind == model.LinkTargetExternal {
				break
			}
			// The source edge points from child to parent. A Plug parent is
			// core-owned hierarchy, not a second generic link. Keep the source
			// edge mirror below for exact reverse provenance.
			if mode == planLegacy {
				return restorePlan{}, fmt.Errorf("%w: relation type %s", ErrRestoreUnavailable, kind)
			}
			if doc.Parent != nil {
				return restorePlan{}, fmt.Errorf("%w: bead %s has multiple parent edges", ErrRestoreUnavailable, from)
			}
			parent := model.RefByID(to, model.Natural64(0))
			doc.Parent = &parent
			metadata, err := dumpformat.SourceLinkMetadata(manifest.EdgeVersion(), source, kind)
			if err != nil {
				return restorePlan{}, fmt.Errorf("%w: parent metadata: %v", ErrRestoreUnavailable, err)
			}
			doc.ParentMetadata = metadata
			docs[from] = doc
			continue
		default:
			if !dumpformat.IsVersionedLocalKnownEdgeType(manifest.EdgeVersion(), targetKindName, kind) &&
				!dumpformat.IsLocalOpaqueEdgeType(manifest.EdgeVersion(), targetKindName, kind) &&
				(targetKind != model.LinkTargetExternal || kind == "" || len(kind) > 32 || !utf8.ValidString(kind)) {
				return restorePlan{}, fmt.Errorf("%w: relation type %s", ErrRestoreUnavailable, kind)
			}
			if targetKind != model.LinkTargetExternal && mode == planLegacy {
				return restorePlan{}, fmt.Errorf("%w: relation type %s requires the versioned import contract", ErrRestoreUnavailable, kind)
			}
		}
		link, err := importedVersionedSourceLink(source, kind, targetKind, manifest.EdgeVersion())
		if err != nil {
			return restorePlan{}, fmt.Errorf("%w: relation %s: %v", ErrRestoreUnavailable, source.ID, err)
		}
		doc.Links = append(doc.Links, link)
		docs[from] = doc
	}

	plan := restorePlan{migration: migration}
	if profile != nil {
		for _, project := range manifest.Projects {
			allocator, err := importedAllocator(archive, project)
			if err != nil {
				return restorePlan{}, err
			}
			plan.allocators = append(plan.allocators, allocator)
		}
	}
	seen := map[string]bool{}
	add := func(ref model.Ref, kind string, body any) error {
		if err := ref.Validate(); err != nil || !ref.IsID() || ref.ID == "" || !ref.Branch.IsZero() {
			return fmt.Errorf("invalid restore record identity %q", ref.ID)
		}
		key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
		if err != nil {
			return err
		}
		if seen[key] {
			return fmt.Errorf("duplicate restore record identity %q", ref.ID)
		}
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		seen[key] = true
		plan.records = append(plan.records, restoreRecord{ref: ref, kind: kind, body: encoded})
		return nil
	}

	for _, source := range beads {
		doc := docs[source.ID]
		if err := add(model.RefByID(source.ID, model.Natural64(0)), "bead", doc); err != nil {
			return restorePlan{}, err
		}
	}
	for _, kind := range []string{"edge", "comment", "memory", "config", "beads-archive"} {
		for _, source := range archive.Records(kind) {
			if source.Source.Table == "leases" && nativeTables && mode == planImport {
				keep, err := nativeLeaseUnexpired(source.Source.Fields, restoreTime)
				if err != nil {
					return restorePlan{}, err
				}
				if !keep {
					continue
				}
			}
			if profile != nil && entities == nil && kind == "memory" {
				// Preserve main's legacy public-memory contract. Source-plane plans
				// retain their separately bound mirror identity and raw envelope.
				if value, ok := source.Body["value"].(string); source.Body["classification"] == "public" && ok {
					key := strings.TrimSpace(stringValue(source.Body["key"]))
					if key == "" {
						return restorePlan{}, fmt.Errorf("imported memory %s has an empty public key", source.ID)
					}
					ref := model.RefByID(sourcePlaneMirrorID(source, entities != nil), model.Natural64(0))
					body := pipeline.MemoryDocument{ID: ref.ID, Key: key, Content: value, Metadata: map[string]any{
						"plug.restore.source_id": source.ID, "plug.restore.source_table": source.Source.Table,
						"plug.restore.source": cloneRow(source.Source.Fields), "plug.restore.archive_sha256": archive.SHA256(),
					}}
					if entities != nil {
						body.Metadata["plug.restore.source_project"] = source.Project
					}
					if err := add(ref, kind, body); err != nil {
						return restorePlan{}, err
					}
				} else {
					body := cloneRow(source.Body)
					body["source_id"], body["source_table"], body["source"] = source.ID, source.Source.Table, cloneRow(source.Source.Fields)
					if err := add(model.RefByID("source/memory/"+source.ID, model.Natural64(0)), kind, body); err != nil {
						return restorePlan{}, err
					}
				}
				continue
			}
			body, err := sourceMirrorBody(source, entities != nil)
			if err != nil {
				return restorePlan{}, err
			}
			if err := add(model.RefByID(sourcePlaneMirrorID(source, entities != nil), model.Natural64(0)), kind, body); err != nil {
				return restorePlan{}, err
			}
		}
	}
	if nativeTables && mode == planImport {
		if profile.SourcePlanes == nil || profile.HistoryMode != driver.RestoreHistoryModeFull {
			return restorePlan{}, fmt.Errorf("%w: native table publication requires bound full source-plane restore", driver.ErrUnsupported)
		}
		current, families, err := nativeCurrentRecords(archive, restoreTime)
		if err != nil {
			return restorePlan{}, err
		}
		for _, row := range current {
			if err := add(row.Ref, row.Kind, json.RawMessage(row.Body)); err != nil {
				return restorePlan{}, err
			}
		}
		plan.nativeFamilies = families
		accounting := nativeFamilyEvidence{Contract: manifest.SourceFamilyContract, RestoreTime: restoreTime.UTC().Format(time.RFC3339Nano), Families: families}
		if err := add(model.RefByID(NativeFamilyReportID, model.Natural64(0)), "instance_config", accounting); err != nil {
			return restorePlan{}, err
		}
	}

	if source, native := archive.NativeStatusOntology(); native {
		plan.nativeOntology = &source
		if err := add(model.RefByID(ImportedOntologySourceID, model.Natural64(0)), "ontology", source); err != nil {
			return restorePlan{}, err
		}
	}
	if err := rebuildDerived(&plan, docs); err != nil {
		return restorePlan{}, err
	}
	if err := retainArchive(&plan, archive, add); err != nil {
		return restorePlan{}, err
	}
	if profile != nil {
		requestHash, err := profile.RequestHash()
		if err != nil {
			return restorePlan{}, err
		}
		binding := importBindingEvidence{Profile: *profile, RequestHash: requestHash}
		for i, project := range manifest.Projects {
			sourceRaw, err := json.Marshal(project)
			if err != nil {
				return restorePlan{}, err
			}
			targetAllocator := plan.allocators[i].Clone()
			var identity *dumpformat.SourceIdentity
			for _, origin := range archive.SourceIdentities() {
				if origin.Project == project.Prefix {
					copy := origin
					identity = &copy
				}
			}
			binding.Projects = append(binding.Projects, importProjectBinding{
				SourceIdentity:  identity,
				SourceAllocator: project,
				TargetAllocator: targetAllocator,
			})
			metadata := importProjectMetadata{
				SourceIdentity: identity,
				Project:        project.Prefix,
				ImportProfile:  *profile,
				Metadata: map[string]json.RawMessage{
					"plug.restore.sourceAllocator": sourceRaw,
				},
				TargetAllocator: targetAllocator,
			}
			if err := add(model.RefByID(importProjectRecordID(project.Prefix), model.Natural64(0)), "instance_config", metadata); err != nil {
				return restorePlan{}, err
			}
		}
		if err := add(model.RefByID(importBindingRecordID, model.Natural64(0)), "instance_config", binding); err != nil {
			return restorePlan{}, err
		}
	}

	// Every bead write needs its exact predicate bump. The graph sentinel is
	// always included so the restore transaction is serialized with blocking
	// relation maintenance, even when this particular archive has no block edge.
	plan.sentinels = append(plan.sentinels, driver.SentinelGraphBlocking)
	for _, source := range beads {
		sentinel, err := driver.NewBeadSentinel(source.ID)
		if err != nil {
			return restorePlan{}, err
		}
		plan.sentinels = append(plan.sentinels, sentinel)
	}
	sort.Slice(plan.records, func(i, j int) bool {
		return plan.records[i].ref.ID < plan.records[j].ref.ID
	})
	sort.Slice(plan.sentinels, func(i, j int) bool { return string(plan.sentinels[i]) < string(plan.sentinels[j]) })
	return plan, nil
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

func targetStatus(source string) (string, bool) {
	switch source {
	case "open":
		return "todo", true
	case "in_progress", "hooked":
		return "in_progress", true
	case "blocked", "deferred", "pinned", "frozen":
		return "backlog", true
	case "closed":
		return "done", true
	default:
		return "", false
	}
}

func validSourceID(id string, project dumpformat.Project, imported bool) bool {
	if imported {
		return dumpformat.ValidSourceID(id, project)
	}
	prefix := project.Prefix + "-"
	if !strings.HasPrefix(id, prefix) || strings.ContainsRune(id, '\x00') {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(id, prefix), ".")
	if len(parts) == 0 || parts[0] == "" || len(parts[0]) < project.HashLength {
		return false
	}
	if len(parts) != 1 {
		return false
	}
	identity, err := hex.DecodeString(parts[0])
	return err == nil && len(identity) == 16 && hex.EncodeToString(identity) == parts[0]
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func cloneRow(input map[string]any) map[string]any {
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = cloneValue(value)
	}
	return output
}

func cloneValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		return cloneRow(value)
	case []any:
		output := make([]any, len(value))
		for i, item := range value {
			output[i] = cloneValue(item)
		}
		return output
	default:
		return value
	}
}

// sourceCreationTime accepts only the admitted row's own creation field.
// It has no clock or history input and never substitutes the canonical mirror.
func sourceCreationTime(source dumpformat.Record) (*time.Time, error) {
	raw := source.Source.Fields["created_at"]
	if raw == nil {
		return nil, nil
	}
	stamp, ok := raw.(string)
	createdAt, err := time.Parse(time.RFC3339Nano, stamp)
	if !ok || err != nil || createdAt.IsZero() {
		return nil, fmt.Errorf("bead %s has invalid source creation instant", source.ID)
	}
	createdAt = createdAt.UTC()
	return &createdAt, nil
}

// importedSourceLink preserves the typed endpoint before any local lookup.
func importedSourceLink(body map[string]any, kind string, targetKind model.LinkTargetKind) (pipeline.Link, error) {
	return importedVersionedSourceLink(dumpformat.Record{Body: body}, kind, targetKind, 1)
}

func importedVersionedSourceLink(source dumpformat.Record, kind string, targetKind model.LinkTargetKind, edgeVersion int) (pipeline.Link, error) {
	link := pipeline.Link{To: model.RefByID(stringValue(source.Body["to"]), model.Natural64(0)), Kind: kind, TargetKind: targetKind}
	if err := model.ValidateLinkTarget(targetKind, link.To); err != nil {
		return pipeline.Link{}, err
	}
	metadata, err := dumpformat.SourceLinkMetadata(edgeVersion, source, kind)
	if err != nil {
		return pipeline.Link{}, err
	}
	link.Metadata = metadata
	return link, nil
}
