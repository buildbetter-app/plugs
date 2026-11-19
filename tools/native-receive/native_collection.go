package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/historyverify"
)

type nativeCollectionVerification struct {
	SourceCount                        int    `json:"source_count"`
	MappedCommits                      []int  `json:"mapped_commits"`
	ImportedReadback                   bool   `json:"imported_byte_readback"`
	ReopenedReadback                   bool   `json:"reopened_byte_readback"`
	Reopen                             string `json:"reopen"`
	ReceivingSourcePreparations        []int  `json:"receiving_source_preparations"`
	ImportedReadbackSourcePreparations []int  `json:"imported_readback_source_preparations"`
	LaterReadbackSourcePreparations    []int  `json:"later_readback_source_preparations"`
}

// /1 retains its exact old source schema, including refusal of a files field.
// /2 supplies that explicit ordered file ledger; no TAR can be relabelled.
func (b *joinedBinding) UnmarshalJSON(raw []byte) error {
	var format struct {
		Format string `json:"format"`
	}
	if err := json.Unmarshal(raw, &format); err != nil {
		return err
	}
	if format.Format == "plug-native-joined-inputs/1" {
		type sourceV1 struct {
			RehearsalProject string `json:"rehearsal_project"`
			Database         string `json:"database"`
			SourceStoreID    string `json:"source_store_id"`
			SourceProject    string `json:"source_project"`
			ArchiveSHA256    string `json:"archive_sha256"`
		}
		var old struct {
			Format        string     `json:"format"`
			PolicySHA256  string     `json:"policy_sha256"`
			ProfileSHA256 string     `json:"profile_sha256"`
			Sources       []sourceV1 `json:"sources"`
		}
		if err := decode(raw, &old); err != nil {
			return err
		}
		*b = joinedBinding{Format: old.Format, PolicySHA256: old.PolicySHA256, ProfileSHA256: old.ProfileSHA256}
		for _, s := range old.Sources {
			b.Sources = append(b.Sources, originalInput{RehearsalProject: s.RehearsalProject, Database: s.Database, SourceStoreID: s.SourceStoreID, SourceProject: s.SourceProject, ArchiveSHA256: s.ArchiveSHA256})
		}
		return nil
	}
	if format.Format != dumpformat.NativeJoinedInputsFormatV2 {
		return driver.ErrUnsupported
	}
	type plain joinedBinding
	var value plain
	if err := decode(raw, &value); err != nil {
		return err
	}
	*b = joinedBinding(value)
	return nil
}

func checkedNativeCollectionBinding(owner *dumpformat.NativeDeltaBudget, bindingBytes, profileBytes, policyBytes []byte) (dumpformat.NativeJoinedBinding, joinedBinding, error) {
	var binding joinedBinding
	if err := owner.ChargeJSON("cli_collection_binding_decoded", json.RawMessage(bindingBytes)); err != nil {
		return dumpformat.NativeJoinedBinding{}, binding, err
	}
	if err := decode(bindingBytes, &binding); err != nil {
		return dumpformat.NativeJoinedBinding{}, binding, err
	}
	if binding.Format != dumpformat.NativeJoinedInputsFormatV2 || digest(policyBytes) != rehearsalPolicySHA256 || binding.PolicySHA256 != rehearsalPolicySHA256 || binding.ProfileSHA256 != digest(profileBytes) || len(binding.Sources) != 2 {
		return dumpformat.NativeJoinedBinding{}, binding, driver.ErrUnsupported
	}
	work := owner.Scope()
	defer work.Release()
	for _, phase := range []string{"cli_collection_expected_schema", "cli_collection_expected_schema_encoding"} {
		if err := work.ChargeBytes(phase, uint64(len(profileBytes))); err != nil {
			return dumpformat.NativeJoinedBinding{}, binding, err
		}
	}
	want, err := dumpformat.NativeSchemaForVersion(66, 26)
	if err != nil {
		return dumpformat.NativeJoinedBinding{}, binding, err
	}
	canonical, err := json.Marshal(want)
	if err != nil {
		return dumpformat.NativeJoinedBinding{}, binding, err
	}
	if !bytes.Equal(profileBytes, append(canonical, '\n')) {
		return dumpformat.NativeJoinedBinding{}, binding, policyRefusal{"unsupported_profile_encoding", "schema_profile", "canonical-json-newline", 1}
	}
	result := dumpformat.NativeJoinedBinding{Format: binding.Format, PolicySHA256: binding.PolicySHA256, ProfileSHA256: binding.ProfileSHA256}
	for i, s := range binding.Sources {
		if s.RehearsalProject != []string{"cody-ops", "signals-v2"}[i] || s.Database != []string{"ops", "sv2"}[i] || s.SourceProject != s.Database || s.SourceStoreID == "" || len(s.Files) < 3 {
			return result, binding, driver.ErrUnsupported
		}
		result.Sources = append(result.Sources, dumpformat.NativeJoinedInput{RehearsalProject: s.RehearsalProject, Database: s.Database, SourceStoreID: s.SourceStoreID, SourceProject: s.SourceProject, ArchiveSHA256: s.ArchiveSHA256, Files: s.Files})
	}
	return result, binding, nil
}

func nativeCollectionKey(owner *dumpformat.NativeDeltaBudget, binding dumpformat.NativeJoinedBinding) (string, string, error) {
	work := owner.Scope()
	defer work.Release()
	if err := work.ChargeJSON("cli_collection_key_binding", binding); err != nil {
		return "", "", err
	}
	raw, err := json.Marshal(binding)
	if err != nil {
		return "", "", err
	}
	commitment := struct {
		Format string                         `json:"format"`
		Inputs dumpformat.NativeJoinedBinding `json:"inputs"`
	}{"plug-native-rehearsal-idempotency/2", binding}
	if err := work.ChargeJSON("cli_collection_key_encoding", commitment); err != nil {
		return "", "", err
	}
	preimage, err := json.Marshal(commitment)
	if err != nil {
		return "", "", err
	}
	return digest(preimage), digest(raw), nil
}

func nativeCollectionOpeners(paths archivePaths, counts []int) []historyverify.NativeCaptureSetOpener {
	result := make([]historyverify.NativeCaptureSetOpener, 0, len(paths))
	for i, path := range paths {
		i, path := i, path
		result = append(result, func(ctx context.Context, options dumpformat.NativeCaptureSetOptions) (*dumpformat.Archive, error) {
			a, err := dumpformat.ReadNativeCaptureSetDirectory(ctx, path, options)
			if err == nil && counts != nil {
				counts[i]++
			}
			return a, err
		})
	}
	return result
}

// Source authentication, existing policy construction, provider preparation
// and final Plan binding are one source traversal. Receiving consumes the
// resulting opaque handle directly and cannot substitute its final request.
func prepareNativeCollection(ctx context.Context, paths archivePaths, binding dumpformat.NativeJoinedBinding, external joinedBinding, lifetime driver.CheckpointLifetime, target driver.SourcePlaneTargetV1, when time.Time, counts []int) (prepared *converterroundtrip.PreparedNativeCaptureCollectionImport, input request, profile driver.RestoreImportProfile, err error) {
	if len(paths) != 2 {
		return nil, input, profile, driver.ErrUnsupported
	}
	key, archiveHash, err := nativeCollectionKey(dumpformat.NativeDeltaBudgetFromContext(ctx), binding)
	if err != nil {
		return nil, input, profile, err
	}
	projects := []string{}
	sources := []dumpformat.SourceIdentity{}
	for _, s := range binding.Sources {
		projects = append(projects, s.SourceProject)
		sources = append(sources, dumpformat.SourceIdentity{StoreID: s.SourceStoreID, Project: s.SourceProject})
	}
	policyBytes, err := rehearsalConstructionPolicy(archiveHash, "merge-"+archiveHash, projects, sources, key, lifetime)
	if err != nil {
		return nil, input, profile, err
	}
	var policy constructionPolicy
	if err = decode(policyBytes, &policy); err != nil {
		return nil, input, profile, err
	}
	profile = driver.RestoreImportProfile{Admission: policy.Admission, SourceStoreID: driver.StoreID(policy.SourceStoreID), Method: "offline.restore.import", IdempotencyKey: key, ArchiveDigest: archiveHash, HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: when.UTC().Format(time.RFC3339Nano)}
	issuer, err := historyverify.NewNativeCollection(ctx, binding, historyverify.NativeCollectionAdmission{Profile: profile, Target: target, Provider: policy.Provider}, when)
	if err != nil {
		return nil, input, profile, err
	}
	owned := true
	defer func() {
		if owned {
			err = errors.Join(err, issuer.Close())
		}
	}()
	for _, open := range nativeCollectionOpeners(paths, counts) {
		if err = issuer.Append(ctx, open); err != nil {
			return nil, input, profile, err
		}
	}
	a, err := issuer.FinishCurrent()
	if err != nil {
		return nil, input, profile, err
	}
	input, err = constructArchiveRequest(a.WithNativeDeltaAccounting(ctx), a, policyBytes, lifetime)
	if err != nil {
		return nil, input, profile, err
	}
	input.PolicySHA256, input.OriginalInputs = rehearsalPolicySHA256, &external
	input.CollectionSourcePreparations = append([]int(nil), counts...)
	plan := input.SourcePlan
	plan.Target = target
	owned = false // Bind transfers issuer even on an error.
	prepared, err = converterroundtrip.BindNativeCaptureCollectionImport(ctx, issuer, profile, plan)
	if err != nil {
		return nil, input, profile, err
	}
	profile, err = prepared.Profile()
	if err != nil {
		err = errors.Join(err, prepared.Close())
		return nil, input, profile, err
	}
	return prepared, input, profile, nil
}

// File storage is admitted before allocation. Same-file, size, terminal EOF
// and final metadata checks reject replacement/growth/truncation of intent.
func readNativeCollectionInput(owner *dumpformat.NativeDeltaBudget, path, phase string) ([]byte, error) {
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Size() < 0 || before.Size() > driver.SourcePlaneMaxPlanBytes {
		return nil, driver.ErrUnsupported
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return nil, driver.ErrConflict
	}
	if err = owner.ChargeBytes(phase, uint64(before.Size())); err != nil {
		return nil, err
	}
	raw := make([]byte, int(before.Size()))
	if _, err = io.ReadFull(f, raw); err != nil {
		return nil, err
	}
	var extra [1]byte
	if n, e := f.Read(extra[:]); n != 0 || e != io.EOF {
		return nil, driver.ErrConflict
	}
	after, err := f.Stat()
	if err != nil || after.Size() != before.Size() || after.ModTime() != before.ModTime() {
		return nil, driver.ErrConflict
	}
	return raw, nil
}

func nativeCollectionInputs(owner *dumpformat.NativeDeltaBudget, policyPath, bindingPath, profilePath string) (policy, binding, profile []byte, err error) {
	if policyPath == "" || bindingPath == "" || profilePath == "" {
		return nil, nil, nil, driver.ErrUnsupported
	}
	policy, err = readNativeCollectionInput(owner, policyPath, "cli_collection_policy")
	if err != nil {
		return
	}
	binding, err = readNativeCollectionInput(owner, bindingPath, "cli_collection_binding")
	if err != nil {
		return
	}
	profile, err = readNativeCollectionInput(owner, profilePath, "cli_collection_schema_profile")
	return
}

func prepareNativeCaptureCollectionRequest(ctx context.Context, paths archivePaths, policyPath, bindingPath, profilePath string, lifetime driver.CheckpointLifetime) (input request, err error) {
	ctx, cancel := context.WithTimeout(ctx, rehearsalSessionLimit)
	defer cancel()
	budget := dumpformat.NewNativeDeltaBudget(0)
	ctx = dumpformat.WithNativeDeltaBudget(ctx, budget)
	inputs := budget.Scope()
	keep := false
	defer func() {
		if !keep {
			input = request{}
			inputs.Release()
		}
	}()
	policy, rawBinding, schema, err := nativeCollectionInputs(inputs, policyPath, bindingPath, profilePath)
	if err != nil {
		return request{}, err
	}
	binding, external, err := checkedNativeCollectionBinding(inputs, rawBinding, schema, policy)
	if err != nil {
		return request{}, err
	}
	target := driver.SourcePlaneTargetV1{StoreID: "pending-new-target", Incarnation: "pending-new-target", RequireEmpty: true, Lifetime: lifetime, RuntimeContract: driver.SourcePlaneRuntimeContractV1}
	prepared, input, _, err := prepareNativeCollection(ctx, paths, binding, external, lifetime, target, time.Now().UTC(), []int{0, 0})
	if err != nil {
		return request{}, err
	}
	input.cleanup = func() error {
		input = request{}
		binding = dumpformat.NativeJoinedBinding{}
		external = joinedBinding{}
		policy, rawBinding, schema = nil, nil, nil
		err := prepared.Close()
		inputs.Release()
		return errors.Join(err, nativeCollectionOwnerReleased(budget))
	}
	keep = true
	return input, nil
}

// Preserve a compact commitment across dropping the first session. Recovery
// must reproduce every report channel, rather than only its receipt.
func nativeCollectionReportCommitment(budget *dumpformat.NativeDeltaBudget, report converterroundtrip.Report) (string, error) {
	work := budget.Scope()
	defer work.Release()
	channels := []any{report.Receipt, report.NativeFamilies, report.NativeSchemas, report.NativeHistories, report.SourcePlanes}
	// Only the accounting view unwraps the model's validating marshalers.
	// The actual commitment below still encodes every original report field.
	counted := []any{(*nativeCollectionReceiptJSON)(report.Receipt), report.NativeFamilies, report.NativeSchemas, report.NativeHistories, report.SourcePlanes}
	if err := work.ChargeJSON("cli_collection_report_encoding", counted); err != nil {
		return "", err
	}
	if report.Receipt != nil {
		// Commit/Changeset marshal through their own aggregate buffer while
		// the enclosing report encoder is live. Commit validation and nested
		// OpResult encoding can also hold a descendant candidate. Each fits
		// within the full receipt's wire size; admit both before either runs.
		for _, phase := range []string{"cli_collection_receipt_encoding", "cli_collection_receipt_validation"} {
			if err := work.ChargeJSON(phase, (*nativeCollectionReceiptJSON)(report.Receipt)); err != nil {
				return "", err
			}
		}
	}
	h := sha256.New()
	if err := json.NewEncoder(h).Encode(channels); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// The model marshalers validate and then encode these same wire fields. These
// borrowed views count them without a detached receipt or aggregate encoding.
// Unsupported custom values in open-ended details or event plans still refuse
// through the ordinary counter; there is no shared accounting exception.
type nativeCollectionReceiptJSON driver.IdempotencyRecord

// MarshalJSON is the borrowed public receipt view. AttemptID is private
// storage metadata and must not alter native/public receipt byte identity.
func (r nativeCollectionReceiptJSON) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		RequestHash     string
		Commit          nativeCollectionCommitJSON
		Changeset       nativeCollectionChangesetJSON
		ResponsePending bool
	}{r.RequestHash, nativeCollectionCommitJSON(r.Commit), nativeCollectionChangesetJSON(r.Changeset), r.ResponsePending})
}

type nativeCollectionCommitJSON model.Commit
type nativeCollectionChangesetJSON model.Changeset
type nativeCollectionResultsJSON []model.OpResult
type nativeCollectionWarningsJSON []model.Warning
type nativeCollectionViolationsJSON []model.Violation
type nativeCollectionChangesJSON []model.ChangeRecord

func (r nativeCollectionReceiptJSON) NativeDeltaJSONSize() (uint64, error) {
	return dumpformat.NativeDeltaJSONSize(struct {
		RequestHash     string
		Commit          nativeCollectionCommitJSON
		Changeset       nativeCollectionChangesetJSON
		ResponsePending bool
	}{r.RequestHash, nativeCollectionCommitJSON(r.Commit), nativeCollectionChangesetJSON(r.Changeset), r.ResponsePending})
}

func (c nativeCollectionCommitJSON) NativeDeltaJSONSize() (uint64, error) {
	return dumpformat.NativeDeltaJSONSize(struct {
		Revision       model.Revision                 `json:"revision"`
		Results        nativeCollectionResultsJSON    `json:"results"`
		Warnings       nativeCollectionWarningsJSON   `json:"warnings"`
		WouldCommit    bool                           `json:"wouldCommit,omitempty"`
		Violations     nativeCollectionViolationsJSON `json:"violations,omitempty"`
		LeaseExpiresAt *time.Time                     `json:"leaseExpiresAt,omitempty"`
		LeaseTtlMs     *model.Natural                 `json:"leaseTtlMs,omitempty"`
	}{c.Revision, nativeCollectionResultsJSON(c.Results), nativeCollectionWarningsJSON(c.Warnings), c.WouldCommit, nativeCollectionViolationsJSON(c.Violations), c.LeaseExpiresAt, c.LeaseTtlMs})
}

func (c nativeCollectionChangesetJSON) NativeDeltaJSONSize() (uint64, error) {
	return dumpformat.NativeDeltaJSONSize(struct {
		Revision       model.Revision              `json:"revision"`
		Branch         model.Branch                `json:"branch"`
		Incarnation    string                      `json:"incarnation"`
		Kind           string                      `json:"kind"`
		Time           time.Time                   `json:"time"`
		Principal      model.PrincipalRef          `json:"principal"`
		Chain          []string                    `json:"chain"`
		IdempotencyKey string                      `json:"idempotencyKey,omitempty"`
		Mutators       []string                    `json:"mutators,omitempty"`
		Changes        nativeCollectionChangesJSON `json:"changes"`
		Events         []model.EventDescriptor     `json:"events,omitempty"`
	}{c.Revision, c.Branch, c.Incarnation, string(c.Kind), c.Time, c.Principal, c.Chain, c.IdempotencyKey, c.Mutators, nativeCollectionChangesJSON(c.Changes), c.Events})
}

// Each element size includes an LF. Remove it when adding the element to the
// array, then account for the array's own terminal LF. No converted slice is
// materialized, and checked addition preserves the shared overflow refusal.
func nativeCollectionJSONArraySize[T any](values []T, size func(T) (uint64, error)) (uint64, error) {
	if values == nil {
		return 5, nil // null plus LF
	}
	n := uint64(3) // [] plus LF
	for i, value := range values {
		item, err := size(value)
		if err != nil {
			return 0, err
		}
		if item == 0 {
			return 0, errors.New("empty collection report accounting domain")
		}
		item--
		if i != 0 {
			if item == math.MaxUint64 {
				return 0, errors.New("collection report size integer overflow")
			}
			item++
		}
		if item > math.MaxUint64-n {
			return 0, errors.New("collection report size integer overflow")
		}
		n += item
	}
	return n, nil
}

func (values nativeCollectionResultsJSON) NativeDeltaJSONSize() (uint64, error) {
	return nativeCollectionJSONArraySize(values, func(value model.OpResult) (uint64, error) {
		type wire model.OpResult
		return dumpformat.NativeDeltaJSONSize(wire(value))
	})
}

func (values nativeCollectionWarningsJSON) NativeDeltaJSONSize() (uint64, error) {
	return nativeCollectionJSONArraySize(values, func(value model.Warning) (uint64, error) {
		return dumpformat.NativeDeltaJSONSize(struct {
			Code    string         `json:"code"`
			Message string         `json:"message"`
			Details map[string]any `json:"details,omitempty"`
		}{string(value.Code), value.Message, value.Details})
	})
}

func (values nativeCollectionViolationsJSON) NativeDeltaJSONSize() (uint64, error) {
	return nativeCollectionJSONArraySize(values, func(value model.Violation) (uint64, error) {
		type wire model.Violation
		return dumpformat.NativeDeltaJSONSize(wire(value))
	})
}

func (values nativeCollectionChangesJSON) NativeDeltaJSONSize() (uint64, error) {
	return nativeCollectionJSONArraySize(values, func(value model.ChangeRecord) (uint64, error) {
		return dumpformat.NativeDeltaJSONSize(struct {
			Ref          model.Ref      `json:"ref"`
			Op           string         `json:"op"`
			FromRevision model.Revision `json:"fromRevision"`
			ChangedPaths []string       `json:"changedPaths"`
		}{value.Ref, string(value.Op), value.FromRevision, value.ChangedPaths})
	})
}

func runNativeCaptureCollection(ctx context.Context, paths archivePaths, requestPath, engine, targetPath, binaryHash, policyPath, bindingPath, profilePath string) (out receipt, runErr error) {
	phase := "inputs"
	budget := dumpformat.NewNativeDeltaBudget(0)
	ctx = dumpformat.WithNativeDeltaBudget(ctx, budget)
	inputs := budget.Scope()
	keepResponse := false
	defer func() {
		if !keepResponse {
			inputs.Release()
		}
		if runErr != nil {
			var limit *dumpformat.NativeDeltaDecodedLimitError
			errors.As(runErr, &limit)
			usage := budget.Usage()
			runErr = &captureSetRefusal{Format: "plug-native-receiving-refusal/1", Phase: phase, Reason: runErr.Error(), DecodedLimit: limit, Measurements: captureSetMeasurements{Decoded: &usage, Process: receivingProcessMemory()}, cause: runErr}
		}
	}()
	policy, rawBinding, schema, err := nativeCollectionInputs(inputs, policyPath, bindingPath, profilePath)
	if err != nil {
		return out, err
	}
	rawRequest, err := readNativeCollectionInput(inputs, requestPath, "cli_collection_request")
	if err != nil {
		return out, err
	}
	if err = inputs.ChargeJSON("cli_collection_request_decoded", json.RawMessage(rawRequest)); err != nil {
		return out, err
	}
	var input request
	if err = decode(rawRequest, &input); err != nil {
		return out, err
	}
	binding, external, err := checkedNativeCollectionBinding(inputs, rawBinding, schema, policy)
	if err != nil {
		return out, err
	}
	if input.PolicySHA256 != rehearsalPolicySHA256 || input.OriginalInputs == nil || !reflect.DeepEqual(*input.OriginalInputs, external) || input.SourcePlan.Target.StoreID != "" || input.SourcePlan.Target.Incarnation != "" {
		return out, driver.ErrConflict
	}
	checked := input.SourcePlan
	checked.Target.StoreID, checked.Target.Incarnation = "pending-new-target", "pending-new-target"
	checked.Providers = append([]driver.SourcePlaneProviderV1(nil), checked.Providers...)
	for i := range checked.Providers {
		if checked.Providers[i].LogicalImageSHA256 == "" && checked.Providers[i].LogicalImageSize == 0 {
			checked.Providers[i].LogicalImageSHA256 = fmt.Sprintf("%064d", 0)
		}
	}
	if err = checked.Validate(); err != nil {
		return out, err
	}
	phase = "target"
	session, actualTarget, err := newDisposableSession(ctx, engine, targetPath)
	if err != nil {
		return out, err
	}
	defer func() { runErr = errors.Join(runErr, session.close()) }()
	open, err := opener(ctx, engine, actualTarget)
	if err != nil {
		return out, err
	}
	store, err := open()
	if err != nil {
		return out, err
	}
	var release func() error
	defer func() {
		if release != nil {
			runErr = errors.Join(runErr, release())
		}
		if store != nil {
			runErr = errors.Join(runErr, store.Close())
		}
	}()
	op, lease, release, err := hold(ctx, store)
	if err != nil {
		return out, err
	}
	meta, err := store.Meta(op)
	if err != nil {
		return out, err
	}
	capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
	if err != nil {
		return out, err
	}
	if checked.Target.Lifetime != capability.WorkflowCheckpointLifetime() {
		return out, driver.ErrUnsupported
	}
	target := checked.Target
	target.StoreID, target.Incarnation = string(meta.StoreID), meta.Incarnation
	phase = "authenticate-and-bind"
	preparations := []int{0, 0}
	prepared, constructed, profile, err := prepareNativeCollection(op, paths, binding, external, target.Lifetime, target, time.Now().UTC(), preparations)
	if err != nil {
		return out, err
	}
	defer prepared.Close()
	if !reflect.DeepEqual(constructed, input) {
		return out, errors.New("request differs from exact policy construction")
	}
	phase = "import"
	committed, err := prepared.RestoreWithLease(op, store, lease)
	if err != nil {
		return out, err
	}
	defer committed.Close()
	phase = "imported-readback"
	imported := []int{0, 0}
	if err = converterroundtrip.VerifyNativeCaptureCollectionReadback(op, store, binding, nativeCollectionOpeners(paths, imported), profile, committed); err != nil {
		return out, err
	}
	wantReport, err := nativeCollectionReportCommitment(budget, committed)
	if err != nil {
		return out, err
	}
	constructed = request{}
	profile = driver.RestoreImportProfile{}
	if err = committed.Close(); err != nil {
		return out, err
	}
	if err = release(); err != nil {
		release = nil
		return out, err
	}
	release = nil
	phase = "reopen"
	reopen := "same-process-memory-lease-reacquired"
	if engine != "memory" {
		if err = store.Close(); err != nil {
			return out, err
		}
		store = nil
		store, err = open()
		if err != nil {
			return out, err
		}
		reopen = "driver-reopened"
	}
	op, lease, release, err = hold(ctx, store)
	if err != nil {
		return out, err
	}
	phase = "recover"
	recoveredProfile, recovered, found, err := converterroundtrip.RecoverBoundSourceImport(op, store)
	keepRecovered := false
	defer func() {
		if !keepRecovered {
			runErr = errors.Join(runErr, recovered.Close())
		}
	}()
	if err != nil {
		return out, err
	}
	if !found || recovered.Archive == nil || recovered.Archive.SHA256() != input.ArchiveSHA256 {
		return out, driver.ErrConflict
	}
	gotReport, e := nativeCollectionReportCommitment(budget, recovered)
	if e != nil || gotReport != wantReport {
		return out, driver.ErrConflict
	}
	metadata, err := recovered.Archive.NativeSourceHistoryMetadata()
	if err != nil || len(metadata) != 2 {
		return out, driver.ErrConflict
	}
	phase = "reopened-readback"
	later := []int{0, 0}
	if err = converterroundtrip.VerifyNativeCaptureCollectionReadback(op, store, binding, nativeCollectionOpeners(paths, later), recoveredProfile, recovered); err != nil {
		return out, err
	}
	phase = "intent-recheck"
	for _, pair := range []struct {
		path string
		raw  []byte
	}{{policyPath, policy}, {bindingPath, rawBinding}, {profilePath, schema}, {requestPath, rawRequest}} {
		scope := inputs.Scope()
		again, e := readNativeCollectionInput(scope, pair.path, "cli_collection_intent_recheck")
		same := bytes.Equal(again, pair.raw)
		again = nil
		scope.Release()
		if e != nil || !same {
			return out, driver.ErrConflict
		}
	}
	if err = release(); err != nil {
		release = nil
		return out, err
	}
	release = nil
	if err = store.Close(); err != nil {
		return out, err
	}
	store = nil
	if err = session.finish(ctx); err != nil {
		return out, err
	}
	usage := budget.Usage()
	out = receipt{Format: "plug-native-receiving-receipt/1", PolicySHA256: rehearsalPolicySHA256, OriginalInputs: &external, BinarySHA256: binaryHash, RequestSHA256: digest(rawRequest), ArchiveSHA256: input.ArchiveSHA256, Driver: engine, HistoryMode: "full", RestoreTime: recoveredProfile.HistoryTime, Report: recovered, SessionCeilingSeconds: int64(rehearsalSessionLimit / time.Second), CaptureSetMeasurements: &captureSetMeasurements{Decoded: &usage, Process: receivingProcessMemory()}, CollectionVerification: &nativeCollectionVerification{SourceCount: 2, MappedCommits: []int{metadata[0].ImageCount, metadata[1].ImageCount}, ImportedReadback: true, ReopenedReadback: true, Reopen: reopen, ReceivingSourcePreparations: preparations, ImportedReadbackSourcePreparations: imported, LaterReadbackSourcePreparations: later}}
	out.cleanup = func() error {
		recoveredProfile = driver.RestoreImportProfile{}
		input = request{}
		binding = dumpformat.NativeJoinedBinding{}
		external = joinedBinding{}
		err := recovered.Close()
		policy, rawBinding, schema, rawRequest = nil, nil, nil, nil
		inputs.Release()
		return errors.Join(err, nativeCollectionOwnerReleased(budget))
	}
	keepResponse, keepRecovered = true, true
	return out, nil
}

// main invokes response cleanup after dropping the encoded response views.
// A successful command therefore includes complete byte and charge cleanup.
func nativeCollectionOwnerReleased(budget *dumpformat.NativeDeltaBudget) error {
	usage := budget.Usage()
	if usage.CurrentAccountedDecodedBytes != 0 || len(usage.CurrentCategories) != 0 || len(usage.CurrentCharges) != 0 {
		return fmt.Errorf("native collection response owner still reserved")
	}
	return nil
}
