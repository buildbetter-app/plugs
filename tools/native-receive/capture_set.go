package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type receiverProcessMemory struct {
	Scope    string  `json:"scope"`
	RSSBytes *uint64 `json:"rss_bytes,omitempty"`
	HWMBytes *uint64 `json:"hwm_bytes,omitempty"`
	Error    string  `json:"error,omitempty"`
}

type captureSetMeasurements struct {
	Decoded      *dumpformat.NativeDeltaDecodedUsage `json:"decoded,omitempty"`
	AfterRelease *dumpformat.NativeDeltaDecodedUsage `json:"after_release,omitempty"`
	Process      receiverProcessMemory               `json:"process_memory"`
}

type captureSetVerification struct {
	MappedCommits     int    `json:"mapped_commits"`
	ImportedReadback  bool   `json:"imported_byte_readback"`
	ReopenedReadback  bool   `json:"reopened_byte_readback"`
	CompleteSnapshots bool   `json:"complete_snapshots"`
	Reopen            string `json:"reopen"`
}

type captureSetRefusal struct {
	Format         string                                   `json:"format"`
	Phase          string                                   `json:"phase"`
	Reason         string                                   `json:"reason"`
	DecodedLimit   *dumpformat.NativeDeltaDecodedLimitError `json:"decoded_limit,omitempty"`
	HistoryRefusal *driver.ErrRestoreHistoryRefused         `json:"history_refusal,omitempty"`
	Measurements   captureSetMeasurements                   `json:"measurements"`
	cause          error
}

func (e *captureSetRefusal) Error() string { return e.Reason }
func (e *captureSetRefusal) Unwrap() error { return e.cause }

func receivingProcessMemory() receiverProcessMemory {
	out := receiverProcessMemory{Scope: "native-receive process including CLI and storage engine; distinct from controlled decoded accounting"}
	raw, err := readBounded("/proc/self/status", 1<<20)
	if err != nil {
		out.Error = err.Error()
		return out
	}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "VmRSS:" && fields[0] != "VmHWM:" {
			continue
		}
		if len(fields) != 3 || fields[2] != "kB" {
			out.Error = "process memory field has unsupported units or shape"
			return out
		}
		n, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil || n > math.MaxUint64/1024 {
			out.Error = "process memory field overflows bytes"
			return out
		}
		n *= 1024
		if fields[0] == "VmRSS:" {
			out.RSSBytes = &n
		} else {
			out.HWMBytes = &n
		}
	}
	if out.RSSBytes == nil || out.HWMBytes == nil {
		out.Error = "process memory fields unavailable"
	}
	return out
}

// The request remains external trusted host intent. It binds the complete
// capture root digest, source identity, admission and every source disposition.
// This route never treats the portable head attachment as the whole input.
func runCaptureSet(ctx context.Context, path, requestPath, engine, target, binaryHash, policyPath, bindingPath, profilePath string) (out receipt, runErr error) {
	phase := "inputs"
	var archive *dumpformat.Archive
	defer func() {
		measurements := captureSetMeasurements{Process: receivingProcessMemory()}
		if archive != nil {
			usage, _ := archive.NativeDecodedUsage()
			measurements.Decoded = &usage
			archive.ReleaseNativeDecoded()
			after, _ := archive.NativeDecodedUsage()
			measurements.AfterRelease = &after
		}
		if runErr != nil {
			var limit *dumpformat.NativeDeltaDecodedLimitError
			if errors.As(runErr, &limit) && measurements.Decoded == nil {
				measurements.Decoded = &limit.Usage
			}
			var history driver.ErrRestoreHistoryRefused
			var historyRefusal *driver.ErrRestoreHistoryRefused
			if errors.As(runErr, &history) {
				historyRefusal = &history
			}
			runErr = &captureSetRefusal{Format: "plug-native-receiving-refusal/1", Phase: phase, Reason: runErr.Error(), DecodedLimit: limit, HistoryRefusal: historyRefusal, Measurements: measurements, cause: runErr}
		}
		out.CaptureSetMeasurements = &measurements
	}()
	if policyPath != "" || bindingPath != "" || profilePath != "" {
		return out, errors.New("capture-set input requires an explicit request without policy, bindings or profile options")
	}
	phase = "authenticate"
	var err error
	archive, err = dumpformat.ReadNativeCaptureSetDirectory(ctx, path, dumpformat.NativeCaptureSetOptions{})
	if err != nil {
		return out, err
	}
	ctx = archive.WithNativeDeltaAccounting(ctx)
	work, releaseWork := archive.NativeDeltaAccountingScope()
	defer releaseWork()
	phase = "request"
	raw, err := readCaptureRequest(work, requestPath)
	if err != nil {
		return out, err
	}
	if err := work.ChargeNativeDeltaJSON("cli_request_decoded", json.RawMessage(raw)); err != nil {
		return out, err
	}
	var input request
	if err := decode(raw, &input); err != nil {
		return out, err
	}
	mode, selection, err := captureSetHistorySelection(input)
	if err != nil {
		return out, err
	}
	if selection != nil {
		metadata, err := work.NativeSourceHistoryMetadata()
		if err != nil {
			return out, err
		}
		if mode == driver.RestoreHistoryModePartial && (len(metadata) != 1 || selection.FirstSequence > uint64(metadata[0].ImageCount)) {
			return out, driver.ErrRestoreHistoryRefused{Mode: mode, History: archive.Manifest().History, Reason: driver.RestoreHistoryReasonNonContiguous}
		}
	}
	manifest, err := work.ManifestChecked()
	if err != nil {
		return out, err
	}
	projects := []string{}
	for _, project := range manifest.Projects {
		projects = append(projects, project.Prefix)
	}
	plan := input.SourcePlan
	if input.Format != "plug-native-receiving-request/1" || input.PolicySHA256 != "" || input.OriginalInputs != nil || input.ArchiveSHA256 != archive.SHA256() || input.ArchiveSHA256 != plan.ArchiveSHA256 || input.SourceStoreID != manifest.StoreID || input.SourceStoreID != plan.SourceStoreID || !reflect.DeepEqual(input.Projects, projects) || plan.Target.StoreID != "" || plan.Target.Incarnation != "" {
		return out, errors.New("explicit capture/source/project or new-target binding differs")
	}
	// Validate the complete external declaration before creating a destination.
	checked := plan
	checked.Target.StoreID, checked.Target.Incarnation = "pending-new-target", "pending-new-target"
	checked.Providers = append([]driver.SourcePlaneProviderV1(nil), plan.Providers...)
	for i := range checked.Providers {
		if checked.Providers[i].LogicalImageSHA256 == "" && checked.Providers[i].LogicalImageSize == 0 {
			checked.Providers[i].LogicalImageSHA256 = strings.Repeat("0", 64)
		}
	}
	if err := checked.Validate(); err != nil {
		return out, err
	}
	phase = "target"
	session, actualTarget, err := newDisposableSession(ctx, engine, target)
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
	if plan.Target.Lifetime != capability.WorkflowCheckpointLifetime() {
		return out, errors.New("declared target lifetime differs from actual driver")
	}
	plan.Target.StoreID, plan.Target.Incarnation = string(meta.StoreID), meta.Incarnation
	profile := driver.RestoreImportProfile{Admission: input.Admission, SourceStoreID: driver.StoreID(input.SourceStoreID), Method: "offline.restore.import", IdempotencyKey: input.IdempotencyKey, ArchiveDigest: input.ArchiveSHA256, HistoryMode: mode, HistoryTime: time.Now().UTC().Format(time.RFC3339Nano), NativeDeltaHistory: selection}
	phase = "bind"
	profile, err = converterroundtrip.BindNativeCaptureSetImport(op, work, profile, plan)
	if err != nil {
		return out, err
	}
	phase = "import"
	committed, err := converterroundtrip.RestoreNativeCaptureSetImportWithLease(op, work, store, lease, profile)
	if err != nil {
		return out, err
	}
	phase = "imported-readback"
	if err := converterroundtrip.VerifyNativeCaptureSetReadback(op, store, work, committed); err != nil {
		return out, err
	}
	if err := release(); err != nil {
		release = nil
		return out, err
	}
	release = nil
	phase = "reopen"
	reopen := "same-process-memory-lease-reacquired"
	if engine != "memory" {
		if err := store.Close(); err != nil {
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
	if recovered.Archive != nil {
		defer recovered.Archive.ReleaseNativeDecoded()
	}
	if err != nil {
		return out, err
	}
	if !found || recovered.Archive == nil || recovered.Archive.SHA256() != archive.SHA256() || !reflect.DeepEqual(committed.Receipt, recovered.Receipt) || !reflect.DeepEqual(committed.NativeSchemas, recovered.NativeSchemas) || !reflect.DeepEqual(committed.NativeFamilies, recovered.NativeFamilies) || !reflect.DeepEqual(committed.NativeHistories, recovered.NativeHistories) || !reflect.DeepEqual(committed.SourcePlanes, recovered.SourcePlanes) {
		return out, errors.New("committed capture receipt/accounting recovery differs")
	}
	phase = "reopened-readback"
	if err := converterroundtrip.VerifyNativeCaptureSetReadback(op, store, work, recovered); err != nil {
		return out, err
	}
	phase = "input-recheck"
	if err := work.VisitNativeCaptureFiles(op, func(string, []byte) error { return nil }); err != nil {
		return out, err
	}
	recheck, releaseRecheck := archive.NativeDeltaAccountingScope()
	defer releaseRecheck()
	again, err := readCaptureRequest(recheck, requestPath)
	if err != nil || !bytes.Equal(raw, again) {
		return out, errors.New("receiving request changed during execution")
	}
	metadata, err := work.NativeSourceHistoryMetadata()
	if err != nil || len(metadata) != 1 {
		return out, errors.New("capture history metadata differs")
	}
	proof := captureSetVerification{MappedCommits: metadata[0].ImageCount, ImportedReadback: true, ReopenedReadback: true, CompleteSnapshots: true, Reopen: reopen}
	if selection != nil {
		proof.MappedCommits = 0
		if mode == driver.RestoreHistoryModePartial {
			proof.MappedCommits = metadata[0].ImageCount - int(selection.FirstSequence) + 1
		}
	}
	phase = "finish"
	if err := release(); err != nil {
		release = nil
		return out, err
	}
	release = nil
	if err := store.Close(); err != nil {
		return out, err
	}
	store = nil
	if err := session.finish(ctx); err != nil {
		return out, err
	}
	return receipt{Format: "plug-native-receiving-receipt/1", BinarySHA256: binaryHash, RequestSHA256: digest(raw), ArchiveSHA256: archive.SHA256(), Driver: engine, HistoryMode: string(mode), RestoreTime: recoveredProfile.HistoryTime, CaptureSetVerification: &proof, Report: recovered, SessionCeilingSeconds: int64(rehearsalSessionLimit / time.Second)}, nil
}

func captureSetHistorySelection(input request) (driver.RestoreHistoryMode, *driver.NativeDeltaHistoryPolicy, error) {
	if input.RestoreMode != "" && input.RestoreMode != "full" && input.RestoreMode != "partial" && input.RestoreMode != "snapshot" {
		return "", nil, driver.ErrRestoreHistoryMode{Value: input.RestoreMode}
	}
	mode, err := driver.ParseRestoreHistoryMode(input.RestoreMode)
	if err != nil {
		return "", nil, err
	}
	if mode != driver.RestoreHistoryModePartial && len(input.RestoreFirstSequence) != 0 {
		return mode, nil, driver.ErrRestoreHistoryRefused{Mode: mode, Reason: driver.RestoreHistoryReasonUnmappable}
	}
	if mode == driver.RestoreHistoryModeFull {
		return mode, nil, nil
	}
	var first uint64
	if mode == driver.RestoreHistoryModePartial && (len(input.RestoreFirstSequence) == 0 || json.Unmarshal(input.RestoreFirstSequence, &first) != nil || first == 0) {
		return mode, nil, driver.ErrRestoreHistoryRefused{Mode: mode, Reason: driver.RestoreHistoryReasonUnmappable}
	}
	policy := &driver.NativeDeltaHistoryPolicy{Format: driver.NativeDeltaHistoryPolicyFormat, FirstSequence: first}
	if err := policy.Validate(mode); err != nil {
		return mode, nil, err
	}
	return mode, policy, nil
}

// Admit the exact bounded request buffer before allocating or decoding it.
// Refuse file replacement, growth or truncation during the read.
func readCaptureRequest(archive *dumpformat.Archive, path string) ([]byte, error) {
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Size() < 0 || before.Size() > driver.SourcePlaneMaxPlanBytes {
		return nil, errors.New("regular bounded receiving request required")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return nil, errors.New("receiving request file changed")
	}
	if err := archive.ChargeNativeDeltaBytes("cli_request_raw", uint64(before.Size())); err != nil {
		return nil, err
	}
	raw := make([]byte, int(before.Size()))
	if _, err := io.ReadFull(f, raw); err != nil {
		return nil, err
	}
	var extra [1]byte
	if n, err := f.Read(extra[:]); n != 0 || err != io.EOF {
		return nil, errors.New("receiving request grew during execution")
	}
	after, err := f.Stat()
	if err != nil || after.Size() != before.Size() || after.ModTime() != before.ModTime() {
		return nil, errors.New("receiving request file changed")
	}
	return raw, nil
}
