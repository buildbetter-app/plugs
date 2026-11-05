package composition

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// RestoreImportPlanner is a trusted converter-owned inspection seam. It must
// parse and validate the supplied archive with the same deterministic planner
// used by RestoreImport and return the exact source/archive/derived binding.
// The composition layer never accepts a caller-supplied digest as authority;
// the consumer rechecks the inspection before any write.
type RestoreImportPlanner func(context.Context, io.Reader) (converterroundtrip.ImportInspection, error)

// RestoreImportRequest contains transport metadata for a host-private import.
// Actor, source store and archive digest are deliberately absent: the adapter
// derives those from the live context and validated archive.
type RestoreImportRequest struct {
	IdempotencyKey string
	AgentLabel     string
	HistoryMode    driver.RestoreHistoryMode
	Presence       restore.Presence
	SourcePlanes   *driver.SourcePlanePlanV1
}

// Import is the existing restore.Manager finish hook. The manager has already
// checked the paged archive, canonical manifest hash and initiating principal;
// this hook performs the trusted converter inspection/admission and returns a
// normal restore status for the same session.
func (a *restoreImportAdapter) Import(ctx context.Context, restoreID string, principal model.PrincipalRef, archive []byte, manifestHash string, historyMode driver.RestoreHistoryMode, meta model.WriteMeta, presence restore.Presence) (restore.RestoreStatus, error) {
	if strings.TrimSpace(manifestHash) == "" {
		return restore.RestoreStatus{}, fmt.Errorf("restore import manifest hash is required")
	}
	request := RestoreImportRequest{IdempotencyKey: meta.IdempotencyKey, AgentLabel: meta.AgentLabel, HistoryMode: historyMode, Presence: presence}
	report, err := a.importArchive(ctx, principal, bytes.NewReader(archive), request)
	if err != nil {
		return restore.RestoreStatus{}, err
	}
	return restore.RestoreStatus{RestoreID: restoreID, State: "complete", Warnings: []model.Warning{}, Incarnation: report.Restored.Incarnation}, nil
}

type restoreImportAdapter struct {
	store         driver.Store
	lease         driver.ActiveLease
	authMode      string
	presence      restore.PresenceVerifier
	planner       RestoreImportPlanner
	sourcePlanner RestoreSourcePlanner
	installSource func(context.Context, driver.RestoreImportProfile, converterroundtrip.Report) error
}

func newRestoreImportAdapter(store driver.Store, lease driver.ActiveLease, authMode string, presence restore.PresenceVerifier, planner RestoreImportPlanner) *restoreImportAdapter {
	return &restoreImportAdapter{store: store, lease: lease, authMode: authMode, presence: presence, planner: planner}
}

// RestoreImport admits and executes an unchanged converter archive through the
// host-private import path. It is not an RPC and is intended for the trusted
// composition/restore runner. Authenticated calls require a live interactive
// user; anonymous calls require loopback composition plus the injected D32
// verifier. Both checks are repeated immediately before publication.
func (r *Runtime) RestoreImport(ctx context.Context, principal model.PrincipalRef, input io.Reader, request RestoreImportRequest) (converterroundtrip.Report, error) {
	if r == nil || r.restoreImport == nil {
		return converterroundtrip.Report{}, errors.New("restore import host adapter is unavailable")
	}
	return r.restoreImport.importArchive(ctx, principal, input, request)
}

func (a *restoreImportAdapter) importArchive(ctx context.Context, principal model.PrincipalRef, input io.Reader, request RestoreImportRequest) (converterroundtrip.Report, error) {
	if a == nil || a.store == nil || a.lease == nil || a.presence == nil || a.planner == nil {
		return converterroundtrip.Report{}, converterroundtrip.ErrRestoreUnavailable
	}
	if ctx == nil {
		return converterroundtrip.Report{}, errors.New("restore import context is required")
	}
	if input == nil {
		return converterroundtrip.Report{}, errors.New("restore import archive is required")
	}
	if strings.TrimSpace(request.IdempotencyKey) != request.IdempotencyKey || request.IdempotencyKey == "" {
		return converterroundtrip.Report{}, errors.New("restore import idempotency key is required")
	}
	historyMode, err := driver.ParseRestoreHistoryMode(string(request.HistoryMode))
	if err != nil {
		return converterroundtrip.Report{}, err
	}
	request.HistoryMode = historyMode
	if request.Presence == nil {
		return converterroundtrip.Report{}, authn.ErrHumanPresenceRequired
	}
	data, err := io.ReadAll(io.LimitReader(input, dumpformat.MaxArchiveBytes+1))
	if err != nil {
		return converterroundtrip.Report{}, err
	}
	if len(data) > dumpformat.MaxArchiveBytes {
		return converterroundtrip.Report{}, fmt.Errorf("restore import archive exceeds %d bytes", dumpformat.MaxArchiveBytes)
	}
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		return converterroundtrip.Report{}, err
	}
	archiveDigest := archive.SHA256()
	if archive.Manifest().SourcePlaneAddressContract != "" {
		return a.importSourceArchive(ctx, principal, data, request, archive)
	}
	if _, _, _, _, err := a.binding(ctx, principal, request.AgentLabel); err != nil {
		return converterroundtrip.Report{}, err
	}
	inspection, err := a.planner(ctx, bytes.NewReader(data))
	if err != nil {
		return converterroundtrip.Report{}, err
	}
	if inspection.SourceStoreID != driver.StoreID(archive.Manifest().StoreID) || inspection.ArchiveSHA256 != archiveDigest || !validImportDigest(inspection.DerivedSHA256) {
		return converterroundtrip.Report{}, fmt.Errorf("%w: converter inspection did not return the exact archive and derived digests", converterroundtrip.ErrRestoreUnavailable)
	}
	mode, actor, scope, authority, err := a.binding(ctx, principal, request.AgentLabel)
	if err != nil {
		return converterroundtrip.Report{}, err
	}
	admissionDigest, err := importAdmissionDigest(mode, actor, scope, authority, request.HistoryMode, archive.Manifest().StoreID, archiveDigest, inspection.DerivedSHA256, request.IdempotencyKey)
	if err != nil {
		return converterroundtrip.Report{}, err
	}
	// D32 binds the operation to the complete non-secret import binding. The
	// derived image digest alone is insufficient: distinct archives can rebuild
	// the same graph while differing in comments, provenance or source files.
	if err := a.admit(ctx, principal, request.AgentLabel, admissionDigest, request.Presence); err != nil {
		return converterroundtrip.Report{}, err
	}
	profile := driver.RestoreImportProfile{
		Admission: driver.RestoreImportAdmission{
			Mode: mode, Actor: actor, Scope: scope, Authority: authority, AdmissionDigest: admissionDigest,
		},
		SourceStoreID: driver.StoreID(archive.Manifest().StoreID), Method: "offline.restore.import",
		IdempotencyKey: request.IdempotencyKey, ArchiveDigest: archiveDigest, DerivedDigest: inspection.DerivedSHA256,
		HistoryMode: request.HistoryMode,
	}
	if _, err := profile.Detached(); err != nil {
		return converterroundtrip.Report{}, err
	}
	// Recheck both live identity and operation-bound presence after planning,
	// immediately before the converter opens its guarded write transaction.
	mode, actor, scope, authority, err = a.binding(ctx, principal, request.AgentLabel)
	if err != nil {
		return converterroundtrip.Report{}, err
	}
	currentDigest, err := importAdmissionDigest(mode, actor, scope, authority, request.HistoryMode, archive.Manifest().StoreID, archiveDigest, inspection.DerivedSHA256, request.IdempotencyKey)
	if err != nil {
		return converterroundtrip.Report{}, err
	}
	if currentDigest != admissionDigest {
		return converterroundtrip.Report{}, fmt.Errorf("%w: restore import admission changed during planning", converterroundtrip.ErrRestoreUnavailable)
	}
	if err := a.admit(ctx, principal, request.AgentLabel, currentDigest, request.Presence); err != nil {
		return converterroundtrip.Report{}, err
	}
	report, err := converterroundtrip.RestoreImportWithLease(ctx, bytes.NewReader(data), a.store, a.lease, profile)
	if err == nil {
		return report, nil
	}
	// A prior attempt may have published durably before the caller observed its
	// result. RecoverImport verifies the full profile, receipt and retained
	// archive marker without replaying records. A populated unrelated target
	// still returns the original target-not-empty error when recovery conflicts.
	recoverable := errors.Is(err, driver.ErrRestoreTargetNotEmpty)
	var uncertain driver.ErrIndeterminate
	recoverable = recoverable || errors.As(err, &uncertain)
	if !recoverable {
		return report, err
	}
	recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	recovered, recoveryErr := converterroundtrip.RecoverImport(recoveryCtx, a.store, profile, converterroundtrip.Verification{ArchiveSHA256: archiveDigest, DerivedSHA256: inspection.DerivedSHA256})
	cancel()
	if recoveryErr != nil {
		return report, err
	}
	recovered.Archive = report.Archive
	return recovered, nil
}

func (a *restoreImportAdapter) binding(ctx context.Context, principal model.PrincipalRef, agent string) (string, model.PrincipalRef, string, string, error) {
	switch a.authMode {
	case "authenticated":
		if principal.Kind != "user" || principal.Sponsor != nil || len(principal.Caveats) != 0 || authn.ValidatePrincipal(principal) != nil {
			return "", model.PrincipalRef{}, "", "", authn.ErrUnauthenticated
		}
		if err := authn.RequireInteractiveUser(ctx, principal); err != nil {
			return "", model.PrincipalRef{}, "", "", err
		}
		return "authenticated", principal, "", "restore-admin", nil
	case "anonymous":
		if principal.Kind != "anonymous" || principal.ID != "anonymous" || principal.Sponsor != nil || len(principal.Caveats) != 0 {
			return "", model.PrincipalRef{}, "", "", authn.ErrHumanPresenceRequired
		}
		if agent != "" && driver.ValidateCheckpointKey(agent) != nil {
			return "", model.PrincipalRef{}, "", "", fmt.Errorf("invalid anonymous restore scope")
		}
		return "anonymous-local", model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, agent, "trusted-human-presence", nil
	default:
		return "", model.PrincipalRef{}, "", "", converterroundtrip.ErrRestoreUnavailable
	}
}

func (a *restoreImportAdapter) admit(ctx context.Context, principal model.PrincipalRef, agent, operationDigest string, presence restore.Presence) error {
	if _, _, _, _, err := a.binding(ctx, principal, agent); err != nil {
		return err
	}
	if err := a.presence.Verify(ctx, "admin.restore.begin", operationDigest, presence); err != nil {
		return &model.Error{Data: model.ErrorData{Code: "human_presence_required", Hint: err.Error()}}
	}
	return nil
}

func importAdmissionDigest(mode string, actor model.PrincipalRef, scope, authority string, historyMode driver.RestoreHistoryMode, sourceStoreID, archiveDigest, derivedDigest, idempotencyKey string) (string, error) {
	binding := struct {
		Version        string                    `json:"version"`
		Mode           string                    `json:"mode"`
		Actor          model.PrincipalRef        `json:"actor"`
		Scope          string                    `json:"scope"`
		Authority      string                    `json:"authority"`
		HistoryMode    driver.RestoreHistoryMode `json:"historyMode"`
		Operation      string                    `json:"operation"`
		SourceStoreID  string                    `json:"sourceStoreId"`
		ArchiveDigest  string                    `json:"archiveDigest"`
		DerivedDigest  string                    `json:"derivedDigest"`
		IdempotencyKey string                    `json:"idempotencyKey"`
	}{"restore-import/admission/v1", mode, actor, scope, authority, historyMode, "admin.restore.begin", sourceStoreID, archiveDigest, derivedDigest, idempotencyKey}
	data, err := json.Marshal(binding)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(append([]byte("plug.restore.import.admission/v1\x00"), data...))
	return hex.EncodeToString(sum[:]), nil
}

func validImportDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && strings.ToLower(value) == value
}
