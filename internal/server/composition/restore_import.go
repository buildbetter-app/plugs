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
// used by RestoreImport and return its rebuilt derived digest. The composition
// layer never accepts a caller-supplied derived digest as authority; a wrong
// value is rechecked and refused by RestoreImport before any write.
type RestoreImportPlanner func(context.Context, []byte) (converterroundtrip.Verification, error)

// RestoreImportRequest contains transport metadata for a host-private import.
// Actor, source store and archive digest are deliberately absent: the adapter
// derives those from the live context and validated archive.
type RestoreImportRequest struct {
	IdempotencyKey string
	AgentLabel     string
	Presence       restore.Presence
}

type restoreImportAdapter struct {
	store    driver.Store
	lease    driver.ActiveLease
	authMode string
	presence restore.PresenceVerifier
	planner  RestoreImportPlanner
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
	if _, _, _, _, err := a.binding(ctx, principal, request.AgentLabel); err != nil {
		return converterroundtrip.Report{}, err
	}
	verification, err := a.planner(ctx, append([]byte(nil), data...))
	if err != nil {
		return converterroundtrip.Report{}, err
	}
	if verification.ArchiveSHA256 != archiveDigest || !validImportDigest(verification.DerivedSHA256) {
		return converterroundtrip.Report{}, fmt.Errorf("%w: converter inspection did not return the exact archive and derived digests", converterroundtrip.ErrRestoreUnavailable)
	}
	mode, actor, scope, authority, err := a.binding(ctx, principal, request.AgentLabel)
	if err != nil {
		return converterroundtrip.Report{}, err
	}
	admissionDigest, err := importAdmissionDigest(mode, actor, scope, authority, archive.Manifest().StoreID, archiveDigest, verification.DerivedSHA256, request.IdempotencyKey)
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
		IdempotencyKey: request.IdempotencyKey, ArchiveDigest: archiveDigest, DerivedDigest: verification.DerivedSHA256,
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
	currentDigest, err := importAdmissionDigest(mode, actor, scope, authority, archive.Manifest().StoreID, archiveDigest, verification.DerivedSHA256, request.IdempotencyKey)
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
	recovered, recoveryErr := converterroundtrip.RecoverImport(recoveryCtx, a.store, profile, converterroundtrip.Verification{ArchiveSHA256: archiveDigest, DerivedSHA256: verification.DerivedSHA256})
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

func importAdmissionDigest(mode string, actor model.PrincipalRef, scope, authority, sourceStoreID, archiveDigest, derivedDigest, idempotencyKey string) (string, error) {
	binding := struct {
		Version        string             `json:"version"`
		Mode           string             `json:"mode"`
		Actor          model.PrincipalRef `json:"actor"`
		Scope          string             `json:"scope"`
		Authority      string             `json:"authority"`
		Operation      string             `json:"operation"`
		SourceStoreID  string             `json:"sourceStoreId"`
		ArchiveDigest  string             `json:"archiveDigest"`
		DerivedDigest  string             `json:"derivedDigest"`
		IdempotencyKey string             `json:"idempotencyKey"`
	}{"restore-import/admission/v1", mode, actor, scope, authority, "admin.restore.begin", sourceStoreID, archiveDigest, derivedDigest, idempotencyKey}
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
