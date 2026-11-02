package pipeline

// This file owns the replica-local lease overlay used by bead.heartbeat.
// Claims remain ordinary graph writes for compatibility with the existing
// claim response and reclaim transaction. Heartbeats only move this overlay;
// checkpoint generations are storage metadata and never become graph
// revisions, versions, changesets, or events.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type leaseOverlay struct {
	Principal     model.PrincipalRef `json:"principal"`
	Agent         string             `json:"agent,omitempty"`
	ExpiresAt     time.Time          `json:"expiresAt"`
	TtlMs         model.Natural      `json:"ttlMs"`
	Epoch         model.Natural      `json:"epoch"`
	LastHeartbeat time.Time          `json:"lastHeartbeat"`
}

type heartbeatReceipt struct {
	ID             string    `json:"id"`
	RequestHash    string    `json:"requestHash"`
	LeaseExpiresAt time.Time `json:"leaseExpiresAt"`
	// State makes receipt publication a durable two-phase operation. A
	// reserved receipt owns the idempotency key while its overlay is being
	// published; only the published state may be returned as a replay.
	// An empty state is the pre-reservation format and remains final for
	// receipts written by an earlier host.
	State string `json:"state,omitempty"`
	// The exact overlay and durable claim boundary are reserved with the
	// response, so a retry after a crash cannot pick a new heartbeat time or
	// apply an old reservation to a later claim.
	Overlay        *leaseOverlay `json:"overlay,omitempty"`
	ClaimExpiresAt time.Time     `json:"claimExpiresAt,omitempty"`
}

const (
	heartbeatReceiptReserved  = "reserved"
	heartbeatReceiptPublished = "published"
)

func leaseCheckpointKey(id string) string {
	digest := sha256.Sum256([]byte("plug/lease/" + id))
	return "plug/lease/" + hex.EncodeToString(digest[:])
}

func heartbeatReceiptKey(principal, agent, method, idempotencyKey string) string {
	// The target is deliberately absent from the lookup scope. It belongs in
	// the request hash below, so reusing a key on another target is refused
	// rather than selecting another receipt namespace.
	digest := sha256.Sum256([]byte(principal + "\x00" + agent + "\x00" + method + "\x00" + idempotencyKey))
	return "plug/heartbeat/" + hex.EncodeToString(digest[:])
}

func heartbeatRequestHash(id, principal, agent string, meta model.WriteMeta) string {
	// Target and all request parameters remain in the digest even though the
	// idempotency lookup scope is principal/agent/method/key.
	data, _ := json.Marshal(struct {
		ID, Principal, Agent, Method, Project, Return string
		DryRun                                        bool
	}{id, principal, agent, "bead.heartbeat", meta.Project, meta.Return, meta.DryRun})
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func cloneLease(value *ClaimLease) *ClaimLease {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func decodeLeaseOverlay(payload []byte) (leaseOverlay, bool, error) {
	if len(payload) == 0 {
		return leaseOverlay{}, false, nil
	}
	var value leaseOverlay
	if err := json.Unmarshal(payload, &value); err != nil {
		return leaseOverlay{}, false, fmt.Errorf("invalid lease overlay: %w", err)
	}
	if value.ExpiresAt.IsZero() || value.LastHeartbeat.IsZero() || value.TtlMs == "" || value.Epoch == "" {
		return leaseOverlay{}, false, errors.New("invalid lease overlay fields")
	}
	return value, true, nil
}

func loadLeaseOverlay(ctx context.Context, store driver.CheckpointStore, id string) (leaseOverlay, uint64, bool, error) {
	checkpoint, err := store.LoadCheckpoint(ctx, leaseCheckpointKey(id))
	if err != nil {
		return leaseOverlay{}, 0, false, err
	}
	value, present, err := decodeLeaseOverlay(checkpoint.Payload)
	return value, checkpoint.Generation, present, err
}

// EffectiveLease returns a detached claim lease with a matching quiet
// heartbeat overlay applied. A stale overlay is ignored rather than allowed
// to resurrect a later claim: the principal, anonymous agent label, and
// storage epoch must all match the durable claim image.
func EffectiveLease(ctx context.Context, store driver.CheckpointStore, id string, base *ClaimLease) (*ClaimLease, error) {
	if base == nil || store == nil {
		return cloneLease(base), nil
	}
	overlay, _, present, err := loadLeaseOverlay(ctx, store, id)
	if err != nil {
		if errors.Is(err, driver.ErrUnsupported) {
			return cloneLease(base), nil
		}
		return nil, err
	}
	return effectiveLease(base, overlay, present), nil
}

func effectiveLease(base *ClaimLease, overlay leaseOverlay, present bool) *ClaimLease {
	// A reclaimed bead can be claimed again under the same principal, agent,
	// and storage epoch.  Its new durable expiry is the claim boundary; an
	// older checkpoint must never resurrect the prior lease image.
	if !present || !sameLeaseIdentity(base, overlay) || !overlay.ExpiresAt.After(base.ExpiresAt) {
		return cloneLease(base)
	}
	value := cloneLease(base)
	value.ExpiresAt = overlay.ExpiresAt
	value.TtlMs = overlay.TtlMs
	value.Epoch = overlay.Epoch
	return value
}

func sameLeaseIdentity(base *ClaimLease, overlay leaseOverlay) bool {
	basePrincipal, baseErr := driver.PrincipalKey(base.Principal)
	overlayPrincipal, overlayErr := driver.PrincipalKey(overlay.Principal)
	return baseErr == nil && overlayErr == nil && basePrincipal == overlayPrincipal &&
		base.Agent == overlay.Agent && base.Epoch.String() == overlay.Epoch.String()
}

func saveLeaseOverlay(ctx context.Context, store driver.RecordGuardedCheckpointStore, lease driver.ActiveLease, ref model.Ref, revision model.Revision, incarnation string, expected uint64, value leaseOverlay) (uint64, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return 0, err
	}
	saved, err := store.SaveCheckpointIfRecord(ctx, lease, leaseCheckpointKey(ref.ID), expected, payload, ref, revision, incarnation)
	if err != nil {
		return 0, err
	}
	return saved.Generation, nil
}

func loadHeartbeatReceipt(ctx context.Context, store driver.CheckpointStore, key string) (heartbeatReceipt, uint64, bool, error) {
	checkpoint, err := store.LoadCheckpoint(ctx, key)
	if err != nil {
		return heartbeatReceipt{}, 0, false, err
	}
	if len(checkpoint.Payload) == 0 {
		return heartbeatReceipt{}, checkpoint.Generation, false, nil
	}
	var value heartbeatReceipt
	if err := json.Unmarshal(checkpoint.Payload, &value); err != nil {
		return heartbeatReceipt{}, 0, false, fmt.Errorf("invalid heartbeat receipt: %w", err)
	}
	if value.ID == "" || value.RequestHash == "" || value.LeaseExpiresAt.IsZero() {
		return heartbeatReceipt{}, 0, false, errors.New("invalid heartbeat receipt fields")
	}
	return value, checkpoint.Generation, true, nil
}

func saveHeartbeatReceipt(ctx context.Context, store driver.CheckpointStore, lease driver.ActiveLease, key string, expected uint64, value heartbeatReceipt) (uint64, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return 0, err
	}
	saved, err := store.SaveCheckpoint(ctx, lease, key, expected, payload)
	if err != nil {
		return 0, err
	}
	return saved.Generation, nil
}

func leaseDuration(ttl model.Natural, fallback time.Duration) (time.Duration, error) {
	ms, err := ttl.Int64()
	if err != nil || ms < 1 || ms > int64((1<<63-1)/time.Millisecond) {
		if fallback >= time.Millisecond {
			return fallback, nil
		}
		return 0, failure("lease_not_held", "claim has an invalid lease TTL")
	}
	return time.Duration(ms) * time.Millisecond, nil
}

// HeartbeatAuthorizationAvailable exposes only the capability needed by the
// service seam to fail closed when the host reports an installed authorizer but
// this writer was composed without one. A nil authorizer remains the explicit
// no-plugin mode and is allowed by the adopted contract.
func (w *Writer) HeartbeatAuthorizationAvailable() bool {
	return w != nil && w.cfg.authorizer != nil
}

func heartbeatProposal(by model.PrincipalRef, ref model.Ref, meta model.WriteMeta) port.ProposedChange {
	meta.Method = "bead.heartbeat"
	return port.ProposedChange{
		Principal: by,
		Meta:      meta,
		Changes: []port.Change{{
			Resource: model.ResourceRef{Kind: "bead", Ref: ref},
			Op:       model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Kind: model.OpPatch, Target: ref, Fields: map[string]any{}}},
		}},
	}
}

func heartbeatReceiptMatches(receipt heartbeatReceipt, id, hash string) bool {
	return receipt.ID == id && receipt.RequestHash == hash
}

func heartbeatReceiptIsPublished(receipt heartbeatReceipt) bool {
	// State was added after the first recorder head. An absent state is the
	// old final format, never an in-flight reservation.
	return receipt.State == "" || receipt.State == heartbeatReceiptPublished
}

// Heartbeat is the protected writer seam for bead.heartbeat. It validates the
// current durable claim, applies a replica-local overlay with checkpoint CAS,
// and stores a keyed response for exact retries. No Writer.Apply path is
// entered, so no graph head, revision, version, event, or ordinary receipt is
// changed.
func (w *Writer) Heartbeat(ctx context.Context, by model.PrincipalRef, ref model.Ref, meta model.WriteMeta) (port.HeartbeatOutcome, error) {
	if w == nil || w.store == nil || w.lease == nil || w.cfg.now == nil {
		return port.HeartbeatOutcome{}, failure("internal_error", "writer is not configured")
	}
	if err := ref.Validate(); err != nil || !ref.IsID() || ref.ID == "" || !ref.Branch.IsZero() {
		return port.HeartbeatOutcome{}, failure("invalid_params", "heartbeat requires a nonempty bead ID on the main branch")
	}
	if !validKey(meta.IdempotencyKey) {
		return port.HeartbeatOutcome{}, failure("invalid_params", "idempotency key must encode at least 128 bits as UUIDv4, ULID, hex or base64url")
	}
	select {
	case <-w.lease.Lost():
		return port.HeartbeatOutcome{}, failure("lease_not_held", "active storage lease has been lost")
	default:
	}
	principal, err := driver.PrincipalKey(by)
	if err != nil {
		return port.HeartbeatOutcome{}, failure("invalid_params", "invalid principal")
	}
	agent := meta.AgentLabel
	if by.Kind != "anonymous" {
		agent = ""
	} else if agent == "" {
		agent = meta.Agent
	}
	meta.Method = "bead.heartbeat"
	hash := heartbeatRequestHash(ref.ID, principal, agent, meta)
	receiptKey := heartbeatReceiptKey(principal, agent, meta.Method, meta.IdempotencyKey)

	// Authorization is the first target-sensitive operation. The tracked
	// snapshot is handed to the installed chain before the writer reads the
	// target or learns whether its lease exists. With no authorizer this is a
	// deliberate no-plugin allow.
	reader, err := w.reader(ctx)
	if err != nil {
		return port.HeartbeatOutcome{}, err
	}
	defer reader.Close()
	snapshot := &snapshotReader{reader: reader, reads: &readSet{states: map[string]driver.LockState{}}}
	proposal := heartbeatProposal(by, ref, meta)
	proposal.BaseRevision = *snapshot.Snapshot().At
	if err := w.authorize(ctx, port.AuthorizeBefore, proposal, snapshot); err != nil {
		return port.HeartbeatOutcome{}, err
	}

	checkpoints, ok := w.store.(driver.CheckpointStore)
	if !ok {
		return port.HeartbeatOutcome{}, failure("internal_error", "heartbeat requires durable checkpoint metadata")
	}
	guarded, ok := w.store.(driver.RecordGuardedCheckpointStore)
	if !ok {
		return port.HeartbeatOutcome{}, failure("internal_error", "heartbeat requires record-guarded checkpoint metadata")
	}

	// Look up the target-free receipt before existence or lease details. A
	// changed target therefore gets idempotency_key_reused before inspecting
	// that target's existence or lease, after the current policy has allowed it.
	receipt, _, receiptPresent, err := loadHeartbeatReceipt(ctx, checkpoints, receiptKey)
	if err != nil {
		return port.HeartbeatOutcome{}, err
	}
	replay := false
	if receiptPresent {
		if err := w.authorize(ctx, port.AuthorizeReplay, proposal, snapshot); err != nil {
			return port.HeartbeatOutcome{}, err
		}
		if !heartbeatReceiptMatches(receipt, ref.ID, hash) {
			return port.HeartbeatOutcome{}, failure("idempotency_key_reused", "idempotency key was used with different parameters")
		}
		if heartbeatReceiptIsPublished(receipt) {
			return port.HeartbeatOutcome{LeaseExpiresAt: receipt.LeaseExpiresAt, Replayed: true}, nil
		}
		if receipt.State != heartbeatReceiptReserved || receipt.LeaseExpiresAt.IsZero() {
			return port.HeartbeatOutcome{}, failure("internal_error", "invalid heartbeat receipt state")
		}
		// A reserved receipt is completed below by whichever concurrent caller
		// reaches the target and overlay first. Its stored expiry is immutable.
		replay = true
	}

	rows, err := snapshot.Get(ctx, []model.Ref{ref})
	if err != nil {
		return port.HeartbeatOutcome{}, err
	}
	if len(rows) != 1 || rows[0].Kind != "bead" {
		return port.HeartbeatOutcome{}, failure("not_found", "bead was not found")
	}
	doc, err := DecodeDocument(rows[0].Body)
	if err != nil || doc.ID != ref.ID {
		return port.HeartbeatOutcome{}, failure("internal_error", "invalid persisted bead")
	}
	base := doc.Lease
	if base == nil {
		return port.HeartbeatOutcome{}, failure("lease_not_held", "bead has no active claim")
	}
	effective, err := EffectiveLease(ctx, checkpoints, ref.ID, base)
	if err != nil {
		return port.HeartbeatOutcome{}, err
	}
	holder, err := driver.PrincipalKey(effective.Principal)
	if err != nil || holder != principal || by.Kind == "anonymous" && effective.Agent != agent {
		return port.HeartbeatOutcome{}, failure("lease_not_held", "bead is claimed by another principal")
	}
	now := w.cfg.now().UTC()
	if !now.Before(effective.ExpiresAt) {
		return port.HeartbeatOutcome{}, failure("lease_expired", "bead claim has expired")
	}
	ttl, err := leaseDuration(effective.TtlMs, w.cfg.claimTTL)
	if err != nil {
		return port.HeartbeatOutcome{}, err
	}

	// Compute one response expiry without writing. If another caller already
	// reserved this key, its stored expiry is authoritative and this call must
	// not arm a competing overlay.
	overlay, overlayGeneration, overlayPresent, err := loadLeaseOverlay(ctx, checkpoints, ref.ID)
	if err != nil {
		return port.HeartbeatOutcome{}, err
	}
	resultExpiry := effective.ExpiresAt
	if receiptPresent {
		resultExpiry = receipt.LeaseExpiresAt
	} else if !overlayPresent || !sameLeaseIdentity(base, overlay) || now.After(overlay.LastHeartbeat.Add(time.Second)) {
		resultExpiry = now.Add(ttl)
	}
	if resultExpiry.IsZero() {
		return port.HeartbeatOutcome{}, failure("internal_error", "heartbeat response has no expiry")
	}

	if !receiptPresent {
		candidate := heartbeatReceipt{
			ID: ref.ID, RequestHash: hash, LeaseExpiresAt: resultExpiry, State: heartbeatReceiptReserved,
			ClaimExpiresAt: base.ExpiresAt,
			Overlay:        &leaseOverlay{Principal: effective.Principal, Agent: effective.Agent, ExpiresAt: resultExpiry, TtlMs: effective.TtlMs, Epoch: effective.Epoch, LastHeartbeat: now},
		}
		for attempt := 0; attempt < 8; attempt++ {
			current, generation, present, loadErr := loadHeartbeatReceipt(ctx, checkpoints, receiptKey)
			if loadErr != nil {
				return port.HeartbeatOutcome{}, loadErr
			}
			if present {
				if err := w.authorize(ctx, port.AuthorizeReplay, proposal, snapshot); err != nil {
					return port.HeartbeatOutcome{}, err
				}
				if !heartbeatReceiptMatches(current, ref.ID, hash) {
					return port.HeartbeatOutcome{}, failure("idempotency_key_reused", "idempotency key was used with different parameters")
				}
				if heartbeatReceiptIsPublished(current) {
					return port.HeartbeatOutcome{LeaseExpiresAt: current.LeaseExpiresAt, Replayed: true}, nil
				}
				if current.State != heartbeatReceiptReserved || current.LeaseExpiresAt.IsZero() {
					return port.HeartbeatOutcome{}, failure("internal_error", "invalid heartbeat receipt state")
				}
				receipt, receiptPresent, replay = current, true, true
				resultExpiry = current.LeaseExpiresAt
				break
			}
			_, saveErr := saveHeartbeatReceipt(ctx, checkpoints, w.lease, receiptKey, generation, candidate)
			if saveErr == nil {
				receipt, receiptPresent = candidate, true
				break
			}
			if !errors.Is(saveErr, driver.ErrCheckpointConflict) {
				return port.HeartbeatOutcome{}, saveErr
			}
		}
		if !receiptPresent {
			return port.HeartbeatOutcome{}, failure("busy", "heartbeat response changed concurrently")
		}
	}

	if receipt.Overlay == nil || !receipt.ClaimExpiresAt.Equal(base.ExpiresAt) ||
		!sameLeaseIdentity(base, *receipt.Overlay) || !receipt.Overlay.ExpiresAt.Equal(receipt.LeaseExpiresAt) {
		return port.HeartbeatOutcome{}, failure("lease_not_held", "heartbeat reservation belongs to a different claim")
	}

	// Publish the overlay only for the reserved expiry. A newer overlay from a
	// different idempotency key is already at least as fresh and must win; an
	// older one is advanced with CAS. Same-key duplicates cannot rearm it.
	overlayReady := overlayPresent && sameLeaseIdentity(base, overlay) && !overlay.ExpiresAt.Before(resultExpiry)
	if !overlayReady {
		armed := *receipt.Overlay
		for attempt := 0; attempt < 8; attempt++ {
			next, saveErr := saveLeaseOverlay(ctx, guarded, w.lease, ref, rows[0].Revision, snapshot.Snapshot().Incarnation, overlayGeneration, armed)
			if saveErr == nil {
				overlayGeneration = next
				overlayReady = true
				break
			}
			if errors.Is(saveErr, driver.ErrConflict) {
				return port.HeartbeatOutcome{}, failure("lease_not_held", "claim changed before heartbeat publication")
			}
			if !errors.Is(saveErr, driver.ErrCheckpointConflict) {
				return port.HeartbeatOutcome{}, saveErr
			}
			overlay, overlayGeneration, overlayPresent, err = loadLeaseOverlay(ctx, checkpoints, ref.ID)
			if err != nil {
				return port.HeartbeatOutcome{}, err
			}
			if overlayPresent && sameLeaseIdentity(base, overlay) && !overlay.ExpiresAt.Before(resultExpiry) {
				overlayReady = true
				break
			}
		}
	}
	if !overlayReady {
		return port.HeartbeatOutcome{}, failure("busy", "heartbeat lease overlay changed concurrently")
	}

	// Turn the reservation into the durable response. Every retry reloads the
	// actual stored receipt; a loser never returns its local expiry.
	for attempt := 0; attempt < 8; attempt++ {
		current, generation, present, loadErr := loadHeartbeatReceipt(ctx, checkpoints, receiptKey)
		if loadErr != nil {
			return port.HeartbeatOutcome{}, loadErr
		}
		if !present {
			return port.HeartbeatOutcome{}, failure("busy", "heartbeat response reservation disappeared")
		}
		if !heartbeatReceiptMatches(current, ref.ID, hash) {
			return port.HeartbeatOutcome{}, failure("idempotency_key_reused", "idempotency key was used with different parameters")
		}
		if heartbeatReceiptIsPublished(current) {
			if err := w.authorize(ctx, port.AuthorizeReplay, proposal, snapshot); err != nil {
				return port.HeartbeatOutcome{}, err
			}
			return port.HeartbeatOutcome{LeaseExpiresAt: current.LeaseExpiresAt, Replayed: true}, nil
		}
		if current.State != heartbeatReceiptReserved {
			return port.HeartbeatOutcome{}, failure("internal_error", "invalid heartbeat receipt state")
		}
		published := current
		published.State = heartbeatReceiptPublished
		_, saveErr := saveHeartbeatReceipt(ctx, checkpoints, w.lease, receiptKey, generation, published)
		if saveErr == nil {
			return port.HeartbeatOutcome{LeaseExpiresAt: published.LeaseExpiresAt, Replayed: replay}, nil
		}
		if !errors.Is(saveErr, driver.ErrCheckpointConflict) {
			return port.HeartbeatOutcome{}, saveErr
		}
	}
	return port.HeartbeatOutcome{}, failure("busy", "heartbeat response changed concurrently")
}
