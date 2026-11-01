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
}

func leaseCheckpointKey(id string) string {
	digest := sha256.Sum256([]byte("plug/lease/" + id))
	return "plug/lease/" + hex.EncodeToString(digest[:])
}

func heartbeatReceiptKey(id, principal, agent, method, idempotencyKey string) string {
	digest := sha256.Sum256([]byte(id + "\x00" + principal + "\x00" + agent + "\x00" + method + "\x00" + idempotencyKey))
	return "plug/heartbeat/" + hex.EncodeToString(digest[:])
}

func heartbeatRequestHash(id, principal, agent string) string {
	digest := sha256.Sum256([]byte(id + "\x00" + principal + "\x00" + agent + "\x00bead.heartbeat"))
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
	if !present || !sameLeaseIdentity(base, overlay) {
		return cloneLease(base), nil
	}
	value := cloneLease(base)
	value.ExpiresAt = overlay.ExpiresAt
	value.TtlMs = overlay.TtlMs
	value.Epoch = overlay.Epoch
	return value, nil
}

func sameLeaseIdentity(base *ClaimLease, overlay leaseOverlay) bool {
	basePrincipal, baseErr := driver.PrincipalKey(base.Principal)
	overlayPrincipal, overlayErr := driver.PrincipalKey(overlay.Principal)
	return baseErr == nil && overlayErr == nil && basePrincipal == overlayPrincipal &&
		base.Agent == overlay.Agent && base.Epoch.String() == overlay.Epoch.String()
}

func saveLeaseOverlay(ctx context.Context, store driver.CheckpointStore, lease driver.ActiveLease, id string, expected uint64, value leaseOverlay) (uint64, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return 0, err
	}
	saved, err := store.SaveCheckpoint(ctx, lease, leaseCheckpointKey(id), expected, payload)
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
	return value, checkpoint.Generation, true, nil
}

func saveHeartbeatReceipt(ctx context.Context, store driver.CheckpointStore, lease driver.ActiveLease, key string, expected uint64, value heartbeatReceipt) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = store.SaveCheckpoint(ctx, lease, key, expected, payload)
	return err
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
	checkpoints, ok := w.store.(driver.CheckpointStore)
	if !ok {
		return port.HeartbeatOutcome{}, failure("internal_error", "heartbeat requires durable checkpoint metadata")
	}
	reader, err := w.reader(ctx)
	if err != nil {
		return port.HeartbeatOutcome{}, err
	}
	rows, err := reader.Get(ctx, []model.Ref{ref})
	_ = reader.Close()
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
	hash := heartbeatRequestHash(ref.ID, principal, agent)
	receiptKey := heartbeatReceiptKey(ref.ID, principal, agent, "bead.heartbeat", meta.IdempotencyKey)
	if receipt, _, present, receiptErr := loadHeartbeatReceipt(ctx, checkpoints, receiptKey); receiptErr != nil {
		return port.HeartbeatOutcome{}, receiptErr
	} else if present {
		if receipt.ID != ref.ID || receipt.RequestHash != hash {
			return port.HeartbeatOutcome{}, failure("idempotency_key_reused", "idempotency key was used with different parameters")
		}
		return port.HeartbeatOutcome{LeaseExpiresAt: receipt.LeaseExpiresAt, Replayed: true}, nil
	}

	// A second heartbeat in the same controlled second is intentionally quiet:
	// it observes the already armed expiry and only records its own replayable
	// response. This preserves the Beads same-second no-rearm boundary.
	overlay, generation, present, err := loadLeaseOverlay(ctx, checkpoints, ref.ID)
	if err != nil {
		return port.HeartbeatOutcome{}, err
	}
	resultExpiry := effective.ExpiresAt
	if !present || !sameLeaseIdentity(base, overlay) || now.After(overlay.LastHeartbeat.Add(time.Second)) {
		overlay = leaseOverlay{Principal: effective.Principal, Agent: effective.Agent, ExpiresAt: now.Add(ttl), TtlMs: effective.TtlMs, Epoch: effective.Epoch, LastHeartbeat: now}
		for attempt := 0; attempt < 8; attempt++ {
			next, saveErr := saveLeaseOverlay(ctx, checkpoints, w.lease, ref.ID, generation, overlay)
			if saveErr == nil {
				generation = next
				resultExpiry = overlay.ExpiresAt
				break
			}
			if !errors.Is(saveErr, driver.ErrCheckpointConflict) {
				return port.HeartbeatOutcome{}, saveErr
			}
			overlay, generation, present, err = loadLeaseOverlay(ctx, checkpoints, ref.ID)
			if err != nil {
				return port.HeartbeatOutcome{}, err
			}
			if present && sameLeaseIdentity(base, overlay) && !now.After(overlay.LastHeartbeat.Add(time.Second)) {
				resultExpiry = overlay.ExpiresAt
				break
			}
		}
		if resultExpiry.IsZero() {
			return port.HeartbeatOutcome{}, failure("busy", "heartbeat lease overlay changed concurrently")
		}
	}
	receipt := heartbeatReceipt{ID: ref.ID, RequestHash: hash, LeaseExpiresAt: resultExpiry}
	for attempt := 0; attempt < 8; attempt++ {
		_, receiptGeneration, receiptPresent, receiptErr := loadHeartbeatReceipt(ctx, checkpoints, receiptKey)
		if receiptErr != nil {
			return port.HeartbeatOutcome{}, receiptErr
		}
		if receiptPresent {
			if receipt.RequestHash != hash || receipt.ID != ref.ID {
				return port.HeartbeatOutcome{}, failure("idempotency_key_reused", "idempotency key was used with different parameters")
			}
			return port.HeartbeatOutcome{LeaseExpiresAt: receipt.LeaseExpiresAt, Replayed: true}, nil
		}
		if err := saveHeartbeatReceipt(ctx, checkpoints, w.lease, receiptKey, receiptGeneration, receipt); err == nil {
			return port.HeartbeatOutcome{LeaseExpiresAt: resultExpiry}, nil
		} else if !errors.Is(err, driver.ErrCheckpointConflict) {
			return port.HeartbeatOutcome{}, err
		}
	}
	return port.HeartbeatOutcome{}, failure("busy", "heartbeat response changed concurrently")
}
