package pipeline

import (
	"context"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// claimReleaseMode is private so a caller cannot turn a generic Writer.Apply
// into a lease release by supplying bead.release as metadata.
type claimReleaseMode struct {
	reclaim bool
}

func defaultClaimReclaimGrace(ttl time.Duration) time.Duration {
	const maxDuration = time.Duration(1<<63 - 1)
	if ttl > maxDuration/2 {
		return maxDuration
	}
	return ttl * 2
}

// ApplyClaimRelease is the only Writer entry point for bead.release. The
// writer independently selects system:core recovery and owns current lease,
// expiry/grace, storage fencing and record CAS checks. Callers cannot supply a
// recovery flag, reduce the grace window, or provide a status patch.
func (w *Writer) ApplyClaimRelease(ctx context.Context, by model.PrincipalRef, ref model.Ref, reason string, meta model.WriteMeta) (port.WriteOutcome, error) {
	meta.Method = "bead.release"
	ops := []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: map[string]any{}}}}
	if reason != "" {
		ops = append(ops, model.Op{Kind: model.OpComment, Comment: &model.CommentOp{Target: ref, Body: reason}})
	}
	mode := &claimReleaseMode{reclaim: by.Kind == "system" && by.ID == "core"}
	return w.applyWithOutcome(ctx, by, model.ChangeSet{Ops: ops}, meta, mode, nil)
}

func (w *Writer) checkClaimRelease(ctx context.Context, p *prepared, doc Document, by model.PrincipalRef, meta model.WriteMeta, mode *claimReleaseMode) error {
	lease := doc.Lease
	if lease != nil {
		overlayStore, ok := w.store.(driver.CheckpointStore)
		if !ok {
			return driver.ErrUnsupported
		}
		overlay, generation, present, err := loadLeaseOverlay(ctx, overlayStore, doc.ID)
		if err != nil {
			return err
		}
		lease = effectiveLease(lease, overlay, present)
		if p.checkpointReads == nil {
			p.checkpointReads = make(map[string]uint64)
		}
		key := leaseCheckpointKey(doc.ID)
		if prior, exists := p.checkpointReads[key]; exists && prior != generation {
			return errStale
		}
		p.checkpointReads[key] = generation
	}
	if lease == nil {
		return failure("lease_not_held", "bead has no active claim")
	}
	epoch, err := lease.Epoch.Uint64()
	if err != nil || epoch == 0 || epoch > w.lease.Epoch() {
		return failure("lease_not_held", "claim has an invalid storage lease epoch")
	}
	now := w.cfg.now()
	if !mode.reclaim {
		principal, _ := driver.PrincipalKey(by)
		holder, err := driver.PrincipalKey(lease.Principal)
		if err != nil || principal != holder || by.Kind == "anonymous" && meta.AgentLabel != lease.Agent {
			return failure("lease_not_held", "bead is claimed by another principal")
		}
		if !now.Before(lease.ExpiresAt) {
			return failure("lease_expired", "bead claim has expired")
		}
		return nil
	}
	// A new active server may recover a previous server's stale claim. The
	// stored epoch and whole lease image are guarded by the record CAS; the
	// transaction is fenced by the CURRENT storage lease, never the old one.
	ttl, err := lease.TtlMs.Int64()
	if err != nil || ttl < 1 || ttl > int64((1<<63-1)/time.Millisecond) {
		return failure("lease_not_held", "claim has an invalid lease TTL")
	}
	cutoff := now.Add(-defaultClaimReclaimGrace(time.Duration(ttl) * time.Millisecond))
	if !lease.ExpiresAt.Before(cutoff) {
		return failure("lease_expired", "bead claim is inside the reclaim grace window")
	}
	return nil
}
