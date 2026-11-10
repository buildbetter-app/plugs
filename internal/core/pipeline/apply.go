package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

var errStale = errors.New("pipeline stale snapshot")
var keyPattern = regexp.MustCompile(`(?i)^(?:[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}|[0-7][0-9A-HJKMNP-TV-Z]{25})$`)
var uuidLike = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func validKey(key string) bool {
	if keyPattern.MatchString(key) {
		return true
	}
	if uuidLike.MatchString(key) {
		return false
	}
	if bytes, err := hex.DecodeString(key); err == nil && len(bytes) >= 16 {
		return true
	}
	bytes, err := base64.RawURLEncoding.Strict().DecodeString(key)
	return err == nil && len(bytes) >= 16
}

// Apply prepares callbacks on an immutable snapshot, then atomically publishes
// records, versions, predicates, response and changeset under one sorted lock
// set. A stale read restarts from a fresh snapshot at most three times.
func (w *Writer) Apply(ctx context.Context, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta) (model.Commit, error) {
	outcome, err := w.ApplyWithOutcome(ctx, by, cs, meta)
	return outcome.Commit, err
}

// ApplyWithOutcome keeps transient execution metadata outside the durable
// response. A first write or admitted no-op is not a replay, even when its
// response must be reloaded after receipt finalization.
func (w *Writer) ApplyWithOutcome(ctx context.Context, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta) (port.WriteOutcome, error) {
	if meta.Method == "bead.release" {
		return port.WriteOutcome{}, failure("invalid_params", "bead.release requires the protected claim release entry point")
	}
	return w.applyWithOutcome(ctx, by, cs, meta, nil, nil)
}

// ApplyClaimNextEmpty is the transaction-level seam for a terminal claimNext
// null. The probe runs only after the transaction has acquired the head and
// before it can publish the durable H+1 receipt.
func (w *Writer) ApplyClaimNextEmpty(ctx context.Context, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta, probe port.ClaimNextReadyProbe) (port.WriteOutcome, error) {
	if meta.Method == "bead.release" {
		return port.WriteOutcome{}, failure("invalid_params", "bead.release requires the protected claim release entry point")
	}
	return w.applyWithOutcome(ctx, by, cs, meta, nil, probe)
}

func (w *Writer) applyWithOutcome(ctx context.Context, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta, release *claimReleaseMode, terminalProbe port.ClaimNextReadyProbe) (outcome port.WriteOutcome, retErr error) {
	started := time.Now()
	committed := false
	defer func() {
		result := classifyOutcome(ctx, outcome, retErr, committed, meta.DryRun)
		w.observeOutcome(result)
		w.observePhaseResult(PhaseApply, time.Since(started), result)
	}()
	if meta.Method == "bead.release" && release == nil {
		return port.WriteOutcome{}, failure("invalid_params", "bead.release requires the protected claim release entry point")
	}
	if ctx.Value(callbackKey{}) != nil {
		return port.WriteOutcome{}, failure("plugin_reentrant_write", "graph writes are forbidden in synchronous callbacks")
	}
	if w.err != nil {
		return port.WriteOutcome{}, failure("invalid_params", w.err.Error())
	}
	if err := ctx.Err(); err != nil {
		return port.WriteOutcome{}, err
	}
	select {
	case <-w.lease.Lost():
		return port.WriteOutcome{}, failure("lease_not_held", "active storage lease has been lost")
	default:
	}
	principal, err := driver.PrincipalKey(by)
	if err != nil {
		return port.WriteOutcome{}, failure("invalid_params", err.Error())
	}
	if meta.Method == "" {
		meta.Method = "tx.apply"
	}
	if meta.Return == "" {
		meta.Return = "full"
	}
	if meta.Return != "full" && meta.Return != "minimal" {
		return port.WriteOutcome{}, failure("invalid_params", "return must be full or minimal")
	}
	if meta.AgentLabel == "" {
		meta.AgentLabel = meta.Agent
	}
	if by.Kind != "anonymous" {
		meta.AgentLabel = ""
	}
	if !validKey(meta.IdempotencyKey) {
		return port.WriteOutcome{}, failure("invalid_params", "idempotency key must encode at least 128 bits as UUIDv4, ULID, hex or base64url")
	}
	if (len(cs.Ops) == 0 && meta.Method != "bead.claimNext") || len(cs.Ops) > 1000 {
		return port.WriteOutcome{}, failure("invalid_params", "change set requires 1 to 1000 operations")
	}
	// Validate the closed tagged unions and detach all caller-owned maps.
	cs, err = clone(cs)
	if err != nil {
		return port.WriteOutcome{}, failure("invalid_params", err.Error())
	}
	by, err = clone(by)
	if err != nil {
		return port.WriteOutcome{}, failure("invalid_params", err.Error())
	}
	hash, err := requestHash(cs, meta)
	if err != nil {
		return port.WriteOutcome{}, failure("invalid_params", err.Error())
	}
	preCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for attempt := 0; attempt <= 3; attempt++ {
		phaseStarted := time.Now()
		reader, err := w.reader(preCtx)
		w.observePhaseSince(preCtx, PhaseAdmission, phaseStarted, err)
		if err != nil {
			return port.WriteOutcome{}, err
		}
		defer reader.Close()
		phaseStarted = time.Now()
		if commit, found, replayErr := w.replay(preCtx, reader, principal, by, cs, meta, hash); found || replayErr != nil {
			w.observePhaseSince(preCtx, PhaseReplay, phaseStarted, replayErr)
			return port.WriteOutcome{Commit: commit, Replayed: found && replayErr == nil}, replayErr
		}
		w.observePhaseSince(preCtx, PhaseReplay, phaseStarted, nil)

		phaseStarted = time.Now()
		var p *prepared
		if memoryMethod(meta.Method) {
			p, err = w.prepareMemory(preCtx, reader, by, cs, meta)
		} else {
			p, err = w.prepareSourceOperations(preCtx, reader, by, cs, meta, release)
			if err == nil {
				err = p.preserveWorkflowComments(preCtx)
			}
		}
		w.observePhaseSince(preCtx, PhasePrepare, phaseStarted, err)
		if memoryMethod(meta.Method) {
			if err == nil {
				// Memory records are outside bead ontology, graph and lease
				// maintenance, but the Writer's authorization boundary still
				// applies to the exact proposed change.
				phaseStarted = time.Now()
				err = w.authorize(preCtx, port.AuthorizeBefore, p.proposal, p.snapshot)
				if err == nil && p.refusal != nil {
					err = p.refusal
				}
				if err == nil {
					err = w.authorize(preCtx, port.AuthorizeAfter, p.proposal, p.snapshot)
				}
				if err == nil {
					err = w.checkRollbackWindow(p)
				}
				w.observePhaseSince(preCtx, PhasePre, phaseStarted, err)
			}
		} else {
			if err == nil {
				phaseStarted = time.Now()
				err = w.pre(preCtx, p)
				w.observePhaseSince(preCtx, PhasePre, phaseStarted, err)
			}
			if err == nil {
				phaseStarted = time.Now()
				err = p.graph(preCtx)
				w.observePhaseSince(preCtx, PhaseGraph, phaseStarted, err)
			}
			if err == nil {
				phaseStarted = time.Now()
				err = w.claims(p, by, meta)
				w.observePhaseSince(preCtx, PhaseClaims, phaseStarted, err)
			}
		}
		if err == nil {
			phaseStarted = time.Now()
			err = p.detectChanges()
			w.observePhaseSince(preCtx, PhaseChanges, phaseStarted, err)
		}
		if err == nil && !memoryMethod(meta.Method) {
			err = w.prepareReadySortKeys(p)
		}
		if err == nil {
			phaseStarted = time.Now()
			err = w.prepareAudit(preCtx, p)
			w.observePhaseSince(preCtx, PhaseAudit, phaseStarted, err)
		}
		if err == nil && meta.DryRun {
			if p.audit != nil {
				n, e := reader.Snapshot().At.Uint64()
				if e != nil || n == math.MaxUint64 {
					return port.WriteOutcome{}, failure("write_conflict", "driver revision space is exhausted")
				}
				if _, _, e := p.events(model.Natural64(n+1), p.changeRecords()); e != nil {
					return port.WriteOutcome{}, e
				}
			}
			phaseStarted = time.Now()
			commit, err := p.response(*reader.Snapshot().At, meta)
			w.observePhaseSince(preCtx, PhaseResponse, phaseStarted, err)
			commit.WouldCommit = err == nil && len(p.changed) > 0
			return port.WriteOutcome{Commit: commit}, err
		}
		var commit model.Commit
		if err == nil {
			phaseStarted = time.Now()
			commit, err = w.transact(preCtx, p, by, meta, principal, hash, terminalProbe)
			w.observePhaseSince(preCtx, PhaseCommit, phaseStarted, err)
			if err == nil {
				committed = true
			}
		}
		if errors.Is(err, errStale) || errors.Is(err, driver.ErrConflict) {
			if errors.Is(err, errStale) {
				w.observeConflict(ConflictStaleSnapshot)
			} else {
				w.observeConflict(ConflictDriver)
			}
			continue
		}
		if err != nil {
			var unknown driver.ErrIndeterminate
			if errors.As(err, &unknown) {
				// Only a durable receipt can resolve an uncertain outcome. Never
				// execute another attempt merely because its response was lost.
				phaseStarted = time.Now()
				fresh, e := w.reader(ctx)
				w.observePhaseSince(ctx, PhaseAdmission, phaseStarted, e)
				if e == nil {
					defer fresh.Close()
					phaseStarted = time.Now()
					result, found, e := w.replay(ctx, fresh, principal, by, cs, meta, hash)
					w.observePhaseSince(ctx, PhaseReplay, phaseStarted, e)
					if found || e != nil {
						return port.WriteOutcome{Commit: result, Replayed: found && e == nil}, e
					}
				}
			}
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				return port.WriteOutcome{}, failure("busy", "write pre phases exceeded their deadline")
			}
			return port.WriteOutcome{}, err
		}
		if len(p.changed) != 0 && len(w.cfg.posts) > 0 {
			phaseStarted = time.Now()
			warnings, postErr := w.post(ctx, p, commit)
			postResult := ResultSuccess
			if len(warnings) != 0 {
				postResult = ResultWarning
			}
			w.observePhaseResult(PhasePost, time.Since(phaseStarted), postResult)
			if postErr != nil {
				return port.WriteOutcome{}, responseUnavailable(meta, commit.Revision, true)
			}
			phaseStarted = time.Now()
			commit, err = w.finalizeResponse(ctx, principal, meta, hash, commit.Revision, warnings)
			w.observePhaseSince(ctx, PhaseResponse, phaseStarted, err)
		} else {
			phaseStarted = time.Now()
			commit, err = w.durableResponse(ctx, principal, meta, hash, commit.Revision)
			w.observePhaseSince(ctx, PhaseResponse, phaseStarted, err)
		}
		return port.WriteOutcome{Commit: commit}, err
	}
	return port.WriteOutcome{}, failure("write_conflict", "snapshot changed during all four write attempts")
}

func (w *Writer) reader(ctx context.Context) (driver.Reader, error) {
	meta, err := w.store.Meta(ctx)
	if err != nil {
		return nil, err
	}
	reader, err := w.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		return nil, err
	}
	if reader.Snapshot().At == nil {
		_ = reader.Close()
		return nil, fmt.Errorf("driver returned an unbound snapshot")
	}
	return reader, nil
}

func requestHash(cs model.ChangeSet, meta model.WriteMeta) (string, error) {
	var (
		data []byte
		err  error
	)
	if meta.GateAction == "" {
		// Keep the pre-gate request identity byte-for-byte compatible so a
		// rolling restart can still replay receipts created by an older host.
		data, err = json.Marshal(struct {
			ChangeSet               model.ChangeSet
			Method, Project, Return string
			ClaimNextIdentity       string `json:",omitempty"`
			DryRun                  bool
		}{cs, meta.Method, meta.Project, meta.Return, meta.ClaimNextIdentity, meta.DryRun})
	} else {
		data, err = json.Marshal(struct {
			ChangeSet                                      model.ChangeSet
			Method, GateAction, GateState, Project, Return string
			GateDetails                                    map[string]any
			DryRun                                         bool
		}{cs, meta.Method, meta.GateAction, meta.GateState, meta.Project, meta.Return, meta.GateDetails, meta.DryRun})
	}
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:]), nil
}

func (w *Writer) replay(ctx context.Context, reader driver.Reader, principal string, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta, hash string) (model.Commit, bool, error) {
	entry, err := reader.IdempotencyByKey(ctx, principal, meta.AgentLabel, meta.Method, meta.IdempotencyKey)
	if err != nil {
		return model.Commit{}, false, err
	}
	if entry == nil {
		return model.Commit{}, false, nil
	}
	if entry.RequestHash != hash {
		return model.Commit{}, true, failure("idempotency_key_reused", "idempotency key was used with different parameters")
	}
	if entry.ResponsePending {
		// Reauthorize the known receipt before exposing pending metadata, then
		// again against the fresh snapshot that holds its final response.
		if _, err := w.replayReceipt(ctx, reader, entry, by, cs, meta); err != nil {
			return model.Commit{}, true, err
		}
		result, err := w.waitResponse(ctx, principal, by, cs, meta, hash, entry.Commit.Revision)
		return result, true, err
	}
	result, err := w.replayReceipt(ctx, reader, entry, by, cs, meta)
	return result, true, err
}

func (w *Writer) replayReceipt(ctx context.Context, reader driver.Reader, entry *driver.IdempotencyRecord, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta) (model.Commit, error) {
	p := port.ProposedChange{RequestID: meta.IdempotencyKey, Principal: by, Meta: meta, BaseRevision: *reader.Snapshot().At, Changes: []port.Change{}, Expect: replayExpectations(cs, entry.Commit.Results)}
	snapshot := &snapshotReader{reader: reader, reads: &readSet{states: map[string]driver.LockState{}}}
	for i, result := range entry.Commit.Results {
		if i >= len(cs.Ops) {
			return model.Commit{}, failure("internal_error", "stored response does not match original request")
		}
		kind := "bead"
		if memoryMethod(meta.Method) {
			kind = "memory"
		}
		if source, _ := ctx.Value(sourceOperationsKey{}).(sourceOperations); source.kind != nil {
			if selected := source.kind(result.Ref); selected != "" {
				kind = selected
			}
		}
		p.Changes = append(p.Changes, port.Change{Resource: model.ResourceRef{Kind: kind, Ref: result.Ref}, Op: cs.Ops[i], After: result.Record, SetBy: result.SetBy})
		if memoryMethod(meta.Method) {
			guards, err := memoryReplayGuards(ctx, snapshot, result.Ref, meta.Method)
			if err != nil {
				return model.Commit{}, err
			}
			p.Changes = append(p.Changes, guards...)
		}
	}
	if err := w.authorize(ctx, port.AuthorizeReplay, p, snapshot); err != nil {
		return model.Commit{}, err
	}
	return clone(entry.Commit)
}

func replayExpectations(cs model.ChangeSet, results []model.OpResult) []model.Precondition {
	aliases := make(map[string]model.Ref)
	for i, op := range cs.Ops {
		if i >= len(results) || op.Kind != model.OpCreate || op.Create == nil || op.Create.Key == "" {
			continue
		}
		aliases[op.Create.Key] = results[i].Ref
	}
	all := append([]model.Precondition{}, cs.Expect...)
	for _, op := range cs.Ops {
		if op.Kind == model.OpClose && op.Close != nil {
			all = append(all, op.Close.Expect...)
		}
	}
	expects := make([]model.Precondition, 0, len(all))
	for _, expect := range all {
		canonical := expect
		if !expect.Ref.IsID() {
			if ref, ok := aliases[expect.Ref.Key]; ok {
				canonical.Ref = ref
			}
		}
		expects = append(expects, canonical)
	}
	return expects
}

func (p *prepared) response(revision model.Revision, meta model.WriteMeta) (model.Commit, error) {
	commit := model.Commit{Revision: revision, Results: []model.OpResult{}, Warnings: append([]model.Warning{}, p.warnings...)}
	for i, ref := range p.refs {
		if active, ok := p.external[i]; ok {
			result := active.Result
			if meta.Return == "minimal" {
				result.Record = nil
			}
			commit.Results = append(commit.Results, result)
			if active.LeaseExpiresAt != nil {
				commit.LeaseExpiresAt, commit.LeaseTtlMs = active.LeaseExpiresAt, active.LeaseTtlMs
			}
			continue
		}
		pending := p.writes[ref.ID]
		result := model.OpResult{Ref: ref, Revision: revision, SetBy: cloneMap(pending.setBy)}
		if !p.changed[ref.ID] {
			result.Revision = pending.revision
		}
		if !pending.deleted && !(pending.memory != nil && pending.memory.Overlay != nil && pending.memory.Overlay.Deleted) && meta.Return != "minimal" {
			body, err := pending.body()
			if err != nil {
				return model.Commit{}, err
			}
			result.Record = body
		}
		commit.Results = append(commit.Results, result)
		if isClaim(meta.Method) && pending.doc.Lease != nil {
			expires, ttl := pending.doc.Lease.ExpiresAt, pending.doc.Lease.TtlMs
			commit.LeaseExpiresAt, commit.LeaseTtlMs = &expires, &ttl
		}
	}
	return clone(commit)
}

func (w *Writer) transact(ctx context.Context, p *prepared, by model.PrincipalRef, meta model.WriteMeta, principal, hash string, terminalProbe port.ClaimNextReadyProbe) (model.Commit, error) {
	sentinels := p.sentinels()
	writes := map[string]*pending{}
	for id, pending := range p.writes {
		if p.changed[id] {
			writes[id] = pending
		}
	}
	allocatorKeys := make([]driver.LockKey, 0)
	allocatorPrefixes := map[string]bool{}
	parentCounters := map[string]bool{}
	for _, pending := range p.writes {
		// Receipt-only no-ops consume no previews. In an ordinary commit,
		// include transient creates too: deleting a preview later in the
		// batch removes its final row, not its consumed number.
		if len(writes) == 0 || (pending.allocatorPrefix == "" && pending.allocatorChild == "") {
			continue
		}
		ref := pending.ref
		allocatorKeys = append(allocatorKeys, driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
		if pending.allocatorPrefix == "" && pending.allocatorChild != "" {
			if !parentCounters[pending.allocatorChild] {
				key, e := driver.ParentCounterLock(p.snapshot.Snapshot().Incarnation, pending.allocatorChild)
				if e != nil {
					return model.Commit{}, e
				}
				allocatorKeys = append(allocatorKeys, key)
				parentCounters[pending.allocatorChild] = true
			}
			continue
		}
		if pending.allocatorPrefix == "" || allocatorPrefixes[pending.allocatorPrefix] {
			continue
		}
		key, e := driver.AllocatorLock(pending.allocatorPrefix)
		if e != nil {
			return model.Commit{}, e
		}
		allocatorKeys = append(allocatorKeys, key)
		allocatorPrefixes[pending.allocatorPrefix] = true
	}
	if p.hierarchyDelta != nil {
		allocatorKeys = append(allocatorKeys, p.hierarchyDelta.locks...)
	}
	if (p.graphValidated || p.graphPreserved) && len(writes) != 0 {
		allocatorKeys = append(allocatorKeys, GraphValidityLockKey())
	}
	allocatorKeys = append(allocatorKeys, p.readySort.locks()...)
	locks, err := p.snapshot.locks(writes, sentinels, allocatorKeys...)
	if err != nil {
		return model.Commit{}, err
	}
	tx, err := w.store.Write(ctx, w.lease)
	if err != nil {
		return model.Commit{}, err
	}
	defer tx.Close()
	defer tx.Rollback(context.WithoutCancel(ctx))
	states, err := tx.Lock(ctx, locks)
	if err != nil {
		return model.Commit{}, err
	}
	if len(states) != len(locks) {
		return model.Commit{}, fmt.Errorf("driver failed to acquire the complete lock set")
	}
	if err := p.snapshot.check(states); err != nil {
		return model.Commit{}, err
	}
	if p.source != nil && p.source.Mutation != nil {
		staged, ok := tx.(driver.CheckpointWriteTx)
		if !ok {
			return model.Commit{}, driver.ErrUnsupported
		}
		mutation := p.source.Mutation
		if err := staged.StageCheckpoint(ctx, mutation.Key, mutation.Expected, mutation.Payload); err != nil {
			if errors.Is(err, driver.ErrCheckpointConflict) {
				return model.Commit{}, errStale
			}
			return model.Commit{}, err
		}
	}
	if mutation, ok := ctx.Value(checkpointMutationKey{}).(driver.CheckpointMutation); ok {
		if mutation.Incarnation != p.snapshot.Snapshot().Incarnation {
			return model.Commit{}, driver.ErrConflict
		}
		staged, ok := tx.(driver.CheckpointWriteTx)
		if !ok {
			return model.Commit{}, driver.ErrUnsupported
		}
		if err := staged.StageCheckpoint(ctx, mutation.Key, mutation.Expected, mutation.Payload); err != nil {
			return model.Commit{}, err
		}
	}
	head, err := tx.Head(ctx)
	if err != nil {
		return model.Commit{}, err
	}
	if p.source != nil && head.String() != p.source.Head.Head.String() {
		return model.Commit{}, errStale
	}
	if len(p.checkpointReads) != 0 {
		guard, ok := tx.(driver.CheckpointGuardTx)
		if !ok {
			return model.Commit{}, driver.ErrUnsupported
		}
		keys := make([]string, 0, len(p.checkpointReads))
		for key := range p.checkpointReads {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if err := guard.ExpectCheckpoint(ctx, key, p.checkpointReads[key]); err != nil {
				if errors.Is(err, driver.ErrCheckpointConflict) {
					return model.Commit{}, errStale
				}
				return model.Commit{}, err
			}
		}
	}
	// Two callers may prepare the same random key concurrently. Recheck after
	// serialization, before staging anything, and replay outside the transaction.
	if prior, err := tx.IdempotencyByKey(ctx, principal, meta.AgentLabel, meta.Method, meta.IdempotencyKey); err != nil {
		return model.Commit{}, err
	} else if prior != nil {
		return model.Commit{}, errStale
	}
	// The claimNext empty result is special: after the head lock, this probe
	// observes the latest committed ready set through the write transaction.
	// A candidate means the earlier live walk became stale, so release the
	// transaction and let the service restart before publishing a null receipt.
	if len(writes) == 0 && meta.Method == "bead.claimNext" && terminalProbe != nil {
		ready, err := terminalProbe(ctx, tx)
		if err != nil {
			return model.Commit{}, err
		}
		if ready {
			return model.Commit{}, port.ErrClaimNextReady
		}
	}
	if len(allocatorPrefixes) > 0 || len(parentCounters) > 0 {
		allocator, supportsProfiles := tx.(driver.AllocatorContinuation)
		parentAllocator, supportsParents := tx.(driver.ParentCounterContinuation)
		if (len(allocatorPrefixes) > 0 && !supportsProfiles) || (len(parentCounters) > 0 && !supportsParents) {
			return model.Commit{}, driver.ErrUnsupported
		}
		consumed := map[string]bool{}
		for _, ref := range p.refs {
			pending := p.writes[ref.ID]
			if pending == nil || (pending.allocatorPrefix == "" && pending.allocatorChild == "") || consumed[ref.ID] {
				continue
			}
			var allocationErr error
			switch {
			case pending.allocatorPrefix == "" && pending.allocatorChild != "":
				allocationErr = parentAllocator.ConsumeParentChild(ctx, pending.allocatorChild, pending.allocatorExpected)
			case pending.allocatorChild != "":
				allocationErr = allocator.AllocateChild(ctx, pending.allocatorChild, pending.allocatorExpected)
			case pending.allocatorHash != nil:
				hashAllocator, supported := tx.(driver.HashAllocatorContinuation)
				if !supported {
					return model.Commit{}, driver.ErrUnsupported
				}
				allocationErr = hashAllocator.AllocateHashTopLevel(ctx, pending.allocatorPrefix, *pending.allocatorHash, pending.allocatorExpected)
			default:
				allocationErr = allocator.AllocateTopLevel(ctx, pending.allocatorPrefix, pending.allocatorExpected)
			}
			if allocationErr != nil {
				return model.Commit{}, allocationErr
			}
			consumed[ref.ID] = true
		}
	}
	keys := make([]driver.LockKey, len(locks))
	for i, lock := range locks {
		keys[i] = lock.Key
	}
	// Method is server-bound audit metadata; authorization remains in the
	// service and canonical Writer checks. No caller-supplied kind is accepted.
	kind := model.ChangesetWrite
	if meta.Method == "history.revert" {
		kind = model.ChangesetRevert
	}
	// Legacy no-op receipts keep the existing head-only NoopCommitter
	// semantics. claimNext's terminal null is different: the contract gives
	// it a durable H+1 receipt, with no graph changes, so it continues through
	// the ordinary changeset commit path below.
	if len(writes) == 0 && meta.Method != "bead.claimNext" {
		noop, ok := tx.(driver.NoopCommitter)
		if !ok {
			return model.Commit{}, driver.ErrUnsupported
		}
		response, err := p.response(head, meta)
		if err != nil {
			return model.Commit{}, err
		}
		commit, err := noop.CommitNoop(ctx, driver.ChangesetHeader{Incarnation: p.snapshot.Snapshot().Incarnation, Kind: kind, Time: w.cfg.now().UTC(), Principal: by, IdempotencyKey: meta.IdempotencyKey, Method: meta.Method, AgentLabel: meta.AgentLabel, RequestHash: hash, Response: response, LockKeys: keys})
		if errors.Is(err, driver.ErrCheckpointConflict) {
			return model.Commit{}, errStale
		}
		return commit, err
	}
	n, err := head.Uint64()
	if err != nil || n == math.MaxUint64 {
		return model.Commit{}, failure("write_conflict", "driver revision space is exhausted")
	}
	revision := model.Natural64(n + 1)
	commit, err := p.response(revision, meta)
	if err != nil {
		return model.Commit{}, err
	}
	ids := make([]string, 0, len(writes))
	for id := range writes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		pending := p.writes[id]
		if pending.deleted {
			err = tx.Delete(ctx, pending.ref)
		} else {
			body, e := pending.body()
			if e != nil {
				return model.Commit{}, e
			}
			err = PutSourceRecord(ctx, tx, driver.Record{Ref: pending.ref, Kind: pending.kind, Body: body})
		}
		if err != nil {
			return model.Commit{}, err
		}
	}
	if p.hierarchyDelta != nil && (len(p.hierarchyDelta.remove) != 0 || len(p.hierarchyDelta.add) != 0) {
		if err := tx.PutDerived(ctx, driver.DerivedAncestorClosure, p.hierarchyDelta.remove, p.hierarchyDelta.add); err != nil {
			return model.Commit{}, err
		}
	}
	if err := p.readySort.publish(ctx, tx, revision); err != nil {
		return model.Commit{}, err
	}
	changes := p.changeRecords()
	events, chain, err := p.events(revision, changes)
	if err != nil {
		return model.Commit{}, err
	}
	if err := tx.BumpSentinels(ctx, sentinels); err != nil {
		return model.Commit{}, err
	}
	graphValidated := p.graphValidated || p.graphPreserved && currentGraphCertificate(states)
	if graphValidated && len(writes) != 0 {
		if err := tx.BumpReadFacts(ctx, []driver.ReadFact{GraphValidityReadFact}); err != nil {
			return model.Commit{}, err
		}
	}
	entry, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: p.snapshot.Snapshot().Incarnation, Kind: kind, Time: w.cfg.now().UTC(), Principal: by, Chain: chain, IdempotencyKey: meta.IdempotencyKey, Method: meta.Method, AgentLabel: meta.AgentLabel, RequestHash: hash, Response: commit, ResponsePending: len(p.changed) != 0 && len(w.cfg.posts) > 0, Mutators: p.mutators, Changes: changes, Events: events, LockKeys: keys})
	if err != nil {
		if errors.Is(err, driver.ErrCheckpointConflict) {
			return model.Commit{}, errStale
		}
		return model.Commit{}, err
	}
	if entry.Revision.String() != revision.String() {
		return model.Commit{}, driver.ErrIndeterminate{IdempotencyKey: meta.IdempotencyKey}
	}
	return commit, nil
}

func (p *prepared) changeRecords() []model.ChangeRecord {
	ids := make([]string, 0, len(p.changed))
	for id := range p.changed {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	changes := make([]model.ChangeRecord, 0, len(ids))
	for _, id := range ids {
		pending := p.writes[id]
		paths := make([]string, 0, len(pending.setBy))
		for path := range pending.setBy {
			paths = append(paths, changedPath(path))
		}
		sort.Strings(paths)
		kind := model.OpPatch
		for _, c := range p.proposal.Changes {
			if c.Resource.Ref.ID == id {
				kind = c.Op.Kind
				if kind == model.OpCreate {
					break
				}
			}
		}
		if pending.deleted || pending.memory != nil && pending.memory.Overlay != nil && pending.memory.Overlay.Deleted {
			kind = model.OpDelete
		} else if kind != model.OpCreate && pending.kind != "memory" && len(pending.before) != 0 {
			before, err := DecodeDocument(pending.before)
			if err == nil && parentID(before.Parent) != parentID(pending.doc.Parent) {
				kind = model.OpReparent
			}
		}
		// Keep a real typed close transition anchored to its lifecycle arm,
		// including when another operation reparents the same stored target.
		// statusChanged excludes creates, deletes and same-slot settlements.
		if p.audit != nil && statusChanged(pending) {
			if p.closes[id] {
				kind = model.OpClose
			} else if isSlot(p.proposal.Meta.Method) {
				kind = model.OpPatch
			}
		}
		changes = append(changes, model.ChangeRecord{Ref: pending.ref, Op: kind, FromRevision: pending.revision, ChangedPaths: paths})
	}
	return changes
}

func changedPath(path string) string {
	escape := func(part string) string { return strings.ReplaceAll(strings.ReplaceAll(part, "~", "~0"), "/", "~1") }
	if name, ok := strings.CutPrefix(path, "fields."); ok {
		return "/fields/" + escape(name)
	}
	if name, ok := strings.CutPrefix(path, "metadata."); ok {
		return "/metadata/" + escape(name)
	}
	return "/" + escape(path)
}
