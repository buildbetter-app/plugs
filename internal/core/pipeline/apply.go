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
	return w.applyWithOutcome(ctx, by, cs, meta, nil)
}

func (w *Writer) applyWithOutcome(ctx context.Context, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta, release *claimReleaseMode) (port.WriteOutcome, error) {
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
	if len(cs.Ops) == 0 || len(cs.Ops) > 1000 {
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
		reader, err := w.reader(preCtx)
		if err != nil {
			return port.WriteOutcome{}, err
		}
		defer reader.Close()
		if commit, found, err := w.replay(preCtx, reader, principal, by, cs, meta, hash); found || err != nil {
			return port.WriteOutcome{Commit: commit, Replayed: found && err == nil}, err
		}
		p, err := w.prepare(preCtx, reader, by, cs, meta, release)
		if err == nil {
			err = w.pre(preCtx, p)
		}
		if err == nil {
			err = p.graph(preCtx)
		}
		if err == nil {
			err = w.claims(p, by, meta)
		}
		if err == nil {
			err = p.detectChanges()
		}
		if err == nil {
			err = w.prepareAudit(preCtx, p)
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
			commit, err := p.response(*reader.Snapshot().At, meta)
			commit.WouldCommit = err == nil && len(p.changed) > 0
			return port.WriteOutcome{Commit: commit}, err
		}
		var commit model.Commit
		if err == nil {
			commit, err = w.transact(preCtx, p, by, meta, principal, hash)
		}
		if errors.Is(err, errStale) || errors.Is(err, driver.ErrConflict) {
			continue
		}
		if err != nil {
			var unknown driver.ErrIndeterminate
			if errors.As(err, &unknown) {
				// Only a durable receipt can resolve an uncertain outcome. Never
				// execute another attempt merely because its response was lost.
				if fresh, e := w.reader(ctx); e == nil {
					defer fresh.Close()
					if result, found, e := w.replay(ctx, fresh, principal, by, cs, meta, hash); found || e != nil {
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
			warnings := w.post(ctx, p, commit)
			commit, err = w.finalizeResponse(ctx, principal, meta, hash, commit.Revision, warnings)
		} else {
			commit, err = w.durableResponse(ctx, principal, meta, hash, commit.Revision)
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
	if meta.GateAutoCAS && len(cs.Expect) != 0 {
		// GateService appends its current bead revision after the caller's
		// expectations. That value changes after the first approval/report, so
		// keep it in the transaction CAS but out of the durable request hash.
		cs.Expect = append([]model.Precondition(nil), cs.Expect[:len(cs.Expect)-1]...)
	}
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
			DryRun                  bool
		}{cs, meta.Method, meta.Project, meta.Return, meta.DryRun})
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
	p := port.ProposedChange{RequestID: meta.IdempotencyKey, Principal: by, Meta: meta, BaseRevision: *reader.Snapshot().At, Changes: []port.Change{}}
	for i, result := range entry.Commit.Results {
		if i >= len(cs.Ops) {
			return model.Commit{}, failure("internal_error", "stored response does not match original request")
		}
		p.Changes = append(p.Changes, port.Change{Resource: model.ResourceRef{Kind: "bead", Ref: result.Ref}, Op: cs.Ops[i], After: result.Record, SetBy: result.SetBy})
	}
	snapshot := &snapshotReader{reader: reader, reads: &readSet{states: map[string]driver.LockState{}}}
	if err := w.authorize(ctx, port.AuthorizeReplay, p, snapshot); err != nil {
		return model.Commit{}, err
	}
	return clone(entry.Commit)
}

func (p *prepared) response(revision model.Revision, meta model.WriteMeta) (model.Commit, error) {
	commit := model.Commit{Revision: revision, Results: []model.OpResult{}, Warnings: append([]model.Warning{}, p.warnings...)}
	for _, ref := range p.refs {
		pending := p.writes[ref.ID]
		result := model.OpResult{Ref: ref, Revision: revision, SetBy: cloneMap(pending.setBy)}
		if !p.changed[ref.ID] {
			result.Revision = pending.revision
		}
		if !pending.deleted && meta.Return != "minimal" {
			body, err := json.Marshal(pending.doc)
			if err != nil {
				return model.Commit{}, err
			}
			result.Record = body
		}
		commit.Results = append(commit.Results, result)
		if meta.Method == "bead.claim" && pending.doc.Lease != nil {
			expires, ttl := pending.doc.Lease.ExpiresAt, pending.doc.Lease.TtlMs
			commit.LeaseExpiresAt, commit.LeaseTtlMs = &expires, &ttl
		}
	}
	return clone(commit)
}

func (w *Writer) transact(ctx context.Context, p *prepared, by model.PrincipalRef, meta model.WriteMeta, principal, hash string) (model.Commit, error) {
	sentinels := p.sentinels()
	writes := map[string]*pending{}
	for id, pending := range p.writes {
		if p.changed[id] {
			writes[id] = pending
		}
	}
	locks, err := p.snapshot.locks(writes, sentinels)
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
	head, err := tx.Head(ctx)
	if err != nil {
		return model.Commit{}, err
	}
	// Two callers may prepare the same random key concurrently. Recheck after
	// serialization, before staging anything, and replay outside the transaction.
	if prior, err := tx.IdempotencyByKey(ctx, principal, meta.AgentLabel, meta.Method, meta.IdempotencyKey); err != nil {
		return model.Commit{}, err
	} else if prior != nil {
		return model.Commit{}, errStale
	}
	keys := make([]driver.LockKey, len(locks))
	for i, lock := range locks {
		keys[i] = lock.Key
	}
	if len(writes) == 0 {
		noop, ok := tx.(driver.NoopCommitter)
		if !ok {
			return model.Commit{}, driver.ErrUnsupported
		}
		response, err := p.response(head, meta)
		if err != nil {
			return model.Commit{}, err
		}
		return noop.CommitNoop(ctx, driver.ChangesetHeader{Incarnation: p.snapshot.Snapshot().Incarnation, Kind: model.ChangesetWrite, Time: w.cfg.now().UTC(), Principal: by, IdempotencyKey: meta.IdempotencyKey, Method: meta.Method, AgentLabel: meta.AgentLabel, RequestHash: hash, Response: response, LockKeys: keys})
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
			body, e := json.Marshal(pending.doc)
			if e != nil {
				return model.Commit{}, e
			}
			err = tx.Put(ctx, driver.Record{Ref: pending.ref, Kind: "bead", Body: body})
		}
		if err != nil {
			return model.Commit{}, err
		}
	}
	changes := p.changeRecords()
	events, chain, err := p.events(revision, changes)
	if err != nil {
		return model.Commit{}, err
	}
	if err := tx.BumpSentinels(ctx, sentinels); err != nil {
		return model.Commit{}, err
	}
	entry, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: p.snapshot.Snapshot().Incarnation, Kind: model.ChangesetWrite, Time: w.cfg.now().UTC(), Principal: by, Chain: chain, IdempotencyKey: meta.IdempotencyKey, Method: meta.Method, AgentLabel: meta.AgentLabel, RequestHash: hash, Response: commit, ResponsePending: len(w.cfg.posts) > 0, Mutators: p.mutators, Changes: changes, Events: events, LockKeys: keys})
	if err != nil {
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
		if pending.deleted {
			kind = model.OpDelete
		}
		// A slot patch plus its optional reason comment is one transition.
		// Keep the changed blocker anchored to the patch, not the comment.
		if p.audit != nil && isSlot(p.proposal.Meta.Method) && statusChanged(pending) {
			kind = model.OpPatch
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
