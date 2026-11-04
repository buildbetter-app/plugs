package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/readyindex"
)

// readyOrderPublicationState is transaction-owned. The signature also catches
// accepted mutation paths which write staging maps without a common mutator.
type readyOrderPublicationState struct {
	publication driver.ReadyOrderPublication
	overlay     string
}

type readyOrderCommitProof struct{ publication readyindex.Publication }

func (t *transaction) readyOrderOverlaySignature() (string, error) {
	data, err := json.Marshal([]any{t.records, t.derived, t.bumps, t.allocators, t.parentCounters, t.restore, t.offlineHistory})
	return driver.SelectionDigest(data), err
}

func (t *transaction) invalidateReadyOrderPublication() { t.readyPublication = nil }

func (t *transaction) StageReadyOrderPublication(ctx context.Context, p driver.ReadyOrderPublication) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	if !t.hasHead {
		return driver.ErrUnsupported
	}
	if _, err := p.Data(); err != nil {
		return driver.ErrUnsupported
	}
	signature, err := t.readyOrderOverlaySignature()
	if err != nil {
		return err
	}
	t.readyPublication = &readyOrderPublicationState{publication: p, overlay: signature}
	return nil
}

func readyOrderReadState(ctx context.Context, c *sql.Conn, key driver.LockKey) (driver.LockState, error) {
	state := driver.LockState{Key: key, Revision: model.Natural64(0)}
	var data []byte
	err := c.QueryRowContext(ctx, `SELECT data FROM key_history WHERE key=? ORDER BY rev DESC LIMIT 1`, keyID(key)).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	if json.Unmarshal(data, &state) != nil {
		return state, driver.ErrUnsupported
	}
	return state, nil
}

func readyOrderSameImage(a, b driver.LockState, revision bool) bool {
	if a.Present != b.Present || revision && a.Revision != b.Revision {
		return false
	}
	if !a.Present {
		return true
	}
	return a.Derived != nil && b.Derived != nil && a.Derived.Key == b.Derived.Key && a.Derived.Index == nil && b.Derived.Index == nil && bytes.Equal(a.Derived.Value, b.Derived.Value)
}

// Called under store.immediate, after all preparation but before ANY physical
// source/projection write. An unavailable optional proof never rejects a write.
func (t *transaction) prepareReadyOrderCommit(ctx context.Context, c *sql.Conn, meta driver.Meta, revision model.Revision) (*readyOrderCommitProof, error) {
	if t.readyPublication == nil || t.restore != nil || t.offlineHistory != nil {
		return nil, nil
	}
	signature, err := t.readyOrderOverlaySignature()
	if err != nil {
		return nil, err
	}
	if signature != t.readyPublication.overlay {
		return nil, nil
	}
	p, err := readyindex.ValidatePublication(ctx, t.readyPublication.publication)
	if errors.Is(err, driver.ErrUnsupported) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if p.Data.Incarnation != meta.Incarnation || p.Data.Base != meta.Head || p.Data.Revision != revision {
		return nil, nil
	}
	var predecessor, virgin bool
	if err = c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM ready_order_fence WHERE singleton=1 AND dirty=0 AND certified_generation=generation AND incarnation=? AND head=? AND authority_head=head AND format=2),EXISTS(SELECT 1 FROM ready_order_fence WHERE singleton=1 AND ever_source=0)`, meta.Incarnation, revKey(mustUint(meta.Head))).Scan(&predecessor, &virgin); err != nil {
		return nil, err
	}
	if !predecessor {
		if !meta.Head.IsZero() || !virgin {
			return nil, nil
		}
		var occupied bool
		if err = c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records) OR EXISTS(SELECT 1 FROM versions) OR EXISTS(SELECT 1 FROM key_history)`).Scan(&occupied); err != nil {
			return nil, err
		}
		if occupied {
			return nil, nil
		}
	}
	witnesses := map[string]readyindex.SourceWitness{}
	for key, record := range t.records {
		kind := ""
		if record != nil {
			kind = record.Kind
		} else if err = c.QueryRowContext(ctx, `SELECT kind FROM objects WHERE key=?`, key).Scan(&kind); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		witnesses[key] = readyindex.SourceWitness{Record: record, Kind: kind}
	}
	if err = p.ValidateOverlay(witnesses, t.derived, t.acquired, t.bumps); err != nil {
		return nil, nil
	}
	for _, before := range p.Data.Before {
		actual, err := readyOrderReadState(ctx, c, before.Key)
		if errors.Is(err, driver.ErrUnsupported) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		if !readyOrderSameImage(actual, before, true) {
			return nil, nil
		}
	}
	return &readyOrderCommitProof{publication: p}, nil
}

// No watched tables may change after this call. Meta, receipts, changesets,
// allocator journals and blobs are deliberately outside the watched scope.
func (t *transaction) finishReadyOrderCommit(ctx context.Context, c *sql.Conn, proof *readyOrderCommitProof, target driver.Meta) error {
	if proof == nil {
		return nil
	}
	p := proof.publication
	if target.Incarnation != p.Data.Incarnation || target.Head != p.Data.Revision {
		return nil
	}
	for _, want := range p.FinalImages() {
		actual, err := readyOrderReadState(ctx, c, want.Key)
		if errors.Is(err, driver.ErrUnsupported) {
			return nil
		}
		if err != nil {
			return err
		}
		if !readyOrderSameImage(actual, want, false) {
			return nil
		}
	}
	_, err := c.ExecContext(ctx, `UPDATE ready_order_fence SET certified_generation=generation,dirty=0,format=2,incarnation=?,head=?,authority_head=?,root_hash=?,root_count=?,eligibility_digest=?,publication_digest=? WHERE singleton=1`, target.Incarnation, revKey(mustUint(target.Head)), revKey(mustUint(target.Head)), p.Root.Hash, revKey(p.Root.Count), p.EligibilityDigest, p.Digest)
	return err
}

// The predicate is evaluated against the same pinned snapshot, or W10's one
// current meta/Ready/authority statement. Core eligibility is additionally
// checked by readyindex against this request's effective ontology binding.
const readyOrderPhysicalPredicate = `EXISTS(SELECT 1 FROM ready_order_fence rc WHERE rc.singleton=1 AND rc.dirty=0 AND rc.certified_generation=rc.generation AND rc.format=2 AND rc.incarnation=m.incarnation AND rc.head=m.head AND rc.authority_head=m.head AND rc.root_hash IS NOT NULL AND rc.eligibility_digest IS NOT NULL AND rc.publication_digest IS NOT NULL)`

var _ driver.ReadyOrderPublicationWriter = (*transaction)(nil)
