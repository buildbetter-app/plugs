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
	finalized   bool
}

type readyOrderCommitProof struct {
	publication        readyindex.Publication
	sourceHistoryEmpty bool
}

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
	if p.IsZero() {
		return driver.ErrUnsupported
	}
	signature, err := t.readyOrderOverlaySignature()
	if err != nil {
		return err
	}
	t.readyPublication = &readyOrderPublicationState{publication: p, overlay: signature}
	return nil
}

// FinalizeReadyOrderPublication is called only after the readyindex producer
// has staged every source, derived row, allocator and predicate mutation. The
// final StageReadyOrderPublication call has already computed the overlay
// signature against that complete set; retaining the marker avoids serializing
// the same large overlay again in Commit while preserving the ordinary
// signature recheck for non-finalized/manual callers.
func (t *transaction) FinalizeReadyOrderPublication(ctx context.Context, p driver.ReadyOrderPublication) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	if t.readyPublication == nil || t.readyPublication.publication.Digest() != p.Digest() {
		return driver.ErrUnsupported
	}
	t.readyPublication.finalized = true
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
	if !t.readyPublication.finalized {
		signature, err := t.readyOrderOverlaySignature()
		if err != nil {
			return nil, err
		}
		if signature != t.readyPublication.overlay {
			return nil, nil
		}
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
	if err = c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM ready_order_fence WHERE singleton=1 AND dirty=0 AND certified_generation=generation AND incarnation=? AND head=? AND authority_head=head AND format=2 AND NOT EXISTS(SELECT 1 FROM ready_order_changes)),EXISTS(SELECT 1 FROM ready_order_fence WHERE singleton=1 AND ever_source=0)`, meta.Incarnation, revKey(mustUint(meta.Head))).Scan(&predecessor, &virgin); err != nil {
		return nil, err
	}
	emptyKeyHistory := false
	if !predecessor {
		if !meta.Head.IsZero() || !virgin {
			return nil, nil
		}
		var occupied bool
		if err = c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records) OR EXISTS(SELECT 1 FROM versions) OR EXISTS(SELECT 1 FROM key_history) OR EXISTS(SELECT 1 FROM hierarchy_source_current) OR EXISTS(SELECT 1 FROM hierarchy_source_history) OR EXISTS(SELECT 1 FROM graph_link_source_current) OR EXISTS(SELECT 1 FROM graph_link_current) OR EXISTS(SELECT 1 FROM graph_link_source_history) OR EXISTS(SELECT 1 FROM graph_link_history) OR EXISTS(SELECT 1 FROM objects) OR EXISTS(SELECT 1 FROM selection_source_history) OR EXISTS(SELECT 1 FROM selection_bead_history) OR EXISTS(SELECT 1 FROM selection_fact_sets) OR EXISTS(SELECT 1 FROM selection_facts)`).Scan(&occupied); err != nil {
			return nil, err
		}
		if occupied {
			return nil, nil
		}
		// The same-connection bootstrap proof also establishes that every
		// requested Ready before-image is absent. Retain the exact comparison
		// below without repeating its physical point reads.
		emptyKeyHistory = true
	}
	sources := map[string]readyindex.StagedSource{}
	for key, record := range t.records {
		kind := ""
		if record != nil {
			kind = record.Kind
		} else if err = c.QueryRowContext(ctx, `SELECT kind FROM objects WHERE key=?`, key).Scan(&kind); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		sources[key] = readyindex.StagedSource{Record: record, Kind: kind}
	}
	if err = p.ValidateOverlay(sources, t.derived, t.acquired, t.bumps); err != nil {
		return nil, nil
	}
	beforeKeys := make([]driver.LockKey, len(p.Data.Before))
	for i, before := range p.Data.Before {
		beforeKeys[i] = before.Key
	}
	var beforeStates map[string]driver.LockState
	if emptyKeyHistory {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		beforeStates = make(map[string]driver.LockState, len(beforeKeys))
		for _, key := range beforeKeys {
			id := keyID(key)
			if _, duplicate := beforeStates[id]; duplicate {
				return nil, nil
			}
			beforeStates[id] = driver.LockState{Key: key, Revision: model.Natural64(0)}
		}
	} else {
		var cached bool
		beforeStates, cached = t.readyOrderAcquiredStates(beforeKeys)
		if !cached {
			beforeStates, err = readyOrderReadStates(ctx, c, beforeKeys)
			if errors.Is(err, driver.ErrUnsupported) {
				return nil, nil
			}
			if err != nil {
				return nil, err
			}
		}
	}
	for _, before := range p.Data.Before {
		actual := beforeStates[keyID(before.Key)]
		if !readyOrderSameImage(actual, before, true) {
			return nil, nil
		}
	}
	return &readyOrderCommitProof{publication: p, sourceHistoryEmpty: emptyKeyHistory}, nil
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
	rows, err := c.QueryContext(ctx, `SELECT domain,key FROM ready_order_changes`)
	if err != nil {
		return err
	}
	unknown := false
	for rows.Next() {
		var domain string
		var key string
		if err = rows.Scan(&domain, &key); err != nil {
			break
		}
		_, record := t.records[key]
		_, derived := t.derived[key]
		_, bump := t.bumps[key]
		if domain == "source" && !record || domain == "state" && !record && !derived && !bump || domain != "source" && domain != "state" {
			unknown = true
		}
	}
	if err == nil {
		err = rows.Err()
	}
	closeErr := rows.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if unknown {
		return nil
	}
	sourceKeys := make([]string, len(p.Data.Mutations))
	for i, mutation := range p.Data.Mutations {
		sourceKeys[i], _ = recordKey(mutation.Ref)
	}
	sourceImages, err := readyOrderReadPhysical(ctx, c, sourceKeys, false)
	if errors.Is(err, driver.ErrUnsupported) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, mutation := range p.Data.Mutations {
		id, _ := recordKey(mutation.Ref)
		image := sourceImages[id]
		if mutation.Item == nil {
			if !image.present {
				continue
			}
			return nil
		}
		if !image.present {
			return nil
		}
		var record driver.Record
		if json.Unmarshal(image.data, &record) != nil || record.Kind != "bead" || record.Revision != target.Head || driver.SelectionDigest(record.Body) != mutation.SourceDigest {
			return nil
		}
	}
	finalImages := p.FinalImages()
	finalKeys := make([]driver.LockKey, len(finalImages))
	for i, want := range finalImages {
		finalKeys[i] = want.Key
	}
	finalStates, err := readyOrderReadStates(ctx, c, finalKeys)
	if errors.Is(err, driver.ErrUnsupported) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, want := range finalImages {
		actual := finalStates[keyID(want.Key)]
		if !readyOrderSameImage(actual, want, false) {
			return nil
		}
	}
	if _, err = c.ExecContext(ctx, `DELETE FROM ready_order_changes`); err != nil {
		return err
	}
	_, err = c.ExecContext(ctx, `UPDATE ready_order_fence SET certified_generation=generation,dirty=0,format=2,incarnation=?,head=?,authority_head=?,root_hash=?,root_count=?,eligibility_digest=?,publication_digest=? WHERE singleton=1`, target.Incarnation, revKey(mustUint(target.Head)), revKey(mustUint(target.Head)), p.Root.Hash, revKey(p.Root.Count), p.EligibilityDigest, p.Digest)
	return err
}

// The predicate is evaluated against the same pinned snapshot, or W10's one
// current meta/Ready/authority statement. Core eligibility is additionally
// checked by readyindex against this request's effective ontology binding.
const readyOrderPhysicalPredicate = `EXISTS(SELECT 1 FROM ready_order_fence rc WHERE rc.singleton=1 AND rc.dirty=0 AND rc.certified_generation=rc.generation AND rc.format=2 AND rc.incarnation=m.incarnation AND rc.head=m.head AND rc.authority_head=m.head AND rc.root_hash IS NOT NULL AND rc.eligibility_digest IS NOT NULL AND rc.publication_digest IS NOT NULL AND NOT EXISTS(SELECT 1 FROM ready_order_changes))`

var _ driver.ReadyOrderPublicationWriter = (*transaction)(nil)
