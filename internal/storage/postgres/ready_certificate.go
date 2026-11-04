package postgres

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

func readyOrderReadState(ctx context.Context, c *sql.Tx, key driver.LockKey) (driver.LockState, error) {
	state := driver.LockState{Key: key, Revision: model.Natural64(0)}
	var data []byte
	err := c.QueryRowContext(ctx, `SELECT value FROM plug_postgres.states WHERE key=$1 ORDER BY revision DESC LIMIT 1`, []byte(readyOrderKeyID(key))).Scan(&data)
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

// Called under the Head-locked SQL transaction, after all preparation but before ANY physical
// source/projection write. An unavailable optional proof never rejects a write.
func (t *transaction) prepareReadyOrderCommit(ctx context.Context, c *sql.Tx, meta driver.Meta, revision model.Revision) (*readyOrderCommitProof, error) {
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
	if err = c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM plug_postgres.ready_order_fence WHERE singleton AND NOT dirty AND certified_generation=generation AND incarnation=$1 AND head=$2 AND authority_head=head AND format=2 AND NOT EXISTS(SELECT 1 FROM plug_postgres.ready_order_changes)),EXISTS(SELECT 1 FROM plug_postgres.ready_order_fence WHERE singleton AND NOT ever_source)`, meta.Incarnation, meta.Head.String()).Scan(&predecessor, &virgin); err != nil {
		return nil, err
	}
	if !predecessor {
		if !meta.Head.IsZero() || !virgin {
			return nil, nil
		}
		var occupied bool
		if err = c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM plug_postgres.records) OR EXISTS(SELECT 1 FROM plug_postgres.versions) OR EXISTS(SELECT 1 FROM plug_postgres.states) OR EXISTS(SELECT 1 FROM plug_postgres.hierarchy_source_current) OR EXISTS(SELECT 1 FROM plug_postgres.hierarchy_source_history) OR EXISTS(SELECT 1 FROM plug_postgres.graph_link_source_current) OR EXISTS(SELECT 1 FROM plug_postgres.graph_link_current) OR EXISTS(SELECT 1 FROM plug_postgres.graph_link_source_history) OR EXISTS(SELECT 1 FROM plug_postgres.graph_link_history) OR EXISTS(SELECT 1 FROM plug_postgres.selection_sources) OR EXISTS(SELECT 1 FROM plug_postgres.selection_rows) OR EXISTS(SELECT 1 FROM plug_postgres.selection_facts)`).Scan(&occupied); err != nil {
			return nil, err
		}
		if occupied {
			return nil, nil
		}
	}
	sources := map[string]readyindex.StagedSource{}
	for key, record := range t.records {
		kind := ""
		if record != nil {
			kind = record.Kind
		} else if err = c.QueryRowContext(ctx, `SELECT kind FROM plug_postgres.records WHERE key=$1`, []byte(key)).Scan(&kind); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		sources[key] = readyindex.StagedSource{Record: record, Kind: kind}
	}
	if err = p.ValidateOverlay(sources, t.derived, t.acquired, t.bumps); err != nil {
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
func (t *transaction) finishReadyOrderCommit(ctx context.Context, c *sql.Tx, proof *readyOrderCommitProof, target driver.Meta) error {
	if proof == nil {
		return nil
	}
	p := proof.publication
	if target.Incarnation != p.Data.Incarnation || target.Head != p.Data.Revision {
		return nil
	}
	rows, err := c.QueryContext(ctx, `SELECT domain,key FROM plug_postgres.ready_order_changes`)
	if err != nil {
		return err
	}
	unknown := false
	for rows.Next() {
		var domain string
		var rawKey []byte
		if err = rows.Scan(&domain, &rawKey); err != nil {
			break
		}
		key := string(rawKey)
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
	for _, mutation := range p.Data.Mutations {
		id, _ := recordKey(mutation.Ref)
		var kind, revision string
		var body []byte
		var deleted bool
		err = c.QueryRowContext(ctx, `SELECT kind,revision::text,body,deleted FROM plug_postgres.records WHERE key=$1`, []byte(id)).Scan(&kind, &revision, &body, &deleted)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if mutation.Item == nil {
			if deleted {
				continue
			}
			return nil
		}
		if deleted || kind != "bead" || revision != target.Head.String() || driver.SelectionDigest(body) != mutation.SourceDigest {
			return nil
		}
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
	if _, err = c.ExecContext(ctx, `DELETE FROM plug_postgres.ready_order_changes`); err != nil {
		return err
	}
	_, err = c.ExecContext(ctx, `UPDATE plug_postgres.ready_order_fence SET certified_generation=generation,dirty=false,format=2,incarnation=$1,head=$2,authority_head=$2,root_hash=$3,root_count=$4,eligibility_digest=$5,publication_digest=$6 WHERE singleton`, target.Incarnation, target.Head.String(), p.Root.Hash, model.Natural64(p.Root.Count).String(), p.EligibilityDigest, p.Digest)
	return err
}

// The predicate is evaluated against the same pinned snapshot, or W10's one
// current meta/Ready/authority statement. Core eligibility is additionally
// checked by readyindex against this request's effective ontology binding.
const readyOrderPhysicalPredicate = `EXISTS(SELECT 1 FROM plug_postgres.ready_order_fence rc WHERE rc.singleton AND NOT rc.dirty AND rc.certified_generation=rc.generation AND rc.format=2 AND rc.incarnation=m.incarnation AND rc.head=m.head AND rc.authority_head=m.head AND rc.root_hash IS NOT NULL AND rc.eligibility_digest IS NOT NULL AND rc.publication_digest IS NOT NULL AND NOT EXISTS(SELECT 1 FROM plug_postgres.ready_order_changes))`

func readyOrderKeyID(key driver.LockKey) string { id, _ := keyID(key); return id }

// All ordinary keys are already held in sorted order. NOWAIT remains inside
// complete_lock_set so contention releases the entire prefix. Raw statements
// and maintenance synchronize on this SAME existing Head row, before taking
// either Ready or Selection generation fences.
func (t *transaction) lockReadyOrderPhysical(ctx context.Context, requests []driver.LockRequest) error {
	global, _ := driver.NewReadySentinel("*", "*")
	for _, request := range requests {
		if request.Key.Kind == driver.LockSentinel && request.Key.Sentinel == global && request.Mode != driver.LockShare {
			var singleton bool
			return t.sql.QueryRowContext(ctx, `SELECT singleton FROM plug_postgres.meta WHERE singleton FOR UPDATE NOWAIT`).Scan(&singleton)
		}
	}
	return nil
}

var _ driver.ReadyOrderPublicationWriter = (*transaction)(nil)
