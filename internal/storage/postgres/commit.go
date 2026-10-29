package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (t *transaction) header(h driver.ChangesetHeader) ([]byte, error) {
	if !t.hasHead {
		return nil, fmt.Errorf("postgres: commit requires Head")
	}
	if err := driver.ValidateHeaderLocks(h.LockKeys, t.acquired); err != nil {
		return nil, err
	}
	if err := branchZero(h.Branch); err != nil {
		return nil, err
	}
	if h.Incarnation != t.meta.Incarnation {
		return nil, driver.ErrIncarnationChanged{From: h.Incarnation, To: t.meta.Incarnation}
	}
	principal, err := driver.PrincipalKey(h.Principal)
	if err != nil {
		return nil, err
	}
	if err = h.Kind.Validate(); err != nil {
		return nil, err
	}
	if h.ResponsePending && h.IdempotencyKey == "" {
		return nil, fmt.Errorf("postgres: pending response requires an idempotency key")
	}
	if h.IdempotencyKey == "" {
		return nil, nil
	}
	if !canonical(h.RequestHash) {
		return nil, fmt.Errorf("postgres: idempotent commit requires request digest")
	}
	return scopeKey(principal, h.AgentLabel, h.Method, h.IdempotencyKey)
}

func (t *transaction) validateChanges(ctx context.Context, changes []model.ChangeRecord) error {
	seen := make(map[string]bool, len(changes))
	for _, change := range changes {
		if err := change.Op.Validate(); err != nil {
			return err
		}
		id, err := recordKey(change.Ref)
		if err != nil {
			return err
		}
		value, ok := t.records[id]
		if !ok || seen[id] {
			return fmt.Errorf("postgres: changes must describe every staged record exactly once")
		}
		seen[id] = true
		if (value == nil) != (change.Op == model.OpDelete) {
			return fmt.Errorf("postgres: deletion and operation disagree")
		}
		previous, ok, err := t.recordAt(ctx, id)
		if err != nil {
			return err
		}
		revision := model.Natural64(0)
		if ok {
			revision = previous.record.Revision
		}
		from, err := change.FromRevision.Uint64()
		if err != nil {
			return err
		}
		n, _ := revision.Uint64()
		if from != n {
			return driver.ErrConflict
		}
	}
	if len(seen) != len(t.records) {
		return fmt.Errorf("postgres: undeclared record write")
	}
	return nil
}

// Row payloads are opaque. Core supplies exact affected scopes; drivers prove
// that required sentinel families are locked and bumped in this publication.
func (t *transaction) validatePredicates(ctx context.Context) error {
	for _, request := range t.acquired {
		if request.Key.Kind == driver.LockSentinel && request.Mode != driver.LockShare {
			id, _ := keyID(request.Key)
			if _, ok := t.bumps[id]; !ok {
				return fmt.Errorf("postgres: update sentinel requires atomic bump")
			}
		}
	}
	for id, record := range t.records {
		kind := ""
		var ref driver.Ref
		if record != nil {
			kind, ref = record.Kind, record.Ref
		} else {
			kind, ref = t.identities[id].Kind, t.identities[id].Ref
		}
		if kind != "bead" {
			continue
		}
		if !ref.IsID() {
			return fmt.Errorf("postgres: bead writes require resolved id")
		}
		sentinel, err := driver.NewBeadSentinel(ref.ID)
		if err != nil {
			return err
		}
		key, _ := keyID(driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
		if _, ok := t.bumps[key]; !ok {
			return fmt.Errorf("postgres: bead write requires exact bead sentinel bump")
		}
	}
	for _, value := range t.derived {
		family := ""
		switch value.Key.Derived.Table {
		case driver.DerivedReadyState:
			family = "ready/"
		case driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute:
			family = "closure/"
		case driver.DerivedGrant:
			family = "grant/"
		case driver.DerivedOntology:
			family = "ontology"
		case driver.DerivedPlugins:
			family = "plugins"
		}
		if family == "" {
			continue
		}
		found := false
		for _, key := range t.bumps {
			if key.Kind == driver.LockSentinel && (string(key.Sentinel) == family || strings.HasSuffix(family, "/") && strings.HasPrefix(string(key.Sentinel), family)) {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("postgres: derived table %s requires %s predicate bump", value.Key.Derived.Table, family)
		}
	}
	return nil
}

func (t *transaction) abortFenced(err error) error {
	var fenced driver.ErrFenced
	if errors.As(err, &fenced) {
		_ = t.finish()
	}
	return err
}
func (t *transaction) fence(ctx context.Context) error {
	_, err := t.store.checkLease(ctx, t.sql, t.lease, true)
	if err != nil {
		_ = t.finish()
	}
	return err
}

func (t *transaction) saveReceipt(ctx context.Context, scope []byte, h driver.ChangesetHeader, response model.Commit, entry model.Changeset) error {
	if scope == nil {
		return nil
	}
	raw, err := encodeReceipt(driver.IdempotencyRecord{RequestHash: h.RequestHash, Commit: response, Changeset: entry, ResponsePending: h.ResponsePending})
	if err != nil {
		return err
	}
	_, err = t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.receipts (scope,revision,expires,value,finalized) VALUES ($1,$2,clock_timestamp()+interval '24 hours',$3,$4) ON CONFLICT (scope) DO UPDATE SET revision=EXCLUDED.revision,expires=EXCLUDED.expires,value=EXCLUDED.value,finalized=EXCLUDED.finalized,post=NULL`, scope, response.Revision.String(), raw, !h.ResponsePending)
	return err
}

// All earlier SQL failures roll back determinately. A failed COMMIT without a
// server SQLSTATE cannot prove whether publication reached the engine.
func (t *transaction) publish(ctx context.Context, key string) error {
	if err := t.fence(ctx); err != nil {
		return err
	}
	err := t.sql.Commit()
	_ = t.finish()
	if err != nil && sqlState(err) == "" {
		return driver.ErrIndeterminate{IdempotencyKey: key}
	}
	return err
}

func (t *transaction) Commit(ctx context.Context, h driver.ChangesetHeader) (model.Changeset, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	zero := model.Changeset{}
	if err := t.check(ctx); err != nil {
		return zero, t.abortFenced(err)
	}
	scope, err := t.header(h)
	if err != nil {
		return zero, err
	}
	// G4 replay is checked before validating fresh operations: a retry may
	// propose different rows but must never publish them under an existing key.
	if err = t.fence(ctx); err != nil {
		return zero, err
	}
	if scope != nil {
		previous, err := lookupReceipt(ctx, t.sql, scope, t.at)
		if err != nil {
			_ = t.finish()
			return zero, err
		}
		if previous != nil {
			if previous.RequestHash != h.RequestHash || previous.Changeset.Kind == "" {
				return zero, driver.ErrConflict
			}
			value := previous.Changeset
			if err = t.publish(ctx, h.IdempotencyKey); err != nil {
				return zero, err
			}
			return value, nil
		}
	}
	if err = t.validateChanges(ctx, h.Changes); err != nil {
		return zero, err
	}
	if err = t.validatePredicates(ctx); err != nil {
		return zero, err
	}
	if t.at == math.MaxUint64 {
		return zero, fmt.Errorf("%w: postgres revision exhausted", driver.ErrUnsupported)
	}
	revision := model.Natural64(t.at + 1)
	when := h.Time
	if when.IsZero() {
		if err = t.sql.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&when); err != nil {
			_ = t.finish()
			return zero, err
		}
	}
	if h.Chain == nil {
		h.Chain = []string{}
	}
	if h.Changes == nil {
		h.Changes = []model.ChangeRecord{}
	}
	entry, err := copied(model.Changeset{Revision: revision, Branch: model.Natural64(0), Incarnation: h.Incarnation, Kind: h.Kind, Time: when, Principal: h.Principal, Chain: h.Chain, IdempotencyKey: h.IdempotencyKey, Mutators: h.Mutators, Changes: h.Changes})
	if err != nil {
		return zero, err
	}
	response, err := responseAt(h.Response, revision)
	if err != nil {
		return zero, err
	}
	// Once database publication begins, every failure closes and rolls back
	// the transaction, including all already-inserted version and blob rows.
	if err = t.writeRecords(ctx, entry); err != nil {
		_ = t.finish()
		return zero, err
	}
	for id, state := range t.derived {
		state.Revision = revision
		if err = t.writeState(ctx, id, state); err != nil {
			_ = t.finish()
			return zero, err
		}
	}
	for id, key := range t.bumps {
		if err = t.writeState(ctx, id, driver.LockState{Key: key, Revision: revision, Present: true}); err != nil {
			_ = t.finish()
			return zero, err
		}
	}
	for hash, data := range t.blobs {
		if _, err = t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.blobs VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`, string(hash), revision.String(), data); err != nil {
			_ = t.finish()
			return zero, err
		}
	}
	raw, err := encode(entry)
	if err != nil {
		_ = t.finish()
		return zero, err
	}
	if _, err = t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.changesets VALUES ($1,$2)`, revision.String(), raw); err != nil {
		_ = t.finish()
		return zero, err
	}
	if _, err = t.sql.ExecContext(ctx, `UPDATE plug_postgres.meta SET head=$1 WHERE singleton`, revision.String()); err != nil {
		_ = t.finish()
		return zero, err
	}
	if err = t.saveReceipt(ctx, scope, h, response, entry); err != nil {
		_ = t.finish()
		return zero, err
	}
	if err = t.publish(ctx, h.IdempotencyKey); err != nil {
		return zero, err
	}
	return entry, nil
}

func (t *transaction) writeState(ctx context.Context, id string, state driver.LockState) error {
	raw, err := encode(state)
	if err != nil {
		return err
	}
	_, err = t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.states VALUES ($1,$2,$3)`, []byte(id), state.Revision.String(), raw)
	return err
}

func (t *transaction) writeRecords(ctx context.Context, entry model.Changeset) error {
	for id, record := range t.records {
		previous, exists, err := t.recordAt(ctx, id)
		if err != nil {
			return err
		}
		count := previous.count
		if count == math.MaxUint64 {
			return fmt.Errorf("%w: postgres record version counter exhausted", driver.ErrUnsupported)
		}
		var ref driver.Ref
		kind := ""
		var body []byte
		if record != nil {
			ref, kind, body = record.Ref, record.Kind, record.Body
		} else {
			ref, kind = t.identities[id].Ref, t.identities[id].Kind
		}
		version := driver.Version{Ref: ref, Revision: entry.Revision, CreatedAt: entry.Time, Principal: entry.Principal, Deleted: record == nil, Base: count%driver.VersionBaseEvery == 0 || exists && previous.deleted}
		if record != nil {
			if version.Base {
				version.Body = body
			} else {
				version.Patch, err = encode([]map[string]any{{"op": "replace", "path": "", "value": json.RawMessage(body)}})
				if err != nil {
					return err
				}
			}
		}
		refRaw, err := encode(ref)
		if err != nil {
			return err
		}
		raw, err := encode(version)
		if err != nil {
			return err
		}
		if _, err = t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.records VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (key) DO UPDATE SET revision=EXCLUDED.revision,body=EXCLUDED.body,deleted=EXCLUDED.deleted,version_count=EXCLUDED.version_count`, []byte(id), refRaw, kind, entry.Revision.String(), body, record == nil, decimal(count+1)); err != nil {
			return err
		}
		if _, err = t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.versions VALUES ($1,$2,$3,$4)`, []byte(id), entry.Revision.String(), version.Base, raw); err != nil {
			return err
		}
	}
	return nil
}

func (t *transaction) CommitNoop(ctx context.Context, h driver.ChangesetHeader) (model.Commit, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	zero := model.Commit{}
	if err := t.check(ctx); err != nil {
		return zero, t.abortFenced(err)
	}
	scope, err := t.header(h)
	if err != nil {
		return zero, err
	}
	if len(h.Changes)+len(t.records)+len(t.derived)+len(t.blobs)+len(t.bumps) != 0 {
		return zero, fmt.Errorf("postgres: no-op cannot contain staged mutations")
	}
	if h.ResponsePending {
		return zero, fmt.Errorf("postgres: no-op response must be final")
	}
	response, err := responseAt(h.Response, model.Natural64(t.at))
	if err != nil {
		return zero, err
	}
	if err = t.fence(ctx); err != nil {
		return zero, err
	}
	if scope != nil {
		previous, err := lookupReceipt(ctx, t.sql, scope, t.at)
		if err != nil {
			_ = t.finish()
			return zero, err
		}
		if previous != nil {
			if previous.RequestHash != h.RequestHash || previous.ResponsePending {
				return zero, driver.ErrConflict
			}
			response = previous.Commit
		} else {
			if err = t.saveReceipt(ctx, scope, h, response, model.Changeset{}); err != nil {
				_ = t.finish()
				return zero, err
			}
		}
	}
	if err = t.publish(ctx, h.IdempotencyKey); err != nil {
		return zero, err
	}
	return response, nil
}

var _ driver.NoopCommitter = (*transaction)(nil)
