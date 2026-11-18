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
	expected := t.meta.Incarnation
	if t.restore != nil {
		validate := driver.ValidateOfflineRestoreHeader
		if t.nativeRestore != nil {
			if !t.nativeAllocators {
				return nil, driver.NewNativeAllocatorSnapshotError(driver.NativeAllocatorBindingRequired, nil)
			}
			validate = t.nativeRestore.ValidateHeader
		}
		if t.importRestore != nil {
			validate = t.importRestore.ValidateHeader
		}
		if err := validate(h); err != nil {
			return nil, err
		}
		if t.importRestore != nil {
			for _, r := range t.records {
				if r != nil {
					if err := t.importRestore.ValidateRecord(*r, h.Time); err != nil {
						return nil, err
					}
				}
			}
		}
		if t.sourcePlaneImport != nil {
			if err := t.sourcePlaneImport.Validate(h, t.checkpointWrites, t.records); err != nil {
				return nil, err
			}
		}
		expected = t.restore.Incarnation
	}
	if h.Incarnation != expected {
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
	raw, err := encodeReceipt(driver.IdempotencyRecord{AttemptID: h.AttemptID, RequestHash: h.RequestHash, Commit: response, Changeset: entry, ResponsePending: h.ResponsePending})
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
	if t.offlineHistory != nil || t.stream != nil {
		return zero, fmt.Errorf("prepared history requires CommitOfflineHistory")
	}
	scope, err := t.header(h)
	if err != nil {
		return zero, err
	}
	if t.restore != nil {
		if err := t.restoreEmpty(ctx); err != nil {
			return zero, err
		}
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
	if err = t.checkCheckpointGuards(ctx); err != nil {
		return zero, err
	}
	if err = t.publishCheckpoints(ctx); err != nil {
		return zero, err
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
	// A non-nil event plan is a complete, ordered description of this
	// publication. Validate it against the actual changes and allocated
	// revision before touching any graph, version, log or receipt row. Clone
	// first so the SQL transaction owns every nested collection and source pin.
	entry := model.Changeset{Revision: revision, Branch: model.Natural64(0), Incarnation: h.Incarnation, Kind: h.Kind, Time: when, Principal: h.Principal, Chain: h.Chain, IdempotencyKey: h.IdempotencyKey, Mutators: h.Mutators, Changes: h.Changes, Events: model.CloneEventDescriptors(h.Events)}
	if err := entry.ValidateEvents(); err != nil {
		return zero, fmt.Errorf("postgres: event plan: %w", err)
	}
	entry, err = copied(entry)
	if err != nil {
		return zero, err
	}
	response, err := responseAt(h.Response, revision)
	if err != nil {
		return zero, err
	}
	sourcePlan, err := t.prepareSourcePublication(ctx, revision)
	if err != nil {
		return zero, err
	}
	target := t.meta
	target.Head = revision
	if t.restore != nil {
		target = *t.restore
	}
	// Once database publication begins, every failure closes and rolls back
	// the transaction, including allocator state, source indexes and records.
	if err = t.publishAllocatorJournal(ctx, t.sql, target, t.restore != nil); err != nil {
		_ = t.finish()
		return zero, err
	}
	if err = t.writeRecords(ctx, entry); err != nil {
		_ = t.finish()
		return zero, err
	}
	if err = sourcePlan.Publish(ctx, t.sql); err != nil {
		_ = t.finish()
		return zero, err
	}
	if err = t.writeStates(ctx, revision); err != nil {
		_ = t.finish()
		return zero, err
	}
	for prefix, allocator := range t.allocators {
		if err = allocator.Validate(); err != nil {
			_ = t.finish()
			return zero, err
		}
		raw, e := encode(allocator)
		if e != nil {
			_ = t.finish()
			return zero, e
		}
		if _, err = t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.allocator_states(prefix,data) VALUES ($1,$2) ON CONFLICT (prefix) DO UPDATE SET data=EXCLUDED.data`, prefix, raw); err != nil {
			_ = t.finish()
			return zero, err
		}
	}
	if err = t.publishParentCounters(ctx, t.sql); err != nil {
		_ = t.finish()
		return zero, err
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
	if t.restore != nil {
		if _, err = t.sql.ExecContext(ctx, `UPDATE plug_postgres.meta SET store_id=$1,incarnation=$2,history_floor=$3 WHERE singleton`, string(t.restore.StoreID), t.restore.Incarnation, t.restore.HistoryFloor.String()); err != nil {
			_ = t.finish()
			return zero, err
		}
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

// Ordinary publication appends the same state images in bounded batches within
// the existing SQL transaction. This avoids a round trip for every closure row
// and predicate bump without accumulating one unbounded array of state values.
func (t *transaction) writeStates(ctx context.Context, revision model.Revision) error {
	const batchRows = 256
	keys := make([][]byte, 0, batchRows)
	values := make([][]byte, 0, batchRows)
	flush := func() error {
		if len(keys) == 0 {
			return nil
		}
		_, err := t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.states (key,revision,value)
		 SELECT key,$2::numeric(20,0),value FROM unnest($1::bytea[],$3::bytea[]) AS state_rows(key,value)`, keys, revision.String(), values)
		keys, values = keys[:0], values[:0]
		return err
	}
	appendState := func(id string, state driver.LockState) error {
		state.Revision = revision
		raw, err := encode(state)
		if err != nil {
			return err
		}
		keys = append(keys, []byte(id))
		values = append(values, raw)
		if len(keys) == batchRows {
			return flush()
		}
		return nil
	}
	for id, state := range t.derived {
		if err := appendState(id, state); err != nil {
			return err
		}
	}
	for id, key := range t.bumps {
		if err := appendState(id, driver.LockState{Key: key, Present: true}); err != nil {
			return err
		}
	}
	return flush()
}

func (t *transaction) writeState(ctx context.Context, id string, state driver.LockState) error {
	raw, err := encode(state)
	if err != nil {
		return err
	}
	_, err = t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.states VALUES ($1,$2,$3)`, []byte(id), state.Revision.String(), raw)
	return err
}

// Bound record and version publication within the existing fenced transaction.
const publicationBatchRows = 256

func (t *transaction) writeRecords(ctx context.Context, entry model.Changeset) error {
	keys := make([][]byte, 0, publicationBatchRows)
	refs := make([][]byte, 0, publicationBatchRows)
	kinds := make([]string, 0, publicationBatchRows)
	bodies := make([][]byte, 0, publicationBatchRows)
	deleted := make([]bool, 0, publicationBatchRows)
	counts := make([]string, 0, publicationBatchRows)
	bases := make([]bool, 0, publicationBatchRows)
	versions := make([][]byte, 0, publicationBatchRows)
	queryFields := make([]string, 0, publicationBatchRows)
	flush := func() error {
		if len(keys) == 0 {
			return nil
		}
		if _, err := t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.records (key,ref,kind,revision,body,deleted,version_count)
		 SELECT key,ref,kind,$4::numeric(20,0),body,deleted,version_count::numeric(20,0)
		 FROM unnest($1::bytea[],$2::bytea[],$3::text[],$5::bytea[],$6::boolean[],$7::text[]) AS record_rows(key,ref,kind,body,deleted,version_count)
		 ON CONFLICT (key) DO UPDATE SET revision=EXCLUDED.revision,body=EXCLUDED.body,deleted=EXCLUDED.deleted,version_count=EXCLUDED.version_count`, keys, refs, kinds, entry.Revision.String(), bodies, deleted, counts); err != nil {
			return err
		}
		if _, err := t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.versions (key,revision,is_base,value,query_fields)
		 SELECT key,$2::numeric(20,0),is_base,value,query_fields::jsonb FROM unnest($1::bytea[],$3::boolean[],$4::bytea[],$5::text[]) AS version_rows(key,is_base,value,query_fields)`, keys, entry.Revision.String(), bases, versions, queryFields); err != nil {
			return err
		}
		keys, refs, kinds, bodies = keys[:0], refs[:0], kinds[:0], bodies[:0]
		deleted, counts, bases, versions = deleted[:0], counts[:0], bases[:0], versions[:0]
		queryFields = queryFields[:0]
		return nil
	}
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
		keys = append(keys, []byte(id))
		refs = append(refs, refRaw)
		kinds = append(kinds, kind)
		bodies = append(bodies, body)
		deleted = append(deleted, record == nil)
		counts = append(counts, decimal(count+1))
		bases = append(bases, version.Base)
		fields, err := historicalQueryFields(raw)
		if err != nil {
			return err
		}
		queryFields = append(queryFields, fields)
		versions = append(versions, raw)
		if len(keys) == publicationBatchRows {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
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
	if t.restore != nil {
		return zero, fmt.Errorf("prepared restore cannot commit a no-op")
	}
	if h.Events != nil {
		return zero, fmt.Errorf("postgres: no-op cannot contain an event plan")
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
	replayed := false
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
			replayed = true
		} else {
			if err = t.saveReceipt(ctx, scope, h, response, model.Changeset{}); err != nil {
				_ = t.finish()
				return zero, err
			}
		}
	}
	if !replayed {
		if err = t.checkCheckpointGuards(ctx); err != nil {
			return zero, err
		}
		if err = t.publishCheckpoints(ctx); err != nil {
			return zero, err
		}
	}
	if err = t.publish(ctx, h.IdempotencyKey); err != nil {
		return zero, err
	}
	return response, nil
}

var _ driver.NoopCommitter = (*transaction)(nil)
