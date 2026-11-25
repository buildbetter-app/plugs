package sqlite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sourceindex"
)

type transaction struct {
	*reader
	lease             *activeLease
	locked, hasHead   bool
	restore           *driver.Meta
	offlineHistory    *driver.PreparedOfflineHistory
	stream            *historyStreamStage
	nativeRestore     *driver.NativeRestoreProfile
	nativeAllocators  bool
	importRestore     *driver.RestoreImportProfile
	sourcePlaneImport *driver.SourcePlaneImportPublication
	acquired          driver.LockSet
	files             []*os.File
	guards            []guardHandle
	done              chan struct{}
	records           map[string]*driver.Record
	derived           map[string]driver.LockState
	blobs             map[driver.Hash][]byte
	bumps             map[string]driver.LockKey
	allocators        map[string]driver.OfflineAllocator
	parentCounters    map[string]driver.ParentCounter
	allocated         map[string]bool

	nativeRestoreBlobs driver.NativeRestoreBlobSource

	checkpointGuards          map[string]uint64
	checkpointWrites          map[string]driver.Checkpoint
	sourceOverlay             sourceindex.Overlay
	sourceIdentities          map[string]driver.Record
	sourceCoverage            sourceindex.CoveragePair
	sourceCoverageHead        model.Revision
	sourceCoverageIncarnation string
	sourceCoverageValid       bool
}

func (s *Store) Write(ctx context.Context, l driver.ActiveLease) (driver.WriteTx, error) {
	lease, err := s.leaseFor(ctx, l)
	if err != nil {
		return nil, err
	}
	m, err := s.Meta(ctx)
	if err != nil {
		return nil, err
	}
	t := &transaction{reader: &reader{store: s}, lease: lease, done: make(chan struct{}), records: make(map[string]*driver.Record), derived: make(map[string]driver.LockState), blobs: make(map[driver.Hash][]byte), bumps: make(map[string]driver.LockKey), allocators: make(map[string]driver.OfflineAllocator)}
	t.reader.owner = t
	if err = t.pin(ctx, driver.View{Incarnation: m.Incarnation}); err != nil {
		return nil, err
	}
	if err = s.track(t); err != nil {
		_ = t.tx.Rollback()
		return nil, err
	}
	// Lease expiry must release stale guard/head resources even when a caller
	// disappears. Close remains rollback, and later operations return ErrFenced.
	go func() {
		select {
		case <-lease.Lost():
			_ = t.Close()
		case <-t.done:
		}
	}()
	return t, nil
}
func (t *transaction) finish() {
	if t.closed {
		return
	}
	if t.stream != nil {
		_ = t.stream.close()
	}
	t.closed = true
	_ = t.reader.closeLegacyBlobs()
	_ = t.tx.Rollback()
	t.tx = nil
	releaseGuards(t.guards)
	t.guards = nil
	for _, f := range t.files {
		unlockFile(f)
	}
	t.files = nil
	t.clearSourceOverlay()
	t.records = nil
	t.derived = nil
	t.blobs = nil
	t.nativeRestoreBlobs = nil
	t.bumps = nil
	t.allocators = nil
	t.parentCounters = nil
	t.allocated = nil
	t.store.untrack(t)
	close(t.done)
}
func (t *transaction) Close() error { return t.Rollback(context.Background()) }
func (t *transaction) Rollback(context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.finish()
	return nil
}
func (t *transaction) Lock(ctx context.Context, requests []driver.LockRequest) ([]driver.LockState, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := driver.ValidateLockSet(requests); err != nil {
		return nil, err
	}
	for _, r := range requests {
		if r.Key.Kind == driver.LockRecord {
			if _, err := recordKey(*r.Key.Ref); err != nil {
				return nil, err
			}
		}
	}
	if t.locked || t.hasHead {
		return nil, fmt.Errorf("sqlite: acquire the complete lock set once")
	}
	stripes, indexes := planGuardStripes(requests)
	for {
		if err := t.check(ctx); err != nil {
			return nil, err
		}
		guards := make([]guardHandle, 0, len(stripes))
		var held [guardStripeCount]bool
		blocked := false
		var acquireErr error
		for _, stripe := range stripes {
			guard, ok, err := tryGuardLock(guardStripePath(t.store.lockDir, stripe.stripe), stripe.share)
			if err != nil {
				acquireErr = err
				break
			}
			if !ok {
				if stripe.skipLocked {
					continue
				}
				blocked = true
				break
			}
			held[stripe.stripe] = true
			guards = append(guards, guard)
		}
		if acquireErr != nil || blocked {
			releaseGuards(guards)
			if acquireErr != nil {
				return nil, acquireErr
			}
			timer := time.NewTimer(time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-t.lease.Lost():
				timer.Stop()
				return nil, driver.ErrFenced{ExpectedEpoch: t.lease.epoch}
			case <-timer.C:
			}
			continue
		}
		selected := make(driver.LockSet, 0, len(requests))
		for i, request := range requests {
			if held[indexes[i]] {
				request.Key = copyKey(request.Key)
				selected = append(selected, request)
			}
		}
		// Every requested non-skipped key is held before reading any state. Failed
		// acquisition retains no partial lock set; different processes obey this too.
		if err := t.pin(ctx, driver.View{Incarnation: t.meta.Incarnation}); err != nil {
			releaseGuards(guards)
			return nil, err
		}
		states, err := t.observeLockSet(ctx, selected)
		if err == nil {
			// Acquisition and observation can outlast the lease. Never publish
			// guards that the lease watcher is already waiting to release.
			err = t.check(ctx)
		}
		if err != nil {
			releaseGuards(guards)
			return nil, err
		}
		t.acquired = selected
		t.guards = guards
		t.locked = true
		return states, nil
	}
}

// observeLockSet runs only after Lock has acquired the complete selected set
// and pinned the current head. Batches avoid preparing a history/record lookup
// for each guard. Raw SQL string bindings preserve opaque key bytes; JSON
// string encoding would replace invalid UTF-8 accepted by the driver keys.
// The scalar reader keeps its historical reconstruction and public behavior.
func (t *transaction) observeLockSet(ctx context.Context, requests driver.LockSet) ([]driver.LockState, error) {
	keys := make([]string, len(requests))
	for i, request := range requests {
		keys[i] = keyID(request.Key)
	}
	states := make([]driver.LockState, len(requests))
	const batchSize = 256
	for start := 0; start < len(requests); start += batchSize {
		end := min(start+batchSize, len(requests))
		var query strings.Builder
		query.WriteString("WITH requested(ordinal, key) AS (VALUES ")
		args := make([]any, 0, end-start+1)
		for i := start; i < end; i++ {
			if i != start {
				query.WriteByte(',')
			}
			fmt.Fprintf(&query, "(%d,?)", i)
			args = append(args, keys[i])
		}
		query.WriteString(`) SELECT requested.ordinal,
			(SELECT data FROM key_history WHERE key=requested.key AND rev<=? ORDER BY rev DESC LIMIT 1),
			(SELECT data FROM records WHERE key=requested.key)
			FROM requested`)
		args = append(args, revKey(t.at))
		err := func() error {
			rows, err := t.tx.QueryContext(ctx, query.String(), args...)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var i int
				var history, record []byte
				if err := rows.Scan(&i, &history, &record); err != nil {
					return err
				}
				key := requests[i].Key
				state := driver.LockState{Key: copyKey(key), Revision: model.Natural64(0)}
				if history != nil {
					if err := json.Unmarshal(history, &state); err != nil {
						return err
					}
				}
				if key.Kind == driver.LockRecord {
					if staged, ok := t.records[keys[i]]; ok {
						state.Present = staged != nil
						if staged != nil {
							value := copyRecord(*staged)
							state.Record = &value
						}
					} else {
						state.Present = record != nil
						if record != nil {
							var value driver.Record
							if err := json.Unmarshal(record, &value); err != nil {
								return err
							}
							state.Record = &value
						}
					}
				}
				if staged, ok := t.derived[keys[i]]; ok {
					state.Present = staged.Present
					state.Derived = nil
					if staged.Derived != nil {
						value := *staged.Derived
						value.Value = bytes.Clone(value.Value)
						state.Derived = &value
					}
				}
				states[i] = state
			}
			return rows.Err()
		}()
		if err != nil {
			return nil, err
		}
	}
	return states, nil
}

func (t *transaction) Head(ctx context.Context) (model.Revision, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return "", err
	}
	if !t.locked {
		return "", fmt.Errorf("sqlite: Head requires complete lock set")
	}
	if t.hasHead {
		return t.meta.Head, nil
	}
	f, err := t.store.fileLock(ctx, "head", false, t.lease.Lost())
	if err != nil {
		return "", err
	}
	if err = t.pin(ctx, driver.View{Incarnation: t.meta.Incarnation}); err != nil {
		unlockFile(f)
		return "", err
	}
	t.files = append(t.files, f)
	t.hasHead = true
	return t.meta.Head, nil
}
func (t *transaction) writable(key driver.LockKey) error {
	if !t.locked {
		return fmt.Errorf("sqlite: mutation requires complete lock set")
	}
	id := keyID(key)
	// Lock keeps a sorted subset even when skip-locked keys are omitted.
	i := sort.Search(len(t.acquired), func(i int) bool { return keyID(t.acquired[i].Key) >= id })
	if i < len(t.acquired) && keyID(t.acquired[i].Key) == id && t.acquired[i].Mode != driver.LockShare {
		return nil
	}
	return fmt.Errorf("sqlite: missing update lock for %s", id)
}
func (t *transaction) Put(ctx context.Context, record driver.Record) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	return t.putSourceRecord(ctx, record, nil)
}
func (t *transaction) Delete(ctx context.Context, ref driver.Ref) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	id, err := recordKey(ref)
	if err != nil {
		return err
	}
	if err = t.writable(driver.LockKey{Kind: driver.LockRecord, Ref: &ref}); err != nil {
		return err
	}
	record, found, err := t.recordAt(ctx, id)
	if err != nil {
		return err
	}
	if !found {
		return driver.ErrNotFound
	}
	if record.Kind == "bead" {
		coverage, err := t.currentSourceCoverage(ctx)
		if err != nil {
			return err
		}
		if coverage.Parent.Ready || coverage.Links.Ready {
			if err = t.writable(driver.GraphBlockingSentinelLock); err != nil {
				return err
			}
		}
	}
	if t.sourceIdentities == nil {
		t.sourceIdentities = make(map[string]driver.Record)
	}
	t.sourceIdentities[id] = driver.Record{Ref: ref, Kind: record.Kind}
	t.records[id] = nil
	t.sourceOverlay.Replace(id, nil)
	return nil
}
func (t *transaction) PutBlob(ctx context.Context, data []byte) (driver.Hash, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return "", err
	}
	if !t.locked {
		return "", fmt.Errorf("sqlite: blob requires complete lock set")
	}
	hash := sha256.Sum256(data)
	id := driver.Hash(hex.EncodeToString(hash[:]))
	t.blobs[id] = bytes.Clone(data)
	return id, nil
}
func (t *transaction) PutDerived(ctx context.Context, table driver.DerivedTable, del, add []driver.DerivedRow) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	if err := (driver.DerivedRowKey{Table: table, Key: "validation"}).Validate(); err != nil {
		return err
	}
	for _, rows := range [][]driver.DerivedRow{del, add} {
		for _, row := range rows {
			if row.Index != nil {
				return fmt.Errorf("%w: SQLite selection indexes are not implemented", driver.ErrUnsupported)
			}
			key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: row.Key}}
			if err := key.Validate(); err != nil {
				return err
			}
			if err := t.writable(key); err != nil {
				return err
			}
		}
	}
	for _, row := range del {
		key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: row.Key}}
		t.derived[keyID(key)] = driver.LockState{Key: key, Revision: model.Natural64(0)}
	}
	for _, row := range add {
		key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: row.Key}}
		row.Value = bytes.Clone(row.Value)
		t.derived[keyID(key)] = driver.LockState{Key: key, Derived: &row, Present: true, Revision: model.Natural64(0)}
	}
	return nil
}
func (t *transaction) bump(ctx context.Context, keys []driver.LockKey) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	for _, key := range keys {
		if err := key.Validate(); err != nil {
			return err
		}
		if err := t.writable(key); err != nil {
			return err
		}
	}
	for _, key := range keys {
		t.bumps[keyID(key)] = copyKey(key)
	}
	return nil
}
func (t *transaction) BumpSentinels(ctx context.Context, values []driver.Sentinel) error {
	keys := make([]driver.LockKey, len(values))
	for i, v := range values {
		keys[i] = driver.LockKey{Kind: driver.LockSentinel, Sentinel: v}
	}
	return t.bump(ctx, keys)
}
func (t *transaction) BumpReadFacts(ctx context.Context, values []driver.ReadFact) error {
	keys := make([]driver.LockKey, len(values))
	for i, v := range values {
		keys[i] = driver.LockKey{Kind: driver.LockReadFact, Fact: v}
	}
	return t.bump(ctx, keys)
}
func (t *transaction) recordRef(id string) driver.Ref {
	// Lock retains the globally encoded-key-sorted acquired subset, including
	// mixed modes and skip-locked omissions. Use the same lower-bound lookup as
	// writable; recordRef deliberately accepts every lock mode.
	i := sort.Search(len(t.acquired), func(i int) bool {
		return keyID(t.acquired[i].Key) >= id
	})
	if i < len(t.acquired) && keyID(t.acquired[i].Key) == id && t.acquired[i].Key.Ref != nil {
		return *t.acquired[i].Key.Ref
	}
	panic("sqlite: record missing validated lock")
}
func (t *transaction) Commit(ctx context.Context, h driver.ChangesetHeader) (model.Changeset, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.offlineHistory != nil || t.stream != nil {
		return model.Changeset{}, fmt.Errorf("prepared history requires CommitOfflineHistory")
	}
	if t.nativeRestore != nil && !t.nativeAllocators {
		return model.Changeset{}, driver.NewNativeAllocatorSnapshotError(driver.NativeAllocatorBindingRequired, nil)
	}
	if err := t.validateHeader(ctx, h); err != nil {
		return model.Changeset{}, err
	}
	var result model.Changeset
	err := t.store.immediatePublication(ctx, t.lease, t.restore != nil, func(c *sql.Conn) error {
		if t.restore != nil {
			if err := restoreEmpty(ctx, c); err != nil {
				return err
			}
		}
		replay, err := t.replay(ctx, c, h)
		if err != nil {
			return err
		}
		if replay != nil {
			if replay.Changeset.Kind == "" {
				if t.importRestore != nil {
					return driver.ErrRestoreBindingMismatch{Field: "receipt.response"}
				}
				return driver.ErrConflict
			}
			result = replay.Changeset
			return nil
		}
		if err := t.checkCheckpointGuards(ctx, c); err != nil {
			return err
		}
		if err := t.publishCheckpoints(ctx, c); err != nil {
			return err
		}
		if err = t.validateChanges(ctx, h.Changes); err != nil {
			return err
		}
		if err = t.validatePredicates(ctx); err != nil {
			return err
		}
		meta, err := readMeta(ctx, c)
		if err != nil {
			return err
		}
		head := mustUint(meta.Head)
		if head == math.MaxUint64 {
			return fmt.Errorf("sqlite: revision exhausted")
		}
		revision := model.Natural64(head + 1)
		when := h.Time
		if when.IsZero() {
			when = t.store.now().UTC()
		}
		entry := model.Changeset{Revision: revision, Branch: model.Natural64(0), Incarnation: h.Incarnation, Kind: h.Kind, Time: when, Principal: h.Principal, Chain: h.Chain, IdempotencyKey: h.IdempotencyKey, Mutators: h.Mutators, Changes: h.Changes, Events: model.CloneEventDescriptors(h.Events)}
		// Validate the complete plan against the actual changes and allocated
		// revision before preparing any graph, log, or receipt publication. Nil
		// retains the legacy history discriminator; an empty plan is invalid.
		if err := entry.ValidateEvents(); err != nil {
			return fmt.Errorf("sqlite: event plan: %w", err)
		}
		data, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		// Decode now so the result never aliases header-owned slices or maps.
		if err = json.Unmarshal(data, &result); err != nil {
			return err
		}
		sourcePlan, err := t.prepareSourcePublication(ctx, c, meta, revision)
		if err != nil {
			return err
		}
		target := meta
		target.Head = revision
		if t.restore != nil {
			target = *t.restore
		}
		// Allocator journals, records, counters and source indexes all publish
		// inside this immediate transaction and roll back together on failure.
		if err = t.publishAllocatorJournal(ctx, c, target, t.restore != nil); err != nil {
			return err
		}
		for id, record := range t.records {
			ref := t.recordRef(id)
			v := driver.Version{Ref: ref, Revision: revision, CreatedAt: when, Principal: h.Principal, Deleted: record == nil}
			var count uint64
			if err = c.QueryRowContext(ctx, `SELECT COUNT(*) FROM versions WHERE key=?`, id).Scan(&count); err != nil {
				return err
			}
			// A tombstone is also a complete base when its cadence is due.
			v.Base = count%driver.VersionBaseEvery == 0
			if record != nil {
				var wasLive bool
				if err = c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records WHERE key=?)`, id).Scan(&wasLive); err != nil {
					return err
				}
				v.Base = v.Base || !wasLive
			}
			if record == nil {
				if _, err = c.ExecContext(ctx, `DELETE FROM records WHERE key=?`, id); err != nil {
					return err
				}
			} else {
				value := copyRecord(*record)
				value.Revision = revision
				recordData, err := json.Marshal(value)
				if err != nil {
					return err
				}
				if _, err = c.ExecContext(ctx, `INSERT INTO records(key,data) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET data=excluded.data`, id, recordData); err != nil {
					return err
				}
				if _, err = c.ExecContext(ctx, `INSERT OR IGNORE INTO objects(key,kind) VALUES(?,?)`, id, value.Kind); err != nil {
					return err
				}
				// Each recreated lifetime starts with a full base, in addition to
				// the regular every-32-version cadence.
				if v.Base {
					v.Body = bytes.Clone(value.Body)
				} else {
					v.Patch, err = json.Marshal([]map[string]any{{"op": "replace", "path": "", "value": json.RawMessage(value.Body)}})
					if err != nil {
						return err
					}
				}
			}
			versionData, err := json.Marshal(v)
			if err != nil {
				return err
			}
			if _, err = c.ExecContext(ctx, `INSERT INTO versions(key,rev,base,data) VALUES(?,?,?,?)`, id, revKey(head+1), v.Base, versionData); err != nil {
				return err
			}
			if err = populateHistoricalQueryFields(ctx, c, id, revKey(head+1), versionData); err != nil {
				return err
			}
			state := driver.LockState{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Revision: revision, Present: record != nil}
			if err = publishKey(ctx, c, state); err != nil {
				return err
			}
		}
		if err = sourcePlan.Publish(ctx, c); err != nil {
			return err
		}
		for _, v := range t.derived {
			v.Revision = revision
			if err = publishKey(ctx, c, v); err != nil {
				return err
			}
		}
		for _, key := range t.bumps {
			if err = publishKey(ctx, c, driver.LockState{Key: key, Revision: revision, Present: true}); err != nil {
				return err
			}
		}
		for prefix, allocator := range t.allocators {
			allocatorData, marshalErr := json.Marshal(allocator)
			if marshalErr != nil {
				return marshalErr
			}
			if _, err = c.ExecContext(ctx, `INSERT INTO allocator_states(prefix,data) VALUES(?,?) ON CONFLICT(prefix) DO UPDATE SET data=excluded.data`, prefix, allocatorData); err != nil {
				return err
			}
		}
		if err = t.publishParentCounters(ctx, c); err != nil {
			return err
		}
		if t.nativeRestoreBlobs != nil {
			if err := publishNativeRestoreBlobs(ctx, c, revKey(head+1), t.nativeRestoreBlobs); err != nil {
				return err
			}
		}
		for hash, blob := range t.blobs {
			if blob == nil {
				blob = []byte{}
			}
			if _, err = c.ExecContext(ctx, `INSERT OR IGNORE INTO blobs(hash,rev,data) VALUES(?,?,?)`, string(hash), revKey(head+1), blob); err != nil {
				return err
			}
		}
		if len(t.records) != 0 || t.restore != nil {
			if err = rebuildQueryOrder(ctx, c); err != nil {
				return err
			}
		}
		if _, err = c.ExecContext(ctx, `INSERT INTO changesets(rev,data) VALUES(?,?)`, revKey(head+1), data); err != nil {
			return err
		}
		if _, err = c.ExecContext(ctx, `UPDATE meta SET head=? WHERE singleton=1`, revKey(head+1)); err != nil {
			return err
		}
		if t.restore != nil {
			if _, err = c.ExecContext(ctx, `UPDATE meta SET store_id=?,incarnation=?,history_floor=? WHERE singleton=1`, string(t.restore.StoreID), t.restore.Incarnation, revKey(1)); err != nil {
				return err
			}
		}
		return t.store.saveReplay(ctx, c, h, revision, data)
	})
	if err == nil {
		t.finish()
		return result, nil
	}
	var uncertain commitError
	if errors.As(err, &uncertain) {
		t.finish()
		return model.Changeset{}, driver.ErrIndeterminate{
			IdempotencyKey: h.IdempotencyKey, RestoreInterrupted: t.restore != nil && uncertain.interrupted(),
		}
	}
	var fenced driver.ErrFenced
	if errors.As(err, &fenced) {
		t.finish()
	}
	return model.Changeset{}, err
}
func publishKey(ctx context.Context, c *sql.Conn, v driver.LockState) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = c.ExecContext(ctx, `INSERT INTO key_history(key,rev,data) VALUES(?,?,?)`, keyID(v.Key), revKey(mustUint(v.Revision)), data)
	return err
}

var _ driver.WriteTx = (*transaction)(nil)
