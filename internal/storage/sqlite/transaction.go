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
	lease            *activeLease
	locked, hasHead  bool
	restore          *driver.Meta
	offlineHistory   *driver.PreparedOfflineHistory
	nativeRestore    *driver.NativeRestoreProfile
	nativeAllocators bool
	importRestore    *driver.RestoreImportProfile
	acquired         driver.LockSet
	// acquiredStates retains detached Ready images observed while the complete
	// lock set was acquired. Ready publication already declares those same
	// before-images; reusing this fenced observation avoids a second latest-row
	// read during Commit without trusting mutable states returned from Lock.
	acquiredStates  map[string]driver.LockState
	acquiredModes   map[string]driver.LockMode
	acquiredRecords map[string]driver.Ref
	files           []*os.File
	guards          []guardHandle
	done            chan struct{}
	records         map[string]*driver.Record
	// recordStates retains the first lock observation for each staged record.
	// A present=false, revision-zero observation proves that no version history
	// exists for a first lifetime, so Commit can avoid re-counting that row.
	// Existing and tombstoned rows keep the scalar COUNT(*) path.
	recordStates map[string]driver.LockState
	// recordKinds carries the lock-time objects.kind observation, including a
	// present=false marker, so staging can retain kind validation without a
	// second query for every record.
	recordKinds map[string]recordKindObservation
	// recordKindStmt preserves the scalar kind validation in source staging
	// while avoiding database/sql prepare/finalize work for every record.
	recordKindStmt *sql.Stmt
	// validationHistoryStmt keeps validateChanges scalar and ordered while
	// avoiding repeated prepare/finalize work for its revision-only lookup.
	validationHistoryStmt *sql.Stmt
	// Lock batches retain only unambiguous first-lifetime observations. The
	// cache is used for ordinary record-only writes; selection producers keep
	// their scalar observation count and ordering.
	firstLifetimeState map[string]driver.LockState
	reuseFirstLifetime bool
	derived            map[string]driver.LockState
	blobs              map[driver.Hash][]byte
	bumps              map[string]driver.LockKey
	allocators         map[string]driver.OfflineAllocator
	parentCounters     map[string]driver.ParentCounter
	allocated          map[string]bool
	selectionAnnotated bool
	// Reverse edges identify annotations affected by a later staged source
	// change. Complete validation still runs against the final overlay at Commit.
	selectionDependents map[string]map[string]bool
	selectionRecheck    map[string]bool
	readyPublication    *readyOrderPublicationState

	checkpointGuards          map[string]uint64
	checkpointWrites          map[string]driver.Checkpoint
	sourceOverlay             sourceindex.Overlay
	sourceIdentities          map[string]driver.Record
	sourceCoverage            sourceindex.CoveragePair
	sourceCoverageHead        model.Revision
	sourceCoverageIncarnation string
	sourceCoverageValid       bool
}

type recordKindObservation struct {
	kind    string
	present bool
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
	t := &transaction{reader: &reader{store: s}, lease: lease, done: make(chan struct{}), records: make(map[string]*driver.Record), recordStates: make(map[string]driver.LockState), recordKinds: make(map[string]recordKindObservation), derived: make(map[string]driver.LockState), blobs: make(map[driver.Hash][]byte), bumps: make(map[string]driver.LockKey), allocators: make(map[string]driver.OfflineAllocator)}
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
	t.closed = true
	if t.recordKindStmt != nil {
		_ = t.recordKindStmt.Close()
		t.recordKindStmt = nil
	}
	if t.validationHistoryStmt != nil {
		_ = t.validationHistoryStmt.Close()
		t.validationHistoryStmt = nil
	}
	if t.observeHistoryStmt != nil {
		_ = t.observeHistoryStmt.Close()
		t.observeHistoryStmt = nil
	}
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
	t.recordStates = nil
	t.recordKinds = nil
	t.acquiredModes = nil
	t.acquiredStates = nil
	t.acquiredRecords = nil
	t.firstLifetimeState = nil
	t.reuseFirstLifetime = false
	t.derived = nil
	t.blobs = nil
	t.bumps = nil
	t.allocators = nil
	t.parentCounters = nil
	t.allocated = nil
	t.selectionAnnotated = false
	t.selectionDependents = nil
	t.selectionRecheck = nil
	t.invalidateReadyOrderPublication()
	t.store.untrack(t)
	close(t.done)
}

func (t *transaction) recordKind(ctx context.Context, id string) (string, error) {
	if t.recordKindStmt == nil {
		statement, err := t.tx.PrepareContext(ctx, `SELECT kind FROM objects WHERE key=?`)
		if err != nil {
			return "", err
		}
		t.recordKindStmt = statement
	}
	var kind string
	err := t.recordKindStmt.QueryRowContext(ctx, id).Scan(&kind)
	return kind, err
}

func (t *transaction) recordRevision(ctx context.Context, id string) (model.Revision, error) {
	if t.validationHistoryStmt == nil {
		statement, err := t.tx.PrepareContext(ctx, `SELECT data FROM key_history WHERE key=? AND rev<=? ORDER BY rev DESC LIMIT 1`)
		if err != nil {
			return "", err
		}
		t.validationHistoryStmt = statement
	}
	var data []byte
	err := t.validationHistoryStmt.QueryRowContext(ctx, id, revKey(t.at)).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Natural64(0), nil
	}
	if err != nil {
		return "", err
	}
	var state driver.LockState
	if err := json.Unmarshal(data, &state); err != nil {
		return "", err
	}
	return state.Revision, nil
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
		t.reuseFirstLifetime = true
		for _, request := range selected {
			if request.Key.Kind == driver.LockDerived {
				t.reuseFirstLifetime = false
				break
			}
		}
		// The selected set is immutable after Lock. Cache its exact encoded
		// identities once, including modes and record refs, for the many
		// staged projection checks before Commit.
		modes := make(map[string]driver.LockMode, len(selected))
		records := make(map[string]driver.Ref)
		observed := make(map[string]driver.LockState)
		for _, request := range selected {
			id := keyID(request.Key)
			modes[id] = request.Mode
			if request.Key.Kind == driver.LockRecord && request.Key.Ref != nil {
				records[id] = *request.Key.Ref
			}
		}
		for _, state := range states {
			if state.Key.Kind != driver.LockDerived || state.Key.Derived == nil || state.Key.Derived.Table != driver.DerivedReadyState {
				continue
			}
			copy := state
			copy.Key = copyKey(state.Key)
			if state.Record != nil {
				record := copyRecord(*state.Record)
				copy.Record = &record
			}
			if state.Derived != nil {
				row := state.Derived.Clone()
				copy.Derived = &row
			}
			observed[keyID(copy.Key)] = copy
		}
		t.acquired = selected
		t.acquiredStates = observed
		t.acquiredModes = modes
		t.acquiredRecords = records
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
			(SELECT data FROM records WHERE key=requested.key),
			(SELECT kind FROM objects WHERE key=requested.key)
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
				var objectKind sql.NullString
				if err := rows.Scan(&i, &history, &record, &objectKind); err != nil {
					return err
				}
				key := requests[i].Key
				if key.Kind == driver.LockRecord {
					if t.recordKinds == nil {
						t.recordKinds = make(map[string]recordKindObservation)
					}
					t.recordKinds[keys[i]] = recordKindObservation{kind: objectKind.String, present: objectKind.Valid}
				}
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
					if !state.Present && state.Revision.IsZero() {
						if t.firstLifetimeState == nil {
							t.firstLifetimeState = make(map[string]driver.LockState)
						}
						t.firstLifetimeState[keys[i]] = state
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
	if mode, found := t.acquiredMode(id); found && mode != driver.LockShare {
		return nil
	}
	return fmt.Errorf("sqlite: missing update lock for %s", id)
}

func (t *transaction) acquiredMode(id string) (driver.LockMode, bool) {
	if t.acquiredModes != nil {
		mode, found := t.acquiredModes[id]
		return mode, found
	}
	// A few narrow tests construct a transaction from an acquired set without
	// calling Lock. Retain the canonical sorted-set lookup for those values.
	i := sort.Search(len(t.acquired), func(i int) bool { return keyID(t.acquired[i].Key) >= id })
	if i < len(t.acquired) && keyID(t.acquired[i].Key) == id {
		return t.acquired[i].Mode, true
	}
	return "", false
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
	t.selectionSourceStaged(id)
	t.invalidateReadyOrderPublication()
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
	return t.putDerived(ctx, table, del, add, false)
}

// PutSelectionDerived is the optional trusted typed-publication capability.
// Ordinary PutDerived keeps its existing logical-write contract and cannot
// supply annotations. Both entrypoints validate the complete batch before
// any staged row changes.
func (t *transaction) PutSelectionDerived(ctx context.Context, table driver.DerivedTable, del, add []driver.DerivedRow) error {
	return t.putDerived(ctx, table, del, add, true)
}

func (t *transaction) putDerived(ctx context.Context, table driver.DerivedTable, del, add []driver.DerivedRow, indexed bool) error {
	return t.putDerivedWithSourceStates(ctx, table, del, add, indexed, nil)
}

// PutSelectionDerivedWithSourceStates is used only by the ancestor projection
// publisher after it has observed the complete source lock set. It preserves
// the normal typed staging and validation rules while avoiding a second scalar
// history read for each annotation. A missing supplied state falls back to the
// ordinary observer, so this seam cannot broaden admission.
func (t *transaction) PutSelectionDerivedWithSourceStates(ctx context.Context, table driver.DerivedTable, del, add []driver.DerivedRow, sources []driver.LockState) error {
	observed := make(map[string]driver.LockState, len(sources))
	for _, state := range sources {
		if state.Key.Kind != driver.LockRecord || state.Key.Ref == nil {
			return fmt.Errorf("sqlite: ancestor source observation requires record keys")
		}
		observed[keyID(state.Key)] = state
	}
	return t.putDerivedWithSourceStates(ctx, table, del, add, true, observed)
}

func (t *transaction) putDerivedWithSourceStates(ctx context.Context, table driver.DerivedTable, del, add []driver.DerivedRow, indexed bool, sourceStates map[string]driver.LockState) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	if err := (driver.DerivedRowKey{Table: table, Key: "validation"}).Validate(); err != nil {
		return err
	}
	for which, rows := range [][]driver.DerivedRow{del, add} {
		for _, row := range rows {
			if which == 0 && row.Index != nil || !indexed && row.Index != nil || indexed && which == 1 && row.Index == nil {
				return fmt.Errorf("%w: invalid SQLite selection publication arm", driver.ErrUnsupported)
			}
			if err := row.Index.Validate(table, row); err != nil {
				return err
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
	proposed := make(map[string]driver.LockState, len(add)+len(del))
	annotated := t.selectionAnnotated
	for _, row := range del {
		key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: row.Key}}
		proposed[keyID(key)] = driver.LockState{Key: key, Revision: model.Natural64(0)}
	}
	for _, row := range add {
		key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: row.Key}}
		copy := row.Clone()
		proposed[keyID(key)] = driver.LockState{Key: key, Derived: &copy, Present: true, Revision: model.Natural64(0)}
		annotated = annotated || copy.Index != nil
	}
	observe := t.observe
	if len(sourceStates) != 0 {
		observe = func(_ context.Context, key driver.LockKey) (driver.LockState, error) {
			if state, ok := sourceStates[keyID(key)]; ok {
				state.Key = copyKey(key)
				if state.Record != nil {
					value := copyRecord(*state.Record)
					state.Record = &value
				}
				return state, nil
			}
			return t.observe(ctx, key)
		}
	}
	if err := t.stageSelectionDeltaObserved(ctx, proposed, annotated, observe); err != nil {
		return err
	}
	t.selectionAnnotated = annotated
	if len(proposed) != 0 {
		t.invalidateReadyOrderPublication()
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
		t.selectionSourceStaged(keyID(key))
	}
	if len(keys) != 0 {
		t.invalidateReadyOrderPublication()
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
	if t.acquiredRecords != nil {
		if ref, found := t.acquiredRecords[id]; found {
			return ref
		}
		panic("sqlite: record missing validated lock")
	}
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
	if t.offlineHistory != nil {
		return model.Changeset{}, fmt.Errorf("prepared history requires CommitOfflineHistory")
	}
	if t.nativeRestore != nil && !t.nativeAllocators {
		return model.Changeset{}, driver.NewNativeAllocatorSnapshotError(driver.NativeAllocatorBindingRequired, nil)
	}
	if err := t.validateHeader(ctx, h); err != nil {
		return model.Changeset{}, err
	}
	var result model.Changeset
	err := t.store.immediate(ctx, t.lease, func(c *sql.Conn) (err error) {
		// The record/history publication loop deliberately keeps one scalar
		// execution per logical row. Reusing the connection-bound statements
		// removes database/sql's implicit prepare/finalize work without changing
		// row order, trigger boundaries, or the first-error position.
		var statements recordPublicationStatements
		defer statements.close(&err)
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
		if t.selectionAnnotated {
			if err = t.validateSelectionWrites(ctx, t.derived); err != nil {
				return err
			}
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
		readyProof, err := t.prepareReadyOrderCommit(ctx, c, meta, revision)
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
			firstLifetime := false
			if record != nil {
				if initial, known := t.recordStates[id]; known {
					firstLifetime = !initial.Present && initial.Revision.IsZero()
				}
			}
			countVersions, prepareErr := statements.countVersions(ctx, c)
			if prepareErr != nil {
				return prepareErr
			}
			// Keep the statement preparation at the original first-use boundary;
			// only the result lookup is unnecessary for a proven first lifetime.
			if !firstLifetime {
				if err = countVersions.QueryRowContext(ctx, id).Scan(&count); err != nil {
					return err
				}
			}
			// A tombstone is also a complete base when its cadence is due.
			v.Base = firstLifetime || count%driver.VersionBaseEvery == 0
			// A scheduled base is complete regardless of whether this row already
			// exists. Avoid the existence probe in that arm; it cannot change the
			// result and new records are the common boundary-batch case. Retain the
			// probe for non-base updates, where a missing live row starts a lifetime
			// and must force a full image.
			if record != nil && !v.Base {
				var wasLive bool
				existsRecord, prepareErr := statements.existsRecord(ctx, c)
				if prepareErr != nil {
					return prepareErr
				}
				if err = existsRecord.QueryRowContext(ctx, id).Scan(&wasLive); err != nil {
					return err
				}
				v.Base = v.Base || !wasLive
			}
			if record == nil {
				deleteRecord, prepareErr := statements.deleteRecord(ctx, c)
				if prepareErr != nil {
					return prepareErr
				}
				if _, err = deleteRecord.ExecContext(ctx, id); err != nil {
					return err
				}
			} else {
				value := copyRecord(*record)
				value.Revision = revision
				recordData, err := json.Marshal(value)
				if err != nil {
					return err
				}
				upsertRecord, prepareErr := statements.upsertRecord(ctx, c)
				if prepareErr != nil {
					return prepareErr
				}
				if _, err = upsertRecord.ExecContext(ctx, id, recordData); err != nil {
					return err
				}
				insertObject, prepareErr := statements.insertObject(ctx, c)
				if prepareErr != nil {
					return prepareErr
				}
				if _, err = insertObject.ExecContext(ctx, id, value.Kind); err != nil {
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
			insertVersion, prepareErr := statements.insertVersion(ctx, c)
			if prepareErr != nil {
				return prepareErr
			}
			if _, err = insertVersion.ExecContext(ctx, id, revKey(head+1), v.Base, versionData); err != nil {
				return err
			}
			state := driver.LockState{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Revision: revision, Present: record != nil}
			if err = statements.publishRecordKey(ctx, c, state); err != nil {
				return err
			}
		}
		if err = sourcePlan.Publish(ctx, c); err != nil {
			return err
		}
		// Rebind changed source annotations only in this commit-local overlay.
		// If any later publication statement fails, t.derived remains bound to
		// the observed prepublication source token for a safe retry/rollback.
		derived := t.reboundSelectionSources(revision)
		if err = publishDerivedKeys(ctx, c, revision, derived); err != nil {
			return err
		}
		if err = t.publishSelection(ctx, c, meta, revision, derived, readyProof); err != nil {
			return err
		}
		if err = publishBumpKeys(ctx, c, revision, t.bumps); err != nil {
			return err
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
		for hash, blob := range t.blobs {
			if blob == nil {
				blob = []byte{}
			}
			if _, err = c.ExecContext(ctx, `INSERT OR IGNORE INTO blobs(hash,rev,data) VALUES(?,?,?)`, string(hash), revKey(head+1), blob); err != nil {
				return err
			}
		}
		if err = t.finishReadyOrderCommit(ctx, c, readyProof, target); err != nil {
			return err
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
		return model.Changeset{}, driver.ErrIndeterminate{IdempotencyKey: h.IdempotencyKey}
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
	if v.Key.Kind == driver.LockDerived {
		present, digest, err := derivedSourceMetadata(v)
		if err != nil {
			return err
		}
		_, err = c.ExecContext(ctx, `INSERT INTO key_history(key,rev,data,derived_source_present,derived_source_digest) VALUES(?,?,?,?,?)`, keyID(v.Key), revKey(mustUint(v.Revision)), data, present, digest)
		return err
	}
	_, err = c.ExecContext(ctx, `INSERT INTO key_history(key,rev,data) VALUES(?,?,?)`, keyID(v.Key), revKey(mustUint(v.Revision)), data)
	return err
}

// recordPublicationStatements keeps the record/history publication scalar.
// Each statement is prepared at its original first-use position and then
// reused for later rows on Commit's held connection. Lazy preparation retains
// the old native error precedence: a prepare failure occurs at the same
// logical arm, after the same earlier rows have been published in the SQL
// transaction. Closing an already-used statement is cleanup only and cannot
// hide an earlier prepare, execution, cancellation, or commit error.
type recordPublicationStatements struct {
	countVersionsStmt *sql.Stmt
	existsRecordStmt  *sql.Stmt
	deleteRecordStmt  *sql.Stmt
	upsertRecordStmt  *sql.Stmt
	insertObjectStmt  *sql.Stmt
	insertVersionStmt *sql.Stmt
	insertKeyStmt     *sql.Stmt
}

func prepareRecordPublicationStatement(ctx context.Context, c *sql.Conn, slot **sql.Stmt, query string) (*sql.Stmt, error) {
	if *slot != nil {
		return *slot, nil
	}
	statement, err := c.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	*slot = statement
	return statement, nil
}

func (s *recordPublicationStatements) countVersions(ctx context.Context, c *sql.Conn) (*sql.Stmt, error) {
	return prepareRecordPublicationStatement(ctx, c, &s.countVersionsStmt, `SELECT COUNT(*) FROM versions WHERE key=?`)
}

func (s *recordPublicationStatements) existsRecord(ctx context.Context, c *sql.Conn) (*sql.Stmt, error) {
	return prepareRecordPublicationStatement(ctx, c, &s.existsRecordStmt, `SELECT EXISTS(SELECT 1 FROM records WHERE key=?)`)
}

func (s *recordPublicationStatements) deleteRecord(ctx context.Context, c *sql.Conn) (*sql.Stmt, error) {
	return prepareRecordPublicationStatement(ctx, c, &s.deleteRecordStmt, `DELETE FROM records WHERE key=?`)
}

func (s *recordPublicationStatements) upsertRecord(ctx context.Context, c *sql.Conn) (*sql.Stmt, error) {
	return prepareRecordPublicationStatement(ctx, c, &s.upsertRecordStmt, `INSERT INTO records(key,data) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET data=excluded.data`)
}

func (s *recordPublicationStatements) insertObject(ctx context.Context, c *sql.Conn) (*sql.Stmt, error) {
	return prepareRecordPublicationStatement(ctx, c, &s.insertObjectStmt, `INSERT OR IGNORE INTO objects(key,kind) VALUES(?,?)`)
}

func (s *recordPublicationStatements) insertVersion(ctx context.Context, c *sql.Conn) (*sql.Stmt, error) {
	return prepareRecordPublicationStatement(ctx, c, &s.insertVersionStmt, `INSERT INTO versions(key,rev,base,data) VALUES(?,?,?,?)`)
}

func (s *recordPublicationStatements) insertKey(ctx context.Context, c *sql.Conn) (*sql.Stmt, error) {
	return prepareRecordPublicationStatement(ctx, c, &s.insertKeyStmt, `INSERT INTO key_history(key,rev,data) VALUES(?,?,?)`)
}

func (s *recordPublicationStatements) publishRecordKey(ctx context.Context, c *sql.Conn, v driver.LockState) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	statement, err := s.insertKey(ctx, c)
	if err != nil {
		return err
	}
	_, err = statement.ExecContext(ctx, keyID(v.Key), revKey(mustUint(v.Revision)), data)
	return err
}

func (s *recordPublicationStatements) close(prior *error) {
	for _, statement := range []*sql.Stmt{
		s.countVersionsStmt,
		s.existsRecordStmt,
		s.deleteRecordStmt,
		s.upsertRecordStmt,
		s.insertObjectStmt,
		s.insertVersionStmt,
		s.insertKeyStmt,
	} {
		if statement == nil {
			continue
		}
		if err := statement.Close(); err != nil && *prior == nil {
			*prior = err
		}
	}
}

var _ driver.WriteTx = (*transaction)(nil)
