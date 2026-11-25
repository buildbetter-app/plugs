package memory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type heldLock struct {
	writer  *transaction
	readers map[*transaction]bool
}
type transaction struct {
	store                               *Store
	lease                               driver.ActiveLease
	base                                *state
	snapshotAt                          model.Revision
	incarnation                         string
	restore                             *driver.Meta
	offlineHistory                      *driver.PreparedOfflineHistory
	stream                              *historyStreamStage
	nativeRestore                       *driver.NativeRestoreProfile
	nativeAllocators                    bool
	importRestore                       *driver.RestoreImportProfile
	sourcePlaneImport                   *driver.SourcePlaneImportPublication
	closed, locked, hasHead, committing bool
	acquired                            driver.LockSet
	records                             map[string]*driver.Record
	derived                             map[string]driver.LockState
	selectionAnnotated                  bool
	blobs                               map[driver.Hash][]byte
	bumps                               map[string]driver.LockKey
	allocators                          map[string]driver.OfflineAllocator
	parentCounters                      map[string]driver.ParentCounter
	allocated                           map[string]bool
	checkpointGuards                    map[string]uint64
	checkpointWrites                    map[string]driver.Checkpoint
	sourceDeclarations                  map[string]sourceDeclaration
	stagedParents                       map[string][]string
	stagedIncoming                      map[sourceLinkBucket][]string
}

func (t *transaction) check(ctx context.Context) error {
	if err := t.store.check(ctx); err != nil {
		return err
	}
	if t.committing {
		return fmt.Errorf("memory: transaction commit in progress")
	}
	if err := t.store.checkLease(t.lease); err != nil {
		return err
	}
	if t.closed {
		return driver.ErrTxClosed
	}
	if t.incarnation != t.store.state.meta.Incarnation {
		return driver.ErrIncarnationChanged{From: t.incarnation, To: t.store.state.meta.Incarnation}
	}
	return nil
}

// checkCommit is used only by a commit that has temporarily released Store.mu
// while its immutable publication plan is prepared. The committing transaction
// is otherwise unavailable to callers until the final publish or cleanup.
func (t *transaction) checkCommit(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if t.store.closed {
		return fmt.Errorf("memory: store closed")
	}
	if err := t.store.checkLease(t.lease); err != nil {
		return err
	}
	if t.closed {
		return driver.ErrTxClosed
	}
	if t.incarnation != t.store.state.meta.Incarnation {
		return driver.ErrIncarnationChanged{From: t.incarnation, To: t.store.state.meta.Incarnation}
	}
	return nil
}

func (t *transaction) finish() {
	if t.closed {
		return
	}
	if t.stream != nil {
		_ = t.stream.close()
	}
	s := t.store
	for _, request := range t.acquired {
		key := keyID(request.Key)
		held := s.locks[key]
		if held == nil {
			continue
		}
		if held.writer == t {
			held.writer = nil
		}
		delete(held.readers, t)
		if held.writer == nil && len(held.readers) == 0 {
			delete(s.locks, key)
		}
	}
	if s.head == t {
		s.head = nil
	}
	t.closed = true
	t.committing = false
	delete(s.transactions, t)
	t.base = nil
	t.acquired = nil
	t.records = nil
	t.derived = nil
	t.blobs = nil
	t.bumps = nil
	t.allocators = nil
	t.parentCounters = nil
	t.allocated = nil
	t.sourceDeclarations = nil
	t.stagedParents = nil
	t.stagedIncoming = nil
	s.signal()
}

// Close is cleanup, never an implicit commit. Rollback deliberately releases
// resources even when the original request context has been cancelled.
func (t *transaction) Close() error { return t.Rollback(context.Background()) }

func (t *transaction) Rollback(ctx context.Context) error {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	// Cleanup must still happen when the caller's context has been cancelled.
	t.finish()
	return nil
}

func (t *transaction) Lock(ctx context.Context, requests []driver.LockRequest) ([]driver.LockState, error) {
	if err := driver.ValidateLockSet(requests); err != nil {
		return nil, err
	}
	for _, request := range requests {
		if request.Key.Kind == driver.LockRecord {
			if _, err := recordKey(*request.Key.Ref); err != nil {
				return nil, err
			}
		}
	}
	s := t.store
	for {
		s.mu.Lock()
		if err := t.check(ctx); err != nil {
			s.mu.Unlock()
			return nil, err
		}
		if t.locked || t.hasHead {
			s.mu.Unlock()
			return nil, fmt.Errorf("memory: acquire the complete lock set once")
		}
		selected := make(driver.LockSet, 0, len(requests))
		blocked := false
		for _, request := range requests {
			held := s.locks[keyID(request.Key)]
			conflict := held != nil && (held.writer != nil || (request.Mode != driver.LockShare && len(held.readers) > 0))
			if conflict {
				if request.Mode == driver.LockSkipLocked {
					continue
				}
				blocked = true
				break
			}
			selected = append(selected, request)
		}
		if blocked {
			changed := s.notify
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-changed:
				continue
			}
		}
		states := make([]driver.LockState, 0, len(selected))
		for _, request := range selected {
			// Copy pointer-bearing keys so callers cannot retarget held locks.
			request.Key = copyLock(driver.LockState{Key: request.Key}).Key
			t.acquired = append(t.acquired, request)
			key := keyID(request.Key)
			held := s.locks[key]
			if held == nil {
				held = &heldLock{readers: make(map[*transaction]bool)}
				s.locks[key] = held
			}
			if request.Mode == driver.LockShare {
				held.readers[t] = true
			} else {
				held.writer = t
			}
			value, exists := s.state.keys[key]
			if !exists {
				value = driver.LockState{Key: request.Key, Revision: model.Natural64(0)}
			}
			states = append(states, copyLock(value))
		}
		t.locked = true
		t.base = s.state
		t.snapshotAt = s.state.meta.Head
		s.mu.Unlock()
		return states, nil
	}
}

func (t *transaction) Head(ctx context.Context) (model.Revision, error) {
	s := t.store
	for {
		s.mu.Lock()
		if err := t.check(ctx); err != nil {
			s.mu.Unlock()
			return "", err
		}
		if !t.locked {
			s.mu.Unlock()
			return "", fmt.Errorf("memory: Head requires the complete lock set")
		}
		if s.head == nil || s.head == t {
			s.head = t
			t.hasHead = true
			t.base = s.state
			t.snapshotAt = s.state.meta.Head
			head := s.state.meta.Head
			s.mu.Unlock()
			return head, nil
		}
		changed := s.notify
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-changed:
		}
	}
}

func (t *transaction) writable(key driver.LockKey) error {
	if !t.locked {
		return fmt.Errorf("memory: write requires locks")
	}
	want := keyID(key)
	// ValidateLockSet and selection preserve the canonical global key order.
	i := sort.Search(len(t.acquired), func(i int) bool { return keyID(t.acquired[i].Key) >= want })
	if i < len(t.acquired) && keyID(t.acquired[i].Key) == want && t.acquired[i].Mode != driver.LockShare {
		return nil
	}
	return fmt.Errorf("memory: missing update lock for %s", want)
}

func (t *transaction) Put(ctx context.Context, record driver.Record) error {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	key, err := t.validateSourcePut(record)
	if err != nil {
		return err
	}
	if record.Kind == "bead" && (hierarchyReady(t.base.hierarchy) || sourceLinksReady(t.base.sourceLinks)) {
		return fmt.Errorf("%w: certified bead publication requires typed source metadata", driver.ErrUnsupported)
	}
	t.stageSourceRecord(key, record, nil)
	return nil
}

func (t *transaction) Delete(ctx context.Context, ref driver.Ref) error {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	key, err := recordKey(ref)
	if err != nil {
		return err
	}
	if err := t.writable(driver.LockKey{Kind: driver.LockRecord, Ref: &ref}); err != nil {
		return err
	}
	value, staged := t.records[key]
	if (!staged && t.base.records[key].Kind == "") || (staged && value == nil) {
		return driver.ErrNotFound
	}
	kind := t.base.kinds[key]
	if value != nil {
		kind = value.Kind
	}
	if kind == "bead" && (hierarchyReady(t.base.hierarchy) || sourceLinksReady(t.base.sourceLinks)) {
		if err := t.writable(driver.GraphBlockingSentinelLock); err != nil {
			return err
		}
	}
	t.replaceSourceDeclaration(key, nil)
	t.records[key] = nil
	return nil
}

func (t *transaction) PutBlob(ctx context.Context, data []byte) (driver.Hash, error) {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return "", err
	}
	if !t.locked {
		return "", fmt.Errorf("memory: blob write requires complete lock set")
	}
	hash := sha256.Sum256(data)
	id := driver.Hash(hex.EncodeToString(hash[:]))
	t.blobs[id] = bytes.Clone(data)
	return id, nil
}

func (t *transaction) PutDerived(ctx context.Context, table driver.DerivedTable, del, add []driver.DerivedRow) error {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	// Validate the whole call before staging any row.
	if err := (driver.DerivedRowKey{Table: table, Key: "validation"}).Validate(); err != nil {
		return err
	}
	for _, rows := range [][]driver.DerivedRow{del, add} {
		for _, row := range rows {
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
	annotated := t.selectionAnnotated
	for _, row := range add {
		annotated = annotated || row.Index != nil
	}
	proposed := t.derived
	if annotated {
		proposed = make(map[string]driver.LockState, len(t.derived)+len(add)+len(del))
		for id, value := range t.derived {
			proposed[id] = value
		}
	}
	for _, row := range del {
		key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: row.Key}}
		proposed[keyID(key)] = driver.LockState{Key: key, Revision: model.Natural64(0)}
	}
	for _, row := range add {
		key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: row.Key}}
		copy := row.Clone()
		proposed[keyID(key)] = driver.LockState{Key: key, Derived: &copy, Present: true, Revision: model.Natural64(0)}
	}
	if annotated {
		if err := t.validateSelectionWrites(proposed); err != nil {
			return err
		}
	}
	t.derived = proposed
	t.selectionAnnotated = annotated
	return nil
}

func (t *transaction) bump(ctx context.Context, keys []driver.LockKey) error {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
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
		t.bumps[keyID(key)] = key
	}
	return nil
}

func (t *transaction) BumpSentinels(ctx context.Context, sentinels []driver.Sentinel) error {
	keys := make([]driver.LockKey, len(sentinels))
	for i, value := range sentinels {
		keys[i] = driver.LockKey{Kind: driver.LockSentinel, Sentinel: value}
	}
	return t.bump(ctx, keys)
}

func (t *transaction) BumpReadFacts(ctx context.Context, facts []driver.ReadFact) error {
	keys := make([]driver.LockKey, len(facts))
	for i, value := range facts {
		keys[i] = driver.LockKey{Kind: driver.LockReadFact, Fact: value}
	}
	return t.bump(ctx, keys)
}

func (t *transaction) Commit(ctx context.Context, header driver.ChangesetHeader) (model.Changeset, error) {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return model.Changeset{}, err
	}
	if t.offlineHistory != nil || t.stream != nil {
		return model.Changeset{}, fmt.Errorf("prepared history requires CommitOfflineHistory")
	}
	if !t.hasHead || s.head != t {
		return model.Changeset{}, fmt.Errorf("memory: commit requires Head")
	}
	if err := driver.ValidateHeaderLocks(header.LockKeys, t.acquired); err != nil {
		return model.Changeset{}, err
	}
	if err := branchZero(header.Branch); err != nil {
		return model.Changeset{}, err
	}
	expected := s.state.meta.Incarnation
	if header.Kind == model.ChangesetRestore && t.restore == nil {
		return model.Changeset{}, fmt.Errorf("%w: restore requires the offline capability", driver.ErrUnsupported)
	}
	if t.restore != nil {
		if err := s.restoreEmpty(); err != nil {
			return model.Changeset{}, err
		}
		validate := driver.ValidateOfflineRestoreHeader
		if t.nativeRestore != nil {
			if !t.nativeAllocators {
				return model.Changeset{}, driver.NewNativeAllocatorSnapshotError(driver.NativeAllocatorBindingRequired, nil)
			}
			validate = t.nativeRestore.ValidateHeader
		}
		if t.importRestore != nil {
			validate = t.importRestore.ValidateHeader
		}
		if err := validate(header); err != nil {
			return model.Changeset{}, err
		}
		if t.importRestore != nil {
			for _, record := range t.records {
				if record != nil {
					if err := t.importRestore.ValidateRecord(*record, header.Time); err != nil {
						return model.Changeset{}, err
					}
				}
			}
		}
		if t.sourcePlaneImport != nil {
			if err := t.sourcePlaneImport.Validate(header, t.checkpointWrites, t.records); err != nil {
				return model.Changeset{}, err
			}
		}
		expected = t.restore.Incarnation
	}
	if header.Incarnation != expected {
		return model.Changeset{}, driver.ErrIncarnationChanged{From: header.Incarnation, To: s.state.meta.Incarnation}
	}
	principal, err := driver.PrincipalKey(header.Principal)
	if err != nil {
		return model.Changeset{}, err
	}
	switch header.Kind {
	case model.ChangesetWrite, model.ChangesetDerived, model.ChangesetRevert, model.ChangesetMigration, model.ChangesetRedact, model.ChangesetRestore:
	default:
		return model.Changeset{}, fmt.Errorf("memory: unknown changeset kind")
	}
	if header.ResponsePending && header.IdempotencyKey == "" {
		return model.Changeset{}, fmt.Errorf("memory: pending response requires an idempotency key")
	}
	key := normalizedScope(principal, header.AgentLabel, header.Method, header.IdempotencyKey)
	if header.IdempotencyKey != "" {
		if err := validateReplayScope(header, principal); err != nil {
			return model.Changeset{}, err
		}
		if previous, exists := s.state.replays[key]; exists && s.now().Before(previous.at.Add(driver.IdempotencyWindow)) {
			if previous.value.RequestHash != header.RequestHash {
				if t.importRestore != nil {
					return model.Changeset{}, driver.ErrRestoreBindingMismatch{Field: "requestHash"}
				}
				return model.Changeset{}, driver.ErrConflict
			}
			if previous.value.Changeset.Kind == "" {
				// This scope completed without a graph commit. Callers replay its
				// Commit through IdempotencyByKey or CommitNoop, never invent a log.
				if t.importRestore != nil {
					return model.Changeset{}, driver.ErrRestoreBindingMismatch{Field: "receipt.response"}
				}
				return model.Changeset{}, driver.ErrConflict
			}
			copy, err := cloneJSON(previous.value.Changeset)
			if err != nil {
				return model.Changeset{}, err
			}
			t.finish()
			return copy, nil
		}
	}
	if err := t.checkCheckpointGuards(); err != nil {
		return model.Changeset{}, err
	}
	if err := t.validateChanges(header.Changes); err != nil {
		return model.Changeset{}, err
	}
	if err := t.validatePredicates(); err != nil {
		return model.Changeset{}, err
	}
	if t.selectionAnnotated {
		if err := t.validateSelectionWrites(t.derived); err != nil {
			return model.Changeset{}, err
		}
	}
	head, err := s.state.meta.Head.Uint64()
	if err != nil {
		return model.Changeset{}, err
	}
	if head == math.MaxUint64 {
		return model.Changeset{}, fmt.Errorf("memory: revision exhausted")
	}
	revision := model.Natural64(head + 1)
	when := header.Time
	if when.IsZero() {
		when = s.now().UTC()
	}
	entry := model.Changeset{Revision: revision, Branch: model.Natural64(0), Incarnation: header.Incarnation, Kind: header.Kind, Time: when, Principal: header.Principal, Chain: header.Chain, IdempotencyKey: header.IdempotencyKey, Mutators: header.Mutators, Changes: header.Changes, Events: header.Events}
	// Validate the complete plan against the actual changes and allocated
	// revision before preparing any graph, log or receipt publication. Nil
	// retains the legacy ChangeRecord index space; an empty plan is invalid.
	if err := entry.ValidateEvents(); err != nil {
		return model.Changeset{}, fmt.Errorf("memory: event plan: %w", err)
	}
	entry.Events = model.CloneEventDescriptors(entry.Events)
	entry, err = cloneJSON(entry)
	if err != nil {
		return model.Changeset{}, err
	}
	responseInput := header.Response
	if responseInput.Results == nil {
		responseInput.Results = []model.OpResult{}
	}
	if responseInput.Warnings == nil {
		responseInput.Warnings = []model.Warning{}
	}
	response, err := cloneJSON(responseInput)
	if err != nil {
		return model.Changeset{}, err
	}
	response.Revision = revision
	response, err = cloneJSON(response)
	if err != nil {
		return model.Changeset{}, err
	}
	next := s.state.fork()
	for prefix, allocator := range t.allocators {
		next.allocators[prefix] = allocator.Clone()
	}
	if len(t.parentCounters) > 0 && next.parentCounters == nil {
		next.parentCounters = make(map[string]driver.ParentCounter)
	}
	for key, counter := range t.parentCounters {
		next.parentCounters[key] = counter
	}
	for id, record := range t.records {
		ref := t.recordRef(id)
		version := driver.Version{Ref: ref, Revision: revision, CreatedAt: when, Principal: entry.Principal, Deleted: record == nil}
		if record == nil {
			delete(next.records, id)
		} else {
			copy := copyRecord(*record)
			copy.Revision = revision
			next.records[id] = copy
			next.kinds[id] = copy.Kind
			previous := next.versions[id]
			// A tombstone may occupy a scheduled base slot. Start a new
			// lifetime with a full image so replay never crosses that gap.
			version.Base = len(previous)%driver.VersionBaseEvery == 0 || len(previous) > 0 && previous[len(previous)-1].Deleted
			if version.Base {
				version.Body = bytes.Clone(copy.Body)
			} else {
				version.Patch, err = json.Marshal([]map[string]any{{"op": "replace", "path": "", "value": json.RawMessage(copy.Body)}})
				if err != nil {
					return model.Changeset{}, err
				}
			}
		}
		next.versions[id] = append(append([]driver.Version(nil), next.versions[id]...), version)
		lock := driver.LockState{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Revision: revision, Present: record != nil}
		if record != nil {
			v := next.records[id]
			lock.Record = &v
		}
		next.publishKey(id, lock)
	}
	for id, value := range t.derived {
		value = copyLock(value)
		value.Revision = revision
		if value.Derived != nil && value.Derived.Index != nil && t.selectionSourceChanged(value.Derived.Index.Source.Key) {
			value.Derived.Index.Source.Revision = revision
		}
		next.publishKey(id, value)
	}
	next.publishDerivedKeys(t.derived)
	for id, key := range t.bumps {
		next.publishKey(id, driver.LockState{Key: key, Revision: revision, Present: true})
	}
	for hash, value := range t.blobs {
		if _, exists := next.blobs[hash]; !exists {
			next.blobs[hash] = blob{bytes: bytes.Clone(value), revision: head + 1}
		}
	}
	next.meta.Head = revision
	if t.restore != nil {
		next.meta = *t.restore
	}
	if err := t.publishSourceIndexes(next); err != nil {
		return model.Changeset{}, err
	}
	next.log = append(append([]model.Changeset(nil), next.log...), entry)
	if header.IdempotencyKey != "" {
		next.replays[key] = replay{value: driver.IdempotencyRecord{AttemptID: header.AttemptID, RequestHash: header.RequestHash, Commit: response, Changeset: entry, ResponsePending: header.ResponsePending}, at: s.now()}
	}
	// Allocation and validation are complete. Epoch and publication share this
	// mutex, so a takeover cannot interleave with the final fenced commit.
	// Cancellation during validation/copying must also publish nothing.
	if err := t.check(ctx); err != nil {
		return model.Changeset{}, err
	}
	if err := next.publishAllocatorImage(s.state, t.restore != nil); err != nil {
		return model.Changeset{}, err
	}
	checkpoints := t.prepareCheckpointPublication()
	if s.beforeCommitHook != nil {
		s.beforeCommitHook()
	}
	if err := t.check(ctx); err != nil {
		return model.Changeset{}, err
	}
	publishCtx := context.Background()
	if t.restore != nil {
		publishCtx = ctx
	}
	if err := s.withLeasePublicationContext(publishCtx, t.lease, func() {
		s.state = next
		s.checkpoints = checkpoints
		t.finish()
	}); err != nil {
		return model.Changeset{}, err
	}
	return cloneJSON(entry)
}

func (s *state) publishKey(id string, value driver.LockState) {
	if value.Key.Kind == driver.LockRecord && s.kinds[id] == "bead" {
		if parent, number := driver.ChildIDOrder(id); parent != "" {
			order := parent + "\x00" + number + "\x00" + id
			s.childIDHistory = childInsert(s.childIDHistory, order, id)
			if value.Present {
				s.childIDs = childInsert(s.childIDs, order, id)
			} else {
				s.childIDs = childRemove(s.childIDs, order)
			}
		}
	}
	s.keys[id] = value
	s.keyHistory[id] = append(append([]driver.LockState(nil), s.keyHistory[id]...), value)
}

func (t *transaction) recordRef(id string) driver.Ref {
	i := sort.Search(len(t.acquired), func(i int) bool { return keyID(t.acquired[i].Key) >= id })
	if i < len(t.acquired) && keyID(t.acquired[i].Key) == id && t.acquired[i].Key.Kind == driver.LockRecord {
		return *t.acquired[i].Key.Ref
	}
	panic("validated record lost its update lock")
}

func (t *transaction) validateChanges(changes []model.ChangeRecord) error {
	seen := make(map[string]bool)
	for _, change := range changes {
		switch change.Op {
		case model.OpCreate, model.OpPatch, model.OpReparent, model.OpLink, model.OpUnlink, model.OpComment, model.OpAttach, model.OpDelete, model.OpSetMeta, model.OpClose:
		default:
			return fmt.Errorf("memory: unknown change operation")
		}
		id, err := recordKey(change.Ref)
		if err != nil {
			return err
		}
		if _, staged := t.records[id]; !staged || seen[id] {
			return fmt.Errorf("memory: changes must describe each staged record exactly once")
		}
		seen[id] = true
		if (t.records[id] == nil) != (change.Op == model.OpDelete) {
			return fmt.Errorf("memory: deletion and change operation disagree")
		}
		previous := t.store.state.keys[id].Revision
		if change.FromRevision.String() != previous.String() {
			return driver.ErrConflict
		}
	}
	if len(seen) != len(t.records) {
		return fmt.Errorf("memory: undeclared record write")
	}
	return nil
}

// Opaque row values do not encode affected predicate scopes. Core selects the
// exact scopes; this structural check proves required families and atomic bumps,
// rather than claiming to verify the semantic coverage of every affected row.
func (t *transaction) validatePredicates() error {
	for _, request := range t.acquired {
		if request.Key.Kind == driver.LockSentinel && request.Mode != driver.LockShare {
			if _, bumped := t.bumps[keyID(request.Key)]; !bumped {
				return fmt.Errorf("memory: update sentinel must be bumped")
			}
		}
	}
	for id, record := range t.records {
		kind := t.base.kinds[id]
		if record != nil {
			kind = record.Kind
		}
		if kind != "bead" {
			continue
		}
		ref := t.recordRef(id)
		if !ref.IsID() {
			return fmt.Errorf("memory: bead writes require a resolved id")
		}
		sentinel, err := driver.NewBeadSentinel(ref.ID)
		if err != nil {
			return err
		}
		if _, bumped := t.bumps[keyID(driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})]; !bumped {
			return fmt.Errorf("memory: bead write requires its exact bead sentinel bump")
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
			return fmt.Errorf("memory: derived table %s requires a %s predicate bump", value.Key.Derived.Table, family)
		}
	}
	return nil
}
