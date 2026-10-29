package memory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type heldLock struct {
	writer  *transaction
	readers map[*transaction]bool
}
type transaction struct {
	store                   *Store
	lease                   driver.ActiveLease
	base                    *state
	closed, locked, hasHead bool
	acquired                driver.LockSet
	records                 map[string]*driver.Record
	derived                 map[string]driver.LockState
	blobs                   map[driver.Hash][]byte
	bumps                   map[string]driver.LockKey
}

func (t *transaction) check(ctx context.Context) error {
	if err := t.store.check(ctx); err != nil {
		return err
	}
	if err := t.store.checkLease(t.lease); err != nil {
		return err
	}
	if t.closed {
		return driver.ErrTxClosed
	}
	return nil
}

func (t *transaction) finish() {
	if t.closed {
		return
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
	delete(s.transactions, t)
	t.records = nil
	t.derived = nil
	t.blobs = nil
	t.bumps = nil
	s.signal()
}

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
	for _, request := range t.acquired {
		if keyID(request.Key) == want && request.Mode != driver.LockShare {
			return nil
		}
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
	key, err := recordKey(record.Ref)
	if err != nil {
		return err
	}
	if err := t.writable(driver.LockKey{Kind: driver.LockRecord, Ref: &record.Ref}); err != nil {
		return err
	}
	if record.Kind == "" || !json.Valid(record.Body) {
		return fmt.Errorf("memory: record requires kind and valid JSON body")
	}
	if kind, exists := t.base.kinds[key]; exists && kind != record.Kind {
		return fmt.Errorf("memory: changing a record kind is unsupported")
	}
	if previous, ok := t.records[key]; ok && previous != nil && previous.Kind != record.Kind {
		return fmt.Errorf("memory: changing a staged record kind is unsupported")
	}
	record = copyRecord(record)
	record.Ref.Branch = model.Natural64(0)
	t.records[key] = &record
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
		copy := driver.DerivedRow{Key: row.Key, Value: bytes.Clone(row.Value)}
		t.derived[keyID(key)] = driver.LockState{Key: key, Derived: &copy, Present: true, Revision: model.Natural64(0)}
	}
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
	if !t.hasHead || s.head != t {
		return model.Changeset{}, fmt.Errorf("memory: commit requires Head")
	}
	if err := driver.ValidateHeaderLocks(header.LockKeys, t.acquired); err != nil {
		return model.Changeset{}, err
	}
	if err := branchZero(header.Branch); err != nil {
		return model.Changeset{}, err
	}
	if header.Incarnation != s.state.meta.Incarnation {
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
	key := normalizedScope(principal, header.AgentLabel, header.Method, header.IdempotencyKey)
	if header.IdempotencyKey != "" {
		if !canonicalKey(header.Method) || header.RequestHash == "" {
			return model.Changeset{}, fmt.Errorf("memory: idempotent write requires method and request hash")
		}
		if previous, exists := s.state.replays[key]; exists && s.now().Before(previous.at.Add(driver.IdempotencyWindow)) {
			if previous.value.RequestHash != header.RequestHash {
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
	if err := t.validateChanges(header.Changes); err != nil {
		return model.Changeset{}, err
	}
	if err := t.validatePredicates(); err != nil {
		return model.Changeset{}, err
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
	entry := model.Changeset{Revision: revision, Branch: model.Natural64(0), Incarnation: header.Incarnation, Kind: header.Kind, Time: when, Principal: header.Principal, Chain: header.Chain, IdempotencyKey: header.IdempotencyKey, Mutators: header.Mutators, Changes: header.Changes}
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
	for i := range response.Results {
		response.Results[i].Revision = revision
	}
	response, err = cloneJSON(response)
	if err != nil {
		return model.Changeset{}, err
	}
	next := s.state.fork()
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
			version.Base = len(next.versions[id])%driver.VersionBaseEvery == 0
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
		value.Revision = revision
		next.publishKey(id, value)
	}
	for id, key := range t.bumps {
		next.publishKey(id, driver.LockState{Key: key, Revision: revision, Present: true})
	}
	for hash, value := range t.blobs {
		if _, exists := next.blobs[hash]; !exists {
			next.blobs[hash] = blob{bytes: bytes.Clone(value), revision: head + 1}
		}
	}
	next.meta.Head = revision
	next.log = append(append([]model.Changeset(nil), next.log...), entry)
	if header.IdempotencyKey != "" {
		next.replays[key] = replay{value: driver.IdempotencyRecord{RequestHash: header.RequestHash, Commit: response, Changeset: entry}, at: s.now()}
	}
	// Allocation and validation are complete. Epoch and publication share this
	// mutex, so a takeover cannot interleave with the final fenced commit.
	if err := s.checkLease(t.lease); err != nil {
		return model.Changeset{}, err
	}
	s.state = next
	t.finish()
	return cloneJSON(entry)
}

func (s *state) publishKey(id string, value driver.LockState) {
	s.keys[id] = value
	s.keyHistory[id] = append(append([]driver.LockState(nil), s.keyHistory[id]...), value)
}

func (t *transaction) recordRef(id string) driver.Ref {
	for _, request := range t.acquired {
		if keyID(request.Key) == id {
			return *request.Key.Ref
		}
	}
	panic("validated record lost its update lock")
}

func (t *transaction) validateChanges(changes []model.ChangeRecord) error {
	seen := make(map[string]bool)
	for _, change := range changes {
		switch change.Op {
		case model.OpCreate, model.OpPatch, model.OpReparent, model.OpLink, model.OpUnlink, model.OpComment, model.OpAttach, model.OpDelete, model.OpSetMeta:
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
