package memory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type reader struct {
	mu          sync.RWMutex
	state       *state
	at          uint64
	incarnation string
	now         func() time.Time
}

func newReader(s *state, v driver.View, now func() time.Time) (*reader, error) {
	if v.Incarnation != s.meta.Incarnation {
		return nil, driver.ErrIncarnationChanged{From: v.Incarnation, To: s.meta.Incarnation}
	}
	if err := branchZero(v.Branch); err != nil {
		return nil, err
	}
	head, err := s.meta.Head.Uint64()
	if err != nil {
		return nil, err
	}
	at := head
	requested := s.meta.Head
	if v.At != nil {
		requested = *v.At
		at, err = requested.Uint64()
		if err != nil {
			return nil, err
		}
	}
	floor, err := s.meta.HistoryFloor.Uint64()
	if err != nil {
		return nil, err
	}
	if at < floor {
		return nil, driver.ErrTruncated{Floor: s.meta.HistoryFloor}
	}
	if at > head {
		return nil, driver.ErrFutureRevision{Requested: requested, Head: s.meta.Head}
	}
	return &reader{state: s, at: at, incarnation: s.meta.Incarnation, now: now}, nil
}

// Close drops the pinned state after in-flight reads finish. Snapshot returns
// copied identity metadata even after close; data operations require a live
// reader. Closing never changes committed storage.
func (r *reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = nil
	r.now = nil
	return nil
}

func (r *reader) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.state == nil {
		return fmt.Errorf("memory: reader closed")
	}
	return nil
}

func branchZero(b model.Branch) error {
	if err := b.Validate(); err != nil {
		return err
	}
	if !b.IsZero() {
		return fmt.Errorf("%w: memory supports branch zero", driver.ErrUnsupported)
	}
	return nil
}

func recordKey(ref driver.Ref) (string, error) {
	if err := branchZero(ref.Branch); err != nil {
		return "", err
	}
	return driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
}

func keyID(key driver.LockKey) string { value, _ := driver.EncodeLockKey(key); return value }

func (r *reader) Snapshot() driver.View {
	at := model.Natural64(r.at)
	return driver.View{Branch: model.Natural64(0), At: &at, Incarnation: r.incarnation}
}

func copyRecord(value driver.Record) driver.Record {
	value.Body = bytes.Clone(value.Body)
	return value
}

func (r *reader) Get(ctx context.Context, refs []driver.Ref) ([]driver.Record, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	result := make([]driver.Record, 0, len(refs))
	for _, ref := range refs {
		key, err := recordKey(ref)
		if err != nil {
			return nil, err
		}
		record, exists, err := r.recordAt(key)
		if err != nil {
			return nil, err
		}
		if exists {
			result = append(result, copyRecord(record))
		}
	}
	return result, nil
}

// Full bases bound forward replay to at most 31 JSON patches. A root replace
// is an RFC 6902 forward patch; current records retain their exact JSON bytes.
func (r *reader) recordAt(key string) (driver.Record, bool, error) {
	head, _ := r.state.meta.Head.Uint64()
	if r.at == head {
		record, ok := r.state.records[key]
		return record, ok, nil
	}
	versions := r.state.versions[key]
	last := sort.Search(len(versions), func(i int) bool { v, _ := versions[i].Revision.Uint64(); return v > r.at }) - 1
	if last < 0 {
		return driver.Record{}, false, nil
	}
	if versions[last].Deleted {
		return driver.Record{}, false, nil
	}
	first := last
	for first > 0 && !versions[first].Base {
		first--
	}
	var body []byte
	for _, version := range versions[first : last+1] {
		if version.Deleted {
			body = nil
			continue
		}
		if version.Base {
			body = version.Body
			continue
		}
		var patch []struct {
			Op    string          `json:"op"`
			Path  string          `json:"path"`
			Value json.RawMessage `json:"value"`
		}
		if err := json.Unmarshal(version.Patch, &patch); err != nil {
			return driver.Record{}, false, err
		}
		if len(patch) != 1 || patch[0].Op != "replace" || patch[0].Path != "" {
			return driver.Record{}, false, fmt.Errorf("memory: invalid stored forward patch")
		}
		body = patch[0].Value
	}
	return driver.Record{Ref: versions[last].Ref, Kind: r.state.kinds[key], Revision: versions[last].Revision, Body: body}, true, nil
}

func (r *reader) Blob(ctx context.Context, hash driver.Hash) ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	b, ok := r.state.blobs[hash]
	if !ok || b.revision > r.at {
		return nil, driver.ErrNotFound
	}
	return bytes.Clone(b.bytes), nil
}

func copyLock(value driver.LockState) driver.LockState {
	if value.Key.Ref != nil {
		v := *value.Key.Ref
		value.Key.Ref = &v
	}
	if value.Key.Derived != nil {
		v := *value.Key.Derived
		value.Key.Derived = &v
	}
	if value.Record != nil {
		v := copyRecord(*value.Record)
		value.Record = &v
	}
	if value.Derived != nil {
		v := value.Derived.Clone()
		value.Derived = &v
	}
	return value
}

func (r *reader) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	if err := driver.ValidateLockKeys(keys); err != nil {
		return nil, err
	}
	result := make([]driver.LockState, 0, len(keys))
	for _, key := range keys {
		if key.Kind == driver.LockRecord {
			if _, err := recordKey(*key.Ref); err != nil {
				return nil, err
			}
		}
		value := driver.LockState{Key: key, Revision: model.Natural64(0)}
		history := r.state.keyHistory[keyID(key)]
		index := sort.Search(len(history), func(i int) bool { n, _ := history[i].Revision.Uint64(); return n > r.at }) - 1
		if index >= 0 {
			value = history[index]
		}
		result = append(result, copyLock(value))
	}
	return result, nil
}

func filterDigest(filter map[string]any) (string, error) {
	encoded, err := json.Marshal(filter)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(encoded)
	return hex.EncodeToString(hash[:]), nil
}

func (r *reader) cursor(cursor *driver.Cursor, method, digest string) (uint64, error) {
	if cursor == nil {
		return 0, nil
	}
	if cursor.Incarnation != r.state.meta.Incarnation {
		return 0, driver.ErrIncarnationChanged{From: cursor.Incarnation, To: r.state.meta.Incarnation}
	}
	if cursor.Method != method || cursor.FilterDigest != digest || cursor.PrincipalDigest != "" || !cursor.OntologyRevision.IsZero() {
		return 0, fmt.Errorf("memory: cursor binding mismatch")
	}
	at, err := cursor.Revision.Uint64()
	if err != nil {
		return 0, err
	}
	if at != r.at {
		return 0, fmt.Errorf("memory: cursor snapshot mismatch")
	}
	return cursor.Index.Uint64()
}

func limitValue(limit int) (int, error) {
	if limit < 0 || limit > 10000 {
		return 0, fmt.Errorf("memory: limit must be between 0 and 10000")
	}
	if limit == 0 {
		return 100, nil
	}
	return limit, nil
}

func (r *reader) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(ctx); err != nil {
		return driver.Page[driver.Record]{}, err
	}
	if q.AsOf != nil && q.AsOf.String() != model.Natural64(r.at).String() {
		return driver.Page[driver.Record]{}, fmt.Errorf("memory: query as-of differs from reader snapshot")
	}
	for field := range q.Filter {
		switch field {
		case "kind", "id", "key", "type", "project", "status":
		default:
			return driver.Page[driver.Record]{}, fmt.Errorf("%w: query filter %q", driver.ErrUnsupported, field)
		}
		if _, ok := q.Filter[field].(string); !ok {
			return driver.Page[driver.Record]{}, fmt.Errorf("memory: equality filter %q requires string", field)
		}
	}
	digest, err := filterDigest(q.Filter)
	if err != nil {
		return driver.Page[driver.Record]{}, err
	}
	start, err := r.cursor(q.Cursor, "query", digest)
	if err != nil {
		return driver.Page[driver.Record]{}, err
	}
	limit, err := limitValue(q.Limit)
	if err != nil {
		return driver.Page[driver.Record]{}, err
	}
	keys := make([]string, 0, len(r.state.kinds))
	for key := range r.state.kinds {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	items := make([]driver.Record, 0)
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return driver.Page[driver.Record]{}, err
		}
		record, exists, err := r.recordAt(key)
		if err != nil {
			return driver.Page[driver.Record]{}, err
		}
		if !exists {
			continue
		}
		match, err := matches(record, q.Filter)
		if err != nil {
			return driver.Page[driver.Record]{}, err
		}
		if match {
			items = append(items, record)
		}
	}
	if start > uint64(len(items)) {
		return driver.Page[driver.Record]{}, fmt.Errorf("memory: cursor index out of range")
	}
	end := start + uint64(limit)
	if end > uint64(len(items)) {
		end = uint64(len(items))
	}
	page := driver.Page[driver.Record]{Items: make([]driver.Record, 0, end-start), AsOf: model.Natural64(r.at)}
	for _, record := range items[start:end] {
		page.Items = append(page.Items, copyRecord(record))
	}
	if end < uint64(len(items)) {
		page.NextCursor = &driver.Cursor{Incarnation: r.state.meta.Incarnation, Revision: page.AsOf, Index: model.Natural64(end), Method: "query", FilterDigest: digest}
	}
	return page, nil
}

func matches(record driver.Record, filter map[string]any) (bool, error) {
	var body map[string]json.RawMessage
	for field, want := range filter {
		var value any
		switch field {
		case "kind":
			value = record.Kind
		case "id":
			value = record.Ref.ID
		case "key":
			value = record.Ref.Key
		default:
			if body == nil {
				if err := json.Unmarshal(record.Body, &body); err != nil {
					return false, err
				}
			}
			raw, exists := body[field]
			if !exists {
				return false, nil
			}
			if err := json.Unmarshal(raw, &value); err != nil {
				return false, err
			}
		}
		if !reflect.DeepEqual(value, want) {
			return false, nil
		}
	}
	return true, nil
}

func (r *reader) Versions(ctx context.Context, ref driver.Ref, req driver.PageReq) (driver.Page[driver.Version], error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(ctx); err != nil {
		return driver.Page[driver.Version]{}, err
	}
	key, err := recordKey(ref)
	if err != nil {
		return driver.Page[driver.Version]{}, err
	}
	start, err := r.cursor(req.Cursor, "versions", key)
	if err != nil {
		return driver.Page[driver.Version]{}, err
	}
	limit, err := limitValue(req.Limit)
	if err != nil {
		return driver.Page[driver.Version]{}, err
	}
	versions := r.state.versions[key]
	count := sort.Search(len(versions), func(i int) bool { value, _ := versions[i].Revision.Uint64(); return value > r.at })
	if start > uint64(count) {
		return driver.Page[driver.Version]{}, fmt.Errorf("memory: version cursor out of range")
	}
	end := start + uint64(limit)
	if end > uint64(count) {
		end = uint64(count)
	}
	page := driver.Page[driver.Version]{Items: make([]driver.Version, 0, end-start), AsOf: model.Natural64(r.at)}
	for _, version := range versions[start:end] {
		version.Body = bytes.Clone(version.Body)
		version.Patch = bytes.Clone(version.Patch)
		version.Principal, err = cloneJSON(version.Principal)
		if err != nil {
			return driver.Page[driver.Version]{}, err
		}
		page.Items = append(page.Items, version)
	}
	if end < uint64(count) {
		page.NextCursor = &driver.Cursor{Incarnation: r.state.meta.Incarnation, Revision: page.AsOf, Index: model.Natural64(end), Method: "versions", FilterDigest: key}
	}
	return page, nil
}

func cloneJSON[T any](value T) (T, error) {
	var copy T
	encoded, err := json.Marshal(value)
	if err != nil {
		return copy, err
	}
	err = json.Unmarshal(encoded, &copy)
	return copy, err
}

func (r *reader) Changesets(ctx context.Context, after *driver.Cursor, limit int) ([]model.Changeset, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	max, err := limitValue(limit)
	if err != nil {
		return nil, err
	}
	var revision uint64
	if after != nil {
		if after.Incarnation != r.state.meta.Incarnation {
			return nil, driver.ErrIncarnationChanged{From: after.Incarnation, To: r.state.meta.Incarnation}
		}
		if !after.Index.IsZero() || after.Index.Validate() != nil {
			return nil, fmt.Errorf("memory: changeset cursor index must be zero")
		}
		if after.Method != "" && after.Method != "changesets" {
			return nil, fmt.Errorf("memory: changeset cursor method mismatch")
		}
		if after.FilterDigest != "" || after.PrincipalDigest != "" || !after.OntologyRevision.IsZero() {
			return nil, fmt.Errorf("memory: changeset cursor binding mismatch")
		}
		revision, err = after.Revision.Uint64()
		if err != nil {
			return nil, err
		}
	}
	floor, _ := r.state.meta.HistoryFloor.Uint64()
	if revision < floor {
		return nil, driver.ErrTruncated{Floor: r.state.meta.HistoryFloor}
	}
	if revision > r.at {
		return nil, fmt.Errorf("memory: changeset cursor is beyond snapshot")
	}
	result := make([]model.Changeset, 0)
	for _, entry := range r.state.log {
		value, _ := entry.Revision.Uint64()
		if value <= revision {
			continue
		}
		if value > r.at {
			break
		}
		copy, err := cloneJSON(entry)
		if err != nil {
			return nil, err
		}
		result = append(result, copy)
		if len(result) == max {
			break
		}
	}
	return result, nil
}

func normalizedScope(principal, agent, method, key string) scope {
	if !strings.HasPrefix(principal, "anonymous:") {
		agent = ""
	}
	return scope{principal: principal, agent: agent, method: method, key: key}
}

func (r *reader) IdempotencyByKey(ctx context.Context, principal, agent, method, key string) (*driver.IdempotencyRecord, error) {
	return r.readReceipt(ctx, principal, agent, method, key, false)
}

func (r *reader) ReadSourceRecoveryReceipt(ctx context.Context, principal, agent, method, key string) (*driver.IdempotencyRecord, error) {
	return r.readReceipt(ctx, principal, agent, method, key, driver.RetainSourceImportReceipt(ctx, method))
}

func (r *reader) readReceipt(ctx context.Context, principal, agent, method, key string, retainExpired bool) (*driver.IdempotencyRecord, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	entry, ok := r.state.replays[normalizedScope(principal, agent, method, key)]
	if !ok || (!retainExpired && !r.now().Before(entry.at.Add(driver.IdempotencyWindow))) {
		return nil, nil
	}
	revision, _ := entry.value.Commit.Revision.Uint64()
	if revision > r.at {
		return nil, nil
	}
	copy, err := copyIdempotency(entry.value)
	if err != nil {
		return nil, err
	}
	return &copy, nil
}

func (r *reader) ChangesetByKey(ctx context.Context, principal, agent, method, key string) (*model.Changeset, error) {
	entry, err := r.IdempotencyByKey(ctx, principal, agent, method, key)
	if err != nil || entry == nil || entry.Changeset.Kind == "" {
		return nil, err
	}
	return &entry.Changeset, nil
}
