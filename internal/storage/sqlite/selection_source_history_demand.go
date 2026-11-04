package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// publishDemandSelectionSourceHistory is the demand boundary for derived
// source history. Record observations remain eager because records are the
// public source image. A derived key is demanded only when an already
// validated selection annotation names it as its source. Ready and the four
// selection projections are still published by their existing callers; this
// helper only removes their otherwise-unused thin source observations.
//
// A cold derived source is read from the exact transaction snapshot (the
// key_history row at t.at, before this commit). The source row is then seeded
// at next in the same fenced transaction as the annotation. A missing
// key_history row proves only the never-present state; it is never replaced by
// a current-image lookup. Malformed JSON or an identity mismatch is a native
// refusal and is returned before any source-history statement executes.
func (t *transaction) publishDemandSelectionSourceHistory(ctx context.Context, c selectionPublicationExecutor, incarnation, next string) (err error) {
	if err = ctx.Err(); err != nil {
		return err
	}
	changed := make(map[string]struct{}, len(t.records)+len(t.derived))
	observations := make(map[string]selectionSourceObservation, len(t.records))
	demanded := make(map[string]struct{})
	demandedSources := make(map[string]driver.SelectionSource)
	commitRevision, parseErr := parseRev(next)
	if parseErr != nil {
		return parseErr
	}
	// Match the commit-local rebinding that publishSelection will use below.
	// This keeps the staged-source proof and the persisted annotation metadata
	// on the same H+1 token without changing admission validation.
	boundDerived := t.reboundSelectionSources(commitRevision)

	// Records retain their existing eager source history, including explicit
	// tombstones. Their source revision is this commit's revision.
	for id, record := range t.records {
		changed[id] = struct{}{}
		observation := selectionSourceObservation{key: id, revision: next}
		if record != nil {
			observation.present = 1
			observation.digest = driver.SelectionDigest(record.Body)
		}
		observations[id] = observation
	}

	// Every derived mutation closes any prior open source interval. Only a
	// derived key named by a validated annotation is inserted again. This
	// includes staged tombstones and empty values; presence is never inferred
	// from a row's value bytes.
	for id, state := range boundDerived {
		if state.Key.Derived != nil {
			changed[id] = struct{}{}
		}
		if !state.Present || state.Derived == nil || state.Derived.Index == nil {
			continue
		}
		source := state.Derived.Index.Source.Key
		if source.Kind == driver.LockDerived {
			id := keyID(source)
			demanded[id] = struct{}{}
			want := state.Derived.Index.Source
			if previous, found := demandedSources[id]; found &&
				(previous.Revision.String() != want.Revision.String() || previous.Present != want.Present || previous.Digest != want.Digest) {
				return fmt.Errorf("%w: SQLite demanded source observations disagree", driver.ErrUnsupported)
			}
			demandedSources[id] = want
		}
	}

	// Resolve demanded derived sources before touching selection_source_history.
	// A source changed in this transaction is proven by its staged image. A
	// cold source is proven by the pinned key_history image at t.at, and an
	// absent key is represented as an explicit zero-revision tombstone.
	reused := make(map[string]struct{})
	demandedKeys := make([]string, 0, len(demanded))
	for id := range demanded {
		demandedKeys = append(demandedKeys, id)
	}
	sort.Strings(demandedKeys)
	for _, id := range demandedKeys {
		if state, staged := t.derived[id]; staged && state.Key.Derived != nil {
			query, queryOK := c.(selectionSourceHistoryQueryer)
			if !queryOK {
				return fmt.Errorf("%w: SQLite demand history requires the held query connection", driver.ErrUnsupported)
			}
			if err = demandSelectionSourceNoOverlap(ctx, query, incarnation, id); err != nil {
				return err
			}
			observation := selectionSourceObservation{key: id, revision: next}
			if state.Present {
				if state.Derived == nil {
					return fmt.Errorf("%w: SQLite demanded derived source image missing", driver.ErrUnsupported)
				}
				if err = validateDemandedDerivedImage(state); err != nil {
					return err
				}
				observation.present = 1
				observation.digest = driver.SelectionDigest(state.Derived.Value)
			} else if state.Derived != nil {
				return fmt.Errorf("%w: SQLite demanded tombstone carries a derived image", driver.ErrUnsupported)
			}
			if err = verifyDemandedSelectionSource(observation, demandedSources[id]); err != nil {
				return err
			}
			if err = ctx.Err(); err != nil {
				return err
			}
			observations[id] = observation
			continue
		}
		query, queryOK := c.(selectionSourceHistoryQueryer)
		if !queryOK {
			return fmt.Errorf("%w: SQLite demand history requires the held query connection", driver.ErrUnsupported)
		}
		if t.reader == nil {
			return fmt.Errorf("%w: SQLite demand history requires the pinned transaction reader", driver.ErrUnsupported)
		}
		observation, loadErr := loadDemandedSelectionSource(ctx, query, demandedSources[id].Key, t.at)
		if loadErr != nil {
			return loadErr
		}
		if err = verifyDemandedSelectionSource(observation, demandedSources[id]); err != nil {
			return err
		}
		reuse, checkErr := demandSelectionSourceCurrent(ctx, query, incarnation, observation)
		if checkErr != nil {
			return checkErr
		}
		if reuse {
			reused[id] = struct{}{}
			continue
		}
		if observation.revision == "" {
			return fmt.Errorf("%w: SQLite demanded source revision missing", driver.ErrUnsupported)
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		observations[id] = observation
	}

	publicationKeys := make(map[string]struct{}, len(changed)+len(observations))
	for id := range changed {
		publicationKeys[id] = struct{}{}
	}
	for id := range observations {
		if _, skip := reused[id]; !skip {
			publicationKeys[id] = struct{}{}
		}
	}
	keys := make([]string, 0, len(publicationKeys))
	for id := range publicationKeys {
		keys = append(keys, id)
	}
	sort.Strings(keys)

	// Keep the existing close/insert order within each bounded source batch.
	// In particular an eager record insert error must precede the next close
	// preparation. Sparse arms can have fewer than 128 rows; cache by arity.
	// All writes still share Commit's fenced SQL transaction.
	var closeStatement, insertStatement keyPublicationStatement
	closeStatements := make(map[int]keyPublicationStatement)
	insertStatements := make(map[int]keyPublicationStatement)
	var opened []keyPublicationStatement
	defer func() {
		for i := len(opened) - 1; i >= 0; i-- {
			closeKeyPublicationStatement(opened[i], &err)
		}
	}()
	for start := 0; start < len(keys); start += selectionSourceHistoryBatch {
		end := min(start+selectionSourceHistoryBatch, len(keys))
		batch := keys[start:end]
		closeArgs := []any{next, incarnation, revKey(0)}
		insertArgs := make([]any, 0, 7*len(batch))
		for _, id := range batch {
			if _, close := changed[id]; close {
				closeArgs = append(closeArgs, id)
			}
			if observation, insert := observations[id]; insert {
				insertArgs = append(insertArgs, incarnation, revKey(0), id, next,
					observation.revision, observation.present, observation.digest)
			}
		}
		if count := len(closeArgs) - 3; count != 0 {
			closeStatement = closeStatements[count]
			if closeStatement == nil {
				closeStatement, err = prepareKeyPublication(ctx, c, selectionSourceHistoryCloseSQL(count))
				if err != nil {
					return err
				}
				opened = append(opened, closeStatement)
				closeStatements[count] = closeStatement
			}
			if _, err = closeStatement.ExecContext(ctx, closeArgs...); err != nil {
				return err
			}
		}
		if count := len(insertArgs) / 7; count != 0 {
			insertStatement = insertStatements[count]
			if insertStatement == nil {
				insertStatement, err = prepareKeyPublication(ctx, c, selectionSourceHistoryInsertSQL(count))
				if err != nil {
					return err
				}
				opened = append(opened, insertStatement)
				insertStatements[count] = insertStatement
			}
			if _, err = insertStatement.ExecContext(ctx, insertArgs...); err != nil {
				return err
			}
		}
	}
	return nil
}

type selectionSourceHistoryQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadDemandedSelectionSource(ctx context.Context, c selectionSourceHistoryQueryer, key driver.LockKey, at uint64) (selectionSourceObservation, error) {
	id := keyID(key)
	observation := selectionSourceObservation{key: id}
	var rowKey, rowRevision string
	var raw []byte
	err := c.QueryRowContext(ctx, `SELECT key,rev,data FROM key_history WHERE key=? AND rev<=? ORDER BY rev DESC LIMIT 1`, id, revKey(at)).Scan(&rowKey, &rowRevision, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		// No key_history row is the exact never-present observation. This is a
		// proof of absence at the pinned token, not a substitution from records
		// or a current derived table.
		observation.revision = revKey(0)
		return observation, nil
	}
	if err != nil {
		return observation, err
	}
	if err := ctx.Err(); err != nil {
		return observation, err
	}
	var state driver.LockState
	if err := json.Unmarshal(raw, &state); err != nil {
		return observation, err
	}
	// SQL key is the lossless identity. JSON replaces invalid UTF-8 in string
	// fields, so comparing decoded keyID(state.Key) to a raw opaque key would
	// reject values which the existing driver admits. Compare the decoded
	// envelope only to the same JSON encoding of the already-admitted key.
	encodedKey, err := json.Marshal(key)
	if err != nil {
		return observation, err
	}
	var historyKey driver.LockKey
	if err := json.Unmarshal(encodedKey, &historyKey); err != nil {
		return observation, err
	}
	if rowKey != id || keyID(state.Key) != keyID(historyKey) || state.Key.Kind != driver.LockDerived || state.Key.Derived == nil {
		return observation, fmt.Errorf("%w: SQLite demanded source key identity mismatch", driver.ErrUnsupported)
	}
	storedRevision, err := parseRev(rowRevision)
	if err != nil {
		return observation, fmt.Errorf("%w: SQLite demanded key history revision malformed: %v", driver.ErrUnsupported, err)
	}
	stateRevision, err := state.Revision.Uint64()
	if err != nil || stateRevision != mustUint(storedRevision) {
		return observation, fmt.Errorf("%w: SQLite demanded key history revision disagrees with source state", driver.ErrUnsupported)
	}
	var revisionErr error
	observation.revision, revisionErr = demandedSourceRevisionKey(state.Revision)
	if revisionErr != nil {
		return observation, revisionErr
	}
	if state.Present {
		if state.Derived == nil {
			return observation, fmt.Errorf("%w: SQLite demanded derived source image missing", driver.ErrUnsupported)
		}
		if err := validateDemandedDerivedImage(state); err != nil {
			return observation, err
		}
		observation.present = 1
		observation.digest = driver.SelectionDigest(state.Derived.Value)
	} else if state.Derived != nil {
		return observation, fmt.Errorf("%w: SQLite demanded tombstone carries a derived image", driver.ErrUnsupported)
	}
	return observation, nil
}

func validateDemandedDerivedImage(state driver.LockState) error {
	if state.Key.Derived == nil || state.Derived == nil || state.Derived.Key != state.Key.Derived.Key {
		return fmt.Errorf("%w: SQLite demanded source row identity mismatch", driver.ErrUnsupported)
	}
	// The source's own Index is not a transitive proof. Existing binders
	// consume the opaque Value; they do not revalidate that Index's namespace,
	// branch, value envelope, or source metadata.
	return nil
}

func verifyDemandedSelectionSource(observation selectionSourceObservation, want driver.SelectionSource) error {
	wantRevision, err := demandedSourceRevisionKey(want.Revision)
	if err != nil {
		return err
	}
	wantPresent := 0
	if want.Present {
		wantPresent = 1
	}
	if observation.revision != wantRevision || observation.present != wantPresent || observation.digest != want.Digest {
		return fmt.Errorf("%w: SQLite demanded source is not the admitted pinned image", driver.ErrSelectionUnavailable)
	}
	return nil
}

func demandedSourceRevisionKey(revision model.Revision) (string, error) {
	value, err := revision.Uint64()
	if err != nil {
		return "", fmt.Errorf("%w: SQLite demanded source revision malformed: %v", driver.ErrUnsupported, err)
	}
	return revKey(value), nil
}

func demandSelectionSourceCurrent(ctx context.Context, c selectionSourceHistoryQueryer, incarnation string, observation selectionSourceObservation) (bool, error) {
	if err := demandSelectionSourceNoOverlap(ctx, c, incarnation, observation.key); err != nil {
		return false, err
	}
	var count, present int
	var revision, digest string
	err := c.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MAX(source_revision),''),COALESCE(MAX(source_present),0),COALESCE(MAX(source_digest),'')
		FROM selection_source_history
		WHERE incarnation=? AND branch=? AND source_key=? AND end_revision IS NULL`,
		incarnation, revKey(0), observation.key).Scan(&count, &revision, &present, &digest)
	if err != nil {
		return false, err
	}
	if count == 0 {
		return false, nil
	}
	if count != 1 {
		return false, fmt.Errorf("%w: SQLite demanded source history has duplicate open intervals", driver.ErrUnsupported)
	}
	if present != 0 && present != 1 {
		return false, fmt.Errorf("%w: SQLite demanded source history presence malformed", driver.ErrUnsupported)
	}
	current, parseErr := parseRev(revision)
	if parseErr != nil {
		return false, fmt.Errorf("%w: SQLite demanded source history revision malformed: %v", driver.ErrUnsupported, parseErr)
	}
	if present != observation.present || revKey(mustUint(current)) != observation.revision || digest != observation.digest {
		return false, fmt.Errorf("%w: SQLite demanded source history disagrees with pinned source", driver.ErrSelectionUnavailable)
	}
	return true, nil
}

func demandSelectionSourceNoOverlap(ctx context.Context, c selectionSourceHistoryQueryer, incarnation, sourceKey string) error {
	var overlap int
	maxRevision := revKey(^uint64(0))
	err := c.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1
		FROM selection_source_history a
		JOIN selection_source_history b
		  ON a.incarnation=b.incarnation AND a.branch=b.branch AND a.source_key=b.source_key
		 AND a.start_revision < b.start_revision
		WHERE a.incarnation=? AND a.branch=? AND a.source_key=?
		  AND a.start_revision < COALESCE(b.end_revision,?)
		  AND b.start_revision < COALESCE(a.end_revision,?))`,
		incarnation, revKey(0), sourceKey, maxRevision, maxRevision).Scan(&overlap)
	if err != nil {
		return err
	}
	if overlap != 0 {
		return fmt.Errorf("%w: SQLite demanded source history intervals overlap", driver.ErrUnsupported)
	}
	return nil
}
