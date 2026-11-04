package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (t *transaction) validateHeader(ctx context.Context, h driver.ChangesetHeader) error {
	if err := t.check(ctx); err != nil {
		return err
	}
	if !t.hasHead {
		return fmt.Errorf("sqlite: commit requires Head")
	}
	if h.ResponsePending && h.IdempotencyKey == "" {
		return fmt.Errorf("sqlite: pending response requires an idempotency key")
	}
	if err := driver.ValidateHeaderLocks(h.LockKeys, t.acquired); err != nil {
		return err
	}
	if err := branchZero(h.Branch); err != nil {
		return err
	}
	expected := t.meta.Incarnation
	if h.Kind == model.ChangesetRestore && t.restore == nil {
		return fmt.Errorf("%w: restore requires the offline capability", driver.ErrUnsupported)
	}
	if t.restore != nil {
		validate := driver.ValidateOfflineRestoreHeader
		if t.nativeRestore != nil {
			validate = t.nativeRestore.ValidateHeader
		}
		if t.importRestore != nil {
			validate = t.importRestore.ValidateHeader
		}
		if err := validate(h); err != nil {
			return err
		}
		if t.importRestore != nil {
			for _, record := range t.records {
				if record != nil {
					if err := t.importRestore.ValidateRecord(*record, h.Time); err != nil {
						return err
					}
				}
			}
		}
		expected = t.restore.Incarnation
	}
	if h.Incarnation != expected {
		return driver.ErrIncarnationChanged{From: h.Incarnation, To: t.meta.Incarnation}
	}
	if _, err := driver.PrincipalKey(h.Principal); err != nil {
		return err
	}
	return h.Kind.Validate()
}
func (t *transaction) validateChanges(ctx context.Context, changes []model.ChangeRecord) error {
	seen := make(map[string]bool)
	for _, change := range changes {
		switch change.Op {
		case model.OpCreate, model.OpPatch, model.OpReparent, model.OpLink, model.OpUnlink, model.OpComment, model.OpAttach, model.OpDelete, model.OpSetMeta, model.OpClose:
		default:
			return fmt.Errorf("sqlite: invalid change operation")
		}
		id, err := recordKey(change.Ref)
		if err != nil {
			return err
		}
		staged, ok := t.records[id]
		if !ok || seen[id] {
			return fmt.Errorf("sqlite: each staged record must appear exactly once")
		}
		seen[id] = true
		if (staged == nil) != (change.Op == model.OpDelete) {
			return fmt.Errorf("sqlite: tombstone disagrees with change operation")
		}
		if err = change.FromRevision.Validate(); err != nil {
			return err
		}
		previous := model.Natural64(0)
		if state, cached := t.recordStates[id]; cached {
			// Lock already observed this exact record before staging. The
			// transaction holds the complete update lock, so its revision cannot
			// change; reuse that observation instead of rereading key_history.
			previous = state.Revision
		} else {
			var err error
			previous, err = t.recordRevision(ctx, id)
			if err != nil {
				return err
			}
		}
		if change.FromRevision.String() != previous.String() {
			return driver.ErrConflict
		}
	}
	if len(seen) != len(t.records) {
		return fmt.Errorf("sqlite: undeclared record mutation")
	}
	return nil
}
func (t *transaction) validatePredicates(ctx context.Context) error {
	for _, r := range t.acquired {
		if r.Key.Kind == driver.LockSentinel && r.Mode != driver.LockShare {
			if _, ok := t.bumps[keyID(r.Key)]; !ok {
				return fmt.Errorf("sqlite: update sentinel must be bumped")
			}
		}
	}
	for id, record := range t.records {
		var kind string
		if record != nil {
			kind = record.Kind
		} else {
			if err := t.tx.QueryRowContext(ctx, `SELECT kind FROM objects WHERE key=?`, id).Scan(&kind); err != nil && err != sql.ErrNoRows {
				return err
			}
		}
		if kind != "bead" {
			continue
		}
		ref := t.recordRef(id)
		if !ref.IsID() {
			return fmt.Errorf("sqlite: bead requires resolved ID")
		}
		sentinel, err := driver.NewBeadSentinel(ref.ID)
		if err != nil {
			return err
		}
		if _, ok := t.bumps[keyID(driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})]; !ok {
			return fmt.Errorf("sqlite: bead requires its exact sentinel bump")
		}
	}
	_, err := t.validateDerivedPredicates()
	return err
}

// One pass over the staged bumps records the five predicate families. The
// family check for each derived row is then constant work, independent of the
// number of other bumped keys in the transaction.
type predicateValidationWork struct {
	BumpsVisited   int
	DerivedVisited int
	FamilyChecks   int
}

func (t *transaction) validateDerivedPredicates() (predicateValidationWork, error) {
	work := predicateValidationWork{}
	var ready, closure, grant, ontology, plugins bool
	for _, key := range t.bumps {
		work.BumpsVisited++
		if key.Kind != driver.LockSentinel {
			continue
		}
		sentinel := string(key.Sentinel)
		switch {
		case strings.HasPrefix(sentinel, "ready/"):
			ready = true
		case strings.HasPrefix(sentinel, "closure/"):
			closure = true
		case strings.HasPrefix(sentinel, "grant/"):
			grant = true
		case sentinel == "ontology":
			ontology = true
		case sentinel == "plugins":
			plugins = true
		}
	}
	// Derived row contents are opaque. This checks structural family coverage;
	// core is responsible for selecting every semantically affected exact scope.
	for _, v := range t.derived {
		work.DerivedVisited++
		var family string
		var found bool
		switch v.Key.Derived.Table {
		case driver.DerivedReadyState:
			family, found = "ready/", ready
		case driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute:
			family, found = "closure/", closure
		case driver.DerivedGrant:
			family, found = "grant/", grant
		case driver.DerivedOntology:
			family, found = "ontology", ontology
		case driver.DerivedPlugins:
			family, found = "plugins", plugins
		}
		if family == "" {
			continue
		}
		work.FamilyChecks++
		if !found {
			return work, fmt.Errorf("sqlite: derived table %s requires %s predicate bump", v.Key.Derived.Table, family)
		}
	}
	return work, nil
}
