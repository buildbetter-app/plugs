package conformance

import (
	"context"
	"fmt"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// FixtureChange is a complete post-image for a storage integration fixture,
// or an explicit OpDelete. It does not execute product write semantics.
type FixtureChange struct {
	Ref          driver.Ref
	Kind         string
	Op           model.OpKind
	Body         []byte
	ChangedPaths []string
}

// Seed commits real records/history through the driver. Changes retain caller
// order while lock acquisition is sorted. Meta supplies a missing incarnation;
// header supplies time, principal and attribution. The helper owns the complete
// lock/change declarations and bumps bead predicates, so event tests never
// receive or import a transaction handle.
func Seed(ctx context.Context, store driver.Store, lease driver.ActiveLease, header driver.ChangesetHeader, changes []FixtureChange) (model.Changeset, error) {
	return seed(ctx, store, lease, header, changes, false)
}

// SeedRollback stages the same fixture but aborts, publishing no revision.
func SeedRollback(ctx context.Context, store driver.Store, lease driver.ActiveLease, header driver.ChangesetHeader, changes []FixtureChange) error {
	_, err := seed(ctx, store, lease, header, changes, true)
	return err
}

func seed(ctx context.Context, store driver.Store, lease driver.ActiveLease, header driver.ChangesetHeader, changes []FixtureChange, rollback bool) (model.Changeset, error) {
	meta, err := store.Meta(ctx)
	if err != nil {
		return model.Changeset{}, err
	}
	if header.Incarnation == "" {
		header.Incarnation = meta.Incarnation
	}
	if header.Kind == "" {
		header.Kind = model.ChangesetWrite
	}
	if header.Principal.Kind == "" {
		header.Principal = model.PrincipalRef{Kind: "system", ID: "storage-fixture"}
	}
	requests := map[string]driver.LockRequest{}
	seen := map[string]bool{}
	sentinels := []driver.Sentinel{}
	for _, change := range changes {
		key := driver.LockKey{Kind: driver.LockRecord, Ref: &change.Ref}
		id, err := driver.EncodeLockKey(key)
		if err != nil {
			return model.Changeset{}, err
		}
		if seen[id] {
			return model.Changeset{}, fmt.Errorf("fixture has duplicate record")
		}
		seen[id] = true
		requests[id] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
		// Deletions of beads supply Kind too, making fixture intent explicit.
		if change.Kind == "bead" {
			if !change.Ref.IsID() {
				return model.Changeset{}, fmt.Errorf("bead fixture requires resolved id")
			}
			sentinel, err := driver.NewBeadSentinel(change.Ref.ID)
			if err != nil {
				return model.Changeset{}, err
			}
			sentinels = append(sentinels, sentinel)
			key := driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}
			id, _ := driver.EncodeLockKey(key)
			requests[id] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
		}
	}
	ids := make([]string, 0, len(requests))
	for id := range requests {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	locks := make([]driver.LockRequest, 0, len(ids))
	for _, id := range ids {
		locks = append(locks, requests[id])
	}
	tx, err := store.Write(ctx, lease)
	if err != nil {
		return model.Changeset{}, err
	}
	defer tx.Close()
	states, err := tx.Lock(ctx, locks)
	if err != nil {
		return model.Changeset{}, err
	}
	previous := map[string]model.Revision{}
	for _, state := range states {
		if state.Key.Kind == driver.LockRecord {
			id, _ := driver.EncodeLockKey(state.Key)
			previous[id] = state.Revision
		}
	}
	header.LockKeys = lockKeys(locks)
	header.Changes = make([]model.ChangeRecord, 0, len(changes))
	if _, err := tx.Head(ctx); err != nil {
		return model.Changeset{}, err
	}
	for _, change := range changes {
		id, _ := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &change.Ref})
		if change.Op == model.OpDelete {
			err = tx.Delete(ctx, change.Ref)
		} else {
			err = tx.Put(ctx, driver.Record{Ref: change.Ref, Kind: change.Kind, Body: change.Body})
		}
		if err != nil {
			return model.Changeset{}, err
		}
		header.Changes = append(header.Changes, model.ChangeRecord{Ref: change.Ref, Op: change.Op, FromRevision: previous[id], ChangedPaths: append([]string(nil), change.ChangedPaths...)})
	}
	if err = tx.BumpSentinels(ctx, sentinels); err != nil {
		return model.Changeset{}, err
	}
	if rollback {
		return model.Changeset{}, tx.Rollback(ctx)
	}
	return tx.Commit(ctx, header)
}
