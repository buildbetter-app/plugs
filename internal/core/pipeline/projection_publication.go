package pipeline

import (
	"context"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Stage the already prepared source images first, then one complete call per
// projection table. Revalidating every accumulated row after each individual
// source would make a supported batch quadratic before its unchanged deadline.
func publishSourceProjections(ctx context.Context, tx driver.WriteTx, plans []*SourceProjectionPlan) error {
	byKey := make(map[string]driver.LockKey, len(plans))
	for _, p := range plans {
		if !p.deleted && p.doc != nil {
			ref := p.record.Ref
			key := driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
			encoded, err := driver.EncodeLockKey(key)
			if err != nil {
				return err
			}
			byKey[encoded] = key
		}
	}
	// Plans follow request order; Observe requires canonical lock order.
	keys := sortedKeys(byKey)
	states, err := tx.Observe(ctx, keys)
	if err != nil {
		return err
	}
	if len(states) != len(keys) {
		return fmt.Errorf("source publication observation was incomplete")
	}
	revisions := make(map[string]model.Revision, len(states))
	for i, state := range states {
		revisions[keys[i].Ref.ID] = state.Revision
	}
	var revision model.Revision
	if len(keys) != 0 {
		revision, err = sourcePublicationRevision(tx)
		if err != nil {
			return err
		}
	}
	remove, add := map[driver.DerivedTable][]driver.DerivedRow{}, map[driver.DerivedTable][]driver.DerivedRow{}
	for _, p := range plans {
		switch {
		case p.deleted:
			if err := tx.Delete(ctx, p.record.Ref); err != nil {
				return err
			}
			for _, key := range p.keys {
				remove[key.Table] = append(remove[key.Table], driver.DerivedRow{Key: key.Key})
			}
		case p.doc == nil:
			if err := tx.Put(ctx, p.record); err != nil {
				return err
			}
		default:
			if err := putDecodedGraphSource(ctx, tx, p.record, *p.doc, p.links); err != nil {
				return err
			}
			rows, err := p.SelectionRows(tx.Snapshot().Incarnation, revisions[p.record.Ref.ID])
			if err != nil {
				return err
			}
			if err := BindReadySelectionRows(*p.doc, revision, rows); err != nil {
				return err
			}
			for i, row := range rows {
				add[p.keys[i].Table] = append(add[p.keys[i].Table], row)
			}
		}
	}
	for _, table := range []driver.DerivedTable{driver.DerivedSortKey, driver.DerivedAttribute, driver.DerivedReferenceIndex} {
		if len(remove[table])+len(add[table]) != 0 {
			if err := PutSelectionDerived(ctx, tx, table, remove[table], add[table]); err != nil {
				return err
			}
		}
	}
	return nil
}

// Head has already rebound the transaction snapshot under the complete lock
// census. Keep source-observation revisions unchanged until driver Commit,
// while semantic Ready leaves name the actual revision being published.
func sourcePublicationRevision(tx driver.WriteTx) (model.Revision, error) {
	view := tx.Snapshot()
	if view.At == nil {
		return "", fmt.Errorf("source publication requires a bound transaction head")
	}
	n, err := view.At.Uint64()
	if err != nil || n == ^uint64(0) {
		return "", fmt.Errorf("source publication revision space is exhausted")
	}
	return model.Natural64(n + 1), nil
}
