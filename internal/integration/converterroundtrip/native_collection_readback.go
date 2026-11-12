package converterroundtrip

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/historyverify"
)

type collectionReadbackImage struct {
	kind, hash string
	bytes      uint64
	charged    uint64
}

func collectionReadbackKey(owner *dumpformat.Archive, ref model.Ref) (string, error) {
	if err := owner.ChargeNativeDeltaBytes("collection_readback_key_workspace", uint64(len(ref.ID)+len(ref.Key))*2+128); err != nil {
		return "", err
	}
	return driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
}

// VerifyNativeCaptureCollectionReadback freshly authenticates each original
// exactly once, independently of the import's preparation. A compact rolling
// hash index is compared to complete unfiltered target snapshots at EVERY
// global revision, retaining source0's HEAD while source1 advances. Final
// HEAD/current records are checked against independent verifier checkpoints.
func VerifyNativeCaptureCollectionReadback(ctx context.Context, store driver.Store, binding dumpformat.NativeJoinedBinding, sources []historyverify.NativeCaptureSetOpener, profile driver.RestoreImportProfile, report Report) (err error) {
	if store == nil || report.Restored.Incarnation == "" {
		return driver.ErrUnsupported
	}
	p, err := prepareNativeCaptureCollection(ctx, binding, sources, profile, 0)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, p.close()) }()
	owner, release := p.archive.NativeDeltaAccountingScope()
	defer release()
	ctx = owner.WithNativeDeltaAccounting(ctx)
	request, err := nativeReceivingRequestHash(owner, p.profile)
	if err != nil || request != report.RequestHash || p.stream.Header().Head == ^uint64(0) || report.Restored.Head != model.Natural64(p.stream.Header().Head+1) {
		return driver.ErrConflict
	}
	final := report.Restored.Head
	reader, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &final})
	if err != nil {
		return err
	}
	rows, getErr := reader.Get(ctx, []model.Ref{model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0)), model.RefByID(importBindingRecordID, model.Natural64(0))})
	closeErr := reader.Close()
	if err = errors.Join(getErr, closeErr); err != nil {
		return err
	}
	if len(rows) != 2 {
		return driver.ErrConflict
	}
	for _, record := range rows {
		if record.Ref.ID == driver.OfflineHistoryEvidenceID {
			if err = p.stream.ValidateEvidence(ctx, p.profile, record.Body); err != nil {
				return err
			}
		} else {
			if err = owner.ChargeNativeDeltaBytes("collection_readback_binding_decode", uint64(len(record.Body))*8+1024); err != nil {
				return err
			}
			var actual importBindingEvidence
			if json.Unmarshal(record.Body, &actual) != nil || actual.RequestHash != request {
				return driver.ErrConflict
			}
			got, e := nativeReceivingRequestHash(owner, actual.Profile)
			if e != nil || got != request {
				return driver.ErrConflict
			}
		}
	}
	expected := map[string]collectionReadbackImage{}
	var sequence, completed uint64
	compare := func(at model.Revision, current bool) error {
		work, release := owner.NativeDeltaAccountingScope()
		defer release()
		r, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &at})
		if err != nil {
			return err
		}
		groups, queryErr := queryNativeRowGroups(ctx, r, report.Restored.Incarnation, at, work)
		closeErr := r.Close()
		if err = errors.Join(queryErr, closeErr); err != nil {
			return err
		}
		count := 0
		for _, records := range groups {
			count += len(records)
		}
		if current {
			if err := work.ChargeNativeDeltaBytes("collection_readback_current_slots", uint64(count)*128); err != nil {
				return err
			}
			all := make([]driver.Record, 0, count)
			for _, records := range groups {
				all = append(all, records...)
			}
			return p.stream.ValidateNativeCurrent(ctx, all)
		}
		if count != len(expected) {
			return fmt.Errorf("native collection historical readback census differs")
		}
		for _, records := range groups {
			for _, record := range records {
				key, e := collectionReadbackKey(work, record.Ref)
				if e != nil {
					return e
				}
				want, ok := expected[key]
				if !ok || want.kind != record.Kind || want.bytes != uint64(len(record.Body)) || want.hash != hashBytes(record.Body) {
					return fmt.Errorf("native collection historical readback body differs")
				}
			}
		}
		return nil
	}
	err = p.stream.VisitVerifiedFrames(ctx, func(frame driver.OfflineHistoryStreamFrame) error {
		if frame.Commit != nil {
			if sequence != completed || frame.Commit.Sequence != completed+1 {
				return driver.ErrConflict
			}
			sequence = frame.Commit.Sequence
		}
		if table := frame.Table; table != nil {
			work, release := owner.NativeDeltaAccountingScope()
			defer release()
			key, err := collectionReadbackKey(work, table.Ref)
			if err != nil {
				return err
			}
			old, present := expected[key]
			if table.Before == nil && present || table.Before != nil && (!present || old.kind != table.Kind || old.bytes != table.Before.Bytes || old.hash != table.Before.SHA256) {
				return fmt.Errorf("native collection historical readback preimage differs")
			}
			oldCharge := old.charged
			if table.After == nil {
				delete(expected, key)
			} else {
				n := uint64(len(key)+len(table.Kind)+len(table.After.SHA256)) + 256
				if err := owner.ChargeNativeDeltaBytes("collection_readback_rolling_hash_index", n); err != nil {
					return err
				}
				expected[strings.Clone(key)] = collectionReadbackImage{strings.Clone(table.Kind), strings.Clone(table.After.SHA256), table.After.Bytes, n}
			}
			old = collectionReadbackImage{}
			owner.ReleaseNativeDeltaBytes("collection_readback_rolling_hash_index", oldCharge)
		}
		if frame.CommitEnd != nil {
			if sequence != completed+1 {
				return driver.ErrConflict
			}
			if err := compare(model.Natural64(sequence), false); err != nil {
				return err
			}
			completed = sequence
		}
		return nil
	})
	if err != nil {
		return err
	}
	if completed != p.stream.Header().Head {
		return driver.ErrConflict
	}
	return compare(final, true)
}
