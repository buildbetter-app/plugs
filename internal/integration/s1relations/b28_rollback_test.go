package s1relations_test

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This ordinary fault is after the real driver transaction has received the
// public write. No compare-and-set, claim, authorization or concurrency seam
// is changed or exercised here.
type b28CommitFault struct {
	driver.Store
	driver.CheckpointStore
	driver.ResponseFinalizer
	staged atomic.Bool
}

type b28CommitFaultTx struct {
	driver.WriteTx
	owner *b28CommitFault
}

func (s *b28CommitFault) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return &b28CommitFaultTx{WriteTx: tx, owner: s}, nil
}

func (tx *b28CommitFaultTx) Commit(ctx context.Context, _ driver.ChangesetHeader) (model.Changeset, error) {
	rows, err := tx.WriteTx.Get(ctx, []model.Ref{ref("b28-staging")})
	if err != nil || len(rows) != 1 {
		return model.Changeset{}, errors.New("real staged row unavailable")
	}
	doc, err := pipeline.DecodeDocument(rows[0].Body)
	if err != nil || doc.Fields["title"] != "must roll back after staging" {
		return model.Changeset{}, errors.New("real driver did not stage expected value")
	}
	tx.owner.staged.Store(true)
	return model.Changeset{}, &model.Error{Message: "controlled ordinary commit refusal", Data: model.ErrorData{Code: "internal_error"}}
}

func TestB28PublicFailureAfterRealDriverStagingRollsBackWithoutPost(t *testing.T) {
	stores(t, func(t *testing.T, kind string) {
		var posts atomic.Int32
		f := newFixture(t, kind, []string{"b28-staging"}, pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "b28/staging-observer", Subscriber: b28Post(func(context.Context, port.CommittedChange, port.SnapshotReader) error { posts.Add(1); return nil })}))
		seed := f.write("bead.create", "b28-staging-seed", map[string]any{"type": "issue", "fields": map[string]any{"title": "exact durable seed"}})
		if seed.Revision.String() != "1" || posts.Load() != 1 {
			t.Fatal("ordinary staging seed failed")
		}
		before := f.image()
		fault := &b28CommitFault{Store: f.store, CheckpointStore: f.store.(driver.CheckpointStore), ResponseFinalizer: f.store.(driver.ResponseFinalizer)}
		f.store = fault
		f.bind()
		got := f.call("bead.update", "b28-staging-fault", map[string]any{"id": ref("b28-staging"), "fields": map[string]any{"title": "must roll back after staging"}})
		if got.Error == nil || got.Error.Data.Code != "internal_error" || len(got.Result) != 0 || !fault.staged.Load() || posts.Load() != 1 || f.receipt("bead.update", "b28-staging-fault") != nil || !bytes.Equal(before, f.image()) {
			t.Fatalf("real driver staging failure escaped rollback: %+v staged=%v posts=%d", got, fault.staged.Load(), posts.Load())
		}
		f.reopen()
		if !bytes.Equal(before, f.image()) || f.document("b28-staging").Fields["title"] != "exact durable seed" || posts.Load() != 1 || f.receipt("bead.update", "b28-staging-fault") != nil {
			t.Fatal("SQL reopen retained refused staging, graph history or receipt")
		}
		t.Log("PUBLIC_DRIVER_STAGING_REACHED: exact pending title observed before injected commit refusal; row/version/event/head/receipt conserved; no post")
	})
}
