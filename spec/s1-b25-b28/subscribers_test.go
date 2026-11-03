package s1b25b28_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
)

type refusingMutator struct{}

func (refusingMutator) Mutate(context.Context, port.ProposedChange, port.SnapshotReader) ([]port.Patch, error) {
	return nil, errors.New("private controlled pre failure")
}

func TestB28PostSnapshotsOrderNoopAndPreFailureAcrossDrivers(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite", "postgres"} {
		t.Run(kind, func(t *testing.T) {
			s := openStack(t, kind, []string{"s1-hook-a", "s1-hook-b"})
			var calls atomic.Int32
			var last port.CommittedChange
			observer := pipeline.PostBinding{ID: "s1/observe", Subscriber: postFunc(func(ctx context.Context, c port.CommittedChange, r port.SnapshotReader) error {
				calls.Add(1)
				if r.Snapshot().At == nil || r.Snapshot().At.String() != c.Revision.String() {
					return errors.New("post reader did not pin the committed revision")
				}
				for _, change := range c.Changes {
					rows, err := r.Get(ctx, []model.Ref{change.Resource.Ref})
					if err != nil {
						return err
					}
					if change.Op.Kind == model.OpDelete {
						if len(rows) != 0 || len(change.Before) == 0 || len(change.After) != 0 && string(change.After) != "null" {
							return errors.New("delete post lost before image or retained a live image")
						}
					} else if len(rows) != 1 || !bytes.Equal(rows[0].Body, change.After) {
						return errors.New("post snapshot differs from changed row")
					}
				}
				last = c
				return nil
			})}
			w := s.newWriter(pipeline.WithPostSubscribers(observer))
			cs := model.ChangeSet{Ops: []model.Op{
				{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": "first"}}},
				{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": "second"}}},
			}}
			first, err := w.Apply(s.ctx, s.principal, cs, writeMeta(90))
			if err != nil || len(first.Warnings) != 0 || calls.Load() != 1 || len(last.Changes) != 2 || last.Changes[0].Resource.Ref.ID != "s1-hook-a" || last.Changes[1].Resource.Ref.ID != "s1-hook-b" {
				t.Fatalf("ordered landed post images: %+v %+v calls=%d err=%v", first, last, calls.Load(), err)
			}
			replay, err := w.Apply(s.ctx, s.principal, cs, writeMeta(90))
			if err != nil || !reflect.DeepEqual(first, replay) || calls.Load() != 1 {
				t.Fatalf("replay double-fired post: %+v %v", replay, err)
			}
			patch := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref("s1-hook-a"), Fields: map[string]any{"title": "first"}}}}}
			noop, err := w.Apply(s.ctx, s.principal, patch, writeMeta(91))
			if err != nil || noop.Revision.String() != "1" || calls.Load() != 1 {
				t.Fatalf("no-op fired post: %+v %v", noop, err)
			}
			patch.Ops[0].Patch.Fields["title"] = "updated"
			before := relationState(t, s)
			closed := s.newWriter(pipeline.WithPostSubscribers(observer), pipeline.WithMutators(pipeline.MutatorBinding{ID: "s1/refuse", Mutator: refusingMutator{}}))
			_, err = closed.Apply(s.ctx, s.principal, patch, writeMeta(92))
			var typed *model.Error
			if !errors.As(err, &typed) || typed.Data.Code != "subscriber_unavailable" || strings.Contains(err.Error(), "private controlled") || calls.Load() != 1 || !reflect.DeepEqual(before, relationState(t, s)) {
				t.Fatalf("fail-closed pre must refuse atomically without a post or private error: %v", err)
			}
			assertNoReceipt(t, s, writeMeta(92))
			open := s.newWriter(pipeline.WithPostSubscribers(observer), pipeline.WithMutators(pipeline.MutatorBinding{ID: "s1/refuse", FailOpen: true, Mutator: refusingMutator{}}))
			committed, err := open.Apply(s.ctx, s.principal, patch, writeMeta(93))
			if err != nil || committed.Revision.String() != "2" || calls.Load() != 2 || len(committed.Warnings) != 1 || committed.Warnings[0].Code != "subscriber_skipped" {
				t.Fatalf("explicit fail-open did not return committed warning/post: %+v calls=%d err=%v", committed, calls.Load(), err)
			}
			deleted, err := w.Apply(s.ctx, s.principal, model.ChangeSet{Ops: []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: ref("s1-hook-b")}}}}, writeMeta(94))
			if err != nil || len(deleted.Warnings) != 0 || calls.Load() != 3 || len(last.Changes) != 1 || last.Changes[0].Op.Kind != model.OpDelete {
				t.Fatalf("delete post image: %+v %+v calls=%d err=%v", deleted, last, calls.Load(), err)
			}
			assertPersistedClock(t, s)
		})
	}
}
