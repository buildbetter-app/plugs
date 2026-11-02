package s1relations_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type timedPost struct {
	ordinary atomic.Int32
	timed    atomic.Int32
	observed atomic.Bool
}

func (p *timedPost) After(context.Context, port.CommittedChange, port.SnapshotReader) error {
	p.ordinary.Add(1)
	return errors.New("unexpected ordinary callback path")
}

func (p *timedPost) AfterWithTimeout(ctx context.Context, timeout time.Duration, change port.CommittedChange, reader port.SnapshotReader) error {
	p.timed.Add(1)
	if timeout <= 0 || timeout > time.Second || reader.Snapshot().At == nil || *reader.Snapshot().At != change.Revision {
		return errors.New("timed callback did not receive its deadline and committed snapshot")
	}
	rows, err := reader.Get(ctx, []model.Ref{change.Changes[0].Resource.Ref})
	if err != nil || len(rows) != 1 {
		return errors.New("timed callback could not read its committed record")
	}
	doc, err := pipeline.DecodeDocument(rows[0].Body)
	if err != nil || doc.Fields["title"] != "committed warning" {
		return errors.New("timed callback observed the wrong record image")
	}
	p.observed.Store(true)
	panic("private controlled timed callback failure")
}

func TestB28TimedPostWarningAndDeadLetterReplayThroughPublicRPCAcrossAllStores(t *testing.T) {
	stores(t, func(t *testing.T, kind string) {
		post := &timedPost{}
		f := newFixture(t, kind, []string{"s1-warning"}, pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "s1/timed-post", Subscriber: post}))
		params := map[string]any{"type": "issue", "fields": map[string]any{"title": "committed warning"}}
		first := f.call("bead.create", "warning", params)
		if first.Error != nil || first.Meta.Replayed {
			t.Fatalf("committed post failure was returned as an RPC error: %+v", first)
		}
		var commit model.Commit
		if err := json.Unmarshal(first.Result, &commit); err != nil {
			t.Fatal(err)
		}
		if commit.Revision.String() != "1" || len(commit.Warnings) != 1 || commit.Warnings[0].Code != "post_subscriber_failed" || !post.observed.Load() || post.timed.Load() != 1 || post.ordinary.Load() != 0 {
			t.Fatalf("timed path did not produce the committed warning: %+v observed=%v timed=%d ordinary=%d", commit, post.observed.Load(), post.timed.Load(), post.ordinary.Load())
		}
		receipt := f.receipt("bead.create", "warning")
		if receipt == nil || receipt.ResponsePending {
			t.Fatalf("RPC success lacks a final durable response: %+v", receipt)
		}
		equalJSON(t, commit, receipt.Commit)
		meta, err := f.store.Meta(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		deadKey := pipeline.PostDeadLetterKey(meta.Incarnation, commit.Revision, "s1/timed-post")
		dead, err := f.store.(driver.CheckpointStore).LoadCheckpoint(f.ctx, deadKey)
		var entry pipeline.PostDeadLetter
		if err != nil || dead.Generation != 1 || json.Unmarshal(dead.Payload, &entry) != nil || entry.Incarnation != meta.Incarnation || entry.Revision != commit.Revision || entry.Binding != "s1/timed-post" || entry.Reason != "post_subscriber_failed" {
			t.Fatalf("sanitized dead letter missing: %+v %+v err=%v", dead, entry, err)
		}
		for _, payload := range [][]byte{first.Result, dead.Payload} {
			if bytes.Contains(payload, []byte("private controlled")) {
				t.Fatal("public warning/dead letter disclosed callback panic text")
			}
		}
		image := f.image()
		for attempt := 0; attempt < 2; attempt++ {
			f.reopen()
			replay := f.call("bead.create", "warning", params)
			if replay.Error != nil || !replay.Meta.Replayed || post.timed.Load() != 1 || post.ordinary.Load() != 0 || !bytes.Equal(image, f.image()) {
				t.Fatalf("reopened wire replay changed graph or redelivered the post: %+v timed=%d", replay, post.timed.Load())
			}
			equalJSON(t, first.Result, replay.Result)
			equalJSON(t, receipt, f.receipt("bead.create", "warning"))
			reloaded, err := f.store.(driver.CheckpointStore).LoadCheckpoint(f.ctx, deadKey)
			if err != nil || reloaded.Generation != dead.Generation || !bytes.Equal(reloaded.Payload, dead.Payload) {
				t.Fatalf("reopened dead letter changed: %+v err=%v", reloaded, err)
			}
		}
		noop := f.write("bead.update", "no-op", map[string]any{"id": ref("s1-warning"), "fields": map[string]any{"title": "committed warning"}})
		if noop.Revision != commit.Revision || len(noop.Warnings) != 0 || post.timed.Load() != 1 || !bytes.Equal(image, f.image()) {
			t.Fatal("no-op redelivered the failed post or minted a version/event/warning")
		}
		changed := f.call("bead.create", "warning", map[string]any{"type": "issue", "fields": map[string]any{"title": "different request"}})
		if changed.Error == nil || changed.Error.Data.Code != "idempotency_key_reused" || post.timed.Load() != 1 || !bytes.Equal(image, f.image()) {
			t.Fatal("changed request reused a committed warning key or ran a callback")
		}
	})
}

type validatorFunc func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error)

func (fn validatorFunc) Validate(ctx context.Context, proposed port.ProposedChange, reader port.SnapshotReader) (port.Verdict, error) {
	return fn(ctx, proposed, reader)
}

func TestB28FailClosedPreRefusalDoesNotBecomeCommittedWarningOnAnyStore(t *testing.T) {
	stores(t, func(t *testing.T, kind string) {
		var available atomic.Bool
		post := &timedPost{}
		f := newFixture(t, kind, []string{"s1-refused", "s1-retried"},
			pipeline.WithValidators(pipeline.ValidatorBinding{ID: "s1/fail-closed", Validator: validatorFunc(func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error) {
				if !available.Load() {
					return port.Verdict{}, errors.New("controlled unavailable pre subscriber")
				}
				return port.Verdict{Allow: true}, nil
			})}),
			pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "s1/timed-post", Subscriber: post}),
		)
		before := f.image()
		params := map[string]any{"type": "issue", "fields": map[string]any{"title": "committed warning"}}
		refused := f.call("bead.create", "pre-retry", params)
		if refused.Error == nil || refused.Error.Data.Code != "subscriber_unavailable" || len(refused.Result) != 0 || post.timed.Load() != 0 || f.receipt("bead.create", "pre-retry") != nil || !bytes.Equal(before, f.image()) {
			t.Fatalf("fail-closed refusal committed graph/response/post effects: %+v", refused)
		}
		f.reopen()
		if !bytes.Equal(before, f.image()) {
			t.Fatal("refused pre subscriber left durable state after reopen")
		}
		available.Store(true)
		committed := f.write("bead.create", "pre-retry", params)
		if committed.Revision.String() != "1" || len(committed.Warnings) != 1 || committed.Warnings[0].Code != "post_subscriber_failed" || post.timed.Load() != 1 || !post.observed.Load() {
			t.Fatalf("same-key retry did not commit after pre recovery: %+v", committed)
		}
		image := f.image()
		f.reopen()
		replay := f.call("bead.create", "pre-retry", params)
		if replay.Error != nil || !replay.Meta.Replayed || post.timed.Load() != 1 || !bytes.Equal(image, f.image()) {
			t.Fatal("recovered pre request was not a stored callback-free replay")
		}
		equalJSON(t, json.RawMessage(jsonBytes(t, committed)), replay.Result)
	})
}
