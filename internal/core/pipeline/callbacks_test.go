package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestPostFailureIsCommittedAndDurablyReplayed(t *testing.T) {
	h := setup(t)
	var pre, posts atomic.Int32
	w := h.writer(WithValidators(ValidatorBinding{ID: "pre", Validator: validatorFunc(func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error) {
		pre.Add(1)
		return port.Verdict{Allow: true}, nil
	})}), WithPostSubscribers(PostBinding{ID: "external", Subscriber: postFunc(func(ctx context.Context, change port.CommittedChange, r port.SnapshotReader) error {
		posts.Add(1)
		if r.Snapshot().At.String() != change.Revision.String() {
			return errors.New("post reader not pinned")
		}
		records, err := r.Get(ctx, []model.Ref{change.Changes[0].Resource.Ref})
		if err != nil || len(records) != 1 {
			return errors.New("post did not see committed data")
		}
		return errors.New("external sink unavailable")
	})}))
	cs := model.ChangeSet{Ops: []model.Op{create("new")}}
	result := apply(t, w, cs, meta(1))
	if result.Revision.String() != "1" || len(result.Warnings) != 1 || result.Warnings[0].Code != "post_subscriber_failed" {
		t.Fatalf("result=%+v", result)
	}
	// A new Writer proves replay uses durable storage, not a callback cache.
	again := apply(t, h.writer(), cs, meta(1))
	if !reflect.DeepEqual(result, again) || pre.Load() != 1 || posts.Load() != 1 {
		t.Fatalf("replay=%+v counters=%d/%d", again, pre.Load(), posts.Load())
	}
	if h.read(t).Snapshot().At.String() != "1" {
		t.Fatal("post failure/finalization advanced graph head")
	}
}

func TestMutatorDataflowOwnershipAndFailOpen(t *testing.T) {
	h := setup(t)
	producer := MutatorBinding{ID: "z-producer", Owns: []string{"fields.source"}, Mutator: mutatorFunc(func(_ context.Context, p port.ProposedChange, _ port.SnapshotReader) ([]port.Patch, error) {
		return []port.Patch{{Resource: p.Changes[0].Resource, Paths: map[string]json.RawMessage{"fields.source": json.RawMessage(`"derived"`)}}}, nil
	})}
	consumer := MutatorBinding{ID: "a-consumer", Reads: []string{"fields.source"}, Owns: []string{"fields.dest"}, Mutator: mutatorFunc(func(_ context.Context, p port.ProposedChange, _ port.SnapshotReader) ([]port.Patch, error) {
		doc, err := DecodeDocument(p.Changes[0].After)
		if err != nil {
			return nil, err
		}
		if doc.Fields["source"] != "derived" {
			return nil, errors.New("dataflow order wrong")
		}
		// Attempting to mutate the isolated proposal cannot change stored input.
		p.Changes[0].Op.Create.Fields["title"] = "callback wrote its argument"
		return []port.Patch{{Resource: p.Changes[0].Resource, Paths: map[string]json.RawMessage{"fields.dest": json.RawMessage(`"ok"`)}}}, nil
	})}
	skip := ValidatorBinding{ID: "offline", FailOpen: true, Validator: validatorFunc(func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error) {
		return port.Verdict{}, errors.New("offline")
	})}
	w := h.writer(WithMutators(consumer, producer), WithValidators(skip))
	result := apply(t, w, model.ChangeSet{Ops: []model.Op{create("new")}}, meta(1))
	doc, err := DecodeDocument(result.Results[0].Record)
	if err != nil {
		t.Fatal(err)
	}
	if doc.Fields["dest"] != "ok" || doc.Fields["title"] != "initial" || result.Results[0].SetBy["fields.source"] != "z-producer" || len(result.Warnings) != 1 || result.Warnings[0].Code != "subscriber_skipped" {
		t.Fatalf("body=%+v result=%+v", doc, result)
	}
	bad := producer
	bad.ID = "unowned"
	bad.Owns = nil
	_, err = h.writer(WithMutators(bad)).Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{create("other")}}, meta(2))
	code(t, err, "plugin_patch_out_of_scope")
	conflict := producer
	conflict.ID = "second-owner"
	_, err = h.writer(WithMutators(producer, conflict)).Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{create("other")}}, meta(3))
	code(t, err, "invalid_params")
}

type authorizerFunc func(context.Context, port.AuthorizationStage, port.ProposedChange, port.SnapshotReader) error

func (f authorizerFunc) Authorize(ctx context.Context, s port.AuthorizationStage, p port.ProposedChange, r port.SnapshotReader) error {
	return f(ctx, s, p, r)
}

func TestReplayReauthorizesAndDifferentParamsRefuse(t *testing.T) {
	h := setup(t)
	cs := model.ChangeSet{Ops: []model.Op{create("new")}}
	w := h.writer()
	apply(t, w, cs, meta(1))
	var stages []port.AuthorizationStage
	denied := h.writer(WithAuthorizer(authorizerFunc(func(_ context.Context, s port.AuthorizationStage, _ port.ProposedChange, _ port.SnapshotReader) error {
		stages = append(stages, s)
		return failure("forbidden", "current authority revoked")
	})))
	_, err := denied.Apply(context.Background(), by, cs, meta(1))
	code(t, err, "forbidden")
	if !reflect.DeepEqual(stages, []port.AuthorizationStage{port.AuthorizeReplay}) {
		t.Fatalf("stages=%v", stages)
	}
	cs.Ops[0].Create.Fields["title"] = "different"
	_, err = w.Apply(context.Background(), by, cs, meta(1))
	code(t, err, "idempotency_key_reused")
	if h.read(t).Snapshot().At.String() != "1" {
		t.Fatal("replay refusal wrote graph")
	}
}

func TestConcurrentSameKeyRunsPostOnce(t *testing.T) {
	h := setup(t)
	var posts atomic.Int32
	w := h.writer(WithPostSubscribers(PostBinding{ID: "post", Subscriber: postFunc(func(context.Context, port.CommittedChange, port.SnapshotReader) error { posts.Add(1); return nil })}))
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make([]model.Commit, 8)
	errs := make([]error, 8)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{create("new")}}, meta(1))
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("call%d: %v", i, err)
		}
		if results[i].Revision.String() != "1" || results[i].Results[0].Ref.ID != results[0].Results[0].Ref.ID {
			t.Fatalf("different durable receipts: %+v", results)
		}
	}
	if posts.Load() != 1 || h.read(t).Snapshot().At.String() != "1" {
		t.Fatalf("posts=%d head=%s", posts.Load(), h.read(t).Snapshot().At)
	}
}

func TestConcurrentRevisionCASHasOneWinner(t *testing.T) {
	h := setup(t)
	w := h.writer()
	seed := apply(t, w, model.ChangeSet{Ops: []model.Op{create("new")}}, meta(1))
	ref := seed.Results[0].Ref
	cs := model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{"title": "winner"})}, Expect: []model.Precondition{{Ref: ref, Revision: &seed.Revision}}}
	start := make(chan struct{})
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func(i int) { <-start; _, err := w.Apply(context.Background(), by, cs, meta(i+2)); errs <- err }(i)
	}
	close(start)
	a, b := <-errs, <-errs
	if a == nil && b == nil || a != nil && b != nil {
		t.Fatalf("want one winner: %v %v", a, b)
	}
	if a != nil {
		code(t, a, "precondition_failed")
	}
	if b != nil {
		code(t, b, "precondition_failed")
	}
	r := h.read(t)
	versions, err := r.Versions(context.Background(), ref, driver.PageReq{})
	if err != nil || len(versions.Items) != 2 || r.Snapshot().At.String() != "2" {
		t.Fatalf("versions=%+v head=%s err=%v", versions, r.Snapshot().At, err)
	}
}
