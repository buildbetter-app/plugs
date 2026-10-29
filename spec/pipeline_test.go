package spec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/acceptance"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
)

func createOp(key, title string) model.Op {
	return model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Key: key, Type: "issue", Fields: map[string]any{"title": title}}}
}

func patchOp(ref model.Ref, fields map[string]any) model.Op {
	return model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: fields}}
}

func errorCode(t *testing.T, err error, want model.ErrorCode) {
	t.Helper()
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != want {
		t.Fatalf("want typed %s refusal, got %T: %v", want, err, err)
	}
}

// B21: this exercises the real Writer and memory driver, not an RPC handler
// invented by the acceptance test. RPC coverage is a separate boundary.
func TestB21AtomicBatchBackwardReferencesAndRollback(t *testing.T) {
	f := newCore(t, []string{"pl-first", "pl-second", "pl-rolled-back"})
	cs := model.ChangeSet{Ops: []model.Op{
		createOp("first", "before"),
		patchOp(model.RefByKey("first"), map[string]any{"title": "after"}),
		createOp("second", "second"),
	}}
	original, err := json.Marshal(cs)
	if err != nil {
		t.Fatal(err)
	}
	commit := f.apply(t, "batch", cs)
	if commit.Revision.String() != "1" || len(commit.Results) != 3 {
		t.Fatalf("batch must have one revision and ordered results: %+v", commit)
	}
	for i, want := range []string{"pl-first", "pl-first", "pl-second"} {
		if commit.Results[i].Ref.ID != want || commit.Results[i].Revision.String() != "1" {
			t.Fatalf("result %d lost declaration order/revision: %+v", i, commit.Results[i])
		}
	}
	if got := f.document(t, "pl-first").Fields["title"]; got != "after" {
		t.Fatalf("backward-key patch did not see the in-request create: %v", got)
	}
	current, _ := json.Marshal(cs)
	if err := acceptance.EqualJSON(original, current); err != nil {
		t.Fatalf("batch mutated caller input: %v", err)
	}
	f.assertLog(t, 1)

	_, err = f.writer.Apply(f.ctx, f.by, model.ChangeSet{Ops: []model.Op{
		createOp("temporary", "must disappear"),
		patchOp(model.RefByID("pl-absent", model.Natural64(0)), map[string]any{"title": "refused"}),
	}}, f.meta("batch-refused"))
	errorCode(t, err, "not_found")
	f.assertAbsent(t, "pl-rolled-back")
	f.assertLog(t, 1)

	_, err = f.writer.Apply(f.ctx, f.by, model.ChangeSet{Ops: []model.Op{
		patchOp(model.RefByKey("later"), map[string]any{"title": "forward reference"}),
		createOp("later", "later"),
	}}, f.meta("forward-refused"))
	errorCode(t, err, "invalid_params")
	f.assertLog(t, 1)
	tooMany := make([]model.Op, 1001)
	for i := range tooMany {
		tooMany[i] = patchOp(model.RefByID("pl-first", model.Natural64(0)), map[string]any{"title": "over cap"})
	}
	_, err = f.writer.Apply(f.ctx, f.by, model.ChangeSet{Ops: tooMany}, f.meta("over-cap"))
	errorCode(t, err, "invalid_params")
	f.assertLog(t, 1)
	boundary := make([]model.Op, 1000)
	for i := range boundary {
		boundary[i] = patchOp(model.RefByID("pl-first", model.Natural64(0)), map[string]any{"title": "at boundary"})
	}
	landed := f.apply(t, "at-cap", model.ChangeSet{Ops: boundary})
	if len(landed.Results) != 1000 {
		t.Fatalf("1,000-op boundary lost results: %d", len(landed.Results))
	}
	f.assertLog(t, 2)
}

func TestB22CASConcurrentWinnerAndAtomicHistory(t *testing.T) {
	f := newCore(t, []string{"pl-cas"})
	created := f.apply(t, "create", model.ChangeSet{Ops: []model.Op{createOp("bead", "original")}})
	ref := created.Results[0].Ref
	old := created.Revision
	updated := f.apply(t, "current", model.ChangeSet{
		Ops:    []model.Op{patchOp(ref, map[string]any{"title": "current"})},
		Expect: []model.Precondition{{Ref: ref, Revision: &old}},
	})
	_, err := f.writer.Apply(f.ctx, f.by, model.ChangeSet{
		Ops:    []model.Op{patchOp(ref, map[string]any{"title": "stale must not land"})},
		Expect: []model.Precondition{{Ref: ref, Revision: &old}},
	}, f.meta("stale"))
	errorCode(t, err, "precondition_failed")
	f.assertLog(t, 2)
	if got := f.document(t, ref.ID).Fields["title"]; got != "current" {
		t.Fatalf("stale CAS changed the record: %v", got)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	outcomes := make(chan error, 2)
	for i, field := range []string{"title", "description"} {
		wg.Add(1)
		go func(i int, field string) {
			defer wg.Done()
			<-start
			revision := updated.Revision
			_, err := f.writer.Apply(f.ctx, f.by, model.ChangeSet{
				Ops:    []model.Op{patchOp(ref, map[string]any{field: fmt.Sprintf("winner-%d", i)})},
				Expect: []model.Precondition{{Ref: ref, Revision: &revision}},
			}, f.meta(fmt.Sprintf("race-%d", i)))
			outcomes <- err
		}(i, field)
	}
	close(start)
	wg.Wait()
	close(outcomes)
	wins, refusals := 0, 0
	for err := range outcomes {
		if err == nil {
			wins++
		} else {
			errorCode(t, err, "precondition_failed")
			refusals++
		}
	}
	if wins != 1 || refusals != 1 {
		t.Fatalf("disjoint CAS writes: wins=%d refusals=%d", wins, refusals)
	}
	fields := f.document(t, ref.ID).Fields
	titleWon := fields["title"] == "winner-0"
	descriptionWon := fields["description"] == "winner-1"
	if titleWon == descriptionWon || (descriptionWon && fields["title"] != "current") {
		t.Fatalf("landed state does not contain exactly one winner: %+v", fields)
	}
	f.assertLog(t, 3)
	versions := f.versions(t, ref)
	if len(versions) != 3 {
		t.Fatalf("want one version per landed write, got %d", len(versions))
	}
}

type postFunc func(context.Context, port.CommittedChange, port.SnapshotReader) error

func (fn postFunc) After(ctx context.Context, change port.CommittedChange, reader port.SnapshotReader) error {
	return fn(ctx, change, reader)
}

type validatorFunc func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error)

func (fn validatorFunc) Validate(ctx context.Context, change port.ProposedChange, reader port.SnapshotReader) (port.Verdict, error) {
	return fn(ctx, change, reader)
}

func TestB28CommittedPostWarningAndReplay(t *testing.T) {
	var calls atomic.Int32
	var sawCommit atomic.Bool
	f := newCore(t, []string{"pl-warning"}, pipeline.WithPostSubscribers(pipeline.PostBinding{
		ID: "x.accept/failing-post",
		Subscriber: postFunc(func(ctx context.Context, change port.CommittedChange, reader port.SnapshotReader) error {
			calls.Add(1)
			view := reader.Snapshot()
			if view.At != nil && view.At.String() == change.Revision.String() {
				rows, err := reader.Get(ctx, []model.Ref{model.RefByID("pl-warning", model.Natural64(0))})
				if err == nil && len(rows) == 1 && rows[0].Revision.String() == change.Revision.String() {
					sawCommit.Store(true)
				}
			}
			return errors.New("controlled destination failure")
		}),
	}))
	cs := model.ChangeSet{Ops: []model.Op{createOp("bead", "committed despite post failure")}}
	first := f.apply(t, "post-warning", cs)
	if len(first.Warnings) != 1 || first.Warnings[0].Code != "post_subscriber_failed" {
		t.Fatalf("committed post failure did not return a coded warning: %+v", first)
	}
	if !sawCommit.Load() {
		t.Fatal("post callback could not read the actual committed record/revision")
	}
	if got := f.document(t, "pl-warning").Fields["title"]; got != "committed despite post failure" {
		t.Fatalf("post failure lost committed state: %v", got)
	}
	replay := f.apply(t, "post-warning", cs)
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(replay)
	if err := acceptance.EqualJSON(a, b); err != nil {
		t.Fatalf("replay did not preserve the committed warning: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("post ran %d times for one idempotency key", calls.Load())
	}
	f.assertLog(t, 1)
}

func TestB28FailOpenPreReturnsExplicitWarning(t *testing.T) {
	f := newCore(t, []string{"pl-fail-open"}, pipeline.WithValidators(pipeline.ValidatorBinding{
		ID: "x.accept/optional-pre", FailOpen: true,
		Validator: validatorFunc(func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error) {
			return port.Verdict{}, errors.New("controlled optional validator failure")
		}),
	}))
	commit := f.apply(t, "fail-open", model.ChangeSet{Ops: []model.Op{createOp("bead", "committed with warning")}})
	if len(commit.Warnings) != 1 || commit.Warnings[0].Code != "subscriber_skipped" || commit.Warnings[0].Details["binding"] != "x.accept/optional-pre" {
		t.Fatalf("fail-open pre failure must be an explicit warning: %+v", commit)
	}
	if got := f.document(t, "pl-fail-open").Fields["title"]; got != "committed with warning" {
		t.Fatalf("fail-open write did not commit: %v", got)
	}
	f.assertLog(t, 1)
}

func TestB28PreFailureRollsBackWithoutPost(t *testing.T) {
	var posts atomic.Int32
	f := newCore(t, []string{"pl-never"},
		pipeline.WithValidators(pipeline.ValidatorBinding{ID: "x.accept/pre", Validator: validatorFunc(func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error) {
			return port.Verdict{}, errors.New("controlled pre failure")
		})}),
		pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "x.accept/post", Subscriber: postFunc(func(context.Context, port.CommittedChange, port.SnapshotReader) error {
			posts.Add(1)
			return nil
		})}),
	)
	_, err := f.writer.Apply(f.ctx, f.by, model.ChangeSet{Ops: []model.Op{createOp("bead", "must not land")}}, f.meta("pre-refused"))
	errorCode(t, err, "subscriber_unavailable")
	f.assertAbsent(t, "pl-never")
	f.assertLog(t, 0)
	if posts.Load() != 0 {
		t.Fatalf("rollback ran %d post callbacks", posts.Load())
	}
}
