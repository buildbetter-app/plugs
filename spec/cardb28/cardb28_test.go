package cardb28

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
)

func TestB28PrePostOrderingAtCommit(t *testing.T) {
	var mu sync.Mutex
	var phases []string
	mark := func(name string) {
		mu.Lock()
		defer mu.Unlock()
		phases = append(phases, name)
	}
	f := newFixture(t, []string{"pl-b28-order"},
		pipeline.WithMutators(pipeline.MutatorBinding{
			ID: "card-b28/mutator", Owns: []string{"fields.title"},
			Mutator: mutatorFunc(func(_ context.Context, p port.ProposedChange, _ port.SnapshotReader) ([]port.Patch, error) {
				mark("pre-mutator")
				return []port.Patch{{Resource: p.Changes[0].Resource, Paths: map[string]json.RawMessage{"fields.title": json.RawMessage(`"mutated before commit"`)}}}, nil
			}),
		}),
		pipeline.WithValidators(pipeline.ValidatorBinding{
			ID: "card-b28/validator",
			Validator: validatorFunc(func(_ context.Context, p port.ProposedChange, _ port.SnapshotReader) (port.Verdict, error) {
				mark("pre-validator")
				doc, err := pipeline.DecodeDocument(p.Changes[0].After)
				if err != nil {
					return port.Verdict{}, err
				}
				if doc.Fields["title"] != "mutated before commit" {
					return port.Verdict{}, errors.New("validator saw pre-mutation image")
				}
				return port.Verdict{Allow: true}, nil
			}),
		}),
		pipeline.WithPostSubscribers(pipeline.PostBinding{
			ID: "card-b28/post",
			Subscriber: postFunc(func(ctx context.Context, change port.CommittedChange, reader port.SnapshotReader) error {
				mark("post")
				if reader.Snapshot().At == nil || reader.Snapshot().At.String() != change.Revision.String() {
					return errors.New("post reader was not pinned to commit")
				}
				rows, err := reader.Get(ctx, []model.Ref{change.Changes[0].Resource.Ref})
				if err != nil || len(rows) != 1 {
					return errors.New("post could not read committed row")
				}
				doc, err := pipeline.DecodeDocument(rows[0].Body)
				if err != nil || doc.Fields["title"] != "mutated before commit" {
					return errors.New("post saw an uncommitted image")
				}
				return nil
			}),
		}),
	)
	commit := f.write(t, "bead.create", createParams("request image"), key("order"))
	if commit.Revision.String() != "1" || len(commit.Warnings) != 0 || commit.Results[0].Ref.ID != "pl-b28-order" {
		t.Fatalf("ordering commit = %+v", commit)
	}
	mu.Lock()
	got := append([]string(nil), phases...)
	mu.Unlock()
	if !reflect.DeepEqual(got, []string{"pre-mutator", "pre-validator", "post"}) {
		t.Fatalf("subscriber order = %v", got)
	}
	r := f.reader(t)
	defer r.Close()
	if r.Snapshot().At.String() != "1" {
		t.Fatalf("head = %s, want committed revision 1", r.Snapshot().At)
	}
	t.Log("B28 bounded Plug projection: pre mutation/validation precede commit, post reads the committed revision")
}

func TestB28AbortSuppressesCommittedDelivery(t *testing.T) {
	var posts atomic.Int32
	f := newFixture(t, []string{"pl-b28-abort"},
		pipeline.WithValidators(pipeline.ValidatorBinding{ID: "card-b28/fail-closed", Validator: validatorFunc(func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error) {
			return port.Verdict{}, errors.New("controlled pre refusal")
		})}),
		pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "card-b28/post", Subscriber: postFunc(func(context.Context, port.CommittedChange, port.SnapshotReader) error {
			posts.Add(1)
			return nil
		})}),
	)
	_, got := f.roundTrip(t, requestBytes(t, 1, "bead.create", createParams("must not land"), key("abort")))
	if got.Error == nil || got.Error.Data.Code != "subscriber_unavailable" {
		t.Fatalf("refusal = %+v", got)
	}
	if posts.Load() != 0 {
		t.Fatalf("post callbacks after abort = %d", posts.Load())
	}
	r := f.reader(t)
	defer r.Close()
	rows, err := r.Get(f.ctx, []model.Ref{ref("pl-b28-abort")})
	if err != nil || len(rows) != 0 || !r.Snapshot().At.IsZero() {
		t.Fatalf("aborted state rows=%d head=%s err=%v", len(rows), r.Snapshot().At, err)
	}
	t.Log("B28 bounded Plug projection: fail-closed pre refusal emits no committed row, head, or post delivery")
}

func TestB28WarningResultIsDeterministicAndReplaySkipsPost(t *testing.T) {
	var posts atomic.Int32
	f := newFixture(t, []string{"pl-b28-warning"}, pipeline.WithPostSubscribers(pipeline.PostBinding{
		ID: "card-b28/failing-post", Subscriber: postFunc(func(context.Context, port.CommittedChange, port.SnapshotReader) error {
			posts.Add(1)
			return errors.New("controlled destination failure")
		}),
	}))
	raw := requestBytes(t, 1, "bead.create", createParams("committed with warning"), key("warning"))
	firstRaw, first := f.roundTrip(t, raw)
	if first.Error != nil {
		t.Fatalf("first warning write refused: %+v", first.Error)
	}
	var firstCommit model.Commit
	if err := json.Unmarshal(first.Result, &firstCommit); err != nil {
		t.Fatal(err)
	}
	if firstCommit.Revision.String() != "1" || len(firstCommit.Warnings) != 1 || firstCommit.Warnings[0].Code != "post_subscriber_failed" {
		t.Fatalf("warning result = %+v", firstCommit)
	}
	secondRaw, second := f.roundTrip(t, raw)
	if second.Error != nil {
		t.Fatalf("replay warning write refused: %+v", second.Error)
	}
	sameResult(t, first.Result, second.Result)
	if posts.Load() != 1 {
		t.Fatalf("post callback count after replay = %d", posts.Load())
	}
	if len(firstRaw) == 0 || len(secondRaw) == 0 {
		t.Fatal("byte-RPC responses were empty")
	}
	r := f.reader(t)
	defer r.Close()
	rows, err := r.Get(f.ctx, []model.Ref{ref("pl-b28-warning")})
	if err != nil || len(rows) != 1 {
		t.Fatalf("committed warning row count = %d, err=%v", len(rows), err)
	}
	t.Log("B28 bounded Plug projection: post failure is a committed result warning; durable replay is result-stable and callback-free")
}

func TestB28NoOpReplayDoesNotInvokePost(t *testing.T) {
	var posts atomic.Int32
	f := newFixture(t, []string{"pl-b28-noop"}, pipeline.WithPostSubscribers(pipeline.PostBinding{
		ID: "card-b28/post", Subscriber: postFunc(func(context.Context, port.CommittedChange, port.SnapshotReader) error {
			posts.Add(1)
			return nil
		}),
	}))
	created := f.write(t, "bead.create", createParams("stable"), key("seed"))
	if created.Revision.String() != "1" {
		t.Fatalf("seed = %+v", created)
	}
	raw := requestBytes(t, 2, "bead.update", updateParams("pl-b28-noop", "stable"), key("noop"))
	firstRaw, first := f.roundTrip(t, raw)
	if first.Error != nil {
		t.Fatalf("no-op refused: %+v", first.Error)
	}
	secondRaw, second := f.roundTrip(t, raw)
	if second.Error != nil {
		t.Fatalf("no-op replay refused: %+v", second.Error)
	}
	var firstCommit, secondCommit model.Commit
	if err := json.Unmarshal(first.Result, &firstCommit); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(second.Result, &secondCommit); err != nil {
		t.Fatal(err)
	}
	if firstCommit.Revision.String() != "1" || len(firstCommit.Results) != 1 || firstCommit.Results[0].Revision.String() != "1" || len(firstCommit.Warnings) != 0 || !reflect.DeepEqual(firstCommit, secondCommit) {
		t.Fatalf("no-op/replay results changed: first=%+v second=%+v", firstCommit, secondCommit)
	}
	if posts.Load() != 1 {
		t.Fatalf("post callbacks for seed/no-op/replay = %d", posts.Load())
	}
	sameResult(t, first.Result, second.Result)
	if len(firstRaw) == 0 || len(secondRaw) == 0 {
		t.Fatal("byte-RPC no-op responses were empty")
	}
	t.Log("B28 bounded Plug projection: same-value no-op retains its row revision and replay does not redeliver post")
}
