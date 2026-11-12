package s1relations_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
)

type b28Mutator func(context.Context, port.ProposedChange, port.SnapshotReader) ([]port.Patch, error)

func (fn b28Mutator) Mutate(ctx context.Context, p port.ProposedChange, r port.SnapshotReader) ([]port.Patch, error) {
	return fn(ctx, p, r)
}

type b28Post func(context.Context, port.CommittedChange, port.SnapshotReader) error

func (fn b28Post) After(ctx context.Context, c port.CommittedChange, r port.SnapshotReader) error {
	return fn(ctx, c, r)
}

// The input values below are independent expectations. Callback observations,
// RPC replies and reopened driver rows are checked against them separately.
func TestB28PublicStructuredSnapshotsOrderingAndSuppression(t *testing.T) {
	stores(t, func(t *testing.T, kind string) {
		var mu sync.Mutex
		var phases []string
		var deliveries []port.CommittedChange
		var refuse atomic.Bool
		mark := func(phase string) {
			mu.Lock()
			defer mu.Unlock()
			phases = append(phases, phase)
		}
		mutate := b28Mutator(func(ctx context.Context, p port.ProposedChange, r port.SnapshotReader) ([]port.Patch, error) {
			mark("mutate")
			if r.Snapshot().At == nil || *r.Snapshot().At != p.BaseRevision {
				return nil, errors.New("pre reader is not at the independent base revision")
			}
			var patches []port.Patch
			for _, change := range p.Changes {
				if change.Op.Kind != model.OpCreate {
					continue
				}
				rows, err := r.Get(ctx, []model.Ref{change.Resource.Ref})
				if err != nil || len(rows) != 0 {
					return nil, errors.New("pre create observed a published row")
				}
				doc, err := pipeline.DecodeDocument(change.After)
				if err != nil {
					return nil, err
				}
				value, err := json.Marshal("normalized " + fmt.Sprint(doc.Fields["title"]))
				if err != nil {
					return nil, err
				}
				patches = append(patches, port.Patch{Resource: change.Resource, Paths: map[string]json.RawMessage{"fields.title": value}})
			}
			return patches, nil
		})
		validate := validatorFunc(func(_ context.Context, p port.ProposedChange, _ port.SnapshotReader) (port.Verdict, error) {
			mark("validate")
			if refuse.Load() {
				return port.Verdict{}, errors.New("controlled ordinary validator failure")
			}
			for _, change := range p.Changes {
				if change.Op.Kind == model.OpCreate {
					doc, err := pipeline.DecodeDocument(change.After)
					if err != nil || !strings.HasPrefix(fmt.Sprint(doc.Fields["title"]), "normalized ") || change.SetBy["fields.title"] != "b28/normalize" {
						return port.Verdict{}, errors.New("validator missed mutator value or attribution")
					}
				}
			}
			return port.Verdict{Allow: true}, nil
		})
		post := b28Post(func(ctx context.Context, c port.CommittedChange, r port.SnapshotReader) error {
			mark("post")
			if r.Snapshot().At == nil || *r.Snapshot().At != c.Revision {
				return errors.New("post reader is not at committed revision")
			}
			for _, change := range c.Changes {
				rows, err := r.Get(ctx, []model.Ref{change.Resource.Ref})
				if err != nil {
					return err
				}
				if change.Op.Kind == model.OpDelete {
					if len(rows) != 0 || len(change.Before) == 0 || len(change.After) != 0 && string(change.After) != "null" {
						return errors.New("delete snapshot retained live row or lost before image")
					}
				} else if len(rows) != 1 || !bytes.Equal(rows[0].Body, change.After) {
					return errors.New("post image does not equal committed readback")
				}
			}
			mu.Lock()
			defer mu.Unlock()
			deliveries = append(deliveries, c)
			return nil
		})
		f := newFixture(t, kind, []string{"b28-first", "b28-second"},
			pipeline.WithMutators(pipeline.MutatorBinding{ID: "b28/normalize", Owns: []string{"fields.title"}, Mutator: mutate}),
			pipeline.WithValidators(pipeline.ValidatorBinding{ID: "b28/validate", Validator: validate}),
			pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "b28/observe", Subscriber: post}),
		)
		seedOps := []model.Op{
			{Kind: model.OpCreate, Create: &model.CreateOp{Kind: model.OpCreate, Type: "issue", Fields: map[string]any{"title": "first"}}},
			{Kind: model.OpCreate, Create: &model.CreateOp{Kind: model.OpCreate, Type: "issue", Fields: map[string]any{"title": "second"}}},
		}
		seed := f.batch("b28-seed", seedOps...)
		first, second := ref("b28-first"), ref("b28-second")
		commits := []model.Commit{seed,
			f.write("bead.update", "b28-patch", map[string]any{"id": first, "fields": map[string]any{"title": "edited"}}),
			f.batch("b28-comment", model.Op{Kind: model.OpComment, Comment: &model.CommentOp{Kind: model.OpComment, Target: first, Body: "exact comment α\nsecond line"}}),
			f.batch("b28-link", link(first, second, "relates-to")),
			f.batch("b28-unlink", model.Op{Kind: model.OpUnlink, Link: &model.LinkOp{Kind: model.OpUnlink, From: first, To: second, LinkKind: "relates-to"}}),
			f.batch("b28-delete", model.Op{Kind: model.OpDelete, Delete: &model.DeleteOp{Kind: model.OpDelete, Target: second}}),
		}
		mu.Lock()
		observed := append([]port.CommittedChange(nil), deliveries...)
		observedPhases := append([]string(nil), phases...)
		mu.Unlock()
		if len(observed) != 6 {
			t.Fatalf("fresh RPC deliveries=%d want 6", len(observed))
		}
		for i, commit := range commits {
			if commit.Revision.String() != fmt.Sprint(i+1) || len(commit.Warnings) != 0 || observed[i].Revision != commit.Revision || observed[i].Principal.ID != actor.ID {
				t.Fatalf("commit/delivery %d: %+v %+v", i, commit, observed[i])
			}
		}
		wantPhases := make([]string, 0, 18)
		for range commits {
			wantPhases = append(wantPhases, "mutate", "validate", "post")
		}
		if !reflect.DeepEqual(observedPhases, wantPhases) {
			t.Fatalf("public callback ordering=%v want=%v", observedPhases, wantPhases)
		}
		if len(observed[0].Changes) != 2 || observed[0].Changes[0].Resource.Ref.ID != first.ID || observed[0].Changes[1].Resource.Ref.ID != second.ID {
			t.Fatal("multi-create operation order changed")
		}
		imageTitle := func(raw json.RawMessage, title string) pipeline.Document {
			t.Helper()
			doc, err := pipeline.DecodeDocument(raw)
			if err != nil || doc.Fields["title"] != title {
				t.Fatalf("snapshot title want=%q raw=%s err=%v", title, raw, err)
			}
			return doc
		}
		for i, title := range []string{"normalized first", "normalized second"} {
			change := observed[0].Changes[i]
			if change.Op.Kind != model.OpCreate || len(change.Before) != 0 && string(change.Before) != "null" {
				t.Fatalf("create before/op=%+v", change)
			}
			imageTitle(change.After, title)
		}
		imageTitle(observed[1].Changes[0].Before, "normalized first")
		imageTitle(observed[1].Changes[0].After, "edited")
		beforeComment := imageTitle(observed[2].Changes[0].Before, "edited")
		afterComment := imageTitle(observed[2].Changes[0].After, "edited")
		if observed[2].Changes[0].Op.Kind != model.OpComment || len(beforeComment.Comments) != 0 || len(afterComment.Comments) != 1 || afterComment.Comments[0].Body != "exact comment α\nsecond line" || !reflect.DeepEqual(afterComment.Comments[0].Principal, actor) || afterComment.Comments[0].Time != fixedTime {
			t.Fatalf("structured comment before=%+v after=%+v", beforeComment, afterComment)
		}
		linked := imageTitle(observed[3].Changes[0].After, "edited")
		if observed[3].Changes[0].Op.Kind != model.OpLink || len(linked.Links) != 1 || linked.Links[0].Kind != "relates-to" || linked.Links[0].To.ID != second.ID {
			t.Fatalf("structured dependency=%+v", linked.Links)
		}
		if len(imageTitle(observed[4].Changes[0].Before, "edited").Links) != 1 || len(imageTitle(observed[4].Changes[0].After, "edited").Links) != 0 {
			t.Fatal("unlink lost structured before/after dependency")
		}
		imageTitle(observed[5].Changes[0].Before, "normalized second")
		// Exact independent current values are checked through public get as well
		// as the committed snapshot reader, then again after an SQL close/open.
		assertCurrent := func() {
			t.Helper()
			got := f.call("bead.get", "b28-current", map[string]any{"id": first})
			if got.Error != nil {
				t.Fatalf("current read refused: %+v", got)
			}
			var current struct {
				Bead struct {
					Fields   map[string]any `json:"fields"`
					Revision model.Revision `json:"revision"`
				} `json:"bead"`
				Comments []struct {
					Body string `json:"body"`
				} `json:"comments"`
			}
			if json.Unmarshal(got.Result, &current) != nil || current.Bead.Fields["title"] != "edited" || current.Bead.Revision.String() != "5" || len(current.Comments) != 1 || current.Comments[0].Body != "exact comment α\nsecond line" {
				t.Fatalf("public current readback=%s", got.Result)
			}
			if doc := f.document(first.ID); len(doc.Links) != 0 || !reflect.DeepEqual(doc.Comments[0].Principal, actor) || doc.Comments[0].Time != fixedTime {
				t.Fatalf("stored structured current tuple=%+v", doc)
			}
			deleted := f.call("bead.get", "b28-deleted", map[string]any{"id": second})
			if deleted.Error == nil || deleted.Error.Data.Code != "not_found" {
				t.Fatalf("deleted public readback=%+v", deleted)
			}
		}
		assertCurrent()
		before := f.image()
		refuse.Store(true)
		refused := f.call("bead.update", "b28-refusal", map[string]any{"id": first, "fields": map[string]any{"title": "must refuse"}})
		if refused.Error == nil || refused.Error.Data.Code != "subscriber_unavailable" || len(refused.Result) != 0 || f.receipt("bead.update", "b28-refusal") != nil || !bytes.Equal(before, f.image()) {
			t.Fatalf("pre refusal published state/result: %+v", refused)
		}
		refuse.Store(false)
		// An ordinary malformed edge after a valid proposed patch must refuse
		// the whole public request without publishing the earlier proposal.
		rollback := f.call("tx.apply", "b28-rollback", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
			patch(first, map[string]any{"title": "must roll back"}), link(first, first, "blocks"),
		}}})
		if rollback.Error == nil || rollback.Error.Data.Code != "ontology_guard_failed" || len(rollback.Result) != 0 || f.receipt("tx.apply", "b28-rollback") != nil || !bytes.Equal(before, f.image()) {
			t.Fatalf("ordinary multi-operation refusal published effects: %+v", rollback)
		}
		noop := f.write("bead.update", "b28-noop", map[string]any{"id": first, "fields": map[string]any{"title": "edited"}})
		if noop.Revision.String() != "6" || len(noop.Results) != 1 || noop.Results[0].Ref.ID != first.ID || noop.Results[0].Revision.String() != "5" || len(noop.Warnings) != 0 || !bytes.Equal(before, f.image()) {
			t.Fatalf("no-op changed committed graph: %+v", noop)
		}
		f.reopen()
		assertCurrent()
		replay := f.call("tx.apply", "b28-seed", map[string]any{"changeSet": model.ChangeSet{Ops: seedOps}})
		if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(before, f.image()) {
			t.Fatalf("old response replay changed current graph: %+v", replay)
		}
		equalJSON(t, json.RawMessage(jsonBytes(t, seed)), replay.Result)
		assertCurrent()
		mu.Lock()
		count := len(deliveries)
		mu.Unlock()
		if count != 6 {
			t.Fatalf("refusal/no-op/reopen/replay redelivered subscriber: %d", count)
		}
		if dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); dir != "" {
			if err := os.WriteFile(filepath.Join(dir, "b28-structured-"+kind+".json"), jsonBytes(t, observed), 0600); err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestB28PublicExplicitPreFailOpenCommitsWarningAndReopens(t *testing.T) {
	for _, lane := range []string{"mutator", "validator"} {
		t.Run(lane, func(t *testing.T) {
			stores(t, func(t *testing.T, kind string) {
				var calls atomic.Int32
				options := []pipeline.Option{pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "b28/observe", Subscriber: b28Post(func(context.Context, port.CommittedChange, port.SnapshotReader) error { calls.Add(1); return nil })})}
				if lane == "mutator" {
					options = append(options, pipeline.WithMutators(pipeline.MutatorBinding{ID: "b28/unavailable", FailOpen: true, Mutator: b28Mutator(func(context.Context, port.ProposedChange, port.SnapshotReader) ([]port.Patch, error) {
						return nil, errors.New("controlled pre detail")
					})}))
				} else {
					options = append(options, pipeline.WithValidators(pipeline.ValidatorBinding{ID: "b28/unavailable", FailOpen: true, Validator: validatorFunc(func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error) {
						return port.Verdict{}, errors.New("controlled pre detail")
					})}))
				}
				f := newFixture(t, kind, []string{"b28-open"}, options...)
				params := map[string]any{"type": "issue", "fields": map[string]any{"title": "exact fail-open value"}}
				commit := f.write("bead.create", "b28-open", params)
				if commit.Revision.String() != "1" || len(commit.Warnings) != 1 || commit.Warnings[0].Code != "subscriber_skipped" || commit.Warnings[0].Details["binding"] != "b28/unavailable" || calls.Load() != 1 || bytes.Contains(jsonBytes(t, commit), []byte("controlled pre detail")) {
					t.Fatalf("explicit fail-open result=%+v calls=%d", commit, calls.Load())
				}
				if f.document("b28-open").Fields["title"] != "exact fail-open value" {
					t.Fatal("fail-open altered request value")
				}
				receipt, image := f.receipt("bead.create", "b28-open"), f.image()
				if receipt == nil || receipt.ResponsePending {
					t.Fatal("fail-open warning not finalized durably")
				}
				equalJSON(t, commit, receipt.Commit)
				f.reopen()
				replay := f.call("bead.create", "b28-open", params)
				if replay.Error != nil || !replay.Meta.Replayed || calls.Load() != 1 || !bytes.Equal(image, f.image()) || f.document("b28-open").Fields["title"] != "exact fail-open value" {
					t.Fatalf("fail-open reopened replay=%+v calls=%d", replay, calls.Load())
				}
				equalJSON(t, json.RawMessage(jsonBytes(t, commit)), replay.Result)
				equalReceipt(t, receipt, f.receipt("bead.create", "b28-open"))
			})
		})
	}
}
