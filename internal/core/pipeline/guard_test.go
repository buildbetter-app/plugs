package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func assertEmpty(t *testing.T, h *harness) {
	t.Helper()
	r := h.read(t)
	page, err := r.Query(context.Background(), driver.Query{})
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("records=%+v err=%v", page, err)
	}
	log, err := r.Changesets(context.Background(), nil, 100)
	if err != nil || len(log) != 0 || !r.Snapshot().At.IsZero() {
		t.Fatalf("head=%s log=%+v err=%v", r.Snapshot().At, log, err)
	}
}

func TestFailedPreAndGuardPublishNothing(t *testing.T) {
	cases := []struct {
		name, code string
		option     Option
	}{
		{"mutator", "subscriber_unavailable", WithMutators(MutatorBinding{ID: "broken", Mutator: mutatorFunc(func(context.Context, port.ProposedChange, port.SnapshotReader) ([]port.Patch, error) {
			return nil, errors.New("fault")
		})})},
		{"validator-error", "subscriber_unavailable", WithValidators(ValidatorBinding{ID: "broken", Validator: validatorFunc(func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error) {
			return port.Verdict{}, errors.New("fault")
		})})},
		{"validator-verdict", "ontology_guard_failed", WithValidators(ValidatorBinding{ID: "deny", Validator: validatorFunc(func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error) {
			return port.Verdict{Allow: false}, nil
		})})},
		{"ontology-guard", "ontology_guard_failed", WithOntology(testOntology{reject: true})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := setup(t)
			var posts atomic.Int32
			w := h.writer(tc.option, WithPostSubscribers(PostBinding{ID: "post", Subscriber: postFunc(func(context.Context, port.CommittedChange, port.SnapshotReader) error { posts.Add(1); return nil })}))
			_, err := w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{create("new")}}, meta(1))
			code(t, err, tc.code)
			assertEmpty(t, h)
			if posts.Load() != 0 {
				t.Fatal("post called for rejected write")
			}
		})
	}
}

func TestFailedBatchPublishesNoPartialCreate(t *testing.T) {
	h := setup(t)
	_, err := h.writer().Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{create("first"), patch(model.Ref{ID: "missing"}, map[string]any{"title": "bad"})}}, meta(1))
	code(t, err, "not_found")
	assertEmpty(t, h)
	_, err = h.writer().Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{patch(model.Ref{Key: "later"}, map[string]any{}), create("later")}}, meta(2))
	code(t, err, "invalid_params")
	assertEmpty(t, h)
}

func TestObservedReadRetriesAndRefusesAfterBound(t *testing.T) {
	for _, continual := range []bool{false, true} {
		t.Run(fmt.Sprint(continual), func(t *testing.T) {
			h := setup(t)
			ordinary := h.writer()
			seed := apply(t, ordinary, model.ChangeSet{Ops: []model.Op{create("watched"), create("target")}}, meta(1))
			watched, target := seed.Results[0].Ref, seed.Results[1].Ref
			var calls atomic.Int32
			validator := validatorFunc(func(ctx context.Context, _ port.ProposedChange, r port.SnapshotReader) (port.Verdict, error) {
				if _, err := r.Get(ctx, []model.Ref{watched}); err != nil {
					return port.Verdict{}, err
				}
				call := calls.Add(1)
				if continual || call == 1 {
					_, err := ordinary.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{patch(watched, map[string]any{"title": fmt.Sprint(call)})}}, meta(100+int(call)))
					if err != nil {
						return port.Verdict{}, err
					}
				}
				return port.Verdict{Allow: true}, nil
			})
			w := h.writer(WithValidators(ValidatorBinding{ID: "reads-policy", Validator: validator}))
			result, err := w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{patch(target, map[string]any{"title": "committed"})}}, meta(2))
			if continual {
				code(t, err, "write_conflict")
				if calls.Load() != 4 {
					t.Fatalf("calls=%d want4", calls.Load())
				}
				records, e := h.read(t).Get(context.Background(), []model.Ref{target})
				if e != nil {
					t.Fatal(e)
				}
				d, e := DecodeDocument(records[0].Body)
				if e != nil {
					t.Fatal(e)
				}
				if d.Fields["title"] != "initial" {
					t.Fatal("refused attempt published data")
				}
				if h.read(t).Snapshot().At.String() != "5" {
					t.Fatal("refused write allocated a log revision")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if calls.Load() != 2 || result.Revision.String() != "3" {
					t.Fatalf("calls=%d result=%+v", calls.Load(), result)
				}
			}
		})
	}
}

func TestObservedMissAndQueryPhantomAreGuarded(t *testing.T) {
	for _, query := range []bool{false, true} {
		t.Run(fmt.Sprint(query), func(t *testing.T) {
			h := setup(t)
			var calls atomic.Int32
			other := h.writer(WithIDGenerator(func() (string, error) { return "pl-observed", nil }))
			validator := validatorFunc(func(ctx context.Context, _ port.ProposedChange, r port.SnapshotReader) (port.Verdict, error) {
				if query {
					if _, err := r.Query(ctx, driver.Query{Filter: map[string]any{"kind": "bead"}}); err != nil {
						return port.Verdict{}, err
					}
				} else {
					if _, err := r.Get(ctx, []model.Ref{{ID: "pl-observed"}}); err != nil {
						return port.Verdict{}, err
					}
				}
				if calls.Add(1) == 1 {
					if _, err := other.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{create("new")}}, meta(99)); err != nil {
						return port.Verdict{}, err
					}
				}
				return port.Verdict{Allow: true}, nil
			})
			result := apply(t, h.writer(WithValidators(ValidatorBinding{ID: "reads", Validator: validator})), model.ChangeSet{Ops: []model.Op{create("target")}}, meta(1))
			if calls.Load() != 2 || result.Revision.String() != "2" {
				t.Fatalf("miss was not retried: calls=%d result=%+v", calls.Load(), result)
			}
		})
	}
}

func TestReadBudgetAndReentrantWriteFailClosed(t *testing.T) {
	for _, reentrant := range []bool{false, true} {
		t.Run(fmt.Sprint(reentrant), func(t *testing.T) {
			h := setup(t)
			var w *Writer
			validator := validatorFunc(func(ctx context.Context, _ port.ProposedChange, r port.SnapshotReader) (port.Verdict, error) {
				if reentrant {
					_, err := w.Apply(ctx, by, model.ChangeSet{Ops: []model.Op{create("nested")}}, meta(2))
					code(t, err, "plugin_reentrant_write")
					return port.Verdict{}, err
				}
				refs := make([]model.Ref, 33)
				for i := range refs {
					refs[i] = model.Ref{ID: fmt.Sprintf("pl-%d", i)}
				}
				_, err := r.Get(ctx, refs)
				if err == nil {
					return port.Verdict{Allow: true}, nil
				}
				return port.Verdict{}, err
			})
			w = h.writer(WithValidators(ValidatorBinding{ID: "bounded", Validator: validator}))
			_, err := w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{create("new")}}, meta(1))
			code(t, err, "subscriber_unavailable")
			assertEmpty(t, h)
		})
	}
}
