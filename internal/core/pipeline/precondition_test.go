package pipeline

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
)

func TestHashCASRefusalRequiresExpectationAuthorization(t *testing.T) {
	for _, selector := range []string{"fields.title", "metadata.missing"} {
		for _, disposition := range []string{"forbidden", "not_found", "allow"} {
			t.Run(selector+"/"+disposition, func(t *testing.T) {
				h := setup(t)
				seed := apply(t, h.writer(), model.ChangeSet{Ops: []model.Op{create("target"), create("expectation-only")}}, meta(1))
				target, guard := seed.Results[0].Ref, seed.Results[1].Ref
				cs := model.ChangeSet{
					Ops:    []model.Op{patch(target, map[string]any{"title": "must-not-publish"})},
					Expect: []model.Precondition{model.PreconditionByHash(guard, selector, metadataDigest(`"stale"`))},
				}
				var decisions, mutations, posts atomic.Int32
				w := h.writer(
					WithAuthorizer(authorizerFunc(func(_ context.Context, stage port.AuthorizationStage, p port.ProposedChange, _ port.SnapshotReader) error {
						decisions.Add(1)
						if stage != port.AuthorizeBefore || len(p.Expect) != 1 || p.Expect[0].Ref.ID != guard.ID || len(p.Changes) != 1 || p.Changes[0].Resource.Ref.ID != target.ID {
							return failure("internal_error", "expectation-only target did not reach pre-image authorization")
						}
						if disposition != "allow" {
							return failure(model.ErrorCode(disposition), "expectation target is hidden or denied")
						}
						return nil
					})),
					WithMutators(MutatorBinding{ID: "must-not-run", Mutator: mutatorFunc(func(context.Context, port.ProposedChange, port.SnapshotReader) ([]port.Patch, error) {
						mutations.Add(1)
						return nil, nil
					})}),
					WithPostSubscribers(PostBinding{ID: "must-not-publish", Subscriber: postFunc(func(context.Context, port.CommittedChange, port.SnapshotReader) error {
						posts.Add(1)
						return nil
					})}),
				)
				before := metadataGraph(t, h)
				result, err := w.Apply(context.Background(), by, cs, meta(2))
				if !reflect.DeepEqual(result, model.Commit{}) || !reflect.DeepEqual(before, metadataGraph(t, h)) || metadataReceipt(t, h, meta(2)) != nil {
					t.Fatal("hash refusal published response, graph, versions, predicates, history or receipt")
				}
				if decisions.Load() != 1 || mutations.Load() != 0 || posts.Load() != 0 {
					t.Fatalf("hash refusal crossed callback boundary: decisions=%d mutators=%d posts=%d", decisions.Load(), mutations.Load(), posts.Load())
				}
				var typed *model.Error
				if !errors.As(err, &typed) || typed.Data.Retryable {
					t.Fatalf("expected nonretryable typed refusal: %v", err)
				}
				if disposition != "allow" {
					code(t, err, disposition)
					if typed.Data.Details != nil {
						t.Fatalf("denied/hidden hash CAS disclosed private refusal: %+v", typed.Data.Details)
					}
				} else {
					code(t, err, "precondition_failed")
					if !reflect.DeepEqual(typed.Data.Details, map[string]any{"reason": "hash", "ref": guard}) {
						t.Fatalf("authorized hash CAS must release exactly its reason and resolved guard address: %+v", typed.Data.Details)
					}
				}
			})
		}
	}
}
