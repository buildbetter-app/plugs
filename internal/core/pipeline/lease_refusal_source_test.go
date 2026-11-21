package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// A joint delete in source-operation mode, [delete claimed bead X, delete
// workflow item W], stops ordinary preparation at X's lease refusal. The
// provider must stop with it: this fake provider, like a real one, plans on
// the whole changeset and reports W's final result at position 1, which a
// half-prepared batch never handled. If the provider kept going, that result
// would surface as internal_error before authorization, and only while X is
// claimed. Instead an authorized caller gets the lease refusal, an ungranted
// caller gets forbidden, and nothing is published.
func TestLeaseRefusalStopsSourcePreparationBeforeAuthorization(t *testing.T) {
	h := setup(t)
	ctx := context.Background()
	w := h.writer(WithOntology(&sameClaimOntology{}))
	seed := apply(t, w, model.ChangeSet{Ops: []model.Op{create("")}}, meta(1))
	x := seed.Results[0].Ref
	claim := meta(2)
	claim.Method = "bead.claim"
	apply(t, w, model.ChangeSet{Ops: []model.Op{patch(x, map[string]any{})}}, claim)

	ungranted := model.PrincipalRef{Kind: "user", ID: "ungranted"}
	other := model.PrincipalRef{Kind: "user", ID: "other"}
	var authorized []string
	gated := h.writer(WithOntology(&sameClaimOntology{}), WithAuthorizer(authorizerFunc(func(_ context.Context, stage port.AuthorizationStage, proposal port.ProposedChange, _ port.SnapshotReader) error {
		if stage == port.AuthorizeBefore {
			authorized = append(authorized, proposal.Principal.ID)
		}
		if proposal.Principal.ID == ungranted.ID {
			return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
		}
		return nil
	})))

	workflow := model.RefByID("workflow:w", model.Natural64(0))
	kind := func(ref model.Ref) string {
		if ref.ID == workflow.ID {
			return "workflow_item"
		}
		return ""
	}
	var stopped, reported int
	provider := func(ctx context.Context, reader driver.Reader, run func(SourceOperationHandler) error) (SourcePublication, error) {
		err := run(func(_ context.Context, op model.Op, _ SourceCoreImage) (model.Op, *SourceOperation, error) {
			if op.Kind == model.OpDelete && op.Delete.Target.ID == workflow.ID {
				return op, &SourceOperation{
					Change: port.Change{Resource: model.ResourceRef{Kind: "workflow_item", Ref: workflow}, Op: op},
					Result: model.OpResult{Ref: workflow},
				}, nil
			}
			return op, nil, nil
		})
		if err != nil {
			stopped++
			return SourcePublication{}, err
		}
		reported++
		snapshot := reader.Snapshot()
		meta, metaErr := h.store.Meta(ctx)
		if metaErr != nil {
			return SourcePublication{}, metaErr
		}
		head := driver.CoreSnapshotExpectation{StoreID: meta.StoreID, Incarnation: snapshot.Incarnation, Head: *snapshot.At}
		return SourcePublication{
			Head:         head,
			Namespace:    driver.CheckpointExpectation{StoreID: meta.StoreID, Incarnation: snapshot.Incarnation, Key: "plug/test/joint-delete"},
			FinalResults: map[int]model.OpResult{1: {Ref: workflow}},
		}, nil
	}
	cs := model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: x}},
		{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: workflow}},
	}}
	before, err := h.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i, row := range []struct {
		by   model.PrincipalRef
		want string
	}{{other, "lease_not_held"}, {ungranted, "forbidden"}} {
		authorized, stopped, reported = nil, 0, 0
		m := meta(10 + i)
		_, err := gated.ApplyWithSourceOperations(ctx, row.by, cs, m, provider, kind)
		var typed *model.Error
		if errors.As(err, &typed) && typed.Data.Code == "internal_error" {
			t.Fatalf("%s: joint delete over a claimed bead returned internal_error: %v", row.by.ID, err)
		}
		code(t, err, row.want)
		if stopped != 1 || reported != 0 {
			t.Fatalf("%s: provider was not stopped at the lease refusal: stopped=%d reported=%d", row.by.ID, stopped, reported)
		}
		if len(authorized) != 1 || authorized[0] != row.by.ID {
			t.Fatalf("%s: lease refusal was not authorized exactly once before release: %v", row.by.ID, authorized)
		}
		after, err := h.store.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if after.Head.String() != before.Head.String() {
			t.Fatalf("%s: refused joint delete published revision %s", row.by.ID, after.Head)
		}
		principal, err := driver.PrincipalKey(row.by)
		if err != nil {
			t.Fatal(err)
		}
		receipt, err := h.read(t).IdempotencyByKey(ctx, principal, m.AgentLabel, m.Method, m.IdempotencyKey)
		if err != nil || receipt != nil {
			t.Fatalf("%s: refused joint delete stored a receipt: %+v %v", row.by.ID, receipt, err)
		}
	}
}
