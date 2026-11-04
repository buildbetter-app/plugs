package viewops

import (
	"context"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestExternalDirectBlockerProjectionNeverAuthorizesLocalCollision(t *testing.T) {
	for _, externalFirst := range []bool{true, false} {
		dependent := model.RefByID("dependent", model.Natural64(0))
		target := model.RefByID("collision", model.Natural64(0))
		store := &syntheticStore{rows: []driver.Record{
			{Ref: dependent, Kind: "bead", Revision: model.Natural64(1), Body: explainDocument(t, dependent.ID, "dependent-project")},
			{Ref: target, Kind: "bead", Revision: model.Natural64(1), Body: explainDocument(t, target.ID, "target-project")},
		}, meta: driver.Meta{StoreID: "store", Incarnation: "inc", Head: model.Natural64(1)}}
		checked := 0
		svc := explainService(t, store, func(context.Context, driver.Reader, model.Ref, int) (DirectBlockerVerdict, error) {
			local := DirectBlockerEdge{Blocker: target, Kind: "blocks", State: DirectBlockerUnsatisfied}
			external := DirectBlockerEdge{Blocker: target, Kind: "blocks", State: DirectBlockerUnknown, TargetKind: model.LinkTargetExternal}
			edges := []DirectBlockerEdge{local, external}
			if externalFirst {
				edges[0], edges[1] = edges[1], edges[0]
			}
			return DirectBlockerVerdict{Edges: edges}, nil
		}, nil)
		svc.cfg.AuthorizeRead = func(_ context.Context, _ model.PrincipalRef, resource model.ResourceRef, _ string) error {
			if resource.Ref.ID == target.ID {
				checked++
				return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
			}
			return nil
		}
		result, err := svc.Explain(context.Background(), model.PrincipalRef{Kind: "user", ID: "reader"}, ExplainRequest{ViewID: "issues", ItemID: dependent.ID})
		if err != nil || result.DirectBlockers == nil || checked != 1 || result.DirectBlockers.HiddenCount != 1 || len(result.DirectBlockers.Edges) != 1 {
			t.Fatalf("externalFirst=%v result=%+v checks=%d err=%v", externalFirst, result, checked, err)
		}
		edge := result.DirectBlockers.Edges[0]
		if edge.TargetKind != model.LinkTargetExternal || edge.State != DirectBlockerUnknown || edge.Blocker.ID != target.ID {
			t.Fatalf("external identity lost: %+v", edge)
		}
	}
}
