package rpc

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

// CompactionService binds only implemented capabilities. Generic historical
// body replacement and protected-record restore are deliberately absent.
type CompactionService interface {
	CompactWithOutcome(context.Context, model.PrincipalRef, service.CompactionRequest, model.WriteMeta) (port.WriteOutcome, error)
	RevertCompactionWithOutcome(context.Context, model.PrincipalRef, service.CompactionRevertRequest, model.WriteMeta) (port.WriteOutcome, error)
}

func dispatchCompaction(ctx context.Context, svc CompactionService, c Call) (any, error) {
	if err := requireAdminCaller(ctx, c.Principal); err != nil {
		return nil, err
	}
	switch c.Method {
	case "admin.compact":
		var p struct {
			BeforeRevision *model.Revision `json:"beforeRevision"`
			AfterID        *string         `json:"afterId"`
			DryRun         bool            `json:"dryRun"`
			Meta           wire.Meta       `json:"_meta"`
		}
		if err := wire.DecodeInto(c.Params, &p); err != nil {
			return nil, invalidCompactionParams(err)
		}
		out, err := svc.CompactWithOutcome(ctx, c.Principal, service.CompactionRequest{BeforeRevision: p.BeforeRevision, AfterID: p.AfterID, DryRun: p.DryRun}, writeMeta(c))
		return writeOutcome(out), err
	case "history.revert":
		var p struct {
			Ref           model.Ref            `json:"ref"`
			BaseRevision  model.Revision       `json:"baseRevision"`
			Expect        []model.Precondition `json:"expect"`
			HumanPresence map[string]any       `json:"humanPresence"`
			Meta          wire.Meta            `json:"_meta"`
		}
		if err := wire.DecodeInto(c.Params, &p); err != nil {
			return nil, invalidCompactionParams(err)
		}
		out, err := svc.RevertCompactionWithOutcome(ctx, c.Principal, service.CompactionRevertRequest{Ref: p.Ref, BaseRevision: p.BaseRevision, Expect: p.Expect, HumanPresence: p.HumanPresence}, writeMeta(c))
		return writeOutcome(out), err
	}
	return nil, &model.Error{Data: model.ErrorData{Code: "method_not_found"}}
}

func invalidCompactionParams(err error) error {
	return &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
}
