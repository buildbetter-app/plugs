package rpc

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

// CyclesService is additive; a wrapper without the real producer stays unbound.
type CyclesService interface {
	Cycles(context.Context, model.PrincipalRef, service.CyclesRequest) (service.CyclesResult, error)
}

func dispatchCycles(ctx context.Context, svc CyclesService, call Call) (any, error) {
	var params struct {
		IncludeTracks    bool          `json:"includeTracks"`
		IncludeEphemeral bool          `json:"includeEphemeral"`
		Limit            model.Natural `json:"limit"`
		Meta             wire.Meta     `json:"_meta"`
	}
	if err := wire.DecodeInto(call.Params, &params); err != nil {
		return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params"}}
	}
	limit, err := params.Limit.Uint64()
	if err != nil || limit > 1000 {
		return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params"}}
	}
	return svc.Cycles(ctx, call.Principal, service.CyclesRequest{
		Project: call.Meta.Project, IncludeTracks: params.IncludeTracks,
		IncludeEphemeral: params.IncludeEphemeral, Limit: int(limit),
	})
}
