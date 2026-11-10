package rpc

import (
	"context"
	"encoding/json"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

type GateListingService interface {
	GateList(context.Context, model.PrincipalRef, service.GateListRequest) (service.BeadPage, error)
}

func dispatchGateList(ctx context.Context, svc GateListingService, call Call) (any, error) {
	var params struct {
		All              bool        `json:"all"`
		ExcludeTemplates bool        `json:"excludeTemplates"`
		Limit            json.Number `json:"limit"`
		AwaitType        string      `json:"awaitType"`
		Meta             wire.Meta   `json:"_meta"`
	}
	if err := wire.DecodeInto(call.Params, &params); err != nil {
		return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
	}
	var limit int64
	if params.Limit != "" {
		var err error
		limit, err = params.Limit.Int64()
		if err != nil || limit < 0 {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: "gate limit must be a nonnegative int64"}}
		}
	}
	page, err := svc.GateList(ctx, call.Principal, service.GateListRequest{Project: call.Meta.Project, Agent: call.Meta.Agent, All: params.All, ExcludeTemplates: params.ExcludeTemplates, Limit: limit, AwaitType: params.AwaitType})
	if err != nil {
		return nil, err
	}
	return pageJSON(page), nil
}
