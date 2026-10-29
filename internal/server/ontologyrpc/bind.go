// Package ontologyrpc binds actual ontology reads and migration planning to
// the canonical registry without changing shared descriptors or BindService.
package ontologyrpc

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontologyops"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
)

type Service interface {
	Get(context.Context, model.PrincipalRef, string) (ontologyops.Result, error)
	Describe(context.Context, model.PrincipalRef, string, string) (ontologyops.Result, error)
	Schema(context.Context, model.PrincipalRef, string, string, string) (map[string]any, error)
	Check(context.Context, model.PrincipalRef, string) (ontologyops.CheckResult, error)
	Plan(context.Context, model.PrincipalRef, ontologyops.PlanRequest) (ontologyops.PlanResult, error)
	History(context.Context, model.PrincipalRef, string, model.Revision, int) (ontologyops.HistoryResult, error)
	Diff(context.Context, model.PrincipalRef, string, model.Revision, model.Revision) (ontologyops.DiffResult, error)
}

var names = []string{"ontology.get", "ontology.describe", "ontology.schema", "ontology.check", "ontology.plan", "ontology.history", "ontology.diff"}

// Bind must run before rpc.New snapshots the registry. All bindings use the
// authoritative catalog. ontology.apply is deliberately absent: no proof or
// atomic apply capability is implemented by this slice.
func Bind(registry *rpc.Registry, service Service) error {
	if registry == nil || service == nil {
		return fmt.Errorf("ontology binding requires a registry and service")
	}
	bound := map[string]bool{}
	for _, descriptor := range registry.Descriptors() {
		bound[descriptor.Name] = true
	}
	for _, name := range names {
		if bound[name] {
			return fmt.Errorf("duplicate handler: %s", name)
		}
	}
	for _, name := range names {
		if err := registry.Register(name, func(ctx context.Context, call rpc.Call) (any, error) { return dispatch(ctx, service, call) }); err != nil {
			return err
		}
	}
	return nil
}

type params struct {
	Project      *string              `json:"project,omitempty"`
	Type         string               `json:"type,omitempty"`
	Status       string               `json:"status,omitempty"`
	Document     json.RawMessage      `json:"document,omitempty"`
	Expect       []model.Precondition `json:"expect,omitempty"`
	After        model.Revision       `json:"after,omitempty"`
	Limit        model.Natural        `json:"limit,omitempty"`
	FromRevision model.Revision       `json:"fromRevision,omitempty"`
	ToRevision   model.Revision       `json:"toRevision,omitempty"`
	Meta         wire.Meta            `json:"_meta,omitempty"`
}

func dispatch(ctx context.Context, service Service, call rpc.Call) (any, error) {
	var p params
	if err := wire.DecodeInto(call.Params, &p); err != nil {
		return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
	}
	project := call.Meta.Project
	if p.Project != nil {
		project = *p.Project
	}
	switch call.Method {
	case "ontology.get":
		return service.Get(ctx, call.Principal, project)
	case "ontology.describe":
		return service.Describe(ctx, call.Principal, project, p.Type)
	case "ontology.schema":
		return service.Schema(ctx, call.Principal, project, p.Type, p.Status)
	case "ontology.check":
		return service.Check(ctx, call.Principal, project)
	case "ontology.plan":
		return service.Plan(ctx, call.Principal, ontologyops.PlanRequest{Project: project, Document: p.Document, Expect: p.Expect})
	case "ontology.history":
		limit, err := p.Limit.Uint64()
		if err != nil || limit > 1000 {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: "limit outside 1..1000"}}
		}
		return service.History(ctx, call.Principal, project, p.After, int(limit))
	case "ontology.diff":
		return service.Diff(ctx, call.Principal, project, p.FromRevision, p.ToRevision)
	default:
		return nil, &model.Error{Data: model.ErrorData{Code: "method_not_found"}}
	}
}
