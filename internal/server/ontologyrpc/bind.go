// Package ontologyrpc binds actual ontology reads, planning and protected
// apply to the canonical registry without changing shared descriptors or
// BindService.
package ontologyrpc

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontologyops"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/server/authn"
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

type ApplyService interface {
	Service
	Apply(context.Context, model.PrincipalRef, ontologyops.ApplyRequest, model.WriteMeta) (port.WriteOutcome, error)
}

var names = []string{"ontology.get", "ontology.describe", "ontology.schema", "ontology.check", "ontology.plan", "ontology.history", "ontology.diff"}

type protectedApplyCapability interface{ ProtectedApplyEnabled() bool }

// Bind must run before rpc.New snapshots the registry. All bindings use the
// authoritative catalog. ontology.apply is added only when the supplied
// service explicitly exposes the protected Writer capability.
func Bind(registry *rpc.Registry, service Service) error {
	if registry == nil || service == nil {
		return fmt.Errorf("ontology binding requires a registry and service")
	}
	bound := map[string]bool{}
	for _, descriptor := range registry.Descriptors() {
		bound[descriptor.Name] = true
	}
	bindNames := append([]string(nil), names...)
	if capability, ok := service.(protectedApplyCapability); ok && capability.ProtectedApplyEnabled() {
		if _, ok := service.(ApplyService); !ok {
			return fmt.Errorf("ontology service advertises protected apply without an Apply implementation")
		}
		bindNames = append(bindNames, "ontology.apply")
	}
	for _, name := range bindNames {
		if bound[name] {
			return fmt.Errorf("duplicate handler: %s", name)
		}
	}
	for _, name := range bindNames {
		if err := registry.Register(name, func(ctx context.Context, call rpc.Call) (any, error) { return dispatch(ctx, service, call) }); err != nil {
			return err
		}
	}
	return nil
}

type params struct {
	Project       *string                   `json:"project,omitempty"`
	Type          string                    `json:"type,omitempty"`
	Status        string                    `json:"status,omitempty"`
	Document      json.RawMessage           `json:"document,omitempty"`
	TypeMappings  []ontologyops.TypeMapping `json:"typeMappings,omitempty"`
	Expect        []model.Precondition      `json:"expect,omitempty"`
	After         model.Revision            `json:"after,omitempty"`
	Limit         model.Natural             `json:"limit,omitempty"`
	FromRevision  model.Revision            `json:"fromRevision,omitempty"`
	ToRevision    model.Revision            `json:"toRevision,omitempty"`
	PlanDigest    string                    `json:"planDigest,omitempty"`
	HumanPresence humanPresence             `json:"humanPresence,omitempty"`
	Meta          wire.Meta                 `json:"_meta,omitempty"`
}

type humanPresence struct {
	Mode      string `json:"mode"`
	SessionID string `json:"sessionId,omitempty"`
	Challenge string `json:"challenge"`
	Proof     string `json:"proof"`
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
		return service.Plan(ctx, call.Principal, ontologyops.PlanRequest{Project: project, Document: p.Document, Expect: p.Expect, TypeMappings: p.TypeMappings})
	case "ontology.apply":
		if err := admitHumanApply(ctx, call.Principal, p.PlanDigest, p.HumanPresence); err != nil {
			return nil, err
		}
		meta := model.WriteMeta{Method: call.Method, AgentLabel: call.Meta.Agent, Project: project, IdempotencyKey: call.Meta.IdempotencyKey, DryRun: call.Meta.DryRun, Return: call.Meta.Return, Traceparent: call.Meta.Traceparent, Agent: call.Meta.Agent}
		applyService, ok := service.(ApplyService)
		if !ok {
			return nil, &model.Error{Data: model.ErrorData{Code: "method_not_found"}}
		}
		outcome, err := applyService.Apply(ctx, call.Principal, ontologyops.ApplyRequest{PlanDigest: p.PlanDigest, Expect: p.Expect}, meta)
		if err != nil {
			return nil, err
		}
		return rpc.Outcome{Result: commitResult(outcome.Commit), Replayed: outcome.Replayed}, nil
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

func admitHumanApply(ctx context.Context, by model.PrincipalRef, planDigest string, proof humanPresence) error {
	switch by.Kind {
	case "anonymous":
		if proof.Mode != "presence_check" || proof.Challenge != planDigest || proof.Proof != planDigest {
			return &model.Error{Data: model.ErrorData{Code: "human_presence_required", Hint: "anonymous ontology.apply requires typing the plan digest as the presence challenge and proof"}}
		}
		return nil
	case "user":
		if proof.Mode != "interactive_session" || proof.SessionID == "" || proof.Challenge != planDigest || proof.Proof == "" {
			return &model.Error{Data: model.ErrorData{Code: "human_presence_required", Hint: "ontology.apply requires an interactive user session"}}
		}
		if err := authn.RequireInteractiveUser(ctx, by); err != nil {
			return err
		}
		return nil
	default:
		return &model.Error{Data: model.ErrorData{Code: "human_presence_required", Hint: "API keys, services and agents cannot apply ontology plans"}}
	}
}

func commitResult(commit model.Commit) map[string]any {
	results := commit.Results
	if results == nil {
		results = []model.OpResult{}
	}
	warnings := commit.Warnings
	if warnings == nil {
		warnings = []model.Warning{}
	}
	out := map[string]any{"revision": commit.Revision, "results": results, "warnings": warnings}
	if commit.WouldCommit {
		out["wouldCommit"] = true
	}
	if commit.Violations != nil {
		out["violations"] = commit.Violations
	}
	if commit.LeaseExpiresAt != nil {
		out["leaseExpiresAt"] = commit.LeaseExpiresAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00")
	}
	if commit.LeaseTtlMs != nil {
		out["leaseTtlMs"] = commit.LeaseTtlMs
	}
	return out
}
