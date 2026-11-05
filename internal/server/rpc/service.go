package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// Service is the domain-only port implemented by core/service. Core never
// imports RPC or wire DTOs. Named writes and tx.apply all lower to the one
// Writer pipeline.
type Service interface {
	Get(context.Context, model.PrincipalRef, service.GetRequest) (service.GetResult, error)
	Ready(context.Context, model.PrincipalRef, service.ReadyRequest) (service.BeadPage, error)
	CreateWithOutcome(context.Context, model.PrincipalRef, service.CreateRequest, model.WriteMeta) (port.WriteOutcome, error)
	UpdateWithOutcome(context.Context, model.PrincipalRef, service.UpdateRequest, model.WriteMeta) (port.WriteOutcome, error)
	ClaimWithOutcome(context.Context, model.PrincipalRef, service.ClaimRequest, model.WriteMeta) (port.WriteOutcome, error)
	ClaimNextWithOutcome(context.Context, model.PrincipalRef, service.ClaimNextRequest, model.WriteMeta) (*port.WriteOutcome, error)
	CommentWithOutcome(context.Context, model.PrincipalRef, service.CommentRequest, model.WriteMeta) (port.WriteOutcome, error)
	CloseWithOutcome(context.Context, model.PrincipalRef, service.CloseRequest, model.WriteMeta) (port.WriteOutcome, error)
	ApplyWithOutcome(context.Context, model.PrincipalRef, model.ChangeSet, model.WriteMeta) (port.WriteOutcome, error)
	LinkWithOutcome(context.Context, model.PrincipalRef, service.LinkRequest, model.WriteMeta) (port.WriteOutcome, error)
	UnlinkWithOutcome(context.Context, model.PrincipalRef, service.LinkRequest, model.WriteMeta) (port.WriteOutcome, error)
	ReopenWithOutcome(context.Context, model.PrincipalRef, service.ReopenRequest, model.WriteMeta) (port.WriteOutcome, error)
	DeferWithOutcome(context.Context, model.PrincipalRef, service.DeferRequest, model.WriteMeta) (port.WriteOutcome, error)
	CancelWithOutcome(context.Context, model.PrincipalRef, service.CancelRequest, model.WriteMeta) (port.WriteOutcome, error)
	FailWithOutcome(context.Context, model.PrincipalRef, service.FailRequest, model.WriteMeta) (port.WriteOutcome, error)
	History(context.Context, model.PrincipalRef, service.HistoryRequest) (service.BeadPage, error)
}

// WorkflowService is optional so existing service test doubles and hosts that
// do not install durable workflow storage keep the unchanged core inventory.
// A composed provider binds x.workflow.invoke and the standard bead adapters.
type WorkflowService interface {
	WorkflowInvoke(context.Context, model.PrincipalRef, workflowplugin.InvokeRequest, model.WriteMeta) (workflowplugin.InvokeResult, error)
}

type WorkflowLifecycleService interface {
	WorkflowPromote(context.Context, model.PrincipalRef, workflowplugin.PromoteRequest, model.WriteMeta) (workflowplugin.PromoteResult, error)
	WorkflowDemote(context.Context, model.PrincipalRef, workflowplugin.DemoteRequest, model.WriteMeta) (workflowplugin.DemoteResult, error)
}

// WorkflowClaimNextService is an optional standard-tool adapter seam. The
// guarded composition asks it for a provider-owned durable replay before
// walking the live queue, while existing Service test doubles remain valid.
type WorkflowClaimNextService interface {
	ReplayWorkflowClaimNext(context.Context, model.PrincipalRef, service.ClaimNextRequest, model.WriteMeta) (*port.WriteOutcome, bool, error)
}

// MemoryService is an additive capability. Keeping it separate from Service
// preserves the existing RPC test/service wrappers while allowing the real
// domain service to bind the frozen memory and brief descriptors.
type MemoryService interface {
	MemoryGet(context.Context, model.PrincipalRef, service.MemoryGetRequest) (service.Memory, error)
	MemoryList(context.Context, model.PrincipalRef, service.MemoryListRequest) (service.MemoryPage, error)
	MemorySaveWithOutcome(context.Context, model.PrincipalRef, service.MemorySaveRequest, model.WriteMeta) (port.WriteOutcome, error)
	MemoryDeleteWithOutcome(context.Context, model.PrincipalRef, service.MemoryDeleteRequest, model.WriteMeta) (port.WriteOutcome, error)
	Brief(context.Context, model.PrincipalRef, service.BriefRequest) (service.BriefResult, error)
}

type serviceParams struct {
	TargetKind   model.LinkTargetKind           `json:"targetKind"`
	ID           json.RawMessage                `json:"id"`
	Ref          model.Ref                      `json:"ref"`
	AsOf         *asOf                          `json:"asOf"`
	Project      string                         `json:"project"`
	ViewID       string                         `json:"viewId"`
	Filter       *filter                        `json:"filter"`
	Limit        model.Natural                  `json:"limit"`
	Cursor       string                         `json:"cursor"`
	After        string                         `json:"after"`
	FromRevision model.Revision                 `json:"fromRevision"`
	ToRevision   model.Revision                 `json:"toRevision"`
	Type         string                         `json:"type"`
	Label        string                         `json:"label"`
	Title        string                         `json:"title"`
	Fields       map[string]any                 `json:"fields"`
	Assignment   *model.ClaimAssignment         `json:"assignment"`
	Parent       *model.Ref                     `json:"parent"`
	Expect       []model.Precondition           `json:"expect"`
	Body         string                         `json:"body"`
	CommentPage  *commentPageSelection          `json:"commentPage"`
	Reason       string                         `json:"reason"`
	Key          string                         `json:"key"`
	Content      string                         `json:"content"`
	Metadata     map[string]any                 `json:"metadata"`
	Query        string                         `json:"query"`
	BudgetTokens model.Natural                  `json:"budgetTokens"`
	ChangeSet    model.ChangeSet                `json:"changeSet"`
	From         model.Ref                      `json:"from"`
	To           model.Ref                      `json:"to"`
	Kind         string                         `json:"kind"`
	State        string                         `json:"state"`
	Details      map[string]any                 `json:"details"`
	Template     string                         `json:"template"`
	Mode         string                         `json:"mode"`
	Checklist    []workflowplugin.ChecklistItem `json:"checklist"`
	Item         model.Ref                      `json:"item"`
	Target       model.Ref                      `json:"target"`
	Source       model.Ref                      `json:"source"`
	Labels       []string                       `json:"labels"`
	Links        []workflowplugin.RelationSpec  `json:"links"`
	Meta         wire.Meta                      `json:"_meta"`
}
type asOf struct {
	Revision    model.Revision `json:"revision"`
	Incarnation string         `json:"incarnation,omitempty"`
}
type filter struct {
	Query            string   `json:"query,omitempty"`
	Resources        []string `json:"resources,omitempty"`
	Types            []string `json:"types,omitempty"`
	Events           []string `json:"events,omitempty"`
	ViewID           string   `json:"viewId,omitempty"`
	IncludeEphemeral *bool    `json:"includeEphemeral,omitempty"`
}

func (p *asOf) domain() *service.AsOf {
	if p == nil {
		return nil
	}
	return &service.AsOf{Revision: p.Revision, Incarnation: p.Incarnation}
}
func (p *filter) domain() *service.Filter {
	if p == nil {
		return nil
	}
	return &service.Filter{Query: p.Query, Resources: p.Resources, Types: p.Types, Events: p.Events, ViewID: p.ViewID, IncludeEphemeral: p.IncludeEphemeral}
}

func (p serviceParams) refID() (model.Ref, error) {
	var ref model.Ref
	if len(p.ID) == 0 {
		return ref, fmt.Errorf("id is required")
	}
	if err := wire.DecodeInto(p.ID, &ref); err != nil {
		return ref, err
	}
	return ref, nil
}

func (p serviceParams) memoryID() (model.Ref, error) {
	var id string
	if len(p.ID) == 0 || wire.DecodeInto(p.ID, &id) != nil || id == "" {
		return model.Ref{}, fmt.Errorf("memory id must be a nonempty string")
	}
	return model.RefByID(id, model.Natural64(0)), nil
}

// BindService atomically binds the vertical slice. No descriptor is added for
// an unimplemented method, and duplicates fail before the registry is changed.
func BindService(registry *Registry, svc Service) error {
	if registry == nil || svc == nil {
		return fmt.Errorf("nil service or registry")
	}
	names := []string{"bead.get", "bead.ready", "bead.create", "bead.update", "bead.claim", "bead.claimNext", "bead.comment", "bead.close", "bead.reopen", "bead.defer", "bead.cancel", "bead.fail", "bead.link", "bead.unlink", "tx.apply", "history.query"}
	if _, ok := svc.(CyclesService); ok {
		names = append(names, "bead.cycles")
	}
	if _, ok := svc.(CompactionService); ok {
		names = append(names, "admin.compact", "history.revert")
	}
	if _, ok := svc.(MemoryService); ok {
		names = append(names, "agent.brief", "memory.get", "memory.list", "memory.save", "memory.delete")
	}
	if _, ok := svc.(port.GateService); ok {
		names = append(names, "gate.approve", "gate.report")
	}
	if _, ok := svc.(WorkflowService); ok {
		names = append(names, "x.workflow.invoke")
	}
	if _, ok := svc.(WorkflowLifecycleService); ok {
		names = append(names, "x.workflow.run.promote", "x.workflow.run.demote")
	}
	if capability, ok := any(svc).(interface{ EventListConfigured() bool }); !ok || capability.EventListConfigured() {
		if _, ok := any(svc).(service.EventReader); ok {
			names = append(names, "event.list")
		}
	}
	if _, ok := any(svc).(service.HistoryReader); ok {
		names = append(names, "history.get", "history.diff")
	}
	if _, ok := any(svc).(service.HealthReader); ok {
		names = append(names, "admin.health")
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	for _, name := range names {
		if _, exists := registry.bindings[name]; exists {
			return fmt.Errorf("duplicate handler: %s", name)
		}
		if _, ok := registry.catalog.Descriptor(name); !ok {
			return fmt.Errorf("missing service descriptor: %s", name)
		}
	}
	for _, name := range names {
		d, _ := registry.catalog.Descriptor(name)
		registry.bindings[name] = binding{desc: d, handler: func(ctx context.Context, c Call) (any, error) { return dispatchService(ctx, svc, c) }}
	}
	return nil
}

func dispatchService(ctx context.Context, svc Service, c Call) (any, error) {
	if c.Method == "bead.cycles" {
		return dispatchCycles(ctx, svc.(CyclesService), c)
	}
	if c.Method == "admin.compact" || c.Method == "history.revert" {
		return dispatchCompaction(ctx, svc.(CompactionService), c)
	}
	var p serviceParams
	if err := wire.DecodeInto(c.Params, &p); err != nil {
		return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
	}
	limit, err := p.Limit.Uint64()
	if err != nil || limit > 1000 {
		return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: "limit outside 1..1000"}}
	}
	meta := model.WriteMeta{Method: c.Method, AgentLabel: c.Meta.Agent, Project: c.Meta.Project, IdempotencyKey: c.Meta.IdempotencyKey, DryRun: c.Meta.DryRun, Return: c.Meta.Return, Traceparent: c.Meta.Traceparent, Agent: c.Meta.Agent}
	memory, memoryCapable := svc.(MemoryService)
	switch c.Method {
	case "x.workflow.invoke":
		workflow, ok := svc.(WorkflowService)
		if !ok {
			return nil, &model.Error{Data: model.ErrorData{Code: "method_not_found"}}
		}
		v, err := workflow.WorkflowInvoke(ctx, c.Principal, workflowplugin.InvokeRequest{Template: p.Template, Project: p.Project, Mode: p.Mode, Checklist: p.Checklist}, meta)
		return v, err
	case "x.workflow.run.promote":
		lifecycle, ok := svc.(WorkflowLifecycleService)
		if !ok {
			return nil, &model.Error{Data: model.ErrorData{Code: "method_not_found"}}
		}
		v, err := lifecycle.WorkflowPromote(ctx, c.Principal, workflowplugin.PromoteRequest{Item: p.Item, Target: p.Target}, meta)
		return v, err
	case "x.workflow.run.demote":
		lifecycle, ok := svc.(WorkflowLifecycleService)
		if !ok {
			return nil, &model.Error{Data: model.ErrorData{Code: "method_not_found"}}
		}
		v, err := lifecycle.WorkflowDemote(ctx, c.Principal, workflowplugin.DemoteRequest{Source: p.Source, Template: p.Template, Project: p.Project, Label: p.Label, Title: p.Title, Type: p.Type, Labels: p.Labels, Metadata: p.Metadata, Parent: p.Parent, Links: p.Links}, meta)
		return v, err
	case "bead.get":
		id, err := p.refID()
		if err != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
		}
		if p.CommentPage != nil {
			return dispatchCommentPage(ctx, svc, c, p, id)
		}
		v, err := svc.Get(ctx, c.Principal, service.GetRequest{ID: id, AsOf: p.AsOf.domain(), Project: c.Meta.Project, Agent: c.Meta.Agent})
		if err != nil {
			return nil, err
		}
		return beadGetJSON(v, true), nil
	case "bead.ready":
		position, err := c.paging.decode(c, p)
		if err != nil {
			return nil, err
		}
		v, err := svc.Ready(ctx, c.Principal, service.ReadyRequest{Project: c.Meta.Project, Agent: c.Meta.Agent, ViewID: p.ViewID, Filter: p.Filter.domain(), Limit: int(limit), Position: position})
		if err != nil {
			return nil, err
		}
		return c.paging.page(c, p, position, v)
	case "history.query":
		if p.AsOf == nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: "asOf is required"}}
		}
		position, err := c.paging.decode(c, p)
		if err != nil {
			return nil, err
		}
		v, err := svc.History(ctx, c.Principal, service.HistoryRequest{Project: c.Meta.Project, AsOf: *p.AsOf.domain(), Filter: p.Filter.domain(), Limit: int(limit), Position: position})
		if err != nil {
			return nil, err
		}
		return c.paging.page(c, p, position, v)
	case "event.list":
		reader, ok := any(svc).(service.EventReader)
		if !ok || p.Filter == nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "internal_error", Hint: "event reader is not configured"}}
		}
		if p.After == "" && p.Cursor != "" {
			p.After = p.Cursor
		}
		v, err := reader.EventList(ctx, c.Principal, service.EventListRequest{Filter: p.Filter.domain(), After: p.After, Limit: int(limit)})
		if err != nil {
			return nil, err
		}
		return eventPageJSON(v), nil
	case "history.get":
		reader, ok := any(svc).(service.HistoryReader)
		if !ok || p.AsOf == nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "internal_error", Hint: "history reader is not configured"}}
		}
		v, err := reader.HistoryGet(ctx, c.Principal, service.HistoryVersionRequest{Ref: p.Ref, AsOf: *p.AsOf.domain()})
		if err != nil {
			return nil, err
		}
		return historyVersionJSON(v), nil
	case "history.diff":
		reader, ok := any(svc).(service.HistoryReader)
		if !ok {
			return nil, &model.Error{Data: model.ErrorData{Code: "internal_error", Hint: "history reader is not configured"}}
		}
		v, err := reader.HistoryDiff(ctx, c.Principal, service.HistoryDiffRequest{Ref: p.Ref, FromRevision: p.FromRevision, ToRevision: p.ToRevision})
		if err != nil {
			return nil, err
		}
		return historyDiffJSON(v), nil
	case "admin.health":
		reader, ok := any(svc).(service.HealthReader)
		if !ok {
			return nil, &model.Error{Data: model.ErrorData{Code: "internal_error", Hint: "health reader is not configured"}}
		}
		v, err := reader.Health(ctx, c.Principal)
		if err != nil {
			return nil, err
		}
		return healthJSON(v), nil
	case "agent.brief":
		if !memoryCapable {
			return nil, &model.Error{Data: model.ErrorData{Code: "method_not_found"}}
		}
		budget, err := p.BudgetTokens.Uint64()
		if err != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: "budgetTokens is outside the supported range"}}
		}
		project := p.Project
		if project == "" {
			project = c.Meta.Project
		}
		if budget > uint64(^uint(0)>>1) {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: "budgetTokens is outside the supported range"}}
		}
		v, err := memory.Brief(ctx, c.Principal, service.BriefRequest{Project: project, BudgetTokens: int(budget)})
		if err != nil {
			return nil, err
		}
		return briefJSON(v), nil
	case "memory.get":
		if !memoryCapable {
			return nil, &model.Error{Data: model.ErrorData{Code: "method_not_found"}}
		}
		id, err := p.memoryID()
		if err != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
		}
		v, err := memory.MemoryGet(ctx, c.Principal, service.MemoryGetRequest{ID: id})
		if err != nil {
			return nil, err
		}
		return memoryJSON(v), nil
	case "memory.list":
		if !memoryCapable {
			return nil, &model.Error{Data: model.ErrorData{Code: "method_not_found"}}
		}
		position, err := c.paging.decode(c, p)
		if err != nil {
			return nil, err
		}
		v, err := memory.MemoryList(ctx, c.Principal, service.MemoryListRequest{Query: p.Query, Limit: int(limit), Position: position})
		if err != nil {
			return nil, err
		}
		return c.paging.memoryPage(c, p, position, v)
	case "memory.save":
		if !memoryCapable {
			return nil, &model.Error{Data: model.ErrorData{Code: "method_not_found"}}
		}
		v, err := memory.MemorySaveWithOutcome(ctx, c.Principal, service.MemorySaveRequest{Key: p.Key, Content: p.Content, Metadata: p.Metadata, Expect: p.Expect}, meta)
		return writeOutcome(v), err
	case "memory.delete":
		if !memoryCapable {
			return nil, &model.Error{Data: model.ErrorData{Code: "method_not_found"}}
		}
		id, err := p.memoryID()
		if err != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
		}
		v, err := memory.MemoryDeleteWithOutcome(ctx, c.Principal, service.MemoryDeleteRequest{ID: id, Expect: p.Expect}, meta)
		return writeOutcome(v), err
	case "bead.create":
		v, err := svc.CreateWithOutcome(ctx, c.Principal, service.CreateRequest{Type: p.Type, Fields: p.Fields, Parent: p.Parent, Assignment: p.Assignment}, meta)
		return writeOutcome(v), err
	case "bead.update":
		id, err := p.refID()
		if err != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
		}
		v, err := svc.UpdateWithOutcome(ctx, c.Principal, service.UpdateRequest{ID: id, Fields: p.Fields, Expect: p.Expect}, meta)
		return writeOutcome(v), err
	case "bead.comment":
		id, err := p.refID()
		if err != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
		}
		v, err := svc.CommentWithOutcome(ctx, c.Principal, service.CommentRequest{ID: id, Body: p.Body, Expect: p.Expect}, meta)
		return writeOutcome(v), err
	case "bead.close":
		id, err := p.refID()
		if err != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
		}
		v, err := svc.CloseWithOutcome(ctx, c.Principal, service.CloseRequest{ID: id, Reason: p.Reason, Expect: p.Expect}, meta)
		return writeOutcome(v), err
	case "bead.reopen":
		id, err := p.refID()
		if err != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
		}
		v, err := svc.ReopenWithOutcome(ctx, c.Principal, service.ReopenRequest{ID: id, Reason: p.Reason, Expect: p.Expect}, meta)
		return writeOutcome(v), err
	case "bead.defer":
		id, err := p.refID()
		if err != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
		}
		v, err := svc.DeferWithOutcome(ctx, c.Principal, service.DeferRequest{ID: id, Reason: p.Reason, Expect: p.Expect}, meta)
		return writeOutcome(v), err
	case "bead.cancel":
		id, err := p.refID()
		if err != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
		}
		v, err := svc.CancelWithOutcome(ctx, c.Principal, service.CancelRequest{ID: id, Reason: p.Reason, Expect: p.Expect}, meta)
		return writeOutcome(v), err
	case "bead.fail":
		id, err := p.refID()
		if err != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
		}
		v, err := svc.FailWithOutcome(ctx, c.Principal, service.FailRequest{ID: id, Reason: p.Reason, Expect: p.Expect}, meta)
		return writeOutcome(v), err
	case "bead.claim":
		id, err := p.refID()
		if err != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
		}
		v, err := svc.ClaimWithOutcome(ctx, c.Principal, service.ClaimRequest{ID: id, Expect: p.Expect}, meta)
		return writeOutcome(v), err
	case "bead.link":
		var metadata *model.LinkMetadata
		if p.Metadata != nil {
			raw, err := json.Marshal(p.Metadata)
			if err != nil {
				return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
			}
			metadata = new(model.LinkMetadata)
			if err := wire.DecodeInto(raw, metadata); err != nil {
				return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
			}
		}
		v, err := svc.LinkWithOutcome(ctx, c.Principal, service.LinkRequest{From: p.From, To: p.To, TargetKind: p.TargetKind, Kind: p.Kind, Metadata: metadata, Expect: p.Expect}, meta)
		return writeOutcome(v), err
	case "bead.unlink":
		v, err := svc.UnlinkWithOutcome(ctx, c.Principal, service.LinkRequest{From: p.From, To: p.To, TargetKind: p.TargetKind, Kind: p.Kind, Expect: p.Expect}, meta)
		return writeOutcome(v), err
	case "tx.apply":
		v, err := svc.ApplyWithOutcome(ctx, c.Principal, p.ChangeSet, meta)
		return writeOutcome(v), err
	case "bead.claimNext":
		v, err := svc.ClaimNextWithOutcome(ctx, c.Principal, service.ClaimNextRequest{Project: c.Meta.Project, ViewID: p.ViewID, Filter: p.Filter.domain()}, meta)
		if err != nil {
			return nil, err
		}
		if v == nil || v.NullResult {
			// Terminal claimNext is the cataloged JSON null. Keep the outcome
			// wrapper so a keyed durable replay still emits response-only
			// _meta.replayed without changing the stored null result.
			if v == nil {
				return nil, nil
			}
			return Outcome{Result: nil, Replayed: v.Replayed}, nil
		}
		return writeOutcome(*v), nil
	case "gate.approve":
		gate, ok := svc.(port.GateService)
		if !ok {
			return nil, &model.Error{Data: model.ErrorData{Code: "method_not_found"}}
		}
		id, err := p.refID()
		if err != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
		}
		v, err := gate.ApproveGate(ctx, c.Principal, service.GateApproveRequest{ID: id, Expect: p.Expect}, meta)
		return Outcome{Result: gateJSON(v.Result), Replayed: v.Replayed}, err
	case "gate.report":
		gate, ok := svc.(port.GateService)
		if !ok {
			return nil, &model.Error{Data: model.ErrorData{Code: "method_not_found"}}
		}
		id, err := p.refID()
		if err != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
		}
		v, err := gate.ReportGate(ctx, c.Principal, service.GateReportRequest{ID: id, State: p.State, Details: p.Details, Expect: p.Expect}, meta)
		return Outcome{Result: gateJSON(v.Result), Replayed: v.Replayed}, err
	default:
		return nil, &model.Error{Data: model.ErrorData{Code: "method_not_found"}}
	}
}

func gateJSON(v port.GateResult) map[string]any {
	return map[string]any{"commit": commitJSON(v.Commit), "state": v.State, "approvals": v.Approvals}
}

func writeOutcome(v port.WriteOutcome) Outcome {
	return Outcome{Result: commitJSON(v.Commit), Replayed: v.Replayed}
}

func commitJSON(v model.Commit) map[string]any {
	results := v.Results
	if results == nil {
		results = []model.OpResult{}
	}
	warnings := v.Warnings
	if warnings == nil {
		warnings = []model.Warning{}
	}
	out := map[string]any{"revision": v.Revision, "results": results, "warnings": warnings}
	if v.WouldCommit {
		out["wouldCommit"] = true
	}
	if v.Violations != nil {
		out["violations"] = v.Violations
	}
	if v.LeaseExpiresAt != nil {
		out["leaseExpiresAt"] = timestamp(*v.LeaseExpiresAt)
	}
	if v.LeaseTtlMs != nil {
		out["leaseTtlMs"] = v.LeaseTtlMs
	}
	return out
}
func pageJSON(v service.BeadPage) map[string]any {
	items := make([]map[string]any, 0, len(v.Items))
	for _, b := range v.Items {
		items = append(items, beadJSON(b))
	}
	out := map[string]any{"items": items, "asOf": asOf{v.AsOf.Revision, v.AsOf.Incarnation}}
	return out
}

func memoryJSON(m service.Memory) map[string]any {
	out := map[string]any{"id": m.ID, "key": m.Key, "content": m.Content, "revision": m.Revision}
	if m.Metadata != nil {
		out["metadata"] = m.Metadata
	}
	return out
}

func memoryPageJSON(v service.MemoryPage) map[string]any {
	items := make([]map[string]any, 0, len(v.Items))
	for _, memory := range v.Items {
		items = append(items, memoryJSON(memory))
	}
	return map[string]any{"items": items}
}

func briefJSON(v service.BriefResult) map[string]any {
	whoami := map[string]any{"principal": v.WhoAmI.Principal}
	if v.WhoAmI.Sponsor != nil {
		whoami["sponsor"] = *v.WhoAmI.Sponsor
	}
	if v.WhoAmI.Caveats != nil {
		whoami["caveats"] = v.WhoAmI.Caveats
	}
	if v.WhoAmI.DefaultProject != "" {
		whoami["defaultProject"] = v.WhoAmI.DefaultProject
	}
	memories := make([]map[string]any, 0, len(v.Memories))
	for _, memory := range v.Memories {
		memories = append(memories, memoryJSON(memory))
	}
	claims := make([]map[string]any, 0, len(v.Claims))
	for _, bead := range v.Claims {
		claims = append(claims, beadJSON(bead))
	}
	ready := make([]map[string]any, 0, len(v.ReadyTop))
	for _, bead := range v.ReadyTop {
		ready = append(ready, beadJSON(bead))
	}
	recent := v.RecentChanges
	if recent == nil {
		recent = []map[string]any{}
	}
	ontology := v.OntologySummary
	if ontology == nil {
		ontology = map[string]any{}
	}
	conventions := v.Conventions
	if conventions == nil {
		conventions = map[string]any{}
	}
	return map[string]any{"whoami": whoami, "claims": claims, "readyTop": ready, "recentChanges": recent, "ontologySummary": ontology, "conventions": conventions, "memories": memories}
}

func eventPageJSON(v service.EventPage) map[string]any {
	events := v.Events
	if events == nil {
		events = []model.Event{}
	}
	if v.Cursor == "" {
		// The event service always returns an opaque cursor, including an empty
		// page. Keep the wire shape explicit if an injected provider violates
		// that seam so catalog validation reports the handler error.
		return map[string]any{"events": events, "cursor": ""}
	}
	return map[string]any{"events": events, "cursor": v.Cursor}
}
func historyVersionJSON(v service.HistoryVersion) map[string]any {
	patch := v.Patch
	if patch == nil {
		patch = []map[string]any{}
	}
	out := map[string]any{"ref": v.Ref, "revision": v.Revision, "base": v.Base, "patch": patch, "createdAt": timestamp(v.CreatedAt)}
	if v.Body != nil {
		out["body"] = v.Body
	}
	if v.Principal.Kind != "" {
		out["principal"] = v.Principal
	}
	return out
}
func historyDiffJSON(v service.HistoryDiff) map[string]any {
	patch := make([]map[string]any, 0, len(v.Patch))
	for _, operation := range v.Patch {
		item := map[string]any{"op": operation.Op, "path": operation.Path}
		if operation.Op != "remove" {
			item["value"] = operation.Value
		}
		patch = append(patch, item)
	}
	paths := v.ChangedPaths
	if paths == nil {
		paths = []string{}
	}
	return map[string]any{"patch": patch, "changedPaths": paths}
}
func healthJSON(v driver.StoreHealth) map[string]any {
	authorization := v.AuthorizationChain
	if authorization == nil {
		authorization = []string{}
	}
	out := map[string]any{
		"driver": v.Driver, "role": v.Role, "leaseEpoch": model.Natural64(v.LeaseEpoch),
		"head": v.Head, "incarnation": v.Incarnation, "ontology": v.Ontology,
		"pluginSet": v.PluginSet, "authorizationChain": authorization,
	}
	if v.ReplicationLag > 0 {
		out["replicationLagMs"] = model.Natural64(uint64(v.ReplicationLag / time.Millisecond))
	}
	if !v.LastBackupCheckpoint.IsZero() {
		out["lastBackupCheckpoint"] = timestamp(v.LastBackupCheckpoint)
	}
	return out
}
func timestamp(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func beadGetJSON(v service.GetResult, includeComments bool) map[string]any {
	attachments := v.Attachments
	if attachments == nil {
		attachments = []model.Attachment{}
	}
	out := map[string]any{"bead": beadJSON(v.Bead), "attachments": attachments}
	if includeComments && v.Comments != nil {
		comments := make([]map[string]any, len(v.Comments))
		for i, comment := range v.Comments {
			value := map[string]any{"body": comment.Body, "principal": comment.Principal, "time": timestamp(comment.Time)}
			if comment.Origin != "" {
				value["origin"] = comment.Origin
				if comment.SourceTime != nil {
					value["sourceTime"] = timestamp(*comment.SourceTime)
				}
				if comment.SourceAuthor != "" {
					value["sourceAuthor"] = comment.SourceAuthor
				}
			}
			comments[i] = value
		}
		out["comments"] = comments
	}
	if v.Lease != nil {
		out["lease"] = map[string]any{"holder": v.Lease.Principal.ID, "expiresAt": timestamp(v.Lease.ExpiresAt), "ttlMs": v.Lease.TTL.Milliseconds(), "epoch": v.Lease.Epoch}
	}
	return out
}

func beadJSON(b service.Bead) map[string]any {
	out := map[string]any{"id": b.ID, "type": b.Type, "revision": b.Revision}
	if b.Assignment != nil {
		out["assignment"] = b.Assignment
	}
	for k, v := range map[string]string{"project": b.Project, "status": b.Status, "parent": b.Parent, "assignee": b.Assignee} {
		if v != "" {
			out[k] = v
		}
	}
	if b.ParentHidden {
		out["parent"] = map[string]any{"exists": true}
	}
	if b.Fields != nil {
		out["fields"] = b.Fields
	}
	if b.Metadata != nil {
		out["metadata"] = b.Metadata
	}
	if b.Labels != nil {
		out["labels"] = b.Labels
	}
	if b.Links != nil {
		out["links"] = b.Links
	}
	if b.Blocked {
		out["blocked"] = true
	}
	if b.ChildCounter != 0 {
		out["childCounter"] = b.ChildCounter
	}
	if b.HiddenBlockers != 0 {
		out["hiddenBlockers"] = b.HiddenBlockers
	}
	if b.ReadinessPending {
		out["readinessPending"] = true
	}
	if !b.CreatedAt.IsZero() {
		out["createdAt"] = timestamp(b.CreatedAt)
	}
	if !b.UpdatedAt.IsZero() {
		out["updatedAt"] = timestamp(b.UpdatedAt)
	}
	if b.Plane != "" {
		out["plane"] = b.Plane
	}
	if b.Kind != "" {
		out["kind"] = b.Kind
	}
	if b.Plugin != "" {
		out["plugin"] = b.Plugin
	}
	if b.WorkflowRunID != "" {
		out["workflowRunId"] = b.WorkflowRunID
	}
	return out
}
