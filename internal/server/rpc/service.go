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
)

// Service is the domain-only port implemented by core/service. Core never
// imports RPC or wire DTOs. Each write remains a named domain verb whose
// implementation lowers to the one Writer pipeline.
type Service interface {
	Get(context.Context, model.PrincipalRef, service.GetRequest) (service.GetResult, error)
	Ready(context.Context, model.PrincipalRef, service.ReadyRequest) (service.BeadPage, error)
	CreateWithOutcome(context.Context, model.PrincipalRef, service.CreateRequest, model.WriteMeta) (port.WriteOutcome, error)
	UpdateWithOutcome(context.Context, model.PrincipalRef, service.UpdateRequest, model.WriteMeta) (port.WriteOutcome, error)
	ClaimWithOutcome(context.Context, model.PrincipalRef, service.ClaimRequest, model.WriteMeta) (port.WriteOutcome, error)
	CommentWithOutcome(context.Context, model.PrincipalRef, service.CommentRequest, model.WriteMeta) (port.WriteOutcome, error)
	CloseWithOutcome(context.Context, model.PrincipalRef, service.CloseRequest, model.WriteMeta) (port.WriteOutcome, error)
	ReopenWithOutcome(context.Context, model.PrincipalRef, service.ReopenRequest, model.WriteMeta) (port.WriteOutcome, error)
	DeferWithOutcome(context.Context, model.PrincipalRef, service.DeferRequest, model.WriteMeta) (port.WriteOutcome, error)
	CancelWithOutcome(context.Context, model.PrincipalRef, service.CancelRequest, model.WriteMeta) (port.WriteOutcome, error)
	FailWithOutcome(context.Context, model.PrincipalRef, service.FailRequest, model.WriteMeta) (port.WriteOutcome, error)
	History(context.Context, model.PrincipalRef, service.HistoryRequest) (service.BeadPage, error)
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
	ID           json.RawMessage      `json:"id"`
	AsOf         *asOf                `json:"asOf"`
	Project      string               `json:"project"`
	ViewID       string               `json:"viewId"`
	Filter       *filter              `json:"filter"`
	Limit        model.Natural        `json:"limit"`
	Cursor       string               `json:"cursor"`
	Type         string               `json:"type"`
	Fields       map[string]any       `json:"fields"`
	Parent       *model.Ref           `json:"parent"`
	Expect       []model.Precondition `json:"expect"`
	Body         string               `json:"body"`
	Reason       string               `json:"reason"`
	Key          string               `json:"key"`
	Content      string               `json:"content"`
	Metadata     map[string]any       `json:"metadata"`
	Query        string               `json:"query"`
	BudgetTokens model.Natural        `json:"budgetTokens"`
	Meta         wire.Meta            `json:"_meta"`
}
type asOf struct {
	Revision    model.Revision `json:"revision"`
	Incarnation string         `json:"incarnation,omitempty"`
}
type filter struct {
	Query     string   `json:"query,omitempty"`
	Resources []string `json:"resources,omitempty"`
	Types     []string `json:"types,omitempty"`
	Events    []string `json:"events,omitempty"`
	ViewID    string   `json:"viewId,omitempty"`
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
	return &service.Filter{Query: p.Query, Resources: p.Resources, Types: p.Types, Events: p.Events, ViewID: p.ViewID}
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
	names := []string{"bead.get", "bead.ready", "bead.create", "bead.update", "bead.claim", "bead.comment", "bead.close", "bead.reopen", "bead.defer", "bead.cancel", "bead.fail", "history.query"}
	if _, ok := svc.(MemoryService); ok {
		names = append(names, "agent.brief", "memory.get", "memory.list", "memory.save", "memory.delete")
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
	case "bead.get":
		id, err := p.refID()
		if err != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
		}
		v, err := svc.Get(ctx, c.Principal, service.GetRequest{ID: id, AsOf: p.AsOf.domain()})
		if err != nil {
			return nil, err
		}
		attachments := v.Attachments
		if attachments == nil {
			attachments = []model.Attachment{}
		}
		out := map[string]any{"bead": beadJSON(v.Bead), "attachments": attachments}
		if v.Lease != nil {
			out["lease"] = map[string]any{"holder": v.Lease.Principal.ID, "expiresAt": timestamp(v.Lease.ExpiresAt), "ttlMs": v.Lease.TTL.Milliseconds(), "epoch": v.Lease.Epoch}
		}
		return out, nil
	case "bead.ready":
		position, err := c.paging.decode(c, p)
		if err != nil {
			return nil, err
		}
		v, err := svc.Ready(ctx, c.Principal, service.ReadyRequest{Project: c.Meta.Project, ViewID: p.ViewID, Filter: p.Filter.domain(), Limit: int(limit), Position: position})
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
		v, err := memory.MemoryList(ctx, c.Principal, service.MemoryListRequest{Query: p.Query, Limit: int(limit), Cursor: p.Cursor, Position: position})
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
		v, err := svc.CreateWithOutcome(ctx, c.Principal, service.CreateRequest{Type: p.Type, Fields: p.Fields, Parent: p.Parent}, meta)
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
	default:
		return nil, &model.Error{Data: model.ErrorData{Code: "method_not_found"}}
	}
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

func timestamp(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
func beadJSON(b service.Bead) map[string]any {
	out := map[string]any{"id": b.ID, "type": b.Type, "revision": b.Revision}
	for k, v := range map[string]string{"project": b.Project, "status": b.Status, "parent": b.Parent, "assignee": b.Assignee} {
		if v != "" {
			out[k] = v
		}
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
	return out
}
