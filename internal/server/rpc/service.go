package rpc

import (
	"context"
	"fmt"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
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

type serviceParams struct {
	ID           model.Ref            `json:"id"`
	Ref          model.Ref            `json:"ref"`
	AsOf         *asOf                `json:"asOf"`
	ViewID       string               `json:"viewId"`
	Filter       *filter              `json:"filter"`
	Limit        model.Natural        `json:"limit"`
	Cursor       string               `json:"cursor"`
	After        string               `json:"after"`
	FromRevision model.Revision       `json:"fromRevision"`
	ToRevision   model.Revision       `json:"toRevision"`
	Type         string               `json:"type"`
	Fields       map[string]any       `json:"fields"`
	Parent       *model.Ref           `json:"parent"`
	Expect       []model.Precondition `json:"expect"`
	Body         string               `json:"body"`
	Reason       string               `json:"reason"`
	ChangeSet    model.ChangeSet      `json:"changeSet"`
	From         model.Ref            `json:"from"`
	To           model.Ref            `json:"to"`
	Kind         string               `json:"kind"`
	State        string               `json:"state"`
	Details      map[string]any       `json:"details"`
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

// BindService atomically binds the vertical slice. No descriptor is added for
// an unimplemented method, and duplicates fail before the registry is changed.
func BindService(registry *Registry, svc Service) error {
	if registry == nil || svc == nil {
		return fmt.Errorf("nil service or registry")
	}
	names := []string{"bead.get", "bead.ready", "bead.create", "bead.update", "bead.claim", "bead.comment", "bead.close", "bead.reopen", "bead.defer", "bead.cancel", "bead.fail", "bead.link", "bead.unlink", "tx.apply", "history.query"}
	if _, ok := svc.(port.GateService); ok {
		names = append(names, "gate.approve", "gate.report")
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
	var p serviceParams
	if err := wire.DecodeInto(c.Params, &p); err != nil {
		return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
	}
	limit, err := p.Limit.Uint64()
	if err != nil || limit > 1000 {
		return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: "limit outside 1..1000"}}
	}
	meta := model.WriteMeta{Method: c.Method, AgentLabel: c.Meta.Agent, Project: c.Meta.Project, IdempotencyKey: c.Meta.IdempotencyKey, DryRun: c.Meta.DryRun, Return: c.Meta.Return, Traceparent: c.Meta.Traceparent, Agent: c.Meta.Agent}
	switch c.Method {
	case "bead.get":
		v, err := svc.Get(ctx, c.Principal, service.GetRequest{ID: p.ID, AsOf: p.AsOf.domain()})
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
	case "bead.create":
		v, err := svc.CreateWithOutcome(ctx, c.Principal, service.CreateRequest{Type: p.Type, Fields: p.Fields, Parent: p.Parent}, meta)
		return writeOutcome(v), err
	case "bead.update":
		v, err := svc.UpdateWithOutcome(ctx, c.Principal, service.UpdateRequest{ID: p.ID, Fields: p.Fields, Expect: p.Expect}, meta)
		return writeOutcome(v), err
	case "bead.comment":
		v, err := svc.CommentWithOutcome(ctx, c.Principal, service.CommentRequest{ID: p.ID, Body: p.Body, Expect: p.Expect}, meta)
		return writeOutcome(v), err
	case "bead.close":
		v, err := svc.CloseWithOutcome(ctx, c.Principal, service.CloseRequest{ID: p.ID, Reason: p.Reason, Expect: p.Expect}, meta)
		return writeOutcome(v), err
	case "bead.reopen":
		v, err := svc.ReopenWithOutcome(ctx, c.Principal, service.ReopenRequest{ID: p.ID, Reason: p.Reason, Expect: p.Expect}, meta)
		return writeOutcome(v), err
	case "bead.defer":
		v, err := svc.DeferWithOutcome(ctx, c.Principal, service.DeferRequest{ID: p.ID, Reason: p.Reason, Expect: p.Expect}, meta)
		return writeOutcome(v), err
	case "bead.cancel":
		v, err := svc.CancelWithOutcome(ctx, c.Principal, service.CancelRequest{ID: p.ID, Reason: p.Reason, Expect: p.Expect}, meta)
		return writeOutcome(v), err
	case "bead.fail":
		v, err := svc.FailWithOutcome(ctx, c.Principal, service.FailRequest{ID: p.ID, Reason: p.Reason, Expect: p.Expect}, meta)
		return writeOutcome(v), err
	case "bead.claim":
		v, err := svc.ClaimWithOutcome(ctx, c.Principal, service.ClaimRequest{ID: p.ID, Expect: p.Expect}, meta)
		return writeOutcome(v), err
	case "bead.link":
		v, err := svc.LinkWithOutcome(ctx, c.Principal, service.LinkRequest{From: p.From, To: p.To, Kind: p.Kind, Expect: p.Expect}, meta)
		return writeOutcome(v), err
	case "bead.unlink":
		v, err := svc.UnlinkWithOutcome(ctx, c.Principal, service.LinkRequest{From: p.From, To: p.To, Kind: p.Kind, Expect: p.Expect}, meta)
		return writeOutcome(v), err
	case "tx.apply":
		v, err := svc.ApplyWithOutcome(ctx, c.Principal, p.ChangeSet, meta)
		return writeOutcome(v), err
	case "gate.approve":
		gate, ok := svc.(port.GateService)
		if !ok {
			return nil, &model.Error{Data: model.ErrorData{Code: "method_not_found"}}
		}
		v, err := gate.ApproveGate(ctx, c.Principal, service.GateApproveRequest{ID: p.ID, Expect: p.Expect}, meta)
		return Outcome{Result: gateJSON(v.Result), Replayed: v.Replayed}, err
	case "gate.report":
		gate, ok := svc.(port.GateService)
		if !ok {
			return nil, &model.Error{Data: model.ErrorData{Code: "method_not_found"}}
		}
		v, err := gate.ReportGate(ctx, c.Principal, service.GateReportRequest{ID: p.ID, State: p.State, Details: p.Details, Expect: p.Expect}, meta)
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
