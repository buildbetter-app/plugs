package rpc

import (
	"context"
	"encoding/base64"
	"fmt"
	"math"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/server/authn"
)

// TxApplyService is intentionally separate from Service. Existing service
// fakes and older fixture runtimes remain source-compatible until a
// composition explicitly installs the direct ChangeSet binding.
type TxApplyService interface {
	ApplyWithOutcome(context.Context, model.PrincipalRef, model.ChangeSet, model.WriteMeta) (port.WriteOutcome, error)
}

// BindTxApply installs the direct typed transaction entry point.
func BindTxApply(registry *Registry, svc TxApplyService) error {
	if registry == nil || svc == nil {
		return fmt.Errorf("nil tx.apply service or registry")
	}
	return registry.Register("tx.apply", func(ctx context.Context, c Call) (any, error) {
		var params struct {
			ChangeSet model.ChangeSet `json:"changeSet"`
			Meta      wire.Meta       `json:"_meta"`
		}
		if err := wire.DecodeInto(c.Params, &params); err != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
		}
		meta := writeMeta(c)
		outcome, err := svc.ApplyWithOutcome(ctx, c.Principal, params.ChangeSet, meta)
		return writeOutcome(outcome), err
	})
}

// BindAdmin installs the paged dump/restore seam. It is separate from
// BindService so the historical validation fixtures can continue to prove
// that uninstalled admin methods are refused.
func BindAdmin(registry *Registry, manager *restore.Manager) error {
	if registry == nil || manager == nil {
		return fmt.Errorf("nil admin manager or registry")
	}
	for _, name := range []string{"admin.dump", "admin.restore.begin", "admin.restore.chunk", "admin.restore.finish"} {
		if _, ok := registry.Catalog().Descriptor(name); !ok {
			return fmt.Errorf("missing admin descriptor: %s", name)
		}
	}
	if err := registry.Register("admin.dump", func(ctx context.Context, c Call) (any, error) {
		if err := requireAdminCaller(ctx, c.Principal); err != nil {
			return nil, err
		}
		var params struct {
			History           string         `json:"history"`
			Cursor            string         `json:"cursor"`
			PageSize          model.Natural  `json:"pageSize"`
			HumanPresence     map[string]any `json:"humanPresence"`
			IncludeCiphertext bool           `json:"includeCiphertext"`
			Meta              wire.Meta      `json:"_meta"`
		}
		if err := wire.DecodeInto(c.Params, &params); err != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
		}
		pageSize, err := boundedPageSize(params.PageSize)
		if err != nil {
			return nil, err
		}
		page, err := manager.DumpPage(ctx, params.History, params.Cursor, pageSize, restore.Presence(params.HumanPresence))
		if err != nil {
			return nil, err
		}
		return dumpPageJSON(page), nil
	}); err != nil {
		return err
	}
	if err := registry.Register("admin.restore.begin", func(ctx context.Context, c Call) (any, error) {
		if err := requireAdminCaller(ctx, c.Principal); err != nil {
			return nil, err
		}
		var params struct {
			Manifest      map[string]any `json:"manifest"`
			History       string         `json:"history"`
			HistoryDigest string         `json:"historyDigest"`
			HistoryTime   string         `json:"historyTime"`
			HumanPresence map[string]any `json:"humanPresence"`
			Meta          wire.Meta      `json:"_meta"`
		}
		if err := wire.DecodeInto(c.Params, &params); err != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
		}
		status, err := manager.BeginAsWithMetaAndHistory(ctx, c.Principal, params.Manifest, params.History, params.HistoryDigest, params.HistoryTime, writeMeta(c), restore.Presence(params.HumanPresence))
		return restoreStatusJSON(status), err
	}); err != nil {
		return err
	}
	if err := registry.Register("admin.restore.chunk", func(ctx context.Context, c Call) (any, error) {
		if err := requireAdminCaller(ctx, c.Principal); err != nil {
			return nil, err
		}
		var params struct {
			RestoreID     string         `json:"restoreId"`
			Page          model.Natural  `json:"page"`
			Data          string         `json:"data"`
			SHA256        string         `json:"sha256"`
			HumanPresence map[string]any `json:"humanPresence"`
			Meta          wire.Meta      `json:"_meta"`
		}
		if err := wire.DecodeInto(c.Params, &params); err != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
		}
		page, err := params.Page.Uint64()
		if err != nil || page > math.MaxInt64 {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: "invalid restore page"}}
		}
		data, err := base64.StdEncoding.DecodeString(params.Data)
		if err != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "restore_checksum_failed", Hint: "restore page data is not base64"}}
		}
		status, err := manager.ChunkAs(ctx, c.Principal, params.RestoreID, page, data, params.SHA256, restore.Presence(params.HumanPresence))
		return restoreStatusJSON(status), err
	}); err != nil {
		return err
	}
	return registry.Register("admin.restore.finish", func(ctx context.Context, c Call) (any, error) {
		if err := requireAdminCaller(ctx, c.Principal); err != nil {
			return nil, err
		}
		var params struct {
			RestoreID      string         `json:"restoreId"`
			ManifestSHA256 string         `json:"manifestSha256"`
			HumanPresence  map[string]any `json:"humanPresence"`
			Meta           wire.Meta      `json:"_meta"`
		}
		if err := wire.DecodeInto(c.Params, &params); err != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
		}
		status, err := manager.Finish(ctx, params.RestoreID, params.ManifestSHA256, c.Principal, writeMeta(c), restore.Presence(params.HumanPresence))
		return restoreStatusJSON(status), err
	})
}

// requireAdminCaller keeps the existing anonymous-local route while ensuring
// every authenticated admin call is backed by a live interactive User proof.
// A provider/API-key User, service, agent, or caller-shaped principal cannot
// inherit an admin restore session.
func requireAdminCaller(ctx context.Context, principal model.PrincipalRef) error {
	if principal.Kind == "anonymous" && principal.ID == "anonymous" && principal.Sponsor == nil && len(principal.Caveats) == 0 {
		return nil
	}
	if principal.Kind != "user" || principal.Sponsor != nil || len(principal.Caveats) != 0 {
		return authn.ErrHumanPresenceRequired
	}
	if err := authn.ValidatePrincipal(principal); err != nil {
		return authn.ErrUnauthenticated
	}
	return authn.RequireInteractiveUser(ctx, principal)
}

func writeMeta(c Call) model.WriteMeta {
	return model.WriteMeta{Method: c.Method, AgentLabel: c.Meta.Agent, Project: c.Meta.Project, IdempotencyKey: c.Meta.IdempotencyKey, DryRun: c.Meta.DryRun, Return: c.Meta.Return, Traceparent: c.Meta.Traceparent, Agent: c.Meta.Agent}
}

func boundedPageSize(value model.Natural) (int, error) {
	if value.IsZero() {
		return 0, nil
	}
	n, err := value.Uint64()
	if err != nil || n < 1 || n > 67108864 || n > math.MaxInt {
		return 0, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: "pageSize must be between 1 and 67108864"}}
	}
	return int(n), nil
}

func dumpPageJSON(page restore.DumpPage) map[string]any {
	out := map[string]any{"dumpId": page.DumpID, "page": page.Page, "data": base64.StdEncoding.EncodeToString(page.Data), "done": page.Done, "sha256": page.SHA256}
	if page.NextPage != nil {
		out["nextPage"] = *page.NextPage
	}
	return out
}

func restoreStatusJSON(status restore.RestoreStatus) map[string]any {
	warnings := status.Warnings
	if warnings == nil {
		warnings = []model.Warning{}
	}
	out := map[string]any{"restoreId": status.RestoreID, "state": status.State, "receivedPages": status.ReceivedPages, "warnings": warnings}
	if status.Incarnation != "" {
		out["incarnation"] = status.Incarnation
	}
	return out
}
