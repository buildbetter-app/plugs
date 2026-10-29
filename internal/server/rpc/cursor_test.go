package rpc_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
)

// A domain-only fixture checks the wire/Position seam. The real service's
// storage ordering and snapshot semantics are verified by its own tests.
type pagedService struct {
	serviceProbe
	ready   []service.ReadyRequest
	history []service.HistoryRequest
	badPage string
}

func (p *pagedService) page(limit int, at service.AsOf, position *service.PagePosition) service.BeadPage {
	if at.Incarnation == "" {
		at.Incarnation = "inc"
	}
	if position != nil {
		at = position.AsOf
	}
	offset := 0
	if position != nil {
		offset = position.Offset
	}
	if limit == 0 {
		limit = 100
	}
	items := []service.Bead{
		{ID: "p-1", Type: "task", Revision: at.Revision},
		{ID: "p-2", Type: "task", Revision: at.Revision},
		{ID: "p-3", Type: "task", Revision: at.Revision},
	}
	end := min(len(items), offset+limit)
	out := service.BeadPage{Items: items[offset:end], AsOf: at}
	if end < len(items) {
		out.Next = &service.PagePosition{Offset: end, AsOf: at}
	}
	switch p.badPage {
	case "legacy":
		out.NextCursor = "unsigned"
	case "nonadvancing":
		out.Next.Offset = offset
	case "revision":
		out.Next.AsOf.Revision = model.Natural64(2)
	case "incarnation":
		out.Next.AsOf.Incarnation = "other"
	case "resnapshot":
		if position != nil {
			out.AsOf.Revision = model.Natural64(2)
			if out.Next != nil {
				out.Next.AsOf = out.AsOf
			}
		}
	}
	return out
}

func (p *pagedService) Ready(_ context.Context, _ model.PrincipalRef, r service.ReadyRequest) (service.BeadPage, error) {
	p.ready = append(p.ready, r)
	return p.page(r.Limit, service.AsOf{Revision: model.Natural("9007199254740993")}, r.Position), nil
}

func (p *pagedService) History(_ context.Context, _ model.PrincipalRef, r service.HistoryRequest) (service.BeadPage, error) {
	p.history = append(p.history, r)
	return p.page(r.Limit, r.AsOf, r.Position), nil
}

func pagingConfig() rpc.Config {
	return rpc.Config{Incarnation: "inc", DefaultProject: "p", OntologyRevision: model.Natural64(7), CursorKey: bytes.Repeat([]byte{0x37}, 32)}
}

func pagingServer(t *testing.T, svc rpc.Service, config rpc.Config) *rpc.Server {
	t.Helper()
	r := registry(t)
	if err := rpc.BindService(r, svc); err != nil {
		t.Fatal(err)
	}
	return server(t, r, config)
}

func paramsJSON(t *testing.T, params map[string]any) string {
	t.Helper()
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func pageParams(method string) map[string]any {
	p := map[string]any{"limit": 1, "filter": map[string]any{"types": []string{"task"}}}
	if method == "history.query" {
		p["asOf"] = map[string]any{"revision": model.Natural("9007199254740993"), "incarnation": "inc"}
	}
	return p
}

func TestSignedCursorPagingAtByteBoundary(t *testing.T) {
	for _, method := range []string{"bead.ready", "history.query"} {
		t.Run(method, func(t *testing.T) {
			probe := &pagedService{}
			s := pagingServer(t, probe, pagingConfig())
			params := pageParams(method)
			var ids []string
			for i := 0; i < 3; i++ {
				got := result(t, call(t, s, context.Background(), method, paramsJSON(t, params)))
				items := got["items"].([]any)
				if len(items) != 1 {
					t.Fatalf("limit=1: %v", got)
				}
				ids = append(ids, items[0].(map[string]any)["id"].(string))
				if got["asOf"].(map[string]any)["revision"] != json.Number("9007199254740993") {
					t.Fatalf("page lost pinned revision: %v", got)
				}
				if i == 2 {
					if _, ok := got["nextCursor"]; ok {
						t.Fatal("last page has a cursor")
					}
				} else {
					token, ok := got["nextCursor"].(string)
					if !ok || len(strings.Split(token, ".")) != 2 {
						t.Fatal("missing signed cursor")
					}
					params["cursor"] = token
				}
			}
			if !reflect.DeepEqual(ids, []string{"p-1", "p-2", "p-3"}) {
				t.Fatalf("paging repeated or skipped IDs: %v", ids)
			}
			for i := range 3 {
				var raw string
				var pos *service.PagePosition
				if method == "bead.ready" {
					raw, pos = probe.ready[i].Cursor, probe.ready[i].Position
				} else {
					raw, pos = probe.history[i].Cursor, probe.history[i].Position
				}
				if raw != "" || (i == 0 && pos != nil) || (i > 0 && (pos == nil || pos.Offset != i || pos.AsOf.Incarnation != "inc" || pos.AsOf.Revision != model.Natural("9007199254740993"))) {
					t.Fatalf("service received raw cursor or wrong position: page %d, raw=%q pos=%+v", i, raw, pos)
				}
			}
		})
	}
}

func TestCursorBindingsRejectChangedRequestsBeforeService(t *testing.T) {
	base := model.PrincipalRef{Kind: "agent", ID: "a", Sponsor: &model.PrincipalRef{Kind: "user", ID: "u"}, Caveats: map[string]any{"project": "p"}}
	for _, tc := range []struct {
		name, method, code string
		change             func(*rpc.Config, *model.PrincipalRef, map[string]any)
	}{
		{"principal", "bead.ready", "cursor_invalid", func(_ *rpc.Config, by *model.PrincipalRef, _ map[string]any) { by.ID = "b" }},
		{"sponsor", "bead.ready", "cursor_invalid", func(_ *rpc.Config, by *model.PrincipalRef, _ map[string]any) {
			by.Sponsor = &model.PrincipalRef{Kind: "user", ID: "v"}
		}},
		{"caveats", "bead.ready", "cursor_invalid", func(_ *rpc.Config, by *model.PrincipalRef, _ map[string]any) {
			by.Caveats = map[string]any{"project": "other"}
		}},
		{"project", "bead.ready", "cursor_invalid", func(_ *rpc.Config, _ *model.PrincipalRef, p map[string]any) {
			p["_meta"] = map[string]any{"project": "other"}
		}},
		{"query", "bead.ready", "cursor_invalid", func(_ *rpc.Config, _ *model.PrincipalRef, p map[string]any) {
			p["filter"].(map[string]any)["query"] = "status:closed"
		}},
		{"types", "bead.ready", "cursor_invalid", func(_ *rpc.Config, _ *model.PrincipalRef, p map[string]any) {
			p["filter"].(map[string]any)["types"] = []string{"bug"}
		}},
		{"resources", "bead.ready", "cursor_invalid", func(_ *rpc.Config, _ *model.PrincipalRef, p map[string]any) {
			p["filter"].(map[string]any)["resources"] = []string{"p-2"}
		}},
		{"events", "bead.ready", "cursor_invalid", func(_ *rpc.Config, _ *model.PrincipalRef, p map[string]any) {
			p["filter"].(map[string]any)["events"] = []string{"bead.updated"}
		}},
		{"view", "bead.ready", "cursor_invalid", func(_ *rpc.Config, _ *model.PrincipalRef, p map[string]any) { p["viewId"] = "saved" }},
		{"filter-view", "bead.ready", "cursor_invalid", func(_ *rpc.Config, _ *model.PrincipalRef, p map[string]any) {
			p["filter"].(map[string]any)["viewId"] = "saved"
		}},
		{"ontology", "bead.ready", "cursor_invalid", func(c *rpc.Config, _ *model.PrincipalRef, _ map[string]any) { c.OntologyRevision = model.Natural64(8) }},
		{"incarnation", "bead.ready", "incarnation_changed", func(c *rpc.Config, _ *model.PrincipalRef, _ map[string]any) { c.Incarnation = "restored" }},
		{"key", "bead.ready", "cursor_invalid", func(c *rpc.Config, _ *model.PrincipalRef, _ map[string]any) {
			c.CursorKey = bytes.Repeat([]byte{0x38}, 32)
		}},
		{"method", "history.query", "cursor_invalid", func(_ *rpc.Config, _ *model.PrincipalRef, _ map[string]any) {}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := &pagedService{}
			config, by := pagingConfig(), base
			s := pagingServer(t, probe, config)
			first := result(t, call(t, s, rpc.WithPrincipal(context.Background(), by), "bead.ready", paramsJSON(t, pageParams("bead.ready"))))
			params := pageParams(tc.method)
			params["cursor"] = first["nextCursor"]
			tc.change(&config, &by, params)
			s = pagingServer(t, probe, config)
			got := call(t, s, rpc.WithPrincipal(context.Background(), by), tc.method, paramsJSON(t, params))
			requireCode(t, got, tc.code)
			if len(probe.ready) != 1 || len(probe.history) != 0 {
				t.Fatal("mismatched cursor reached the service")
			}
			if tc.code == "incarnation_changed" {
				details := got["error"].(map[string]any)["data"].(map[string]any)["details"].(map[string]any)
				if details["fromIncarnation"] != "inc" || details["toIncarnation"] != "restored" {
					t.Fatal("missing typed incarnation details", details)
				}
			}
		})
	}
}

func TestCursorTamperingAndMalformedTokens(t *testing.T) {
	probe := &pagedService{}
	s := pagingServer(t, probe, pagingConfig())
	params := pageParams("bead.ready")
	first := result(t, call(t, s, context.Background(), "bead.ready", paramsJSON(t, params)))
	token := first["nextCursor"].(string)
	parts := strings.Split(token, ".")
	body, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatal(err)
	}
	body = bytes.Replace(body, []byte(`"offset":1`), []byte(`"offset":2`), 1)
	for _, bad := range []string{"unsigned", "a.b.c", "?.?", parts[0] + ".AA", base64.RawURLEncoding.EncodeToString(body) + "." + parts[1], token + "=", strings.Repeat("x", 16385)} {
		params["cursor"] = bad
		requireCode(t, call(t, s, context.Background(), "bead.ready", paramsJSON(t, params)), "cursor_invalid")
	}
	if len(probe.ready) != 1 {
		t.Fatal("invalid token reached service")
	}
}

func TestCursorRestartAndHistoryAsOf(t *testing.T) {
	probe := &pagedService{}
	config := pagingConfig()
	s := pagingServer(t, probe, config)
	params := pageParams("history.query")
	// Omitted incarnation normalizes to the current store incarnation.
	params["asOf"] = map[string]any{"revision": model.Natural("9007199254740993")}
	first := result(t, call(t, s, context.Background(), "history.query", paramsJSON(t, params)))
	params["cursor"] = first["nextCursor"]
	params["asOf"] = map[string]any{"revision": model.Natural("9007199254740993"), "incarnation": "inc"}
	s = pagingServer(t, probe, config)
	result(t, call(t, s, context.Background(), "history.query", paramsJSON(t, params)))
	for _, at := range []map[string]any{{"revision": 3, "incarnation": "inc"}, {"revision": model.Natural("9007199254740993"), "incarnation": "different"}} {
		params["asOf"] = at
		requireCode(t, call(t, s, context.Background(), "history.query", paramsJSON(t, params)), "cursor_invalid")
	}
	if len(probe.history) != 2 || probe.history[1].Position.Offset != 1 {
		t.Fatal("restart failed or altered asOf reached service")
	}
}

func TestAnonymousCursorBindsAgentLabel(t *testing.T) {
	probe := &pagedService{}
	s := pagingServer(t, probe, pagingConfig())
	params := pageParams("bead.ready")
	params["_meta"] = map[string]any{"agent": "worker-a"}
	first := result(t, call(t, s, context.Background(), "bead.ready", paramsJSON(t, params)))
	params["cursor"] = first["nextCursor"]
	params["_meta"] = map[string]any{"agent": "worker-b"}
	requireCode(t, call(t, s, context.Background(), "bead.ready", paramsJSON(t, params)), "cursor_invalid")
	if len(probe.ready) != 1 {
		t.Fatal("another anonymous agent's cursor reached service")
	}
}

func TestCursorRefusesServiceSnapshotChangeOnLastPage(t *testing.T) {
	probe := &pagedService{badPage: "resnapshot"}
	s := pagingServer(t, probe, pagingConfig())
	params := pageParams("bead.ready")
	first := result(t, call(t, s, context.Background(), "bead.ready", paramsJSON(t, params)))
	params["cursor"] = first["nextCursor"]
	params["limit"] = 2
	requireCode(t, call(t, s, context.Background(), "bead.ready", paramsJSON(t, params)), "internal_error")
}

func TestCursorConfigurationAndServiceInvariants(t *testing.T) {
	config := pagingConfig()
	config.CursorKey = []byte("short")
	if _, err := rpc.New(registry(t), config); err == nil {
		t.Fatal("short signing key accepted")
	}
	for _, bad := range []string{"legacy", "nonadvancing", "revision", "incarnation"} {
		t.Run(bad, func(t *testing.T) {
			s := pagingServer(t, &pagedService{badPage: bad}, pagingConfig())
			requireCode(t, call(t, s, context.Background(), "bead.ready", paramsJSON(t, pageParams("bead.ready"))), "internal_error")
		})
	}
	probe := &pagedService{}
	config = pagingConfig()
	s := pagingServer(t, probe, config)
	params := pageParams("bead.ready")
	first := result(t, call(t, s, context.Background(), "bead.ready", paramsJSON(t, params)))
	params["cursor"] = first["nextCursor"]
	// Server construction must take ownership of key bytes.
	config.CursorKey[0] ^= 0xff
	params["limit"] = 2
	got := result(t, call(t, s, context.Background(), "bead.ready", paramsJSON(t, params)))
	if len(got["items"].([]any)) != 2 || got["nextCursor"] != nil {
		t.Fatal("page size change lost continuation", got)
	}
	config.CursorKey = nil
	s = pagingServer(t, probe, config)
	requireCode(t, call(t, s, context.Background(), "bead.ready", paramsJSON(t, params)), "cursor_invalid")
}
