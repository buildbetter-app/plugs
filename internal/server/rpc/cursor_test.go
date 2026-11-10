package rpc_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
)

func TestReadyMembershipTransportVersionsAndSignedMapping(t *testing.T) {
	for _, tc := range []struct {
		name       string
		membership string
		version    int
	}{
		{"released-unprefixed", strings.Repeat("a", 64), 1},
		{"released-v2", "v2:" + strings.Repeat("b", 64), 2},
		{"combined-order-v3", "v3:" + strings.Repeat("c", 64), 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			at := service.AsOf{Revision: model.Natural64(9), Incarnation: "inc"}
			binding := &service.WorkflowPageSnapshot{Revision: model.Natural64(12)}
			calls := 0
			probe := &incarnationService{readyPage: func(_ context.Context, req service.ReadyRequest) (service.BeadPage, error) {
				calls++
				if req.Position == nil {
					return service.BeadPage{Items: []service.Bead{{ID: "first", Type: "task"}}, AsOf: at, WorkflowSnapshot: binding, ReadyMembership: tc.membership,
						Next: &service.PagePosition{Offset: 1, AsOf: at, WorkflowSnapshot: binding, ReadyMembership: tc.membership}}, nil
				}
				if req.Position.Offset != 1 || req.Position.ReadyMembership != tc.membership || !binding.Equal(req.Position.WorkflowSnapshot) {
					t.Fatalf("signed membership/snapshot changed at service: %+v", req.Position)
				}
				return service.BeadPage{Items: []service.Bead{{ID: "second", Type: "task"}}, AsOf: at, WorkflowSnapshot: binding, ReadyMembership: tc.membership}, nil
			}}
			config := pagingConfig()
			s := pagingServer(t, probe, config)
			params := pageParams("bead.ready")
			first := result(t, call(t, s, context.Background(), "bead.ready", paramsJSON(t, params)))
			token, ok := first["nextCursor"].(string)
			if !ok || token == "" {
				t.Fatalf("version %d did not issue a signed continuation: %+v", tc.version, first)
			}
			body, err := base64.RawURLEncoding.DecodeString(strings.Split(token, ".")[0])
			if err != nil {
				t.Fatal(err)
			}
			var payload map[string]any
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatal(err)
			}
			if payload["version"] != float64(tc.version) || payload["readyMembership"] != tc.membership {
				t.Fatalf("signed transport mapping=%v want version=%d membership=%q", payload, tc.version, tc.membership)
			}
			params["cursor"] = token
			last := result(t, call(t, s, context.Background(), "bead.ready", paramsJSON(t, params)))
			if calls != 2 || last["nextCursor"] != nil || last["items"].([]any)[0].(map[string]any)["id"] != "second" {
				t.Fatalf("signed continuation failed: calls=%d last=%v", calls, last)
			}

			// Even a correctly authenticated body cannot claim another format's
			// transport version and reach the service with that position.
			wrong := tc.version%3 + 1
			payload["version"] = wrong
			changed, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			mac := hmac.New(sha256.New, config.CursorKey)
			_, _ = mac.Write(changed)
			params["cursor"] = base64.RawURLEncoding.EncodeToString(changed) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
			requireCode(t, call(t, s, context.Background(), "bead.ready", paramsJSON(t, params)), "cursor_invalid")
			if calls != 2 {
				t.Fatal("mismatched authenticated version reached the service")
			}
		})
	}
}

func TestWorkflowCursorCarriesSnapshotAcrossPhaseTransition(t *testing.T) {
	deadline := time.Date(2026, 11, 3, 1, 0, 0, 0, time.UTC)
	binding := &service.WorkflowPageSnapshot{Revision: model.Natural64(12), ValidUntil: &deadline}
	at := service.AsOf{Revision: model.Natural64(9), Incarnation: "inc"}
	calls := 0
	probe := &incarnationService{readyPage: func(_ context.Context, req service.ReadyRequest) (service.BeadPage, error) {
		calls++
		if req.Position == nil {
			// The core phase exactly filled the first page. Its provider offset
			// starts at zero, while the captured revision/deadline is signed.
			return service.BeadPage{Items: []service.Bead{{ID: "core", Type: "issue"}}, AsOf: at, WorkflowSnapshot: binding,
				Next: &service.PagePosition{Offset: 0, AsOf: at, Workflow: true, WorkflowSnapshot: binding}}, nil
		}
		if !req.Position.Workflow || req.Position.Offset != 0 || !binding.Equal(req.Position.WorkflowSnapshot) {
			t.Fatalf("signed provider phase lost its snapshot: %+v", req.Position)
		}
		return service.BeadPage{Items: []service.Bead{{ID: "next", Type: "issue"}}, AsOf: at, WorkflowSnapshot: binding}, nil
	}}
	s := pagingServer(t, probe, pagingConfig())
	params := pageParams("bead.ready")
	params["limit"] = 1
	first := result(t, call(t, s, context.Background(), "bead.ready", paramsJSON(t, params)))
	params["cursor"] = first["nextCursor"]
	last := result(t, call(t, s, context.Background(), "bead.ready", paramsJSON(t, params)))
	if calls != 2 || last["nextCursor"] != nil || last["items"].([]any)[0].(map[string]any)["id"] != "next" {
		t.Fatal("provider transition failed", last)
	}
}

func TestWorkflowCursorRejectsInvalidServiceBindings(t *testing.T) {
	for _, invalid := range []string{"unbound-phase", "different-page", "invalid-generation", "history-state"} {
		t.Run(invalid, func(t *testing.T) {
			binding := &service.WorkflowPageSnapshot{Revision: model.Natural64(12)}
			at := service.AsOf{Revision: model.Natural("9007199254740993"), Incarnation: "inc"}
			page := service.BeadPage{Items: []service.Bead{{ID: "item", Type: "issue"}}, AsOf: at, WorkflowSnapshot: binding,
				Next: &service.PagePosition{Offset: 1, AsOf: at, Workflow: true, WorkflowSnapshot: binding}}
			method := "bead.ready"
			switch invalid {
			case "unbound-phase":
				page.WorkflowSnapshot = nil
				page.Next.WorkflowSnapshot = nil
			case "different-page":
				page.Next.WorkflowSnapshot = &service.WorkflowPageSnapshot{Revision: model.Natural64(13)}
			case "invalid-generation":
				binding.Revision = model.Natural("-1")
			case "history-state":
				method = "history.query"
			}
			probe := &incarnationService{
				readyPage:   func(context.Context, service.ReadyRequest) (service.BeadPage, error) { return page, nil },
				historyPage: func(context.Context, service.HistoryRequest) (service.BeadPage, error) { return page, nil },
			}
			s := pagingServer(t, probe, pagingConfig())
			requireCode(t, call(t, s, context.Background(), method, paramsJSON(t, pageParams(method))), "internal_error")
		})
	}
}

// A domain-only fixture checks the wire/Position seam. The real service's
// storage ordering and live/historical semantics are verified by real tests.
type pagedService struct {
	serviceProbe
	ready   []service.ReadyRequest
	history []service.HistoryRequest
	badPage string
}

type incarnationService struct {
	serviceProbe
	readyPage   func(context.Context, service.ReadyRequest) (service.BeadPage, error)
	historyPage func(context.Context, service.HistoryRequest) (service.BeadPage, error)
}

func (p *incarnationService) Ready(ctx context.Context, _ model.PrincipalRef, r service.ReadyRequest) (service.BeadPage, error) {
	return p.readyPage(ctx, r)
}

func (p *incarnationService) History(ctx context.Context, _ model.PrincipalRef, r service.HistoryRequest) (service.BeadPage, error) {
	return p.historyPage(ctx, r)
}

func TestCurrentIncarnationPagesAndOldCursorsOnSameServer(t *testing.T) {
	for _, method := range []string{"bead.ready", "history.query"} {
		for _, omitted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/omitted=%v", method, omitted), func(t *testing.T) {
				current, lookups, calls := "inc", 0, 0
				probe := &incarnationService{}
				probe.readyPage = func(_ context.Context, r service.ReadyRequest) (service.BeadPage, error) {
					calls++
					return (&pagedService{}).page(r.Limit, service.AsOf{Revision: model.Natural("9007199254740993"), Incarnation: current}, r.Position), nil
				}
				probe.historyPage = func(_ context.Context, r service.HistoryRequest) (service.BeadPage, error) {
					calls++
					if r.AsOf.Incarnation == "" {
						t.Fatal("history service did not receive the request's incarnation snapshot")
					}
					return (&pagedService{}).page(r.Limit, r.AsOf, r.Position), nil
				}
				config := pagingConfig()
				config.CurrentIncarnation = func(context.Context) (string, error) { lookups++; return current, nil }
				s := pagingServer(t, probe, config)
				params := pageParams(method)
				if method == "history.query" && omitted {
					delete(params["asOf"].(map[string]any), "incarnation")
				}
				old := result(t, call(t, s, context.Background(), method, paramsJSON(t, params)))["nextCursor"].(string)
				current = "restored"
				params["cursor"] = old
				got := call(t, s, context.Background(), method, paramsJSON(t, params))
				requireCode(t, got, "incarnation_changed")
				details := got["error"].(map[string]any)["data"].(map[string]any)["details"].(map[string]any)
				if details["fromIncarnation"] != "inc" || details["toIncarnation"] != current || calls != 1 {
					t.Fatalf("old token reached service/lost generations: calls=%d %v", calls, got)
				}
				params["filter"].(map[string]any)["query"] = "status:closed"
				requireCode(t, call(t, s, context.Background(), method, paramsJSON(t, params)), "cursor_invalid")
				delete(params["filter"].(map[string]any), "query")
				params["cursor"] = strings.Split(old, ".")[0] + ".AA"
				requireCode(t, call(t, s, context.Background(), method, paramsJSON(t, params)), "cursor_invalid")
				if calls != 1 {
					t.Fatal("invalid old token reached the service")
				}
				delete(params, "cursor")
				if method == "history.query" && !omitted {
					// An explicit old request is preserved, not rewritten to I1.
					requireCode(t, call(t, s, context.Background(), method, paramsJSON(t, params)), "internal_error")
					params["asOf"].(map[string]any)["incarnation"] = current
				}
				for i := 0; i < 3; i++ {
					before := lookups
					page := result(t, call(t, s, context.Background(), method, paramsJSON(t, params)))
					if lookups != before+1 || page["asOf"].(map[string]any)["incarnation"] != current || page["items"].([]any)[0].(map[string]any)["id"] != fmt.Sprintf("p-%d", i+1) {
						t.Fatalf("fresh page lost request snapshot: lookups=%d %v", lookups-before, page)
					}
					if i < 2 {
						params["cursor"] = page["nextCursor"]
					} else if page["nextCursor"] != nil {
						t.Fatal("unexpected final cursor")
					}
				}
			})
		}
	}
}

func TestCurrentIncarnationProviderFailsClosedAndRejectsMismatchedPages(t *testing.T) {
	for _, value := range []string{"", "error"} {
		t.Run(value, func(t *testing.T) {
			probe := &pagedService{}
			config := pagingConfig()
			config.CurrentIncarnation = func(context.Context) (string, error) {
				if value == "error" {
					return "inc", fmt.Errorf("metadata unavailable")
				}
				return value, nil
			}
			s := pagingServer(t, probe, config)
			for _, method := range []string{"bead.ready", "history.query", "server.hello"} {
				params := map[string]any{}
				if method != "server.hello" {
					params = pageParams(method)
				}
				requireCode(t, call(t, s, context.Background(), method, paramsJSON(t, params)), "internal_error")
			}
			if len(probe.ready)+len(probe.history) != 0 {
				t.Fatal("provider failure reached service")
			}
			result(t, call(t, s, context.Background(), "bead.get", `{"id":{"id":"p-1"}}`))
			result(t, call(t, s, context.Background(), "rpc.methods", `{}`))
		})
	}
	for _, bad := range []string{"stale-page", "incarnation"} {
		t.Run(bad, func(t *testing.T) {
			config := pagingConfig()
			config.CurrentIncarnation = func(context.Context) (string, error) {
				if bad == "stale-page" {
					return "restored", nil
				}
				return "inc", nil
			}
			s := pagingServer(t, &pagedService{badPage: bad}, config)
			reply := call(t, s, context.Background(), "bead.ready", paramsJSON(t, pageParams("bead.ready")))
			requireCode(t, reply, "internal_error")
			if reply["result"] != nil {
				t.Fatal("mismatched service page was relabeled")
			}
		})
	}
}

func TestCurrentIncarnationSnapshotIsLocalToEachConcurrentCall(t *testing.T) {
	type generationKey struct{}
	entered, release := make(chan struct{}), make(chan struct{})
	var lookups atomic.Int64
	probe := &incarnationService{readyPage: func(ctx context.Context, r service.ReadyRequest) (service.BeadPage, error) {
		generation := ctx.Value(generationKey{}).(string)
		if generation == "inc" {
			close(entered)
			<-release
		}
		return (&pagedService{}).page(r.Limit, service.AsOf{Revision: model.Natural64(1), Incarnation: generation}, r.Position), nil
	}}
	config := pagingConfig()
	config.CurrentIncarnation = func(ctx context.Context) (string, error) {
		lookups.Add(1)
		return ctx.Value(generationKey{}).(string), nil
	}
	s := pagingServer(t, probe, config)
	oldResponse := make(chan []byte, 1)
	go func() {
		oldResponse <- s.Dispatch(context.WithValue(context.Background(), generationKey{}, "inc"), request("bead.ready", `{"limit":1}`))
	}()
	<-entered
	defer close(release)
	newPage := result(t, call(t, s, context.WithValue(context.Background(), generationKey{}, "restored"), "bead.ready", `{"limit":1}`))
	if newPage["asOf"].(map[string]any)["incarnation"] != "restored" {
		t.Fatal("concurrent new call lost snapshot", newPage)
	}
	// Let the earlier request finish while the later request's codec exists.
	release <- struct{}{}
	var old map[string]any
	if err := json.Unmarshal(<-oldResponse, &old); err != nil {
		t.Fatal(err)
	}
	oldPage := result(t, old)
	if oldPage["asOf"].(map[string]any)["incarnation"] != "inc" || lookups.Load() != 2 {
		t.Fatalf("shared codec mutated or provider reread: lookups=%d %v", lookups.Load(), oldPage)
	}
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
			out.AsOf.Revision = model.Natural("9007199254740994")
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
					t.Fatalf("page lost observed revision: %v", got)
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

func TestLiveReadyAllowsNewRevisionButHistoryRemainsPinned(t *testing.T) {
	for _, method := range []string{"bead.ready", "history.query"} {
		for _, limit := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s/limit%d", method, limit), func(t *testing.T) {
				probe := &pagedService{badPage: "resnapshot"}
				s := pagingServer(t, probe, pagingConfig())
				params := pageParams(method)
				first := result(t, call(t, s, context.Background(), method, paramsJSON(t, params)))
				params["cursor"] = first["nextCursor"]
				params["limit"] = limit
				reply := call(t, s, context.Background(), method, paramsJSON(t, params))
				if method == "history.query" {
					requireCode(t, reply, "internal_error")
					return
				}
				page := result(t, reply)
				if len(page["items"].([]any)) != limit || page["asOf"].(map[string]any)["revision"] != json.Number("9007199254740994") {
					t.Fatal("live Ready did not accept a newer page", page)
				}
				if limit == 1 {
					params["cursor"] = page["nextCursor"]
					page = result(t, call(t, s, context.Background(), method, paramsJSON(t, params)))
					position := probe.ready[2].Position
					if position.Offset != 2 || position.AsOf.Revision != model.Natural("9007199254740994") || page["items"].([]any)[0].(map[string]any)["id"] != "p-3" {
						t.Fatal("live Ready lost the newly signed continuation", position, page)
					}
				}
				if page["nextCursor"] != nil {
					t.Fatal("live Ready returned an extra final cursor", page)
				}
			})
		}
	}
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
