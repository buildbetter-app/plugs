package watchhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/collab"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
)

func TestActualHTTPRPCWriteNDJSONAndRepairedCollabSessionLifetime(t *testing.T) {
	f := newTransportFixture(t)
	f.principal = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	sessions, err := collab.New(f.ctx, collab.Config{Service: f.svc, Lease: f.lease, AuthorizationInstalled: func() bool { return false }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sessions.Close() })
	caller := collab.Caller{Principal: f.principal, Agent: "watch-http-test"}
	session, err := sessions.Open(f.ctx, caller, "local", nil)
	if err != nil {
		t.Fatal(err)
	}
	lifetime, err := sessions.SessionContext(f.ctx, caller, session.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := authn.New(authn.Config{})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpjson.New(f.server, admission, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle(httpjson.Path, handler)
	mux.Handle(DefaultPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, err := admission.AdmitHTTP(r)
		if err != nil {
			http.Error(w, "unauthenticated", 401)
			return
		}
		f.adapter.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), identity.Principal())))
	}))
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r.WithContext(WithSession(r.Context(), session.SessionID, lifetime)))
	}))
	t.Cleanup(host.Close)
	client := host.Client()
	client.Timeout = 3 * time.Second
	call := func(method string, params any) json.RawMessage {
		t.Helper()
		body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
		if err != nil {
			t.Fatal(err)
		}
		request, err := http.NewRequest(http.MethodPost, host.URL+httpjson.Path, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Plug-RPC", "1")
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		var envelope struct {
			Result json.RawMessage `json:"result"`
			Error  json.RawMessage `json:"error"`
		}
		if err = json.Unmarshal(raw, &envelope); err != nil || response.StatusCode != 200 || len(envelope.Error) > 0 {
			t.Fatalf("HTTP RPC %s status=%d body=%s err=%v", method, response.StatusCode, raw, err)
		}
		return envelope.Result
	}
	open := func() string {
		t.Helper()
		raw := call("event.subscribe", map[string]any{"filter": map[string]any{"events": []string{"bead.created"}}, "_meta": map[string]any{"project": "local"}})
		var result struct {
			ID string `json:"subscriptionId"`
		}
		if err := json.Unmarshal(raw, &result); err != nil || result.ID == "" {
			t.Fatalf("subscribe=%s err=%v", raw, err)
		}
		return result.ID
	}
	attach := func(id string) *http.Response {
		t.Helper()
		request, err := http.NewRequest(http.MethodPost, host.URL+DefaultPath, strings.NewReader(fmt.Sprintf(`{"subscriptionId":%q}`, id)))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Plug-RPC", "1")
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = response.Body.Close() })
		if response.StatusCode != 200 || response.Header.Get("Content-Type") != "application/x-ndjson" {
			t.Fatalf("stream status=%d headers=%v", response.StatusCode, response.Header)
		}
		return response
	}
	id := open() // The real HTTP RPC request context has now ended.
	response := attach(id)
	decoder := json.NewDecoder(response.Body)
	var snapshot, delta eventNotify
	if err := decoder.Decode(&snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.SubscriptionID != id || len(snapshot.Events) != 0 {
		t.Fatalf("snapshot bytes=%+v", snapshot)
	}
	call("bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "HTTP write"}, "_meta": map[string]any{"project": "local", "idempotencyKey": fmt.Sprintf("%032x", 201)}})
	if err := decoder.Decode(&delta); err != nil {
		t.Fatal(err)
	}
	if len(delta.Events) != 1 || delta.Events[0].Control == nil || delta.Events[0].Control.Type != model.EventWatchDelta {
		t.Fatalf("delta=%+v", delta)
	}
	details := delta.Events[0].Control.Details.(model.WatchDeltaDetails)
	if details.FromCursor != snapshot.Cursor || len(details.Events) != 1 || details.Events[0].Durable == nil || details.Events[0].Durable.After["fields"].(map[string]any)["title"] != "HTTP write" {
		t.Fatalf("actual RPC write missing from delivered bytes: %+v", details)
	}
	if err := sessions.CloseSession(f.ctx, caller, session.SessionID); err != nil {
		t.Fatal(err)
	}
	if lifetime.Err() == nil {
		t.Fatal("actual repaired session close did not cancel its lifetime")
	}
	if err := decoder.Decode(new(eventNotify)); err != io.EOF {
		t.Fatalf("session close did not finish HTTP stream: %v", err)
	}
	waitCount(t, f, session.SessionID, 0)
	t.Log("actual HTTP RPC request ended; stream delivered actual core write; real collab close canceled stream and released watch slot")
}

func TestActualHTTPViewWatchSnapshotDeltaUsesSavedViewResolver(t *testing.T) {
	f := newTransportFixture(t)
	f.principal = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	f.views["resource"] = viewops.Definition{Filter: &viewops.Filter{Resources: []string{"http-bead"}}}
	sessions, err := collab.New(f.ctx, collab.Config{Service: f.svc, Lease: f.lease, AuthorizationInstalled: func() bool { return false }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sessions.Close() })
	caller := collab.Caller{Principal: f.principal, Agent: "view-watch-http-test"}
	session, err := sessions.Open(f.ctx, caller, "local", nil)
	if err != nil {
		t.Fatal(err)
	}
	lifetime, err := sessions.SessionContext(f.ctx, caller, session.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := authn.New(authn.Config{})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpjson.New(f.server, admission, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle(httpjson.Path, handler)
	mux.Handle(DefaultPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, err := admission.AdmitHTTP(r)
		if err != nil {
			http.Error(w, "unauthenticated", http.StatusUnauthorized)
			return
		}
		f.adapter.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), identity.Principal())))
	}))
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r.WithContext(WithSession(r.Context(), session.SessionID, lifetime)))
	}))
	t.Cleanup(host.Close)
	client := host.Client()
	client.Timeout = 3 * time.Second
	call := func(method string, params any) json.RawMessage {
		t.Helper()
		body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
		if err != nil {
			t.Fatal(err)
		}
		request, err := http.NewRequest(http.MethodPost, host.URL+httpjson.Path, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Plug-RPC", "1")
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		var envelope struct {
			Result json.RawMessage `json:"result"`
			Error  json.RawMessage `json:"error"`
		}
		if err = json.Unmarshal(raw, &envelope); err != nil || response.StatusCode != http.StatusOK || len(envelope.Error) != 0 {
			t.Fatalf("HTTP RPC %s status=%d body=%s err=%v", method, response.StatusCode, raw, err)
		}
		return envelope.Result
	}
	raw := call("view.watch", map[string]any{"viewId": "resource", "_meta": map[string]any{"project": "local"}})
	var opened struct {
		ID string `json:"subscriptionId"`
	}
	if err := json.Unmarshal(raw, &opened); err != nil || opened.ID == "" {
		t.Fatalf("view.watch=%s err=%v", raw, err)
	}
	request, err := http.NewRequest(http.MethodPost, host.URL+DefaultPath, strings.NewReader(fmt.Sprintf(`{"subscriptionId":%q}`, opened.ID)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Plug-RPC", "1")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("view stream status=%d", response.StatusCode)
	}
	decoder := json.NewDecoder(response.Body)
	var snapshot, delta eventNotify
	if err := decoder.Decode(&snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.SubscriptionID != opened.ID || len(snapshot.Events) != 0 || snapshot.Projection == nil || snapshot.Projection.Binding != "core" {
		t.Fatalf("view HTTP snapshot=%+v", snapshot)
	}
	f.commit(t)
	if err := decoder.Decode(&delta); err != nil {
		t.Fatal(err)
	}
	if delta.SubscriptionID != opened.ID || len(delta.Events) != 1 || delta.Events[0].Control == nil || delta.Events[0].Control.Type != model.EventWatchDelta || delta.Projection == nil || delta.Projection.Binding != "core" {
		t.Fatalf("view HTTP delta=%+v", delta)
	}
	details := delta.Events[0].Control.Details.(model.WatchDeltaDetails)
	if len(details.Events) != 1 || details.Events[0].Durable == nil || details.Events[0].Durable.Resource != "bead/http-bead" {
		t.Fatalf("view HTTP delta details=%+v", details)
	}
	_ = sessions.CloseSession(f.ctx, caller, session.SessionID)
}

func waitCount(t *testing.T, f *transportFixture, id string, want int) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if f.manager.WatchCount(id) == want {
			return
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatalf("watch slots=%d want %d", f.manager.WatchCount(id), want)
		}
	}
}

func TestRPCRequiresTrustedLifetimeAndRejectsUnsupportedFilters(t *testing.T) {
	f := newTransportFixture(t)
	for _, test := range []struct {
		name, params string
		lifetime     bool
	}{
		{"missing-lifetime", `{"filter":{}}`, false},
		{"bead-type", `{"filter":{"types":["issue"]}}`, true},
		{"query", `{"filter":{"query":"status:todo"}}`, true},
		{"view", `{"filter":{"viewId":"unadopted"}}`, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := rpc.WithPrincipal(context.Background(), f.principal)
			if test.lifetime {
				ctx = WithSession(ctx, "s", f.ctx)
			}
			raw := f.server.Dispatch(ctx, []byte(`{"jsonrpc":"2.0","id":1,"method":"event.subscribe","params":`+test.params+`}`))
			var envelope struct {
				Error struct {
					Data struct {
						Code string `json:"code"`
					} `json:"data"`
				} `json:"error"`
			}
			if err := json.Unmarshal(raw, &envelope); err != nil || envelope.Error.Data.Code != "invalid_params" {
				t.Fatalf("unsupported request=%s err=%v", raw, err)
			}
			if f.manager.WatchCount("s") != 0 {
				t.Fatal("refusal retained watch")
			}
		})
	}
}
