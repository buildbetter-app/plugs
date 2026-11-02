package gatecontract_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
)

func TestB14PublicMetadataEndpointAuthorizationAndCurrentReplay(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		source, _ := f.create(map[string]any{"title": "authorized waiter"}, "")
		spawner, _ := f.create(map[string]any{"title": "authorized spawner"}, "")
		hiddenCommit := f.call("bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "hidden spawner"}, "_meta": map[string]any{"project": "hidden"}}, true, "")
		hidden := resultID(hiddenCommit, 0)
		m := f.meta()
		ns := authz.Namespace{StoreID: string(m.StoreID), Incarnation: m.Incarnation, InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))}
		base := authzfixed.Facts{Namespace: ns,
			Projects: []authzfixed.Project{
				{Ref: model.RefByID("local", model.Natural64(0)), Name: "local", InstallationID: "fixed", Active: true},
				{Ref: model.RefByID("hidden", model.Natural64(0)), Name: "hidden", InstallationID: "fixed", Active: true},
			},
			Grants: []authz.Grant{{Subject: authz.SubjectRef{Kind: "user", ID: "user:operator"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: model.RefByID("local", model.Natural64(0))}, Role: "involved"}},
		}
		facts, err := authzrpc.NewMutableFacts(base)
		if err != nil {
			t.Fatal(err)
		}
		keys := authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
		app, err := authzrpc.New(context.Background(), authzrpc.Config{Store: f.store, Lease: f.lease, Provider: keys, Facts: facts, Namespace: ns, DefaultProject: "local"})
		if err != nil {
			t.Fatal(err)
		}
		by := model.PrincipalRef{Kind: "agent", ID: "agent:gate-worker", Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:operator"}}
		token, err := keys.Issue(by, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		call := func(to, key, code string) map[string]any {
			t.Helper()
			params := map[string]any{"from": ref(source), "to": ref(to), "kind": "waits-for", "metadata": map[string]any{"gate": "any-children", "also_blocks": true}, "_meta": map[string]any{"idempotencyKey": key}}
			raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "bead.link", "params": params})
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/rpc/v1", bytes.NewReader(raw))
			req.RemoteAddr = "127.0.0.1:12345"
			req.Header.Set("Plug-RPC", "1")
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+token.Token)
			recorder := httptest.NewRecorder()
			app.Handler.ServeHTTP(recorder, req)
			var reply map[string]any
			if err = json.Unmarshal(recorder.Body.Bytes(), &reply); err != nil {
				t.Fatal(err)
			}
			if code != "" {
				e, ok := reply["error"].(map[string]any)
				if !ok || e["data"].(map[string]any)["code"] != code || reply["result"] != nil {
					t.Fatalf("want %s got %s", code, recorder.Body.String())
				}
			} else if reply["error"] != nil {
				t.Fatalf("authorized link refused: %s", recorder.Body.String())
			}
			return reply
		}
		before := f.meta()
		call(hidden, "00000000000000000000000000002001", "forbidden")
		if !reflect.DeepEqual(before, f.meta()) {
			t.Fatal("unauthorized endpoint wrote metadata")
		}
		first := call(spawner, "00000000000000000000000000002002", "")
		assertMetadata(t, f.currentBody(source), "any-children", true)
		before = f.meta()
		replay := call(spawner, "00000000000000000000000000002002", "")
		if !reflect.DeepEqual(first["result"], replay["result"]) || replay["_meta"].(map[string]any)["replayed"] != true || !reflect.DeepEqual(before, f.meta()) {
			t.Fatal("authorized metadata replay changed content")
		}
		base.Grants = nil
		if err = facts.Replace(base); err != nil {
			t.Fatal(err)
		}
		call(spawner, "00000000000000000000000000002002", "forbidden")
		if !reflect.DeepEqual(before, f.meta()) {
			t.Fatal("revoked metadata replay changed content")
		}
	})
}
