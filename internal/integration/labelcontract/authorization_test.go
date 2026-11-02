package labelcontract_test

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
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// The recorded fixed-permission slice refuses permission-bearing label
// reassignment. Declaring the typed field must not turn it into an unguarded
// side channel, or grant a cached receipt authority after revocation.
func TestB16PublicLabelFixedAuthorizationAndCurrentReplay(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		id, _ := f.create(map[string]any{"title": "authorized", "labels": []string{"kept"}}, "")
		m := f.meta()
		ns := authz.Namespace{StoreID: string(m.StoreID), Incarnation: m.Incarnation, InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))}
		base := authzfixed.Facts{Namespace: ns,
			Projects: []authzfixed.Project{{Ref: model.RefByID("local", model.Natural64(0)), Name: "local", InstallationID: "fixed", Active: true}},
			Grants:   []authz.Grant{{Subject: authz.SubjectRef{Kind: "user", ID: "user:operator"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: model.RefByID("local", model.Natural64(0))}, Role: "involved"}},
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
		by := model.PrincipalRef{Kind: "agent", ID: "agent:label-worker", Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:operator"}}
		token, err := keys.Issue(by, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		call := func(fields map[string]any, key string, code string) map[string]any {
			t.Helper()
			raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "bead.update", "params": map[string]any{"id": id, "fields": fields, "_meta": map[string]any{"idempotencyKey": key}}})
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/rpc/v1", bytes.NewReader(raw))
			req.RemoteAddr = "127.0.0.1:12345"
			req.Header.Set("Plug-RPC", "1")
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+token.Token)
			r := httptest.NewRecorder()
			app.Handler.ServeHTTP(r, req)
			var reply map[string]any
			if err := json.Unmarshal(r.Body.Bytes(), &reply); err != nil {
				t.Fatal(err)
			}
			if code != "" {
				e, ok := reply["error"].(map[string]any)
				if !ok || e["data"].(map[string]any)["code"] != code || reply["result"] != nil {
					t.Fatalf("want %s, got %s", code, r.Body.String())
				}
			} else if reply["error"] != nil {
				t.Fatalf("authorized label request: %s", r.Body.String())
			}
			return reply
		}
		noop := map[string]any{"labels": []string{"kept", "kept", ""}}
		before := f.meta()
		first := call(noop, "00000000000000000000000000001001", "")
		if !reflect.DeepEqual(before, f.meta()) {
			t.Fatal("authorized label no-op advanced content")
		}
		replay := call(noop, "00000000000000000000000000001001", "")
		if !reflect.DeepEqual(first["result"], replay["result"]) || replay["_meta"].(map[string]any)["replayed"] != true {
			t.Fatalf("label no-op was not replayed: %v", replay)
		}
		call(map[string]any{"title": "permitted scalar change"}, "00000000000000000000000000001002", "")
		f.labels(id, "kept")
		before = f.meta()
		call(map[string]any{"labels": []string{"reassigned"}}, "00000000000000000000000000001003", "forbidden")
		if !reflect.DeepEqual(before, f.meta()) {
			t.Fatal("permission-bearing label refusal advanced content")
		}
		r, err := f.store.Read(context.Background(), driver.View{Incarnation: before.Incarnation})
		if err != nil {
			t.Fatal(err)
		}
		principal, err := driver.PrincipalKey(by)
		if err != nil {
			t.Fatal(err)
		}
		receipt, err := r.IdempotencyByKey(context.Background(), principal, "", "bead.update", "00000000000000000000000000001003")
		if closeErr := r.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		if err != nil || receipt != nil {
			t.Fatalf("label refusal left a receipt: %+v %v", receipt, err)
		}
		f.labels(id, "kept")
		base.Grants = nil
		if err := facts.Replace(base); err != nil {
			t.Fatal(err)
		}
		before = f.meta()
		call(noop, "00000000000000000000000000001001", "forbidden")
		if !reflect.DeepEqual(before, f.meta()) {
			t.Fatal("revoked label replay advanced content")
		}
	})
}
