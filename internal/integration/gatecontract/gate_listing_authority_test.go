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

// This explicitly installed read-only test rule exercises the host callback
// seam. The production fixed component and its finite policy remain intact.
type gateListingReadRule struct {
	authz.RuleSet
	denied  string
	failure bool
}

func (r *gateListingReadRule) Eval(ctx context.Context, facts authz.Slice, req []authz.CheckRequest) ([]authz.Decision, error) {
	if r.failure {
		return nil, &authz.Error{Code: "authz_unavailable"}
	}
	decisions, err := r.RuleSet.Eval(ctx, facts, req)
	if err != nil {
		return nil, err
	}
	for i, q := range req {
		if q.Action == authz.Read && q.Resource.Ref.ID == r.denied {
			decisions[i] = authz.Decision{Effect: authz.Deny}
		}
	}
	return decisions, nil
}

func TestB14PublicGateListingComposedReadVisibilityBeforeLimit(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		hidden := gateCreate(f, "gate", "timer", map[string]any{"priority": 0})
		created := f.call("bead.create", map[string]any{"type": "gate", "parent": ref(hidden), "fields": map[string]any{"title": "visible child gate", "await_type": "gh:run", "priority": 1}}, true, "")
		child := resultID(created, 0)
		gateCreate(f, "gate", "timer", nil)
		m := f.meta()
		ns := authz.Namespace{StoreID: string(m.StoreID), Incarnation: m.Incarnation, InstallationID: "b14-gate-list-read", Authority: model.RefByID("authority", model.Natural64(0))}
		facts, err := authzrpc.NewMutableFacts(authzfixed.Facts{Namespace: ns, Projects: []authzfixed.Project{{Ref: model.RefByID("local", model.Natural64(0)), Name: "local", InstallationID: ns.InstallationID, Active: true}}})
		if err != nil {
			t.Fatal(err)
		}
		keys := authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
		app, err := authzrpc.New(context.Background(), authzrpc.Config{Store: f.store, Lease: f.lease, Provider: keys, Facts: facts, Namespace: ns, DefaultProject: "local"})
		if err != nil {
			t.Fatal(err)
		}
		rule := &gateListingReadRule{RuleSet: app.Chain.Rules[0], denied: hidden}
		app.Chain.Rules[0] = rule
		token, err := keys.Issue(model.PrincipalRef{Kind: "user", ID: "user:gate-reader"}, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		call := func(code string) map[string]any {
			t.Helper()
			raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "gate.list", "params": map[string]any{"limit": 1}})
			req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/rpc/v1", bytes.NewReader(raw))
			req.RemoteAddr = "127.0.0.1:12345"
			req.Header.Set("Plug-RPC", "1")
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+token.Token)
			w := httptest.NewRecorder()
			app.Handler.ServeHTTP(w, req)
			var response map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if code != "" {
				err, ok := response["error"].(map[string]any)
				if !ok || err["data"].(map[string]any)["code"] != code || response["result"] != nil {
					t.Fatalf("want %s: %s", code, w.Body.String())
				}
				return nil
			}
			if response["error"] != nil {
				t.Fatalf("composed gate.list: %s", w.Body.String())
			}
			return response["result"].(map[string]any)
		}
		before, history := f.meta(), f.historyCount()
		page := call("")
		if !reflect.DeepEqual(gateIDs(page), []string{child}) {
			t.Fatalf("hidden prefix consumed selected gate limit: %v", page)
		}
		bead := page["items"].([]any)[0].(map[string]any)
		if !reflect.DeepEqual(bead["parent"], map[string]any{"exists": true}) {
			t.Fatalf("hidden parent projection=%v", bead["parent"])
		}
		rule.failure = true
		call("authz_unavailable")
		if !reflect.DeepEqual(before, f.meta()) || history != f.historyCount() {
			t.Fatal("authorized listing or failed policy read wrote state/history")
		}
	})
}
