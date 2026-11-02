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
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
)

func TestB14PublicMetadataFiniteFixedPolicyKeepsLinksRefused(t *testing.T) {
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
		// The adopted CP7 component admits only claim/claimNext/update.
		// A local involved grant does not admit a new named link verb.
		call(spawner, "00000000000000000000000000002002", "forbidden")
		call(spawner, "00000000000000000000000000002002", "forbidden")
		if !reflect.DeepEqual(before, f.meta()) {
			t.Fatal("finite-policy refused link or repeated key changed content")
		}
		base.Grants = nil
		if err = facts.Replace(base); err != nil {
			t.Fatal(err)
		}
		// Replacing installed facts publishes a derived ACL revision. The
		// refused request must publish nothing after that policy change.
		if before.Head.String() == f.meta().Head.String() {
			t.Fatal("policy replacement did not advance its ACL revision")
		}
		before = f.meta()
		call(spawner, "00000000000000000000000000002002", "forbidden")
		if !reflect.DeepEqual(before, f.meta()) {
			t.Fatal("revoked repeated link request changed content")
		}
	})
}

// This composition-root authorizer exercises the existing public Writer port.
// It is a bounded callback contract, not an expansion of finite CP7 policy.
type endpointAuthority struct {
	t      *testing.T
	denied string
	stages []port.AuthorizationStage
}

func (a *endpointAuthority) Authorize(ctx context.Context, stage port.AuthorizationStage, proposal port.ProposedChange, reader port.SnapshotReader) error {
	a.t.Helper()
	a.stages = append(a.stages, stage)
	if len(proposal.Changes) != 1 || proposal.Changes[0].Op.Link == nil {
		a.t.Fatalf("link authority did not receive original operation: %+v", proposal)
	}
	endpoint := proposal.Changes[0].Op.Link.To
	rows, err := reader.Get(ctx, []model.Ref{endpoint})
	if err != nil || len(rows) != 1 || rows[0].Ref.ID != endpoint.ID {
		a.t.Fatalf("authority lost current endpoint snapshot: %v %v", rows, err)
	}
	if endpoint.ID == a.denied {
		return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
	}
	return nil
}

func TestB14PublicMetadataUsesCurrentCompositionAuthority(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		source, _ := f.create(map[string]any{"title": "authorized waiter"}, "")
		spawner, _ := f.create(map[string]any{"title": "spawner"}, "")
		denied, _ := f.create(map[string]any{"title": "denied endpoint"}, "")
		authority := &endpointAuthority{t: t, denied: denied}
		f.bind(pipeline.WithAuthorizer(authority))
		f.refuse("bead.link", map[string]any{"from": ref(source), "to": ref(denied), "kind": "waits-for", "metadata": map[string]any{}}, "forbidden")
		if !reflect.DeepEqual(authority.stages, []port.AuthorizationStage{port.AuthorizeBefore}) {
			t.Fatalf("denied endpoint stages: %v", authority.stages)
		}
		authority.stages = nil
		params := map[string]any{"from": ref(source), "to": ref(spawner), "kind": "waits-for", "metadata": map[string]any{"gate": "any-children", "also_blocks": true}}
		first := f.call("bead.link", params, true, "")
		key := f.lastKey
		assertMetadata(t, f.currentBody(source), "any-children", true)
		if !reflect.DeepEqual(authority.stages, []port.AuthorizationStage{port.AuthorizeBefore, port.AuthorizeAfter}) {
			t.Fatalf("allowed endpoint stages: %v", authority.stages)
		}
		before := f.meta()
		authority.stages = nil
		if replay := f.request("bead.link", params, key, ""); !reflect.DeepEqual(first, replay) || !reflect.DeepEqual(before, f.meta()) {
			t.Fatal("authorized replay changed response or content")
		}
		if !reflect.DeepEqual(authority.stages, []port.AuthorizationStage{port.AuthorizeReplay}) {
			t.Fatalf("replay stages: %v", authority.stages)
		}
		authority.stages = nil
		authority.denied = spawner
		f.request("bead.link", params, key, "forbidden")
		if !reflect.DeepEqual(before, f.meta()) || !reflect.DeepEqual(authority.stages, []port.AuthorizationStage{port.AuthorizeReplay}) {
			t.Fatal("revoked endpoint reused receipt authority or advanced content")
		}
	})
}
