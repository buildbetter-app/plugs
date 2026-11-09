package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

func jointOwnedProfile(t *testing.T, data []byte, p driver.RestoreImportProfile, owner model.PrincipalRef) driver.RestoreImportProfile {
	t.Helper()
	plan, err := p.SourcePlanes.Plan()
	if err != nil {
		t.Fatal(err)
	}
	plan.Principals[0].PrincipalJSON, err = json.Marshal(owner)
	if err != nil {
		t.Fatal(err)
	}
	plan.Providers[0].LogicalImageSHA256, plan.Providers[0].LogicalImageSize = "", 0
	p.SourcePlanes, p.DerivedDigest = nil, ""
	p, err = BindSourceImport(context.Background(), bytes.NewReader(data), p, plan)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func sourceHTTPCall(t *testing.T, handler http.Handler, keys *authnapikey.Provider, actor model.PrincipalRef, method string, key int, params map[string]any, want string) sourcePublicReply {
	t.Helper()
	issued, err := keys.Issue(actor, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return sourceHTTPTokenCall(t, handler, issued.Token, method, key, params, want)
}

func sourceHTTPTokenCall(t *testing.T, handler http.Handler, token, method string, key int, params map[string]any, want string) sourcePublicReply {
	t.Helper()
	meta := map[string]any{"project": "demo"}
	if key != 0 {
		meta["idempotencyKey"] = fmt.Sprintf("%032d", key)
	}
	params["_meta"] = meta
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1"+httpjson.Path, bytes.NewReader(body))
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Plug-RPC", "1")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	out := httptest.NewRecorder()
	handler.ServeHTTP(out, req)
	var reply sourcePublicReply
	if err := json.Unmarshal(out.Body.Bytes(), &reply); err != nil {
		t.Fatalf("%s HTTP%d %s: %v", method, out.Code, out.Body.String(), err)
	}
	if want != "" {
		if reply.Error == nil || reply.Error.Data.Code != want || len(reply.Result) != 0 {
			t.Fatalf("%s want %s HTTP%d: %s", method, want, out.Code, out.Body.String())
		}
	} else if reply.Error != nil || len(reply.Result) == 0 {
		t.Fatalf("%s HTTP%d: %s", method, out.Code, out.Body.String())
	}
	return reply
}

func TestJointSourceInstalledAuthenticationAndFixedAuthorization(t *testing.T) {
	data := jointSourceFixture(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, kind := range []string{"user", "agent"} {
			t.Run(engine+"/"+kind, func(t *testing.T) {
				ctx := context.Background()
				store, lease, _ := openIdentityTarget(t, engine)
				owner := model.PrincipalRef{Kind: "user", ID: "user:restore-admin"}
				if kind == "agent" {
					owner = model.PrincipalRef{Kind: "agent", ID: "agent:source-worker", Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:source-sponsor"}}
				}
				p := jointOwnedProfile(t, data, jointProfile(t, data, store, lease), owner)
				report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, p)
				if err != nil {
					t.Fatal(err)
				}
				ns := authz.Namespace{StoreID: string(report.Restored.StoreID), Incarnation: report.Restored.Incarnation, InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))}
				project := model.RefByID("source-project", model.Natural64(0))
				base := authzfixed.Facts{Namespace: ns, Projects: []authzfixed.Project{{Ref: project, Name: "demo", InstallationID: "fixed", Active: true}}, Grants: []authz.Grant{{Subject: authz.SubjectRef{Kind: "user", ID: "user:source-sponsor"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: project}, Role: "involved"}}}
				facts, err := authzrpc.NewMutableFacts(base)
				if err != nil {
					t.Fatal(err)
				}
				keys := authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
				installed, err := authzrpc.New(ctx, authzrpc.Config{Store: store, Lease: lease, Provider: keys, Facts: facts, Namespace: ns, DefaultProject: "demo"})
				if err != nil {
					t.Fatal(err)
				}
				cap, err := driver.RequireWorkflowCheckpointStore(store, lease)
				if err != nil {
					t.Fatal(err)
				}
				binding := report.SourcePlanes.Providers[0]
				state, err := pluginhost.NewSourceRuntimeState(ctx, cap, lease, binding)
				if err != nil {
					t.Fatal(err)
				}
				provider, err := workflowplugin.NewSourceRuntime(state, binding)
				if err != nil {
					t.Fatal(err)
				}
				// The real authenticated HTTP handler is already bound. Installation must
				// update its existing delegate and keep the fixed policy unchanged.
				if report.Receipt == nil {
					t.Fatal("source import has no original receipt")
				}
				if err := installed.Service.InstallSourceWorkflow(ctx, provider, p, *report.SourcePlanes, *report.Receipt); err != nil {
					t.Fatal(err)
				}
				ref := model.RefByID("demo-7", model.Natural64(0))
				current, err := cap.LoadCheckpoint(ctx, binding.Provider.Namespace)
				if err != nil {
					t.Fatal(err)
				}
				if kind == "user" {
					got := sourceHTTPCall(t, installed.Handler, keys, owner, "bead.get", 0, map[string]any{"id": ref}, "")
					if !bytes.Contains(got.Result, []byte("Exact active collision")) {
						t.Fatal("installed user read missed source", string(got.Result))
					}
					assertSourceCommentAttribution(t, got.Result, p, report)
					// Fixed Read/Bead permits users; the provider separately rejects
					// foreign ownership, and the guarded service preserves its refusal.
					sourceHTTPCall(t, installed.Handler, keys, model.PrincipalRef{Kind: "user", ID: "user:foreign-owner"}, "bead.get", 0, map[string]any{"id": ref}, "forbidden")
					sourceHTTPCall(t, installed.Handler, keys, owner, "bead.update", 601, map[string]any{"id": ref, "fields": map[string]any{"title": "forbidden user write"}}, "forbidden")
				} else {
					sourceHTTPCall(t, installed.Handler, keys, owner, "bead.get", 0, map[string]any{"id": ref}, "not_found")
					updated := sourceHTTPCall(t, installed.Handler, keys, owner, "bead.update", 602, map[string]any{"id": ref, "fields": map[string]any{"title": "fixed-authorized current title"}}, "")
					replay := sourceHTTPCall(t, installed.Handler, keys, owner, "bead.update", 602, map[string]any{"id": ref, "fields": map[string]any{"title": "fixed-authorized current title"}}, "")
					if !replay.Meta.Replayed || !bytes.Equal(updated.Result, replay.Result) {
						t.Fatal("installed update replay changed")
					}
					claim := sourceHTTPCall(t, installed.Handler, keys, owner, "bead.claim", 603, map[string]any{"id": ref}, "")
					claimReplay := sourceHTTPCall(t, installed.Handler, keys, owner, "bead.claim", 603, map[string]any{"id": ref}, "")
					if !claimReplay.Meta.Replayed || !bytes.Equal(claim.Result, claimReplay.Result) {
						t.Fatal("installed claim replay changed")
					}
					current, err = cap.LoadCheckpoint(ctx, binding.Provider.Namespace)
					if err != nil {
						t.Fatal(err)
					}
					for _, method := range []string{"bead.comment", "bead.close", "tx.apply"} {
						params := map[string]any{"id": ref, "body": "forbidden comment"}
						if method == "bead.close" {
							params = map[string]any{"id": ref}
						}
						if method == "tx.apply" {
							params = map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: map[string]any{"title": "forbidden transaction"}}}}}}
						}
						sourceHTTPCall(t, installed.Handler, keys, owner, method, 604, params, "forbidden")
					}
					base.Grants = nil
					if err := facts.Replace(base); err != nil {
						t.Fatal(err)
					}
					sourceHTTPCall(t, installed.Handler, keys, owner, "bead.update", 602, map[string]any{"id": ref, "fields": map[string]any{"title": "fixed-authorized current title"}}, "forbidden")
					sourceHTTPCall(t, installed.Handler, keys, owner, "bead.claim", 603, map[string]any{"id": ref}, "forbidden")
				}
				after, err := cap.LoadCheckpoint(ctx, binding.Provider.Namespace)
				if err != nil || !reflect.DeepEqual(current, after) {
					t.Fatal("denied installed requests changed namespace", err)
				}
			})
		}
	}
}
