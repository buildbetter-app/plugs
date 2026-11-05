package b02publicnotices_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
)

// The installed finite CP7 component has no event.list permit or EventReader
// adapter. A real in-flight HTTP request must not borrow the separate anonymous
// fixture's provider, even when current authority changes before admission.
// This proves refusal, not an authorized fixed-policy event-list capability.
func TestB02FixedPolicyEventListRefusesConcurrentAuthorityChange(t *testing.T) {
	eachEngine(t, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		c := categories[0] // completed success produces the two real notices
		f.install(t, blockerProject, projectCatalog(false, "frozen"))
		f.install(t, dependentProject, projectCatalog(true, "frozen"))
		g := f.graph(t)
		before := f.retained(t, g.all())
		baseline := f.setupBaseline(t)
		request := f.writeParams(blockerProject, map[string]any{
			"id":     g.blocker,
			"expect": []model.Precondition{model.PreconditionByRevision(g.blocker, before[g.blocker.ID].Record.Revision)},
		})
		first := f.call(t, c.method, request)
		var commit model.Commit
		decodeResult(t, first, &commit)
		set := f.changeset(t, commit.Revision)
		assertPlan(t, f, c, g, before, set, request)
		assertEvents(t, f.events(t, baseline.Cursor, 100).Events, set, c)
		f.readyAfter(t, c, g)
		retained := f.retained(t, g.all())
		receipt := f.receipt(t, c.method, request)
		if receipt == nil || receipt.ResponsePending {
			t.Fatal("notice-producing public write has no complete original receipt")
		}

		meta := f.meta(t)
		namespace := authz.Namespace{
			StoreID: string(meta.StoreID), Incarnation: meta.Incarnation,
			InstallationID: "b02-fixed", Authority: model.RefByID("b02-fixed-authority", model.Natural64(0)),
		}
		user := model.PrincipalRef{Kind: "user", ID: "user:b02-viewer"}
		base := authzfixed.Facts{
			Namespace: namespace,
			Projects: []authzfixed.Project{
				{Ref: model.RefByID("b02-fixed-blocker-project", model.Natural64(0)), Name: blockerProject, InstallationID: namespace.InstallationID, Active: true},
				{Ref: model.RefByID("b02-fixed-dependent-project", model.Natural64(0)), Name: dependentProject, InstallationID: namespace.InstallationID, Active: true},
			},
			Grants: []authz.Grant{{
				Subject: authz.SubjectRef{Kind: "user", ID: user.ID},
				Scope:   authz.Scope{Kind: authz.ProjectScope, Ref: model.RefByID("b02-fixed-dependent-project", model.Natural64(0))},
				Role:    "involved",
			}},
		}
		facts, err := authzrpc.NewMutableFacts(base)
		if err != nil {
			t.Fatal(err)
		}
		keys := authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
		installed, err := authzrpc.New(f.ctx, authzrpc.Config{
			Store: f.store, Lease: f.lease, Provider: keys, Facts: facts,
			Namespace: namespace, DefaultProject: dependentProject,
		})
		if err != nil {
			t.Fatal(err)
		}
		issued, err := keys.Issue(user, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		revoked := base
		revoked.Grants = nil
		for _, step := range []struct {
			name string
			next authzfixed.Facts
		}{{"revoke_grants", revoked}, {"restore_grants", base}} {
			t.Run(step.name, func(t *testing.T) {
				oldBinding := fixedNoticeAuthorityBinding(t, f, installed.Checker, user)
				ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
				defer cancel()
				entered, resume := make(chan struct{}), make(chan struct{})
				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(resume) }) }
				defer release()
				// Hold only event.list at HTTP ingress. Authentication still
				// uses the real API-key provider and its unchanged 50ms bound;
				// the test never blocks inside, or extends, that deadline.
				gate := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Header.Get("X-B02-Authority-Overlap") == "event.list" {
						close(entered)
						select {
						case <-resume:
						case <-r.Context().Done():
							return
						}
					}
					installed.Handler.ServeHTTP(w, r)
				})
				server := httptest.NewServer(gate)
				defer server.Close()
				// Release the handler before server.Close, including on a
				// failed assertion while the concurrent request is held.
				defer release()
				probe, err := fixedNoticeHTTP(ctx, server, issued.Token, "bead.get", map[string]any{"id": g.blocker}, false)
				if err != nil {
					t.Fatal(err)
				}
				var got struct{ Bead struct{ Status string } }
				decodeResult(t, probe, &got)
				if got.Bead.Status != c.status {
					t.Fatalf("fixed-policy control read did not reach the real stored blocker: %+v", got)
				}
				type response struct {
					value reply
					err   error
				}
				completed := make(chan response, 1)
				go func() {
					value, err := fixedNoticeHTTP(ctx, server, issued.Token, "event.list", map[string]any{
						"filter": map[string]any{"events": eventTypes}, "after": baseline.Cursor, "limit": 100,
					}, true)
					completed <- response{value: value, err: err}
				}()
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("event.list did not reach the actual HTTP ingress barrier")
				}
				authorityBefore := f.meta(t)
				changed := make(chan error, 1)
				go func() { changed <- facts.Replace(step.next) }()
				select {
				case err := <-changed:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("real current-authority replacement did not complete while event.list was in flight")
				}
				authorityAfter := f.meta(t)
				acl := f.changeset(t, authorityAfter.Head)
				if authorityAfter.Head.String() == authorityBefore.Head.String() || authorityAfter.Incarnation != authorityBefore.Incarnation || acl.Kind != model.ChangesetDerived || len(acl.Changes) != 0 || len(acl.Events) != 0 || acl.Principal.Kind != "system" || acl.Principal.ID != "authzrpc" {
					t.Fatalf("authority replacement did not commit its own isolated ACL generation: %+v", acl)
				}
				err = installed.Checker.ValidateReadAuthorityBinding(ctx, user, oldBinding)
				var stale *model.Error
				if !errors.As(err, &stale) || stale.Data.Code != "authz_unavailable" || !stale.Data.Retryable {
					t.Fatalf("stale pre-change authority binding was accepted: %v", err)
				}
				fresh := fixedNoticeAuthorityBinding(t, f, installed.Checker, user)
				if fresh == oldBinding {
					t.Fatal("real ACL replacement retained the old authority binding")
				}
				if err := installed.Checker.ValidateReadAuthorityBinding(ctx, user, fresh); err != nil {
					t.Fatalf("fresh current-authority binding failed: %v", err)
				}
				release()
				var result response
				select {
				case result = <-completed:
				case <-ctx.Done():
					t.Fatal("original event.list request did not finish after authority replacement")
				}
				if result.err != nil {
					t.Fatal(result.err)
				}
				if result.value.Error == nil || result.value.Error.Data.Code != "method_not_found" || len(result.value.Result) != 0 || result.value.Meta.Replayed {
					t.Fatalf("finite fixed policy exposed event.list data, cursor or replay: %+v", result.value)
				}
				if err := f.registry.Catalog().ValidateError(result.value.Error); err != nil {
					t.Fatalf("fixed-policy event refusal violates the canonical public schema: %v", err)
				}
				// CP7 User reads remain allowed independently of involvement;
				// grant revocation must not silently invent a narrower policy.
				probe, err = fixedNoticeHTTP(ctx, server, issued.Token, "bead.get", map[string]any{"id": g.blocker}, false)
				if err != nil {
					t.Fatal(err)
				}
				decodeResult(t, probe, &got)
				if got.Bead.Status != c.status {
					t.Fatal("authority change reinterpreted the stored blocker category")
				}
				equalJSON(t, f.retained(t, g.all()), retained)
				equalJSON(t, f.changeset(t, commit.Revision), set)
				equalJSON(t, f.receipt(t, c.method, request), receipt)
				assertEvents(t, f.events(t, baseline.Cursor, 100).Events, set, c)
				f.readyAfter(t, c, g)
				t.Logf("B02 fixed policy %s/%s: actual authenticated HTTP event.list overlapped persisted ACL replacement; unavailable method disclosed no event/cursor, stale authority refused, fresh authority accepted, original category/notices/receipt/history retained", e.name, step.name)
			})
		}
	})
}

func fixedNoticeAuthorityBinding(t *testing.T, f *fixture, checker *authzrpc.Checker, by model.PrincipalRef) string {
	t.Helper()
	snapshot := f.read(t)
	binding, err := checker.ReadAuthorityBinding(f.ctx, by, snapshot)
	closeErr := snapshot.Close()
	if err != nil || closeErr != nil || len(binding) != 64 {
		t.Fatalf("current fixed-policy authority binding unavailable: %v close=%v", err, closeErr)
	}
	return binding
}

// This is an actual HTTP client; it makes no authentication or policy decision.
// The forwarding ingress barrier belongs only to the concurrent event request.
func fixedNoticeHTTP(ctx context.Context, server *httptest.Server, token, method string, params map[string]any, overlap bool) (reply, error) {
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "b02-fixed", "method": method, "params": params})
	if err != nil {
		return reply{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/rpc/v1", bytes.NewReader(body))
	if err != nil {
		return reply{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Plug-RPC", "1")
	req.Header.Set("Authorization", "Bearer "+token)
	if overlap {
		req.Header.Set("X-B02-Authority-Overlap", "event.list")
	}
	response, err := server.Client().Do(req)
	if err != nil {
		return reply{}, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		return reply{}, err
	}
	if response.StatusCode != http.StatusOK {
		return reply{}, fmt.Errorf("fixed-policy HTTP status %d", response.StatusCode)
	}
	var result reply
	if err := json.Unmarshal(raw, &result); err != nil {
		return reply{}, err
	}
	return result, nil
}
