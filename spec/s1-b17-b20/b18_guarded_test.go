package s1b17b20

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
)

// This is the installed fixed-permission adapter and real domain delegate.
// Product writes stay on the existing paired-store Writer; these witnesses
// exercise reads through the guarded service and its actual RPC binding.
func guardedInstantRuntime(t *testing.T, p *pairedStore) *runtime {
	t.Helper()
	meta, err := p.store.Meta(p.ctx)
	if err != nil {
		t.Fatal(err)
	}
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))}
	project := model.RefByID("cardb20", model.Natural64(0))
	app, err := authzrpc.New(p.ctx, authzrpc.Config{
		Store: p.store, Lease: p.lease, Namespace: ns, DefaultProject: "cardb20",
		Provider: authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }}),
		Facts:    authzrpc.StaticFacts{Facts: authzfixed.Facts{Namespace: ns, Projects: []authzfixed.Project{{Ref: project, Name: "cardb20", InstallationID: "fixed", Active: true}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, app.Service); err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{DefaultProject: "cardb20", Incarnation: meta.Incarnation, OntologyRevision: model.Natural64(1), CursorKey: []byte(b20CursorKey)})
	if err != nil {
		t.Fatal(err)
	}
	return &runtime{ctx: p.ctx, store: p.store, lease: p.lease, server: server, principal: model.PrincipalRef{Kind: "user", ID: "user:instant", Caveats: map[string]any{"projects": []model.Ref{project}}}}
}

func TestB18GuardedGetResolvesInstantAndKeepsCurrentAuthority(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			p := newPairedStore(t, engine)
			writes := p.rpc(t)
			at := p.now
			created := success[model.Commit](t, "create", writes.call(t, "", "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "historical"}}, true))
			ref := created.Results[0].Ref
			p.advance(t)
			updated := success[model.Commit](t, "update", writes.call(t, "", "bead.update", map[string]any{"id": ref, "fields": map[string]any{"title": "current"}}, true))
			check := func(t *testing.T) {
				reads := guardedInstantRuntime(t, p)
				for _, tc := range []struct {
					selector any
					title    string
					revision model.Revision
				}{
					{instantAt(at), "historical", created.Revision},
					{map[string]any{"revision": json.Number(created.Revision.String())}, "historical", created.Revision},
					{nil, "current", updated.Revision},
				} {
					params := map[string]any{"id": ref}
					if tc.selector != nil {
						params["asOf"] = tc.selector
					}
					result := success[struct {
						Bead service.Bead `json:"bead"`
					}](t, "guarded get", reads.call(t, "", "bead.get", params, false))
					if result.Bead.Fields["title"] != tc.title || result.Bead.Revision.String() != tc.revision.String() || result.Bead.Revision.IsZero() {
						t.Fatalf("guarded selector lost historical revision: %+v", result)
					}
				}
				// The historical bytes do not confer old permission. A current
				// caller caveat that excludes this project must still hide them.
				reads.principal.Caveats = map[string]any{"projects": []model.Ref{model.RefByID("other", model.Natural64(0))}}
				denied := reads.call(t, "", "bead.get", map[string]any{"id": ref, "asOf": instantAt(at)}, false)
				if denied.Error == nil || denied.Error.Data.Code != "not_found" || len(denied.Result) != 0 {
					t.Fatalf("instant bypassed current authority: %+v", denied)
				}
			}
			check(t)
			p.reopen(t)
			t.Run("after-persistent-reopen", check)
		})
	}
}

func TestB18GuardedHistoryInstantContinuationKeepsResolvedSnapshot(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			p := newPairedStore(t, engine)
			nextID := 0
			writer := pipeline.New(p.store, p.lease, pipeline.WithOntology(service.NewOntology(nil)), pipeline.WithClock(p.clock.Now), pipeline.WithIDGenerator(func() (string, error) { nextID++; return fmt.Sprintf("pl-guarded-instant-%d", nextID), nil }))
			writes := p.rpcWithWriter(t, writer)
			ops := []map[string]any{}
			for i := 0; i < 3; i++ {
				ops = append(ops, map[string]any{"kind": "create", "key": fmt.Sprintf("item-%d", i), "type": "issue", "fields": map[string]any{"title": fmt.Sprintf("original-%d", i)}})
			}
			created := success[model.Commit](t, "tx.apply", writes.call(t, "", "tx.apply", map[string]any{"changeSet": map[string]any{"ops": ops}}, true))
			reads := guardedInstantRuntime(t, p)
			selector := instantAt(p.now.Add(time.Hour))
			first := success[map[string]any](t, "guarded history first", reads.call(t, "", "history.query", map[string]any{"asOf": selector, "limit": 1}, false))
			token, ok := first["nextCursor"].(string)
			if !ok || token == "" {
				t.Fatalf("guarded first page has no cursor: %+v", first)
			}
			p.advance(t)
			updated := success[model.Commit](t, "update", writes.call(t, "", "bead.update", map[string]any{"id": created.Results[1].Ref, "fields": map[string]any{"title": "later"}}, true))
			page := success[map[string]any](t, "guarded history continuation", reads.call(t, "", "history.query", map[string]any{"asOf": selector, "limit": 2, "cursor": token}, false))
			if natural(t, page["asOf"].(map[string]any)["revision"]).String() != created.Revision.String() || len(page["items"].([]any)) != 2 {
				t.Fatalf("guarded continuation re-resolved instant: %+v", page)
			}
			firstIncarnation, ok := first["asOf"].(map[string]any)["incarnation"].(string)
			if !ok || firstIncarnation == "" || page["asOf"].(map[string]any)["incarnation"] != firstIncarnation {
				t.Fatalf("guarded continuation changed resolved incarnation: first=%+v next=%+v", first, page)
			}
			for _, item := range page["items"].([]any) {
				if item.(map[string]any)["fields"].(map[string]any)["title"] == "later" {
					t.Fatal("concurrent append contaminated guarded page")
				}
			}
			fresh := success[map[string]any](t, "guarded history fresh", reads.call(t, "", "history.query", map[string]any{"asOf": selector, "limit": 3}, false))
			if natural(t, fresh["asOf"].(map[string]any)["revision"]).String() != updated.Revision.String() {
				t.Fatal("fresh request failed to see committed append")
			}
			changed := reads.call(t, "", "history.query", map[string]any{"asOf": instantAt(p.now.Add(2 * time.Hour)), "cursor": token, "limit": 2}, false)
			if changed.Error == nil || changed.Error.Data.Code != "cursor_invalid" || len(changed.Result) != 0 {
				t.Fatalf("signed cursor accepted a changed selector: %+v", changed)
			}
			reads.principal.Caveats = map[string]any{"projects": []model.Ref{model.RefByID("other", model.Natural64(0))}}
			denied := success[map[string]any](t, "guarded history current authority", reads.call(t, "", "history.query", map[string]any{"asOf": selector}, false))
			if len(denied["items"].([]any)) != 0 {
				t.Fatal("historical selector bypassed current permission")
			}
		})
	}
}
