package viewrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type savedViews struct{ values map[string]viewops.Definition }

func (v savedViews) GetView(_ context.Context, _ model.PrincipalRef, id string) (viewops.Definition, error) {
	definition, ok := v.values[id]
	if !ok {
		return viewops.Definition{}, &model.Error{Data: model.ErrorData{Code: "not_found", Hint: "saved view is absent"}}
	}
	return definition, nil
}

type projectionStatusFunc func(context.Context, string) (coreevents.ProjectionStatus, error)

func (fn projectionStatusFunc) Status(ctx context.Context, binding string) (coreevents.ProjectionStatus, error) {
	return fn(ctx, binding)
}

type fixture struct {
	transport *inprocess.Transport
	server    *rpc.Server
	store     *memory.Store
	lease     driver.ActiveLease
	service   *service.Service
	core      *viewops.Service
	views     *savedViews
	deny      map[string]bool
	codec     *CursorCodec
	sequence  int
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	store, err := memory.NewWithOptions(memory.Options{Clock: func() time.Time { return time.Unix(0, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(ctx, "viewrpc-test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(ctx) })
	workflow := service.NewOntology(nil)
	writer := pipeline.New(store, lease, pipeline.WithOntology(workflow))
	domain := service.New(store, lease, writer, service.WithOntology(workflow))
	deny := map[string]bool{}
	views := &savedViews{values: map[string]viewops.Definition{}}
	core, err := viewops.New(store, lease, viewops.Config{
		Views: views,
		AuthorizeRead: func(_ context.Context, _ model.PrincipalRef, resource model.ResourceRef, _ string) error {
			if deny[resource.Ref.ID] {
				return &model.Error{Data: model.ErrorData{Code: "forbidden", Hint: "read denied"}}
			}
			return nil
		},
		OntologyRevision:  model.Natural64(1),
		ProjectionBinding: "core",
		ProjectionStatus: projectionStatusFunc(func(_ context.Context, binding string) (coreevents.ProjectionStatus, error) {
			meta, err := store.Meta(context.Background())
			if err != nil {
				return coreevents.ProjectionStatus{}, err
			}
			return coreevents.ProjectionStatus{Binding: binding, Position: coreevents.Position{Incarnation: meta.Incarnation}}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("0123456789abcdef0123456789abcdef")
	codec, err := NewCursorCodec(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := BindWithConfig(registry, core, Config{CursorKey: key}); err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		t.Fatal(err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "p", OntologyRevision: model.Natural64(1), CursorKey: key})
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{transport: inprocess.New(server), server: server, store: store, lease: lease, service: domain, core: core, views: views, deny: deny, codec: codec}
}

func (f *fixture) call(t *testing.T, ctx context.Context, method string, params any) map[string]any {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "view-test", "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := f.transport.RoundTrip(ctx, payload)
	if err != nil {
		t.Fatal(err)
	}
	var response map[string]any
	if err := json.Unmarshal(raw, &response); err != nil {
		t.Fatalf("%s: %s: %v", method, raw, err)
	}
	return response
}

func resultMap(t *testing.T, response map[string]any) map[string]any {
	t.Helper()
	if response["error"] != nil {
		t.Fatalf("unexpected error: %v", response)
	}
	return response["result"].(map[string]any)
}

func requireCode(t *testing.T, response map[string]any, want string) {
	t.Helper()
	errObject, ok := response["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected %s, got %v", want, response)
	}
	data, _ := errObject["data"].(map[string]any)
	if data["code"] != want {
		t.Fatalf("expected %s, got %v", want, response)
	}
}

func createBead(t *testing.T, f *fixture, title string) string {
	t.Helper()
	f.sequence++
	params := map[string]any{"type": "issue", "fields": map[string]any{"title": title}, "_meta": map[string]any{"idempotencyKey": fmt.Sprintf("e33d4848-10c7-40cb-a3d4-49e34b0d%04d", f.sequence)}}
	response := f.call(t, context.Background(), "bead.create", params)
	commit := resultMap(t, response)
	results := commit["results"].([]any)
	return results[0].(map[string]any)["ref"].(map[string]any)["id"].(string)
}

func TestByteRPCViewRunAuthorizationGroupingAndExplain(t *testing.T) {
	f := newFixture(t)
	first := createBead(t, f, "first")
	second := createBead(t, f, "second")
	third := createBead(t, f, "third")
	f.deny[second] = true
	f.views.values["issues"] = viewops.Definition{Filter: &viewops.Filter{Types: []string{"issue"}}}

	definition := map[string]any{"filter": map[string]any{"types": []string{"issue"}}, "fields": []string{"id"}, "groupBy": []string{"type"}, "budgetTokens": 1}
	run := resultMap(t, f.call(t, context.Background(), "view.run", map[string]any{"definition": definition, "_meta": map[string]any{"project": "p", "agent": "grouped"}}))
	if projection, ok := run["projection"].(map[string]any); !ok || projection["binding"] != "core" {
		t.Fatalf("view.run omitted projection metadata: %v", run)
	}
	if len(run["items"].([]any)) != 0 {
		t.Fatal("grouped result leaked top-level items")
	}
	groups := run["groups"].([]any)
	if len(groups) != 1 {
		t.Fatalf("groups=%v", groups)
	}
	group := groups[0].(map[string]any)
	if group["count"] != float64(2) || len(group["items"].([]any)) != 1 || group["elided"] == nil {
		t.Fatalf("authorized grouping lost count/elision: %v", group)
	}
	if strings.Contains(string(mustJSON(t, run)), second) {
		t.Fatal("denied row appeared in result or elision")
	}
	next := run["nextCursor"].(string)
	if next == "" {
		t.Fatal("budget cut did not return a continuation cursor")
	}
	continuation := group["elided"].(map[string]any)["continueWith"].(map[string]any)
	if continuation["method"] != "view.run" || continuation["params"].(map[string]any)["cursor"] != next {
		t.Fatalf("bad continuation: %v", continuation)
	}
	continuationParams := continuation["params"].(map[string]any)
	continuationMeta, ok := continuationParams["_meta"].(map[string]any)
	if !ok || continuationMeta["agent"] != "grouped" {
		t.Fatalf("grouped continuation dropped agent metadata: %v", continuation)
	}
	lastPage := resultMap(t, f.call(t, context.Background(), "view.run", continuationParams))
	lastGroup := lastPage["groups"].([]any)[0].(map[string]any)
	if len(lastGroup["items"].([]any)) != 1 || lastGroup["elided"] != nil || lastPage["nextCursor"] != nil {
		t.Fatalf("continuation did not finish the group: %v", lastPage)
	}

	typeSummary := resultMap(t, f.call(t, context.Background(), "view.run", map[string]any{"definition": map[string]any{"filter": map[string]any{}, "fields": []string{}, "groupBy": []string{"type"}}, "_meta": map[string]any{"project": "p"}}))
	if len(typeSummary["groups"].([]any)) != 1 || typeSummary["nextCursor"] != nil || len(typeSummary["groups"].([]any)[0].(map[string]any)["items"].([]any)) != 0 {
		t.Fatalf("type summary bounds/shape: %v", typeSummary)
	}

	explain := resultMap(t, f.call(t, context.Background(), "view.explain", map[string]any{"viewId": "issues", "itemId": first, "_meta": map[string]any{"project": "p"}}))
	if explain["included"] != true || explain["viewId"] != "issues" {
		t.Fatalf("included explain: %v", explain)
	}
	denied := f.call(t, context.Background(), "view.explain", map[string]any{"viewId": "issues", "itemId": second, "_meta": map[string]any{"project": "p"}})
	requireCode(t, denied, "not_found")
	_ = third
}

func TestByteRPCViewRunRefusesOverCapWithRealMemorySnapshot(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	for start := 0; start <= viewops.OrdinaryCandidateCap; start += 1000 {
		end := start + 1000
		if end > viewops.OrdinaryCandidateCap+1 {
			end = viewops.OrdinaryCandidateCap + 1
		}
		changes := make([]conformance.FixtureChange, 0, end-start)
		for i := start; i < end; i++ {
			id := fmt.Sprintf("cap-%05d", i)
			body, err := json.Marshal(pipeline.Document{ID: id, Type: "issue", Project: "p", Fields: map[string]any{"status": "open"}})
			if err != nil {
				t.Fatal(err)
			}
			changes = append(changes, conformance.FixtureChange{
				Ref:  model.RefByID(id, model.Natural64(0)),
				Kind: "bead",
				Op:   model.OpCreate,
				Body: body,
			})
		}
		if _, err := conformance.Seed(ctx, f.store, f.lease, driver.ChangesetHeader{}, changes); err != nil {
			t.Fatal(err)
		}
	}
	response := f.call(t, ctx, "view.run", map[string]any{
		"definition": map[string]any{"filter": map[string]any{}, "fields": []string{"id"}},
		"_meta":      map[string]any{"project": ""},
	})
	requireCode(t, response, "scope_too_large")
}

func TestViewCursorIsSignedSnapshotAndCallerBound(t *testing.T) {
	f := newFixture(t)
	createBead(t, f, "one")
	createBead(t, f, "two")
	definition := map[string]any{"filter": map[string]any{}, "fields": []string{"id"}, "budgetTokens": 1}
	first := resultMap(t, f.call(t, context.Background(), "view.run", map[string]any{"definition": definition, "_meta": map[string]any{"project": "p"}}))
	token := first["nextCursor"].(string)
	if _, err := f.codec.Decode(token); err != nil {
		t.Fatal(err)
	}
	last := "A"
	if token[len(token)-1] == 'A' {
		last = "B"
	}
	tampered := token[:len(token)-1] + last
	requireCode(t, f.call(t, context.Background(), "view.run", map[string]any{"definition": definition, "cursor": tampered, "_meta": map[string]any{"project": "p"}}), "cursor_invalid")
	caller := rpc.WithPrincipal(context.Background(), model.PrincipalRef{Kind: "user", ID: "different"})
	requireCode(t, f.call(t, caller, "view.run", map[string]any{"definition": definition, "cursor": token, "_meta": map[string]any{"project": "p"}}), "cursor_invalid")
	decoded, err := f.codec.Decode(token)
	if err != nil {
		t.Fatal(err)
	}
	decoded.Incarnation = "stale-incarnation"
	stale, err := f.codec.Encode(decoded)
	if err != nil {
		t.Fatal(err)
	}
	requireCode(t, f.call(t, context.Background(), "view.run", map[string]any{"definition": definition, "cursor": stale, "_meta": map[string]any{"project": "p"}}), "incarnation_changed")
}

func TestUnsupportedViewShapesRefuseAndSavedWritesStayUnbound(t *testing.T) {
	f := newFixture(t)
	for _, tc := range []struct {
		name   string
		params map[string]any
		code   string
	}{
		{name: "unknown field", params: map[string]any{"definition": map[string]any{"filter": map[string]any{}, "fields": []string{"fields.not-supported"}}}, code: "unknown_field"},
		{name: "event filter", params: map[string]any{"definition": map[string]any{"filter": map[string]any{"events": []string{"bead.created"}}}}, code: "invalid_params"},
		{name: "unknown query field", params: map[string]any{"definition": map[string]any{"filter": map[string]any{"query": "title=bug"}}}, code: "unknown_field"},
		{name: "query missing conjunction", params: map[string]any{"definition": map[string]any{"filter": map[string]any{"query": "id=a type=issue"}}}, code: "invalid_params"},
		{name: "query leading conjunction", params: map[string]any{"definition": map[string]any{"filter": map[string]any{"query": "AND id=a"}}}, code: "invalid_params"},
		{name: "query trailing conjunction", params: map[string]any{"definition": map[string]any{"filter": map[string]any{"query": "id=a AND"}}}, code: "invalid_params"},
		{name: "query repeated conjunction", params: map[string]any{"definition": map[string]any{"filter": map[string]any{"query": "id=a AND AND type=issue"}}}, code: "invalid_params"},
		{name: "group without explicit fields", params: map[string]any{"definition": map[string]any{"filter": map[string]any{}, "groupBy": []string{"type"}}}, code: "invalid_params"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireCode(t, f.call(t, context.Background(), "view.run", tc.params), tc.code)
		})
	}
	requireCode(t, f.call(t, context.Background(), "view.save", map[string]any{"definition": map[string]any{"filter": map[string]any{}}}), "method_not_found")
	requireCode(t, f.call(t, context.Background(), "view.delete", map[string]any{"viewId": "missing"}), "method_not_found")
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
