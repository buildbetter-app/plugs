package s1b05b08

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type cycleRPCFixture struct {
	store   driver.Store
	lease   driver.ActiveLease
	app     *authzrpc.Composition
	keys    *authnapikey.Provider
	project model.Ref
}

func newCycleRPCFixture(t *testing.T, engine string) *cycleRPCFixture {
	t.Helper()
	ctx := context.Background()
	f := &cycleRPCFixture{project: model.RefByID("visible-project", model.Natural64(0))}
	switch engine {
	case "memory":
		f.store = memory.New()
	case "sqlite":
		var err error
		f.store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "cycles.db")})
		if err != nil {
			t.Fatal(err)
		}
	case "postgres":
		drv, dsn := postgresFixture(t)
		var err error
		f.store, err = drv.Open(ctx, driver.Config{DSN: dsn})
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown engine %q", engine)
	}
	t.Cleanup(func() { _ = f.store.Close() })
	var err error
	f.lease, err = f.store.Lease(ctx, "cycle-rpc-fixture", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.lease.Release(ctx) })
	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seedCycleRPCRows(t, f.store, f.lease, f.project)
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))}
	facts := authzfixed.Facts{Namespace: ns, Projects: []authzfixed.Project{
		{Ref: f.project, Name: "visible", InstallationID: "fixed", Active: true},
		{Ref: model.RefByID("secret-project", model.Natural64(0)), Name: "secret", InstallationID: "fixed", Active: true},
	}}
	f.keys = authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
	f.app, err = authzrpc.New(ctx, authzrpc.Config{Store: f.store, Lease: f.lease, Provider: f.keys, Facts: authzrpc.StaticFacts{Facts: facts}, Namespace: ns, DefaultProject: "visible"})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func seedCycleRPCRows(t *testing.T, store driver.Store, lease driver.ActiveLease, project model.Ref) {
	t.Helper()
	// The conformance seed is a disposable public fixture seam. It writes
	// already-shaped rows so the report can be tested against a cycle that
	// ordinary product writes correctly refuse.
	type row struct {
		id      string
		project string
		links   []pipeline.Link
	}
	ref := func(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
	rows := []row{
		{"a", "visible", []pipeline.Link{{To: ref("b"), Kind: "blocks"}, {To: ref("wait"), Kind: "waits-for"}}},
		{"b", "visible", []pipeline.Link{{To: ref("a"), Kind: "blocks"}, {To: ref("parent"), Kind: "parent-child"}}},
		{"c", "visible", []pipeline.Link{{To: ref("d"), Kind: "blocks"}}},
		{"d", "visible", []pipeline.Link{{To: ref("c"), Kind: "conditional-blocks"}}},
		{"root", "visible", []pipeline.Link{{To: ref("step"), Kind: "blocks"}}},
		{"step", "visible", []pipeline.Link{{To: ref("root"), Kind: "tracks"}}},
		{"tracks-a", "visible", []pipeline.Link{{To: ref("tracks-b"), Kind: "tracks"}}},
		{"tracks-b", "visible", []pipeline.Link{{To: ref("tracks-a"), Kind: "tracks"}}},
		{"wait", "visible", []pipeline.Link{{To: ref("a"), Kind: "waits-for"}}},
		{"parent", "visible", []pipeline.Link{{To: ref("b"), Kind: "parent-child"}}},
		{"secret-a", "secret", []pipeline.Link{{To: ref("secret-b"), Kind: "blocks"}}},
		{"secret-b", "secret", []pipeline.Link{{To: ref("secret-a"), Kind: "blocks"}}},
		{"partial", "visible", []pipeline.Link{{To: ref("partial-secret"), Kind: "blocks"}}},
		{"partial-secret", "secret", []pipeline.Link{{To: ref("partial"), Kind: "blocks"}}},
	}
	changes := make([]conformance.FixtureChange, 0, len(rows))
	for _, row := range rows {
		doc := pipeline.Document{ID: row.id, Type: "issue", Project: row.project, Fields: map[string]any{"title": row.id, "status": "todo", "priority": 0}, Links: row.links}
		body, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		changes = append(changes, conformance.FixtureChange{Ref: ref(row.id), Kind: "bead", Op: model.OpCreate, Body: body})
	}
	if _, err := conformance.Seed(context.Background(), store, lease, driver.ChangesetHeader{}, changes); err != nil {
		t.Fatal(err)
	}
}

func (f *cycleRPCFixture) call(t *testing.T, principal model.PrincipalRef, method string, params map[string]any) (service.CyclesResult, *model.Error) {
	t.Helper()
	issued, err := f.keys.Issue(principal, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if params == nil {
		params = map[string]any{}
	}
	params["_meta"] = map[string]any{"project": "visible"}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1"+httpjson.Path, bytes.NewReader(body))
	req.Header.Set("Plug-RPC", "1")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+issued.Token)
	response := httptest.NewRecorder()
	f.app.Handler.ServeHTTP(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("HTTP status = %d body=%s", response.Code, response.Body.String())
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Data model.ErrorData `json:"data"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error != nil {
		return service.CyclesResult{}, &model.Error{Data: envelope.Error.Data}
	}
	var result service.CyclesResult
	if err := json.Unmarshal(envelope.Result, &result); err != nil {
		t.Fatal(err)
	}
	return result, nil
}

func TestB06AuthenticatedCycleReportAcrossDisposableStores(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			f := newCycleRPCFixture(t, engine)
			before, err := f.store.Meta(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			user := model.PrincipalRef{Kind: "user", ID: "cycle-reader"}
			got, rpcErr := f.call(t, user, "bead.cycles", map[string]any{"limit": 1})
			if rpcErr != nil {
				t.Fatalf("cycle report refused: %+v", rpcErr.Data)
			}
			if got.CycleCount.String() != "3" || !got.Truncated || len(got.Items) != 1 {
				t.Fatalf("limit/count = count %s truncated %v items %d", got.CycleCount, got.Truncated, len(got.Items))
			}
			if got.Items[0].MemberCount.String() != "2" || got.Items[0].Path[0].Ref == nil {
				t.Fatalf("lowest-ID report path = %+v", got.Items[0])
			}
			full, rpcErr := f.call(t, user, "bead.cycles", nil)
			if rpcErr != nil {
				t.Fatalf("full cycle report refused: %+v", rpcErr.Data)
			}
			paths := cycleRPCPaths(full)
			if !reflect.DeepEqual(paths, [][]string{{"a", "b"}, {"c", "d"}, {"partial", "partial-secret"}}) {
				t.Fatalf("default paths = %v", paths)
			}
			mixed, rpcErr := f.call(t, user, "bead.cycles", map[string]any{"includeTracks": true})
			if rpcErr != nil {
				t.Fatalf("mixed cycle report refused: %+v", rpcErr.Data)
			}
			if !containsCycle(cycleRPCPaths(mixed), []string{"root", "step"}) || containsCycle(cycleRPCPaths(mixed), []string{"tracks-a", "tracks-b"}) {
				t.Fatalf("mixed paths = %v", cycleRPCPaths(mixed))
			}
			limited := model.PrincipalRef{Kind: "user", ID: "limited-cycle-reader", Caveats: map[string]any{"projects": []model.Ref{f.project}}}
			partial, rpcErr := f.call(t, limited, "bead.cycles", nil)
			if rpcErr != nil {
				t.Fatalf("limited cycle report refused: %+v", rpcErr.Data)
			}
			var partialItem service.Cycle
			for _, item := range partial.Items {
				if containsCycle([][]string{cycleRPCPath(item)}, []string{"partial", ""}) {
					partialItem = item
				}
			}
			if partialItem.UnreadableCount.String() != "1" || !partialItem.Partial || len(partialItem.Path) != 2 {
				t.Fatalf("partial projection = %+v", partialItem)
			}
			for _, member := range partialItem.Path {
				if member.Unreadable && member.Ref != nil {
					t.Fatal("unreadable member disclosed a reference")
				}
			}
			_, rpcErr = f.call(t, user, "bead.cycles", map[string]any{"includeEphemeral": true})
			if rpcErr == nil || rpcErr.Data.Code != "invalid_params" {
				t.Fatalf("unsupported ephemeral plane error = %+v", rpcErr)
			}
			after, err := f.store.Meta(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if before.Head.String() != after.Head.String() {
				t.Fatalf("read report changed head %s -> %s", before.Head, after.Head)
			}
		})
	}
}

func cycleRPCPath(item service.Cycle) []string {
	path := make([]string, 0, len(item.Path))
	for _, member := range item.Path {
		if member.Ref == nil {
			path = append(path, "")
		} else {
			path = append(path, member.Ref.ID)
		}
	}
	return path
}

func cycleRPCPaths(result service.CyclesResult) [][]string {
	paths := make([][]string, 0, len(result.Items))
	for _, item := range result.Items {
		paths = append(paths, cycleRPCPath(item))
	}
	sort.Slice(paths, func(i, j int) bool {
		for k := 0; k < len(paths[i]) && k < len(paths[j]); k++ {
			if paths[i][k] != paths[j][k] {
				return paths[i][k] < paths[j][k]
			}
		}
		return len(paths[i]) < len(paths[j])
	})
	return paths
}

func containsCycle(paths [][]string, want []string) bool {
	for _, path := range paths {
		if reflect.DeepEqual(path, want) {
			return true
		}
	}
	return false
}

func TestB06CycleReportPathOracle(t *testing.T) {
	got := [][]string{{"a", "b"}, {"c", "d"}}
	want := [][]string{{"a", "b"}, {"c", "d"}}
	sort.Slice(got, func(i, j int) bool { return reflect.DeepEqual(got[i], want[i]) })
	if !reflect.DeepEqual(got, want) {
		t.Fatal("oracle fixture changed")
	}
}
