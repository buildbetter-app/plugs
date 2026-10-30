package rpc_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type failingPost struct{ calls int }

func (p *failingPost) After(context.Context, port.CommittedChange, port.SnapshotReader) error {
	p.calls++
	return errors.New("test subscriber unavailable")
}

func openSQLiteMemoryServer(t *testing.T, path string, options ...service.Option) (*rpc.Server, driver.Store, driver.ActiveLease) {
	return openSQLiteMemoryServerWithReadHook(t, path, nil, options...)
}

func openSQLiteMemoryServerWithReadHook(t *testing.T, path string, afterHistoryRead func(), options ...service.Option) (*rpc.Server, driver.Store, driver.ActiveLease) {
	t.Helper()
	ctx := context.Background()
	store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	if afterHistoryRead != nil {
		store = &historyRPCStore{Store: store, afterRead: afterHistoryRead}
	}
	lease, err := store.Lease(ctx, "rpc-memory", driver.LeaseTTL)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		_ = lease.Release(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	ontology := service.NewOntology(nil)
	writer := pipeline.New(store, lease, pipeline.WithOntology(ontology))
	policy := service.MemoryReadAuthorizerFunc(func(_ context.Context, _ model.PrincipalRef, memory service.Memory) (bool, error) {
		return memory.Key != "hidden", nil
	})
	options = append([]service.Option{
		service.WithOntology(ontology),
		service.WithMemoryReadAuthorizer(policy),
	}, options...)
	domain := service.New(store, lease, writer, options...)
	registry, err := rpc.NewRegistry()
	if err != nil {
		_ = lease.Release(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		_ = lease.Release(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	config := pagingConfig()
	config.Incarnation = meta.Incarnation
	server, err := rpc.New(registry, config)
	if err != nil {
		_ = lease.Release(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	return server, store, lease
}

type historyRPCStore struct {
	driver.Store
	afterRead func()
}

func (s *historyRPCStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil || view.At == nil {
		return r, err
	}
	return historyRPCReader{Reader: r, afterRead: s.afterRead}, nil
}

type historyRPCReader struct {
	driver.Reader
	afterRead func()
}

func (r historyRPCReader) Get(ctx context.Context, refs []driver.Ref) ([]driver.Record, error) {
	rows, err := r.Reader.Get(ctx, refs)
	if err == nil {
		r.afterRead()
	}
	return rows, err
}

func (r historyRPCReader) Versions(ctx context.Context, ref driver.Ref, req driver.PageReq) (driver.Page[driver.Version], error) {
	page, err := r.Reader.Versions(ctx, ref, req)
	if err == nil {
		r.afterRead()
	}
	return page, err
}

func memoryIDs(t *testing.T, value map[string]any) []string {
	t.Helper()
	items := value["items"].([]any)
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.(map[string]any)["key"].(string))
	}
	return ids
}

func assertMemoryWireRefusal(t *testing.T, response map[string]any, code, number string, retryable bool) {
	t.Helper()
	requireCode(t, response, code)
	if _, present := response["result"]; present {
		t.Fatalf("refused memory read returned a result: %v", response)
	}
	e := response["error"].(map[string]any)
	data := e["data"].(map[string]any)
	if e["code"] != json.Number(number) || data["retryable"] != retryable || data["details"] != nil {
		t.Fatalf("wrong typed refusal or disclosed error details: %v", response)
	}
	raw, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"private-memory-body", "changed-private-body", "old-secret", "changed-secret", "current-secret", "hidden-current-secret", "content:hidden"} {
		if bytes.Contains(raw, []byte(secret)) {
			t.Fatalf("refused memory read disclosed %q: %s", secret, raw)
		}
	}
}

func TestRealSQLiteMemoryBriefAuthorizationAndRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "brief.db")
	server, store, lease := openSQLiteMemoryServer(t, path)
	save := func(key string) map[string]any {
		t.Helper()
		return writeResult(t, server, "memory.save", writeParams(t, map[string]any{
			"key": key, "content": "content:" + key,
		}), false)
	}
	// Insert out of lexical order; the read contract sorts by key, then ID.
	var hiddenID, replaceID string
	var hiddenCommit map[string]any
	for _, key := range []string{"zeta", "hidden", "alpha", "middle", "replace"} {
		commit := save(key)
		if key == "hidden" {
			hiddenID = createdRef(t, commit)["id"].(string)
			hiddenCommit = commit
		}
		if key == "replace" {
			replaceID = createdRef(t, commit)["id"].(string)
		}
	}
	updated := writeResult(t, server, "memory.save", writeParams(t, map[string]any{
		"key": "replace", "content": "updated replace",
	}), false)
	if got := createdRef(t, updated)["id"].(string); got != replaceID {
		t.Fatalf("memory replacement minted a new record: got %s want %s", got, replaceID)
	}
	replaced := result(t, call(t, server, ctx, "memory.get", paramsJSON(t, map[string]any{"id": replaceID})))
	if replaced["content"] != "updated replace" {
		t.Fatalf("memory replacement did not update content: %v", replaced)
	}
	hiddenUpdate := writeResult(t, server, "memory.save", writeParams(t, map[string]any{
		"key": "hidden", "content": "hidden-current-secret",
	}), false)
	historyRef := map[string]any{"id": hiddenID}
	historyCalls := []struct {
		name   string
		method string
		params map[string]any
	}{
		{"get", "history.get", map[string]any{"ref": historyRef, "asOf": map[string]any{"revision": hiddenCommit["revision"]}}},
		{"diff", "history.diff", map[string]any{"ref": historyRef, "fromRevision": hiddenCommit["revision"], "toRevision": hiddenUpdate["revision"]}},
		{"equal-diff", "history.diff", map[string]any{"ref": historyRef, "fromRevision": hiddenCommit["revision"], "toRevision": hiddenCommit["revision"]}},
	}
	for _, historyCall := range historyCalls {
		response := call(t, server, ctx, historyCall.method, paramsJSON(t, historyCall.params))
		requireCode(t, response, "not_found")
		raw, err := json.Marshal(response)
		if err != nil || bytes.Contains(raw, []byte("hidden-current-secret")) || bytes.Contains(raw, []byte("content:hidden")) {
			t.Fatalf("%s disclosed hidden history: %s", historyCall.name, raw)
		}
		if _, ok := response["result"]; ok {
			t.Fatalf("%s returned a partial result: %v", historyCall.name, response)
		}
	}
	writeResult(t, server, "memory.delete", writeParams(t, map[string]any{"id": replaceID}), false)
	brief := result(t, call(t, server, ctx, "agent.brief", `{"budgetTokens":200}`))
	if got := memoryIDs(t, map[string]any{"items": brief["memories"]}); !reflect.DeepEqual(got, []string{"alpha", "middle", "zeta"}) {
		t.Fatalf("brief ordering or authorization: %v", got)
	}
	list := result(t, call(t, server, ctx, "memory.list", `{"limit":10}`))
	if got := memoryIDs(t, list); !reflect.DeepEqual(got, []string{"alpha", "middle", "zeta"}) {
		t.Fatalf("list ordering or authorization: %v", got)
	}
	// The hidden ID comes only from its write receipt; current authorization
	// still maps the point read to not_found.
	requireCode(t, call(t, server, ctx, "memory.get", paramsJSON(t, map[string]any{"id": hiddenID})), "not_found")
	query := result(t, call(t, server, ctx, "memory.list", `{"query":"hidden"}`))
	if items := query["items"].([]any); len(items) != 0 {
		t.Fatalf("hidden memory leaked through query: %v", items)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	server, store, lease = openSQLiteMemoryServer(t, path)
	t.Cleanup(func() {
		_ = lease.Release(context.Background())
		_ = store.Close()
	})
	list = result(t, call(t, server, ctx, "memory.list", `{"limit":10}`))
	if got := memoryIDs(t, list); !reflect.DeepEqual(got, []string{"alpha", "middle", "zeta"}) {
		t.Fatalf("reopened ordering or persistence: %v", got)
	}
	brief = result(t, call(t, server, ctx, "agent.brief", `{}`))
	if got := memoryIDs(t, map[string]any{"items": brief["memories"]}); !reflect.DeepEqual(got, []string{"alpha", "middle", "zeta"}) {
		t.Fatalf("reopened brief ordering: %v", got)
	}
}

func TestRealSQLiteMemoryPaginationReadsLiveRows(t *testing.T) {
	ctx := context.Background()
	s, store, lease := openSQLiteMemoryServer(t, filepath.Join(t.TempDir(), "paging.db"))
	t.Cleanup(func() {
		_ = lease.Release(ctx)
		_ = store.Close()
	})
	save := func(key, content string) map[string]any {
		t.Helper()
		return writeResult(t, s, "memory.save", writeParams(t, map[string]any{"key": key, "content": content}), false)
	}
	for _, key := range []string{"zeta", "hidden", "alpha", "middle"} {
		save(key, "original:"+key)
	}
	first := result(t, call(t, s, ctx, "memory.list", `{"limit":1}`))
	if got := memoryIDs(t, first); !reflect.DeepEqual(got, []string{"alpha"}) || first["nextCursor"] == nil {
		t.Fatal("first authorized page", first)
	}
	second := result(t, call(t, s, ctx, "memory.list", paramsJSON(t, map[string]any{"limit": 1, "cursor": first["nextCursor"]})))
	if got := memoryIDs(t, second); !reflect.DeepEqual(got, []string{"middle"}) || second["nextCursor"] == nil {
		t.Fatal("unchanged continuation", second)
	}
	// A continuation must use current rows, including later inserts and updates.
	updated := save("zeta", "current:zeta")
	save("omega", "later:omega")
	third := result(t, call(t, s, ctx, "memory.list", paramsJSON(t, map[string]any{"limit": 1, "cursor": second["nextCursor"]})))
	if got := memoryIDs(t, third); !reflect.DeepEqual(got, []string{"omega"}) || third["nextCursor"] == nil {
		t.Fatal("live insert missing from continuation", third)
	}
	last := result(t, call(t, s, ctx, "memory.list", paramsJSON(t, map[string]any{"limit": 1, "cursor": third["nextCursor"]})))
	if got := memoryIDs(t, last); !reflect.DeepEqual(got, []string{"zeta"}) || last["nextCursor"] != nil {
		t.Fatal("final live page", last)
	}
	row := last["items"].([]any)[0].(map[string]any)
	if row["content"] != "current:zeta" || row["revision"] != updated["revision"] {
		t.Fatal("continuation returned a stale row", row)
	}
	for _, changed := range []map[string]any{
		{"query": "hidden"},
		{"_meta": map[string]any{"project": "other"}},
		{"_meta": map[string]any{"agent": "other"}},
		{"cursor": first["nextCursor"].(string) + "tampered"},
	} {
		params := map[string]any{"limit": 1, "cursor": first["nextCursor"]}
		for key, value := range changed {
			params[key] = value
		}
		requireCode(t, call(t, s, ctx, "memory.list", paramsJSON(t, params)), "cursor_invalid")
	}
	for _, by := range []model.PrincipalRef{
		{Kind: "user", ID: "reader"},
		{Kind: "anonymous", ID: "anonymous", Sponsor: &model.PrincipalRef{Kind: "user", ID: "sponsor"}},
		{Kind: "anonymous", ID: "anonymous", Caveats: map[string]any{"scope": "other"}},
	} {
		params := paramsJSON(t, map[string]any{"limit": 1, "cursor": first["nextCursor"]})
		requireCode(t, call(t, s, rpc.WithPrincipal(ctx, by), "memory.list", params), "cursor_invalid")
	}
}

func TestRealSQLiteMemoryPaginationAllowsShrunkenTail(t *testing.T) {
	ctx := context.Background()
	s, store, lease := openSQLiteMemoryServer(t, filepath.Join(t.TempDir(), "shrink.db"))
	t.Cleanup(func() {
		_ = lease.Release(ctx)
		_ = store.Close()
	})
	var ids []string
	for _, key := range []string{"alpha", "middle", "zeta"} {
		commit := writeResult(t, s, "memory.save", writeParams(t, map[string]any{"key": key, "content": key}), false)
		ids = append(ids, createdRef(t, commit)["id"].(string))
	}
	first := result(t, call(t, s, ctx, "memory.list", `{"limit":2}`))
	if first["nextCursor"] == nil {
		t.Fatal("missing continuation", first)
	}
	for _, id := range ids {
		writeResult(t, s, "memory.delete", writeParams(t, map[string]any{"id": id}), false)
	}
	last := result(t, call(t, s, ctx, "memory.list", paramsJSON(t, map[string]any{"limit": 2, "cursor": first["nextCursor"]})))
	if len(last["items"].([]any)) != 0 || last["nextCursor"] != nil {
		t.Fatal("valid continuation did not terminate after list shrink", last)
	}
}

func TestRealSQLiteMemoryAuthorizationInstallationFailsClosed(t *testing.T) {
	for _, scope := range []struct {
		name string
		by   model.PrincipalRef
	}{
		{"agent", model.PrincipalRef{Kind: "agent", ID: "worker"}},
		{"sponsored", model.PrincipalRef{Kind: "user", ID: "reader", Sponsor: &model.PrincipalRef{Kind: "user", ID: "sponsor"}}},
		{"caveated", model.PrincipalRef{Kind: "user", ID: "reader", Caveats: map[string]any{"project": "local"}}},
	} {
		t.Run(scope.name, func(t *testing.T) {
			installed, changeDuringRead, policyCalls := true, false, 0
			policy := service.MemoryReadAuthorizerFunc(func(_ context.Context, _ model.PrincipalRef, memory service.Memory) (bool, error) {
				policyCalls++
				if changeDuringRead {
					installed = false
					return true, nil
				}
				return memory.Key != "hidden", nil
			})
			s, store, lease := openSQLiteMemoryServer(t, filepath.Join(t.TempDir(), "capability.db"),
				service.WithAuthorizationInstalled(func() bool { return installed }),
				service.WithMemoryReadAuthorizer(policy))
			t.Cleanup(func() {
				_ = lease.Release(context.Background())
				_ = store.Close()
			})
			commit := writeResult(t, s, "memory.save", writeParams(t, map[string]any{
				"key": "hidden", "content": "private-memory-body",
			}), false)
			hiddenID := createdRef(t, commit)["id"].(string)
			updated := writeResult(t, s, "memory.save", writeParams(t, map[string]any{"key": "hidden", "content": "changed-private-body"}), false)
			writeResult(t, s, "memory.save", writeParams(t, map[string]any{"key": "visible", "content": "public-memory-body"}), false)
			ctx := rpc.WithPrincipal(context.Background(), scope.by)
			for _, read := range []struct {
				name, method string
				params       map[string]any
				list         bool
			}{
				{"memory.get", "memory.get", map[string]any{"id": hiddenID}, false},
				{"memory.list", "memory.list", map[string]any{}, true},
				{"agent.brief", "agent.brief", map[string]any{}, true},
				{"history.get-base", "history.get", map[string]any{"ref": map[string]any{"id": hiddenID}, "asOf": map[string]any{"revision": commit["revision"]}}, false},
				{"history.get-patch", "history.get", map[string]any{"ref": map[string]any{"id": hiddenID}, "asOf": map[string]any{"revision": updated["revision"]}}, false},
				{"history.diff", "history.diff", map[string]any{"ref": map[string]any{"id": hiddenID}, "fromRevision": commit["revision"], "toRevision": updated["revision"]}, false},
				{"history.equal-diff", "history.diff", map[string]any{"ref": map[string]any{"id": hiddenID}, "fromRevision": updated["revision"], "toRevision": updated["revision"]}, false},
			} {
				t.Run(read.name, func(t *testing.T) {
					params := paramsJSON(t, read.params)
					installed, changeDuringRead = true, false
					response := call(t, s, ctx, read.method, params)
					if !read.list {
						assertMemoryWireRefusal(t, response, "not_found", "-32000", false)
					} else {
						value := result(t, response)
						if read.method == "agent.brief" {
							value = map[string]any{"items": value["memories"]}
						}
						if got := memoryIDs(t, value); !reflect.DeepEqual(got, []string{"visible"}) {
							t.Fatal("installed policy disclosed a hidden memory", value)
						}
					}
					// Keep the policy pointer, but disable its live installation.
					installed = false
					before := policyCalls
					response = call(t, s, ctx, read.method, params)
					assertMemoryWireRefusal(t, response, "authz_not_installed", "-32009", false)
					if policyCalls != before {
						t.Fatal("disabled capability used a stale policy", response)
					}
					// Installation changes inside an allowing policy discard every
					// output field, including historical bodies and patches.
					installed, changeDuringRead = true, true
					response = call(t, s, ctx, read.method, params)
					assertMemoryWireRefusal(t, response, "authz_unavailable", "-32005", true)
				})
			}
		})
	}
}

func TestRealSQLiteHistoryMemoryRevalidatesCurrentRead(t *testing.T) {
	for _, scenario := range []string{"disable", "install", "policy-denies", "current-value-denied"} {
		t.Run(scenario, func(t *testing.T) {
			installed, allowed, calls, scans := true, true, 0, 0
			var duringRead func()
			var expectedContent, expectedRevision string
			policy := service.MemoryReadAuthorizerFunc(func(_ context.Context, _ model.PrincipalRef, memory service.Memory) (bool, error) {
				calls++
				if memory.Content != expectedContent || memory.Revision.String() != expectedRevision {
					t.Fatalf("history authorized a stale value: %+v want content=%s revision=%s", memory, expectedContent, expectedRevision)
				}
				return allowed && memory.Metadata["denied"] != true, nil
			})
			s, store, lease := openSQLiteMemoryServerWithReadHook(t, filepath.Join(t.TempDir(), "history-revalidation.db"), func() {
				scans++
				if change := duringRead; change != nil {
					duringRead = nil
					change()
				}
			}, service.WithMemoryReadAuthorizer(policy), service.WithAuthorizationInstalled(func() bool { return installed }))
			t.Cleanup(func() { _ = lease.Release(context.Background()); _ = store.Close() })
			save := func(content string, denied bool) map[string]any {
				t.Helper()
				commit := writeResult(t, s, "memory.save", writeParams(t, map[string]any{
					"key": "history", "content": content, "metadata": map[string]any{"denied": denied},
				}), false)
				expectedContent, expectedRevision = content, commit["revision"].(json.Number).String()
				return commit
			}
			first := save("old-secret", false)
			second := save("changed-secret", false)
			ref := createdRef(t, first)
			for _, read := range []struct {
				name, method string
				params       map[string]any
			}{
				{"get-base", "history.get", map[string]any{"ref": ref, "asOf": map[string]any{"revision": first["revision"]}}},
				{"get-patch", "history.get", map[string]any{"ref": ref, "asOf": map[string]any{"revision": second["revision"]}}},
				{"diff", "history.diff", map[string]any{"ref": ref, "fromRevision": first["revision"], "toRevision": second["revision"]}},
				{"equal-diff", "history.diff", map[string]any{"ref": ref, "fromRevision": second["revision"], "toRevision": second["revision"]}},
			} {
				t.Run(read.name, func(t *testing.T) {
					save("current-secret", false)
					installed, allowed, calls, scans = scenario != "install", true, 0, 0
					ctx := rpc.WithPrincipal(context.Background(), model.PrincipalRef{Kind: "agent", ID: "worker"})
					if !installed {
						ctx = context.Background()
					}
					duringRead = func() {
						switch scenario {
						case "disable", "install":
							installed = !installed
						case "policy-denies":
							allowed = false
						case "current-value-denied":
							save("current-secret", true)
						}
					}
					before, err := store.Meta(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					response := call(t, s, ctx, read.method, paramsJSON(t, read.params))
					if scenario == "disable" || scenario == "install" {
						assertMemoryWireRefusal(t, response, "authz_unavailable", "-32005", true)
					} else {
						assertMemoryWireRefusal(t, response, "not_found", "-32000", false)
					}
					if scans == 0 || duringRead != nil || scenario == "disable" && calls != 1 || scenario == "install" && calls != 0 {
						t.Fatalf("transition not exercised or stale policy used: calls=%d scans=%d", calls, scans)
					}
					after, err := store.Meta(context.Background())
					if err != nil || scenario != "current-value-denied" && !reflect.DeepEqual(before, after) || after.Head.String() != expectedRevision {
						t.Fatalf("history read mutated storage: before=%+v after=%+v err=%v", before, after, err)
					}
				})
			}
		})
	}
}

func TestRealSQLiteMemoryHistoryDeletionAndReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "history-lifetime.db")
	server, store, lease := openSQLiteMemoryServer(t, path)
	t.Cleanup(func() { _ = lease.Release(ctx); _ = store.Close() })
	save := func(content string) map[string]any {
		return writeResult(t, server, "memory.save", writeParams(t, map[string]any{"key": "history", "content": content}), false)
	}
	first, second := save("old-secret"), save("changed-secret")
	save("current-secret")
	ref := createdRef(t, first)
	reads := []struct {
		method string
		params map[string]any
	}{
		{"history.get", map[string]any{"ref": ref, "asOf": map[string]any{"revision": first["revision"]}}},
		{"history.get", map[string]any{"ref": ref, "asOf": map[string]any{"revision": second["revision"]}}},
		{"history.diff", map[string]any{"ref": ref, "fromRevision": first["revision"], "toRevision": second["revision"]}},
		{"history.diff", map[string]any{"ref": ref, "fromRevision": second["revision"], "toRevision": second["revision"]}},
	}
	for i, read := range reads {
		value := result(t, call(t, server, ctx, read.method, paramsJSON(t, read.params)))
		switch i {
		case 0, 1:
			body := value["body"].(map[string]any)
			want, revision := "old-secret", first["revision"]
			if i == 1 {
				want, revision = "changed-secret", second["revision"]
				patch := value["patch"].([]any)
				if value["base"] != false || len(patch) != 1 || !reflect.DeepEqual(patch[0].(map[string]any)["value"], body) {
					t.Fatalf("historical Body/Patch differ: %v", value)
				}
			}
			if body["content"] != want || value["revision"] != revision || value["deleted"] == true {
				t.Fatalf("historical body or version metadata changed: %v", value)
			}
		case 2:
			if !reflect.DeepEqual(value["changedPaths"], []any{"/content"}) || value["patch"].([]any)[0].(map[string]any)["value"] != "changed-secret" {
				t.Fatalf("historical diff changed: %v", value)
			}
		case 3:
			if len(value["patch"].([]any))+len(value["changedPaths"].([]any)) != 0 {
				t.Fatalf("equal diff changed: %v", value)
			}
		}
	}
	deleted := writeResult(t, server, "memory.delete", writeParams(t, map[string]any{"id": ref["id"]}), false)
	reads = append(reads, struct {
		method string
		params map[string]any
	}{"history.get", map[string]any{"ref": ref, "asOf": map[string]any{"revision": deleted["revision"]}}})
	checkDeleted := func() {
		t.Helper()
		before, err := store.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, read := range reads {
			assertMemoryWireRefusal(t, call(t, server, ctx, read.method, paramsJSON(t, read.params)), "not_found", "-32000", false)
		}
		after, err := store.Meta(ctx)
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("refused history changed storage: before=%+v after=%+v err=%v", before, after, err)
		}
	}
	checkDeleted()
	// Reusing the logical key allocates another reference. Its readable value
	// grants no access to the deleted reference's retained history.
	recreated := save("replacement")
	if createdRef(t, recreated)["id"] == ref["id"] {
		t.Fatal("fixture did not allocate a new memory reference")
	}
	checkDeleted()
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	server, store, lease = openSQLiteMemoryServer(t, path)
	checkDeleted()
	value := result(t, call(t, server, ctx, "history.get", paramsJSON(t, map[string]any{
		"ref": createdRef(t, recreated), "asOf": map[string]any{"revision": recreated["revision"]},
	})))
	if value["body"].(map[string]any)["content"] != "replacement" {
		t.Fatalf("current lifetime did not survive reopen: %v", value)
	}
}

func realServiceServer(t *testing.T) (*rpc.Server, *failingPost, time.Time, uint64) {
	t.Helper()
	ctx := context.Background()
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(ctx, "rpc-integration", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(ctx) })
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	ontology := service.NewOntology(nil)
	post := &failingPost{}
	writer := pipeline.New(store, lease, pipeline.WithOntology(ontology), pipeline.WithClock(clock), pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "test-post", Subscriber: post}))
	domain := service.New(store, lease, writer, service.WithOntology(ontology), service.WithClock(clock))
	// This assignment also checks the actual pure service port at compile time.
	var svc rpc.Service = domain
	config := pagingConfig()
	config.Incarnation = meta.Incarnation
	return pagingServer(t, svc, config), post, now, lease.Epoch()
}

func writeParams(t *testing.T, params map[string]any) map[string]any {
	t.Helper()
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatal(err)
	}
	params["_meta"] = map[string]any{"idempotencyKey": hex.EncodeToString(key[:]), "agent": "rpc-test"}
	return params
}

func createdRef(t *testing.T, commit map[string]any) map[string]any {
	t.Helper()
	rows := commit["results"].([]any)
	if len(rows) != 1 {
		t.Fatalf("missing create result: %v", commit)
	}
	return rows[0].(map[string]any)["ref"].(map[string]any)
}

func writeResult(t *testing.T, s *rpc.Server, method string, params map[string]any, replayed bool) map[string]any {
	t.Helper()
	reply := call(t, s, context.Background(), method, paramsJSON(t, params))
	meta, valid := reply["_meta"].(map[string]any)
	_, present := reply["_meta"]
	if (replayed && (!valid || meta["replayed"] != true)) || (!replayed && present) {
		t.Fatalf("wrong replay metadata for %s (want %t): %v", method, replayed, reply)
	}
	return result(t, reply)
}

func TestRealWriteReplayMetadataPreservesResultBytes(t *testing.T) {
	s, post, _, _ := realServiceServer(t)
	ctx := context.Background()
	head := func() any { return result(t, call(t, s, ctx, "bead.ready", `{}`))["asOf"].(map[string]any)["revision"] }
	var ref map[string]any
	for _, method := range []string{"bead.create", "bead.claim", "bead.update", "bead.comment", "bead.close"} {
		t.Run(method, func(t *testing.T) {
			params := map[string]any{"id": ref}
			switch method {
			case "bead.create":
				params = map[string]any{"type": "issue", "fields": map[string]any{"title": "Original"}}
			case "bead.update":
				params["fields"] = map[string]any{"title": "Changed"}
			case "bead.comment":
				params["body"] = "Replay evidence"
			case "bead.close":
				params["reason"] = "Completed"
			}
			writeParams(t, params)
			if method == "bead.claim" || method == "bead.update" {
				params["_meta"].(map[string]any)["return"] = "minimal"
			}
			var original json.RawMessage
			var postCalls int
			var revision any
			for i := range 2 {
				raw, err := inprocess.New(s).RoundTrip(ctx, request(method, paramsJSON(t, params)))
				if err != nil {
					t.Fatal(err)
				}
				var reply map[string]json.RawMessage
				if err := json.Unmarshal(raw, &reply); err != nil {
					t.Fatal(err)
				}
				if len(reply["error"]) != 0 || len(reply["result"]) == 0 {
					t.Fatalf("write failed: %s", raw)
				}
				if i == 0 {
					if len(reply["_meta"]) != 0 {
						t.Fatalf("first committed/finalized response marked replay: %s", raw)
					}
					original, postCalls, revision = reply["result"], post.calls, head()
					if method == "bead.create" {
						v, err := wire.DecodeJSON(original)
						if err != nil {
							t.Fatal(err)
						}
						ref = createdRef(t, v.(map[string]any))
					}
				} else if string(reply["_meta"]) != `{"replayed":true}` || !bytes.Equal(original, reply["result"]) || head() != revision || post.calls != postCalls {
					t.Fatalf("replay changed stored result bytes/head/post effects or omitted metadata: %s", raw)
				}
			}
		})
	}
}

func TestRealServiceLifecycleThroughInprocess(t *testing.T) {
	s, post, now, epoch := realServiceServer(t)
	ctx := context.Background()
	createParams := writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "Original", "priority": 1}})
	created := result(t, call(t, s, ctx, "bead.create", paramsJSON(t, createParams)))
	ref := createdRef(t, created)
	warnings := created["warnings"].([]any)
	if len(warnings) != 1 || warnings[0].(map[string]any)["code"] != "post_subscriber_failed" {
		t.Fatal("committed warning was not visible at byte boundary", created)
	}
	replay := result(t, call(t, s, ctx, "bead.create", paramsJSON(t, createParams)))
	if !reflect.DeepEqual(created, replay) || post.calls != 1 {
		t.Fatal("replay changed result or reran post subscriber", replay, post.calls)
	}
	ready := result(t, call(t, s, ctx, "bead.ready", `{"limit":1}`))
	if len(ready["items"].([]any)) != 1 || ready["items"].([]any)[0].(map[string]any)["id"] != ref["id"] {
		t.Fatal("real create did not reach ready", ready)
	}
	claimParams := writeParams(t, map[string]any{"id": ref})
	claimParams["_meta"].(map[string]any)["return"] = "minimal"
	claim := result(t, call(t, s, ctx, "bead.claim", paramsJSON(t, claimParams)))
	if claim["leaseTtlMs"] != json.Number("300000") || claim["leaseExpiresAt"] != now.Add(5*time.Minute).Format(time.RFC3339Nano) || claim["commit"] != nil {
		t.Fatal("real claim was not flattened with durable lease values", claim)
	}
	postCalls := post.calls
	replay = result(t, call(t, s, ctx, "bead.claim", paramsJSON(t, claimParams)))
	if !reflect.DeepEqual(claim, replay) || post.calls != postCalls {
		t.Fatal("claim replay changed durable result", replay)
	}
	got := result(t, call(t, s, ctx, "bead.get", paramsJSON(t, map[string]any{"id": ref})))
	lease := got["lease"].(map[string]any)
	if lease["ttlMs"] != json.Number("300000") || lease["epoch"] != json.Number(model.Natural64(epoch).String()) || lease["holder"] != "anonymous" {
		t.Fatal("stored lease wire values changed", lease)
	}
	update := writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"title": "Updated"}, "expect": []any{map[string]any{"ref": ref, "revision": claim["revision"]}}})
	result(t, call(t, s, ctx, "bead.update", paramsJSON(t, update)))
	stale := writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"title": "Stale"}, "expect": update["expect"]})
	requireCode(t, call(t, s, ctx, "bead.update", paramsJSON(t, stale)), "precondition_failed")
	result(t, call(t, s, ctx, "bead.comment", paramsJSON(t, writeParams(t, map[string]any{"id": ref, "body": "Recorded evidence"}))))
	closeParams := writeParams(t, map[string]any{"id": ref, "reason": "Done with evidence"})
	closed := result(t, call(t, s, ctx, "bead.close", paramsJSON(t, closeParams)))
	postCalls = post.calls
	replay = result(t, call(t, s, ctx, "bead.close", paramsJSON(t, closeParams)))
	if !reflect.DeepEqual(closed, replay) || post.calls != postCalls {
		t.Fatal("close replay changed the committed response", replay)
	}
	got = result(t, call(t, s, ctx, "bead.get", paramsJSON(t, map[string]any{"id": ref})))
	if got["bead"].(map[string]any)["fields"].(map[string]any)["title"] != "Updated" || got["bead"].(map[string]any)["revision"] != closed["revision"] {
		t.Fatal("final persisted state not returned by get", got)
	}
	ready = result(t, call(t, s, ctx, "bead.ready", `{"limit":1}`))
	if len(ready["items"].([]any)) != 0 {
		t.Fatal("closed bead remained ready", ready)
	}
	history := result(t, call(t, s, ctx, "history.query", paramsJSON(t, map[string]any{"asOf": map[string]any{"revision": created["revision"]}, "limit": 1})))
	if history["items"].([]any)[0].(map[string]any)["fields"].(map[string]any)["title"] != "Original" {
		t.Fatal("history returned current values", history)
	}
}

func TestRealServiceNamedLifecycleRPCsUseCatalogAndSharedWriter(t *testing.T) {
	s, _, _, _ := realServiceServer(t)
	ctx := context.Background()
	created := writeResult(t, s, "bead.create", writeParams(t, map[string]any{
		"type": "issue", "fields": map[string]any{"title": "named lifecycle"},
	}), false)
	ref := createdRef(t, created)

	methods := result(t, call(t, s, ctx, "rpc.methods", `{}`))["methods"].([]any)
	for _, want := range []string{"bead.close", "bead.reopen", "bead.defer", "bead.cancel", "bead.fail"} {
		found := false
		for _, method := range methods {
			if method == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("cataloged lifecycle method %s was not bound/discovered: %v", want, methods)
		}
	}
	openrpc, err := s.OpenRPC(ctx, model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Methods []struct {
			Name string `json:"name"`
		} `json:"methods"`
	}
	if err := json.Unmarshal(openrpc, &document); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"bead.reopen", "bead.defer", "bead.cancel", "bead.fail"} {
		found := false
		for _, method := range document.Methods {
			if method.Name == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("generated OpenRPC omitted %s", want)
		}
	}

	callSlot := func(method, reason string) map[string]any {
		t.Helper()
		params := map[string]any{"id": ref}
		if reason != "" {
			params["reason"] = reason
		}
		return writeResult(t, s, method, writeParams(t, params), false)
	}
	checkStatus := func(want string) {
		t.Helper()
		got := result(t, call(t, s, ctx, "bead.get", paramsJSON(t, map[string]any{"id": ref})))
		if bead := got["bead"].(map[string]any); bead["status"] != want {
			t.Fatalf("lifecycle status=%v want=%s", bead["status"], want)
		}
	}
	for _, step := range []struct {
		method string
		reason string
		status string
	}{
		{"bead.defer", "waiting", "backlog"},
		{"bead.reopen", "retrying", "todo"},
		{"bead.claim", "", "in_progress"},
		{"bead.fail", "failed", "failed"},
		{"bead.reopen", "retry after failure", "todo"},
		{"bead.cancel", "canceled", "canceled"},
		{"bead.reopen", "reconsidered", "todo"},
		{"bead.claim", "", "in_progress"},
		{"bead.close", "done", "done"},
		{"bead.reopen", "follow-up", "todo"},
	} {
		commit := callSlot(step.method, step.reason)
		if commit["revision"] == nil {
			t.Fatalf("lifecycle %s returned no commit: %v", step.method, commit)
		}
		checkStatus(step.status)
	}

	// Repeating the exact keyed failure returns the stored result and replay
	// metadata without invoking a second Writer commit.
	failParams := writeParams(t, map[string]any{"id": ref, "reason": "terminal"})
	first := writeResult(t, s, "bead.fail", failParams, false)
	replayed := writeResult(t, s, "bead.fail", failParams, true)
	if !reflect.DeepEqual(first, replayed) {
		t.Fatalf("lifecycle replay changed result: first=%v replay=%v", first, replayed)
	}
	checkStatus("failed")

	// A slot cannot transition to its current target. The refusal is typed and
	// does not publish a new revision or receipt.
	bad := writeParams(t, map[string]any{"id": ref})
	requireCode(t, call(t, s, ctx, "bead.fail", paramsJSON(t, bad)), "invalid_params")
	history := result(t, call(t, s, ctx, "history.query", paramsJSON(t, map[string]any{
		"asOf":   map[string]any{"revision": first["revision"]},
		"filter": map[string]any{"resources": []string{ref["id"].(string)}},
	})))
	if len(history["items"].([]any)) != 1 || history["items"].([]any)[0].(map[string]any)["status"] != "failed" {
		t.Fatalf("history did not expose final lifecycle image: %v", history)
	}
}

func TestRealServiceLiveReadyAndPinnedHistoryAcrossWrites(t *testing.T) {
	s, _, _, _ := realServiceServer(t)
	ctx := context.Background()
	var ids []string
	for priority, title := range []string{"first", "pending-close", "third", "pending-update"} {
		created := writeResult(t, s, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": title, "priority": priority}}), false)
		ids = append(ids, createdRef(t, created)["id"].(string))
	}
	params := map[string]any{"limit": 1}
	first := result(t, call(t, s, ctx, "bead.ready", paramsJSON(t, params)))
	at := first["asOf"].(map[string]any)
	if first["items"].([]any)[0].(map[string]any)["id"] != ids[0] {
		t.Fatal("Ready lost priority order", first)
	}
	// Close a row that has not been delivered. A frozen continuation would
	// return it next; live Ready must instead evaluate the current eligible set.
	later := writeResult(t, s, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "later", "priority": 4}}), false)
	laterID := createdRef(t, later)["id"].(string)
	closed := writeResult(t, s, "bead.close", writeParams(t, map[string]any{"id": map[string]any{"id": ids[1]}, "reason": "Closed between pages"}), false)
	params["cursor"] = first["nextCursor"]
	page := result(t, call(t, s, ctx, "bead.ready", paramsJSON(t, params)))
	if len(page["items"].([]any)) != 1 || page["items"].([]any)[0].(map[string]any)["id"] != ids[2] || page["asOf"].(map[string]any)["revision"] != closed["revision"] {
		t.Fatal("Ready returned the old pending row or rejected the live revision", page)
	}
	updated := writeResult(t, s, "bead.update", writeParams(t, map[string]any{"id": map[string]any{"id": ids[3]}, "fields": map[string]any{"title": "Changed between pages"}}), false)
	params["cursor"] = page["nextCursor"]
	page = result(t, call(t, s, ctx, "bead.ready", paramsJSON(t, params)))
	bead := page["items"].([]any)[0].(map[string]any)
	if bead["id"] != ids[3] || bead["fields"].(map[string]any)["title"] != "Changed between pages" || page["asOf"].(map[string]any)["revision"] != updated["revision"] {
		t.Fatal("Ready did not read the pending row's current value", page)
	}
	params["cursor"] = page["nextCursor"]
	page = result(t, call(t, s, ctx, "bead.ready", paramsJSON(t, params)))
	if len(page["items"].([]any)) != 1 || page["items"].([]any)[0].(map[string]any)["id"] != laterID || page["nextCursor"] != nil {
		t.Fatal("Ready did not traverse the later live insert", page)
	}
	params = map[string]any{"limit": 1, "asOf": at}
	var historyIDs []string
	for i := range 4 {
		page = result(t, call(t, s, ctx, "history.query", paramsJSON(t, params)))
		if len(page["items"].([]any)) != 1 || !reflect.DeepEqual(page["asOf"], at) {
			t.Fatal("real history lost its snapshot", page)
		}
		bead := page["items"].([]any)[0].(map[string]any)
		historyIDs = append(historyIDs, bead["id"].(string))
		if bead["id"] == ids[3] && bead["fields"].(map[string]any)["title"] != "pending-update" {
			t.Fatal("History returned the later pending-row update", page)
		}
		if i < 3 {
			params["cursor"] = page["nextCursor"]
		} else if page["nextCursor"] != nil {
			t.Fatal("history continued beyond pinned items")
		}
	}
	sort.Strings(ids)
	if !reflect.DeepEqual(historyIDs, ids) {
		t.Fatal("real history repeated, skipped or included a later item", historyIDs, ids)
	}
}

func TestRealServiceLiveReadyAcceptsEmptyShrunkenTail(t *testing.T) {
	s, _, _, _ := realServiceServer(t)
	ctx := context.Background()
	var refs []map[string]any
	for priority := range 2 {
		created := writeResult(t, s, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "tail", "priority": priority}}), false)
		refs = append(refs, createdRef(t, created))
	}
	first := result(t, call(t, s, ctx, "bead.ready", `{"limit":1}`))
	var last map[string]any
	for _, ref := range refs {
		last = writeResult(t, s, "bead.close", writeParams(t, map[string]any{"id": ref, "reason": "Shrink list"}), false)
	}
	page := result(t, call(t, s, ctx, "bead.ready", paramsJSON(t, map[string]any{"limit": 1, "cursor": first["nextCursor"]})))
	if len(page["items"].([]any)) != 0 || page["nextCursor"] != nil || page["asOf"].(map[string]any)["revision"] != last["revision"] {
		t.Fatal("valid live continuation did not terminate after list shrink", page)
	}
}

func TestRealServiceNoopReceiptsPreserveWireRevisionsAndLease(t *testing.T) {
	s, post, _, _ := realServiceServer(t)
	ctx := context.Background()
	head := func() any {
		return result(t, call(t, s, ctx, "bead.ready", `{}`))["asOf"].(map[string]any)["revision"]
	}
	created := writeResult(t, s, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "Original"}}), false)
	ref := createdRef(t, created)
	claim := writeResult(t, s, "bead.claim", writeParams(t, map[string]any{"id": ref}), false)
	lease := result(t, call(t, s, ctx, "bead.get", paramsJSON(t, map[string]any{"id": ref})))["lease"]
	postCalls := post.calls
	sameClaimParams := writeParams(t, map[string]any{"id": ref})
	sameClaimParams["_meta"].(map[string]any)["return"] = "minimal"
	sameClaim := writeResult(t, s, "bead.claim", sameClaimParams, false)
	if sameClaim["revision"] != claim["revision"] || sameClaim["leaseTtlMs"] != claim["leaseTtlMs"] || sameClaim["leaseExpiresAt"] != claim["leaseExpiresAt"] || head() != claim["revision"] || post.calls != postCalls {
		t.Fatal("fresh-key same-owner claim changed head, lease or subscriber effects", sameClaim)
	}
	if got := result(t, call(t, s, ctx, "bead.get", paramsJSON(t, map[string]any{"id": ref})))["lease"]; !reflect.DeepEqual(got, lease) {
		t.Fatal("same-owner claim changed the stored lease", got, lease)
	}
	// A no-op receipt reports current head while retaining the older record's
	// own revision. Wire conversion must preserve both independently.
	other := writeResult(t, s, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "Other"}}), false)
	postCalls = post.calls
	noopParams := writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"title": "Original"}})
	noop := writeResult(t, s, "bead.update", noopParams, false)
	rows := noop["results"].([]any)
	if noop["revision"] != other["revision"] || len(rows) != 1 || rows[0].(map[string]any)["revision"] != claim["revision"] || head() != other["revision"] || post.calls != postCalls {
		t.Fatal("no-op changed head or narrowed record/receipt revisions", noop)
	}
	updated := writeResult(t, s, "bead.update", writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"title": "Changed"}}), false)
	postCalls = post.calls
	replay := writeResult(t, s, "bead.update", noopParams, true)
	if !reflect.DeepEqual(replay, noop) || head() != updated["revision"] || post.calls != postCalls {
		t.Fatal("no-op replay lost its exact stored response or advanced head", replay)
	}
	replay = writeResult(t, s, "bead.claim", sameClaimParams, true)
	if !reflect.DeepEqual(replay, sameClaim) || head() != updated["revision"] || post.calls != postCalls {
		t.Fatal("same-owner claim replay changed its receipt or current head", replay)
	}
}

func TestRealDryRunDoesNotBecomeReceiptReplay(t *testing.T) {
	s, post, _, _ := realServiceServer(t)
	ctx := context.Background()
	created := writeResult(t, s, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "Original"}}), false)
	ref := createdRef(t, created)
	params := writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"title": "Previewed"}})
	meta := params["_meta"].(map[string]any)
	meta["dryRun"] = true
	postCalls := post.calls
	for range 2 {
		preview := writeResult(t, s, "bead.update", params, false)
		page := result(t, call(t, s, ctx, "bead.ready", `{}`))
		bead := result(t, call(t, s, ctx, "bead.get", paramsJSON(t, map[string]any{"id": ref})))["bead"].(map[string]any)
		if preview["wouldCommit"] != true || page["asOf"].(map[string]any)["revision"] != created["revision"] || bead["fields"].(map[string]any)["title"] != "Original" || post.calls != postCalls {
			t.Fatal("dry-run changed durable state or post effects", preview, bead)
		}
	}
	// The preview did not reserve a receipt: the same key can now execute the
	// real request, and only the retry of that execution is marked replayed.
	delete(meta, "dryRun")
	committed := writeResult(t, s, "bead.update", params, false)
	if committed["revision"] == created["revision"] || post.calls != postCalls+1 {
		t.Fatal("real execution after dry-run did not commit once", committed)
	}
	replayed := writeResult(t, s, "bead.update", params, true)
	if !reflect.DeepEqual(replayed, committed) || post.calls != postCalls+1 {
		t.Fatal("real receipt after dry-run did not replay exactly", replayed)
	}
}

// This uses the actual fixed-permission GuardedService, not discovery visibility
// or missing authentication as a substitute for record-policy denial.
func TestCommentGetThroughGuardedRPCSnapshotsAndRevocation(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	defer store.Close()
	lease, err := store.Lease(ctx, "comment-rpc", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	metadata, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ref := model.RefByID("pl-comment-rpc", model.Natural64(0))
	doc := pipeline.Document{ID: ref.ID, Type: "issue", Project: "project", Fields: map[string]any{"title": "Comment read", "status": "todo"}}
	seed := func(op model.OpKind) model.Changeset {
		t.Helper()
		body, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		v, err := conformance.Seed(ctx, store, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref, Kind: "bead", Op: op, Body: body}})
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	old := seed(model.OpCreate)
	source := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	restored := time.Date(2026, 10, 30, 10, 0, 0, 0, time.UTC)
	actor := model.PrincipalRef{Kind: "user", ID: "restore-admin"}
	doc.Comments = []pipeline.Comment{{Body: "ordinary", Principal: actor, Time: restored}, {Body: "secret imported body", Principal: actor, Time: restored, Origin: "import", SourceAuthor: "source-user", SourceTime: &source}}
	seed(model.OpPatch)
	ns := authz.Namespace{StoreID: string(metadata.StoreID), Incarnation: metadata.Incarnation, InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))}
	project := model.RefByID("project", model.Natural64(0))
	team := model.RefByID("team", model.Natural64(0))
	facts := authzfixed.Facts{Namespace: ns, Projects: []authzfixed.Project{{Ref: project, InstallationID: "fixed", Name: "project", Active: true}}, Teams: []authzfixed.Team{{Ref: team, InstallationID: "fixed", Active: true}}, Grants: []authz.Grant{{Subject: authz.SubjectRef{Kind: "user", ID: "reader"}, Scope: authz.Scope{Kind: authz.TeamScope, Ref: team}, Role: "member"}, {Subject: authz.SubjectRef{Kind: "team", ID: "team"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: project}, Role: "involved"}}}
	mutable, err := authzrpc.NewMutableFacts(facts)
	if err != nil {
		t.Fatal(err)
	}
	composition, err := authzrpc.New(ctx, authzrpc.Config{Store: store, Lease: lease, Provider: authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }}), Facts: mutable, Namespace: ns, DefaultProject: "project"})
	if err != nil {
		t.Fatal(err)
	}
	config := pagingConfig()
	config.Incarnation = metadata.Incarnation
	config.DefaultProject = "project"
	config.AuthMode = "authenticated"
	config.Visible = composition.Checker.Visible
	server := pagingServer(t, composition.Service, config)
	reader := rpc.WithPrincipal(ctx, model.PrincipalRef{Kind: "user", ID: "reader"})
	params := func(at *service.AsOf) string {
		v := map[string]any{"id": ref, "_meta": map[string]any{"project": "project"}}
		if at != nil {
			v["asOf"] = map[string]any{"revision": at.Revision, "incarnation": metadata.Incarnation}
		}
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	got := result(t, call(t, server, reader, "bead.get", params(nil)))
	comments := got["comments"].([]any)
	if len(comments) != 2 {
		t.Fatalf("comments: %+v", comments)
	}
	legacy := comments[0].(map[string]any)
	imported := comments[1].(map[string]any)
	if _, exists := legacy["origin"]; exists {
		t.Fatal("ordinary comment acquired import provenance")
	}
	if imported["principal"].(map[string]any)["id"] != "restore-admin" || imported["sourceAuthor"] != "source-user" || imported["sourceTime"] != "2020-01-01T00:00:00Z" {
		t.Fatalf("actor/source conflated: %+v", imported)
	}
	historical := result(t, call(t, server, reader, "bead.get", params(&service.AsOf{Revision: old.Revision})))
	if _, exists := historical["comments"]; exists {
		t.Fatal("later comments leaked into historical result")
	}
	outsider := rpc.WithPrincipal(ctx, model.PrincipalRef{Kind: "user", ID: "outsider"})
	for _, at := range []*service.AsOf{nil, {Revision: old.Revision}} {
		denied := call(t, server, outsider, "bead.get", params(at))
		requireCode(t, denied, "not_found")
		if _, exists := denied["result"]; exists {
			t.Fatal("denied result leaked")
		}
	}
	facts.Grants = nil
	if err := mutable.Replace(facts); err != nil {
		t.Fatal(err)
	}
	denied := call(t, server, reader, "bead.get", params(&service.AsOf{Revision: old.Revision}))
	requireCode(t, denied, "not_found")
	if _, exists := denied["result"]; exists {
		t.Fatal("revoked historical result leaked")
	}
	requireCode(t, call(t, server, reader, "bead.comment", `{"id":{"id":"pl-comment-rpc","branch":0},"body":"forged","origin":"import","sourceAuthor":"forged","_meta":{"project":"project"}}`), "invalid_params")
}
