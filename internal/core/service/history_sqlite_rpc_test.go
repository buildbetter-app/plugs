package service_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type sqliteHistoryRPC struct {
	store  driver.Store
	lease  driver.ActiveLease
	server *rpc.Server
	key    int
}

func openSQLiteHistoryRPC(t *testing.T, path string) *sqliteHistoryRPC {
	t.Helper()
	ctx := context.Background()
	store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	f := &sqliteHistoryRPC{store: store}
	t.Cleanup(func() { f.close(t) })
	f.lease, err = store.Lease(ctx, "history-rpc", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	ontology := service.NewOntology(nil)
	writer := pipeline.New(store, f.lease, pipeline.WithOntology(ontology))
	domain := service.New(store, f.lease, writer, service.WithOntology(ontology))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		t.Fatal(err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	f.server, err = rpc.New(registry, rpc.Config{DefaultProject: "local", Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *sqliteHistoryRPC) close(t *testing.T) {
	t.Helper()
	if f.lease != nil {
		if err := f.lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
		f.lease = nil
	}
	if f.store != nil {
		if err := f.store.Close(); err != nil {
			t.Error(err)
		}
		f.store = nil
	}
}

func (f *sqliteHistoryRPC) call(t *testing.T, method string, params map[string]any, out any, wantCode string) {
	t.Helper()
	if method == "bead.create" || method == "bead.update" {
		f.key++
		params["_meta"] = map[string]any{"idempotencyKey": fmt.Sprintf("%032d", f.key), "agent": "history-rpc"}
	}
	request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := inprocess.New(f.server).RoundTrip(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	var response struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code int `json:"code"`
			Data struct {
				Code      string `json:"code"`
				Retryable bool   `json:"retryable"`
			} `json:"data"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		t.Fatal(err)
	}
	if wantCode != "" {
		if response.Error == nil || response.Error.Data.Code != wantCode || response.Error.Data.Retryable || len(response.Result) != 0 {
			t.Fatalf("%s: want typed nonretryable %s without a result, got %s", method, wantCode, raw)
		}
		return
	}
	if response.Error != nil {
		t.Fatalf("%s failed: %s", method, raw)
	}
	decoder := json.NewDecoder(bytes.NewReader(response.Result))
	decoder.UseNumber()
	if err := decoder.Decode(out); err != nil {
		t.Fatalf("%s: %v", method, err)
	}
}

type sqliteHistoryState struct {
	Meta       driver.Meta
	Records    []driver.Record
	Versions   []driver.Version
	Changesets []model.Changeset
}

func (f *sqliteHistoryRPC) state(t *testing.T, ref model.Ref) sqliteHistoryState {
	t.Helper()
	ctx := context.Background()
	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	state := sqliteHistoryState{Meta: meta}
	state.Records, err = r.Get(ctx, []driver.Ref{ref})
	if err != nil {
		t.Fatal(err)
	}
	versions, err := r.Versions(ctx, ref, driver.PageReq{Limit: 100})
	if err != nil || versions.NextCursor != nil {
		t.Fatalf("versions: %+v %v", versions, err)
	}
	state.Versions = versions.Items
	state.Changesets, err = r.Changesets(ctx, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func TestSQLiteHistoryPatchVersionsThroughRPCAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")
	f := openSQLiteHistoryRPC(t, path)
	var created, updated model.Commit
	f.call(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "original"}}, &created, "")
	ref := created.Results[0].Ref
	f.call(t, "bead.update", map[string]any{"id": ref, "fields": map[string]any{"title": "historical"}}, &updated, "")

	check := func(t *testing.T) {
		t.Helper()
		before := f.state(t, ref)
		if len(before.Versions) < 2 {
			t.Fatal("missing real SQLite versions")
		}
		stored := before.Versions[1]
		if stored.Base || stored.Deleted || len(stored.Body) != 0 || len(stored.Patch) == 0 || stored.Revision != updated.Revision {
			t.Fatalf("fixture did not produce a live patch-only SQLite version: %+v", stored)
		}
		var version service.HistoryVersion
		f.call(t, "history.get", map[string]any{"ref": ref, "asOf": map[string]any{"revision": updated.Revision, "incarnation": before.Meta.Incarnation}}, &version, "")
		var patch []map[string]any
		decoder := json.NewDecoder(bytes.NewReader(stored.Patch))
		decoder.UseNumber()
		if err := decoder.Decode(&patch); err != nil {
			t.Fatal(err)
		}
		if version.Base || version.Deleted || version.Ref != stored.Ref || version.Revision != stored.Revision || !version.CreatedAt.Equal(stored.CreatedAt) || !reflect.DeepEqual(version.Principal, stored.Principal) || !reflect.DeepEqual(version.Patch, patch) {
			t.Fatalf("materialization changed selected version metadata or patch: %+v; stored=%+v", version, stored)
		}
		var expected map[string]any
		decoder = json.NewDecoder(bytes.NewReader(updated.Results[0].Record))
		decoder.UseNumber()
		if err := decoder.Decode(&expected); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(version.Body, expected) || version.Body["fields"].(map[string]any)["title"] != "historical" {
			t.Fatalf("historical body came from another revision: %+v", version)
		}
		var base service.HistoryVersion
		f.call(t, "history.get", map[string]any{"ref": ref, "asOf": map[string]any{"revision": created.Revision}}, &base, "")
		if !base.Base || base.Body["fields"].(map[string]any)["title"] != "original" || len(base.Patch) != 0 {
			t.Fatalf("base changed: %+v", base)
		}
		var diff service.HistoryDiff
		f.call(t, "history.diff", map[string]any{"ref": ref, "fromRevision": updated.Revision, "toRevision": updated.Revision}, &diff, "")
		if diff.Patch == nil || diff.ChangedPaths == nil || len(diff.Patch)+len(diff.ChangedPaths) != 0 {
			t.Fatalf("equal diff: %+v", diff)
		}
		f.call(t, "history.diff", map[string]any{"ref": ref, "fromRevision": created.Revision, "toRevision": updated.Revision}, &diff, "")
		if !reflect.DeepEqual(diff.ChangedPaths, []string{"/fields/title"}) || len(diff.Patch) != 1 || diff.Patch[0].Value != "historical" {
			t.Fatalf("pinned diff: %+v", diff)
		}
		f.call(t, "history.get", map[string]any{"ref": ref, "asOf": map[string]any{"revision": model.Natural64(0)}}, nil, "not_found")
		f.call(t, "history.get", map[string]any{"ref": ref, "asOf": map[string]any{"revision": updated.Revision, "incarnation": "wrong-incarnation"}}, nil, "incarnation_changed")
		head, err := before.Meta.Head.Uint64()
		if err != nil {
			t.Fatal(err)
		}
		f.call(t, "history.get", map[string]any{"ref": ref, "asOf": map[string]any{"revision": model.Natural64(head + 1)}}, nil, "not_found")
		if after := f.state(t, ref); !reflect.DeepEqual(before, after) {
			t.Fatal("history reads changed persisted records, versions, log or metadata")
		}
	}
	t.Run("patch-at-head", check)
	var later model.Commit
	f.call(t, "bead.update", map[string]any{"id": ref, "fields": map[string]any{"title": "current"}}, &later, "")
	// An unrelated commit makes the requested snapshot differ from both the
	// current head and the source record's most recent revision.
	f.call(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "unrelated"}}, &later, "")
	t.Run("later-head", check)
	beforeReopen := f.state(t, ref)
	f.close(t)
	f = openSQLiteHistoryRPC(t, path)
	if afterReopen := f.state(t, ref); !reflect.DeepEqual(beforeReopen, afterReopen) {
		t.Fatal("SQLite reopen changed durable history")
	}
	t.Run("after-reopen", check)
}
