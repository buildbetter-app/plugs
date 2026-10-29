package cursorhttp_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const cursorProject = "cursor-http"

var (
	userPrincipal    = model.PrincipalRef{Kind: "user", ID: "user:cursor-owner"}
	workerPrincipal  = model.PrincipalRef{Kind: "service", ID: "service:cursor-worker"}
	testCursorSecret = []byte{
		0x63, 0x75, 0x72, 0x73, 0x6f, 0x72, 0x2d, 0x68,
		0x74, 0x74, 0x70, 0x2d, 0x66, 0x69, 0x78, 0x74,
		0x75, 0x72, 0x65, 0x2d, 0x73, 0x69, 0x67, 0x6e,
		0x69, 0x6e, 0x67, 0x2d, 0x6b, 0x65, 0x79, 0x31,
	}
)

type backend string

const (
	memoryBackend backend = "memory"
	sqliteBackend backend = "sqlite"
)

type fixture struct {
	ctx         context.Context
	cancel      context.CancelFunc
	path        string
	store       driver.Store
	lease       driver.ActiveLease
	keys        *authnapikey.Provider
	userKey     authnapikey.IssuedKey
	workerKey   authnapikey.IssuedKey
	server      *httpjson.Server
	served      chan error
	client      *http.Client
	endpoint    string
	stopRenewal context.CancelFunc
	renewed     chan error
	clock       atomic.Int64
}

func newFixture(t *testing.T, kind backend) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	f := &fixture{ctx: ctx, cancel: cancel, path: filepath.Join(t.TempDir(), "cursor.db")}
	f.clock.Store(time.Date(2026, 10, 29, 16, 0, 0, 0, time.UTC).UnixNano())
	t.Cleanup(func() { f.stop(t) })

	var err error
	switch kind {
	case memoryBackend:
		f.store, err = memory.NewWithOptions(memory.Options{Clock: f.now})
	case sqliteBackend:
		f.store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: f.path})
	default:
		t.Fatalf("unknown backend %q", kind)
	}
	if err != nil {
		t.Fatalf("open %s store: %v", kind, err)
	}
	f.lease, err = f.store.Lease(ctx, "cursor-http-acceptance", driver.LeaseTTL)
	if err != nil {
		t.Fatalf("acquire %s lease: %v", kind, err)
	}
	f.startRenewal(t)

	f.keys = authnapikey.New(authnapikey.Config{Now: f.now})
	f.userKey, err = f.keys.Issue(userPrincipal, f.now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	f.workerKey, err = f.keys.Issue(workerPrincipal, f.now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ontology := service.NewOntology(nil)
	writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(ontology), pipeline.WithClock(f.now))
	domain := service.New(f.store, f.lease, writer, service.WithOntology(ontology), service.WithClock(f.now))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		t.Fatal(err)
	}
	rpcServer, err := rpc.New(registry, rpc.Config{
		Incarnation: meta.Incarnation, DefaultProject: cursorProject,
		AuthMode: "authenticated", OntologyRevision: model.Natural64(1),
		CursorKey: append([]byte(nil), testCursorSecret...),
	})
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := authn.New(authn.Config{Provider: f.keys})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpjson.New(rpcServer, authenticator, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	f.server, err = handler.Listen(ctx, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	f.endpoint = "http://" + f.server.Addr().String() + httpjson.Path
	f.client = &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil}}
	f.served = make(chan error, 1)
	go func() { f.served <- f.server.Serve() }()
	return f
}

func (f *fixture) now() time.Time { return time.Unix(0, f.clock.Load()).UTC() }

func (f *fixture) startRenewal(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(f.ctx)
	f.stopRenewal, f.renewed = cancel, make(chan error, 1)
	lease, renewed := f.lease, f.renewed
	go func() {
		ticker := time.NewTicker(driver.LeaseRenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				renewed <- nil
				return
			case <-ticker.C:
				if err := lease.Renew(ctx); err != nil {
					if ctx.Err() != nil {
						err = nil
					}
					renewed <- err
					return
				}
			}
		}
	}()
}

func (f *fixture) stop(t *testing.T) {
	t.Helper()
	if f.server != nil {
		f.client.CloseIdleConnections()
		if err := f.server.Close(); err != nil {
			t.Error(err)
		}
		select {
		case err := <-f.served:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("cursor HTTP server did not stop")
		}
		f.server = nil
	}
	if f.stopRenewal != nil {
		f.stopRenewal()
		if err := <-f.renewed; err != nil {
			t.Error(err)
		}
		f.stopRenewal = nil
	}
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
	f.cancel()
}

type httpReply struct {
	status int
	head   http.Header
	body   []byte
	value  map[string]any
}

func (f *fixture) call(t *testing.T, token, method string, params map[string]any) httpReply {
	t.Helper()
	data, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": "cursor-test", "method": method, "params": params,
	})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(f.ctx, http.MethodPost, f.endpoint, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Plug-RPC", "1")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := f.client.Do(req)
	if err != nil {
		t.Fatal("cursor HTTP request failed")
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("cursor HTTP response was not JSON: %s", body)
	}
	return httpReply{status: resp.StatusCode, head: resp.Header, body: body, value: value}
}

func success(t *testing.T, reply httpReply) map[string]any {
	t.Helper()
	result, ok := reply.value["result"].(map[string]any)
	if reply.status != http.StatusOK || reply.value["error"] != nil || !ok {
		t.Fatalf("cursor call failed (HTTP %d): %s", reply.status, reply.body)
	}
	return result
}

func errorCode(t *testing.T, reply httpReply, want string) {
	t.Helper()
	if reply.status != http.StatusOK || reply.value["result"] != nil {
		t.Fatalf("cursor refusal was not an RPC error (%d): %s", reply.status, reply.body)
	}
	errValue, ok := reply.value["error"].(map[string]any)
	if !ok {
		t.Fatalf("cursor refusal omitted error: %s", reply.body)
	}
	data, ok := errValue["data"].(map[string]any)
	if !ok || data["code"] != want || data["traceId"] == "" {
		t.Fatalf("cursor refusal code=%v want=%s: %s", data["code"], want, reply.body)
	}
}

func readParams(project string, extra map[string]any) map[string]any {
	params := map[string]any{"_meta": map[string]any{"project": project, "agent": "cursor-test-agent"}}
	for key, value := range extra {
		params[key] = value
	}
	return params
}

func writeParams(t *testing.T, project string, extra map[string]any) map[string]any {
	t.Helper()
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatal(err)
	}
	params := readParams(project, extra)
	params["_meta"] = map[string]any{
		"project": project, "agent": "cursor-test-agent", "idempotencyKey": hex.EncodeToString(key[:]),
	}
	return params
}

func createdRef(t *testing.T, result map[string]any) map[string]any {
	t.Helper()
	rows, ok := result["results"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("missing create result: %#v", result)
	}
	row, ok := rows[0].(map[string]any)
	if !ok {
		t.Fatal("create result was not an object")
	}
	ref, ok := row["ref"].(map[string]any)
	if !ok {
		t.Fatal("create result omitted ref")
	}
	return ref
}

func refID(t *testing.T, ref map[string]any) string {
	t.Helper()
	id, ok := ref["id"].(string)
	if !ok || id == "" {
		t.Fatalf("invalid bead ref: %#v", ref)
	}
	return id
}

func pageItems(t *testing.T, result map[string]any) []map[string]any {
	t.Helper()
	items, ok := result["items"].([]any)
	if !ok {
		t.Fatalf("page omitted items: %#v", result)
	}
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		object, ok := item.(map[string]any)
		if !ok {
			t.Fatal("page item was not an object")
		}
		out = append(out, object)
	}
	return out
}

func cursor(t *testing.T, result map[string]any) string {
	t.Helper()
	value, ok := result["nextCursor"].(string)
	if !ok || value == "" {
		t.Fatalf("page did not return a cursor: %#v", result)
	}
	return value
}

type observedState struct {
	Meta    driver.Meta
	Records []string
	History []string
}

func (f *fixture) state(t *testing.T) observedState {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var records []string
	var page driver.Page[driver.Record]
	var next *driver.Cursor
	for {
		page, err = reader.Query(f.ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000, Cursor: next})
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range page.Items {
			recordBody, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			records = append(records, string(recordBody))
		}
		if page.NextCursor == nil {
			break
		}
		next = page.NextCursor
	}
	changesets, err := reader.Changesets(f.ctx, nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	history := make([]string, 0, len(changesets))
	for _, changeset := range changesets {
		encoded, err := json.Marshal(changeset)
		if err != nil {
			t.Fatal(err)
		}
		history = append(history, string(encoded))
	}
	sort.Strings(records)
	sort.Strings(history)
	return observedState{Meta: meta, Records: records, History: history}
}

func requireSameState(t *testing.T, before, after observedState) {
	t.Helper()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("refusal changed durable state:\nbefore=%+v\nafter=%+v", before, after)
	}
}

func assertRevision(t *testing.T, result map[string]any, want model.Revision) {
	t.Helper()
	value, ok := result["asOf"].(map[string]any)
	if !ok || fmt.Sprint(value["revision"]) != want.String() {
		t.Fatalf("page asOf revision=%v want=%s", value["revision"], want)
	}
}

func assertNoNextCursor(t *testing.T, result map[string]any) {
	t.Helper()
	if _, exists := result["nextCursor"]; exists {
		t.Fatalf("terminal page unexpectedly returned nextCursor: %#v", result)
	}
}
