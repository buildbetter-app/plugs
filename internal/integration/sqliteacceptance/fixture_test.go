package sqliteacceptance_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const project = "sqlite-acceptance"

var anonymous = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}

// Only composition and assertions live here: all writes, workflow rules,
// receipts, cursors and dispatch use the production implementations unchanged.
type fixture struct {
	ctx         context.Context
	path        string
	clock       atomic.Int64
	cursorKey   []byte
	store       driver.Store
	lease       driver.ActiveLease
	writer      *pipeline.Writer
	domain      *service.Service
	rpc         *rpc.Server
	catalog     *wire.Catalog
	server      *httpjson.Server
	served      chan error
	client      *http.Client
	endpoint    string
	stopRenewal context.CancelFunc
	renewed     chan error
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	f := &fixture{ctx: ctx, path: filepath.Join(t.TempDir(), "acceptance.db"), cursorKey: make([]byte, 32)}
	f.clock.Store(time.Date(2026, 10, 29, 12, 40, 0, 0, time.UTC).UnixNano())
	if _, err := rand.Read(f.cursorKey); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.stop(t) })
	f.start(t)
	return f
}

func (f *fixture) now() time.Time { return time.Unix(0, f.clock.Load()).UTC() }

func (f *fixture) start(t *testing.T) {
	t.Helper()
	var err error
	f.store, err = (sqlite.Driver{}).Open(f.ctx, driver.Config{DSN: f.path})
	if err != nil {
		t.Fatal(err)
	}
	f.lease, err = f.store.Lease(f.ctx, "sqlite-acceptance", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	lease := f.lease
	renewCtx, stop := context.WithCancel(f.ctx)
	f.stopRenewal, f.renewed = stop, make(chan error, 1)
	renewed := f.renewed
	go func() {
		ticker := time.NewTicker(driver.LeaseRenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				renewed <- nil
				return
			case <-ticker.C:
				if err := lease.Renew(renewCtx); err != nil {
					if renewCtx.Err() != nil {
						err = nil
					}
					renewed <- err
					return
				}
			}
		}
	}()
	ontology := service.NewOntology(nil)
	f.writer = pipeline.New(f.store, f.lease, pipeline.WithOntology(ontology), pipeline.WithClock(f.now))
	f.domain = service.New(f.store, f.lease, f.writer, service.WithOntology(ontology), service.WithClock(f.now))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, f.domain); err != nil {
		t.Fatal(err)
	}
	f.catalog = registry.Catalog()
	f.rpc, err = rpc.New(registry, rpc.Config{Incarnation: f.meta(t).Incarnation, DefaultProject: project, OntologyRevision: model.Natural64(1), CursorKey: f.cursorKey})
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := authn.New(authn.Config{})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpjson.New(f.rpc, authenticator, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	f.server, err = handler.Listen(f.ctx, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	server, done := f.server, make(chan error, 1)
	f.served = done
	go func() { done <- server.Serve() }()
	f.endpoint = "http://" + server.Addr().String() + httpjson.Path
	f.client = &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil}}
	f.assertSQLite(t)
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
			t.Error("HTTP server did not stop")
		}
		f.server = nil
	}
	if f.stopRenewal != nil {
		f.stopRenewal()
		if err := <-f.renewed; err != nil {
			t.Error("real storage lease renewal:", err)
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
}

func (f *fixture) reopen(t *testing.T) {
	t.Helper()
	before, epoch := f.meta(t), f.lease.Epoch()
	f.stop(t)
	f.start(t)
	if after := f.meta(t); !reflect.DeepEqual(before, after) || f.lease.Epoch() <= epoch {
		t.Fatalf("close/reopen lost identity/head or reused lease epoch: before=%+v after=%+v", before, after)
	}
	t.Logf("real SQLite close/reopen preserved store %s, incarnation %s, head %s; storage epochs %d -> %d", before.StoreID, before.Incarnation, before.Head, epoch, f.lease.Epoch())
}

func (f *fixture) assertSQLite(t *testing.T) {
	t.Helper()
	// Probe the actual engine through a separate read-only SQL connection.
	u := url.URL{Scheme: "file", Path: f.path, RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version, mode string
	if err := db.QueryRowContext(f.ctx, "SELECT sqlite_version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(f.ctx, "PRAGMA journal_mode").Scan(&mode); err != nil || mode != "wal" {
		t.Fatalf("actual SQLite journal mode=%q: %v", mode, err)
	}
	header, err := os.Open(f.path)
	if err != nil {
		t.Fatal(err)
	}
	defer header.Close()
	var magic [16]byte
	if _, err := io.ReadFull(header, magic[:]); err != nil || string(magic[:]) != "SQLite format 3\x00" {
		t.Fatalf("not an actual SQLite file: %q %v", magic, err)
	}
	t.Logf("SQLite %s, journal_mode=%s, disposable regular database %s", version, mode, f.path)
}

func (f *fixture) meta(t *testing.T) driver.Meta {
	t.Helper()
	m, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func key(t *testing.T) string {
	t.Helper()
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(value[:])
}

func writeParams(t *testing.T, params map[string]any) map[string]any {
	t.Helper()
	params["_meta"] = map[string]any{"idempotencyKey": key(t), "agent": "owner"}
	return params
}

func writeMeta(t *testing.T) model.WriteMeta {
	t.Helper()
	return model.WriteMeta{Method: "tx.apply", Project: project, AgentLabel: "owner", IdempotencyKey: key(t)}
}

type reply struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *wire.Error     `json:"error"`
	Meta    *struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
}

// invoke can run in a goroutine: assertions stay in the calling test goroutine.
func (f *fixture) invoke(method string, params map[string]any) (reply, error) {
	var out reply
	data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(`9007199254740993123456789`), "method": method, "params": params})
	if err != nil {
		return out, err
	}
	req, err := http.NewRequestWithContext(f.ctx, http.MethodPost, f.endpoint, bytes.NewReader(data))
	if err != nil {
		return out, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Plug-RPC", "1")
	resp, err := f.client.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return out, err
	}
	if resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("HTTP status %d: %s", resp.StatusCode, raw)
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, err
	}
	if out.JSONRPC != "2.0" || string(out.ID) != "9007199254740993123456789" || (out.Error == nil) == (len(out.Result) == 0) {
		return out, fmt.Errorf("invalid JSON-RPC envelope: %s", raw)
	}
	return out, nil
}

func (f *fixture) call(t *testing.T, method string, params map[string]any) reply {
	t.Helper()
	r, err := f.invoke(method, params)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func decodeResult(t *testing.T, r reply, target any) {
	t.Helper()
	if r.Error != nil {
		t.Fatalf("unexpected RPC error: %+v", r.Error)
	}
	dec := json.NewDecoder(bytes.NewReader(r.Result))
	dec.UseNumber()
	if err := dec.Decode(target); err != nil {
		t.Fatalf("invalid result %s: %v", r.Result, err)
	}
}

func (f *fixture) commit(t *testing.T, method string, params map[string]any) (model.Commit, reply) {
	t.Helper()
	r := f.call(t, method, params)
	var c model.Commit
	decodeResult(t, r, &c)
	return c, r
}

func (f *fixture) create(t *testing.T, title string) (model.Ref, model.Commit, map[string]any) {
	t.Helper()
	params := writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": title, "description": "original"}})
	c, r := f.commit(t, "bead.create", params)
	if len(c.Results) != 1 || c.Results[0].Ref.ID == "" || r.Meta != nil {
		t.Fatalf("invalid first create: %+v", c)
	}
	return c.Results[0].Ref, c, params
}

func (f *fixture) code(t *testing.T, r reply, want string) {
	t.Helper()
	if r.Error == nil || string(r.Error.Data.Code) != want || len(r.Result) != 0 || r.Meta != nil || f.catalog.ValidateError(r.Error) != nil {
		t.Fatalf("want catalog error %s, got %+v", want, r)
	}
}

func domainCode(t *testing.T, err error, want string) {
	t.Helper()
	var typed *model.Error
	if !errors.As(err, &typed) || string(typed.Data.Code) != want {
		t.Fatalf("want %s, got %T %v", want, err, err)
	}
}

type bead struct {
	ID       string         `json:"id"`
	Revision model.Revision `json:"revision"`
	Status   string         `json:"status"`
	Fields   map[string]any `json:"fields"`
}

type asOf struct {
	Revision    model.Revision `json:"revision"`
	Incarnation string         `json:"incarnation"`
}

type beadPage struct {
	Items      []bead `json:"items"`
	AsOf       asOf   `json:"asOf"`
	NextCursor string `json:"nextCursor"`
}

func (f *fixture) get(t *testing.T, ref model.Ref, at *asOf) bead {
	t.Helper()
	params := map[string]any{"id": ref}
	if at != nil {
		params["asOf"] = at
	}
	var result struct {
		Bead bead `json:"bead"`
	}
	decodeResult(t, f.call(t, "bead.get", params), &result)
	return result.Bead
}

// durableState compares only public driver reads, never SQL table layouts or
// private pipeline structures. Small fixtures fit in one record/version page.
type durableState struct {
	Meta     driver.Meta
	Records  []driver.Record
	Versions map[string][]driver.Version
	Log      []model.Changeset
}

func (f *fixture) reader(t *testing.T) driver.Reader {
	t.Helper()
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: f.meta(t).Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func (f *fixture) state(t *testing.T) durableState {
	t.Helper()
	s := durableState{Meta: f.meta(t), Versions: map[string][]driver.Version{}}
	r := f.reader(t)
	defer r.Close()
	page, err := r.Query(f.ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000})
	if err != nil || page.NextCursor != nil {
		t.Fatalf("record query incomplete: %+v %v", page, err)
	}
	s.Records = page.Items
	for _, record := range s.Records {
		versions, err := r.Versions(f.ctx, record.Ref, driver.PageReq{Limit: 1000})
		if err != nil || versions.NextCursor != nil {
			t.Fatalf("version query incomplete: %+v %v", versions, err)
		}
		s.Versions[record.Ref.ID] = versions.Items
	}
	s.Log, err = r.Changesets(f.ctx, nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func (f *fixture) receipt(t *testing.T, method, agent, idempotencyKey string) *driver.IdempotencyRecord {
	t.Helper()
	r := f.reader(t)
	defer r.Close()
	principal, err := driver.PrincipalKey(anonymous)
	if err != nil {
		t.Fatal(err)
	}
	value, err := r.IdempotencyByKey(f.ctx, principal, agent, method, idempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func requireSameState(t *testing.T, before, after durableState) {
	t.Helper()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("durable state changed: before=%+v after=%+v", before, after)
	}
}
