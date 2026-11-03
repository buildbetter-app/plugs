package transports_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/composition"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/transport/mcp"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

type nativeReceipt struct {
	Count     int
	Principal pluginapi.PrincipalRef
	Revision  pluginapi.Revision
}

// The configured SQLite driver, host, Writer, registry and listener are all
// opened by production composition. Only trusted compiled-in callbacks and an
// optional API-key provider are supplied by the embedding host.
func configuredFixture(t *testing.T, path string, authenticated bool, now func() time.Time) (*fixture, *composition.Runtime, func()) {
	t.Helper()
	config, err := composition.LoadConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{principal: model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}}
	if authenticated {
		f.principal = model.PrincipalRef{Kind: "user", ID: "user:i3-configured"}
		f.keys = authnapikey.New(authnapikey.Config{Now: now})
		issued, err := f.keys.Issue(f.principal, now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		f.token, f.keyID = issued.Token, issued.ID
		config.AuthMode, config.Auth = "authenticated", authn.Config{Provider: f.keys}
	}
	native, bindings := newTransportProbe(t, f, authenticated)
	native.Manifest.Requires.State = pluginapi.StateDurable
	construct := native.New
	native.New = func(host pluginapi.Host) (sdk.Registration, error) {
		registration, err := construct(host)
		if err != nil {
			return sdk.Registration{}, err
		}
		post := registration.Posts["inspect"]
		registration.Posts["inspect"] = sdk.AfterFunc(func(ctx context.Context, change pluginapi.CommittedChange, reader pluginapi.SnapshotReader) error {
			if err := post.After(ctx, change, reader); err != nil {
				return err
			}
			state := host.DurableState()
			var previous nativeReceipt
			_, revision, err := state.Read(ctx, "accepted", &previous)
			if err != nil {
				return err
			}
			_, err = state.Put(ctx, "accepted", nativeReceipt{previous.Count + 1, change.Principal, change.Revision}, revision)
			return err
		})
		return registration, nil
	}
	config.NativePlugins, config.NativeBindings = []sdk.Native{native}, bindings
	runtime, err := composition.Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	server, err := runtime.Listen(context.Background(), config.Address, nil)
	if err != nil {
		_ = runtime.Close(context.Background())
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve() }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			if err := runtime.Close(context.Background()); err != nil {
				t.Error(err)
			}
			select {
			case err := <-done:
				if !errors.Is(err, http.ErrServerClosed) {
					t.Errorf("configured listener shutdown: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Error("configured listener survived Runtime.Close")
			}
		})
	}
	t.Cleanup(stop)
	f.rpc, f.auth, f.url = runtime.RPC, runtime.Authenticator, "http://"+server.Addr().String()
	f.client = &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 5 * time.Second}
	t.Cleanup(f.client.CloseIdleConnections)
	f.initializeMCP(t)
	return f, runtime, stop
}

func sqliteServerConfig(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	data := fmt.Sprintf("storage:\n  driver: sqlite\n  dsn: %q\nserver:\n  project: i3\n  listen: 127.0.0.1:0\n  ui: false\n", filepath.Join(dir, "server.db"))
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func acceptedCheckpoint(t *testing.T, runtime *composition.Runtime, count int) nativeReceipt {
	t.Helper()
	store, ok := runtime.Store.(driver.PersistentCheckpointStore)
	if !ok || !store.Persistent() {
		t.Fatal("configured native runtime did not use persistent checkpoint storage")
	}
	checkpoint, err := store.LoadCheckpoint(context.Background(), "plugin/transportprobe/default/durable")
	if err != nil || checkpoint.Generation != uint64(count) {
		t.Fatalf("native callback checkpoint generation=%d want=%d err=%v", checkpoint.Generation, count, err)
	}
	var namespace struct {
		Values map[string]struct {
			Revision uint64
			Value    nativeReceipt
		}
	}
	if err := json.Unmarshal(checkpoint.Payload, &namespace); err != nil {
		t.Fatal(err)
	}
	receipt := namespace.Values["accepted"]
	if receipt.Revision != uint64(count) || receipt.Value.Count != count {
		t.Fatalf("native callback state=%+v want=%d", receipt, count)
	}
	return receipt.Value
}

func TestConfiguredSQLiteNativeTransportsPersistAndRestart(t *testing.T) {
	for _, authenticated := range []bool{false, true} {
		t.Run(fmt.Sprintf("api_key=%t", authenticated), func(t *testing.T) {
			path := sqliteServerConfig(t)
			f, runtime, stop := configuredFixture(t, path, authenticated, time.Now)
			var last json.RawMessage
			var retainedRequest, retainedID, retainedResult json.RawMessage
			for i, writer := range transports {
				canonical := request(t, json.RawMessage(`9007199254740993`), "bead.create", map[string]any{
					"type": "issue", "fields": map[string]any{"title": "Configured <" + writer + ">"},
					"_meta": map[string]any{"idempotencyKey": fmt.Sprintf("550e8400-e29b-41d4-a716-%012d", i+1)},
				})
				first := f.call(t, writer, canonical)
				last = success(t, first)
				retainedRequest = append(json.RawMessage(nil), canonical...)
				retainedID = append(json.RawMessage(nil), first.ID...)
				retainedResult = append(json.RawMessage(nil), last...)
				for _, transport := range transports {
					got := f.call(t, transport, canonical)
					if !got.Meta.Replayed || !bytes.Equal(got.ID, first.ID) || !bytes.Equal(success(t, got), last) {
						t.Fatalf("configured %s changed canonical replay bytes or identity", transport)
					}
				}
				receipt := acceptedCheckpoint(t, runtime, i+1)
				if receipt.Principal.Kind != f.principal.Kind || receipt.Principal.ID != f.principal.ID {
					t.Fatal("native durable receipt lost admitted principal")
				}
			}
			if f.validators.Load() != 4 || f.posts.Load() != 4 || f.targetCalls.Load() != 0 {
				t.Fatal("configured callbacks replayed or made denied outbound requests")
			}
			meta, err := runtime.Store.Meta(context.Background())
			if err != nil || acceptedCheckpoint(t, runtime, 4).Revision != pluginapi.Revision(meta.Head.String()) {
				t.Fatalf("native checkpoint did not describe the configured core commit: %v", err)
			}
			oldHost, oldLease := runtime.NativeHost, runtime.Lease
			oldSession := f.session
			stop()
			if _, err := oldHost.Invoke(context.Background(), "x.transportprobe.absent", json.RawMessage(`{}`)); err == nil {
				t.Fatal("closed composition left native host active")
			} else {
				var diagnostic *pluginhost.Error
				if !errors.As(err, &diagnostic) || diagnostic.Code != pluginhost.ErrInactive {
					t.Fatalf("native host was not closed: %v", err)
				}
			}
			select {
			case <-oldLease.Lost():
			default:
				t.Fatal("Runtime.Close retained its active lease")
			}
			f, runtime, _ = configuredFixture(t, path, authenticated, time.Now)
			reopened, err := runtime.Store.Meta(context.Background())
			if err != nil || reopened.Head != meta.Head || reopened.Incarnation != meta.Incarnation {
				t.Fatal("restart changed configured core store identity or head")
			}
			acceptedCheckpoint(t, runtime, 4)
			for _, transport := range transports {
				replay := f.call(t, transport, retainedRequest)
				if !replay.Meta.Replayed || !bytes.Equal(replay.ID, retainedID) || !bytes.Equal(success(t, replay), retainedResult) {
					t.Fatalf("configured %s lost the pre-restart canonical receipt", transport)
				}
			}
			if f.validators.Load() != 0 || f.posts.Load() != 0 || f.targetCalls.Load() != 0 {
				t.Fatal("reopened replay reran native callbacks or outbound requests")
			}
			afterReplay, err := runtime.Store.Meta(context.Background())
			if err != nil || afterReplay.Head != reopened.Head || afterReplay.Incarnation != reopened.Incarnation {
				t.Fatal("reopened replay changed the configured head or incarnation")
			}
			acceptedCheckpoint(t, runtime, 4)
			status, _, _ := f.post(t, mcp.Path, []byte(`{"jsonrpc":"2.0","id":1,"method":"ping"}`), func(r *http.Request) { r.Header.Set("MCP-Session-Id", oldSession) })
			if status != http.StatusNotFound {
				t.Fatalf("restart retained old MCP authority: %d", status)
			}
			success(t, f.call(t, "http", request(t, "after-restart", "bead.create", map[string]any{
				"type": "issue", "fields": map[string]any{"title": "After restart"},
				"_meta": map[string]any{"idempotencyKey": "550e8400-e29b-41d4-a716-446655440099"},
			})))
			acceptedCheckpoint(t, runtime, 5)
		})
	}
}

func TestConfiguredSQLiteNativeAdmissionAndSSELifecycle(t *testing.T) {
	for _, authenticated := range []bool{false, true} {
		t.Run(fmt.Sprintf("api_key=%t", authenticated), func(t *testing.T) {
			f, runtime, stop := configuredFixture(t, sqliteServerConfig(t), authenticated, time.Now)
			streamRequest := func(change func(*http.Request)) *http.Response {
				t.Helper()
				req, err := http.NewRequest(http.MethodGet, f.url+mcp.Path, nil)
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Plug-RPC", "1")
				req.Header.Set("Origin", f.url)
				req.Header.Set("Accept", "text/event-stream")
				req.Header.Set("MCP-Session-Id", f.session)
				if f.token != "" {
					req.Header.Set("Authorization", "Bearer "+f.token)
				}
				if change != nil {
					change(req)
				}
				response, err := f.client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = response.Body.Close() })
				return response
			}
			canonical := request(t, "denied", "bead.create", map[string]any{
				"type": "issue", "fields": map[string]any{"title": "must not persist"},
				"_meta": map[string]any{"idempotencyKey": "550e8400-e29b-41d4-a716-446655440001"},
			})
			denials := []struct {
				name   string
				change func(*http.Request)
				status int
			}{
				{"host", func(r *http.Request) {
					r.Host = "attacker.invalid"
					r.Header.Set("X-Forwarded-Host", f.url)
					r.Header.Set("X-Forwarded-For", "127.0.0.1")
				}, http.StatusForbidden},
				{"origin", func(r *http.Request) { r.Header.Set("Origin", "http://attacker.invalid") }, http.StatusForbidden},
				{"credential", func(r *http.Request) { r.Header.Set("Authorization", "Bearer invalid-fixture-key") }, http.StatusUnauthorized},
			}
			for _, denial := range denials {
				t.Run(denial.name, func(t *testing.T) {
					for _, path := range []string{"/rpc/v1", mcp.Path} {
						body := canonical
						if path == mcp.Path {
							body = mcpRequest(t, canonical)
						}
						status, _, _ := f.post(t, path, body, denial.change)
						if status != denial.status {
							t.Fatalf("configured %s status=%d want=%d", path, status, denial.status)
						}
					}
					connection, _, status := f.connect(t, denial.change)
					connection.Close()
					if status != denial.status {
						t.Fatalf("configured WebSocket status=%d want=%d", status, denial.status)
					}
					response := streamRequest(denial.change)
					response.Body.Close()
					if response.StatusCode != denial.status {
						t.Fatalf("configured SSE status=%d want=%d", response.StatusCode, denial.status)
					}
				})
			}
			if authenticated {
				key, err := f.keys.Issue(model.PrincipalRef{Kind: "user", ID: "user:i3-foreign"}, time.Now().Add(time.Hour))
				if err != nil {
					t.Fatal(err)
				}
				foreign := streamRequest(func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+key.Token) })
				foreign.Body.Close()
				if foreign.StatusCode != http.StatusNotFound {
					t.Fatal("foreign principal attached to the configured SSE session")
				}
			}
			meta, err := runtime.Store.Meta(context.Background())
			checkpoint, cpErr := runtime.Store.(driver.CheckpointStore).LoadCheckpoint(context.Background(), "plugin/transportprobe/default/durable")
			if err != nil || cpErr != nil || !meta.Head.IsZero() || checkpoint.Generation != 0 || f.validators.Load() != 0 || f.posts.Load() != 0 || f.targetCalls.Load() != 0 {
				t.Fatal("configured denied request reached core, native state or egress")
			}
			stream := streamRequest(nil)
			if stream.StatusCode != http.StatusOK || stream.Header.Get("Content-Type") != "text/event-stream" {
				t.Fatal("configured runtime did not mount an admitted MCP SSE stream")
			}
			finished := make(chan error, 1)
			go func() { _, err := io.Copy(io.Discard, stream.Body); finished <- err }()
			success(t, f.call(t, "http", canonical))
			acceptedCheckpoint(t, runtime, 1)
			stop()
			select {
			case err := <-finished:
				if err != nil {
					t.Fatalf("configured SSE shutdown did not drain: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("Runtime.Close retained the admitted SSE stream")
			}
		})
	}
}

func TestConfiguredSQLiteWebSocketRechecksCredentialsBeforeNativeDispatch(t *testing.T) {
	for _, invalidate := range []string{"revoke", "expire"} {
		t.Run(invalidate, func(t *testing.T) {
			start := time.Now()
			var clock atomic.Int64
			clock.Store(start.UnixNano())
			f, runtime, _ := configuredFixture(t, sqliteServerConfig(t), true, func() time.Time { return time.Unix(0, clock.Load()) })
			connection, frames, status := f.connect(t, nil)
			defer connection.Close()
			if status != http.StatusSwitchingProtocols {
				t.Fatalf("configured upgrade: %d", status)
			}
			create := func(key string) []byte {
				return request(t, "write", "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "live key"}, "_meta": map[string]any{"idempotencyKey": key}})
			}
			writeFrame(t, connection, create("550e8400-e29b-41d4-a716-446655440001"))
			var admitted envelope
			if err := json.Unmarshal(readFrame(t, frames), &admitted); err != nil {
				t.Fatal(err)
			}
			success(t, admitted)
			before, err := runtime.Store.Meta(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			acceptedCheckpoint(t, runtime, 1)
			if invalidate == "revoke" {
				if !f.keys.Revoke(f.keyID) {
					t.Fatal("configured key revocation was not acknowledged")
				}
			} else {
				clock.Store(start.Add(time.Hour).UnixNano())
			}
			writeFrame(t, connection, create("550e8400-e29b-41d4-a716-446655440002"))
			opcode, payload := readServerFrame(t, frames)
			if opcode != 8 || len(payload) < 2 || binary.BigEndian.Uint16(payload[:2]) != 1008 {
				t.Fatal("invalidated configured WebSocket did not close with policy code 1008")
			}
			after, err := runtime.Store.Meta(context.Background())
			if err != nil || after.Head != before.Head || f.validators.Load() != 1 || f.posts.Load() != 1 || f.targetCalls.Load() != 0 {
				t.Fatal("invalidated configured WebSocket reached core, native callbacks or egress")
			}
			acceptedCheckpoint(t, runtime, 1)
		})
	}
}
