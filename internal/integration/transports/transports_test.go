package transports_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/server/transport/mcp"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func request(t *testing.T, id any, method string, params any) []byte {
	t.Helper()
	return marshal(t, map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
}

func success(t *testing.T, got envelope) json.RawMessage {
	t.Helper()
	if len(got.Error) != 0 || len(got.Result) == 0 {
		t.Fatalf("expected canonical result: %+v", got)
	}
	var commit struct{ Warnings []json.RawMessage }
	if err := json.Unmarshal(got.Result, &commit); err != nil || len(commit.Warnings) != 0 {
		t.Fatalf("unexpected result/warnings: %s (%v)", got.Result, err)
	}
	return got.Result
}

func errorCode(t *testing.T, got envelope) string {
	t.Helper()
	var failure struct{ Data struct{ Code string } }
	if len(got.Result) != 0 || json.Unmarshal(got.Error, &failure) != nil || failure.Data.Code == "" {
		t.Fatalf("expected canonical error: %+v", got)
	}
	return failure.Data.Code
}

func TestSharedRequestsPersistIdentitiesNativeCallbacksAndCrossTransportReplay(t *testing.T) {
	for _, authenticated := range []bool{false, true} {
		t.Run(fmt.Sprintf("api_key=%t", authenticated), func(t *testing.T) {
			f := newFixture(t, authenticated)
			for i, writer := range transports {
				t.Run(writer, func(t *testing.T) {
					// A large numeric request ID remains raw JSON, including MCP's
					// tool framing. The very same write is replayed on every adapter.
					id := json.RawMessage(`9007199254740993`)
					if i%2 != 0 {
						id = json.RawMessage(`"escaped\u002did"`)
					}
					params := map[string]any{"type": "issue", "fields": map[string]any{"title": "Shared request <" + writer + ">"}, "_meta": map[string]any{
						"idempotencyKey": fmt.Sprintf("550e8400-e29b-41d4-a716-%012d", i+1), "agent": "i3-fixture",
					}}
					canonical := request(t, id, "bead.create", params)
					first := f.call(t, writer, canonical)
					want := success(t, first)
					if first.Meta.Replayed || !bytes.Equal(first.ID, id) {
						t.Fatal("first write replayed or changed the raw request ID")
					}
					for _, transport := range transports {
						got := f.call(t, transport, canonical)
						if !bytes.Equal(success(t, got), want) || !got.Meta.Replayed || !bytes.Equal(got.ID, first.ID) {
							t.Fatalf("%s changed commit bytes, replay metadata or ID", transport)
						}
					}
					var commit struct {
						Results []struct{ Ref json.RawMessage }
					}
					if err := json.Unmarshal(want, &commit); err != nil || len(commit.Results) != 1 {
						t.Fatalf("create result: %s (%v)", want, err)
					}
					get := request(t, "escaped-id-\u2603", "bead.get", map[string]any{"id": commit.Results[0].Ref})
					var read json.RawMessage
					for _, transport := range transports {
						got := success(t, f.call(t, transport, get))
						if read != nil && !bytes.Equal(read, got) {
							t.Fatalf("%s changed canonical read bytes", transport)
						}
						read = got
					}
				})
			}
			if f.validators.Load() != 4 || f.posts.Load() != 4 || f.targetCalls.Load() != 0 {
				t.Fatalf("validators=%d posts=%d outbound requests=%d", f.validators.Load(), f.posts.Load(), f.targetCalls.Load())
			}
			meta, err := f.store.Meta(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			reader, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			changes, err := reader.Changesets(context.Background(), nil, 10)
			if err != nil || len(changes) != 4 {
				t.Fatalf("durable writes=%d (%v)", len(changes), err)
			}
			for _, change := range changes {
				if !reflect.DeepEqual(change.Principal, f.principal) {
					t.Fatal("admitted identity changed before persistence")
				}
			}
			if f.token != "" && (bytes.Contains(marshal(t, changes), []byte(f.token)) || bytes.Contains(marshal(t, changes), []byte(f.keyID))) {
				t.Fatal("API-key material persisted in a changeset")
			}
		})
	}
}

func TestSharedErrorsAndHumanPresenceGate(t *testing.T) {
	for _, authenticated := range []bool{false, true} {
		t.Run(fmt.Sprintf("api_key=%t", authenticated), func(t *testing.T) {
			f := newFixture(t, authenticated)
			for _, transport := range transports {
				invalid := request(t, "invalid", "bead.create", map[string]any{"type": "issue", "unknown": true})
				if code := errorCode(t, f.call(t, transport, invalid)); code != "invalid_params" {
					t.Fatalf("%s invalid params: %s", transport, code)
				}
				// This is the real admin handler with no presence verifier. An
				// API-key User cannot manufacture an interactive user session.
				admin := request(t, "admin", "admin.dump", map[string]any{"history": "head", "humanPresence": map[string]any{
					"mode": "presence_check", "challenge": "i3-untrusted", "proof": "i3-untrusted",
				}})
				if code := errorCode(t, f.call(t, transport, admin)); code != "human_presence_required" {
					t.Fatalf("%s human presence: %s", transport, code)
				}
			}
			if f.validators.Load() != 0 || f.posts.Load() != 0 || f.targetCalls.Load() != 0 {
				t.Fatal("refused requests reached native callbacks or egress")
			}
		})
	}
}

func TestOpenWebSocketRechecksRevokedAndExpiredAPIKeysBeforeDispatch(t *testing.T) {
	for _, invalidate := range []string{"revoke", "expire"} {
		t.Run(invalidate, func(t *testing.T) {
			var clock atomic.Int64
			start := time.Now()
			clock.Store(start.UnixNano())
			f := newFixtureWithClock(t, true, func() time.Time { return time.Unix(0, clock.Load()) })
			connection, frames, status := f.connect(t, nil)
			defer connection.Close()
			if status != http.StatusSwitchingProtocols {
				t.Fatalf("initial upgrade: %d", status)
			}
			create := func(id, key, title string) []byte {
				return request(t, id, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": title}, "_meta": map[string]any{"idempotencyKey": key}})
			}
			writeFrame(t, connection, create("accepted", "550e8400-e29b-41d4-a716-446655440000", "live key"))
			var accepted envelope
			if err := json.Unmarshal(readFrame(t, frames), &accepted); err != nil {
				t.Fatal(err)
			}
			success(t, accepted)
			before, err := f.store.Meta(context.Background())
			if err != nil || before.Head.IsZero() || f.validators.Load() != 1 || f.posts.Load() != 1 {
				t.Fatal("live connection did not perform one durable write and native callback pair")
			}
			if invalidate == "revoke" {
				if !f.keys.Revoke(f.keyID) {
					t.Fatal("fixture key revocation did not acknowledge")
				}
			} else {
				// Reach the exact expiry without sleeps or external time sources.
				clock.Store(start.Add(time.Hour).UnixNano())
			}
			// Reuse the upgraded socket. A fresh-handshake denial alone does not
			// protect a long-lived connection or a request already in its queue.
			writeFrame(t, connection, create("refused", "550e8400-e29b-41d4-a716-446655440001", "must not persist"))
			opcode, payload := readServerFrame(t, frames)
			if opcode != 8 || len(payload) < 2 || binary.BigEndian.Uint16(payload[:2]) != 1008 {
				t.Errorf("invalidated connection returned opcode %d; want policy close 1008", opcode)
			}
			if bytes.Contains(payload, []byte(f.token)) || bytes.Contains(payload, []byte(f.keyID)) {
				t.Error("WebSocket credential invalidation leaked key material")
			}
			after, err := f.store.Meta(context.Background())
			if err != nil || after.Head != before.Head || f.validators.Load() != 1 || f.posts.Load() != 1 || f.targetCalls.Load() != 0 {
				t.Error("invalidated connection reached a durable write, native callback or egress")
			}
		})
	}
}

func TestHostOriginMarkerAndCredentialDenialsPrecedeCore(t *testing.T) {
	for _, authenticated := range []bool{false, true} {
		t.Run(fmt.Sprintf("api_key=%t", authenticated), func(t *testing.T) {
			f := newFixture(t, authenticated)
			canonical := request(t, 1, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "must not persist"}, "_meta": map[string]any{"idempotencyKey": "550e8400-e29b-41d4-a716-446655440000"}})
			type denial struct {
				name   string
				change func(*http.Request)
				status int
			}
			denials := []denial{
				{"host", func(r *http.Request) {
					r.Host = "attacker.invalid"
					r.Header.Set("X-Forwarded-Host", strings.TrimPrefix(f.url, "http://"))
					r.Header.Set("X-Forwarded-For", "127.0.0.1")
				}, 403},
				{"origin", func(r *http.Request) { r.Header.Set("Origin", "http://attacker.invalid") }, 403},
				{"null origin", func(r *http.Request) { r.Header.Set("Origin", "null") }, 403},
				{"duplicate origin", func(r *http.Request) { r.Header.Add("Origin", f.url) }, 403},
				{"bad credential", func(r *http.Request) { r.Header.Set("Authorization", "Bearer invalid-fixture-key") }, 401},
			}
			if authenticated {
				denials = append(denials, denial{"missing credential", func(r *http.Request) { r.Header.Del("Authorization") }, 401})
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
							t.Fatalf("%s status=%d want=%d", path, status, denial.status)
						}
					}
					connection, _, status := f.connect(t, denial.change)
					connection.Close()
					if status != denial.status {
						t.Fatalf("WebSocket status=%d want=%d", status, denial.status)
					}
				})
			}
			for _, path := range []string{"/rpc/v1", mcp.Path} {
				status, _, _ := f.post(t, path, canonical, func(r *http.Request) { r.Header.Del("Plug-RPC") })
				if status != 403 {
					t.Fatalf("%s missing marker: %d", path, status)
				}
			}
			if authenticated {
				if !f.keys.Revoke(f.keyID) {
					t.Fatal("fixture key did not revoke")
				}
				for _, path := range []string{"/rpc/v1", mcp.Path} {
					status, _, _ := f.post(t, path, canonical, nil)
					if status != 401 {
						t.Fatalf("%s revoked key: %d", path, status)
					}
				}
				connection, _, status := f.connect(t, nil)
				connection.Close()
				if status != 401 {
					t.Fatalf("WebSocket revoked key: %d", status)
				}
				if _, err := f.auth.Admit(context.Background(), f.token); err == nil {
					t.Fatal("revoked stdio admission succeeded")
				}
			}
			meta, err := f.store.Meta(context.Background())
			if err != nil || !meta.Head.IsZero() || f.validators.Load() != 0 || f.posts.Load() != 0 || f.targetCalls.Load() != 0 {
				t.Fatal("denied transport request reached a durable write, callback or egress")
			}
			for _, address := range []string{"0.0.0.0:0", "[::]:0", "attacker.invalid:0"} {
				listener, err := f.auth.Listen(context.Background(), address, nil)
				if listener != nil {
					listener.Close()
				}
				if err == nil {
					t.Fatalf("forbidden listener accepted: %s", address)
				}
			}
		})
	}
}
