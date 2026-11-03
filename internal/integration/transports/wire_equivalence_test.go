package transports_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/composition"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/mcp"
	"github.com/buildbetter-app/plugs/internal/server/transport/stdio"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Omission makes a write a dropped notification; explicit null is a call.
// Exercise that distinction through configured composition, with each adapter
// making the first successful call as well as carrying the identical replay
// and conflicting request. See engdocs/plug-runtime/rpc-http.md and transports.md.
func TestWireOmittedWriteIDAndExplicitNullID(t *testing.T) {
	for _, authenticated := range []bool{false, true} {
		t.Run(fmt.Sprintf("api_key=%t", authenticated), func(t *testing.T) {
			f, runtime, _ := configuredFixture(t, sqliteServerConfig(t), authenticated, time.Now)
			for i, writer := range transports {
				t.Run("first="+writer, func(t *testing.T) {
					meta := map[string]any{"idempotencyKey": fmt.Sprintf("550e8400-e29b-41d4-a716-%012d", i+101)}
					params := map[string]any{"type": "issue", "fields": map[string]any{"title": "Null ID <" + writer + ">"}, "_meta": meta}
					notification := marshal(t, map[string]any{"jsonrpc": "2.0", "method": "bead.create", "params": params})
					before := captureWireState(t, f, runtime)
					for _, transport := range transports {
						t.Run("omitted/"+transport, func(t *testing.T) {
							assertDroppedWireWrite(t, f, transport, notification)
							if after := captureWireState(t, f, runtime); !reflect.DeepEqual(after, before) {
								t.Fatal("omitted-ID write changed store metadata, native checkpoint, callbacks or egress")
							}
						})
					}

					// The same valid params and admitted identity now make a call.
					// It must commit, rather than replay a receipt from a notification.
					canonical := request(t, json.RawMessage(`null`), "bead.create", params)
					first := f.call(t, writer, canonical)
					want := success(t, first)
					if string(first.ID) != "null" || first.Meta.Replayed {
						t.Fatal("first explicit-null call lost its ID or replayed a dropped notification")
					}
					accepted := captureWireState(t, f, runtime)
					if accepted.meta.Head == before.meta.Head || accepted.meta.Incarnation != before.meta.Incarnation ||
						accepted.validators != int64(i+1) || accepted.posts != int64(i+1) || accepted.outbound != 0 {
						t.Fatal("explicit-null positive control did not commit once through the native runtime")
					}
					receipt := acceptedCheckpoint(t, runtime, i+1)
					if receipt.Principal.Kind != f.principal.Kind || receipt.Principal.ID != f.principal.ID || string(receipt.Revision) != accepted.meta.Head.String() {
						t.Fatal("explicit-null call lost its admitted principal or committed native revision")
					}

					conflict := request(t, json.RawMessage(`null`), "bead.create", map[string]any{
						"type": "issue", "fields": map[string]any{"title": "must not replace original"}, "_meta": meta,
					})
					var wantError []byte
					for _, transport := range transports {
						t.Run("null/"+transport, func(t *testing.T) {
							assertReplay := func() {
								got := f.call(t, transport, canonical)
								if string(got.ID) != "null" || !got.Meta.Replayed || !bytes.Equal(success(t, got), want) {
									t.Fatal("explicit-null replay changed ID, canonical commit bytes or replay metadata")
								}
							}
							assertReplay()
							gotError := stableWireConflict(t, f.call(t, transport, conflict))
							if wantError != nil && !bytes.Equal(gotError, wantError) {
								t.Fatalf("typed error changed beyond its fresh traceId: got %s want %s", gotError, wantError)
							}
							wantError = gotError
							if after := captureWireState(t, f, runtime); !reflect.DeepEqual(after, accepted) {
								t.Fatal("conflicting null-ID call changed store metadata, native checkpoint, callbacks or egress")
							}
							// A refusal must also preserve the original durable receipt.
							assertReplay()
							if after := captureWireState(t, f, runtime); !reflect.DeepEqual(after, accepted) {
								t.Fatal("replay after refusal changed durable state or reran native callbacks")
							}
						})
					}
				})
			}
		})
	}
}

type wireState struct {
	meta                        driver.Meta
	checkpoint                  driver.Checkpoint
	validators, posts, outbound int64
}

func captureWireState(t *testing.T, f *fixture, runtime *composition.Runtime) wireState {
	t.Helper()
	meta, err := runtime.Store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	store, ok := runtime.Store.(driver.PersistentCheckpointStore)
	if !ok || !store.Persistent() {
		t.Fatal("wire fixture requires persistent native checkpoint storage")
	}
	checkpoint, err := store.LoadCheckpoint(context.Background(), "plugin/transportprobe/default/durable")
	if err != nil {
		t.Fatal(err)
	}
	return wireState{meta, checkpoint, f.validators.Load(), f.posts.Load(), f.targetCalls.Load()}
}

// Unlike mcpRequest, this wrapper preserves whether the canonical ID exists.
// MCP adds its required tools/call envelope around the same method and params.
func wireMCPRequest(t *testing.T, canonical []byte) []byte {
	t.Helper()
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(canonical, &raw); err != nil {
		t.Fatal(err)
	}
	outer := map[string]any{"jsonrpc": "2.0", "method": "tools/call", "params": map[string]any{
		"name": "plug_call", "arguments": map[string]any{"method": raw["method"], "params": raw["params"]},
	}}
	if id, present := raw["id"]; present {
		outer["id"] = id
	}
	return marshal(t, outer)
}

func assertDroppedWireWrite(t *testing.T, f *fixture, transport string, canonical []byte) {
	t.Helper()
	switch transport {
	case "http", "mcp":
		path, body, wantStatus := "/rpc/v1", canonical, http.StatusNoContent
		if transport == "mcp" {
			path, body, wantStatus = mcp.Path, wireMCPRequest(t, canonical), http.StatusAccepted
		}
		status, _, response := f.post(t, path, body, nil)
		if status != wantStatus || len(response) != 0 {
			t.Fatalf("notification status=%d want=%d body=%q", status, wantStatus, response)
		}
	case "websocket":
		connection, reader, status := f.connect(t, nil)
		defer connection.Close()
		if status != http.StatusSwitchingProtocols {
			t.Fatalf("notification WebSocket handshake: %d", status)
		}
		writeFrame(t, connection, canonical)
		var rejected struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      json.RawMessage `json:"id"`
			Method  string          `json:"method"`
			Params  struct{ Reason, RequestID string }
		}
		response := readFrame(t, reader)
		if err := json.Unmarshal(response, &rejected); err != nil || rejected.JSONRPC != "2.0" || len(rejected.ID) != 0 ||
			rejected.Method != "rpc.rejected" || rejected.Params.Reason != "notification_no_result" || rejected.Params.RequestID != "bead.create" {
			t.Fatalf("notification rejection=%s (%v)", response, err)
		}
	case "stdio":
		identity, err := f.auth.Admit(context.Background(), f.token)
		if err != nil {
			t.Fatal(err)
		}
		ctx := rpc.WithPrincipal(authn.WithIdentity(context.Background(), identity), identity.Principal())
		adapter, err := stdio.New(f.rpc, stdio.Config{})
		if err != nil {
			t.Fatal(err)
		}
		var output bytes.Buffer
		input := append(append([]byte(nil), canonical...), '\n')
		if err := adapter.Serve(ctx, bytes.NewReader(input), &output); err != nil {
			t.Fatal(err)
		}
		if output.Len() != 0 {
			t.Fatalf("stdio notification produced a response frame: %q", output.Bytes())
		}
	default:
		t.Fatalf("unknown wire transport %q", transport)
	}
}

func stableWireConflict(t *testing.T, got envelope) []byte {
	t.Helper()
	if string(got.ID) != "null" || len(got.Result) != 0 || got.Meta.Replayed {
		t.Fatal("conflicting explicit-null call changed its ID or returned a result/replay")
	}
	var failure map[string]json.RawMessage
	if err := json.Unmarshal(got.Error, &failure); err != nil {
		t.Fatal(err)
	}
	var data map[string]json.RawMessage
	if err := json.Unmarshal(failure["data"], &data); err != nil {
		t.Fatal(err)
	}
	var traceID string
	if string(failure["code"]) != "-32020" || string(failure["message"]) != `"idempotency key reused with different parameters"` ||
		string(data["code"]) != `"idempotency_key_reused"` || string(data["retryable"]) != "false" ||
		json.Unmarshal(data["traceId"], &traceID) != nil || traceID == "" {
		t.Fatalf("conflict did not preserve its typed catalog error: %s", got.Error)
	}
	// Each dispatch owns its trace. Preserve every other error member exactly.
	delete(data, "traceId")
	failure["data"] = marshal(t, data)
	return marshal(t, failure)
}
