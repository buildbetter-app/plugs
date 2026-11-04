package authzrpc_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
)

// Keep the wire members raw: decoding retryable into bool would let an omitted
// false value pass, and the ordinary fixture's reply omits JSON-RPC code/ID.
func b04HTTPRefusal(t *testing.T, f *fixture, by model.PrincipalRef, method string, params map[string]any, key int, code string, rpcCode int, retryable bool) string {
	t.Helper()
	issued, err := f.keys.Issue(by, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	params["_meta"] = map[string]any{
		"idempotencyKey": fmt.Sprintf("%032x", key),
		// A foreign authenticated caller must not borrow the holder's identity
		// by asserting its spelling in the anonymous agent-label field.
		"agent": "agent:worker",
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": key, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1"+httpjson.Path, bytes.NewReader(body))
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Plug-RPC", "1")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+issued.Token)
	w := httptest.NewRecorder()
	f.server.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("claim refusal HTTP status=%d", w.Code)
	}
	var frame struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      int             `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   json.RawMessage `json:"error"`
	}
	if err := wire.DecodeInto(w.Body.Bytes(), &frame); err != nil {
		t.Fatal(err)
	}
	if frame.JSONRPC != "2.0" || frame.ID != key || len(frame.Result) != 0 || frame.Error == nil {
		t.Fatalf("claim refusal lost its request ID or returned a success: %s", w.Body.Bytes())
	}
	var e wire.Error
	if err := wire.DecodeInto(frame.Error, &e); err != nil {
		t.Fatal(err)
	}
	var explicit struct {
		Data struct {
			Retryable *bool `json:"retryable"`
		} `json:"data"`
	}
	if err := json.Unmarshal(frame.Error, &explicit); err != nil {
		t.Fatal(err)
	}
	catalog, err := wire.DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.ValidateError(&e); err != nil {
		t.Fatalf("claim refusal violates the closed error catalog: %v", err)
	}
	if e.Code != rpcCode || e.Message != catalog.NewError(code, "", nil).Message || e.Data.Code != code || explicit.Data.Retryable == nil || e.Data.Retryable != retryable {
		t.Fatalf("claim refusal code/retryable=%+v want=%s/%d/%t", e, code, rpcCode, retryable)
	}
	trace, err := hex.DecodeString(e.Data.TraceID)
	if err != nil || len(trace) != 16 {
		t.Fatalf("claim refusal omitted its 16-byte public trace: %q", e.Data.TraceID)
	}
	return e.Data.TraceID
}

func TestS1B04AuthenticatedHTTPErrorBounds(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			var dsn func(*testing.T) string
			if engine == "postgres" {
				dsn = postgresDSNFactory(t)
			}
			for _, row := range []struct {
				name, method, code string
				rpcCode            int
				retryable          bool
				params             map[string]any
				prepare            func(*testing.T, *quietFixture) model.PrincipalRef
			}{
				{"foreign_holder_with_forged_agent_label", "bead.claim", "busy", -32016, true, quietParams(), func(t *testing.T, f *quietFixture) model.PrincipalRef {
					f.claim(t, worker("user:operator"))
					foreign := worker("user:operator")
					foreign.ID = "agent:foreign"
					return foreign
				}},
				{"ungranted_sponsor", "bead.claim", "forbidden", -32004, false, quietParams(), func(*testing.T, *quietFixture) model.PrincipalRef {
					return worker("user:ungranted")
				}},
				{"claim_next_limit", "bead.claimNext", "invalid_params", -32602, false, map[string]any{"limit": 1}, nil},
				{"claim_next_offset", "bead.claimNext", "invalid_params", -32602, false, map[string]any{"offset": 1}, nil},
				{"claim_next_brief", "bead.claimNext", "invalid_params", -32602, false, map[string]any{"brief": true}, nil},
				{"claim_next_changed_receipt_selector", "bead.claimNext", "idempotency_key_reused", -32020, false, map[string]any{"filter": map[string]any{"resources": []string{beadID}}}, func(t *testing.T, f *quietFixture) model.PrincipalRef {
					by := worker("user:operator")
					success(t, f.call(t, by, "bead.claimNext", map[string]any{}, 8100))
					if receipt := f.receipt(t, by, "bead.claimNext", 8100); receipt == nil || receipt.ResponsePending {
						t.Fatal("positive claimNext did not publish its actual receipt")
					}
					return by
				}},
			} {
				t.Run(row.name, func(t *testing.T) {
					f := newQuietFixture(t, engine, dsn)
					by := worker("user:operator")
					if row.prepare != nil {
						by = row.prepare(t, f)
					}
					before := f.state(t, by, 8100)
					receipts := []any{f.receipt(t, by, "bead.claim", 8100), f.receipt(t, by, "bead.claimNext", 8100)}
					var priorTrace string
					for attempt := 0; attempt < 2; attempt++ {
						trace := b04HTTPRefusal(t, f.fixture, by, row.method, row.params, 8100, row.code, row.rpcCode, row.retryable)
						if trace == priorTrace {
							t.Fatal("separate refused HTTP requests reused a public trace")
						}
						priorTrace = trace
						if !reflect.DeepEqual(before, f.state(t, by, 8100)) || !reflect.DeepEqual(receipts, []any{f.receipt(t, by, "bead.claim", 8100), f.receipt(t, by, "bead.claimNext", 8100)}) {
							t.Fatal("claim refusal changed the target row/head/history/versions, selected graph/authority facts, claim overlay, heartbeat checkpoint or claim/claimNext receipts")
						}
					}
					t.Logf("B04 real HTTP+API-key+installed-policy %s retryable=%t trace=16bytes two refusals conserve the target/history/selected facts/checkpoints and both claim-method receipts", row.code, row.retryable)
				})
			}
		})
	}
}
