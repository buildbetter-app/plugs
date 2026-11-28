package rpc_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func registry(t *testing.T) *rpc.Registry {
	t.Helper()
	r, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func server(t *testing.T, r *rpc.Registry, c rpc.Config) *rpc.Server {
	t.Helper()
	if c.Incarnation == "" {
		c.Incarnation = "test-incarnation"
	}
	s, err := rpc.New(r, c)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func getResult() any {
	return map[string]any{"bead": map[string]any{"id": "p-1", "type": "task", "revision": 1}, "attachments": []any{}}
}
func commitResult() any {
	return map[string]any{"revision": 1, "results": []any{}, "warnings": []any{}}
}
func request(method, params string) []byte {
	return []byte(`{"jsonrpc":"2.0","id":1,"method":"` + method + `","params":` + params + `}`)
}
func call(t *testing.T, s *rpc.Server, ctx context.Context, method, params string) map[string]any {
	t.Helper()
	raw, err := inprocess.New(s).RoundTrip(ctx, request(method, params))
	if err != nil {
		t.Fatal(err)
	}
	v, err := wire.DecodeJSON(raw)
	if err != nil {
		t.Fatalf("%s: %v", raw, err)
	}
	return v.(map[string]any)
}
func result(t *testing.T, response map[string]any) map[string]any {
	t.Helper()
	if e, ok := response["error"]; ok {
		t.Fatalf("unexpected error: %v", e)
	}
	return response["result"].(map[string]any)
}
func requireCode(t *testing.T, response map[string]any, code string) {
	t.Helper()
	e, ok := response["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected %s, got %v", code, response)
	}
	data := e["data"].(map[string]any)
	if data["code"] != code {
		t.Fatalf("expected %s, got %v", code, e)
	}
}

func TestGraphWriteAdmissionQueuesWithinRequestDeadline(t *testing.T) {
	r := registry(t)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.Register("bead.create", func(ctx context.Context, _ rpc.Call) (any, error) {
		entered <- struct{}{}
		select {
		case <-release:
			return commitResult(), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.Register("bead.heartbeat", func(context.Context, rpc.Call) (any, error) {
		return map[string]any{"leaseExpiresAt": "2026-11-26T01:00:00Z"}, nil
	}); err != nil {
		t.Fatal(err)
	}
	s := server(t, r, rpc.Config{GraphWriteCapacity: 2})
	create := request("bead.create", `{"type":"task","fields":{},"_meta":{"idempotencyKey":"11111111111111111111111111111111"}}`)
	first := make(chan []byte, 1)
	go func() { first <- s.Dispatch(ctx, create) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("first write never entered")
	}
	result(t, call(t, s, ctx, "rpc.methods", "{}"))
	result(t, call(t, s, ctx, "bead.heartbeat", `{"id":{"id":"p-1","branch":0},"_meta":{"idempotencyKey":"22222222222222222222222222222222"}}`))
	short, stop := context.WithTimeout(ctx, 50*time.Millisecond)
	raw := s.Dispatch(short, create)
	stop()
	var admission struct{ Error *wire.Error }
	if err := json.Unmarshal(raw, &admission); err != nil || admission.Error == nil || admission.Error.Data.Code != "busy" ||
		admission.Error.Data.Hint != "graph write admission deadline or cancellation reached" {
		t.Fatalf("queued write response = %s, error = %v", raw, err)
	}

	queued := make(chan []byte, 1)
	go func() { queued <- s.Dispatch(ctx, create) }()
	close(release)
	for _, response := range []<-chan []byte{first, queued} {
		select {
		case raw := <-response:
			decoded, err := wire.DecodeJSON(raw)
			if err != nil {
				t.Fatal(err)
			}
			result(t, decoded.(map[string]any))
		case <-ctx.Done():
			t.Fatal("accepted graph writes did not complete")
		}
	}
	select {
	case <-entered:
	default:
		t.Fatal("queued write did not reach the handler")
	}
}

func TestWriteDeadlineBeforePublicationReturnsRetryableBusy(t *testing.T) {
	r := registry(t)
	if err := r.Register("bead.create", func(ctx context.Context, _ rpc.Call) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	s := server(t, r, rpc.Config{GraphWriteCapacity: 1})
	body := request("bead.create", `{"type":"task","fields":{},"_meta":{"deadlineMs":20,"idempotencyKey":"11111111111111111111111111111111"}}`)
	raw := s.Dispatch(context.Background(), body)
	var response struct{ Error *wire.Error }
	if err := json.Unmarshal(raw, &response); err != nil || response.Error == nil ||
		response.Error.Data.Code != "busy" || !response.Error.Data.Retryable {
		t.Fatalf("expired write response = %s, error = %v", raw, err)
	}
}

func TestWriteCleanupDeadlinePreservesTypedRefusal(t *testing.T) {
	for _, code := range []model.ErrorCode{"ontology_guard_failed", "write_conflict"} {
		t.Run(string(code), func(t *testing.T) {
			r := registry(t)
			if err := r.Register("bead.create", func(context.Context, rpc.Call) (any, error) {
				refusal := &model.Error{Data: model.ErrorData{Code: code, Hint: "original refusal"}}
				return nil, errors.Join(refusal, fmt.Errorf("cleanup: %w", context.DeadlineExceeded))
			}); err != nil {
				t.Fatal(err)
			}
			s := server(t, r, rpc.Config{})
			raw := s.Dispatch(context.Background(), request("bead.create", `{"type":"task","fields":{},"_meta":{"idempotencyKey":"11111111111111111111111111111111"}}`))
			var response struct{ Error *wire.Error }
			if err := json.Unmarshal(raw, &response); err != nil || response.Error == nil ||
				response.Error.Data.Code != string(code) || response.Error.Data.Hint != "original refusal" {
				t.Fatalf("cleanup deadline obscured %s: response = %s, error = %v", code, raw, err)
			}
		})
	}
}

func TestIndeterminateWriteOutranksJoinedRefusal(t *testing.T) {
	const key = "11111111111111111111111111111111"
	for _, tc := range []struct {
		code model.ErrorCode
		err  error
	}{
		{"ontology_guard_failed", driver.ErrIndeterminate{IdempotencyKey: key}},
		{"write_conflict", &driver.ErrIndeterminate{IdempotencyKey: key}},
	} {
		t.Run(string(tc.code), func(t *testing.T) {
			r := registry(t)
			if err := r.Register("bead.create", func(context.Context, rpc.Call) (any, error) {
				refusal := &model.Error{Data: model.ErrorData{Code: tc.code}}
				return nil, errors.Join(refusal, tc.err, context.DeadlineExceeded)
			}); err != nil {
				t.Fatal(err)
			}
			s := server(t, r, rpc.Config{})
			raw := s.Dispatch(context.Background(), request("bead.create", `{"type":"task","fields":{},"_meta":{"idempotencyKey":"`+key+`"}}`))
			var response struct{ Error *wire.Error }
			if err := json.Unmarshal(raw, &response); err != nil || response.Error == nil ||
				response.Error.Data.Code != "busy" || !response.Error.Data.Retryable ||
				response.Error.Data.Details["outcome"] != "unknown" || response.Error.Data.Details["idempotencyKey"] != key {
				t.Fatalf("uncertainty was obscured by %s: response = %s, error = %v", tc.code, raw, err)
			}
		})
	}
}

func TestCurrentIncarnationResolvesAfterAdmissionForEveryBatchCall(t *testing.T) {
	current, lookups := "before-restore", 0
	type requestKey struct{}
	ctx := context.WithValue(context.Background(), requestKey{}, "same-request")
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	r := registry(t)
	if err := r.Register("bead.ready", func(context.Context, rpc.Call) (any, error) {
		t.Fatal("unadmitted request reached handler")
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.Register("bead.create", func(context.Context, rpc.Call) (any, error) {
		current = "after-restore"
		return commitResult(), nil
	}); err != nil {
		t.Fatal(err)
	}
	s := server(t, r, rpc.Config{Incarnation: "startup", AuthMode: "authenticated", CurrentIncarnation: func(ctx context.Context) (string, error) {
		lookups++
		if ctx.Value(requestKey{}) != "same-request" {
			t.Fatal("provider lost request context")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("provider ran before deadline admission")
		}
		return current, nil
	}})
	deadline := `{}`
	batch := []byte(`[` + string(request("server.hello", deadline)) + `,` + string(request("bead.create", `{"type":"task","fields":{},"_meta":{"idempotencyKey":"11111111111111111111111111111111"}}`)) + `,` + string(request("server.hello", deadline)) + `]`)
	var responses []map[string]any
	if err := json.Unmarshal(s.Dispatch(rpc.WithPrincipal(ctx, model.PrincipalRef{Kind: "user", ID: "actual"}), batch), &responses); err != nil {
		t.Fatal(err)
	}
	if len(responses) != 3 || result(t, responses[0])["incarnation"] != "before-restore" || result(t, responses[2])["incarnation"] != "after-restore" || lookups != 2 {
		t.Fatalf("batch retained one generation or unrelated call read metadata: lookups=%d %v", lookups, responses)
	}
	// Schema/auth/deadline failures must not invoke the metadata provider.
	for _, tc := range []struct{ method, params, code string }{
		{"bead.ready", `{}`, "unauthenticated"},
		{"bead.ready", `{"limit":"invalid"}`, "invalid_params"},
	} {
		requireCode(t, call(t, s, ctx, tc.method, tc.params), tc.code)
	}
	cancelled, stop := context.WithCancel(rpc.WithPrincipal(ctx, model.PrincipalRef{Kind: "user", ID: "actual"}))
	stop()
	value, err := wire.DecodeJSON(s.Dispatch(cancelled, request("bead.ready", `{}`)))
	if err != nil {
		t.Fatal(err)
	}
	requireCode(t, value.(map[string]any), "busy")
	if lookups != 2 {
		t.Fatal("provider ran before request admission")
	}
	// With no provider, startup-only compositions retain their existing hello.
	static := server(t, registry(t), rpc.Config{Incarnation: "static-incarnation"})
	if result(t, call(t, static, context.Background(), "server.hello", `{}`))["incarnation"] != "static-incarnation" {
		t.Fatal("static composition changed")
	}
}

func TestTransportErrorsUseCanonicalEnvelope(t *testing.T) {
	r := registry(t)
	s := server(t, r, rpc.Config{})
	for _, code := range []string{"unauthenticated", "token_expired", "authn_unavailable", "invalid_request", "not_found", "busy"} {
		t.Run(code, func(t *testing.T) {
			raw := s.TransportError(fmt.Errorf("transport admission: %w", &model.Error{Data: model.ErrorData{Code: model.ErrorCode(code)}}))
			var got struct {
				JSONRPC string          `json:"jsonrpc"`
				ID      json.RawMessage `json:"id"`
				Error   wire.Error      `json:"error"`
			}
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			want := r.Catalog().NewError(code, "", nil)
			if got.JSONRPC != "2.0" || string(got.ID) != "null" || got.Error.Code != want.Code || got.Error.Message != want.Message || got.Error.Data.Code != code || got.Error.Data.Retryable != want.Data.Retryable || got.Error.Data.TraceID == "" {
				t.Fatalf("noncanonical transport error: %s", raw)
			}
		})
	}
	raw := s.TransportError(&wire.Error{Code: 123, Message: "private text", Data: wire.ErrorData{Code: "busy", TraceID: "transport-trace"}})
	v, err := wire.DecodeJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	requireCode(t, v.(map[string]any), "busy")
	if v.(map[string]any)["error"].(map[string]any)["data"].(map[string]any)["traceId"] != "transport-trace" || bytes.Contains(raw, []byte("private text")) {
		t.Fatalf("trace or sanitization changed: %s", raw)
	}
	raw = s.TransportError(errors.New("private transport failure"))
	v, err = wire.DecodeJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	requireCode(t, v.(map[string]any), "internal_error")
	if bytes.Contains(raw, []byte("private transport failure")) {
		t.Fatalf("transport failure leaked: %s", raw)
	}
}

func TestHandlerOutcomeKeepsResultBytesAndIDs(t *testing.T) {
	r := registry(t)
	calls := 0
	if err := r.Register("bead.create", func(context.Context, rpc.Call) (any, error) {
		calls++
		return rpc.Outcome{Result: commitResult(), Replayed: calls == 2}, nil
	}); err != nil {
		t.Fatal(err)
	}
	s := server(t, r, rpc.Config{})
	var original json.RawMessage
	for i := range 3 {
		raw := s.Dispatch(context.Background(), []byte(`{"jsonrpc":"2.0","id":1.0e1000,"method":"bead.create","params":{"type":"task","fields":{},"_meta":{"idempotencyKey":"550e8400-e29b-41d4-a716-446655440000"}}}`))
		var reply struct {
			ID     json.RawMessage `json:"id"`
			Result json.RawMessage `json:"result"`
			Meta   *struct {
				Replayed bool `json:"replayed"`
			} `json:"_meta"`
		}
		if err := json.Unmarshal(raw, &reply); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			original = reply.Result
		}
		if string(reply.ID) != "1.0e1000" || len(original) == 0 || !bytes.Equal(original, reply.Result) || (i == 1 && (reply.Meta == nil || !reply.Meta.Replayed)) || (i != 1 && reply.Meta != nil) {
			t.Fatalf("outcome changed ID/result or inferred replay from the repeated revision: %s", raw)
		}
	}
}

func TestOrderedBatchesAndWriteAdmission(t *testing.T) {
	r := registry(t)
	order := []string{}
	dropped := []string{}
	if err := r.Register("bead.create", func(ctx context.Context, c rpc.Call) (any, error) {
		order = append(order, "write:"+c.Meta.Agent)
		return commitResult(), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.Register("bead.get", func(context.Context, rpc.Call) (any, error) { order = append(order, "read"); return getResult(), nil }); err != nil {
		t.Fatal(err)
	}
	s := server(t, r, rpc.Config{DroppedNotification: func(ctx context.Context, method string) { dropped = append(dropped, method) }})
	key := `"_meta":{"idempotencyKey":"550e8400-e29b-41d4-a716-446655440000","agent":"worker"}`
	batch := []byte(`[
 {"jsonrpc":"2.0","id":"first","method":"bead.create","params":{"type":"task","fields":{},` + key + `}},
 {"jsonrpc":"2.0","method":"bead.create","params":{"type":"task","fields":{},` + key + `}},
 {"jsonrpc":"2.0","method":"bead.get","params":{"id":{"id":"p-1"}}},
 4,
 {"jsonrpc":"2.0","id":"last","method":"bead.get","params":{"id":{"id":"p-1"}}}
 ]`)
	raw := s.Dispatch(context.Background(), batch)
	var responses []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &responses); err != nil {
		t.Fatalf("%s: %v", raw, err)
	}
	if len(responses) != 3 || string(responses[0]["id"]) != `"first"` || string(responses[1]["id"]) != "null" || string(responses[2]["id"]) != `"last"` {
		t.Fatalf("response order: %s", raw)
	}
	if !reflect.DeepEqual(order, []string{"write:worker", "read", "read"}) || !reflect.DeepEqual(dropped, []string{"bead.create"}) {
		t.Fatalf("executions=%v dropped=%v", order, dropped)
	}
	for _, params := range []string{`{"type":"task","fields":{}}`, `{"type":"task","fields":{},"_meta":{"idempotencyKey":"w7-1"}}`, `{"type":"task","fields":{},"typo":1,` + key + `}`} {
		requireCode(t, call(t, s, context.Background(), "bead.create", params), "invalid_params")
	}
	if len(order) != 3 {
		t.Fatal("invalid writes reached handler")
	}
	item := `{"jsonrpc":"2.0","id":1,"method":"bead.get","params":{"id":{"id":"p-1"}}}`
	batch = []byte("[" + strings.TrimSuffix(strings.Repeat(item+",", 100), ",") + "]")
	if err := json.Unmarshal(s.Dispatch(context.Background(), batch), &responses); err != nil || len(responses) != 100 {
		t.Fatal("100-call boundary failed", err)
	}
	before := len(order)
	batch = []byte("[" + strings.TrimSuffix(strings.Repeat(item+",", 101), ",") + "]")
	v, _ := wire.DecodeJSON(s.Dispatch(context.Background(), batch))
	requireCode(t, v.(map[string]any), "invalid_request")
	if len(order) != before {
		t.Fatal("oversized batch partially executed")
	}
	if got := s.Dispatch(context.Background(), []byte(`[{"jsonrpc":"2.0","method":"bead.create"},{"jsonrpc":"2.0","method":"unknown"}]`)); got != nil {
		t.Fatalf("notification-only batch answered %s", got)
	}
}

func TestRegistryAndHandlerFailures(t *testing.T) {
	r := registry(t)
	if err := r.Register("not.implemented", func(context.Context, rpc.Call) (any, error) { return nil, nil }); err == nil {
		t.Fatal("unknown descriptor accepted")
	}
	if err := r.Register("rpc.rejected", func(context.Context, rpc.Call) (any, error) { return nil, nil }); err == nil {
		t.Fatal("server event callable")
	}
	if err := r.Register("bead.get", nil); err == nil {
		t.Fatal("nil handler accepted")
	}
	if err := r.Register("bead.get", func(context.Context, rpc.Call) (any, error) { return getResult(), nil }); err != nil {
		t.Fatal(err)
	}
	if err := r.Register("bead.get", func(context.Context, rpc.Call) (any, error) { return nil, nil }); err == nil {
		t.Fatal("duplicate handler accepted")
	}
	s := server(t, r, rpc.Config{})
	if err := r.Register("bead.ready", func(context.Context, rpc.Call) (any, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	requireCode(t, call(t, s, context.Background(), "bead.ready", `{}`), "method_not_found") // immutable server snapshot
	for _, handler := range []rpc.Handler{
		func(context.Context, rpc.Call) (any, error) { return nil, fmt.Errorf("private DSN") },
		func(context.Context, rpc.Call) (any, error) { panic("private panic") },
		func(context.Context, rpc.Call) (any, error) { return map[string]any{"success": true}, nil },
		func(context.Context, rpc.Call) (any, error) {
			return nil, &model.Error{Data: model.ErrorData{Code: "plugin_busy"}}
		},
	} {
		r := registry(t)
		_ = r.Register("bead.get", handler)
		s := server(t, r, rpc.Config{})
		response := call(t, s, context.Background(), "bead.get", `{"id":{"id":"p-1"}}`)
		requireCode(t, response, "internal_error")
		encoded, _ := json.Marshal(response)
		if bytes.Contains(encoded, []byte("private")) {
			t.Fatal("private failure leaked")
		}
	}
}

// The registry is the one shared admission point used by every transport.
// Keep its bound inventory tied to the authoritative catalog: callable
// descriptors must be accepted with their catalog metadata, while event-only
// descriptors must remain uncallable. This catches a transport-specific
// adapter accidentally inventing a method or exposing a notification as RPC.
func TestRegistryCoversCatalogCallableInventoryWithoutEvents(t *testing.T) {
	r := registry(t)
	for _, want := range r.Catalog().Descriptors() {
		name := want.Name
		err := r.Register(name, func(context.Context, rpc.Call) (any, error) {
			return map[string]any{}, nil
		})
		if want.Effect == "event" {
			if err == nil {
				t.Fatalf("event descriptor %q became callable", name)
			}
			continue
		}
		if err != nil {
			t.Fatalf("callable catalog descriptor %q rejected: %v", name, err)
		}
	}

	got := r.Descriptors()
	want := make([]wire.MethodDesc, 0)
	for _, descriptor := range r.Catalog().Descriptors() {
		if descriptor.Effect != "event" {
			want = append(want, descriptor)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("registry descriptors diverged from catalog callable inventory:\n got=%v\nwant=%v", got, want)
	}

	// The catalog comparison above must be paired with the shared dispatcher
	// admission check: a transport may bind a catalog method, but an
	// authenticated server must not reach its handler until the transport has
	// stamped a principal. Use valid read and write envelopes (including the
	// write idempotency key) so schema validation cannot mask that invariant.
	dispatchRegistry := registry(t)
	seen := map[string]int{}
	if err := dispatchRegistry.Register("bead.get", func(context.Context, rpc.Call) (any, error) {
		seen["bead.get"]++
		return getResult(), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := dispatchRegistry.Register("bead.create", func(context.Context, rpc.Call) (any, error) {
		seen["bead.create"]++
		return commitResult(), nil
	}); err != nil {
		t.Fatal(err)
	}
	authenticated := server(t, dispatchRegistry, rpc.Config{AuthMode: "authenticated"})
	requireCode(t, call(t, authenticated, context.Background(), "bead.get", `{"id":{"id":"p-1"}}`), "unauthenticated")
	requireCode(t, call(t, authenticated, context.Background(), "bead.create", `{"type":"task","fields":{},"_meta":{"idempotencyKey":"11111111111111111111111111111111"}}`), "unauthenticated")
	if seen["bead.get"] != 0 || seen["bead.create"] != 0 {
		t.Fatalf("unstamped protected calls reached handlers: %v", seen)
	}
	principal := model.PrincipalRef{Kind: "user", ID: "user:registry-guard"}
	result(t, call(t, authenticated, rpc.WithPrincipal(context.Background(), principal), "bead.get", `{"id":{"id":"p-1"}}`))
	result(t, call(t, authenticated, rpc.WithPrincipal(context.Background(), principal), "bead.create", `{"type":"task","fields":{},"_meta":{"idempotencyKey":"11111111111111111111111111111111"}}`))
	if seen["bead.get"] != 1 || seen["bead.create"] != 1 {
		t.Fatalf("stamped protected calls did not reach handlers: %v", seen)
	}
	result(t, call(t, authenticated, context.Background(), "server.hello", `{}`))
	discover := call(t, authenticated, context.Background(), "rpc.discover", `{}`)
	if _, ok := discover["result"]; !ok {
		t.Fatalf("public discovery exception was not available without a principal: %v", discover)
	}
}

func TestDecidedSessionCloseIsExplicitlyUnbound(t *testing.T) {
	r := registry(t)
	d, ok := r.Catalog().Descriptor("session.close")
	if !ok || d.Effect != "read" || d.Idempotency != "none" || d.Permission != "read" || d.AgentCore {
		t.Fatalf("decided session.close descriptor missing or changed: %+v", d)
	}
	s := server(t, r, rpc.Config{})
	requireCode(t, call(t, s, context.Background(), "session.close", `{"sessionId":"session-1"}`), "method_not_found")
	methods := result(t, call(t, s, context.Background(), "rpc.methods", `{}`))
	for _, method := range methods["methods"].([]any) {
		if method == "session.close" {
			t.Fatal("unimplemented session method advertised")
		}
	}
}

func TestDiscoveryVisibilityAndHash(t *testing.T) {
	r := registry(t)
	_ = r.Register("bead.get", func(context.Context, rpc.Call) (any, error) { return getResult(), nil })
	_ = r.Register("bead.create", func(context.Context, rpc.Call) (any, error) { return commitResult(), nil })
	s := server(t, r, rpc.Config{DefaultProject: "demo", OntologyRevision: model.Natural("9007199254740993"), Visible: func(ctx context.Context, by model.PrincipalRef, d wire.MethodDesc) (bool, error) {
		return d.Name != "bead.create" || by.ID == "writer", nil
	}})
	reader := rpc.WithPrincipal(context.Background(), model.PrincipalRef{Kind: "user", ID: "reader"})
	writer := rpc.WithPrincipal(context.Background(), model.PrincipalRef{Kind: "user", ID: "writer"})
	methods := result(t, call(t, s, reader, "rpc.methods", `{}`))
	hello := result(t, call(t, s, reader, "server.hello", `{}`))
	discover := result(t, call(t, s, reader, "rpc.discover", `{}`))
	if methods["methodsHash"] != hello["methodsHash"] || hello["methodsHash"] != discover["methodsHash"] {
		t.Fatal("discovery hashes disagree")
	}
	if hello["ontologyRevision"] != json.Number("9007199254740993") || hello["defaultProject"] != "demo" {
		t.Fatal("hello lost store metadata", hello)
	}
	if result(t, call(t, s, writer, "rpc.methods", `{}`))["methodsHash"] == methods["methodsHash"] {
		t.Fatal("hash ignored principal method visibility")
	}
	for _, m := range methods["methods"].([]any) {
		if m == "bead.create" || m == "bead.close" {
			t.Fatal("hidden/unbound method advertised")
		}
	}
	requireCode(t, call(t, s, reader, "rpc.describe", `{"method":"bead.create"}`), "method_not_found")
	requireCode(t, call(t, s, reader, "rpc.describe", `{"method":""}`), "method_not_found")
	description := result(t, call(t, s, reader, "rpc.describe", `{"method":"bead.get"}`))
	if description["method"].(map[string]any)["permission"] != "read" || len(description["errors"].([]any)) != 63 || description["components"] == nil {
		t.Fatal("incomplete description")
	}
	full, err := s.OpenRPC(reader, model.PrincipalRef{Kind: "user", ID: "reader"})
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]json.RawMessage
	_ = json.Unmarshal(full, &doc)
	if len(doc["components"]) == 0 {
		t.Fatal("OpenRPC missing referenced schemas")
	}
}

func TestAuthenticationAndDeadlineSeams(t *testing.T) {
	r := registry(t)
	var seen rpc.Call
	_ = r.Register("bead.get", func(ctx context.Context, c rpc.Call) (any, error) { seen = c; return getResult(), nil })
	s := server(t, r, rpc.Config{AuthMode: "authenticated"})
	requireCode(t, call(t, s, context.Background(), "bead.get", `{"id":{"id":"p-1"}}`), "unauthenticated")
	result(t, call(t, s, context.Background(), "server.hello", `{}`))
	result(t, call(t, s, context.Background(), "rpc.discover", `{}`))
	by := model.PrincipalRef{Kind: "user", ID: "actual"}
	ctx := rpc.WithPrincipal(context.Background(), by)
	result(t, call(t, s, ctx, "bead.get", `{"id":{"id":"p-1"},"_meta":{"agent":"asserted","deadlineMs":1e1000000000}}`))
	if seen.Principal.ID != "actual" || seen.Principal.Kind != "user" || seen.Meta.Agent != "asserted" {
		t.Fatal("client label replaced identity")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := inprocess.New(s).RoundTrip(cancelled, request("server.hello", `{}`)); !errors.Is(err, context.Canceled) {
		t.Fatal("transport lost cancellation", err)
	}
	r = registry(t)
	_ = r.Register("bead.get", func(ctx context.Context, c rpc.Call) (any, error) {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 5*time.Millisecond {
			t.Error("missing per-call deadline")
		}
		<-ctx.Done()
		return nil, &model.Error{Data: model.ErrorData{Code: "busy"}}
	})
	s = server(t, r, rpc.Config{})
	requireCode(t, call(t, s, context.Background(), "bead.get", `{"id":{"id":"p-1"},"_meta":{"deadlineMs":1}}`), "busy")
}

type serviceProbe struct {
	method  string
	by      model.PrincipalRef
	request any
	meta    model.WriteMeta
	failure error
}

func (p *serviceProbe) record(method string, by model.PrincipalRef, request any, meta model.WriteMeta) {
	p.method = method
	p.by = by
	p.request = request
	p.meta = meta
}
func (p *serviceProbe) Get(ctx context.Context, by model.PrincipalRef, r service.GetRequest) (service.GetResult, error) {
	p.record("get", by, r, model.WriteMeta{})
	return service.GetResult{Bead: service.Bead{ID: "p-1", Type: "task", Revision: model.Natural("9007199254740993")}, Lease: &service.Lease{Principal: by, TTL: time.Minute, Epoch: model.Natural64(7), ExpiresAt: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)}}, p.failure
}
func (p *serviceProbe) Ready(ctx context.Context, by model.PrincipalRef, r service.ReadyRequest) (service.BeadPage, error) {
	p.record("ready", by, r, model.WriteMeta{})
	return service.BeadPage{AsOf: service.AsOf{Revision: model.Natural("9007199254740993"), Incarnation: "inc"}}, p.failure
}
func (p *serviceProbe) History(ctx context.Context, by model.PrincipalRef, r service.HistoryRequest) (service.BeadPage, error) {
	p.record("history", by, r, model.WriteMeta{})
	return service.BeadPage{Items: []service.Bead{{ID: "p-1", Type: "task", Revision: r.AsOf.Revision}}, AsOf: r.AsOf}, p.failure
}
func (p *serviceProbe) CreateWithOutcome(ctx context.Context, by model.PrincipalRef, r service.CreateRequest, m model.WriteMeta) (port.WriteOutcome, error) {
	p.record("create", by, r, m)
	return port.WriteOutcome{Commit: model.Commit{Revision: model.Natural64(9), Warnings: []model.Warning{{Code: "post_subscriber_failed", Message: "sync will retry"}}}}, p.failure
}
func (p *serviceProbe) UpdateWithOutcome(ctx context.Context, by model.PrincipalRef, r service.UpdateRequest, m model.WriteMeta) (port.WriteOutcome, error) {
	p.record("update", by, r, m)
	return port.WriteOutcome{Commit: model.Commit{Revision: model.Natural64(10)}}, p.failure
}
func (p *serviceProbe) ClaimWithOutcome(ctx context.Context, by model.PrincipalRef, r service.ClaimRequest, m model.WriteMeta) (port.WriteOutcome, error) {
	p.record("claim", by, r, m)
	expires, ttl := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC), model.Natural64(30000)
	return port.WriteOutcome{Commit: model.Commit{Revision: model.Natural64(11), LeaseExpiresAt: &expires, LeaseTtlMs: &ttl}}, p.failure
}
func (p *serviceProbe) ClaimNextWithOutcome(ctx context.Context, by model.PrincipalRef, r service.ClaimNextRequest, m model.WriteMeta) (*port.WriteOutcome, error) {
	p.record("claimNext", by, r, m)
	expires, ttl := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC), model.Natural64(30000)
	return &port.WriteOutcome{Commit: model.Commit{Revision: model.Natural64(14), LeaseExpiresAt: &expires, LeaseTtlMs: &ttl}}, p.failure
}
func (p *serviceProbe) CommentWithOutcome(ctx context.Context, by model.PrincipalRef, r service.CommentRequest, m model.WriteMeta) (port.WriteOutcome, error) {
	p.record("comment", by, r, m)
	return port.WriteOutcome{Commit: model.Commit{Revision: model.Natural64(12)}}, p.failure
}
func (p *serviceProbe) CloseWithOutcome(ctx context.Context, by model.PrincipalRef, r service.CloseRequest, m model.WriteMeta) (port.WriteOutcome, error) {
	p.record("close", by, r, m)
	return port.WriteOutcome{Commit: model.Commit{Revision: model.Natural64(13)}}, p.failure
}
func (p *serviceProbe) ApplyWithOutcome(ctx context.Context, by model.PrincipalRef, r model.ChangeSet, m model.WriteMeta) (port.WriteOutcome, error) {
	p.record("apply", by, r, m)
	return port.WriteOutcome{Commit: model.Commit{Revision: model.Natural64(14)}}, p.failure
}
func (p *serviceProbe) LinkWithOutcome(ctx context.Context, by model.PrincipalRef, r service.LinkRequest, m model.WriteMeta) (port.WriteOutcome, error) {
	p.record("link", by, r, m)
	return port.WriteOutcome{Commit: model.Commit{Revision: model.Natural64(15)}}, p.failure
}
func (p *serviceProbe) UnlinkWithOutcome(ctx context.Context, by model.PrincipalRef, r service.LinkRequest, m model.WriteMeta) (port.WriteOutcome, error) {
	p.record("unlink", by, r, m)
	return port.WriteOutcome{Commit: model.Commit{Revision: model.Natural64(16)}}, p.failure
}
func (p *serviceProbe) ReopenWithOutcome(ctx context.Context, by model.PrincipalRef, r service.ReopenRequest, m model.WriteMeta) (port.WriteOutcome, error) {
	p.record("reopen", by, r, m)
	return port.WriteOutcome{Commit: model.Commit{Revision: model.Natural64(14)}}, p.failure
}
func (p *serviceProbe) DeferWithOutcome(ctx context.Context, by model.PrincipalRef, r service.DeferRequest, m model.WriteMeta) (port.WriteOutcome, error) {
	p.record("defer", by, r, m)
	return port.WriteOutcome{Commit: model.Commit{Revision: model.Natural64(15)}}, p.failure
}
func (p *serviceProbe) CancelWithOutcome(ctx context.Context, by model.PrincipalRef, r service.CancelRequest, m model.WriteMeta) (port.WriteOutcome, error) {
	p.record("cancel", by, r, m)
	return port.WriteOutcome{Commit: model.Commit{Revision: model.Natural64(16)}}, p.failure
}
func (p *serviceProbe) FailWithOutcome(ctx context.Context, by model.PrincipalRef, r service.FailRequest, m model.WriteMeta) (port.WriteOutcome, error) {
	p.record("fail", by, r, m)
	return port.WriteOutcome{Commit: model.Commit{Revision: model.Natural64(17)}}, p.failure
}

func TestServiceBindingsAtByteBoundary(t *testing.T) {
	p := &serviceProbe{}
	r := registry(t)
	if err := rpc.BindService(r, p); err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(r, p); err == nil {
		t.Fatal("duplicate service binding accepted")
	}
	s := server(t, r, rpc.Config{DefaultProject: "default", Incarnation: "inc"})
	ctx := rpc.WithPrincipal(context.Background(), model.PrincipalRef{Kind: "user", ID: "u-1"})
	got := result(t, call(t, s, ctx, "bead.get", `{"id":{"id":"p-1"},"asOf":{"revision":9007199254740993}}`))
	if p.request.(service.GetRequest).AsOf.Revision != model.Natural("9007199254740993") || got["bead"].(map[string]any)["revision"] != json.Number("9007199254740993") {
		t.Fatal("get mapping lost precision")
	}
	lease := got["lease"].(map[string]any)
	if lease["ttlMs"] != json.Number("60000") || lease["epoch"] != json.Number("7") || lease["holder"] != "u-1" {
		t.Fatal("lease mapping", lease)
	}
	got = result(t, call(t, s, ctx, "bead.ready", `{"limit":1.0e3,"viewId":"v","filter":{"types":["task"]}}`))
	ready := p.request.(service.ReadyRequest)
	if ready.Limit != 1000 || ready.Project != "default" || ready.ViewID != "v" || len(ready.Filter.Types) != 1 || len(got["items"].([]any)) != 0 {
		t.Fatal("ready mapping", ready, got)
	}
	result(t, call(t, s, ctx, "history.query", `{"asOf":{"revision":1e1000000000,"incarnation":"inc"},"limit":5,"_meta":{"project":"other"}}`))
	history := p.request.(service.HistoryRequest)
	if history.AsOf.Revision != model.Natural("1e1000000000") || history.Project != "other" || history.Cursor != "" {
		t.Fatal("history mapping", history)
	}
	meta := `"_meta":{"idempotencyKey":"550e8400-e29b-41d4-a716-446655440000","agent":"worker","return":"minimal","traceparent":"trace","dryRun":true}`
	for _, tc := range []struct{ method, params, domain string }{
		{"bead.create", `{"type":"task","fields":{"value":9007199254740993},"parent":{"id":"p"},` + meta + `}`, "create"},
		{"bead.update", `{"id":{"id":"p-1"},"fields":{"clear":null},"expect":[{"ref":{"id":"p-1"},"revision":1.0}],` + meta + `}`, "update"},
		{"bead.claim", `{"id":{"id":"p-1"},` + meta + `}`, "claim"},
		{"bead.comment", `{"id":{"id":"p-1"},"body":"evidence",` + meta + `}`, "comment"},
		{"bead.close", `{"id":{"id":"p-1"},"reason":"done",` + meta + `}`, "close"},
	} {
		got = result(t, call(t, s, ctx, tc.method, tc.params))
		if p.method != tc.domain || p.meta.Method != tc.method || p.meta.Project != "default" || p.meta.Agent != "worker" || p.meta.AgentLabel != "worker" || !p.meta.DryRun || p.meta.Return != "minimal" || p.meta.Traceparent != "trace" || p.by.ID != "u-1" {
			t.Fatalf("lost method/meta: %+v", p)
		}
		if tc.domain == "create" {
			if p.request.(service.CreateRequest).Fields["value"] != json.Number("9007199254740993") || len(got["warnings"].([]any)) != 1 {
				t.Fatal("create/warning mapping", got)
			}
		}
		if tc.domain == "update" {
			v := p.request.(service.UpdateRequest)
			if v.Fields["clear"] != nil || v.Expect[0].Revision == nil || *v.Expect[0].Revision != model.Natural64(1) {
				t.Fatal("patch/precondition mapping", v)
			}
		}
		if tc.domain == "claim" {
			if got["leaseTtlMs"] != json.Number("30000") || got["leaseExpiresAt"] != "2026-10-29T12:00:00Z" || got["commit"] != nil {
				t.Fatal("claim was not flattened", got)
			}
		}
	}
	for _, tc := range []struct {
		method string
		params string
		domain string
	}{
		{"bead.link", `{"from":{"id":"p-1"},"to":{"id":"p-2"},"kind":"blocks","expect":[{"ref":{"id":"p-1"},"revision":4}],` + meta + `}`, "link"},
		{"bead.unlink", `{"from":{"id":"p-1"},"to":{"id":"p-2"},"kind":"blocks",` + meta + `}`, "unlink"},
		{"tx.apply", `{"changeSet":{"ops":[{"kind":"link","from":{"id":"p-1"},"to":{"id":"p-2"},"linkKind":"relates-to"}]},` + meta + `}`, "apply"},
	} {
		got = result(t, call(t, s, ctx, tc.method, tc.params))
		if p.method != tc.domain || p.meta.Method != tc.method || p.meta.Project != "default" || p.by.ID != "u-1" {
			t.Fatalf("lost dependency/transaction mapping for %s: %+v", tc.method, p)
		}
		switch tc.domain {
		case "link", "unlink":
			request := p.request.(service.LinkRequest)
			if request.From.ID != "p-1" || request.To.ID != "p-2" || request.Kind != "blocks" || (tc.domain == "link" && len(request.Expect) != 1) || (tc.domain == "unlink" && len(request.Expect) != 0) {
				t.Fatalf("%s request mapping: %+v", tc.method, request)
			}
		case "apply":
			changeSet := p.request.(model.ChangeSet)
			if len(changeSet.Ops) != 1 || changeSet.Ops[0].Kind != model.OpLink || changeSet.Ops[0].Link == nil || changeSet.Ops[0].Link.LinkKind != "relates-to" {
				t.Fatalf("tx.apply request mapping: %+v", changeSet)
			}
		}
		if got["revision"] != json.Number(map[string]string{"link": "15", "unlink": "16", "apply": "14"}[tc.domain]) {
			t.Fatalf("unexpected %s result: %+v", tc.method, got)
		}
	}
	claimNext := result(t, call(t, s, ctx, "bead.claimNext", `{"filter":{"types":["task"]},`+meta+`}`))
	if p.method != "claimNext" || p.meta.Method != "bead.claimNext" || p.request.(service.ClaimNextRequest).Project != "default" || claimNext["leaseTtlMs"] != json.Number("30000") || claimNext["leaseExpiresAt"] != "2026-10-29T12:00:00Z" {
		t.Fatalf("claimNext mapping or flattened lease result changed: request=%+v result=%v", p.request, claimNext)
	}
	p.failure = &model.Error{Data: model.ErrorData{Code: "precondition_failed", Details: map[string]any{"reason": "revision"}}}
	requireCode(t, call(t, s, ctx, "bead.close", `{"id":{"id":"p-1"},`+meta+`}`), "precondition_failed")
}

func TestTransactionBindingCompositionsRemainExclusive(t *testing.T) {
	ctx := rpc.WithPrincipal(context.Background(), model.PrincipalRef{Kind: "user", ID: "u-1"})
	params := `{"changeSet":{"ops":[{"kind":"link","from":{"id":"p-1"},"to":{"id":"p-2"},"linkKind":"relates-to"}]},"_meta":{"idempotencyKey":"550e8400-e29b-41d4-a716-446655440000"}}`

	t.Run("standalone binding serves and full binding is atomic", func(t *testing.T) {
		probe := &serviceProbe{}
		registry := registry(t)
		if err := rpc.BindTxApply(registry, probe); err != nil {
			t.Fatal(err)
		}
		server := server(t, registry, rpc.Config{DefaultProject: "default", Incarnation: "inc"})
		got := result(t, call(t, server, ctx, "tx.apply", params))
		if probe.method != "apply" || got["revision"] != json.Number("14") {
			t.Fatalf("standalone tx.apply was not served: method=%q result=%v", probe.method, got)
		}
		before := registry.Descriptors()
		if err := rpc.BindService(registry, probe); err == nil {
			t.Fatal("full binding after standalone tx.apply was accepted")
		}
		if after := registry.Descriptors(); !reflect.DeepEqual(after, before) {
			t.Fatalf("failed full binding mutated descriptors: before=%v after=%v", before, after)
		}
	})

	t.Run("full binding rejects standalone duplicate atomically", func(t *testing.T) {
		probe := &serviceProbe{}
		registry := registry(t)
		if err := rpc.BindService(registry, probe); err != nil {
			t.Fatal(err)
		}
		before := registry.Descriptors()
		if err := rpc.BindTxApply(registry, probe); err == nil {
			t.Fatal("standalone tx.apply after full binding was accepted")
		}
		if after := registry.Descriptors(); !reflect.DeepEqual(after, before) {
			t.Fatalf("failed standalone binding mutated descriptors: before=%v after=%v", before, after)
		}
	})
}

func TestRealClaimNextTerminalReceiptReplayAtByteBoundary(t *testing.T) {
	s, post, _, _ := realServiceServer(t)
	ctx := context.Background()
	head := func() any {
		return result(t, call(t, s, ctx, "bead.ready", `{}`))["asOf"].(map[string]any)["revision"]
	}
	if head() != json.Number("0") {
		t.Fatal("fixture is not empty")
	}
	params := writeParams(t, map[string]any{"filter": map[string]any{"types": []string{"issue", "issue"}}})
	checkNull := func(replayed bool) {
		t.Helper()
		reply := call(t, s, ctx, "bead.claimNext", paramsJSON(t, params))
		if value, ok := reply["result"]; !ok || value != nil || reply["error"] != nil {
			t.Fatalf("expected terminal JSON null: %v", reply)
		}
		meta, present := reply["_meta"].(map[string]any)
		if replayed && (!present || meta["replayed"] != true) || !replayed && present {
			t.Fatalf("terminal null replay metadata (want %t): %v", replayed, reply)
		}
	}
	checkNull(false)
	if head() != json.Number("1") || post.calls != 0 {
		t.Fatal("empty receipt must advance H+1 without graph/post effects")
	}
	// The normalized selector replays the same durable receipt.
	params["filter"] = map[string]any{"types": []string{"issue"}}
	checkNull(true)
	if head() != json.Number("1") {
		t.Fatal("empty replay advanced head")
	}
	created := writeResult(t, s, "bead.create", writeParams(t, map[string]any{
		"type": "issue", "fields": map[string]any{"title": "Ready after the terminal receipt"},
	}), false)
	ref := createdRef(t, created)
	before, calls := head(), post.calls
	checkNull(true)
	ready := result(t, call(t, s, ctx, "bead.ready", `{}`))["items"].([]any)
	if head() != before || post.calls != calls || len(ready) != 1 || ready[0].(map[string]any)["id"] != ref["id"] {
		t.Fatal("empty replay claimed a later-ready bead or changed durable state")
	}
	params["filter"] = map[string]any{"types": []string{"task"}}
	requireCode(t, call(t, s, ctx, "bead.claimNext", paramsJSON(t, params)), "idempotency_key_reused")
	params["filter"] = map[string]any{"types": []string{"issue"}}
	params["viewId"] = "different-view"
	requireCode(t, call(t, s, ctx, "bead.claimNext", paramsJSON(t, params)), "idempotency_key_reused")
	delete(params, "viewId")
	if head() != before {
		t.Fatal("changed selector mutated the queue")
	}
	// A new key can claim the row that the earlier durable null must ignore.
	claimed := writeResult(t, s, "bead.claimNext", writeParams(t, map[string]any{"filter": map[string]any{"types": []string{"issue"}}}), false)
	if claimed["results"].([]any)[0].(map[string]any)["ref"].(map[string]any)["id"] != ref["id"] || claimed["leaseTtlMs"] != json.Number("300000") {
		t.Fatalf("real claimNext did not dispatch to the shared writer: %v", claimed)
	}
}
