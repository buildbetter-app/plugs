package httpconformance_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
)

func TestHTTPByteMatrixSupportsRealServiceCalls(t *testing.T) {
	f := newFixture(t)

	create := f.post(t, createRequest("9007199254740993", "00000000000000000000000000000001", "seed"))
	requireStatus(t, create, http.StatusOK)
	if got := responseID(t, create.body); got != "9007199254740993" {
		t.Fatalf("large numeric id was changed: %s", got)
	}
	refID := mustResultRefID(t, create.body)
	createResult := resultObject(t, create.body)
	createRevision, ok := createResult["revision"].(json.Number)
	if !ok || createRevision.String() == "" {
		t.Fatalf("create result has no lossless revision: %v", createResult)
	}

	get := f.post(t, readRequest(`"get\u002did"`, refID))
	requireStatus(t, get, http.StatusOK)
	if got := responseID(t, get.body); got != `"get\u002did"` {
		t.Fatalf("escaped string id was changed: %s", got)
	}
	bead, ok := resultObject(t, get.body)["bead"].(map[string]any)
	if !ok || bead["id"] != refID || bead["fields"].(map[string]any)["title"] != "seed" {
		t.Fatalf("real bead.get did not return the committed bead: %s", get.body)
	}

	ready := f.post(t, `{"jsonrpc":"2.0","id":1e3,"method":"bead.ready","params":{"limit":10}}`)
	requireStatus(t, ready, http.StatusOK)
	if got := responseID(t, ready.body); got != "1e3" {
		t.Fatalf("exponent id was changed: %s", got)
	}
	if items, ok := resultObject(t, ready.body)["items"].([]any); !ok || len(items) != 1 {
		t.Fatalf("real bead.ready did not return the created bead: %s", ready.body)
	}

	claim := f.post(t, writeRequest("bead.claim", `{"id":{"id":`+quote(refID)+`}}`, "00000000000000000000000000000002", `"claim"`))
	requireResult(t, claim)

	update := f.post(t, writeRequest("bead.update", `{"id":{"id":`+quote(refID)+`},"fields":{"title":"updated"}}`, "00000000000000000000000000000003", `"update"`))
	requireResult(t, update)

	comment := f.post(t, writeRequest("bead.comment", `{"id":{"id":`+quote(refID)+`},"body":"HTTP evidence"}`, "00000000000000000000000000000004", `"comment"`))
	requireResult(t, comment)

	closed := f.post(t, writeRequest("bead.close", `{"id":{"id":`+quote(refID)+`},"reason":"done"}`, "00000000000000000000000000000005", `"close"`))
	requireResult(t, closed)

	history := f.post(t, fmt.Sprintf(`{"jsonrpc":"2.0","id":"history","method":"history.query","params":{"asOf":{"revision":%s}}}`, createRevision.String()))
	requireResult(t, history)
	if items, ok := resultObject(t, history.body)["items"].([]any); !ok || len(items) != 1 {
		t.Fatalf("real history.query did not return the pinned bead: %s", history.body)
	}
}

func TestHTTPMixedBatchIsOrderedAndNotificationsDoNotWrite(t *testing.T) {
	f := newFixture(t)
	created := f.post(t, createRequest(`"seed"`, "10000000000000000000000000000001", "before"))
	requireResult(t, created)
	refID := mustResultRefID(t, created.body)
	before := f.state(t)

	batch := fmt.Sprintf(`[
{"jsonrpc":"2.0","id":"before","method":"bead.get","params":{"id":{"id":%s}}},
{"jsonrpc":"2.0","id":7,"method":"bead.update","params":{"id":{"id":%s},"fields":{"title":"after"},"_meta":{"idempotencyKey":"10000000000000000000000000000002"}}},
{"jsonrpc":"2.0","id":"after","method":"bead.get","params":{"id":{"id":%s}}},
{"jsonrpc":"2.0","id":99,"method":"method.unknown","params":{}}
]`, quote(refID), quote(refID), quote(refID))
	mixed := f.post(t, batch)
	requireStatus(t, mixed, http.StatusOK)
	items := batchResponses(t, mixed.body)
	if len(items) != 4 {
		t.Fatalf("mixed batch returned %d responses, want 4: %s", len(items), mixed.body)
	}
	wantIDs := []string{`"before"`, `7`, `"after"`, `99`}
	for i, want := range wantIDs {
		if got := responseID(t, items[i]); got != want {
			t.Fatalf("batch response %d id=%s want=%s: %s", i, got, want, mixed.body)
		}
	}
	if got := resultObject(t, items[1]); got["revision"] == nil {
		t.Fatalf("ordered write response had no commit: %s", items[1])
	}
	beforeBead := resultObject(t, items[0])["bead"].(map[string]any)
	afterBead := resultObject(t, items[2])["bead"].(map[string]any)
	if beforeBead["fields"].(map[string]any)["title"] != "before" || afterBead["fields"].(map[string]any)["title"] != "after" {
		t.Fatalf("batch calls were not executed in wire order: %s", mixed.body)
	}
	if got := errorCode(t, items[3]); got != "method_not_found" {
		t.Fatalf("unknown method in batch returned %q", got)
	}
	after := f.state(t)
	if after.changesets != before.changesets+1 || after.writes != before.writes+1 || after.head == before.head {
		t.Fatalf("mixed batch did not make exactly one real write: before=%+v after=%+v", before, after)
	}

	readNotification := f.post(t, fmt.Sprintf(`{"jsonrpc":"2.0","method":"bead.get","params":{"id":{"id":%s}}}`, quote(refID)))
	if readNotification.status != http.StatusNoContent || len(readNotification.body) != 0 {
		t.Fatalf("read notification was not an empty HTTP response: status=%d body=%s", readNotification.status, readNotification.body)
	}
	noWriteBefore := f.state(t)
	writeNotification := f.post(t, writeRequest("bead.comment", fmt.Sprintf(`{"id":{"id":%s},"body":"dropped"}`, quote(refID)), "10000000000000000000000000000003", ""))
	if writeNotification.status != http.StatusNoContent || len(writeNotification.body) != 0 {
		t.Fatalf("write notification was not dropped without a response: status=%d body=%s", writeNotification.status, writeNotification.body)
	}
	f.assertNoMutation(t, noWriteBefore)

	batchNotification := f.post(t, fmt.Sprintf(`[
{"jsonrpc":"2.0","method":"bead.comment","params":{"id":{"id":%s},"body":"also dropped","_meta":{"idempotencyKey":"10000000000000000000000000000004"}}},
{"jsonrpc":"2.0","id":"survivor","method":"bead.get","params":{"id":{"id":%s}}}
]`, quote(refID), quote(refID)))
	items = batchResponses(t, batchNotification.body)
	if len(items) != 1 || responseID(t, items[0]) != `"survivor"` {
		t.Fatalf("batch notification did not retain only the read response: %s", batchNotification.body)
	}
	f.assertNoMutation(t, noWriteBefore)
}

func TestHTTPRefusalsAreStrictAndDurablyReadOnly(t *testing.T) {
	f := newFixture(t)
	cases := []struct {
		name   string
		body   string
		token  string
		admit  bool
		status int
		code   string
	}{
		{name: "parse error", body: `{"jsonrpc":"2.0","id":1,"method":"bead.ready"`, admit: true, status: http.StatusOK, code: "parse_error"},
		{name: "duplicate member", body: `{"jsonrpc":"2.0","id":1,"method":"bead.ready","params":{},"params":{}}`, admit: true, status: http.StatusOK, code: "invalid_request"},
		{name: "invalid id type", body: `{"jsonrpc":"2.0","id":[],"method":"bead.ready","params":{}}`, admit: true, status: http.StatusOK, code: "invalid_request"},
		{name: "empty batch", body: `[]`, admit: true, status: http.StatusOK, code: "invalid_request"},
		{name: "unknown method", body: `{"jsonrpc":"2.0","id":1,"method":"method.unknown","params":{}}`, admit: true, status: http.StatusOK, code: "method_not_found"},
		{name: "unknown write parameter", body: `{"jsonrpc":"2.0","id":1,"method":"bead.create","params":{"type":"issue","fields":{"title":"refused"},"extra":true,"_meta":{"idempotencyKey":"20000000000000000000000000000001"}}}`, admit: true, status: http.StatusOK, code: "invalid_params"},
		{name: "unknown metadata parameter", body: `{"jsonrpc":"2.0","id":1,"method":"bead.create","params":{"type":"issue","fields":{"title":"refused"},"_meta":{"idempotencyKey":"20000000000000000000000000000002","extra":true}}}`, admit: true, status: http.StatusOK, code: "invalid_params"},
		{name: "array parameters", body: `{"jsonrpc":"2.0","id":1,"method":"bead.ready","params":[]}`, admit: true, status: http.StatusOK, code: "invalid_params"},
		{name: "missing credential", body: `{"jsonrpc":"2.0","id":1,"method":"bead.create","params":{"type":"issue","fields":{"title":"unauthenticated"},"_meta":{"idempotencyKey":"20000000000000000000000000000003"}}}`, status: http.StatusUnauthorized, code: "unauthenticated"},
		{name: "invalid credential", body: `{"jsonrpc":"2.0","id":1,"method":"bead.create","params":{"type":"issue","fields":{"title":"unauthenticated"},"_meta":{"idempotencyKey":"20000000000000000000000000000004"}}}`, token: "not-a-valid-key", status: http.StatusUnauthorized, code: "unauthenticated"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			before := f.state(t)
			token := test.token
			if test.admit {
				token = f.token
			}
			r := f.postWithToken(t, test.body, token)
			requireRPCError(t, r, test.status, test.code)
			f.assertNoMutation(t, before)
		})
	}
}

func createRequest(id, key, title string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"method":"bead.create","params":{"type":"issue","fields":{"title":%s},"_meta":{"idempotencyKey":%s}}}`, id, quote(title), quote(key))
}

func readRequest(id, refID string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"method":"bead.get","params":{"id":{"id":%s}}}`, id, quote(refID))
}

func writeRequest(method, params, key, id string) string {
	idPart := ""
	if id != "" {
		idPart = `,"id":` + id
	}
	return fmt.Sprintf(`{"jsonrpc":"2.0"%s,"method":%s,"params":%s}`, idPart, quote(method), addMeta(params, key))
}

func addMeta(params, key string) string {
	if key == "" {
		return params
	}
	return params[:len(params)-1] + `,"_meta":{"idempotencyKey":` + quote(key) + `}}`
}

func quote(value string) string {
	data, _ := json.Marshal(value)
	return string(data)
}

func requireResult(t *testing.T, r reply) {
	t.Helper()
	requireStatus(t, r, http.StatusOK)
	if envelope := object(t, r.body); envelope["error"] != nil {
		t.Fatalf("RPC call failed: %s", r.body)
	}
	if _, ok := object(t, r.body)["result"]; !ok {
		t.Fatalf("RPC response has no result: %s", r.body)
	}
}

func requireStatus(t *testing.T, r reply, want int) {
	t.Helper()
	if r.status != want {
		t.Fatalf("HTTP status=%d want=%d body=%s", r.status, want, r.body)
	}
}

func requireRPCError(t *testing.T, r reply, status int, code string) {
	t.Helper()
	requireStatus(t, r, status)
	if got := errorCode(t, r.body); got != code {
		t.Fatalf("RPC error code=%q want=%q: %s", got, code, r.body)
	}
}

func responseID(t *testing.T, body []byte) string {
	t.Helper()
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("response is not an object: %v (%s)", err, body)
	}
	id, ok := envelope["id"]
	if !ok {
		t.Fatalf("response has no id: %s", body)
	}
	return string(id)
}

func batchResponses(t *testing.T, body []byte) []json.RawMessage {
	t.Helper()
	var items []json.RawMessage
	if err := json.Unmarshal(body, &items); err != nil {
		t.Fatalf("batch response is not an array: %v (%s)", err, body)
	}
	return items
}
