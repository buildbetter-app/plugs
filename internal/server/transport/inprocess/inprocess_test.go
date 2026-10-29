package inprocess_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
)

func TestByteBoundary(t *testing.T) {
	r, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	s, err := rpc.New(r, rpc.Config{Incarnation: "test-store", DefaultProject: "demo"})
	if err != nil {
		t.Fatal(err)
	}
	transport := inprocess.New(s)
	for _, id := range []string{`9007199254740993`, `1234567890123456789012345678901234567890`, `1e1000000000`, `-2.50e+6`, `"a\u0062<c>"`, `null`} {
		request := []byte(`{"jsonrpc":"2.0","id":` + id + `,"method":"server.hello"}`)
		response, err := transport.RoundTrip(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.HasPrefix(response, []byte(`{"jsonrpc":"2.0","id":`+id+`,"result":`)) {
			t.Fatalf("id changed or call failed: %s", response)
		}
	}
	for _, tc := range []struct{ request, code string }{
		{`{`, "parse_error"},
		{`[]`, "invalid_request"},
		{`null`, "invalid_request"},
		{`{"jsonrpc":"1.0","id":1,"method":"server.hello"}`, "invalid_request"},
		{`{"jsonrpc":"2.0","id":true,"method":"server.hello"}`, "invalid_request"},
		{`{"jsonrpc":"2.0","id":1,"method":"bead.create","params":{"type":"issue","fields":{}}}`, "method_not_found"},
		{`{"jsonrpc":"2.0","id":1,"method":"server.hello","params":{"unexpected":1}}`, "invalid_params"},
		{`{"jsonrpc":"2.0","id":1,"method":"rpc.methods","params":{"_meta":{"principal":"admin"}}}`, "invalid_params"},
		{`{"jsonrpc":"2.0","id":1,"method":"rpc.methods","params":{"_meta":{"replayed":true}}}`, "invalid_params"},
		{`{"jsonrpc":"2.0","id":1,"method":"rpc.methods","params":[]}`, "invalid_params"},
	} {
		response, err := transport.RoundTrip(context.Background(), []byte(tc.request))
		if err != nil {
			t.Fatal(err)
		}
		var got struct {
			Error struct {
				Data struct {
					Code string `json:"code"`
				} `json:"data"`
			} `json:"error"`
		}
		if err = json.Unmarshal(response, &got); err != nil || got.Error.Data.Code != tc.code {
			t.Errorf("%s: got %s, expected %s (%v)", tc.request, response, tc.code, err)
		}
	}
	response, err := transport.RoundTrip(context.Background(), []byte(`{"jsonrpc":"2.0","method":"server.hello"}`))
	if err != nil || response != nil {
		t.Fatalf("notification: %s, %v", response, err)
	}
}
