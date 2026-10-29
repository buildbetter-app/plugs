package graphacceptance_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
)

func TestGraphUnboundPublicMethodMatrix(t *testing.T) {
	f := newGraphFixture(t, "memory")
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, f.domain); err != nil {
		t.Fatal(err)
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{
		Incarnation: meta.Incarnation, DefaultProject: graphProject, OntologyRevision: model.Natural64(1),
		CursorKey: []byte("graph-acceptance-cursor-key-32-bytes!!"),
	})
	if err != nil {
		t.Fatal(err)
	}

	bound := map[string]bool{"server.hello": true, "rpc.methods": true, "rpc.describe": true, "rpc.discover": true}
	for _, descriptor := range registry.Descriptors() {
		bound[descriptor.Name] = true
	}
	before := f.state(t)
	unsupported := 0
	for _, descriptor := range registry.Catalog().Descriptors() {
		if bound[descriptor.Name] {
			continue
		}
		unsupported++
		t.Run(descriptor.Name, func(t *testing.T) {
			request := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":%q,"params":{}}`, descriptor.Name))
			response := server.Dispatch(context.Background(), request)
			var envelope struct {
				JSONRPC string          `json:"jsonrpc"`
				ID      json.RawMessage `json:"id"`
				Error   *wire.Error     `json:"error"`
				Result  json.RawMessage `json:"result"`
			}
			if err := json.Unmarshal(response, &envelope); err != nil {
				t.Fatalf("%s returned malformed RPC response %q: %v", descriptor.Name, response, err)
			}
			if envelope.JSONRPC != "2.0" || string(envelope.ID) != "1" || envelope.Error == nil || envelope.Error.Data.Code != "method_not_found" || len(envelope.Result) != 0 {
				t.Fatalf("unbound %s was not an explicit method_not_found: %s", descriptor.Name, response)
			}
			if err := registry.Catalog().ValidateError(envelope.Error); err != nil {
				t.Fatalf("unbound %s returned an invalid catalog error: %v", descriptor.Name, err)
			}
			t.Logf("unsupported public matrix entry: %s -> method_not_found", descriptor.Name)
		})
	}
	if unsupported == 0 {
		t.Fatal("catalog unexpectedly had no unbound methods")
	}
	requireSameState(t, before, f.state(t))
	t.Logf("unsupported matrix: %d catalog methods explicitly refused as method_not_found; bound set=%d", unsupported, len(bound))
}
