package rpc_test

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
)

//go:embed openrpc.json
var generatedOpenRPC []byte

func TestGeneratedOpenRPCDeterminism(t *testing.T) {
	s := server(t, registry(t), rpc.Config{})
	by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	first, err := s.OpenRPC(context.Background(), by)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.OpenRPC(context.Background(), by)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("generator is nondeterministic")
	}
	if !bytes.Equal(first, generatedOpenRPC) {
		t.Fatal("OpenRPC artifact stale; run bazel run //internal/server/rpc/cmd/openrpcgen -- -out <checkout>/internal/server/rpc/openrpc.json")
	}
	var doc struct {
		Methods []struct {
			Name string `json:"name"`
		} `json:"methods"`
	}
	if err = json.Unmarshal(first, &doc); err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, m := range doc.Methods {
		names = append(names, m.Name)
	}
	if !reflect.DeepEqual(names, []string{"rpc.describe", "rpc.discover", "rpc.methods", "server.hello"}) {
		t.Fatal("artifact advertises unsupported handlers", names)
	}
}
