package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

func TestCandidateABIUsesOnlyTwoFunctionExportsAndOneHostCall(t *testing.T) {
	ctx := context.Background()
	r := wazero.NewRuntime(ctx)
	defer r.Close(ctx)
	compiled, err := r.CompileModule(ctx, CandidateModule())
	if err != nil {
		t.Fatal(err)
	}
	defer compiled.Close(ctx)
	if got := len(compiled.ImportedFunctions()); got != 1 {
		t.Fatalf("imports=%d, want one plug.call import", got)
	}
	moduleName, functionName, ok := compiled.ImportedFunctions()[0].Import()
	if !ok || moduleName != RequiredImport.Module || functionName != RequiredImport.Name {
		t.Fatalf("import=%s.%s present=%v", moduleName, functionName, ok)
	}
	definition := compiled.ExportedFunctions()
	if len(definition) != 2 {
		t.Fatalf("function exports=%v, want exactly two", definition)
	}
	for _, name := range RequiredExports {
		if _, ok := definition[name]; !ok {
			t.Fatalf("missing required export %q", name)
		}
	}
	if _, ok := compiled.ExportedMemories()["memory"]; !ok {
		t.Fatal("candidate does not export memory")
	}

	var calls atomic.Int32
	_, err = r.NewHostModuleBuilder(RequiredImport.Module).
		NewFunctionBuilder().WithFunc(func(_ context.Context, module api.Module, ptr, length uint32) uint64 {
		calls.Add(1)
		message, ok := module.Memory().Read(ptr, length)
		if !ok {
			panic("host read out of bounds")
		}
		message = append([]byte(nil), message...)
		if err := ValidateMessage(message); err != nil {
			panic(err)
		}
		var request map[string]any
		if err := json.Unmarshal(message, &request); err != nil || request["method"] != "x.echo" {
			panic(fmt.Sprintf("unexpected JSON-RPC request %s", message))
		}
		response := []byte(`{"jsonrpc":"2.0","id":1,"result":{"ok":true}}`)
		if !module.Memory().Write(ptr, response) {
			panic("host response out of bounds")
		}
		return PackResult(ptr, uint32(len(response)))
	}).Export(RequiredImport.Name).Instantiate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	module, err := r.InstantiateModule(ctx, compiled, wazero.NewModuleConfig().WithStartFunctions())
	if err != nil {
		t.Fatal(err)
	}
	defer module.Close(ctx)
	alloc := module.ExportedFunction("plug_alloc")
	handle := module.ExportedFunction("plug_handle")
	pointer, err := alloc.Call(ctx, 1024)
	if err != nil {
		t.Fatal(err)
	}
	request := []byte(`{"jsonrpc":"2.0","id":1,"method":"x.echo","params":{"value":"ok"}}`)
	ptr := uint32(pointer[0])
	if !module.Memory().Write(ptr, request) {
		t.Fatal("request did not fit in candidate memory")
	}
	packed, err := handle.Call(ctx, uint64(ptr), uint64(len(request)))
	if err != nil {
		t.Fatal(err)
	}
	responsePtr, responseLength := UnpackResult(packed[0])
	response, ok := module.Memory().Read(responsePtr, responseLength)
	if !ok {
		t.Fatal("response pointer was not guest memory")
	}
	if string(response) != `{"jsonrpc":"2.0","id":1,"result":{"ok":true}}` {
		t.Fatalf("response=%s", response)
	}
	if calls.Load() != 1 {
		t.Fatalf("host calls=%d, want one", calls.Load())
	}
	t.Logf("ABI candidate executed plug_alloc -> plug_handle -> env.plug.call with %d-byte response", len(response))
}

func TestCandidateABIFailsClosedOnHostErrorAndOversizedFrames(t *testing.T) {
	ctx := context.Background()
	if _, err := EncodeFrame([]byte("not-json")); err == nil {
		t.Fatal("invalid JSON frame accepted")
	}
	frame, err := EncodeFrame([]byte(`{"jsonrpc":"2.0","id":1}`))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeFrame(frame)
	if err != nil || string(decoded) != `{"jsonrpc":"2.0","id":1}` {
		t.Fatalf("decoded=%s err=%v", decoded, err)
	}
	if _, err := DecodeFrame(append(frame, 0)); err == nil {
		t.Fatal("trailing frame bytes accepted")
	}
	tooLarge := make([]byte, MaxMessageBytes+1)
	tooLarge[0] = '{'
	if err := ValidateMessage(tooLarge); err == nil {
		t.Fatal("oversized message accepted")
	}

	r := wazero.NewRuntime(ctx)
	defer r.Close(ctx)
	compiled, err := r.CompileModule(ctx, CandidateModule())
	if err != nil {
		t.Fatal(err)
	}
	defer compiled.Close(ctx)
	_, err = r.NewHostModuleBuilder(RequiredImport.Module).
		NewFunctionBuilder().WithFunc(func(context.Context, api.Module, uint32, uint32) uint64 {
		panic("egress denied in hot-path host call")
	}).Export(RequiredImport.Name).Instantiate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	module, err := r.InstantiateModule(ctx, compiled, wazero.NewModuleConfig().WithStartFunctions())
	if err != nil {
		t.Fatal(err)
	}
	defer module.Close(ctx)
	pointer, err := module.ExportedFunction("plug_alloc").Call(ctx, 32)
	if err != nil {
		t.Fatal(err)
	}
	request := []byte(`{"jsonrpc":"2.0","id":1}`)
	ptr := uint32(pointer[0])
	if !module.Memory().Write(ptr, request) {
		t.Fatal("request did not fit")
	}
	if _, err := module.ExportedFunction("plug_handle").Call(ctx, uint64(ptr), uint64(len(request))); err == nil {
		t.Fatal("host error was converted into a successful ABI call")
	}
	if !slices.Equal(RequiredExports[:], []string{"plug_alloc", "plug_handle"}) {
		t.Fatal("required export contract changed unexpectedly")
	}
}
