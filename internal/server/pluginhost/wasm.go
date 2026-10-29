package pluginhost

// This file is the deliberately small mode-2 adapter. A WASI guest has no
// Go callback values to register, so the adapter translates the same public
// pluginapi messages used by the native host into the two-export ABI from the
// runtime design. The guest receives only JSON; its only host import is
// `plug.call`, and the module is instantiated without preopened descriptors,
// environment variables, or a network implementation.

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

const (
	maxWASMMessage = 1 << 20
	maxWASMPages   = 1024 // 64 MiB per guest, including any memory growth.
	wasmModuleName = "plug"
	wasmImportName = "call"
)

// NewWASMNative adapts a verified wasip1 artifact to the existing pluginhost
// registration seam. The bytes are copied before they are retained so callers
// cannot mutate an admitted artifact after its digest was checked.
func NewWASMNative(wasm []byte, manifest pluginapi.PluginManifest, instance string) (sdk.Native, error) {
	if manifest.Runtime != pluginapi.RuntimeWASI {
		return sdk.Native{}, failure(ErrManifest, "WASM registration requires runtime wasip1")
	}
	if !pluginapi.ValidName(instance) {
		return sdk.Native{}, failure(ErrManifest, "invalid WASM instance")
	}
	if err := ValidateWASMArtifact(wasm); err != nil {
		return sdk.Native{}, err
	}
	copyBytes := append([]byte(nil), wasm...)
	return sdk.Native{
		Manifest: manifest,
		Instance: instance,
		New: func(host pluginapi.Host) (sdk.Registration, error) {
			runtime, err := newWASMRuntime(copyBytes, host)
			if err != nil {
				return sdk.Registration{}, err
			}
			return runtime.registration(manifest), nil
		},
	}, nil
}

type wasmRuntime struct {
	mu      sync.Mutex
	runtime wazero.Runtime
	module  api.Module
	alloc   api.Function
	handle  api.Function
	memory  api.Memory
	host    pluginapi.Host
	closed  bool
}

func newWASMRuntime(wasm []byte, host pluginapi.Host) (*wasmRuntime, error) {
	ctx := context.Background()
	runtime := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfigInterpreter().WithCloseOnContextDone(true).WithMemoryLimitPages(maxWASMPages))
	if _, err := wasi_snapshot_preview1.Instantiate(ctx, runtime); err != nil {
		_ = runtime.Close(ctx)
		return nil, failure(ErrManifest, "WASI runtime setup failed: %v", err)
	}
	// The import is intentionally narrow. It is currently a JSON-RPC escape
	// hatch for future host calls; the guest cannot obtain a socket, file or
	// environment capability by importing another module.
	if _, err := runtime.NewHostModuleBuilder(wasmModuleName).
		NewFunctionBuilder().WithFunc(func(context.Context, api.Module, uint32, uint32) uint64 { return 0 }).
		Export(wasmImportName).Instantiate(ctx); err != nil {
		_ = runtime.Close(ctx)
		return nil, failure(ErrManifest, "WASM host import setup failed: %v", err)
	}
	compiled, err := runtime.CompileModule(ctx, wasm)
	if err != nil {
		_ = runtime.Close(ctx)
		return nil, failure(ErrManifest, "WASM compile failed: %v", err)
	}
	if err := validateCompiledWASM(compiled); err != nil {
		_ = runtime.Close(ctx)
		return nil, err
	}
	module, err := runtime.InstantiateModule(ctx, compiled, wazero.NewModuleConfig().WithStartFunctions())
	if err != nil {
		_ = runtime.Close(ctx)
		return nil, failure(ErrManifest, "WASM instantiate failed: %v", err)
	}
	result := &wasmRuntime{runtime: runtime, module: module, alloc: module.ExportedFunction("plug_alloc"), handle: module.ExportedFunction("plug_handle"), memory: module.Memory(), host: host}
	if result.alloc == nil || result.handle == nil || result.memory == nil {
		result.close()
		return nil, failure(ErrManifest, "WASM module does not expose the required ABI")
	}
	// c-shared wasip1 modules expose `_initialize` instead of an exiting
	// `_start`. Calling it once initializes the Go scheduler before any ABI
	// calls, while the module remains resident for the host lifetime.
	if initialize := module.ExportedFunction("_initialize"); initialize != nil {
		if _, err := initialize.Call(ctx); err != nil {
			result.close()
			return nil, failure(ErrManifest, "WASM runtime initialization failed: %v", err)
		}
	}
	return result, nil
}

func (r *wasmRuntime) close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	r.mu.Unlock()
	_ = r.module.Close(context.Background())
	_ = r.runtime.Close(context.Background())
}

type wasmRequest struct {
	Op      string `json:"op"`
	Name    string `json:"name,omitempty"`
	Payload any    `json:"payload,omitempty"`
}

type wasmResponse struct {
	Result  json.RawMessage    `json:"result,omitempty"`
	Patches []pluginapi.Patch  `json:"patches,omitempty"`
	Verdict *pluginapi.Verdict `json:"verdict,omitempty"`
	Error   string             `json:"error,omitempty"`
}

func (r *wasmRuntime) invoke(ctx context.Context, request wasmRequest) (wasmResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return wasmResponse{}, failure(ErrInactive, "WASM module is closed")
	}
	if err := ctx.Err(); err != nil {
		return wasmResponse{}, err
	}
	data, err := json.Marshal(request)
	if err != nil || len(data) > maxWASMMessage {
		return wasmResponse{}, failure(ErrMethod, "WASM request exceeds the message limit")
	}
	ptr, err := r.alloc.Call(ctx, uint64(len(data)))
	if err != nil || len(ptr) != 1 || ptr[0] == 0 || ptr[0] > uint64(^uint32(0)) {
		return wasmResponse{}, failure(ErrMethod, "WASM allocation failed: %v", err)
	}
	input := uint32(ptr[0])
	if ok := r.memory.Write(input, data); !ok {
		return wasmResponse{}, failure(ErrMethod, "WASM input is outside guest memory")
	}
	packed, err := r.handle.Call(ctx, uint64(input), uint64(len(data)))
	if err != nil || len(packed) != 1 {
		return wasmResponse{}, failure(ErrMethod, "WASM handler failed: %v", err)
	}
	outputPtr, outputLen := uint32(packed[0]>>32), uint32(packed[0])
	if outputLen > maxWASMMessage {
		return wasmResponse{}, failure(ErrMethod, "WASM response exceeds the message limit")
	}
	output, ok := r.memory.Read(outputPtr, outputLen)
	if !ok || !json.Valid(output) {
		return wasmResponse{}, failure(ErrMethod, "WASM response is not valid JSON")
	}
	var response wasmResponse
	if err := json.Unmarshal(output, &response); err != nil {
		return wasmResponse{}, failure(ErrMethod, "WASM response envelope is invalid: %v", err)
	}
	if response.Error != "" {
		return wasmResponse{}, fmt.Errorf("WASM plugin: %s", response.Error)
	}
	return response, nil
}

func (r *wasmRuntime) registration(manifest pluginapi.PluginManifest) sdk.Registration {
	registration := sdk.Registration{Mutators: map[string]pluginapi.Mutator{}, Validators: map[string]pluginapi.Validator{}, Posts: map[string]pluginapi.PostSubscriber{}}
	for _, provision := range manifest.Provides {
		switch provision.Kind {
		case "subscriber":
			for _, phase := range provision.Phases {
				name := provision.Name
				switch phase {
				case pluginapi.PhaseMutate:
					registration.Mutators[name] = wasmMutator{runtime: r, name: name}
				case pluginapi.PhaseValidate:
					registration.Validators[name] = wasmValidator{runtime: r, name: name}
				case pluginapi.PhasePost:
					registration.Posts[name] = wasmPost{runtime: r, name: name}
				}
			}
		case "methods":
			for suffix, grant := range provision.Methods {
				name := provision.Prefix + "." + suffix
				registration.Methods = append(registration.Methods, wasmMethod(r, name, grant))
			}
		}
	}
	return registration
}

type wasmMutator struct {
	runtime *wasmRuntime
	name    string
}

func (m wasmMutator) Mutate(ctx context.Context, change pluginapi.ProposedChange, _ pluginapi.SnapshotReader) ([]pluginapi.Patch, error) {
	response, err := m.runtime.invoke(ctx, wasmRequest{Op: "mutate", Name: m.name, Payload: change})
	return response.Patches, err
}

type wasmValidator struct {
	runtime *wasmRuntime
	name    string
}

func (v wasmValidator) Validate(ctx context.Context, change pluginapi.ProposedChange, _ pluginapi.SnapshotReader) (pluginapi.Verdict, error) {
	response, err := v.runtime.invoke(ctx, wasmRequest{Op: "validate", Name: v.name, Payload: change})
	if err != nil {
		return pluginapi.Verdict{}, err
	}
	if response.Verdict == nil {
		return pluginapi.Verdict{}, failure(ErrMethod, "WASM validator returned no verdict")
	}
	return *response.Verdict, nil
}

type wasmPost struct {
	runtime *wasmRuntime
	name    string
}

func (p wasmPost) After(ctx context.Context, change pluginapi.CommittedChange, _ pluginapi.SnapshotReader) error {
	_, err := p.runtime.invoke(ctx, wasmRequest{Op: "post", Name: p.name, Payload: change})
	return err
}

func wasmMethod(runtime *wasmRuntime, name string, grant pluginapi.MethodGrant) sdk.Method {
	return sdk.Method{Descriptor: pluginapi.Method{
		Name: name, Permission: grant.Permission,
		Params: map[string]any{"type": "object"}, Result: map[string]any{"type": "object"},
		Resource: func(params any) []pluginapi.Ref {
			value, ok := wasmLookup(params, strings.TrimPrefix(grant.Resource, "params."))
			if !ok {
				return nil
			}
			if id, ok := value.(string); ok {
				return []pluginapi.Ref{pluginapi.RefByID(id, "")}
			}
			data, err := json.Marshal(value)
			if err != nil {
				return nil
			}
			var ref pluginapi.Ref
			if json.Unmarshal(data, &ref) != nil {
				return nil
			}
			return []pluginapi.Ref{ref}
		},
	}, Handle: func(ctx context.Context, params json.RawMessage) (json.RawMessage, error) {
		response, err := runtime.invoke(ctx, wasmRequest{Op: "method", Name: name, Payload: json.RawMessage(append([]byte(nil), params...))})
		if err != nil {
			return nil, err
		}
		if !json.Valid(response.Result) {
			return nil, failure(ErrMethod, "WASM method returned no valid result")
		}
		return append(json.RawMessage(nil), response.Result...), nil
	}}
}

func wasmLookup(value any, path string) (any, bool) {
	for _, part := range strings.Split(path, ".") {
		object, ok := value.(map[string]any)
		if !ok {
			return nil, false
		}
		value, ok = object[part]
		if !ok {
			return nil, false
		}
	}
	return value, true
}

func validateCompiledWASM(module wazero.CompiledModule) error {
	exports := module.ExportedFunctions()
	for name, function := range exports {
		if name != "plug_alloc" && name != "plug_handle" && name != "_start" && name != "_initialize" {
			return failure(ErrManifest, "WASM export %q is outside the host ABI", name)
		}
		if name == "plug_alloc" && (len(function.ParamTypes()) != 1 || len(function.ResultTypes()) != 1 || function.ParamTypes()[0] != api.ValueTypeI32 || function.ResultTypes()[0] != api.ValueTypeI32) {
			return failure(ErrManifest, "plug_alloc has the wrong ABI")
		}
		if name == "plug_handle" && (len(function.ParamTypes()) != 2 || len(function.ResultTypes()) != 1 || function.ParamTypes()[0] != api.ValueTypeI32 || function.ParamTypes()[1] != api.ValueTypeI32 || function.ResultTypes()[0] != api.ValueTypeI64) {
			return failure(ErrManifest, "plug_handle has the wrong ABI")
		}
	}
	if module.ExportedFunctions()["plug_alloc"] == nil || module.ExportedFunctions()["plug_handle"] == nil {
		return failure(ErrManifest, "WASM exports must include plug_alloc and plug_handle")
	}
	memories := module.ExportedMemories()
	memory, ok := memories["memory"]
	if !ok || len(memories) != 1 {
		return failure(ErrManifest, "WASM exports must include exactly one memory named memory")
	}
	if memory.Min() > maxWASMPages {
		return failure(ErrManifest, "WASM memory minimum exceeds %d pages", maxWASMPages)
	}
	if max, ok := memory.Max(); ok && max > maxWASMPages {
		return failure(ErrManifest, "WASM memory maximum exceeds %d pages", maxWASMPages)
	}
	return nil
}

// ValidateWASMArtifact performs the static portion of the mode-2 admission
// check. Wazero validates all core instructions and function signatures; this
// parser adds the security-relevant module/name and memory checks that the
// public wazero import definition intentionally does not expose.
func ValidateWASMArtifact(data []byte) error {
	if err := validateWASMBytes(data); err != nil {
		return err
	}
	image, err := parseWASM(data)
	if err != nil {
		return failure(ErrManifest, "WASM static validation failed: %v", err)
	}
	if image.memoryCount != 1 || !image.memoryExport {
		return failure(ErrManifest, "WASM must export exactly one memory named memory")
	}
	if image.memoryMin > maxWASMPages || image.memoryMax > maxWASMPages {
		return failure(ErrManifest, "WASM memory exceeds %d pages", maxWASMPages)
	}
	if image.plugCallType >= 0 && !image.plugCallABI {
		return failure(ErrManifest, "plug.call has the wrong ABI")
	}
	ctx := context.Background()
	rt := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfigInterpreter().WithCloseOnContextDone(true).WithMemoryLimitPages(maxWASMPages))
	defer rt.Close(ctx)
	compiled, err := rt.CompileModule(ctx, data)
	if err != nil {
		return failure(ErrManifest, "WASM compile failed: %v", err)
	}
	defer compiled.Close(ctx)
	return validateCompiledWASM(compiled)
}

func validateWASMBytes(data []byte) error {
	if len(data) < 8 || string(data[:4]) != "\x00asm" || binary.LittleEndian.Uint32(data[4:8]) != 1 {
		return failure(ErrManifest, "WASM artifact has an invalid header")
	}
	return nil
}

type wasmImage struct {
	memoryCount  int
	memoryMin    uint32
	memoryMax    uint32
	memoryExport bool
	plugCallType int
	plugCallABI  bool
	types        []wasmFuncType
}

type wasmFuncType struct {
	params  []byte
	results []byte
}

func (t wasmFuncType) isPlugCall() bool {
	return len(t.params) == 2 && len(t.results) == 1 && t.params[0] == 0x7f && t.params[1] == 0x7f && t.results[0] == 0x7e
}

func parseWASM(data []byte) (wasmImage, error) {
	image := wasmImage{plugCallType: -1}
	pos := 8
	for pos < len(data) {
		section := data[pos]
		pos++
		size, err := readWASMUint(data, &pos)
		if err != nil || size > uint32(len(data)-pos) {
			return image, fmt.Errorf("invalid section length")
		}
		end := pos + int(size)
		payload := data[pos:end]
		pos = end
		switch section {
		case 1:
			if err := parseWASMTypes(payload, &image); err != nil {
				return image, err
			}
		case 2:
			if err := parseWASMImports(payload, &image); err != nil {
				return image, err
			}
		case 5:
			if err := parseWASMMemories(payload, &image); err != nil {
				return image, err
			}
		case 7:
			if err := parseWASMExports(payload, &image); err != nil {
				return image, err
			}
		}
	}
	if image.plugCallType >= 0 {
		if image.plugCallType >= len(image.types) {
			return image, fmt.Errorf("plug.call references a missing type")
		}
		image.plugCallABI = image.types[image.plugCallType].isPlugCall()
	}
	return image, nil
}

func parseWASMTypes(payload []byte, image *wasmImage) error {
	pos := 0
	count, err := readWASMUint(payload, &pos)
	if err != nil {
		return fmt.Errorf("invalid type section")
	}
	image.types = make([]wasmFuncType, 0, count)
	for i := uint32(0); i < count; i++ {
		if pos >= len(payload) || payload[pos] != 0x60 {
			return fmt.Errorf("unsupported type form")
		}
		pos++
		params, err := readWASMValTypes(payload, &pos)
		if err != nil {
			return err
		}
		results, err := readWASMValTypes(payload, &pos)
		if err != nil {
			return err
		}
		image.types = append(image.types, wasmFuncType{params: params, results: results})
	}
	if pos != len(payload) {
		return fmt.Errorf("trailing type section bytes")
	}
	return nil
}

func readWASMValTypes(payload []byte, pos *int) ([]byte, error) {
	count, err := readWASMUint(payload, pos)
	if err != nil || count > uint32(len(payload)-*pos) {
		return nil, fmt.Errorf("invalid value type vector")
	}
	result := append([]byte(nil), payload[*pos:*pos+int(count)]...)
	*pos += int(count)
	for _, value := range result {
		switch value {
		case 0x7f, 0x7e, 0x7d, 0x7c:
		default:
			return nil, fmt.Errorf("unsupported value type 0x%x", value)
		}
	}
	return result, nil
}

func parseWASMImports(payload []byte, image *wasmImage) error {
	pos := 0
	count, err := readWASMUint(payload, &pos)
	if err != nil {
		return fmt.Errorf("invalid import section")
	}
	for i := uint32(0); i < count; i++ {
		module, err := readWASMString(payload, &pos)
		if err != nil {
			return err
		}
		name, err := readWASMString(payload, &pos)
		if err != nil {
			return err
		}
		if module != "wasi_snapshot_preview1" && !(module == wasmModuleName && name == wasmImportName) {
			return fmt.Errorf("import %s.%s is not allowed", module, name)
		}
		if pos >= len(payload) {
			return fmt.Errorf("truncated import kind")
		}
		kind := payload[pos]
		pos++
		switch kind {
		case 0:
			typeIndex, err := readWASMUint(payload, &pos)
			if err != nil {
				return fmt.Errorf("invalid function import")
			}
			if module == wasmModuleName {
				if image.plugCallType >= 0 {
					return fmt.Errorf("duplicate plug.call import")
				}
				image.plugCallType = int(typeIndex)
			}
		case 1:
			return fmt.Errorf("table imports are not allowed")
		case 2:
			return fmt.Errorf("memory imports are not allowed")
		case 3:
			return fmt.Errorf("global imports are not allowed")
		default:
			return fmt.Errorf("unsupported import kind %d", kind)
		}
	}
	if pos != len(payload) {
		return fmt.Errorf("trailing import section bytes")
	}
	return nil
}

func parseWASMMemories(payload []byte, image *wasmImage) error {
	pos := 0
	count, err := readWASMUint(payload, &pos)
	if err != nil || count > 1 {
		return fmt.Errorf("WASM must define at most one memory")
	}
	if count == 1 {
		image.memoryCount++
		min, max, err := readWASMLimits(payload, &pos)
		if err != nil {
			return err
		}
		image.memoryMin, image.memoryMax = min, max
	}
	if pos != len(payload) {
		return fmt.Errorf("trailing memory section bytes")
	}
	return nil
}

func parseWASMExports(payload []byte, image *wasmImage) error {
	pos := 0
	count, err := readWASMUint(payload, &pos)
	if err != nil {
		return fmt.Errorf("invalid export section")
	}
	for i := uint32(0); i < count; i++ {
		name, err := readWASMString(payload, &pos)
		if err != nil {
			return err
		}
		if pos >= len(payload) {
			return fmt.Errorf("truncated export kind")
		}
		kind := payload[pos]
		pos++
		if _, err := readWASMUint(payload, &pos); err != nil {
			return fmt.Errorf("invalid export index")
		}
		switch name {
		case "memory":
			if kind != 2 {
				return fmt.Errorf("memory export has the wrong kind")
			}
			image.memoryExport = true
		case "plug_alloc", "plug_handle", "_start", "_initialize":
			if kind != 0 {
				return fmt.Errorf("ABI export %s has the wrong kind", name)
			}
		default:
			return fmt.Errorf("WASM export %q is outside the host ABI", name)
		}
	}
	if pos != len(payload) {
		return fmt.Errorf("trailing export section bytes")
	}
	return nil
}

func readWASMLimits(payload []byte, pos *int) (uint32, uint32, error) {
	if *pos >= len(payload) {
		return 0, 0, fmt.Errorf("truncated memory limits")
	}
	flags := payload[*pos]
	(*pos)++
	if flags != 0 && flags != 1 {
		return 0, 0, fmt.Errorf("unsupported memory limits")
	}
	min, err := readWASMUint(payload, pos)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid memory minimum")
	}
	max := uint32(0)
	if flags == 1 {
		max, err = readWASMUint(payload, pos)
		if err != nil || max < min {
			return 0, 0, fmt.Errorf("invalid memory maximum")
		}
	}
	return min, max, nil
}

func readWASMString(payload []byte, pos *int) (string, error) {
	length, err := readWASMUint(payload, pos)
	if err != nil || length > uint32(len(payload)-*pos) {
		return "", fmt.Errorf("invalid WASM name")
	}
	value := string(payload[*pos : *pos+int(length)])
	*pos += int(length)
	return value, nil
}

func readWASMUint(payload []byte, pos *int) (uint32, error) {
	var value uint32
	for shift := uint(0); shift < 35; shift += 7 {
		if *pos >= len(payload) {
			return 0, fmt.Errorf("truncated varuint")
		}
		byteValue := payload[*pos]
		(*pos)++
		if shift == 28 && byteValue > 0x0f {
			return 0, fmt.Errorf("varuint overflow")
		}
		value |= uint32(byteValue&0x7f) << shift
		if byteValue&0x80 == 0 {
			return value, nil
		}
	}
	return 0, fmt.Errorf("varuint overflow")
}
