package pluginhost

// This file is the deliberately small mode-2 adapter. A WASI guest has no
// Go callback values to register, so the adapter translates the same public
// pluginapi messages used by the native host into the two-export ABI from the
// runtime design. The guest receives only JSON; its only host import is
// `plug.call`, and the module is instantiated without preopened descriptors,
// environment variables, or a network implementation.

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

const (
	maxWASMMessage  = 1 << 20
	maxWASMPages    = 1024 // 64 MiB per guest, including any memory growth.
	wasmInitTimeout = time.Second
	// A reader may ignore its context. Retired guests therefore keep their
	// reader call accounted for until it actually returns. The cap is aligned
	// with the four live pool members, so cancellation cannot create an
	// unbounded set of retained reader workers across guest generations.
	maxWASMReaderWorkers = maxRuntimeInstances
	// The E5 ABI names the single host import env.plug.call. Keep the
	// dotted function name intact so static admission and wazero instantiation
	// enforce the same import contract.
	wasmModuleName = "env"
	wasmImportName = "plug.call"
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
	factory := &wasmFactory{wasm: copyBytes, modules: map[api.Module]*wasmRuntime{}}
	return sdk.Native{
		Manifest: manifest,
		Instance: instance,
		New: func(host pluginapi.Host) (sdk.Registration, error) {
			runtime, err := factory.newRuntime(context.Background(), host)
			if err != nil {
				return sdk.Registration{}, err
			}
			if capabilities, ok := host.(*capabilities); ok {
				capabilities.onDispose = runtime.close
			}
			return runtime.registration(manifest), nil
		},
		NewWithContext: func(ctx context.Context, host pluginapi.Host) (sdk.Registration, error) {
			runtime, err := factory.newRuntime(ctx, host)
			if err != nil {
				return sdk.Registration{}, err
			}
			if capabilities, ok := host.(*capabilities); ok {
				capabilities.onDispose = runtime.close
			}
			return runtime.registration(manifest), nil
		},
	}, nil
}

// wasmFactory owns one compiled immutable module image. Each call to
// newRuntime instantiates a distinct module in the same wazero runtime, so
// linear memory, globals, allocators and invocation capabilities remain
// private while compilation/link setup is paid only once.
type wasmFactory struct {
	mu            sync.Mutex
	wasm          []byte
	runtime       wazero.Runtime
	compiled      wazero.CompiledModule
	modules       map[api.Module]*wasmRuntime
	constructing  int
	readerSlots   chan struct{}
	readerWorkers int
}

// wasmTimer bridges the context-aware lifetime of a guest call to WASI's
// context-free nanosleep hook. The hook is installed once per module, while
// the active wait is replaced for instantiation, initialization, and each
// callback invocation. Explicit module retirement also cancels the active
// wait, so closing a runtime cannot leave a poll sleeping on a guest-selected
// duration.
type wasmTimer struct {
	mu          sync.Mutex
	active      *wasmTimerWait
	retired     bool
	retiredDone chan struct{}
}

type wasmTimerWait struct {
	ctx       context.Context
	observer  func(context.Context)
	abort     chan struct{}
	abortOnce sync.Once
}

func (w *wasmTimerWait) cancel() {
	w.abortOnce.Do(func() { close(w.abort) })
}

func (t *wasmTimer) begin(ctx context.Context, observer func(context.Context)) *wasmTimerWait {
	wait := &wasmTimerWait{ctx: ctx, observer: observer, abort: make(chan struct{})}
	t.mu.Lock()
	previous := t.active
	t.active = wait
	retired := t.retired
	t.mu.Unlock()
	if previous != nil {
		previous.cancel()
	}
	if retired {
		wait.cancel()
	}
	return wait
}

func (t *wasmTimer) end(wait *wasmTimerWait) {
	if wait == nil {
		return
	}
	t.mu.Lock()
	if t.active == wait {
		t.active = nil
	}
	t.mu.Unlock()
}

// retirementSignal is allocated only for the callback pre-arm observer.
func (t *wasmTimer) retirementSignal() <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.retiredDone == nil {
		t.retiredDone = make(chan struct{})
		if t.retired {
			close(t.retiredDone)
		}
	}
	return t.retiredDone
}

func (t *wasmTimer) cancel() {
	t.mu.Lock()
	if !t.retired {
		t.retired = true
		if t.retiredDone != nil {
			close(t.retiredDone)
		}
	}
	wait := t.active
	t.mu.Unlock()
	if wait != nil {
		wait.cancel()
	}
}

func (t *wasmTimer) nanosleep(ns int64) {
	if ns <= 0 {
		return
	}
	t.mu.Lock()
	wait := t.active
	t.mu.Unlock()
	if wait == nil {
		panic(failure(ErrInactive, "WASM timer is outside an active guest call"))
	}
	timer := time.NewTimer(time.Duration(ns))
	defer timer.Stop()
	if wait.observer != nil {
		wait.observer(wait.ctx)
	}
	select {
	case <-timer.C:
	case <-wait.ctx.Done():
		panic(wait.ctx.Err())
	case <-wait.abort:
		// Waking a poll alone lets a guest with a live context poll again
		// while close waits for its invocation mutex. Terminate the active
		// host call before module resources can be retired.
		panic(context.Canceled)
	}
}

type wasmRuntime struct {
	mu      sync.Mutex
	factory *wasmFactory
	runtime wazero.Runtime
	module  api.Module
	alloc   api.Function
	handle  api.Function
	memory  api.Memory
	host    pluginapi.Host
	timer   *wasmTimer
	closed  bool
}

// wasmInvocationKey binds the callback's already-scoped reader to the wazero
// execution context. The import receives that context while the guest is in
// plug_handle; it never receives a storage handle or a caller-selected
// revision. A reader is present only for a host-driven callback, so method
// calls and initialization retain the existing explicit capability refusal.
type wasmInvocationKey struct{}

type wasmInvocation struct {
	reader pluginapi.SnapshotReader
}

// wasmInitializerObserverKey is an internal acceptance-test seam. It carries
// no guest capability; it only lets a source-built test observe a zero-length
// import reached from the real guest initializer without relying on elapsed
// time.
type wasmInitializerObserverKey struct{}

type wasmInitializerContextObserverKey struct{}

type wasmInitializerTimerWaitObserverKey struct{}

type wasmInitializationErrorObserverKey struct{}

type wasmCallbackTimerWaitObserverKey struct{}

type wasmCallbackTimerBeginObserverKey struct{}

type wasmInitializerCallKey struct{}

// WithWASMInitializerObserver returns a context that observes a deterministic
// source-built WASI guest initializer barrier. This is intentionally scoped to
// the internal pluginhost package and has no wire or public plugin effect.
func WithWASMInitializerObserver(ctx context.Context, observer func()) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if observer == nil {
		return ctx
	}
	return context.WithValue(ctx, wasmInitializerObserverKey{}, observer)
}

// WithWASMInitializerContextObserver observes the actual guest initializer
// execution context at the same internal zero-length import barrier.
func WithWASMInitializerContextObserver(ctx context.Context, observer func(context.Context)) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if observer == nil {
		return ctx
	}
	return context.WithValue(ctx, wasmInitializerContextObserverKey{}, observer)
}

// WithWASMInitializerTimerWaitObserver observes positive WASI timer waits
// entered during module instantiation or initialization with the timed init context.
func WithWASMInitializerTimerWaitObserver(ctx context.Context, observer func(context.Context)) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if observer == nil {
		return ctx
	}
	return context.WithValue(ctx, wasmInitializerTimerWaitObserverKey{}, observer)
}

// WithWASMInitializationErrorObserver observes the actual constructor parent
// context and runtime result before initialization error classification.
func WithWASMInitializationErrorObserver(ctx context.Context, observer func(context.Context, error)) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if observer == nil {
		return ctx
	}
	return context.WithValue(ctx, wasmInitializationErrorObserverKey{}, observer)
}

// WithWASMCallbackTimerWaitObserver observes positive WASI timer waits
// entered by a guest callback with its active invocation context.
func WithWASMCallbackTimerWaitObserver(ctx context.Context, observer func(context.Context)) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if observer == nil {
		return ctx
	}
	return context.WithValue(ctx, wasmCallbackTimerWaitObserverKey{}, observer)
}

// WithWASMCallbackTimerBeginObserver observes callback timer arming with the
// actual invocation context and the timer's permanent retirement signal.
func WithWASMCallbackTimerBeginObserver(ctx context.Context, observer func(context.Context, <-chan struct{})) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if observer == nil {
		return ctx
	}
	return context.WithValue(ctx, wasmCallbackTimerBeginObserverKey{}, observer)
}

// wasmReadRequest is the deliberately small internal E5 envelope. The
// `params` value is the canonical JSON representation of the corresponding
// pluginapi.SnapshotReader operation:
//
//	{"op":"read","method":"get","params":{"refs":[...]}}
//	{"op":"read","method":"query","params":{"filter":...,"cursor":...,"limit":...}}
//
// The host returns {"result": <canonical value>} or {"error": <refusal>}.
// This is an internal ABI envelope, not a new public plugin API. The reader
// itself remains responsible for field grants, budgets, pinned revisions,
// principal/binding scope and read-fact tracking.
type wasmReadRequest struct {
	Op     string          `json:"op"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type wasmReadRefs struct {
	Refs []pluginapi.Ref `json:"refs"`
}

type wasmReadClosure struct {
	Ref       pluginapi.Ref `json:"ref"`
	Ancestors bool          `json:"ancestors"`
}

type wasmReadGrants struct {
	Subjects  []pluginapi.SubjectRef `json:"subjects"`
	Resources []pluginapi.Ref        `json:"resources"`
}

func newWASMRuntime(wasm []byte, host pluginapi.Host) (*wasmRuntime, error) {
	factory := &wasmFactory{wasm: append([]byte(nil), wasm...), modules: map[api.Module]*wasmRuntime{}}
	return factory.newRuntime(context.Background(), host)
}

func (f *wasmFactory) ensureLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.runtime != nil && f.compiled != nil {
		return nil
	}
	// The factory owns this shared runtime. Its owner context is stable while
	// the caller context still bounds setup, linking and compilation below.
	runtime := wazero.NewRuntimeWithConfig(context.Background(), wazero.NewRuntimeConfig().WithCloseOnContextDone(true).WithMemoryLimitPages(maxWASMPages))
	if _, err := wasi_snapshot_preview1.Instantiate(ctx, runtime); err != nil {
		_ = runtime.Close(context.Background())
		return failure(ErrManifest, "WASI runtime setup failed: %v", err)
	}
	// The import is linked once for the factory. It resolves the concrete
	// module back to its wasmRuntime, preserving per-guest readers and buffers.
	if _, err := runtime.NewHostModuleBuilder(wasmModuleName).
		NewFunctionBuilder().WithFunc(f.hostCall).
		Export(wasmImportName).Instantiate(ctx); err != nil {
		_ = runtime.Close(context.Background())
		return failure(ErrManifest, "WASM host import setup failed: %v", err)
	}
	compiled, err := runtime.CompileModule(ctx, f.wasm)
	if err != nil {
		_ = runtime.Close(context.Background())
		return failure(ErrManifest, "WASM compile failed: %v", err)
	}
	if err := validateCompiledWASM(compiled); err != nil {
		_ = compiled.Close(context.Background())
		_ = runtime.Close(context.Background())
		return err
	}
	f.runtime, f.compiled = runtime, compiled
	return nil
}

func closeWASMResources(compiled wazero.CompiledModule, runtime wazero.Runtime) {
	if compiled != nil {
		_ = compiled.Close(context.Background())
	}
	if runtime != nil {
		_ = runtime.Close(context.Background())
	}
}

func (f *wasmFactory) detachResourcesLocked() (wazero.CompiledModule, wazero.Runtime) {
	if f.constructing != 0 || len(f.modules) != 0 {
		return nil, nil
	}
	compiled, runtime := f.compiled, f.runtime
	f.compiled, f.runtime = nil, nil
	return compiled, runtime
}

func (f *wasmFactory) endConstruction() {
	f.mu.Lock()
	f.constructing--
	compiled, runtime := f.detachResourcesLocked()
	f.mu.Unlock()
	closeWASMResources(compiled, runtime)
}

func (f *wasmFactory) newRuntime(ctx context.Context, host pluginapi.Host) (*wasmRuntime, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.constructing++
	if err := f.ensureLocked(ctx); err != nil {
		f.constructing--
		f.mu.Unlock()
		return nil, err
	}
	runtime, compiled := f.runtime, f.compiled
	f.mu.Unlock()
	defer f.endConstruction()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	initCtx, cancel := context.WithTimeout(ctx, wasmInitTimeout)
	defer cancel()
	timer := &wasmTimer{}
	initObserver, _ := initCtx.Value(wasmInitializerTimerWaitObserverKey{}).(func(context.Context))
	initWait := timer.begin(initCtx, initObserver)
	defer timer.end(initWait)
	moduleConfig := wazero.NewModuleConfig().WithStartFunctions().WithSysNanotime().WithNanosleep(timer.nanosleep)
	module, err := runtime.InstantiateModule(initCtx, compiled, moduleConfig)
	if err != nil {
		return nil, wasmInitializationError(ctx, initCtx, "WASM instantiate failed", err)
	}
	result := &wasmRuntime{factory: f, runtime: runtime, module: module, host: host, timer: timer}
	result.alloc = module.ExportedFunction("plug_alloc")
	result.handle = module.ExportedFunction("plug_handle")
	result.memory = module.Memory()
	if result.alloc == nil || result.handle == nil || result.memory == nil {
		result.close()
		return nil, failure(ErrManifest, "WASM module does not expose the required ABI")
	}
	f.mu.Lock()
	f.modules[module] = result
	f.mu.Unlock()
	// c-shared wasip1 modules expose `_initialize` instead of an exiting
	// `_start`. Calling it once initializes the Go scheduler before any ABI
	// calls, while the module remains resident for the host lifetime.
	if initialize := module.ExportedFunction("_initialize"); initialize != nil {
		initCtx = context.WithValue(initCtx, wasmInitializerCallKey{}, struct{}{})
		initDone := make(chan error, 1)
		go func() {
			_, err := initialize.Call(initCtx)
			initDone <- err
		}()
		select {
		case err := <-initDone:
			if err != nil {
				result.close()
				return nil, wasmInitializationError(ctx, initCtx, "WASM runtime initialization failed", err)
			}
			if err := wasmInitializationError(ctx, initCtx, "WASM runtime initialization failed", nil); err != nil {
				result.close()
				return nil, err
			}
		case <-initCtx.Done():
			// The timer bridge unwinds active WASI host waits, while wazero's
			// WithCloseOnContextDone terminates guest execution. Join before
			// resource closure so cleanup cannot replace a genuine guest trap
			// with a zero-exit termination result.
			timer.cancel()
			err := <-initDone
			result.close()
			return nil, wasmInitializationError(ctx, initCtx, "WASM runtime initialization failed", err)
		}
	}
	return result, nil
}

// wasmInitializationError keeps cancellation-shaped termination separate from
// genuine guest failures. A caller or owner cancellation is returned as the
// original context error; the private one-second initializer timeout remains a
// manifest error so queue pressure cannot disguise a real guest failure.
func wasmInitializationError(ctx, initCtx context.Context, label string, err error) error {
	if observer, _ := ctx.Value(wasmInitializationErrorObserverKey{}).(func(context.Context, error)); observer != nil {
		observer(ctx, err)
	}
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return failure(ErrManifest, "%s: %v", label, err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if initErr := initCtx.Err(); initErr != nil {
		return failure(ErrManifest, "%s: %v", label, initErr)
	}
	if err != nil {
		return failure(ErrManifest, "%s: %v", label, err)
	}
	return nil
}

// acquireReaderWorker reserves one factory-wide slot for a reader call. A
// canceled callback can return while its reader is still blocked, so the slot
// remains held until the worker's defer runs. This bounds retention across
// retired guest instances instead of treating maxRuntimeInstances as a worker
// limit. When all slots are retained, refuse immediately as typed queue
// pressure; waiting for the callback deadline would turn a retryable busy
// condition into a caller cancellation and can deadlock guest retirement.
func (f *wasmFactory) acquireReaderWorker(ctx context.Context) (func(), error) {
	f.mu.Lock()
	if f.readerSlots == nil {
		f.readerSlots = make(chan struct{}, maxWASMReaderWorkers)
	}
	slots := f.readerSlots
	f.mu.Unlock()
	select {
	case slots <- struct{}{}:
		f.mu.Lock()
		f.readerWorkers++
		f.mu.Unlock()
		var once sync.Once
		return func() {
			once.Do(func() {
				<-slots
				f.mu.Lock()
				f.readerWorkers--
				f.mu.Unlock()
			})
		}, nil
	default:
		return nil, poolBusy(ctx)
	}
}

func (f *wasmFactory) hostCall(ctx context.Context, module api.Module, ptr, length uint32) uint64 {
	f.mu.Lock()
	runtime := f.modules[module]
	f.mu.Unlock()
	if runtime == nil {
		panic("WASM guest is not initialized")
	}
	return runtime.hostCall(ctx, module, ptr, length)
}

func (r *wasmRuntime) close() {
	if r.timer != nil {
		r.timer.cancel()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closeLocked()
}

func (r *wasmRuntime) closeLocked() {
	if r.timer != nil {
		r.timer.cancel()
	}
	if r.closed {
		return
	}
	r.closed = true
	if r.module != nil {
		module := r.module
		r.module = nil
		var compiled wazero.CompiledModule
		var runtime wazero.Runtime
		if r.factory != nil {
			r.factory.mu.Lock()
			delete(r.factory.modules, module)
			compiled, runtime = r.factory.detachResourcesLocked()
			r.factory.mu.Unlock()
		}
		_ = module.Close(context.Background())
		closeWASMResources(compiled, runtime)
	}
}

type wasmRequest struct {
	Op      string          `json:"op"`
	Name    string          `json:"name,omitempty"`
	Payload any             `json:"payload,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type wasmResponse struct {
	Result  json.RawMessage    `json:"result,omitempty"`
	Patches []pluginapi.Patch  `json:"patches,omitempty"`
	Verdict *pluginapi.Verdict `json:"verdict,omitempty"`
	Error   string             `json:"error,omitempty"`
}

type wasmReadOutcome struct {
	value      any
	err        error
	panicValue any
}

// hostCall is the env.plug.call implementation. Invalid guest memory or
// malformed envelopes are traps: the surrounding invoke call disposes of the
// module, preserving the E5 fail-closed rule. Typed reader refusals are
// returned as an explicit {"error": ...} envelope so a guest can make a
// bounded decision without being given an unscoped fallback reader.
func (r *wasmRuntime) hostCall(ctx context.Context, module api.Module, ptr, length uint32) uint64 {
	if err := ctx.Err(); err != nil {
		panic(err)
	}
	// The source acceptance fixture uses a zero-length import from _initialize
	// as a deterministic entry barrier. It carries no reader or guest-memory
	// capability and is accepted only on the private initialization context.
	if _, initializing := ctx.Value(wasmInitializerCallKey{}).(struct{}); initializing && ptr == 0 && length == 0 {
		if observer, ok := ctx.Value(wasmInitializerContextObserverKey{}).(func(context.Context)); ok {
			observer(ctx)
		}
		if observer, ok := ctx.Value(wasmInitializerObserverKey{}).(func()); ok {
			observer()
		}
		return 0
	}
	if length > maxWASMMessage {
		panic(failure(ErrMethod, "WASM host request exceeds the message limit"))
	}
	memory := module.Memory()
	data, ok := memory.Read(ptr, length)
	if !ok {
		panic(failure(ErrMethod, "WASM host request is outside guest memory"))
	}
	data = append([]byte(nil), data...)
	var request wasmInvocation
	if value, ok := ctx.Value(wasmInvocationKey{}).(wasmInvocation); ok {
		request = value
	}
	if request.reader == nil {
		panic("WASM outbound host calls are unsupported outside snapshot callbacks")
	}
	releaseReader, admissionErr := r.factory.acquireReaderWorker(ctx)
	if admissionErr != nil {
		panic(admissionErr)
	}
	readResult := make(chan wasmReadOutcome, 1)
	go func() {
		defer releaseReader()
		outcome := wasmReadOutcome{}
		defer func() {
			if recovered := recover(); recovered != nil {
				outcome.panicValue = recovered
			}
			readResult <- outcome
		}()
		outcome.value, outcome.err = r.read(ctx, request.reader, data)
	}()
	var result any
	var err error
	select {
	case <-ctx.Done():
		// A reader is allowed to be slow or to ignore its context. Return from
		// the guest import immediately so wazero can unwind the canceled guest;
		// the bounded outcome channel keeps a late reader from touching guest
		// memory after the module is retired.
		panic(ctx.Err())
	case outcome := <-readResult:
		if outcome.panicValue != nil {
			panic(outcome.panicValue)
		}
		result, err = outcome.value, outcome.err
	}
	if err != nil {
		if ctx.Err() != nil {
			panic(ctx.Err())
		}
		return r.writeHostResponse(ctx, module, wasmResponse{Error: err.Error()})
	}
	// A typed reader is expected to honor its context, but the host must still
	// revoke the callback capability if an implementation returns late. Never
	// let a late read publish a response into the guest after its deadline.
	if err := ctx.Err(); err != nil {
		panic(err)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		panic(failure(ErrRead, "WASM snapshot result could not be encoded: %v", err))
	}
	return r.writeHostResponse(ctx, module, wasmResponse{Result: encoded})
}

func (r *wasmRuntime) writeHostResponse(ctx context.Context, module api.Module, response wasmResponse) uint64 {
	if err := ctx.Err(); err != nil {
		panic(err)
	}
	data, err := json.Marshal(response)
	if err != nil || len(data) > maxWASMMessage {
		panic(failure(ErrMethod, "WASM host response exceeds the message limit"))
	}
	if r.alloc == nil {
		panic(failure(ErrInactive, "WASM allocator is unavailable"))
	}
	ptr, err := r.alloc.Call(ctx, uint64(len(data)))
	if err != nil || len(ptr) != 1 || ptr[0] == 0 || ptr[0] > uint64(^uint32(0)) {
		panic(failure(ErrMethod, "WASM host response allocation failed: %v", err))
	}
	output := uint32(ptr[0])
	if !module.Memory().Write(output, data) {
		panic(failure(ErrMethod, "WASM host response is outside guest memory"))
	}
	return uint64(output)<<32 | uint64(len(data))
}

func (r *wasmRuntime) read(ctx context.Context, reader pluginapi.SnapshotReader, data []byte) (any, error) {
	request, err := decodeWASMReadRequest(data)
	if err != nil {
		panic(failure(ErrRead, "malformed WASM snapshot request: %v", err))
	}
	switch request.Method {
	case "get":
		var params wasmReadRefs
		if err := decodeWASMParams(request.Params, &params, "refs"); err != nil {
			panic(failure(ErrRead, "malformed WASM get request: %v", err))
		}
		return reader.Get(ctx, params.Refs)
	case "query":
		var params pluginapi.Query
		if err := decodeWASMObject(request.Params, &params); err != nil {
			panic(failure(ErrRead, "malformed WASM query request: %v", err))
		}
		return reader.Query(ctx, params)
	case "beads":
		var params wasmReadRefs
		if err := decodeWASMParams(request.Params, &params, "refs"); err != nil {
			panic(failure(ErrRead, "malformed WASM beads request: %v", err))
		}
		return reader.Beads(ctx, params.Refs)
	case "closure":
		var params wasmReadClosure
		if err := decodeWASMParams(request.Params, &params, "ref", "ancestors"); err != nil {
			panic(failure(ErrRead, "malformed WASM closure request: %v", err))
		}
		return reader.Closure(ctx, params.Ref, params.Ancestors)
	case "grants":
		var params wasmReadGrants
		if err := decodeWASMParams(request.Params, &params, "subjects", "resources"); err != nil {
			panic(failure(ErrRead, "malformed WASM grants request: %v", err))
		}
		return reader.Grants(ctx, params.Subjects, params.Resources)
	default:
		return nil, failure(ErrRead, "unsupported WASM snapshot operation %q", request.Method)
	}
}

func decodeWASMReadRequest(data []byte) (wasmReadRequest, error) {
	var request wasmReadRequest
	if err := decodeWASMJSON(data, &request); err != nil {
		return request, err
	}
	if request.Op != "read" {
		return request, fmt.Errorf("operation must be read")
	}
	if request.Method == "" {
		return request, fmt.Errorf("method is required")
	}
	if len(request.Params) == 0 || bytes.Equal(bytes.TrimSpace(request.Params), []byte("null")) {
		return request, fmt.Errorf("params are required")
	}
	return request, nil
}

func decodeWASMParams(data []byte, dst any, required ...string) error {
	var fields map[string]json.RawMessage
	if err := decodeWASMJSON(data, &fields); err != nil {
		return err
	}
	if fields == nil {
		return fmt.Errorf("params must be an object")
	}
	for _, name := range required {
		value, ok := fields[name]
		if !ok {
			return fmt.Errorf("missing %q", name)
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("%q cannot be null", name)
		}
	}
	return decodeWASMJSON(data, dst)
}

func decodeWASMObject(data []byte, dst any) error {
	var fields map[string]json.RawMessage
	if err := decodeWASMJSON(data, &fields); err != nil {
		return err
	}
	if fields == nil {
		return fmt.Errorf("params must be an object")
	}
	for name, value := range fields {
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("%q cannot be null", name)
		}
	}
	return decodeWASMJSON(data, dst)
}

func decodeWASMJSON(data []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return err
	}
	return nil
}

// readers is variadic to keep the narrow internal test seam source-compatible
// with the original E5 invocation helper. Production callbacks pass exactly
// one scoped reader; methods and direct ABI tests pass none.
func (r *wasmRuntime) invoke(ctx context.Context, request wasmRequest, readers ...pluginapi.SnapshotReader) (wasmResponse, error) {
	if len(readers) > 1 {
		r.close()
		return wasmResponse{}, failure(ErrRead, "multiple WASM snapshot readers")
	}
	if len(readers) == 1 && readers[0] != nil {
		ctx = context.WithValue(ctx, wasmInvocationKey{}, wasmInvocation{reader: readers[0]})
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return wasmResponse{}, failure(ErrInactive, "WASM module is closed")
	}
	if err := ctx.Err(); err != nil {
		r.closeLocked()
		return wasmResponse{}, err
	}
	var timerWait *wasmTimerWait
	if r.timer != nil {
		var observer func(context.Context)
		switch request.Op {
		case "mutate", "validate", "post":
			observer, _ = ctx.Value(wasmCallbackTimerWaitObserverKey{}).(func(context.Context))
			if beginObserver, _ := ctx.Value(wasmCallbackTimerBeginObserverKey{}).(func(context.Context, <-chan struct{})); beginObserver != nil {
				beginObserver(ctx, r.timer.retirementSignal())
			}
		}
		timerWait = r.timer.begin(ctx, observer)
		defer r.timer.end(timerWait)
	}
	data, err := json.Marshal(request)
	if err != nil || len(data) > maxWASMMessage {
		r.closeLocked()
		return wasmResponse{}, failure(ErrMethod, "WASM request exceeds the message limit")
	}
	ptr, err := r.alloc.Call(ctx, uint64(len(data)))
	if err != nil {
		r.closeLocked()
	}
	if err != nil || len(ptr) != 1 || ptr[0] == 0 || ptr[0] > uint64(^uint32(0)) {
		r.closeLocked()
		return wasmResponse{}, failure(ErrMethod, "WASM allocation failed: %v", err)
	}
	input := uint32(ptr[0])
	if ok := r.memory.Write(input, data); !ok {
		r.closeLocked()
		return wasmResponse{}, failure(ErrMethod, "WASM input is outside guest memory")
	}
	packed, err := r.handle.Call(ctx, uint64(input), uint64(len(data)))
	if err != nil {
		var busy *Error
		if errors.As(err, &busy) && busy.CallbackBusy() {
			// wazero wraps a host panic with the original error. Preserve the
			// typed reader-admission refusal so the callback pipeline can map it
			// to retryable busy instead of subscriber_unavailable.
			r.closeLocked()
			return wasmResponse{}, busy
		}
		if errors.Is(err, context.Canceled) {
			// A guest can select a context-shaped exit code. Require caller
			// cancellation or owner retirement before cleanup retires the timer.
			cancellationErr := ctx.Err()
			if cancellationErr == nil && r.timer != nil {
				r.timer.mu.Lock()
				if r.timer.retired {
					cancellationErr = context.Canceled
				}
				r.timer.mu.Unlock()
			}
			if cancellationErr != nil {
				r.closeLocked()
				return wasmResponse{}, cancellationErr
			}
		}
		r.closeLocked()
	}
	if err != nil || len(packed) != 1 {
		r.closeLocked()
		return wasmResponse{}, failure(ErrMethod, "WASM handler failed: %v", err)
	}
	outputPtr, outputLen := uint32(packed[0]>>32), uint32(packed[0])
	if outputLen > maxWASMMessage {
		r.closeLocked()
		return wasmResponse{}, failure(ErrMethod, "WASM response exceeds the message limit")
	}
	output, ok := r.memory.Read(outputPtr, outputLen)
	if !ok || !json.Valid(output) {
		r.closeLocked()
		return wasmResponse{}, failure(ErrMethod, "WASM response is not valid JSON")
	}
	var response wasmResponse
	if err := json.Unmarshal(output, &response); err != nil {
		r.closeLocked()
		return wasmResponse{}, failure(ErrMethod, "WASM response envelope is invalid: %v", err)
	}
	if response.Error != "" {
		r.closeLocked()
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

func (m wasmMutator) Mutate(ctx context.Context, change pluginapi.ProposedChange, reader pluginapi.SnapshotReader) ([]pluginapi.Patch, error) {
	response, err := m.runtime.invoke(ctx, wasmRequest{Op: "mutate", Name: m.name, Payload: change}, reader)
	return response.Patches, err
}

type wasmValidator struct {
	runtime *wasmRuntime
	name    string
}

func (v wasmValidator) Validate(ctx context.Context, change pluginapi.ProposedChange, reader pluginapi.SnapshotReader) (pluginapi.Verdict, error) {
	response, err := v.runtime.invoke(ctx, wasmRequest{Op: "validate", Name: v.name, Payload: change}, reader)
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

func (p wasmPost) After(ctx context.Context, change pluginapi.CommittedChange, reader pluginapi.SnapshotReader) error {
	_, err := p.runtime.invoke(ctx, wasmRequest{Op: "post", Name: p.name, Payload: change}, reader)
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
	if err != nil || count > uint32(len(payload)-pos) {
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
