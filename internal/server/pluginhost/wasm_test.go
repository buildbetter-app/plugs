package pluginhost

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/pluginapi"
)

// e5ABIArtifact is the minimal E5-shaped module: env.plug.call is the only
// import, and the two ABI functions plus one linear memory are exported.
func e5ABIArtifact() []byte { return e5ABIArtifactWithInitializer(false) }

func e5ABIArtifactWithInitializer(loop bool) []byte {
	var module = []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}
	appendSection := func(id byte, payload []byte) {
		module = append(module, id, byte(len(payload)))
		module = append(module, payload...)
	}
	str := func(value string) []byte { return append([]byte{byte(len(value))}, []byte(value)...) }
	section := func(parts ...[]byte) []byte {
		var out []byte
		for _, part := range parts {
			out = append(out, part...)
		}
		return out
	}
	appendSection(1, section([]byte{4}, []byte{0x60, 1, 0x7f, 1, 0x7f}, []byte{0x60, 2, 0x7f, 0x7f, 1, 0x7e}, []byte{0x60, 2, 0x7f, 0x7f, 1, 0x7e}, []byte{0x60, 0, 0}))
	appendSection(2, section([]byte{1}, str("env"), str("plug.call"), []byte{0, 2}))
	appendSection(3, []byte{3, 0, 1, 3})
	appendSection(5, []byte{1, 0, 1})
	appendSection(7, section([]byte{4}, str("memory"), []byte{2, 0}, str("plug_alloc"), []byte{0, 1}, str("plug_handle"), []byte{0, 2}, str("_initialize"), []byte{0, 3}))
	initializer := []byte{2, 0, 0x0b}
	if loop {
		initializer = []byte{7, 0, 0x03, 0x40, 0x0c, 0, 0x0b, 0x0b}
	}
	appendSection(10, section([]byte{3, 5, 0, 0x41, 0x80, 0x20, 0x0b}, []byte{8, 0, 0x20, 0, 0x20, 1, 0x10, 0, 0x0b}, initializer))
	return module
}

func TestWASME5ImportLinksAndDeniesOutboundCalls(t *testing.T) {
	runtime, err := newWASMRuntime(e5ABIArtifact(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.close()
	// The reserved import must actually link and fail closed when invoked;
	// static admission alone does not establish the instantiation contract.
	if _, err := runtime.invoke(context.Background(), wasmRequest{Op: "method", Name: "x.wasi.echo"}); err == nil || !strings.Contains(err.Error(), "outbound host calls are unsupported") {
		t.Fatalf("E5 host import did not fail explicitly: %v", err)
	}
	if _, err := runtime.invoke(context.Background(), wasmRequest{Op: "method", Name: "x.wasi.echo"}); requireCode(err, ErrInactive) != nil {
		t.Fatalf("trapped WASM guest was reused: %v", err)
	}
}

func TestWASMInitializationHasDeadline(t *testing.T) {
	done := make(chan error, 1)
	go func() {
		runtime, err := newWASMRuntime(e5ABIArtifactWithInitializer(true), nil)
		if runtime != nil {
			runtime.close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "deadline") {
			t.Fatalf("unbounded initializer did not hit its deadline: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WASM initialization did not stop at its deadline")
	}
}

func TestWASMStaticChecksRefuseUndeclaredImportsAndMemory(t *testing.T) {
	for _, tc := range []struct {
		name, from, to string
	}{
		{"import", "plug.call", "evil.call"},
		{"memory", string([]byte{5, 3, 1, 0, 1}), string([]byte{5, 4, 1, 0, 0x81, 0x08})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := e5ABIArtifact()
			mutated := bytes.Replace(original, []byte(tc.from), []byte(tc.to), 1)
			if bytes.Equal(original, mutated) {
				t.Fatal("test mutation did not change the module")
			}
			if err := ValidateWASMArtifact(mutated); err == nil {
				t.Fatal("undeclared module capability was accepted")
			}
		})
	}
}

func TestValidateWASMArtifactAcceptsE5HostImport(t *testing.T) {
	if err := ValidateWASMArtifact(e5ABIArtifact()); err != nil {
		t.Fatalf("E5 env.plug.call ABI was rejected: %v", err)
	}
}

type wasmBridgeReader struct {
	gets    int
	queries int
}

func (r *wasmBridgeReader) Get(_ context.Context, refs []pluginapi.Ref) ([]pluginapi.Record, error) {
	r.gets++
	if len(refs) != 1 {
		return nil, fmt.Errorf("want one ref, got %d", len(refs))
	}
	return []pluginapi.Record{{Ref: refs[0], Kind: "bead", Revision: pluginapi.Natural64(7), Body: json.RawMessage(`{"id":"pl-a","type":"issue","fields":{"title":"hello"}}`)}}, nil
}
func (r *wasmBridgeReader) Query(_ context.Context, query pluginapi.Query) (pluginapi.Page[pluginapi.Record], error) {
	r.queries++
	_ = query
	return pluginapi.Page[pluginapi.Record]{
		AsOf: pluginapi.Natural64(7),
		Items: []pluginapi.Record{{
			Ref: pluginapi.RefByID("pl-a", ""), Kind: "bead", Revision: pluginapi.Natural64(7),
			Body: json.RawMessage(`{"id":"pl-a","type":"issue","fields":{"title":"hello"}}`),
		}},
	}, nil
}
func (*wasmBridgeReader) Beads(context.Context, []pluginapi.Ref) ([]pluginapi.View, error) {
	return nil, fmt.Errorf("beads should not be called")
}
func (*wasmBridgeReader) Closure(context.Context, pluginapi.Ref, bool) ([]pluginapi.Ref, error) {
	return nil, fmt.Errorf("closure unsupported")
}
func (*wasmBridgeReader) Grants(context.Context, []pluginapi.SubjectRef, []pluginapi.Ref) ([]pluginapi.Grant, error) {
	return nil, fmt.Errorf("grants unsupported")
}

func TestWASMSnapshotBridgeUsesCallbackReader(t *testing.T) {
	runtime, err := newWASMRuntime(e5ABIArtifact(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.close()
	reader := &wasmBridgeReader{}
	response, err := runtime.invoke(context.Background(), wasmRequest{
		Op: "read", Method: "get", Params: json.RawMessage(`{"refs":[{"id":"pl-a"}]}`),
	}, reader)
	if err != nil || response.Error != "" {
		t.Fatalf("WASM get bridge failed: %+v (%v)", response, err)
	}
	var records []pluginapi.Record
	if err := json.Unmarshal(response.Result, &records); err != nil || len(records) != 1 || records[0].Revision.String() != "7" {
		t.Fatalf("WASM get result was not canonical: %s (%v)", response.Result, err)
	}
	response, err = runtime.invoke(context.Background(), wasmRequest{
		Op: "read", Method: "query", Params: json.RawMessage(`{"limit":1}`),
	}, reader)
	if err != nil || response.Error != "" {
		t.Fatalf("WASM query bridge failed: %+v (%v)", response, err)
	}
	var page pluginapi.Page[pluginapi.Record]
	if err := json.Unmarshal(response.Result, &page); err != nil || page.AsOf.String() != "7" || len(page.Items) != 1 {
		t.Fatalf("WASM query result was not pinned: %s (%v)", response.Result, err)
	}
	if reader.gets != 1 || reader.queries != 1 {
		t.Fatalf("typed reader calls=%d/%d, want 1/1", reader.gets, reader.queries)
	}
	response, err = runtime.invoke(context.Background(), wasmRequest{
		Op: "read", Method: "closure", Params: json.RawMessage(`{"ref":{"id":"pl-a"},"ancestors":true}`),
	}, reader)
	if err == nil || !strings.Contains(err.Error(), "closure unsupported") || response.Result != nil {
		t.Fatalf("unsupported typed reader operation was not an explicit refusal: %+v (%v)", response, err)
	}
}

func TestWASMReadRejectsNullTypedParams(t *testing.T) {
	runtime, err := newWASMRuntime(e5ABIArtifact(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.close()
	_, err = runtime.invoke(context.Background(), wasmRequest{
		Op: "read", Method: "get", Params: json.RawMessage(`{"refs":null}`),
	}, &wasmBridgeReader{})
	if err == nil || !strings.Contains(err.Error(), "cannot be null") {
		t.Fatalf("null typed params were accepted: %v", err)
	}
}

func TestWASMHostBridgeTrapsMalformedGuestBuffer(t *testing.T) {
	runtime, err := newWASMRuntime(e5ABIArtifact(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.close()
	panicSeen := false
	func() {
		defer func() { panicSeen = recover() != nil }()
		runtime.hostCall(context.Background(), runtime.module, 0, maxWASMMessage+1)
	}()
	if !panicSeen {
		t.Fatal("oversized guest buffer did not trap")
	}
	panicSeen = false
	func() {
		defer func() { panicSeen = recover() != nil }()
		runtime.hostCall(context.Background(), runtime.module, 0, 1)
	}()
	if !panicSeen {
		t.Fatal("out-of-bounds guest buffer did not trap")
	}
}

type wasmConcurrentReader struct{ id string }

func (r *wasmConcurrentReader) Get(_ context.Context, refs []pluginapi.Ref) ([]pluginapi.Record, error) {
	if len(refs) != 1 {
		return nil, fmt.Errorf("want one ref, got %d", len(refs))
	}
	return []pluginapi.Record{{
		Ref:      pluginapi.RefByID(r.id, ""),
		Kind:     "bead",
		Revision: pluginapi.Natural64(1),
		Body:     json.RawMessage(`{"type":"issue","fields":{"title":"scoped"}}`),
	}}, nil
}
func (*wasmConcurrentReader) Query(context.Context, pluginapi.Query) (pluginapi.Page[pluginapi.Record], error) {
	return pluginapi.Page[pluginapi.Record]{}, fmt.Errorf("query unsupported")
}
func (*wasmConcurrentReader) Beads(context.Context, []pluginapi.Ref) ([]pluginapi.View, error) {
	return nil, fmt.Errorf("beads unsupported")
}
func (*wasmConcurrentReader) Closure(context.Context, pluginapi.Ref, bool) ([]pluginapi.Ref, error) {
	return nil, fmt.Errorf("closure unsupported")
}
func (*wasmConcurrentReader) Grants(context.Context, []pluginapi.SubjectRef, []pluginapi.Ref) ([]pluginapi.Grant, error) {
	return nil, fmt.Errorf("grants unsupported")
}

func TestWASMConcurrentInvocationsKeepReadersScoped(t *testing.T) {
	runtime, err := newWASMRuntime(e5ABIArtifact(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.close()

	start := make(chan struct{})
	results := make(chan struct {
		want string
		got  string
		err  error
	}, 2)
	var group sync.WaitGroup
	for _, id := range []string{"reader-one", "reader-two"} {
		id := id
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			response, callErr := runtime.invoke(context.Background(), wasmRequest{
				Op: "read", Method: "get", Params: json.RawMessage(fmt.Sprintf(`{"refs":[{"id":%q}]}`, id)),
			}, &wasmConcurrentReader{id: id})
			if callErr != nil {
				results <- struct {
					want string
					got  string
					err  error
				}{want: id, err: callErr}
				return
			}
			var records []pluginapi.Record
			if decodeErr := json.Unmarshal(response.Result, &records); decodeErr != nil {
				results <- struct {
					want string
					got  string
					err  error
				}{want: id, err: decodeErr}
				return
			}
			got := ""
			if len(records) == 1 {
				got = records[0].Ref.ID
			}
			results <- struct {
				want string
				got  string
				err  error
			}{want: id, got: got}
		}()
	}
	close(start)
	group.Wait()
	close(results)
	for result := range results {
		if result.err != nil || result.got != result.want {
			t.Errorf("reader scope crossed invocation boundary: want=%q got=%q err=%v", result.want, result.got, result.err)
		}
	}
}

type wasmLateReader struct{}

func (*wasmLateReader) Get(context.Context, []pluginapi.Ref) ([]pluginapi.Record, error) {
	time.Sleep(20 * time.Millisecond)
	return []pluginapi.Record{{Ref: pluginapi.RefByID("late", ""), Revision: pluginapi.Natural64(1), Body: json.RawMessage(`{}`)}}, nil
}
func (*wasmLateReader) Query(context.Context, pluginapi.Query) (pluginapi.Page[pluginapi.Record], error) {
	return pluginapi.Page[pluginapi.Record]{}, fmt.Errorf("query unsupported")
}
func (*wasmLateReader) Beads(context.Context, []pluginapi.Ref) ([]pluginapi.View, error) {
	return nil, fmt.Errorf("beads unsupported")
}
func (*wasmLateReader) Closure(context.Context, pluginapi.Ref, bool) ([]pluginapi.Ref, error) {
	return nil, fmt.Errorf("closure unsupported")
}
func (*wasmLateReader) Grants(context.Context, []pluginapi.SubjectRef, []pluginapi.Ref) ([]pluginapi.Grant, error) {
	return nil, fmt.Errorf("grants unsupported")
}

func TestWASMLateReaderCannotPublishAfterDeadline(t *testing.T) {
	runtime, err := newWASMRuntime(e5ABIArtifact(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err = runtime.invoke(ctx, wasmRequest{
		Op: "read", Method: "get", Params: json.RawMessage(`{"refs":[{"id":"late"}]}`),
	}, &wasmLateReader{})
	if err == nil || !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("late reader response was not revoked: %v", err)
	}
}
