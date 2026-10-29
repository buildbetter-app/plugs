package pluginhost

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
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
