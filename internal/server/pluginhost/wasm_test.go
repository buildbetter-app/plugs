package pluginhost

import "testing"

// e5ABIArtifact is the minimal E5-shaped module: env.plug.call is the only
// import, and the two ABI functions plus one linear memory are exported.
func e5ABIArtifact() []byte {
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
	appendSection(1, section([]byte{3}, []byte{0x60, 1, 0x7f, 1, 0x7f}, []byte{0x60, 2, 0x7f, 0x7f, 1, 0x7e}, []byte{0x60, 2, 0x7f, 0x7f, 1, 0x7e}))
	appendSection(2, section([]byte{1}, str("env"), str("plug.call"), []byte{0, 2}))
	appendSection(3, []byte{2, 0, 1})
	appendSection(5, []byte{1, 0, 1})
	appendSection(7, section([]byte{3}, str("memory"), []byte{2, 0}, str("plug_alloc"), []byte{0, 1}, str("plug_handle"), []byte{0, 2}))
	appendSection(10, section([]byte{2, 5, 0, 0x41, 0x80, 0x20, 0x0b}, []byte{8, 0, 0x20, 0, 0x20, 1, 0x10, 0, 0x0b}))
	return module
}

func TestValidateWASMArtifactAcceptsE5HostImport(t *testing.T) {
	if err := ValidateWASMArtifact(e5ABIArtifact()); err != nil {
		t.Fatalf("E5 env.plug.call ABI was rejected: %v", err)
	}
}
