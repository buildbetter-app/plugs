package model

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestOpaqueEdgeMetadataRetainsTokensAndDetaches(t *testing.T) {
	tokens := map[string]string{"beads.thread_id": `"  \u03b1 \\ "`, "beads.future": `{ "a": 9007199254740993, "b": [1.0,1e2,null] }`}
	original := LinkMetadata{NativeExtensions: tokens}
	copy, err := original.ForKind("custom kind")
	if err != nil || copy.HasPredicate() {
		t.Fatalf("opaque field gained a predicate: %+v %v", copy, err)
	}
	encoded, err := json.Marshal(copy)
	if err != nil {
		t.Fatal(err)
	}
	var decoded LinkMetadata
	if err = json.Unmarshal(encoded, &decoded); err != nil || !reflect.DeepEqual(decoded, copy) {
		t.Fatalf("token transport changed bytes: %s %v", encoded, err)
	}
	copy.NativeExtensions["beads.thread_id"] = `null`
	if original.NativeExtensions["beads.thread_id"] == `null` {
		t.Fatal("normalized metadata aliases caller map")
	}
	prior := LinkMetadata{Gate: "any-children", AlsoBlocks: true, NativeExtensions: tokens}
	merged := copy.Retaining(&prior)
	if merged.Gate != prior.Gate || merged.AlsoBlocks != prior.AlsoBlocks || merged.NativeExtensions["beads.future"] != tokens["beads.future"] {
		t.Fatal("opaque update erased typed predicate or unknown value")
	}
	if _, err := (LinkMetadata{Gate: "all-children", NativeExtensions: tokens}).ForKind("custom kind"); err == nil {
		t.Fatal("typed predicate became valid for custom kind")
	}
	for _, token := range []string{"", `null true`, `{"x":`, string([]byte{0xff})} {
		if err := ValidateNativeExtensions(map[string]string{"beads.future": token}); err == nil || !strings.Contains(err.Error(), "beads.future") {
			t.Fatalf("malformed token missing field refusal: %q %v", token, err)
		}
	}
}
