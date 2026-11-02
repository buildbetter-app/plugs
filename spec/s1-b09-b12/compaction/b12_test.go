package compaction_test

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

// This is an actual catalogued RPC call after ordinary public create/close,
// not an authored unsupported marker or a fake successful compaction handler.
func TestB12MissingSummaryProviderRefusesWithoutPublication(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			ref := f.create("create-original", strings.Repeat("Original immutable text. ", 400))
			f.call("bead.close", "close-original", map[string]any{"id": ref}, "")
			before := f.call("bead.get", "get-before", map[string]any{"id": ref}, "")
			meta, err := f.store.Meta(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			f.call("admin.compact", "missing-provider", map[string]any{"beforeRevision": meta.Head, "dryRun": false}, "compaction_unavailable")
			f.assertSameHead(meta)
			after := f.call("bead.get", "get-after", map[string]any{"id": ref}, "")
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("unavailable compaction changed original: before=%v after=%v", before, after)
			}
		})
	}
}
