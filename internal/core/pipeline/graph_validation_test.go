package pipeline

import (
	"context"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestTypedExternalGraphValidationPreservesLocalGuards(t *testing.T) {
	ref := func(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
	tests := []struct {
		name      string
		target    model.Ref
		marker    model.LinkTargetKind
		reverse   bool
		wantError bool
	}{
		{name: "opaque external missing locally", target: ref("opaque/é:external"), marker: model.LinkTargetExternal},
		{name: "external self spelling", target: ref("a"), marker: model.LinkTargetExternal},
		{name: "external collision is not a local cycle", target: ref("b"), marker: model.LinkTargetExternal, reverse: true},
		{name: "local cycle stays refused", target: ref("b"), reverse: true, wantError: true},
		{name: "missing local prefix is not external", target: ref("external:missing"), wantError: true},
		{name: "unknown marker stays refused", target: ref("b"), marker: "future", wantError: true},
		{name: "external branch stays refused", target: model.RefByID("b", model.Natural64(1)), marker: model.LinkTargetExternal, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			docs := map[string]Document{
				"a": {ID: "a", Links: []Link{{To: test.target, Kind: "blocks", TargetKind: test.marker}}},
				"b": {ID: "b"},
			}
			if test.reverse {
				doc := docs["b"]
				doc.Links = []Link{{To: ref("a"), Kind: "blocks"}}
				docs["b"] = doc
			}
			err := ValidateGraphDocuments(context.Background(), docs)
			if (err != nil) != test.wantError {
				t.Fatalf("typed graph validation = %v, wantError %v", err, test.wantError)
			}
		})
	}
}

func TestTypedExternalGraphCertificateRejectsLegacyAndStaleFacts(t *testing.T) {
	graph := driver.LockState{Key: driver.GraphBlockingSentinelLock, Present: true, Revision: model.Natural64(7)}
	legacy := driver.LockState{Key: driver.LockKey{Kind: driver.LockReadFact, Fact: "core/graph-validity/v1"}, Present: true, Revision: model.Natural64(7)}
	if currentGraphCertificate([]driver.LockState{legacy, graph}) {
		t.Fatal("legacy graph certificate attested to typed endpoint semantics")
	}
	current := driver.LockState{Key: GraphValidityLockKey(), Present: true, Revision: model.Natural64(7)}
	if !currentGraphCertificate([]driver.LockState{current, graph}) {
		t.Fatal("matching typed graph certificate was refused")
	}
	current.Revision = model.Natural64(6)
	if currentGraphCertificate([]driver.LockState{current, graph}) {
		t.Fatal("stale typed graph certificate bypassed revalidation")
	}
}
