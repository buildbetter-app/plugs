package pipeline

import (
	"github.com/buildbetter-app/plugs/internal/core/model"
	"testing"
)

func TestExactStatusSelectorPreservesOmittedReceiptIdentity(t *testing.T) {
	cs := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID("pl-one", model.Natural64(0)), Fields: map[string]any{}}}}}
	meta := model.WriteMeta{Method: "bead.reopen", Project: "g1", Return: "full"}
	// Frozen pre-selector request bytes. A rolling restart must still find this
	// receipt rather than silently breaking all omitted-target lifecycle replays.
	const legacy = "c8f5e66a549fa7b922b2082b3e6e9c19b4b7084eaba0ffcb7b54bac2a86ea312"
	omitted, err := requestHash(cs, meta)
	if err != nil {
		t.Fatal(err)
	}
	if omitted != legacy {
		t.Fatalf("omitted selector changed legacy hash: %s", omitted)
	}
	a, b := "native-ready-a", "native-ready-b"
	meta.TargetStatus = &a
	first, err := requestHash(cs, meta)
	if err != nil {
		t.Fatal(err)
	}
	meta.TargetStatus = &b
	second, err := requestHash(cs, meta)
	if err != nil {
		t.Fatal(err)
	}
	if first == omitted || second == omitted || first == second {
		t.Fatal("explicit status not bound into receipt identity")
	}
}
