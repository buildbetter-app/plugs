package s1b05b08

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// B07 keeps the pinned reference's open, byte-bounded dependency vocabulary.
// Exercise the public producers and persisted meaning rather than duplicating
// the private validator's implementation in a unit test.
func TestB07OpaqueKindsRoundTripThroughPublicLinkAndUnlink(t *testing.T) {
	kinds := []struct {
		name, value string
	}{
		{"embedded-space", "release notes"},
		{"leading-space", " custom"},
		{"trailing-space", "custom "},
		{"whitespace-only", " \t "},
		{"embedded-tab", "custom\tkind"},
		{"embedded-newline", "custom\nkind"},
		{"exact-32-utf8-bytes", strings.Repeat("界", 10) + "ab"},
	}
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			for _, kind := range kinds {
				t.Run(kind.name, func(t *testing.T) {
					f := newFixture(t, engine)
					from, to := f.create(t, "opaque-source"), f.create(t, "opaque-target")
					params := map[string]any{"from": from, "to": to, "kind": kind.value}
					first := f.call(t, f.ctx, "bead.link", params, key("opaque-link"), actor)
					if first.Error != nil {
						t.Fatalf("public link refused valid opaque kind %q (%d bytes): %s", kind.value, len(kind.value), first.Error.Data.Code)
					}
					requireOpaqueEdge(t, f, from, to, kind.value)
					before := f.state(t, from)
					replay := f.call(t, f.ctx, "bead.link", params, key("opaque-link"), actor)
					if replay.Error != nil || replay.Meta["replayed"] != true || !reflect.DeepEqual(first.Result, replay.Result) {
						t.Fatalf("opaque link receipt was not replayed exactly: first=%s replay=%+v", first.Result, replay)
					}
					duplicate := f.call(t, f.ctx, "bead.link", params, key("opaque-duplicate"), actor)
					if duplicate.Error != nil {
						t.Fatalf("same-type re-add refused: %s", duplicate.Error.Data.Code)
					}
					if after := f.state(t, from); !reflect.DeepEqual(before, after) {
						t.Fatalf("replay or duplicate changed the graph/history: before=%+v after=%+v", before, after)
					}
					retype := f.call(t, f.ctx, "bead.link", map[string]any{"from": from, "to": to, "kind": "blocks"}, key("opaque-retype"), actor)
					if retype.Error == nil || retype.Error.Data.Code != "invalid_params" {
						t.Fatalf("opaque edge was retyped: %+v", retype)
					}
					if after := f.state(t, from); !reflect.DeepEqual(before, after) {
						t.Fatalf("retype refusal changed the graph/history: before=%+v after=%+v", before, after)
					}
					assertNoReceipt(t, f, anonymous, actor, "bead.link", key("opaque-retype"))
					f.reopen(t)
					requireOpaqueEdge(t, f, from, to, kind.value)
					removed := f.call(t, f.ctx, "bead.unlink", params, key("opaque-unlink"), actor)
					if removed.Error != nil {
						t.Fatalf("public unlink refused exact stored kind %q: %s", kind.value, removed.Error.Data.Code)
					}
					f.reopen(t)
					if links := f.document(t, from).Links; len(links) != 0 {
						t.Fatalf("opaque unlink did not survive reopen: %+v", links)
					}
					before = f.state(t, from)
					removedAgain := f.call(t, f.ctx, "bead.unlink", params, key("opaque-unlink-again"), actor)
					if removedAgain.Error != nil {
						t.Fatalf("absent opaque edge was not an idempotent unlink: %+v", removedAgain)
					}
					if after := f.state(t, from); !reflect.DeepEqual(before, after) {
						t.Fatalf("absent unlink changed graph/history: before=%+v after=%+v", before, after)
					}
				})
			}
		})
	}
}

func requireOpaqueEdge(t *testing.T, f *fixture, from, to model.Ref, kind string) {
	t.Helper()
	links := f.document(t, from).Links
	if len(links) != 1 || links[0].To != to || links[0].Kind != kind {
		t.Fatalf("opaque edge bytes/identity changed: got=%+v want=%s -> %s kind=%q", links, from.ID, to.ID, kind)
	}
}

func TestB07OpaqueKindsRemainInertForReadyCyclesAndClose(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			left, right := f.create(t, "opaque-left"), f.create(t, "opaque-right")
			for i, pair := range [][2]model.Ref{{left, right}, {right, left}} {
				got := f.call(t, f.ctx, "bead.link", map[string]any{"from": pair[0], "to": pair[1], "kind": "custom loop"}, key("opaque-loop-"+model.Natural64(uint64(i)).String()), actor)
				if got.Error != nil {
					t.Fatalf("opaque loop incorrectly became a scheduling cycle: %+v", got)
				}
			}
			requireOpaqueReadyIDs(t, f, left.ID, right.ID)
			closed := f.call(t, f.ctx, "bead.close", map[string]any{"id": left}, key("opaque-close"), actor)
			if closed.Error != nil {
				t.Fatalf("opaque edge became a close blocker: %+v", closed)
			}
			requireOpaqueReadyIDs(t, f, right.ID)
			reopened := f.call(t, f.ctx, "bead.reopen", map[string]any{"id": left}, key("opaque-reopen"), actor)
			if reopened.Error != nil {
				t.Fatalf("opaque loop prevented reopen: %+v", reopened)
			}
			requireOpaqueReadyIDs(t, f, left.ID, right.ID)
			f.reopen(t)
			requireOpaqueEdge(t, f, left, right, "custom loop")
			requireOpaqueEdge(t, f, right, left, "custom loop")
			requireOpaqueReadyIDs(t, f, left.ID, right.ID)
		})
	}
}

func requireOpaqueReadyIDs(t *testing.T, f *fixture, want ...string) {
	t.Helper()
	got := f.call(t, f.ctx, "bead.ready", map[string]any{}, "", actor)
	if got.Error != nil {
		t.Fatalf("public ready failed: %+v", got)
	}
	var page struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(got.Result, &page); err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(page.Items))
	for _, item := range page.Items {
		ids = append(ids, item.ID)
	}
	// This assertion is solely the B07 membership effect, not B01 ordering.
	sort.Strings(ids)
	sort.Strings(want)
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("opaque edges changed ready membership: got=%v want=%v", ids, want)
	}
}

func TestB07OpaqueKindBoundsRefuseAtomicallyAtPublicProducers(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			from, to := f.create(t, "bounded-source"), f.create(t, "bounded-target")
			for i, kind := range []string{"", strings.Repeat("x", 33), strings.Repeat("界", 11)} {
				label := model.Natural64(uint64(i)).String()
				before := []durableState{f.state(t, from), f.state(t, to)}
				params := map[string]any{"from": from, "to": to, "kind": kind}
				for _, method := range []string{"bead.link", "bead.unlink"} {
					idempotency := key("opaque-bounds-" + method + "-" + label)
					got := f.call(t, f.ctx, method, params, idempotency, actor)
					if got.Error == nil || got.Error.Data.Code != "invalid_params" {
						t.Fatalf("%s accepted unstorable kind (%d bytes): %+v", method, len(kind), got)
					}
					assertNoReceipt(t, f, anonymous, actor, method, idempotency)
				}
				cs := model.ChangeSet{Ops: []model.Op{
					{Kind: model.OpPatch, Patch: &model.PatchOp{Target: from, Fields: map[string]any{"title": "must roll back"}}},
					{Kind: model.OpLink, Link: &model.LinkOp{Kind: model.OpLink, From: from, To: to, LinkKind: kind}},
				}}
				idempotency := key("opaque-bounds-batch-" + label)
				batch := f.call(t, f.ctx, "tx.apply", map[string]any{"changeSet": cs}, idempotency, actor)
				if batch.Error == nil || batch.Error.Data.Code != "invalid_params" {
					t.Fatalf("batch accepted unstorable kind (%d bytes): %+v", len(kind), batch)
				}
				if after := []durableState{f.state(t, from), f.state(t, to)}; !reflect.DeepEqual(before, after) {
					t.Fatalf("invalid kind partially changed batch/graph/history: before=%+v after=%+v", before, after)
				}
				assertNoReceipt(t, f, anonymous, actor, "tx.apply", idempotency)
			}
		})
	}
}
