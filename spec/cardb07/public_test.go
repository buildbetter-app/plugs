package cardb20

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

type b07Observation struct {
	Name          string `json:"name"`
	Kind          string `json:"kind"`
	From          string `json:"from"`
	To            string `json:"to"`
	Accepted      bool   `json:"accepted"`
	SourceReady   bool   `json:"source_ready"`
	TargetReady   bool   `json:"target_ready"`
	CloseAllowed  bool   `json:"close_allowed"`
	SameKindQuiet bool   `json:"same_kind_quiet"`
	RetypeRefused bool   `json:"retype_refused"`
}

type b07Case struct {
	Name, Kind string
	Valid      bool
}

func b07Cases() []b07Case {
	out := []b07Case{}
	for _, kind := range b07KnownKinds {
		out = append(out, b07Case{kind, kind, true})
	}
	for _, v := range b07OpaqueKindInputs {
		out = append(out, b07Case{v.Name, v.Kind, v.ValidStorage})
	}
	return out
}

func TestB07PublicVocabularyIdempotencyAndGuards(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) { b07Public(t, engine) })
	}
}

func b07Public(t *testing.T, engine string) []b07Observation {
	h := b07NewHost(t, engine)
	observed := []b07Observation{}
	for _, item := range b07Cases() {
		if !t.Run(item.Name, func(t *testing.T) {
			old := h.t
			h.t = t
			defer func() { h.t = old }()
			from, to := h.create(item.Name+"-source", nil), h.create(item.Name+"-target", nil)
			if item.Kind == "waits-for" {
				h.create("waits-for-child", &to)
			}
			baseline := h.ready()
			if !b07Has(baseline, from.ID) || !b07Has(baseline, to.ID) {
				t.Fatal("fresh endpoints not ready")
			}
			params := map[string]any{"from": from, "to": to, "kind": item.Kind}
			before := h.snapshot()
			first := h.call("bead.link", item.Name+"-link", params)
			o := b07Observation{Name: item.Name, Kind: item.Kind, From: from.ID, To: to.ID, Accepted: item.Valid}
			if !item.Valid {
				if first.Error == nil || first.Error.Data.Code != "invalid_params" {
					t.Fatalf("byte-boundary refusal=%+v", first)
				}
				h.same(before)
				h.requireReady(baseline)
				observed = append(observed, o)
				return
			}
			b07Success(t, first)
			h.linkValue(from, to, item.Kind)
			blocked := item.Kind == "blocks" || item.Kind == "conditional-blocks" || item.Kind == "waits-for"
			want := baseline
			if blocked {
				want = b07Without(want, from.ID)
			}
			h.requireReady(want)
			o.SourceReady = b07Has(want, from.ID)
			o.TargetReady = b07Has(want, to.ID)
			original := h.snapshot()
			duplicate := h.call("bead.link", item.Name+"-duplicate", params)
			h.quiet(original, item.Name+"-duplicate", duplicate)
			o.SameKindQuiet = true
			complete := h.snapshot()
			h.reopen()
			replay := h.call("bead.link", item.Name+"-link", params)
			if replay.Error != nil || replay.Meta["replayed"] != true || !bytes.Equal(replay.Result, first.Result) {
				t.Fatal("original public response not replayed after physical reopen")
			}
			h.same(complete)
			different := "blocks"
			if item.Kind == "blocks" {
				different = "related"
			}
			bad := map[string]any{"from": from, "to": to, "kind": different}
			before = h.snapshot()
			refused := h.call("bead.link", item.Name+"-retype", bad)
			if refused.Error == nil || refused.Error.Data.Code != "ontology_guard_failed" {
				t.Fatalf("retype=%+v", refused)
			}
			h.same(before)
			h.linkValue(from, to, item.Kind)
			o.RetypeRefused = true
			// A refusal does not consume the caller key; correction is a true no-op.
			corrected := h.call("bead.link", item.Name+"-retype", params)
			if corrected.Meta["replayed"] == true {
				t.Fatal("refused retype poisoned the caller key")
			}
			h.quiet(before, item.Name+"-retype", corrected)
			before = h.snapshot()
			closed := h.call("bead.close", item.Name+"-close", map[string]any{"id": from})
			if blocked {
				if closed.Error == nil || closed.Error.Data.Code != "ontology_guard_failed" {
					t.Fatalf("fixed blocking kind did not guard close: %+v", closed)
				}
				h.same(before)
				h.requireReady(want)
			} else {
				b07Success(t, closed)
				h.requireReady(b07Without(want, from.ID))
				o.CloseAllowed = true
			}
			observed = append(observed, o)
		}) {
			t.Fatal("public vocabulary control incomplete")
		}
	}
	if !t.Run("opaque_cycle_and_kind_omission", func(t *testing.T) {
		old := h.t
		h.t = t
		defer func() { h.t = old }()
		a, b := h.create("opaque-loop-a", nil), h.create("opaque-loop-b", nil)
		ready := h.ready()
		before := h.snapshot()
		omitted := h.call("bead.link", "kind-omitted", map[string]any{"from": a, "to": b})
		if omitted.Error == nil || omitted.Error.Data.Code != "invalid_params" {
			t.Fatal("public link kind omission must be explicit refusal", omitted.Error)
		}
		h.same(before)
		for i, pair := range [][2]model.Ref{{a, b}, {b, a}} {
			key := []string{"opaque-loop-forward", "opaque-loop-reverse"}[i]
			b07Success(t, h.call("bead.link", key, map[string]any{"from": pair[0], "to": pair[1], "kind": "custom kind"}))
			h.linkValue(pair[0], pair[1], "custom kind")
			h.requireReady(ready)
		}
		h.reopen()
		h.requireReady(ready)
		b07Success(t, h.call("bead.close", "opaque-loop-close-a", map[string]any{"id": a}))
		h.requireReady(b07Without(ready, a.ID))
		b07Success(t, h.call("bead.close", "opaque-loop-close-b", map[string]any{"id": b}))
		h.requireReady(b07Without(b07Without(ready, a.ID), b.ID))
		b07JSON(t, filepath.Join(h.root, "opaque-loop-kind-default.json"), map[string]any{"custom_cycle": "accepted, readiness and close guards inert", "public_omitted_kind": "invalid_params", "native_omitted_batch_kind": "separately recorded CLI default; public RPC requires an explicit kind"})
	}) {
		t.Fatal("opaque cycle or kind omission control incomplete")
	}
	if !t.Run("external_and_missing_local", func(t *testing.T) {
		old := h.t
		h.t = t
		defer func() { h.t = old }()
		from := h.create("external-source", nil)
		before := h.snapshot()
		r := h.call("bead.link", "absent-local", map[string]any{"from": from, "to": model.RefByID("b07-absent", model.Natural64(0)), "kind": "related"})
		if r.Error == nil || r.Error.Data.Code != "not_found" {
			t.Fatalf("missing local=%+v", r)
		}
		h.same(before)
		for _, targetID := range []string{"external:tracker:ticket-1", "remote-1"} {
			target := model.RefByID(targetID, model.Natural64(0))
			ready := h.ready()
			params := map[string]any{"from": from, "to": target, "kind": "related", "targetKind": "external"}
			b07Success(t, h.call("bead.link", "external-add-"+targetID, params))
			h.linkValue(from, target, "related")
			h.requireReady(ready)
			before = h.snapshot()
			h.quiet(before, "external-repeat-"+targetID, h.call("bead.link", "external-repeat-"+targetID, params))
			h.reopen()
			b07Success(t, h.call("bead.unlink", "external-remove-"+targetID, params))
			h.requireReady(ready)
		}
	}) {
		t.Fatal("external control incomplete")
	}
	if !t.Run("atomic_ordinary_batch_refusal", func(t *testing.T) {
		old := h.t
		h.t = t
		defer func() { h.t = old }()
		from, to := h.create("batch-source", nil), h.create("batch-target", nil)
		b07Success(t, h.call("bead.link", "batch-original", map[string]any{"from": from, "to": to, "kind": "related"}))
		before := h.snapshot()
		cs := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: from, Fields: map[string]any{"title": "must not publish"}}}, {Kind: model.OpLink, Link: &model.LinkOp{From: from, To: to, LinkKind: "blocks"}}}}
		r := h.call("tx.apply", "atomic-refusal", map[string]any{"changeSet": cs})
		if r.Error == nil || r.Error.Data.Code != "ontology_guard_failed" {
			t.Fatalf("batch refusal=%+v", r)
		}
		h.same(before)
		h.reopen()
		h.same(before)
	}) {
		t.Fatal("ordinary admission control incomplete")
	}
	if !t.Run("semantic_waits_for_metadata", func(t *testing.T) {
		old := h.t
		h.t = t
		defer func() { h.t = old }()
		from, to := h.create("metadata-source", nil), h.create("metadata-spawner", nil)
		h.create("metadata-child", &to)
		p := map[string]any{"from": from, "to": to, "kind": "waits-for", "metadata": json.RawMessage(`{"gate":"all-children", "also_blocks":false}`)}
		b07Success(t, h.call("bead.link", "metadata-original", p))
		before := h.snapshot()
		p["metadata"] = json.RawMessage(`{ "also_blocks":false, "gate":"all-children" }`)
		h.quiet(before, "metadata-equivalent", h.call("bead.link", "metadata-equivalent", p))
		h.reopen()
		p["metadata"] = map[string]any{"gate": "any-children", "also_blocks": true}
		b07Success(t, h.call("bead.link", "metadata-change", p))
		h.linkValue(from, to, "waits-for")
		b07JSON(t, filepath.Join(h.root, "metadata-scope.json"), map[string]any{"scope": "typed gate/also_blocks equality only", "opaque_edge_fields": "separate exact-token controls"})
	}) {
		t.Fatal("typed metadata control incomplete")
	}
	b07JSON(t, filepath.Join(h.root, "outcomes.json"), observed)
	return observed
}

func b07Compare(t *testing.T, native, plug []b07Observation) {
	t.Helper()
	if len(native) != len(plug) {
		t.Fatal("native/public case cardinality differs")
	}
	for i, n := range native {
		p := plug[i]
		if n.Name != p.Name || n.Kind != p.Kind || n.Accepted != p.Accepted {
			t.Fatalf("literal vocabulary/byte-boundary comparison differs: native=%+v plug=%+v", n, p)
		}
		if !n.Accepted {
			continue
		}
		if n.SourceReady != p.SourceReady || n.TargetReady != p.TargetReady || n.SameKindQuiet != p.SameKindQuiet || n.RetypeRefused != p.RetypeRefused || n.CloseAllowed != p.CloseAllowed {
			t.Fatalf("native/public current edge effect differs for %s: native=%+v plug=%+v", n.Name, n, p)
		}
	}
}
