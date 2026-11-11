package cardb20

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

func (f *b08Fixture) link(label string, from, to model.Ref, kind string) {
	f.t.Helper()
	f.call(label, "bead.link", map[string]any{"from": from, "to": to, "kind": kind}, "")
}

// Count both the public commit and the actual immutable history/versions.
// A successful single-record close must not publish a second revision or
// rewrite a dependent just to refresh its read-time readiness.
func (f *b08Fixture) oneWrite(label, method string, params map[string]any, id model.Ref) b08Reply {
	f.t.Helper()
	before := f.state()
	out := f.call(label, method, params, "")
	var commit model.Commit
	if err := json.Unmarshal(out.Result, &commit); err != nil {
		f.t.Fatal(err)
	}
	after := f.state()
	if commit.Revision.IsZero() || after.Meta.Head != commit.Revision || len(after.History) != len(before.History)+1 || len(after.Versions[id.ID]) != len(before.Versions[id.ID])+1 {
		f.t.Fatalf("%s did not commit exactly once: %s", label, out.Result)
	}
	for _, row := range before.Records {
		if row.Ref.ID == id.ID {
			continue
		}
		found := false
		for _, current := range after.Records {
			if current.Ref.ID == row.Ref.ID {
				found = true
				if !reflect.DeepEqual(row, current) || !reflect.DeepEqual(before.Versions[row.Ref.ID], after.Versions[row.Ref.ID]) {
					f.t.Fatal("close rewrote an unrelated row or version", row.Ref.ID)
				}
			}
		}
		if !found {
			f.t.Fatal("close deleted an unrelated row", row.Ref.ID)
		}
	}
	b08Save(f.t, f.root, label+"-one-write.json", map[string]any{"before": before, "after": after, "commit": commit})
	return out
}

func (f *b08Fixture) closeOnce(label string, id model.Ref, reason string) b08Reply {
	return f.oneWrite(label, "bead.close", map[string]any{"id": id, "reason": reason}, id)
}

func TestB08PublicFixedGuardsAndNoWriteRefusals(t *testing.T) {
	root := b08Root(t, "b08-fixed-guards")
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newB08Fixture(t, engine, filepath.Join(root, engine), false)
			template := f.create("template", "b08-item", map[string]any{"template": true}, nil)
			f.conserved("template", "bead.close", map[string]any{"id": template}, "ontology_guard_failed", "not_template")
			// P23b/17:03 ruling: Plug has no caller-controlled force bypass.
			for i, force := range []any{true, false, "true"} {
				f.conserved(fmt.Sprintf("undeclared-force-%d", i), "bead.close", map[string]any{"id": template, "force": force}, "invalid_params", "")
			}
			parent := f.create("parent", "b08-item", nil, nil)
			child := f.create("child", "b08-item", nil, &parent)
			f.conserved("open-child", "bead.close", map[string]any{"id": parent}, "ontology_guard_failed", "no_open_children")
			f.closeOnce("close-child", child, "child complete")
			f.closeOnce("close-parent", parent, "parent complete")
			blocker := f.create("blocker", "b08-item", nil, nil)
			dependent := f.create("dependent", "b08-item", nil, nil)
			f.link("blocks", dependent, blocker, "blocks")
			f.conserved("live-blocker", "bead.close", map[string]any{"id": dependent}, "ontology_guard_failed", "no_open_blockers")
			f.closeOnce("close-blocker", blocker, "blocker complete")
			f.closeOnce("close-dependent", dependent, "dependent complete")
			for _, kind := range []string{"related", "relates-to", "tracks"} {
				id := f.create("association-"+kind, "b08-item", nil, nil)
				target := f.create("association-target-"+kind, "b08-item", nil, nil)
				f.link("association-link-"+kind, id, target, kind)
				f.closeOnce("association-close-"+kind, id, "association does not block")
			}
			required := f.create("required", "b08-required", nil, nil)
			f.conserved("required-field-absent", "bead.close", map[string]any{"id": required}, "ontology_guard_failed", "fields_required")
			f.call("supply-proof", "bead.update", map[string]any{"id": required, "fields": map[string]any{"proof": "independent public proof"}}, "")
			f.closeOnce("required-field-present", required, "proved")
			evidence := f.create("evidence", "b08-evidence", nil, nil)
			f.conserved("evidence-absent", "bead.close", map[string]any{"id": evidence}, "ontology_guard_failed", "evidence_required")
			f.call("supply-evidence", "tx.apply", map[string]any{"changeSet": map[string]any{"ops": []map[string]any{{"kind": "attach", "target": evidence, "attachment": map[string]any{"kind": "link", "uri": "https://example.invalid/b08-public-proof", "mediaType": "text/plain", "summary": "B08 public proof"}}}}}, "")
			f.closeOnce("evidence-present", evidence, "evidenced")
			gate := f.create("gate", "b08-gate", nil, nil)
			// A required gate with no authoritative satisfied observation must
			// fail closed. This does not manufacture a successful resolver.
			f.conserved("gate-unobserved", "bead.close", map[string]any{"id": gate}, "ontology_guard_failed", "gates_resolved")
			f.reopen("guard-terminal")
		})
	}
}

func TestB08PublicStaleBlockedFlagWithoutLiveBlocker(t *testing.T) {
	root := b08Root(t, "b08-stale-blocked")
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newB08Fixture(t, engine, filepath.Join(root, engine), false)
			stale := f.create("stale-blocked", "b08-item", map[string]any{"blocked": true}, nil)
			// Binding 17:03Z B02 keeps Plug's explicit blocked field as a
			// claim/close guard. Native's stale projection remains separate.
			f.conserved("stored-blocked-no-live-edge", "bead.close", map[string]any{"id": stale}, "ontology_guard_failed", "no_open_blockers")
			f.call("clear-explicit-blocked", "bead.update", map[string]any{"id": stale, "fields": map[string]any{"blocked": false}}, "")
			f.closeOnce("cleared-blocked-no-live-edge", stale, "explicit close guard cleared")
			f.reopen("stale-blocked-terminal")
		})
	}
}

func (f *b08Fixture) invoke(label string) model.Ref {
	f.t.Helper()
	out := f.call("invoke-"+label, "x.workflow.invoke", map[string]any{"template": "b08", "project": b08Project, "mode": "ephemeral", "checklist": []map[string]any{{"label": label, "title": "B08 " + label, "type": "issue"}}}, "")
	var result workflowplugin.InvokeResult
	if err := json.Unmarshal(out.Result, &result); err != nil || result.Plane != "ephemeral" || len(result.Items) != 1 || result.Items[0].Project != b08Project || !strings.HasPrefix(result.Items[0].ID, workflowplugin.ItemPrefix) {
		f.t.Fatalf("actual workflow invocation failed: %s %v", out.Result, err)
	}
	return model.RefByID(result.Items[0].ID, model.Natural64(0))
}

func TestB08PublicOpenChildrenAcrossPlanes(t *testing.T) {
	root := b08Root(t, "b08-mixed-children")
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newB08Fixture(t, engine, filepath.Join(root, engine), false)
			parent := f.create("mixed-parent", "b08-item", nil, nil)
			child := f.invoke("mixed-child")
			f.link("mixed-child-parent", child, parent, "parent-child")
			f.conserved("mixed-open-child", "bead.close", map[string]any{"id": parent}, "ontology_guard_failed", "")
			f.call("close-mixed-child", "bead.close", map[string]any{"id": child}, "")
			f.closeOnce("close-settled-mixed-parent", parent, "all children settled")
			f.reopen("mixed-child-settlement")
		})
	}
}

func TestB08PublicPermissionUnavailableIsTypedAndWritesNothing(t *testing.T) {
	root := b08Root(t, "b08-permission")
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newB08Fixture(t, engine, filepath.Join(root, engine), true)
			id := f.create("permission", "b08-item", nil, nil)
			f.conserved("transition-authority-unavailable", "bead.close", map[string]any{"id": id}, "ontology_guard_failed", "permission")
			f.reopen("permission-refusal")
		})
	}
}

func TestB08PublicFirstCloseIdempotenceAndReopen(t *testing.T) {
	root := b08Root(t, "b08-idempotence")
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newB08Fixture(t, engine, filepath.Join(root, engine), false)
			id := f.create("idempotent", "b08-item", nil, nil)
			first := f.closeOnce("first-close", id, "first exact reason")
			d := f.document(id)
			if d.Fields["status"] != "b08-finished" || d.ClosedAt == nil || len(d.Comments) != 1 || d.Comments[0].Body != "first exact reason" || d.Comments[0].Principal.ID != b08Actor.ID {
				t.Fatalf("first close fields/reason/attribution: %+v", d)
			}
			before := f.state()
			f.clock.Advance(t, time.Second)
			replay := f.call("first-close", "bead.close", map[string]any{"id": id, "reason": "first exact reason"}, "")
			if !replay.Meta.Replayed || !bytes.Equal(first.Result, replay.Result) || !bytes.Equal(b08JSON(t, before), b08JSON(t, f.state())) {
				t.Fatal("same-key close replay changed original receipt or state")
			}
			f.call("new-key-idempotent-close", "bead.close", map[string]any{"id": id}, "")
			after := f.state()
			// A new no-op request may persist its response receipt. Its graph,
			// history, versions, derived state and first close remain unchanged.
			before.Receipts, after.Receipts = nil, nil
			if !bytes.Equal(b08JSON(t, before), b08JSON(t, after)) || !reflect.DeepEqual(d, f.document(id)) {
				t.Fatal("new-key idempotent close changed first close or durable graph")
			}
			f.closeOnce("same-status-additional-reason", id, "second exact reason")
			second := f.document(id)
			if second.ClosedAt == nil || !second.ClosedAt.Equal(*d.ClosedAt) || len(second.Comments) != 2 || second.Comments[1].Body != "second exact reason" {
				t.Fatal("same-status close lost first instant or appended reason")
			}
			f.reopen("closed-receipt")
			replay = f.call("first-close", "bead.close", map[string]any{"id": id, "reason": "first exact reason"}, "")
			if !replay.Meta.Replayed || !bytes.Equal(first.Result, replay.Result) {
				t.Fatal("reopened SQL lost exact first-close receipt")
			}
			f.oneWrite("reopen", "bead.reopen", map[string]any{"id": id, "reason": "reopened exact reason"}, id)
			if got := f.document(id); got.Fields["status"] != "b08-queued" || got.ClosedAt != nil || len(got.Comments) != 3 || got.Comments[2].Body != "reopened exact reason" {
				t.Fatalf("reopen slot/timestamp/reason: %+v", got)
			}
			f.reopen("reopened-receipt")
		})
	}
}

func (f *b08Fixture) readyRoles(label string, refs map[string]model.Ref, want []string) {
	f.t.Helper()
	before := f.state()
	out := f.call(label, "bead.ready", map[string]any{"limit": 1000, "filter": map[string]any{"includeEphemeral": true}}, "")
	var page struct {
		Items []struct{ ID string } `json:"items"`
	}
	if json.Unmarshal(out.Result, &page) != nil {
		f.t.Fatal("invalid public ready response")
	}
	got := []string{}
	for _, item := range page.Items {
		found := false
		for role, ref := range refs {
			if ref.ID == item.ID {
				found = true
				got = append(got, role)
			}
		}
		if !found {
			f.t.Fatal("unknown public ready identity", item.ID)
		}
	}
	sort.Strings(got)
	expected := append([]string{}, want...)
	sort.Strings(expected)
	if !reflect.DeepEqual(got, expected) {
		f.t.Fatalf("public %s ready=%v want=%v", label, got, expected)
	}
	if !bytes.Equal(b08JSON(f.t, before), b08JSON(f.t, f.state())) {
		f.t.Fatal("readiness read mutated complete declared state")
	}
	b08Save(f.t, f.root, label+"-ready.json", map[string]any{"roles": got, "expected": expected, "original": json.RawMessage(out.Result)})
}

func TestB08PublicTransitiveAndMixedSettlement(t *testing.T) {
	root := b08Root(t, "b08-settlement")
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, mixed := range []bool{false, true} {
			name := "durable"
			if mixed {
				name = "mixed"
			}
			t.Run(engine+"/"+name, func(t *testing.T) {
				f := newB08Fixture(t, engine, filepath.Join(root, engine, name), false)
				refs := map[string]model.Ref{}
				refs["blocker"] = f.create("settlement-blocker", "b08-item", nil, nil)
				if mixed {
					var invoked workflowplugin.InvokeResult
					out := f.call("invoke-direct", "x.workflow.invoke", map[string]any{"template": "b08", "project": b08Project, "mode": "ephemeral", "checklist": []map[string]any{{"label": "direct", "title": "B08 direct ephemeral", "type": "issue"}}}, "")
					if json.Unmarshal(out.Result, &invoked) != nil || invoked.Plane != "ephemeral" || len(invoked.Items) != 1 || invoked.Items[0].Plane != "ephemeral" || !strings.HasPrefix(invoked.Items[0].ID, workflowplugin.ItemPrefix) {
						t.Fatal("actual ephemeral invocation did not produce a workflow item")
					}
					refs["direct"] = model.RefByID(invoked.Items[0].ID, model.Natural64(0))
				} else {
					refs["direct"] = f.create("settlement-direct", "b08-item", nil, nil)
				}
				refs["transitive"] = f.create("settlement-transitive", "b08-item", nil, nil)
				f.link("direct-blocks", refs["direct"], refs["blocker"], "blocks")
				f.link("transitive-blocks", refs["transitive"], refs["direct"], "blocks")
				f.readyRoles("before-close", refs, []string{"blocker"})
				f.closeOnce("settle-blocker", refs["blocker"], "blocker settled")
				f.readyRoles("after-blocker-close", refs, []string{"direct"})
				f.reopen("blocker-close-reopen-store")
				f.readyRoles("after-blocker-close-reopened-store", refs, []string{"direct"})
				f.oneWrite("reopen-live-blocker", "bead.reopen", map[string]any{"id": refs["blocker"]}, refs["blocker"])
				f.readyRoles("reopened-live-blocker-reblocks", refs, []string{"blocker"})
				f.closeOnce("resettle-blocker", refs["blocker"], "blocker resettled")
				f.readyRoles("direct-ready-again", refs, []string{"direct"})
				if mixed {
					before := f.state()
					f.call("settle-direct", "bead.close", map[string]any{"id": refs["direct"], "reason": "direct settled"}, "")
					after := f.state()
					if !reflect.DeepEqual(before.Records, after.Records) || !reflect.DeepEqual(before.History, after.History) || after.Checkpoint.Generation != before.Checkpoint.Generation+1 {
						t.Fatal("ephemeral close did not publish exactly one provider checkpoint and no core record/history")
					}
					// The frozen workflow contract forbids recreating a completed
					// operational item by reopening its removed target.
					f.conserved("completed-ephemeral-reopen-refusal", "bead.reopen", map[string]any{"id": refs["direct"]}, "invalid_params", "")
				} else {
					f.closeOnce("settle-direct", refs["direct"], "direct settled")
				}
				f.readyRoles("after-transitive-settlement", refs, []string{"transitive"})
				f.oneWrite("reopen-blocker", "bead.reopen", map[string]any{"id": refs["blocker"]}, refs["blocker"])
				// A closed direct target already satisfies the transitive edge;
				// reopening its former blocker cannot reopen that closed target.
				f.readyRoles("after-blocker-reopen", refs, []string{"blocker", "transitive"})
				f.reopen("final-settlement")
				f.readyRoles("after-final-store-reopen", refs, []string{"blocker", "transitive"})
			})
		}
	}
}
