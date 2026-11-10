package g1exactstatus_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (f *fixture) document(id model.Ref) pipeline.Document {
	f.t.Helper()
	m, err := f.store.Meta(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: m.Incarnation})
	if err != nil {
		f.t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Get(f.ctx, []model.Ref{id})
	if err != nil || len(rows) != 1 {
		f.t.Fatalf("document: %+v %v", rows, err)
	}
	d, err := pipeline.DecodeDocument(rows[0].Body)
	if err != nil {
		f.t.Fatal(err)
	}
	return d
}
func (f *fixture) status(id model.Ref, want string) {
	f.t.Helper()
	b := f.get(id)
	if b.ID != id.ID || b.Status != want || b.Fields["status"] != want {
		f.t.Fatalf("public status want %s: %+v", want, b)
	}
}
func closeParams(id model.Ref, target, reason string) map[string]any {
	p := map[string]any{"id": id, "reason": reason}
	if target != "" {
		p["targetStatus"] = target
	}
	return p
}
func tx(ops ...any) map[string]any { return map[string]any{"changeSet": map[string]any{"ops": ops}} }
func typedClose(id model.Ref, target, reason string) map[string]any {
	return map[string]any{"kind": "close", "target": id, "targetStatus": target, "reason": reason}
}

func TestG1ExactNativePublicCreateCloseReopenAndDurability(t *testing.T) {
	eachFixture(t, func(t *testing.T, f *fixture) {
		legacy := f.write("bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "legacy omission"}}, "legacy-create").Results[0].Ref
		f.status(legacy, "todo")
		f.write("bead.close", closeParams(legacy, "", "legacy reason"), "legacy-close")
		f.status(legacy, "done")
		f.write("bead.reopen", closeParams(legacy, "", "legacy reopen"), "legacy-reopen")
		f.status(legacy, "todo")
		initial := f.create(project, "", "native-initial")
		f.status(initial, "native-open")
		id := f.create(project, "native-ready-alt", "native-explicit-create")
		f.status(id, "native-ready-alt")
		if f.get(id).Type != nativeType {
			t.Fatal("custom hyphenated type was relabeled")
		}
		closeRequest := closeParams(id, "native-done-alt", "exact reason")
		first := f.call(actor, project, "bead.close", closeRequest, "exact-close")
		accepted(t, first, false)
		f.status(id, "native-done-alt")
		d := f.document(id)
		if len(d.Comments) != 1 || d.Comments[0].Body != "exact reason" || d.Comments[0].Principal.ID != actor.ID || d.ClosedAt == nil || d.Lease != nil {
			t.Fatalf("close guards/reason/provenance/times: %+v", d)
		}
		replay := f.call(actor, project, "bead.close", closeRequest, "exact-close")
		accepted(t, replay, true)
		if !bytes.Equal(first.Result, replay.Result) {
			t.Fatal("close receipt changed on replay")
		}
		f.refuse(actor, project, "bead.close", closeParams(id, "native-done", "exact reason"), "exact-close", "idempotency_conflict")
		// Exact same-target settlement remains guarded and can append a reason.
		f.write("bead.close", closeParams(id, "native-done-alt", "settled reason"), "exact-settle")
		if got := f.document(id); len(got.Comments) != 2 || got.Comments[1].Body != "settled reason" {
			t.Fatalf("same-target settlement: %+v", got)
		}
		reopenRequest := closeParams(id, "native-ready-alt", "exact reopen")
		reopened := f.call(actor, project, "bead.reopen", reopenRequest, "exact-reopen")
		accepted(t, reopened, false)
		f.status(id, "native-ready-alt")
		f.refuse(actor, project, "bead.reopen", closeParams(id, "native-open", "exact reopen"), "exact-reopen", "idempotency_conflict")
		if got := f.document(id); got.ClosedAt != nil || got.Lease != nil || len(got.Comments) != 3 || got.Comments[2].Principal.ID != actor.ID {
			t.Fatalf("reopen settlement/provenance: %+v", got)
		}
		if f.engine.name != "memory" {
			before := f.capture(actor, "bead.reopen", "exact-reopen")
			f.reopen()
			f.status(id, "native-ready-alt")
			replay = f.call(actor, project, "bead.reopen", reopenRequest, "exact-reopen")
			accepted(t, replay, true)
			if !bytes.Equal(reopened.Result, replay.Result) || f.capture(actor, "bead.reopen", "exact-reopen") != before {
				t.Fatal("persistent reopen changed state or exact receipt")
			}
		}
		f.write("bead.close", closeParams(id, "", "default close"), "native-default-close")
		f.status(id, "native-done")
		f.write("bead.reopen", closeParams(id, "", "default reopen"), "native-default-reopen")
		f.status(id, "native-open")
		for _, target := range []string{"native-working", "native-frozen"} {
			created := f.create(project, target, "create-"+target)
			f.status(created, target)
		}
		t.Log("G1 actual authenticated HTTP create/close/reopen: exact native names and hyphenated type, omitted legacy defaults, reasons/principals, guarded settlement, selector receipt identity, and persistent SQLite/PostgreSQL reopen replay")
	})
}

func TestG1TargetAndSchemaRefusalsPreserveState(t *testing.T) {
	eachFixture(t, func(t *testing.T, f *fixture) {
		id := f.create(project, "", "refusal-base")
		for _, target := range []any{"", "undeclared", "native-done", "native-failed", "native-archived", "foreign-active", "native-ready-no-route", nil, 7} {
			label := "create-refuse-" + string(mustJSON(t, target))
			f.refuse(actor, project, "bead.create", map[string]any{"type": nativeType, "fields": map[string]any{"title": "refuse"}, "targetStatus": target}, label, "invalid_params")
		}
		f.refuse(actor, project, "bead.create", map[string]any{"type": nativeType, "fields": map[string]any{"status": "native-open"}, "targetStatus": "native-ready-alt"}, "conflicting-create-status", "invalid_params")
		f.refuse(actor, project, "bead.create", map[string]any{"type": "unknown-native-type", "fields": map[string]any{}, "targetStatus": "native-open"}, "unknown-native-type", "invalid_params")
		f.refuse(actor, project, "bead.create", map[string]any{"type": nativeType, "fields": map[string]any{"proof": true}, "targetStatus": "native-ready-alt"}, "invalid-custom-field", "invalid_params")
		for _, target := range []any{"", "undeclared", "native-ready-alt", "native-working", "native-frozen", "native-failed", "native-archived", "foreign-active", "native-done-no-route", nil, 7} {
			params := closeParams(id, "", "must not append")
			params["targetStatus"] = target
			f.refuse(actor, project, "bead.close", params, "close-refuse-"+string(mustJSON(t, target)), "invalid_params")
		}
		for _, target := range []string{"native-open", "native-ready-alt"} {
			f.refuse(actor, project, "bead.update", map[string]any{"id": id, "fields": map[string]any{"status": target}}, "generic-status-"+target, "ontology_use_slot_verb")
		}
		f.refuse(actor, project, "tx.apply", tx(map[string]any{"kind": "patch", "target": id, "fields": map[string]any{}, "targetStatus": "native-ready-alt"}), "patch-selector-forbidden", "invalid_params")
		f.refuse(outsider, project, "bead.create", map[string]any{"type": nativeType, "fields": map[string]any{}, "targetStatus": "native-ready-alt"}, "unauthorized-create", "forbidden")
		f.refuse(outsider, project, "bead.close", closeParams(id, "native-done-alt", ""), "unauthorized-close", "forbidden")
		f.write("bead.close", closeParams(id, "native-done-alt", "close for reopen"), "refusal-close")
		for _, target := range []any{"", "undeclared", "native-done-alt", "native-working", "native-frozen", "native-archived", "foreign-active", "native-ready-no-route", nil, 7} {
			params := closeParams(id, "", "must not append")
			params["targetStatus"] = target
			f.refuse(actor, project, "bead.reopen", params, "reopen-refuse-"+string(mustJSON(t, target)), "invalid_params")
		}
		f.refuse(outsider, project, "bead.reopen", closeParams(id, "native-ready-alt", ""), "unauthorized-reopen", "forbidden")
		other := f.create(otherProject, "native-frozen", "other-project")
		accepted(t, f.call(actor, otherProject, "bead.close", closeParams(other, "native-done-alt", ""), "other-close"), false)
		f.refuse(actor, otherProject, "bead.reopen", closeParams(other, "native-ready-alt", ""), "other-project-category", "invalid_params")
		t.Log("G1 exact target typed refusals: unknown/archived/out-of-workflow/wrong-category/no-transition, null/empty/type/schema, generic status bypass, permission, project-bound category, and unchanged rows/versions/predicates/history/receipts")
	})
}

func TestG1OrderedExactTargetsKeepGraphGuardsAndAtomicRollback(t *testing.T) {
	eachFixture(t, func(t *testing.T, f *fixture) {
		parent := f.create(project, "", "parent")
		child := f.write("bead.create", map[string]any{"type": nativeType, "parent": parent, "fields": map[string]any{"title": "child", "proof": "checked"}}, "child").Results[0].Ref
		dependent := f.create(project, "", "dependent")
		f.write("bead.link", map[string]any{"from": dependent, "to": parent, "kind": "blocks"}, "dependency")
		f.refuse(actor, project, "bead.close", closeParams(parent, "native-done-alt", "must not append"), "open-child", "ontology_guard_failed")
		f.refuse(actor, project, "bead.close", closeParams(dependent, "native-done-alt", "must not append"), "open-blocker", "ontology_guard_failed")
		f.refuse(actor, project, "tx.apply", tx(typedClose(parent, "native-done-alt", "parent too early"), typedClose(child, "native-done-alt", "child later")), "parent-first-rollback", "ontology_guard_failed")
		c := f.write("tx.apply", tx(typedClose(child, "native-done-alt", "child reason"), typedClose(parent, "native-done-alt", "parent reason"), typedClose(dependent, "native-done-alt", "dependent reason")), "ordered-exact-close")
		if len(c.Results) != 3 {
			t.Fatal(c)
		}
		for _, id := range []model.Ref{child, parent, dependent} {
			f.status(id, "native-done-alt")
			if f.document(id).Comments[0].Principal.ID != actor.ID {
				t.Fatal("ordered close principal lost")
			}
		}
		// An explicit target still runs target-specific required-field and template guards.
		noProof := f.write("bead.create", map[string]any{"type": nativeType, "fields": map[string]any{"title": "no proof"}}, "no-proof").Results[0].Ref
		f.refuse(actor, project, "bead.close", closeParams(noProof, "native-done-alt", ""), "required-proof", "ontology_guard_failed")
		templated := f.write("bead.create", map[string]any{"type": nativeType, "fields": map[string]any{"title": "template", "proof": "checked", "template": true}}, "template").Results[0].Ref
		f.refuse(actor, project, "bead.close", closeParams(templated, "native-done-alt", ""), "template-guard", "ontology_guard_failed")
		f.refuse(actor, project, "tx.apply", tx(map[string]any{"kind": "patch", "target": parent, "fields": map[string]any{"title": "must roll back"}}, typedClose(parent, "native-done-no-route", "must not append")), "prefix-rollback", "invalid_params")
		stale := typedClose(parent, "native-done-alt", "must not append")
		stale["expect"] = []model.Precondition{model.PreconditionByRevision(parent, model.Natural64(1))}
		f.refuse(actor, project, "tx.apply", tx(stale), "stale-close-cas", "precondition_failed")
		f.refuse(actor, project, "tx.apply", tx(map[string]any{"kind": "create", "key": "bad", "type": nativeType, "targetStatus": "native-ready-alt", "fields": map[string]any{"title": "rolled back"}}, typedClose(model.RefByKey("bad"), "native-done-no-route", "")), "create-close-rollback", "invalid_params")
		params := tx(
			map[string]any{"kind": "create", "key": "new", "type": nativeType, "targetStatus": "native-frozen", "fields": map[string]any{"title": "loaded"}},
			map[string]any{"kind": "patch", "target": model.RefByKey("new"), "fields": map[string]any{"proof": "checked"}},
			typedClose(model.RefByKey("new"), "native-done-alt", "ordered reason"),
			map[string]any{"kind": "setMeta", "target": model.RefByKey("new"), "key": "i8.host.close_reason", "value": "preserved metadata"},
		)
		r := f.call(actor, project, "tx.apply", params, "create-close-success")
		c = accepted(t, r, false)
		if len(c.Results) != 4 || c.Results[0].Ref.ID != c.Results[2].Ref.ID {
			t.Fatal(c)
		}
		loaded := c.Results[0].Ref
		f.status(loaded, "native-done-alt")
		d := f.document(loaded)
		if string(d.Metadata["i8.host.close_reason"]) != `"preserved metadata"` || len(d.Comments) != 1 || d.Comments[0].Body != "ordered reason" || d.Comments[0].Principal.ID != actor.ID {
			t.Fatalf("load reason/metadata: %+v", d)
		}
		before := f.capture(actor, "tx.apply", "create-close-success")
		replay := f.call(actor, project, "tx.apply", params, "create-close-success")
		accepted(t, replay, true)
		if !bytes.Equal(r.Result, replay.Result) || before != f.capture(actor, "tx.apply", "create-close-success") {
			t.Fatal("tx replay changed atomic state or receipt")
		}
		reader, err := f.store.Read(f.ctx, driver.View{Incarnation: f.facts.Namespace.Incarnation})
		if err != nil {
			t.Fatal(err)
		}
		versions, err := reader.Versions(f.ctx, loaded, driver.PageReq{Limit: 1000})
		reader.Close()
		if err != nil || len(versions.Items) != 1 || versions.Items[0].Revision.String() != c.Revision.String() {
			t.Fatalf("create/close must be one revision/version: %+v %v", versions, err)
		}
		t.Log("G1 actual tx.apply exact create+close: backward aliases, ordered children/dependencies, required/schema/template/CAS guards, prefix/create rollback, exact reason/metadata/principal, one revision/version and replay")
	})
}
func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
