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
		first := f.call(f.actor, project, "bead.close", closeRequest, "exact-close")
		accepted(t, first, false)
		f.status(id, "native-done-alt")
		d := f.document(id)
		if len(d.Comments) != 1 || d.Comments[0].Body != "exact reason" || d.Comments[0].Principal.ID != f.actor.ID || d.ClosedAt == nil || d.Lease != nil {
			t.Fatalf("close guards/reason/provenance/times: %+v", d)
		}
		replay := f.call(f.actor, project, "bead.close", closeRequest, "exact-close")
		accepted(t, replay, true)
		if !bytes.Equal(first.Result, replay.Result) {
			t.Fatal("close receipt changed on replay")
		}
		f.refuse(f.actor, project, "bead.close", closeParams(id, "native-done", "exact reason"), "exact-close", "idempotency_conflict")
		// Exact same-target settlement remains guarded and can append a reason.
		f.write("bead.close", closeParams(id, "native-done-alt", "settled reason"), "exact-settle")
		if got := f.document(id); len(got.Comments) != 2 || got.Comments[1].Body != "settled reason" {
			t.Fatalf("same-target settlement: %+v", got)
		}
		reopenRequest := closeParams(id, "native-ready-alt", "exact reopen")
		reopened := f.call(f.actor, project, "bead.reopen", reopenRequest, "exact-reopen")
		accepted(t, reopened, false)
		f.status(id, "native-ready-alt")
		f.refuse(f.actor, project, "bead.reopen", closeParams(id, "native-open", "exact reopen"), "exact-reopen", "idempotency_conflict")
		if got := f.document(id); got.ClosedAt != nil || got.Lease != nil || len(got.Comments) != 3 || got.Comments[2].Principal.ID != f.actor.ID {
			t.Fatalf("reopen settlement/provenance: %+v", got)
		}
		if f.engine.name != "memory" {
			before := f.capture(f.actor, "bead.reopen", "exact-reopen")
			f.reopen()
			f.status(id, "native-ready-alt")
			replay = f.call(f.actor, project, "bead.reopen", reopenRequest, "exact-reopen")
			accepted(t, replay, true)
			if !bytes.Equal(reopened.Result, replay.Result) || f.capture(f.actor, "bead.reopen", "exact-reopen") != before {
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
			f.refuse(f.actor, project, "bead.create", map[string]any{"type": nativeType, "fields": map[string]any{"title": "refuse"}, "targetStatus": target}, label, "invalid_params")
		}
		f.refuse(f.actor, project, "bead.create", map[string]any{"type": nativeType, "fields": map[string]any{"status": "native-open"}, "targetStatus": "native-ready-alt"}, "conflicting-create-status", "invalid_params")
		f.refuse(f.actor, project, "bead.create", map[string]any{"type": "unknown-native-type", "fields": map[string]any{}, "targetStatus": "native-open"}, "unknown-native-type", "invalid_params")
		f.refuse(f.actor, project, "bead.create", map[string]any{"type": nativeType, "fields": map[string]any{"proof": true}, "targetStatus": "native-ready-alt"}, "invalid-custom-field", "invalid_params")
		for _, target := range []any{"", "undeclared", "native-ready-alt", "native-working", "native-frozen", "native-failed", "native-archived", "foreign-active", "native-done-no-route", nil, 7} {
			params := closeParams(id, "", "must not append")
			params["targetStatus"] = target
			f.refuse(f.actor, project, "bead.close", params, "close-refuse-"+string(mustJSON(t, target)), "invalid_params")
		}
		for _, target := range []string{"native-open", "native-ready-alt"} {
			f.refuse(f.actor, project, "bead.update", map[string]any{"id": id, "fields": map[string]any{"status": target}}, "generic-status-"+target, "ontology_use_slot_verb")
		}
		f.refuse(f.actor, project, "tx.apply", tx(map[string]any{"kind": "patch", "target": id, "fields": map[string]any{}, "targetStatus": "native-ready-alt"}), "patch-selector-forbidden", "invalid_params")
		f.refuse(outsider, project, "bead.create", map[string]any{"type": nativeType, "fields": map[string]any{}, "targetStatus": "native-ready-alt"}, "unauthorized-create", "forbidden")
		f.refuse(outsider, project, "bead.close", closeParams(id, "native-done-alt", ""), "unauthorized-close", "forbidden")
		f.write("bead.close", closeParams(id, "native-done-alt", "close for reopen"), "refusal-close")
		for _, target := range []any{"", "undeclared", "native-done-alt", "native-working", "native-frozen", "native-archived", "foreign-active", "native-ready-no-route", nil, 7} {
			params := closeParams(id, "", "must not append")
			params["targetStatus"] = target
			f.refuse(f.actor, project, "bead.reopen", params, "reopen-refuse-"+string(mustJSON(t, target)), "invalid_params")
		}
		f.refuse(outsider, project, "bead.reopen", closeParams(id, "native-ready-alt", ""), "unauthorized-reopen", "forbidden")
		other := f.create(otherProject, "native-frozen", "other-project")
		accepted(t, f.call(f.actor, otherProject, "bead.close", closeParams(other, "native-done-alt", ""), "other-close"), false)
		f.refuse(f.actor, otherProject, "bead.reopen", closeParams(other, "native-ready-alt", ""), "other-project-category", "invalid_params")
		t.Log("G1 exact target typed refusals: unknown/archived/out-of-workflow/wrong-category/no-transition, null/empty/type/schema, generic status bypass, permission, project-bound category, and unchanged rows/versions/predicates/history/receipts")
	})
}

func TestG1OrderedExactTargetsKeepGraphGuardsAndAtomicRollback(t *testing.T) {
	eachFixture(t, func(t *testing.T, f *fixture) {
		parent := f.create(project, "", "parent")
		child := f.write("bead.create", map[string]any{"type": nativeType, "parent": parent, "fields": map[string]any{"title": "child", "proof": "checked"}}, "child").Results[0].Ref
		dependent := f.create(project, "", "dependent")
		f.write("bead.link", map[string]any{"from": dependent, "to": parent, "kind": "blocks"}, "dependency")
		f.refuse(f.actor, project, "bead.close", closeParams(parent, "native-done-alt", "must not append"), "open-child", "ontology_guard_failed")
		f.refuse(f.actor, project, "bead.close", closeParams(dependent, "native-done-alt", "must not append"), "open-blocker", "ontology_guard_failed")
		f.refuse(f.actor, project, "tx.apply", tx(typedClose(parent, "native-done-alt", "parent too early"), typedClose(child, "native-done-alt", "child later")), "parent-first-rollback", "ontology_guard_failed")
		c := f.write("tx.apply", tx(typedClose(child, "native-done-alt", "child reason"), typedClose(parent, "native-done-alt", "parent reason"), typedClose(dependent, "native-done-alt", "dependent reason")), "ordered-exact-close")
		if len(c.Results) != 3 {
			t.Fatal(c)
		}
		for _, id := range []model.Ref{child, parent, dependent} {
			f.status(id, "native-done-alt")
			if f.document(id).Comments[0].Principal.ID != f.actor.ID {
				t.Fatal("ordered close principal lost")
			}
		}
		// An explicit target still runs target-specific required-field and template guards.
		noProof := f.write("bead.create", map[string]any{"type": nativeType, "fields": map[string]any{"title": "no proof"}}, "no-proof").Results[0].Ref
		f.refuse(f.actor, project, "bead.close", closeParams(noProof, "native-done-alt", ""), "required-proof", "ontology_guard_failed")
		templated := f.write("bead.create", map[string]any{"type": nativeType, "fields": map[string]any{"title": "template", "proof": "checked", "template": true}}, "template").Results[0].Ref
		f.refuse(f.actor, project, "bead.close", closeParams(templated, "native-done-alt", ""), "template-guard", "ontology_guard_failed")
		f.refuse(f.actor, project, "tx.apply", tx(map[string]any{"kind": "patch", "target": parent, "fields": map[string]any{"title": "must roll back"}}, typedClose(parent, "native-done-no-route", "must not append")), "prefix-rollback", "invalid_params")
		stale := typedClose(parent, "native-done-alt", "must not append")
		stale["expect"] = []model.Precondition{model.PreconditionByRevision(parent, model.Natural64(1))}
		f.refuse(f.actor, project, "tx.apply", tx(stale), "stale-close-cas", "precondition_failed")
		f.refuse(f.actor, project, "tx.apply", tx(map[string]any{"kind": "create", "key": "bad", "type": nativeType, "targetStatus": "native-ready-alt", "fields": map[string]any{"title": "rolled back"}}, typedClose(model.RefByKey("bad"), "native-done-no-route", "")), "create-close-rollback", "invalid_params")
		params := tx(
			map[string]any{"kind": "create", "key": "new", "type": nativeType, "targetStatus": "native-frozen", "fields": map[string]any{"title": "loaded"}},
			map[string]any{"kind": "patch", "target": model.RefByKey("new"), "fields": map[string]any{"proof": "checked"}},
			typedClose(model.RefByKey("new"), "native-done-alt", "ordered reason"),
			map[string]any{"kind": "setMeta", "target": model.RefByKey("new"), "key": "i8.host.close_reason", "value": "preserved metadata"},
		)
		r := f.call(f.actor, project, "tx.apply", params, "create-close-success")
		c = accepted(t, r, false)
		if len(c.Results) != 4 || c.Results[0].Ref.ID != c.Results[2].Ref.ID {
			t.Fatal(c)
		}
		loaded := c.Results[0].Ref
		f.status(loaded, "native-done-alt")
		d := f.document(loaded)
		if string(d.Metadata["i8.host.close_reason"]) != `"preserved metadata"` || len(d.Comments) != 1 || d.Comments[0].Body != "ordered reason" || d.Comments[0].Principal.ID != f.actor.ID {
			t.Fatalf("load reason/metadata: %+v", d)
		}
		before := f.capture(f.actor, "tx.apply", "create-close-success")
		replay := f.call(f.actor, project, "tx.apply", params, "create-close-success")
		accepted(t, replay, true)
		if !bytes.Equal(r.Result, replay.Result) || before != f.capture(f.actor, "tx.apply", "create-close-success") {
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

func TestG1RepeatedExactClosesKeepLastTargetAndEveryPrefixGuard(t *testing.T) {
	eachFixture(t, func(t *testing.T, f *fixture) {
		for _, targets := range [][2]string{{"native-done", "native-done-alt"}, {"native-done-alt", "native-done"}} {
			label := targets[0] + "-then-" + targets[1]
			id := f.create(project, "native-ready-alt", label+"-create")
			params := tx(typedClose(id, targets[0], "first exact reason"),
				map[string]any{"kind": "patch", "target": id, "fields": map[string]any{"title": "between closes"}},
				typedClose(id, targets[1], "last exact reason"),
				map[string]any{"kind": "setMeta", "target": id, "key": "i8.host.close_reason", "value": "last exact reason"})
			r := f.call(f.actor, project, "tx.apply", params, label)
			c := accepted(t, r, false)
			f.status(id, targets[1])
			d := f.document(id)
			if d.Fields["title"] != "between closes" || len(d.Comments) != 2 || d.Comments[0].Body != "first exact reason" || d.Comments[1].Body != "last exact reason" || !d.Comments[1].Time.After(d.Comments[0].Time) || string(d.Metadata["i8.host.close_reason"]) != `"last exact reason"` {
				t.Fatalf("repeated close ordering/final image: %+v", d)
			}
			for _, comment := range d.Comments {
				if comment.Principal.ID != f.actor.ID {
					t.Fatal("repeated close lost reason principal")
				}
			}
			reader, err := f.store.Read(f.ctx, driver.View{Incarnation: f.facts.Namespace.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			versions, err := reader.Versions(f.ctx, id, driver.PageReq{Limit: 1000})
			reader.Close()
			if err != nil || len(versions.Items) != 2 {
				t.Fatalf("repeated closes must commit one additional version: %+v %v", versions, err)
			}
			found := false
			for _, version := range versions.Items {
				found = found || version.Revision.String() == c.Revision.String()
			}
			if !found {
				t.Fatal("repeated close version does not match commit")
			}
			before := f.capture(f.actor, "tx.apply", label)
			replay := f.call(f.actor, project, "tx.apply", params, label)
			accepted(t, replay, true)
			if !bytes.Equal(r.Result, replay.Result) || before != f.capture(f.actor, "tx.apply", label) {
				t.Fatal("repeated close replay changed state or receipt")
			}
		}
		// The final target does not require proof. The earlier target does, so
		// selecting the last target must not skip the earlier close's guard.
		noProof := f.write("bead.create", map[string]any{"type": nativeType, "fields": map[string]any{"title": "earlier guard"}}, "repeated-no-proof").Results[0].Ref
		f.refuse(f.actor, project, "tx.apply", tx(typedClose(noProof, "native-done-alt", "must roll back"), typedClose(noProof, "native-done", "must also roll back")), "earlier-close-guard", "ontology_guard_failed")
		f.refuse(f.actor, project, "tx.apply", tx(typedClose(noProof, "native-done-no-route", "must roll back"), typedClose(noProof, "native-done", "must also roll back")), "earlier-close-transition", "invalid_params")
		f.status(noProof, "native-open")
		t.Log("G1 repeated exact closes in both target orders: last target, both ordered reasons/principals, one version, replay, and earlier guard/transition atomic refusal")
	})
}

func TestG1ExactCreateIntentSurvivesOrdinaryFollowupOperations(t *testing.T) {
	eachFixture(t, func(t *testing.T, f *fixture) {
		for _, kind := range []string{"patch", "comment", "setMeta", "combined"} {
			created := model.RefByKey("new")
			ops := []any{map[string]any{"kind": "create", "key": "new", "type": nativeType, "targetStatus": "native-ready-alt", "fields": map[string]any{"title": "initial", "proof": "checked"}}}
			if kind == "patch" || kind == "combined" {
				ops = append(ops, map[string]any{"kind": "patch", "target": created, "fields": map[string]any{"title": "updated"}})
			}
			if kind == "comment" || kind == "combined" {
				ops = append(ops, map[string]any{"kind": "comment", "target": created, "body": "ordinary reason"})
			}
			if kind == "setMeta" || kind == "combined" {
				ops = append(ops, map[string]any{"kind": "setMeta", "target": created, "key": "i8.host.close_reason", "value": "ordinary metadata"})
			}
			// Other new resources keep their own omitted or different explicit
			// selector, even when their follow-up operations share this batch.
			ops = append(ops,
				map[string]any{"kind": "create", "key": "default", "type": nativeType, "fields": map[string]any{"title": "default"}},
				map[string]any{"kind": "patch", "target": model.RefByKey("default"), "fields": map[string]any{"title": "default updated"}},
				map[string]any{"kind": "create", "key": "frozen", "type": nativeType, "targetStatus": "native-frozen", "fields": map[string]any{"title": "frozen"}},
				map[string]any{"kind": "setMeta", "target": model.RefByKey("frozen"), "key": "fixture", "value": "frozen metadata"})
			params := tx(ops...)
			label := "create-followup-" + kind
			r := f.call(f.actor, project, "tx.apply", params, label)
			c := accepted(t, r, false)
			if len(c.Results) != len(ops) {
				t.Fatalf("create/follow-up results: %+v", c)
			}
			id := c.Results[0].Ref
			f.status(id, "native-ready-alt")
			f.status(c.Results[len(c.Results)-4].Ref, "native-open")
			f.status(c.Results[len(c.Results)-2].Ref, "native-frozen")
			d := f.document(id)
			if (kind == "patch" || kind == "combined") && d.Fields["title"] != "updated" {
				t.Fatal("create selector lost ordinary title update")
			}
			if (kind == "comment" || kind == "combined") && (len(d.Comments) != 1 || d.Comments[0].Body != "ordinary reason" || d.Comments[0].Principal.ID != f.actor.ID) {
				t.Fatal("create selector lost ordinary reason/principal")
			}
			if (kind == "setMeta" || kind == "combined") && string(d.Metadata["i8.host.close_reason"]) != `"ordinary metadata"` {
				t.Fatal("create selector lost ordinary metadata")
			}
			before := f.capture(f.actor, "tx.apply", label)
			replay := f.call(f.actor, project, "tx.apply", params, label)
			accepted(t, replay, true)
			if !bytes.Equal(r.Result, replay.Result) || before != f.capture(f.actor, "tx.apply", label) {
				t.Fatal("create/follow-up replay changed state or receipt")
			}
		}
		for _, target := range []string{"native-open", "native-ready-alt"} {
			f.refuse(f.actor, project, "tx.apply", tx(
				map[string]any{"kind": "create", "key": "invalid", "type": nativeType, "targetStatus": "native-ready-alt", "fields": map[string]any{"title": "must roll back"}},
				map[string]any{"kind": "patch", "target": model.RefByKey("invalid"), "fields": map[string]any{"status": target}}), "new-generic-status-"+target, "ontology_use_slot_verb")
		}
		f.refuse(f.actor, project, "tx.apply", tx(
			map[string]any{"kind": "create", "key": "invalid-schema", "type": nativeType, "targetStatus": "native-ready-alt", "fields": map[string]any{"title": "must roll back"}},
			map[string]any{"kind": "patch", "target": model.RefByKey("invalid-schema"), "fields": map[string]any{"proof": true}}), "create-followup-schema", "invalid_params")
		t.Log("G1 exact noninitial create plus no-close patch/comment/metadata: per-resource selectors, omitted default, reasons/principals, metadata, replay, schema and generic-status rollback")
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
