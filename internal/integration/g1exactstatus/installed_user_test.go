package g1exactstatus_test

import (
	"bytes"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
)

// The optional plugin has a smaller supported write surface than the default
// product. Provision beads through the real no-plugin HTTP configuration, then
// close that endpoint and install the actual fixed User-close configuration.
// No Agent lifecycle grant or replacement test policy is introduced.
func TestG1InstalledUserUsesExistingClosePermitsAndRefusesUncoveredWrites(t *testing.T) {
	eachConfiguredFixture(t, "installed-user", func(t *testing.T, f *fixture) {
		id := f.create(project, "", "installed-user-base")
		batchID := f.create(project, "", "installed-user-batch")
		uncovered := f.create(project, "native-ready-alt", "installed-user-uncovered-source")
		parent := f.create(project, "", "installed-user-parent")
		f.write("bead.create", map[string]any{"type": nativeType, "parent": parent, "fields": map[string]any{"title": "open child", "proof": "checked"}}, "installed-user-child")
		f.installUserPolicy([]authzrpc.LifecycleGrant{
			{Principal: user, Project: ref(project), Type: nativeType, FromStatus: "native-open", ToStatus: "native-done", Actions: []authz.Action{authz.Modify, authz.Transition}, Effect: authz.Allow},
			// Replay requires the current transition's grant too; the receipt
			// itself supplies no reusable permission.
			{Principal: user, Project: ref(project), Type: nativeType, FromStatus: "native-done", ToStatus: "native-done", Actions: []authz.Action{authz.Modify, authz.Transition}, Effect: authz.Allow},
		})
		f.status(id, "native-open") // The real guarded User read remains supported.
		f.refuse(user, project, "bead.create", map[string]any{"type": nativeType, "targetStatus": "native-ready-alt", "fields": map[string]any{"title": "unsupported User create"}}, "user-create", "forbidden")
		f.refuse(user, project, "bead.update", map[string]any{"id": id, "fields": map[string]any{"title": "unsupported User update"}}, "user-update", "forbidden")
		for _, target := range []string{"native-done", "native-done-alt"} {
			f.refuse(user, project, "bead.close", closeParams(id, target, "uncovered exact selector"), "user-exact-selector-"+target, "forbidden")
			f.refuse(user, project, "tx.apply", tx(typedClose(id, target, "uncovered exact selector")), "user-batch-exact-selector-"+target, "forbidden")
		}
		f.refuse(user, project, "bead.close", closeParams(uncovered, "", "uncovered from status"), "user-uncovered-source", "forbidden")
		f.refuse(user, project, "bead.close", closeParams(parent, "", "open child must block"), "user-open-child", "ontology_guard_failed")
		for _, who := range []model.PrincipalRef{sponsoredAgent, outsider} {
			f.refuse(who, project, "bead.close", closeParams(id, "", "no Agent close permit"), "agent-close-"+who.ID, "forbidden")
		}
		params := closeParams(id, "", "existing User close")
		r := f.call(user, project, "bead.close", params, "user-close")
		accepted(t, r, false)
		f.status(id, "native-done")
		d := f.document(id)
		if len(d.Comments) != 1 || d.Comments[0].Body != "existing User close" || d.Comments[0].Principal.ID != user.ID || d.Comments[0].Principal.Kind != "user" || d.ClosedAt == nil {
			t.Fatalf("installed User close reason/principal/time: %+v", d)
		}
		before := f.capture(user, "bead.close", "user-close")
		replay := f.call(user, project, "bead.close", params, "user-close")
		accepted(t, replay, true)
		if !bytes.Equal(r.Result, replay.Result) || f.capture(user, "bead.close", "user-close") != before {
			t.Fatal("installed User replay changed receipt or state")
		}
		batch := tx(map[string]any{"kind": "close", "target": batchID, "reason": "existing User batch close"})
		accepted(t, f.call(user, project, "tx.apply", batch, "user-batch-close"), false)
		f.status(batchID, "native-done")
		for _, who := range []model.PrincipalRef{user, sponsoredAgent} {
			f.refuse(who, project, "bead.reopen", closeParams(id, "native-ready-alt", "uncovered reopen"), "uncovered-reopen-"+who.ID, "forbidden")
			for _, kind := range []string{"patch", "comment", "setMeta"} {
				created := model.RefByKey("uncovered-new")
				followup := map[string]any{"kind": kind, "target": created}
				switch kind {
				case "patch":
					followup["fields"] = map[string]any{"title": "unsupported followup"}
				case "comment":
					followup["body"] = "unsupported followup"
				case "setMeta":
					followup["key"], followup["value"] = "fixture", "unsupported followup"
				}
				f.refuse(who, project, "tx.apply", tx(map[string]any{"kind": "create", "key": "uncovered-new", "type": nativeType, "targetStatus": "native-ready-alt", "fields": map[string]any{"title": "unsupported create"}}, followup), "uncovered-create-"+kind+"-"+who.ID, "forbidden")
			}
		}
		if f.engine.name != "memory" {
			f.reopen()
			f.status(id, "native-done")
			replay = f.call(user, project, "bead.close", params, "user-close")
			accepted(t, replay, true)
			if !bytes.Equal(r.Result, replay.Result) {
				t.Fatal("installed User close receipt changed after SQL reopen")
			}
			f.refuse(user, project, "bead.reopen", closeParams(id, "native-ready-alt", "still uncovered"), "durable-uncovered-reopen", "forbidden")
		}
		t.Log("G1 installed fixed User: current exact default-close grants and tx.apply, guarded read/replay/SQL reopen, unchanged child guard, User create/update and Agent/reopen/create-followup refusals with no writes")
	})
}
