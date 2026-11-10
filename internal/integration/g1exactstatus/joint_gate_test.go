package g1exactstatus_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
)

func TestG1B14JointFactoryKeepsInstalledGateCatalogAndPermissionObserver(t *testing.T) {
	// This trusted fixture explicitly installs gate in the selected catalog.
	// The product default and B14's public installation controls stay separate.
	document := nativeDocument()
	gate := document.Types[nativeType]
	gate.Name = "Installed gate"
	document.Types[ontology.GateType] = gate
	eachDocumentFixture(t, "joint-installed-gates", document, func(t *testing.T, f *fixture) {
		gateParams := func(target, title string) map[string]any {
			params := map[string]any{"type": ontology.GateType, "fields": map[string]any{"title": title, "proof": "checked", "await_type": "timer"}}
			if target != "" {
				params["targetStatus"] = target
			}
			return params
		}
		createGate := func(label string) model.Ref {
			return f.write("bead.create", gateParams("", label), label).Results[0].Ref
		}
		open := createGate("joint-open-gate")
		closed := createGate("joint-closed-gate")
		alternateDone := createGate("joint-alternate-done-gate")
		f.write("bead.close", closeParams(closed, "", "default gate close"), "joint-gate-close")
		f.write("bead.close", closeParams(alternateDone, "native-done-alt", "exact alternate gate close"), "joint-gate-close-alt")
		f.status(closed, "native-done")
		f.status(alternateDone, "native-done-alt")
		native := f.create(project, "", "joint-native-user-close")

		f.composed = true
		f.installUserPolicy([]authzrpc.LifecycleGrant{
			{Principal: user, Project: ref(project), Type: nativeType, FromStatus: "native-open", ToStatus: "native-done", Actions: []authz.Action{authz.Modify, authz.Transition}, Effect: authz.Allow},
			{Principal: user, Project: ref(project), Type: nativeType, FromStatus: "native-done", ToStatus: "native-done", Actions: []authz.Action{authz.Modify, authz.Transition}, Effect: authz.Allow},
		})
		assertGates := func(all bool, want ...model.Ref) {
			t.Helper()
			before := f.capture(user, "gate.list", "")
			result := f.call(user, project, "gate.list", map[string]any{"all": all}, "")
			if result.Error != nil {
				t.Fatalf("joint composed gate.list: %+v", result.Error)
			}
			var page service.BeadPage
			if err := json.Unmarshal(result.Result, &page); err != nil {
				t.Fatal(err)
			}
			gotIDs, wantIDs := []string{}, []string{}
			for _, bead := range page.Items {
				if bead.Type != ontology.GateType || bead.Project != project {
					t.Fatalf("joint gate identity: %+v", bead)
				}
				gotIDs = append(gotIDs, bead.ID)
			}
			for _, id := range want {
				wantIDs = append(wantIDs, id.ID)
			}
			sort.Strings(gotIDs)
			sort.Strings(wantIDs)
			if !reflect.DeepEqual(gotIDs, wantIDs) || before != f.capture(user, "gate.list", "") {
				t.Fatalf("joint gate listing all=%v IDs=%v want=%v or read wrote state", all, gotIDs, wantIDs)
			}
		}
		// All lifts only the installed close-slot exclusion. Another Done status
		// stays visible; resetting this runtime to the default fails this oracle.
		assertGates(false, open, alternateDone)
		assertGates(true, open, closed, alternateDone)
		params := closeParams(native, "", "existing User close beside installed gates")
		closedNative := f.call(user, project, "bead.close", params, "joint-native-close")
		accepted(t, closedNative, false)
		f.status(native, "native-done")

		initial := accepted(t, f.call(sponsoredAgent, project, "bead.create", gateParams("native-open", "joint initial Agent gate"), "joint-agent-initial"), false).Results[0].Ref
		initialTx := accepted(t, f.call(sponsoredAgent, project, "tx.apply", tx(map[string]any{"kind": "create", "type": ontology.GateType, "targetStatus": "native-open", "fields": gateParams("", "joint initial Agent tx gate")["fields"]}), "joint-agent-initial-tx"), false).Results[0].Ref
		f.status(initial, "native-open")
		f.status(initialTx, "native-open")
		f.refuse(sponsoredAgent, project, "bead.create", gateParams("native-ready-alt", "joint noninitial Permission gate"), "joint-agent-noninitial", "ontology_guard_failed")
		f.refuse(sponsoredAgent, project, "tx.apply", tx(map[string]any{"kind": "create", "type": ontology.GateType, "targetStatus": "native-ready-alt", "fields": gateParams("", "joint noninitial Permission tx gate")["fields"]}), "joint-agent-noninitial-tx", "ontology_guard_failed")
		f.refuse(user, project, "bead.close", closeParams(open, "native-done-alt", "uncovered exact gate close"), "joint-user-gate-selector", "forbidden")
		if f.engine.name != "memory" {
			f.reopen()
			f.status(native, "native-done")
			f.status(closed, "native-done")
			f.status(alternateDone, "native-done-alt")
		}
		assertGates(false, open, alternateDone, initial, initialTx)
		assertGates(true, open, closed, alternateDone, initial, initialTx)
		before := f.capture(user, "bead.close", "joint-native-close")
		replay := f.call(user, project, "bead.close", params, "joint-native-close")
		accepted(t, replay, true)
		if !bytes.Equal(closedNative.Result, replay.Result) || before != f.capture(user, "bead.close", "joint-native-close") {
			t.Fatal("joint installed catalog replay changed state or receipt")
		}
		t.Log("G1+B14 actual factory shares one explicitly installed gate/native ontology across reads, Writer and permission observation, including exact alternate Done selection, existing initial creates, noninitial refusals and durable replay")
	})
}
