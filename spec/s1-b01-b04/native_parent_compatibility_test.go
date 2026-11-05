package s1b01b04

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These are Plug storage-compatibility extensions of the actual captured four
// roles. Seed publishes real driver records/history and exact typed source
// facts; it does not stand in for native execution, import, or product writes.
// Every existing genuine native capture and public replay must finish first.
func (r *nativeReplay) replayParentCompatibility(capture nativeCapture) {
	parent := r.f.t
	run := func(name string, body func()) {
		if !parent.Run("compatibility/"+name, func(t *testing.T) {
			r.f.t = t
			defer func() { r.f.t = parent }()
			body()
		}) {
			parent.Fatalf("parent compatibility %s failed; original evidence retained", name)
		}
	}
	parents := map[string]string{"child": "parent", "grandchild": "child"}
	forms := map[string]string{}
	for _, form := range []string{"canonical-only", "legacy-only", "matching-forms"} {
		run(form, func() {
			bodies := map[string][]byte{}
			graphs := map[string]*conformance.FixtureGraphSource{}
			for _, role := range []string{"child", "grandchild"} {
				doc := r.rawHierarchyDocument("compatibility-"+form+"-preimage", role)
				ref := r.refs[parents[role]]
				for _, edge := range doc.Links {
					if edge.Kind != "parent-child" || edge.TargetKind == model.LinkTargetExternal || edge.To.ID != ref.ID {
						r.f.t.Fatal("compatibility preimage has an unexpected relation; do not hide it by seeding")
					}
				}
				doc.Parent, doc.Links = nil, nil
				if form != "legacy-only" {
					doc.Parent = &ref
				}
				graph := &conformance.FixtureGraphSource{Parent: &ref}
				if form != "canonical-only" {
					doc.Links = []pipeline.Link{{To: ref, Kind: "parent-child"}}
					graph.Links = []driver.SourceLink{{To: ref, Kind: "parent-child"}}
				}
				bodies[role], graphs[role], forms[role] = nativeDocumentBytes(r.f.t, doc), graph, form
			}
			r.seedCompatibility("compatibility-"+form, bodies, graphs)
			r.observeParentCompatibility(capture, "compatibility-"+form, parents, forms, nativeRoles, "")
			r.reopenParentCompatibility(capture, "compatibility-"+form, parents, forms, nativeRoles, "")
		})
	}
	link := func(from, to string) map[string]any {
		return map[string]any{"from": r.refs[from], "to": r.refs[to], "kind": "parent-child"}
	}
	linkOp := func(from, to string) model.Op {
		return model.Op{Kind: model.OpLink, Link: &model.LinkOp{Kind: model.OpLink, From: r.refs[from], To: r.refs[to], LinkKind: "parent-child"}}
	}
	move := func(role string, to *model.Ref) model.Op {
		return model.Op{Kind: model.OpReparent, Reparent: &model.ReparentOp{Kind: model.OpReparent, Target: r.refs[role], Parent: to}}
	}
	other := r.refs["free"]
	refusals := []struct {
		name, method, code string
		params             map[string]any
	}{
		{"conflicting-parent", "bead.link", "invalid_params", link("child", "free")},
		{"staged-prefix-rollback", "tx.apply", "invalid_params", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
			{Kind: model.OpPatch, Patch: &model.PatchOp{Kind: model.OpPatch, Target: r.refs["free"], Fields: map[string]any{"title": "must not persist"}}},
			linkOp("child", "free"),
		}}}},
		{"two-parents-rollback", "tx.apply", "invalid_params", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{linkOp("free", "parent"), linkOp("free", "child")}}}},
		{"missing-parent", "bead.link", "not_found", map[string]any{"from": r.refs["free"], "to": model.Ref{ID: "plug-b01-parent-does-not-exist"}, "kind": "parent-child"}},
		{"self-parent", "bead.link", "ontology_guard_failed", link("free", "free")},
		{"nonzero-parent-branch", "bead.link", "invalid_params", map[string]any{"from": r.refs["free"], "to": model.RefByID(r.refs["parent"].ID, model.Natural64(1)), "kind": "parent-child"}},
		{"parent-cycle", "bead.link", "ontology_guard_failed", link("parent", "grandchild")},
		{"retained-link-reparent", "tx.apply", "invalid_params", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{move("child", &other)}}}},
		{"retained-link-detach", "tx.apply", "invalid_params", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{move("child", nil)}}}},
		{"retained-endpoint-retype", "bead.link", "ontology_guard_failed", map[string]any{"from": r.refs["child"], "to": r.refs["parent"], "kind": "blocks"}},
	}
	for _, tc := range refusals {
		run("refusal/"+tc.name, func() {
			r.compatibilityRefusal("compatibility-"+tc.name, tc.method, tc.params, tc.code)
		})
	}
	run("same-target-settlement", func() {
		before := r.snapshot()
		commit := r.write("compatibility-same-target", "bead.link", link("child", "parent"))
		r.assertParentCompatibilitySettlement("compatibility-same-target", "bead.link", before, commit)
	})
	run("atomic-unlink-reparent", func() {
		before := r.snapshot()
		prior := r.rawHierarchyDocument("compatibility-atomic-preimage", "child")
		unlink := model.Op{Kind: model.OpUnlink, Link: &model.LinkOp{Kind: model.OpUnlink, From: r.refs["child"], To: r.refs["parent"], LinkKind: "parent-child"}}
		commit := r.write("compatibility-atomic-unlink-reparent", "tx.apply", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{unlink, move("child", &other)}}})
		r.assertHierarchyWrite("compatibility-atomic-unlink-reparent", "child", before, commit)
		after := r.rawHierarchyDocument("compatibility-atomic-postimage", "child")
		r.assertHierarchyNonTargetFields("compatibility-atomic-unlink-reparent", "tx.apply", prior, after)
		parents["child"], forms["child"] = "free", "canonical-only"
		r.observeParentCompatibility(capture, "compatibility-atomic-unlink-reparent", parents, forms, nativeRoles, "")
		r.reopenParentCompatibility(capture, "compatibility-atomic-unlink-reparent", parents, forms, nativeRoles, "")
	})
	// A local interpretation would create free -> grandchild -> child -> free.
	// The opaque external arm must neither resolve that local ID nor make a cycle.
	externalID := r.refs["grandchild"].ID
	external := map[string]any{"from": r.refs["free"], "to": model.Ref{ID: externalID}, "targetKind": "external", "kind": "parent-child"}
	for _, method := range []string{"bead.link", "bead.unlink"} {
		name := "external-same-local-id-added"
		want, target := []string{"parent", "child", "grandchild"}, externalID
		if method == "bead.unlink" {
			name, want, target = "external-same-local-id-removed", nativeRoles, ""
		}
		run(name, func() {
			before := r.snapshot()
			prior := r.rawHierarchyDocument("compatibility-"+name+"-preimage", "free")
			commit := r.write("compatibility-"+name, method, external)
			r.assertHierarchyWrite("compatibility-"+name, "free", before, commit)
			after := r.rawHierarchyDocument("compatibility-"+name+"-postimage", "free")
			r.assertHierarchyNonTargetFields("compatibility-"+name, method, prior, after)
			r.observeParentCompatibility(capture, "compatibility-"+name, parents, forms, want, target)
			r.reopenParentCompatibility(capture, "compatibility-"+name, parents, forms, want, target)
			if method == "bead.link" {
				// No retained-local-link detach guard applies to this external arm.
				before := r.snapshot()
				commit := r.write("compatibility-external-detach", "tx.apply", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{move("free", nil)}}})
				r.assertParentCompatibilitySettlement("compatibility-external-detach", "tx.apply", before, commit)
				r.observeParentCompatibility(capture, "compatibility-external-detach", parents, forms, want, target)
			}
		})
	}
	valid := r.rawHierarchyDocument("compatibility-malformed-valid-preimage", "child")
	validBytes := nativeDocumentBytes(r.f.t, valid)
	localParent := r.refs["free"]
	malformed := []struct {
		name   string
		parent *model.Ref
		links  []pipeline.Link
	}{
		{"conflicting-forms", &localParent, []pipeline.Link{{To: r.refs["parent"], Kind: "parent-child"}}},
		{"multiple-legacy-parents", nil, []pipeline.Link{{To: localParent, Kind: "parent-child"}, {To: r.refs["parent"], Kind: "parent-child"}}},
		{"duplicate-legacy-parent", nil, []pipeline.Link{{To: localParent, Kind: "parent-child"}, {To: localParent, Kind: "parent-child"}}},
		{"canonical-branch", nativeCompatibilityRef(model.RefByID(localParent.ID, model.Natural64(1))), nil},
		{"legacy-branch", nil, []pipeline.Link{{To: model.RefByID(localParent.ID, model.Natural64(1)), Kind: "parent-child"}}},
		{"canonical-key", nativeCompatibilityRef(model.Ref{Key: "unresolved-parent"}), nil},
		{"legacy-key", nil, []pipeline.Link{{To: model.Ref{Key: "unresolved-parent"}, Kind: "parent-child"}}},
	}
	for _, tc := range malformed {
		run("stored-refusal/"+tc.name, func() {
			doc := valid
			doc.Parent, doc.Links = tc.parent, tc.links
			label := "compatibility-stored-" + tc.name
			body := nativeDocumentBytes(r.f.t, doc)
			// No hierarchy is certified from malformed bytes. The typed source
			// fixture is explicit; public reads must refuse the unchanged Body.
			r.seedCompatibility(label, map[string][]byte{"child": body}, map[string]*conformance.FixtureGraphSource{"child": {}})
			r.compatibilityRefusal(label+"-get", "bead.get", map[string]any{"id": r.refs["child"]}, "internal_error")
			r.compatibilityRefusal(label+"-ready", "bead.ready", map[string]any{"limit": 1000}, "internal_error")
			if r.f.backend != "memory" {
				r.reopen(label)
				r.compatibilityRefusal(label+"-reopened-get", "bead.get", map[string]any{"id": r.refs["child"]}, "internal_error")
				r.compatibilityRefusal(label+"-reopened-ready", "bead.ready", map[string]any{"limit": 1}, "internal_error")
			}
			r.seedCompatibility(label+"-restore", map[string][]byte{"child": validBytes}, map[string]*conformance.FixtureGraphSource{"child": {Parent: &localParent}})
		})
	}
	run("restored-readback", func() {
		r.observeParentCompatibility(capture, "compatibility-restored", parents, forms, nativeRoles, "")
	})
}

func nativeCompatibilityRef(ref model.Ref) *model.Ref { return &ref }

func (r *nativeReplay) seedCompatibility(label string, bodies map[string][]byte, graphs map[string]*conformance.FixtureGraphSource) {
	before := r.snapshot()
	changes := []conformance.FixtureChange{}
	for _, role := range nativeRoles {
		if body, exists := bodies[role]; exists {
			if graphs[role] == nil {
				r.f.t.Fatal("compatibility fixture lacks explicit typed source metadata")
			}
			changes = append(changes, conformance.FixtureChange{Ref: r.refs[role], Kind: "bead", Op: model.OpPatch, Body: body, Graph: graphs[role]})
		}
	}
	seeded, err := conformance.Seed(r.f.ctx, r.f.store, r.f.lease, driver.ChangesetHeader{}, changes)
	if err != nil {
		r.f.t.Fatal(err)
	}
	after := r.snapshot()
	var old, current struct {
		Meta     driver.Meta
		Versions map[string][]driver.Version
	}
	nativeDecode(r.f.t, before, &old)
	nativeDecode(r.f.t, after, &current)
	head, err := old.Meta.Head.Uint64()
	if err != nil || head == ^uint64(0) || current.Meta.Head != model.Natural64(head+1) || seeded.Revision != current.Meta.Head || len(seeded.Changes) != len(changes) {
		r.f.t.Fatal("compatibility seed did not publish its one real revision/history entry")
	}
	for _, role := range nativeRoles {
		prior, next := old.Versions[role], current.Versions[role]
		body, changed := bodies[role]
		if !changed {
			if !reflect.DeepEqual(prior, next) {
				r.f.t.Fatal("compatibility seed changed an unrelated record/version")
			}
			continue
		}
		if len(next) != len(prior)+1 || !reflect.DeepEqual(prior, next[:len(prior)]) || next[len(prior)].Revision != seeded.Revision {
			r.f.t.Fatal("compatibility seed did not append exactly one supplied record version")
		}
		actual := r.rawCompatibilityBody(role)
		if !bytes.Equal(actual, body) {
			r.f.t.Fatal("driver normalized literal compatibility input")
		}
	}
	nativeJSON(r.f.t, filepath.Join(r.output, label+"-fixture-seed.json"), map[string]any{
		"before": json.RawMessage(before), "after": json.RawMessage(after), "changeset": seeded,
		"literalBodies": bodies, "typedSourceMetadata": graphs, "membershipParity": nil,
		"qualification": "Plug storage compatibility; real driver fixture history, not a native representation or public write/import claim",
	})
}

func (r *nativeReplay) rawCompatibilityBody(role string) []byte {
	r.renewLease()
	reader, err := r.f.store.Read(r.f.ctx, driver.View{Incarnation: r.f.metaSnapshot().Incarnation})
	if err != nil {
		r.f.t.Fatal(err)
	}
	defer reader.Close()
	rows, err := reader.Get(r.f.ctx, []driver.Ref{r.refs[role]})
	if err != nil || len(rows) != 1 || rows[0].Ref.ID != r.refs[role].ID || !rows[0].Ref.IsID() || !rows[0].Ref.Branch.IsZero() {
		r.f.t.Fatal("compatibility raw body lost its exact record identity")
	}
	return rows[0].Body
}

func (r *nativeReplay) observeParentCompatibility(capture nativeCapture, label string, parents, forms map[string]string, ready []string, externalTarget string) {
	before, epoch := r.snapshot(), r.f.lease.Epoch()
	nativeWrite(r.f.t, filepath.Join(r.output, label+"-state-before.json"), before)
	observed := capture.Before
	observed.Nodes = map[string]nativeNode{}
	proof := map[string]any{}
	for _, role := range nativeRoles {
		node := capture.Before.Nodes[role]
		node.Parent = ""
		wantParent := ""
		if parentRole := parents[role]; parentRole != "" {
			wantParent, node.Parent = r.refs[parentRole].ID, capture.Vector.SourceIDs[parentRole]
		}
		observed.Nodes[role] = node
		value := r.call(label+"-get-"+role, "bead.get", map[string]any{"id": r.refs[role]}, false)
		bead, ok := value["bead"].(map[string]any)
		fields, fieldsOK := bead["fields"].(map[string]any)
		priority, priorityOK := fields["priority"].(json.Number)
		parent, _ := bead["parent"].(string)
		if !ok || !fieldsOK || !priorityOK || bead["id"] != r.refs[role].ID || bead["project"] != project || bead["type"] != "issue" ||
			bead["status"] != "todo" || fields["title"] != node.Title || priority.String() != strconv.Itoa(node.Priority) || parent != wantParent {
			r.f.t.Fatalf("%s public projection changed captured identity/fields/parent for %s", label, role)
		}
		body := r.rawCompatibilityBody(role)
		var raw pipeline.Document
		nativeDecode(r.f.t, body, &raw)
		wantCanonical := wantParent != "" && forms[role] != "legacy-only"
		if (raw.Parent != nil) != wantCanonical || raw.Parent != nil && raw.Parent.ID != wantParent {
			r.f.t.Fatal("compatibility read normalized or lost its raw canonical Parent")
		}
		local, external := 0, 0
		for _, edge := range raw.Links {
			if edge.Kind != "parent-child" {
				continue
			}
			if edge.TargetKind == model.LinkTargetExternal {
				external++
				if role != "free" || externalTarget == "" || edge.To.ID != externalTarget || raw.Parent != nil {
					r.f.t.Fatal("external same-local-ID arm became local hierarchy or changed its target")
				}
			} else {
				local++
				if edge.To.ID != wantParent {
					r.f.t.Fatal("compatibility legacy parent differs from independent topology")
				}
			}
		}
		wantLocal, wantExternal := 0, 0
		if wantParent != "" && forms[role] != "canonical-only" {
			wantLocal = 1
		}
		if role == "free" && externalTarget != "" {
			wantExternal = 1
		}
		if local != wantLocal || external != wantExternal {
			r.f.t.Fatal("compatibility local/external edge cardinality changed")
		}
		proof[role] = map[string]any{"publicBead": bead, "rawBody": json.RawMessage(body), "form": forms[role], "capturedNode": node}
	}
	r.observeHierarchyClosure(label, observed, capture.Vector)
	full := r.call(label+"-full", "bead.ready", map[string]any{"limit": 1000}, false)
	first := r.call(label+"-one", "bead.ready", map[string]any{"limit": 1}, false)
	if !reflect.DeepEqual(r.ids(full), r.wantIDs(ready)) || !reflect.DeepEqual(r.drain(label, first), r.wantIDs(ready)) {
		r.f.t.Fatal("compatibility full/one-row ready membership/order/count differs")
	}
	after := r.snapshot()
	nativeWrite(r.f.t, filepath.Join(r.output, label+"-state-after.json"), after)
	if !bytes.Equal(before, after) || epoch != r.f.lease.Epoch() {
		r.f.t.Fatal("compatibility reads changed raw source/history/versions/receipts/all nine derived/facts/authority")
	}
	select {
	case <-r.f.lease.Lost():
		r.f.t.Fatal("compatibility reads lost active authority")
	default:
	}
	nativeJSON(r.f.t, filepath.Join(r.output, label+"-parent-compatibility.json"), map[string]any{
		"membershipParity": nil, "plugCompatibilityExtension": true, "nativeRepresentationEquivalent": false,
		"capturedVector": capture.Vector.ID, "capturedRun": capture.Run.ID, "capturedBefore": capture.Before,
		"assertedReadyRoles": ready, "assertedParentRoles": parents, "physicalEngine": r.f.backend, "proof": proof,
	})
}

func (r *nativeReplay) reopenParentCompatibility(capture nativeCapture, label string, parents, forms map[string]string, ready []string, externalTarget string) {
	if r.f.backend == "memory" {
		return
	}
	first := r.call(label+"-reopen-first", "bead.ready", map[string]any{"limit": 1}, false)
	if len(ready) > 1 {
		if cursor, _ := first["nextCursor"].(string); cursor == "" {
			r.f.t.Fatal("compatibility multirow control lacks its actual signed cursor")
		}
	}
	r.reopen(label)
	if !reflect.DeepEqual(r.drain(label+"-across-reopen", first), r.wantIDs(ready)) {
		r.f.t.Fatal("compatibility signed continuation changed across SQL reopen")
	}
	r.observeParentCompatibility(capture, label+"-reopened", parents, forms, ready, externalTarget)
}

func (r *nativeReplay) compatibilityRefusal(label, method string, params map[string]any, code string) {
	before, epoch := r.snapshot(), r.f.lease.Epoch()
	r.refuseMembership(label, method, params, code)
	after := r.snapshot()
	if !bytes.Equal(before, after) || epoch != r.f.lease.Epoch() {
		r.f.t.Fatal("compatibility refusal changed source/history/versions/receipts/all nine derived/facts/authority")
	}
	nativeJSON(r.f.t, filepath.Join(r.output, label+"-refusal-conservation.json"), map[string]any{
		"before": json.RawMessage(before), "after": json.RawMessage(after), "expectedErrorCode": code,
		"method": method, "scope": r.calls[len(r.calls)-1], "membershipParity": nil, "plugCompatibilityExtension": true,
	})
}

func (r *nativeReplay) assertParentCompatibilitySettlement(label, method string, before []byte, commit model.Commit) {
	var old, after map[string]json.RawMessage
	nativeDecode(r.f.t, before, &old)
	afterSnapshot := r.snapshot()
	base := filepath.Join(r.output, label+"-quiet-settlement")
	nativeWrite(r.f.t, base+"-before.json", before)
	nativeWrite(r.f.t, base+"-after.json", afterSnapshot)
	nativeJSON(r.f.t, base+"-returned-commit.json", commit)
	nativeDecode(r.f.t, afterSnapshot, &after)
	var prior, current map[string]json.RawMessage
	nativeDecode(r.f.t, old["writeReceipts"], &prior)
	nativeDecode(r.f.t, after["writeReceipts"], &current)
	call := r.calls[len(r.calls)-1]
	key := method + "/" + call.Key
	if !call.Write || call.Method != method || call.Key == "" || prior[key] != nil || len(current) != len(prior)+1 || current[key] == nil {
		r.f.t.Fatal("compatibility quiet call must add precisely its one fresh method/key receipt")
	}
	for key, value := range prior {
		if !bytes.Equal(value, current[key]) {
			r.f.t.Fatal("compatibility quiet call changed a prior receipt")
		}
	}
	var receipt nativeReceiptEvidence
	nativeWrite(r.f.t, base+"-receipt.json", current[key])
	nativeDecode(r.f.t, current[key], &receipt)
	commitEqual := nativeCommitEqual(r.f.t, receipt.Commit, commit)
	nativeJSON(r.f.t, base+"-observation.json", map[string]any{
		"method": method, "idempotencyKey": call.Key, "receiptKey": key,
		"completedReceipt": receipt, "returnedCommit": commit,
		"commitJSONValuesEqual": commitEqual,
		"commitJSONBytesEqual":  bytes.Equal(nativeDocumentBytes(r.f.t, receipt.Commit), nativeDocumentBytes(r.f.t, commit)),
	})
	if receipt.ResponsePending || len(receipt.RequestHash) != 64 || !receipt.ZeroChangeset || receipt.Changeset != nil ||
		!commitEqual {
		r.f.t.Fatal("compatibility quiet receipt lacks its completed response binding and zero changeset")
	}
	delete(old, "writeReceipts")
	delete(after, "writeReceipts")
	var meta driver.Meta
	nativeDecode(r.f.t, old["meta"], &meta)
	if !reflect.DeepEqual(old, after) || commit.Revision != meta.Head {
		r.f.t.Fatal("compatibility quiet call changed head/source/history/versions/derived/facts")
	}
	nativeJSON(r.f.t, filepath.Join(r.output, label+"-quiet-settlement.json"), map[string]any{
		"before": json.RawMessage(before), "after": json.RawMessage(afterSnapshot), "method": method,
		"idempotencyKey": call.Key, "receiptKey": key, "completedReceipt": receipt, "returnedCommit": commit,
	})
}
