package s1b01b04

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// The actual captured continuation is replayed after the unchanged historical
// before/defer/after comparison. Physical store reopen and lifecycle reopen are
// different controls and both must execute in the same current-source unit.
func (r *nativeReplay) replayHierarchyContinuations(capture nativeCapture, coverage *nativeReplayCoverage) {
	var inputs nativeInputs
	nativeDecode(r.f.t, nativeInputsBytes, &inputs)
	steps := []struct {
		phase, method, changedRole string
		params                     map[string]any
		extension                  bool
	}{
		{"resumed", "bead.reopen", "parent", map[string]any{"id": r.refs["parent"]}, false},
		{"blocked-parent", "bead.link", "parent", map[string]any{"from": r.refs["parent"], "to": r.refs["free"], "kind": "blocks"}, false},
		{"detached-from-blocked", "bead.unlink", "child", map[string]any{"from": r.refs["child"], "to": r.refs["parent"], "kind": "parent-child"}, false},
		{"reparented-to-open", "bead.link", "child", map[string]any{"from": r.refs["child"], "to": r.refs["free"], "kind": "parent-child"}, false},
		{"reattached-to-blocked", "tx.apply", "child", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
			{Kind: model.OpUnlink, Link: &model.LinkOp{Kind: model.OpUnlink, From: r.refs["child"], To: r.refs["free"], LinkKind: "parent-child"}},
			{Kind: model.OpLink, Link: &model.LinkOp{Kind: model.OpLink, From: r.refs["child"], To: r.refs["parent"], LinkKind: "parent-child"}},
		}}}, false},
		{"blocker-closed", "bead.close", "free", map[string]any{"id": r.refs["free"]}, false},
		{"blocker-reopened", "bead.reopen", "free", map[string]any{"id": r.refs["free"]}, false},
		{"restored", "bead.unlink", "parent", map[string]any{"from": r.refs["parent"], "to": r.refs["free"], "kind": "blocks"}, false},
		{"external-added", "bead.link", "free", map[string]any{"from": r.refs["free"], "to": model.Ref{ID: inputs.ExternalHierarchyTarget}, "targetKind": "external", "kind": "parent-child"}, true},
		{"external-removed", "bead.unlink", "free", map[string]any{"from": r.refs["free"], "to": model.Ref{ID: inputs.ExternalHierarchyTarget}, "targetKind": "external", "kind": "parent-child"}, true},
	}
	if len(capture.Transitions) != 9 || len(inputs.HierarchyPhases) != 9 {
		r.f.t.Fatal("actual native continuation or finite expected phase set is incomplete")
	}
	parentTest := r.f.t
	for _, step := range steps {
		ok := parentTest.Run(step.phase, func(t *testing.T) {
			r.f.t = t
			defer func() { r.f.t = parentTest }()
			if coverage != nil {
				coverage.Hierarchy[step.phase] = true
			}
			nativePhase := step.phase
			if step.extension {
				// This is the actual native refusal control, never an invented
				// native typed edge. Plug's successful arm is an extension.
				nativePhase = "external-refused"
			}
			observed, exists := capture.Transitions[nativePhase]
			want, expected := inputs.HierarchyPhases[nativePhase]
			if step.phase == "external-added" {
				// The external arm is not hierarchy, but its unresolved direct
				// subject is excluded by readyDependencies. Ancestors of that
				// subject are not manufactured from the opaque target.
				want = []string{"parent", "child", "grandchild"}
			}
			if !exists || !expected {
				t.Fatal("missing actual native phase or independent expected membership")
			}
			beforeDoc := r.rawHierarchyDocument("hierarchy-"+step.phase+"-before-fields", step.changedRole)
			before := r.sourceImages()
			beforeState := r.snapshot()
			commit := r.write("hierarchy-"+step.phase, step.method, step.params)
			r.assertHierarchyWrite(step.phase, step.changedRole, beforeState, commit)
			afterDoc := r.rawHierarchyDocument("hierarchy-"+step.phase+"-after-fields", step.changedRole)
			r.assertHierarchyNonTargetFields(step.phase, step.method, beforeDoc, afterDoc)
			after := r.sourceImages()
			if before[step.changedRole] == after[step.changedRole] {
				t.Fatalf("%s did not publish its actual changed source image/revision", step.phase)
			}
			for _, role := range nativeRoles {
				if role != step.changedRole && before[role] != after[role] {
					t.Fatalf("%s rewrote unchanged %s source bytes/revision", step.phase, role)
				}
			}
			nativeJSON(t, filepath.Join(r.output, step.phase+"-write-source-delta.json"), map[string]any{
				"beforeRecordHashes": before, "afterRecordHashes": after, "onlyChangedRole": step.changedRole,
			})
			externalTarget := ""
			if step.phase == "external-added" {
				externalTarget = inputs.ExternalHierarchyTarget
			}
			r.observeHierarchy(capture, step.phase, observed, want, externalTarget, step.extension)
			if r.f.backend != "memory" {
				first := r.call(step.phase+"-reopen-first", "bead.ready", map[string]any{"limit": 1}, false)
				cursor, _ := first["nextCursor"].(string)
				if len(want) > 1 && cursor == "" {
					t.Fatal("multirow hierarchy continuation lacks an actual signed cursor")
				}
				r.reopen("hierarchy-" + step.phase)
				if got := r.drain(step.phase+"-across-reopen", first); !reflect.DeepEqual(got, r.wantIDs(want)) {
					t.Fatalf("%s signed continuation changed after actual SQL reopen: %v", step.phase, got)
				}
				r.observeHierarchy(capture, step.phase+"-reopened", observed, want, externalTarget, step.extension)
			}
			t.Logf("B01_NATIVE_HIERARCHY_REPLAY phase=%s vector=%s run=%s engine=%s assertedRoles=%v plugTypedExtension=%v", step.phase, capture.Vector.ID, capture.Run.ID, r.f.backend, want, step.extension)
		})
		if !ok {
			parentTest.Fatalf("actual hierarchy replay %s failed; originals retained", step.phase)
		}
	}
}

// Assert the exact published write boundary, independently of the changed body
// hash. One atomic public operation sequence advances H once and appends one
// version for the changed record, even when tx.apply contains unlink plus link.
func (r *nativeReplay) assertHierarchyWrite(label, changedRole string, before []byte, commit model.Commit) {
	r.f.t.Helper()
	type state struct {
		Meta     driver.Meta
		Versions map[string][]driver.Version
	}
	var old, current state
	if err := json.Unmarshal(before, &old); err != nil {
		r.f.t.Fatal(err)
	}
	after := r.snapshot()
	if err := json.Unmarshal(after, &current); err != nil {
		r.f.t.Fatal(err)
	}
	head, err := old.Meta.Head.Uint64()
	if err != nil || head == ^uint64(0) {
		r.f.t.Fatalf("%s fixture head cannot advance: %s (%v)", label, old.Meta.Head, err)
	}
	if commit.Revision != model.Natural64(head+1) || current.Meta.Head != commit.Revision {
		r.f.t.Fatalf("%s must publish exact H+1: before=%s commit=%s after=%s", label, old.Meta.Head, commit.Revision, current.Meta.Head)
	}
	wantMeta := old.Meta
	wantMeta.Head = current.Meta.Head
	if !reflect.DeepEqual(wantMeta, current.Meta) {
		r.f.t.Fatalf("%s changed store identity/incarnation/history floor", label)
	}
	for _, role := range nativeRoles {
		prior, next := old.Versions[role], current.Versions[role]
		if role != changedRole {
			if !reflect.DeepEqual(prior, next) {
				r.f.t.Fatalf("%s added/changed an unrelated %s version", label, role)
			}
			continue
		}
		if len(next) != len(prior)+1 || !reflect.DeepEqual(prior, next[:len(prior)]) || next[len(prior)].Revision != commit.Revision {
			r.f.t.Fatalf("%s did not append exactly one changed-record version at H+1", label)
		}
	}
	nativeJSON(r.f.t, filepath.Join(r.output, label+"-exact-write-boundary.json"), map[string]any{
		"beforeHead": old.Meta.Head, "afterHead": current.Meta.Head, "commitRevision": commit.Revision,
		"changedRole": changedRole, "beforeVersions": old.Versions, "afterVersions": current.Versions,
	})
}

func (r *nativeReplay) observeHierarchy(capture nativeCapture, label string, observed nativePhase, want []string, externalTarget string, extension bool) {
	r.f.t.Helper()
	before, epoch := r.snapshot(), r.f.lease.Epoch()
	nativeWrite(r.f.t, filepath.Join(r.output, label+"-state-before.json"), before)
	proof := map[string]any{}
	for _, role := range nativeRoles {
		node, exists := observed.Nodes[role]
		if !exists {
			r.f.t.Fatalf("actual native phase lacks %s", role)
		}
		value := r.call(label+"-get-"+role, "bead.get", map[string]any{"id": r.refs[role]}, false)
		bead, ok := value["bead"].(map[string]any)
		status := map[string]string{"open": "todo", "closed": "done"}[node.Status]
		if !ok || status == "" || bead["id"] != r.refs[role].ID || bead["status"] != status {
			r.f.t.Fatalf("%s public status/ref differs from actual native phase for %s: %+v", label, role, bead)
		}
		doc := r.rawHierarchyDocument(label, role)
		wantParent := ""
		for parentRole, sourceID := range capture.Vector.SourceIDs {
			if sourceID == node.Parent {
				wantParent = r.refs[parentRole].ID
			}
		}
		if node.Parent != "" && wantParent == "" {
			r.f.t.Fatal("native local parent is outside the captured source map")
		}
		publicParent, _ := bead["parent"].(string)
		if publicParent != wantParent || (doc.Parent == nil) != (wantParent == "") || doc.Parent != nil && doc.Parent.ID != wantParent {
			r.f.t.Fatalf("%s %s public/canonical Parent differs from captured local hierarchy", label, role)
		}
		localCount, externalCount := 0, 0
		wantExternal := ""
		if role == "free" {
			wantExternal = externalTarget
		}
		for _, link := range doc.Links {
			if link.Kind != "parent-child" {
				continue
			}
			if link.TargetKind == model.LinkTargetExternal {
				externalCount++
				if wantExternal == "" || link.To.ID != wantExternal || doc.Parent != nil {
					r.f.t.Fatal("typed external edge became a local Parent or changed its opaque target")
				}
			} else {
				localCount++
				if wantParent == "" || link.To.ID != wantParent {
					r.f.t.Fatal("canonical local Parent and retained legacy edge disagree")
				}
			}
		}
		wantLocal := 0
		if wantParent != "" {
			wantLocal = 1
		}
		wantExternalCount := 0
		if wantExternal != "" {
			wantExternalCount = 1
		}
		if localCount != wantLocal || externalCount != wantExternalCount {
			r.f.t.Fatalf("%s lost local/typed external parent-child distinctions for %s", label, role)
		}
		proof[role] = map[string]any{"canonicalParent": doc.Parent, "legacyAndTypedLinks": doc.Links, "actualNativeNode": node}
	}
	r.observeHierarchyClosure(label, observed, capture.Vector)
	full := r.call(label+"-full", "bead.ready", map[string]any{"limit": 1000}, false)
	first := r.call(label+"-one", "bead.ready", map[string]any{"limit": 1}, false)
	if !reflect.DeepEqual(r.ids(full), r.wantIDs(want)) || !reflect.DeepEqual(r.drain(label, first), r.wantIDs(want)) {
		r.f.t.Fatalf("%s exact membership/order/page differs from %v", label, want)
	}
	// A blocked parent precedes the eligible free row by priority; filtering
	// before limit=1 must still emit free instead of an empty front page.
	filter := map[string]any{"resources": []string{r.refs["parent"].ID, r.refs["free"].ID}}
	filteredRoles := []string{}
	for _, role := range want {
		if role == "parent" || role == "free" {
			filteredRoles = append(filteredRoles, role)
		}
	}
	filtered := r.call(label+"-filtered", "bead.ready", map[string]any{"limit": 1, "filter": filter}, false)
	if got := r.drain(label+"-filtered", filtered, filter); !reflect.DeepEqual(got, r.wantIDs(filteredRoles)) {
		r.f.t.Fatalf("%s filters/eligibility did not precede page slicing: %v", label, got)
	}
	actualNativeRoles, err := nativeRolesForIDs(capture.Vector, observed.Ready)
	if err != nil || !extension && !reflect.DeepEqual(actualNativeRoles, want) || extension && !reflect.DeepEqual(actualNativeRoles, nativeRoles) {
		r.f.t.Fatalf("%s actual native roles=%v want=%v: %v", label, actualNativeRoles, want, err)
	}
	actualNativeFirst, err := nativeRolesForIDs(capture.Vector, observed.First)
	if err != nil || !reflect.DeepEqual(actualNativeFirst, actualNativeRoles[:1]) {
		r.f.t.Fatalf("%s actual native first page=%v full roles=%v: %v", label, actualNativeFirst, actualNativeRoles, err)
	}
	if !reflect.DeepEqual(r.ids(first), r.wantIDs(want[:1])) {
		r.f.t.Fatalf("%s actual Plug first page did not filter blocked rows before its limit", label)
	}
	var parity any = true
	if extension {
		parity = nil // Native refused the typed edge; no typed-arm parity exists.
	}
	nativeJSON(r.f.t, filepath.Join(r.output, label+"-hierarchy-comparison.json"), map[string]any{
		"actualNativeReadyRoles": actualNativeRoles, "assertedPlugReadyRoles": want, "membershipParity": parity,
		"actualNativeFirstRoles": actualNativeFirst, "assertedPlugFirstRoles": want[:1],
		"plugTypedExtension": extension, "nativeTypedEdgeEquivalent": !extension,
		"hierarchyProof": proof, "ruling": "2026-11-04 12:05Z local versus typed external parent-child",
	})
	after := r.snapshot()
	nativeWrite(r.f.t, filepath.Join(r.output, label+"-state-after.json"), after)
	if !bytes.Equal(before, after) || epoch != r.f.lease.Epoch() {
		r.f.t.Fatal("hierarchy public reads changed source/head/incarnation/history/known receipts/authority")
	}
	select {
	case <-r.f.lease.Lost():
		r.f.t.Fatalf("%s lost its active authority", label)
	default:
	}
}

func (r *nativeReplay) observeHierarchyClosure(label string, observed nativePhase, vector nativeVector) {
	r.f.t.Helper()
	want := map[string]driver.DerivedRow{}
	rolesByID := map[string]string{}
	for role, id := range vector.SourceIDs {
		rolesByID[id] = role
	}
	for _, role := range nativeRoles {
		ancestor, depth := role, uint64(0)
		seen := map[string]bool{}
		for {
			if seen[ancestor] {
				r.f.t.Fatal("actual captured hierarchy contains a cycle")
			}
			seen[ancestor] = true
			row, err := pipeline.AncestorRow(r.refs[role].ID, r.refs[ancestor].ID, depth)
			if err != nil {
				r.f.t.Fatal(err)
			}
			want[row.Key] = row
			parent := observed.Nodes[ancestor].Parent
			if parent == "" {
				break
			}
			next, ok := rolesByID[parent]
			if !ok {
				r.f.t.Fatal("actual captured local parent lacks its source identity")
			}
			ancestor, depth = next, depth+1
		}
	}
	reader, err := r.f.store.Read(r.f.ctx, driver.View{Incarnation: r.f.metaSnapshot().Incarnation})
	if err != nil {
		r.f.t.Fatal(err)
	}
	defer reader.Close()
	rows, err := reader.DerivedRows(r.f.ctx, driver.DerivedAncestorClosure, "")
	if err != nil || len(rows) != len(want) {
		r.f.t.Fatalf("%s closure cardinality=%d want=%d error=%v", label, len(rows), len(want), err)
	}
	for _, row := range rows {
		expected, ok := want[row.Key]
		if !ok || !bytes.Equal(row.Value, expected.Value) {
			r.f.t.Fatalf("%s actual closure row differs from captured local hierarchy: %+v", label, row)
		}
		delete(want, row.Key)
	}
	if len(want) != 0 {
		r.f.t.Fatal("actual closure omitted a captured ancestor")
	}
	nativeJSON(r.f.t, filepath.Join(r.output, label+"-exact-ancestor-closure.json"), rows)
}

// Decode the actual persisted bytes directly. The production decoder may
// project a legacy-only Parent in memory; that would not prove the required
// canonical Parent was persisted by a new local parent-child write.
func (r *nativeReplay) rawHierarchyDocument(label, role string) pipeline.Document {
	r.f.t.Helper()
	reader, err := r.f.store.Read(r.f.ctx, driver.View{Incarnation: r.f.metaSnapshot().Incarnation})
	if err != nil {
		r.f.t.Fatal(err)
	}
	defer reader.Close()
	rows, err := reader.Get(r.f.ctx, []driver.Ref{r.refs[role]})
	if err != nil || len(rows) != 1 || rows[0].Ref.ID != r.refs[role].ID {
		r.f.t.Fatalf("raw persisted hierarchy %s: rows=%d error=%v", role, len(rows), err)
	}
	nativeWrite(r.f.t, filepath.Join(r.output, label+"-raw-source-"+role+".json"), rows[0].Body)
	var doc pipeline.Document
	if err := json.Unmarshal(rows[0].Body, &doc); err != nil || doc.ID != r.refs[role].ID {
		r.f.t.Fatalf("raw hierarchy source identity/JSON: %v", err)
	}
	return doc
}
