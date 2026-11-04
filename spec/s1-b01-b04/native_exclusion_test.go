package s1b01b04

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

// This exact default exclusion set is independently declared from accepted
// c8eb sqlbuild.ReadyWorkExcludeTypes and domain.DefaultInfraTypes. Explicit
// native --type overrides that default list; Plug's ReadyEligible=false is
// still enforced when a type filter is present. Keep both answers visible.
func nativeExclusionTypes() []string {
	return []string{"merge-request", "gate", "molecule", "rig", "agent", "role", "message", "bug", "task"}
}

type nativeExclusionNode struct {
	ID, Title, Status, Parent string
	Priority                  int
	Type                      string `json:"issue_type"`
}

type nativeExclusionPhase struct {
	Nodes                      map[string]nativeExclusionNode
	Ready, First, Typed, Tasks []string
}

func recordNativeExclusions(t *testing.T, c *nativeCommand, vector nativeVector) map[string]nativeExclusionPhase {
	t.Helper()
	c.run("exclusions-type-config", "config", "set", "types.custom", "merge-request,rig,agent,role")
	phases := map[string]nativeExclusionPhase{}
	for _, typ := range nativeExclusionTypes() {
		if !t.Run("exclusions/"+typ, func(t *testing.T) {
			c.t = t
			c.run("exclusions-"+typ+"-write", "update", vector.SourceIDs["parent"], "--type", typ, "--json")
			phase := nativeExclusionPhase{Nodes: map[string]nativeExclusionNode{}}
			for priority, role := range nativeRoles {
				var rows []nativeExclusionNode
				nativeDecode(t, c.run("exclusions-"+typ+"-show-"+role, "show", vector.SourceIDs[role], "--json"), &rows)
				wantType, wantParent := "task", ""
				if role == "parent" {
					wantType = typ
				} else if role == "child" {
					wantParent = vector.SourceIDs["parent"]
				} else if role == "grandchild" {
					wantParent = vector.SourceIDs["child"]
				}
				if len(rows) != 1 || rows[0].ID != vector.SourceIDs[role] || rows[0].Type != wantType ||
					rows[0].Title != "B01 "+vector.Mode+" "+role || rows[0].Status != "open" || rows[0].Priority != priority || rows[0].Parent != wantParent {
					t.Fatalf("actual native type control changed identity/title/status/priority/parent: %s/%s %+v", typ, role, rows)
				}
				phase.Nodes[role] = rows[0]
			}
			query := func(name string, limit int, args ...string) []string {
				argv := append([]string{"ready", "--json", "--sort", "hybrid", "--limit", strconv.Itoa(limit)}, args...)
				var rows []struct{ ID string }
				nativeDecode(t, c.run("exclusions-"+typ+"-"+name, argv...), &rows)
				ids, seen := []string{}, map[string]bool{}
				for _, row := range rows {
					if row.ID == "" || seen[row.ID] {
						t.Fatal("native exclusion ready duplicated or omitted an identity")
					}
					seen[row.ID], ids = true, append(ids, row.ID)
				}
				return ids
			}
			phase.Ready, phase.First = query("full", 0), query("one", 1)
			phase.Typed, phase.Tasks = query("typed", 0, "--type", typ), query("tasks", 0, "--type", "task")
			wantRoles, typedRoles, taskRoles := []string{"child", "grandchild", "free"}, []string{"parent"}, []string{"child", "grandchild", "free"}
			if typ == "bug" || typ == "task" {
				wantRoles = nativeRoles
			}
			if typ == "task" {
				typedRoles, taskRoles = nativeRoles, nativeRoles
			}
			ids := func(roles []string) []string {
				result := []string{}
				for _, role := range roles {
					result = append(result, vector.SourceIDs[role])
				}
				return result
			}
			if !reflect.DeepEqual(phase.Ready, ids(wantRoles)) || !reflect.DeepEqual(phase.First, ids(wantRoles[:1])) ||
				!reflect.DeepEqual(phase.Typed, ids(typedRoles)) || !reflect.DeepEqual(phase.Tasks, ids(taskRoles)) {
				t.Fatalf("native default/type filters or hidden prefix page differs: %+v", phase)
			}
			phases[typ] = phase
			nativeJSON(t, filepath.Join(c.output, "exclusions-"+typ+"-projection.json"), phase)
			t.Logf("B01_NATIVE_EXCLUSION type=%s vector=%s ready=%v first=%v typed=%v", typ, vector.ID, phase.Ready, phase.First, phase.Typed)
		}) {
			c.t = t
			t.Fatalf("native exclusion %s failed; actual streams retained", typ)
		}
		c.t = t
	}
	return phases
}

func checkNativeExclusionInventory(t *testing.T, path, output string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var census struct {
		Version         string
		Quiesced        bool
		WorkingSetClean bool `json:"working_set_clean"`
		External        []struct{ Path, State string }
	}
	nativeDecode(t, data, &census)
	if census.Version != "beads-inventory/1" || !census.Quiesced || !census.WorkingSetClean {
		t.Fatal("final native exclusion inventory is not versioned, quiesced and clean")
	}
	sources := path + ".sources"
	info, err := os.Lstat(sources)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("final native exclusion inventory sidecars are missing or refused: %v", err)
	}
	for _, external := range census.External {
		rel, err := filepath.Rel(sources, external.Path)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
			continue
		}
		info, err := os.Lstat(external.Path)
		if err != nil || !info.Mode().IsRegular() {
			t.Fatalf("final exclusion sidecar is not regular original source: %s: %v", external.Path, err)
		}
	}
	closure, err := nativeInventory(sources)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range closure {
		if entry["type"] != "file" && entry["type"] != "directory" {
			t.Fatalf("final exclusion sidecar original is nonregular: %+v", entry)
		}
	}
	nativeJSON(t, filepath.Join(output, "exclusions-sidecar-closure.json"), closure)
}

// Every case has its own actual public-created store and refs. A type is
// immutable in Plug's ordinary patch API; do not impersonate a public retype
// with a trusted storage fixture or rewrite an existing captured document.
func replayNativeExclusionVector(t *testing.T, capture nativeCapture, backend string, harness *postgresHarness) {
	t.Helper()
	if len(capture.Exclusions) != len(nativeExclusionTypes()) {
		t.Fatal("missing genuine native exclusion observations")
	}
	for _, typ := range nativeExclusionTypes() {
		if !t.Run("exclusions/"+typ, func(t *testing.T) {
			dsn := ""
			if backend == "postgres" {
				dsn = harness.dsn(t)
			}
			output := filepath.Join(capture.Output, "exclusion-replay-"+backend, typ)
			if err := os.MkdirAll(output, 0o700); err != nil {
				t.Fatal(err)
			}
			base := ontology.DefaultDocument()
			for _, hidden := range nativeExclusionTypes()[:7] {
				definition := base.Types["issue"]
				no := false
				definition.Name, definition.ReadyEligible = "Hidden "+hidden, &no
				base.Types["native_"+hidden] = definition
			}
			base.Types["ordinary_bug"] = base.Types["issue"]
			compiled, problems := ontology.Compile(base, nil, nil)
			if len(problems) != 0 {
				t.Fatalf("explicit native-type category mapping: %+v", problems)
			}
			ids := []string{}
			for _, role := range nativeRoles {
				ids = append(ids, "plug-b01-exclusion-"+typ+"-"+role)
			}
			f := newFixture(t, backend, dsn, service.NewOntology(compiled.Effective), ids)
			r := &nativeReplay{f: f, output: output, refs: map[string]model.Ref{}, nodes: capture.Before.Nodes}
			observed := capture.Exclusions[typ]
			parentType, hidden := "native_"+typ, true
			if typ == "bug" {
				parentType, hidden = "ordinary_bug", false
			} else if typ == "task" {
				parentType, hidden = "issue", false
			}
			bindings := map[string]any{}
			for _, role := range nativeRoles {
				node, exists := observed.Nodes[role]
				if !exists || node.ID != capture.Vector.SourceIDs[role] {
					t.Fatal("native type observation lost captured source identity")
				}
				beadType := "issue"
				if role == "parent" {
					beadType = parentType
				}
				commit := r.write("exclusion-create-"+role, "bead.create", map[string]any{
					"type": beadType, "fields": map[string]any{"title": node.Title, "priority": node.Priority},
				})
				actual := commit.Results[0].Ref
				if actual.ID == "" || actual.ID == node.ID || !actual.Branch.IsZero() {
					t.Fatal("public exclusion create lacks its distinct returned-ref binding")
				}
				r.refs[role] = actual
				bindings[role] = map[string]any{"native": node, "actualPlugRef": actual, "plugType": beadType}
			}
			for _, edge := range [][2]string{{"child", "parent"}, {"grandchild", "child"}} {
				r.write("exclusion-link-"+edge[0], "bead.link", map[string]any{"from": r.refs[edge[0]], "to": r.refs[edge[1]], "kind": "parent-child"})
			}
			nativeJSON(t, filepath.Join(output, "source-type-and-ref-bindings.json"), map[string]any{"bindings": bindings, "ontology": base})
			r.observeNativeExclusion(capture, typ, parentType, hidden, false, "types")
			if backend != "memory" {
				first := r.call("types-reopen-first", "bead.ready", map[string]any{"limit": 1}, false)
				if cursor, _ := first["nextCursor"].(string); cursor == "" {
					t.Fatal("exclusion multirow state lacks an actual signed continuation")
				}
				r.reopen("types-" + typ)
				want := nativeRoles
				if hidden {
					want = nativeRoles[1:]
				}
				if !reflect.DeepEqual(r.drain("types-across-reopen", first), r.wantIDs(want)) {
					t.Fatal("hidden prefix enumeration changed across actual SQL reopen")
				}
				r.observeNativeExclusion(capture, typ, parentType, hidden, false, "types-reopened")
			}
			if typ == "task" {
				// Native task restoration above is the real reference control.
				// The fixed-kernel template flag below is a separate Plug extension;
				// no native template representation or typed parity is fabricated.
				parentTest := r.f.t
				if !t.Run("template-extension", func(t *testing.T) {
					r.f.t = t
					defer func() { r.f.t = parentTest }()
					before := r.snapshot()
					prior := r.rawHierarchyDocument("template-preimage", "parent")
					commit := r.write("template-enable", "bead.update", map[string]any{"id": r.refs["parent"], "fields": map[string]any{"template": true}})
					r.assertHierarchyWrite("template-enable", "parent", before, commit)
					after := r.rawHierarchyDocument("template-postimage", "parent")
					postimage := nativeDocumentBytes(t, after)
					if value, exists := prior.Fields["template"]; exists {
						after.Fields["template"] = value
					} else {
						delete(after.Fields, "template")
					}
					if !bytes.Equal(nativeDocumentBytes(t, prior), nativeDocumentBytes(t, after)) {
						t.Fatal("template patch changed non-target source fields")
					}
					nativeJSON(t, filepath.Join(output, "template-non-target-fields.json"), map[string]any{
						"before": prior, "after": json.RawMessage(postimage), "conservedProjection": after,
						"scope": "Only the template field may change; prior value and presence are restored for full remaining-record comparison",
					})
					r.observeNativeExclusion(capture, typ, parentType, true, true, "template")
					if backend != "memory" {
						r.reopen("template")
						r.observeNativeExclusion(capture, typ, parentType, true, true, "template-reopened")
					}
				}) {
					t.Fatal("Plug template extension failed; actual evidence retained")
				}
			}
			t.Logf("B01_NATIVE_EXCLUSION_REPLAY type=%s vector=%s run=%s engine=%s hidden=%v", typ, capture.Vector.ID, capture.Run.ID, backend, hidden)
		}) {
			t.Fatalf("native exclusion replay %s failed; actual evidence retained", typ)
		}
	}
}

func (r *nativeReplay) observeNativeExclusion(capture nativeCapture, typ, parentType string, hidden, template bool, label string) {
	r.f.t.Helper()
	before, epoch := r.snapshot(), r.f.lease.Epoch()
	nativeWrite(r.f.t, filepath.Join(r.output, label+"-state-before.json"), before)
	observed := capture.Exclusions[typ]
	for _, role := range nativeRoles {
		node := observed.Nodes[role]
		result := r.call(label+"-get-"+role, "bead.get", map[string]any{"id": r.refs[role]}, false)
		bead, ok := result["bead"].(map[string]any)
		fields, fieldsOK := bead["fields"].(map[string]any)
		priority, priorityOK := fields["priority"].(json.Number)
		wantType := "issue"
		if role == "parent" {
			wantType = parentType
		}
		if !ok || !fieldsOK || !priorityOK || bead["id"] != r.refs[role].ID || bead["type"] != wantType ||
			bead["project"] != project || bead["status"] != "todo" || fields["title"] != node.Title || priority.String() != strconv.Itoa(node.Priority) {
			r.f.t.Fatalf("%s public type/identity/status/fields differ for %s", label, role)
		}
		if role == "parent" && (fields["template"] == true) != template {
			r.f.t.Fatal("public template flag differs from the actual requested extension")
		}
		wantParent := ""
		if role == "child" {
			wantParent = r.refs["parent"].ID
		} else if role == "grandchild" {
			wantParent = r.refs["child"].ID
		}
		if parent, _ := bead["parent"].(string); parent != wantParent {
			r.f.t.Fatal("public type projection changed canonical local parent membership")
		}
	}
	r.observeHierarchyClosure(label, capture.Before, capture.Vector)
	want := nativeRoles
	if hidden {
		want = nativeRoles[1:]
	}
	typed := []string{"parent"}
	if parentType == "issue" {
		typed = want
	} else if hidden {
		typed = []string{}
	}
	ordinary := nativeRoles[1:]
	if typ == "task" && !template {
		ordinary = nativeRoles
	}
	resource := []string{"free"}
	if !hidden {
		resource = []string{"parent", "free"}
	}
	queries := []struct {
		name   string
		filter map[string]any
		want   []string
	}{
		{"all", nil, want},
		{"mapped-type", map[string]any{"types": []string{parentType}}, typed},
		{"ordinary-type", map[string]any{"types": []string{"issue"}}, ordinary},
		{"resource-prefix", map[string]any{"resources": []string{r.refs["parent"].ID, r.refs["free"].ID}}, resource},
		{"empty-intersection", map[string]any{"types": []string{"absent-type"}, "resources": []string{r.refs["parent"].ID}}, []string{}},
	}
	actual := map[string][]string{}
	for _, query := range queries {
		params := map[string]any{"limit": 1000}
		if query.filter != nil {
			params["filter"] = query.filter
		}
		full := r.call(label+"-"+query.name+"-full", "bead.ready", params, false)
		params["limit"] = 1
		first := r.call(label+"-"+query.name+"-one", "bead.ready", params, false)
		actual[query.name] = r.ids(full)
		if !reflect.DeepEqual(r.ids(full), r.wantIDs(query.want)) || !reflect.DeepEqual(r.drain(label+"-"+query.name, first, query.filter), r.wantIDs(query.want)) {
			r.f.t.Fatalf("%s/%s failed filtering before one-row cut", label, query.name)
		}
	}
	if !template {
		nativeRoles, err := nativeRolesForIDs(capture.Vector, observed.Ready)
		if err != nil || !reflect.DeepEqual(nativeRoles, want) {
			r.f.t.Fatal("default hidden-type membership differs from its actual native reference")
		}
	}
	after := r.snapshot()
	nativeWrite(r.f.t, filepath.Join(r.output, label+"-state-after.json"), after)
	if !bytes.Equal(before, after) || epoch != r.f.lease.Epoch() {
		r.f.t.Fatal("hidden-type/template/filter reads changed source/history/versions/receipts/all nine derived/facts/authority")
	}
	select {
	case <-r.f.lease.Lost():
		r.f.t.Fatal("hidden-type read lost active authority")
	default:
	}
	parity := any(true)
	if template {
		parity = nil
	}
	nativeJSON(r.f.t, filepath.Join(r.output, label+"-exclusion-comparison.json"), map[string]any{
		"nativeType": typ, "plugType": parentType, "actualNative": observed, "actualPlugQueries": actual,
		"defaultMembershipParity": parity, "typedFilterParity": nil, "templateExtension": template,
		"filterScope": "Native explicit --type overrides default excluded types; Plug type eligibility stays mandatory. Resource/intersection filters and template are separately labelled Plug extensions.",
	})
}
