package dumpformat

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/ontology"
)

// All inputs below are independently authored discrimination archives. They
// are neither native producer output nor evidence of an executed public path.
func strictUnionFixture(t *testing.T, stored string, yaml any) *mergeFixture {
	t.Helper()
	f := newMergeFixture(t, "pl")
	f.manifest.Verification["ontology"] = StrictUnionOntologyVerification
	f.addConfig("config", "status.custom", stored, "public")
	if yaml != nil {
		for i := range f.config {
			if f.config[i].Input.Family == "beads-dir-config" {
				f.config[i].Input.State = "json-config"
				f.config[i].Values = map[string]any{"status.custom": yaml}
			}
		}
	}
	return f
}

func strictUnionBytes(t *testing.T, f *mergeFixture, mutate func(*Manifest, map[string][]byte)) []byte {
	t.Helper()
	return f.bytes(t, func(m *Manifest, members map[string][]byte) {
		for i := range m.Sources {
			if m.Sources[i].Family == "custom_statuses" || m.Sources[i].Family == "custom_types" {
				m.Sources[i].Action = "carry"
			}
		}
		if mutate != nil {
			mutate(m, members)
		}
	})
}

func typeMapping(name string) OntologyDirective {
	return OntologyDirective{ID: "map-" + name, Kind: "map_type", Target: OntologyTarget{Project: "pl", Type: name}, From: name, To: "issue"}
}

func statusMapping(name, target string) OntologyDirective {
	return OntologyDirective{ID: "map-" + name, Kind: "map_status", Target: OntologyTarget{Project: "pl", Type: "issue"}, From: name, To: target}
}

func TestStrictConfigUnionCompleteRawAndEffectiveImage(t *testing.T) {
	f := strictUnionFixture(t, "queued:active,shared:wip", []any{"shared:wip", "shipped:done", "cold:frozen"})
	f.addArchive(t, "custom_statuses", Row{"name": "tableonly", "category": "active"})
	f.addArchive(t, "custom_types", Row{"name": "decision"})
	f.addConfig("config", "types.custom", `["proposal"]`, "public")
	for i := range f.config {
		if f.config[i].Input.Family == "beads-dir-config" {
			f.config[i].Values["types"] = map[string]any{"custom": []any{"decision", "incident"}}
		}
	}
	f.manifest.Ontology = []OntologyDirective{typeMapping("decision"), typeMapping("proposal"), typeMapping("incident")}
	r := &f.records["bead"][0]
	r.Source.Fields["status"], r.Source.Fields["issue_type"] = "queued", "decision"
	r.Body["type"], r.Body["source_type"] = "decision", "decision"
	a, err := Read(bytes.NewReader(strictUnionBytes(t, f, nil)))
	if err != nil {
		t.Fatal(err)
	}
	migration, ok := a.ImportMigration()
	if !ok {
		t.Fatal("missing validated migration")
	}
	doc := migration.Document()
	gotCategories, gotOutcomes := map[string]ontology.Category{}, map[string]ontology.Outcome{}
	for key, status := range doc.Statuses {
		gotCategories[key], gotOutcomes[key] = status.Category, status.Outcome
	}
	wantCategories := map[string]ontology.Category{
		"todo": ontology.Unstarted, "in_progress": ontology.Started, "done": ontology.Completed,
		"failed": ontology.Completed, "canceled": ontology.Canceled, "backlog": ontology.Backlog,
		"queued": ontology.Unstarted, "shared": ontology.Started, "shipped": ontology.Completed,
		"cold": ontology.Backlog, "tableonly": ontology.Unstarted,
	}
	wantOutcomes := map[string]ontology.Outcome{
		"todo": "", "in_progress": "", "done": ontology.Success, "failed": ontology.Failure,
		"canceled": "", "backlog": "", "queued": "", "shared": "", "shipped": ontology.Success, "cold": "", "tableonly": "",
	}
	if !reflect.DeepEqual(gotCategories, wantCategories) || !reflect.DeepEqual(gotOutcomes, wantOutcomes) {
		t.Fatalf("complete effective statuses: categories=%v outcomes=%v", gotCategories, gotOutcomes)
	}
	if len(doc.Types) != 4 || len(doc.Workflows) != 1 || len(doc.Workflows["issue"].Statuses) != 11 {
		t.Fatalf("incomplete union document: %+v", doc)
	}
	for _, name := range []string{"issue", "decision", "proposal", "incident"} {
		if typ, ok := doc.Types[name]; !ok || typ.Workflow != "issue" || len(typ.Fields) != 4 || typ.Fields["labels"].Type != "strings" || !reflect.DeepEqual(typ.Fields["blocked"], ontology.Field{Type: "boolean"}) {
			t.Fatalf("missing effective type %s: %+v", name, typ)
		}
	}
	wantDeclarations := []string{
		"status/cold/frozen", "status/queued/active", "status/shared/wip", "status/shared/wip", "status/shipped/done", "status/tableonly/active",
		"type/decision/", "type/decision/", "type/incident/", "type/proposal/",
	}
	var gotDeclarations []string
	for _, d := range migration.Declarations() {
		if d.Origin == "" {
			t.Fatal("declaration lost source provenance")
		}
		gotDeclarations = append(gotDeclarations, d.Kind+"/"+d.Name+"/"+d.Category)
	}
	sort.Strings(gotDeclarations)
	if !reflect.DeepEqual(gotDeclarations, wantDeclarations) {
		t.Fatalf("complete raw declaration multiset: %v", gotDeclarations)
	}
	for _, kind := range []string{"bead", "edge", "comment", "config", "memory", "beads-archive"} {
		want := append([]Record{}, f.records[kind]...)
		sort.Slice(want, func(i, j int) bool { return recordKey(want[i]) < recordKey(want[j]) })
		if !equalJSON(a.Records(kind), want) {
			t.Fatalf("raw/typed record changed in %s", kind)
		}
	}
	if target, ok := migration.Target("pl", r.ID); !ok || target != (ImportTarget{Type: "decision", Status: "queued"}) {
		t.Fatalf("wrong typed head target: %+v", target)
	}
	assertUnionTransitionGuards(t, doc)
	// Accessors must not permit a changed document, target or declaration to
	// alter the archive's independently admitted compilation.
	doc.Statuses["queued"] = ontology.Status{}
	doc.Types["decision"].Fields["title"] = ontology.Field{}
	decls := migration.Declarations()
	decls[0].Category = "invented"
	migration.targets["pl\x00"+r.ID] = ImportTarget{}
	again, _ := a.ImportMigration()
	if again.Document().Statuses["queued"].Category != ontology.Unstarted || again.Document().Types["decision"].Fields["title"].Type != "string" || again.Declarations()[0].Category == "invented" {
		t.Fatal("migration accessor leaked mutable compilation state")
	}
	if target, _ := again.Target("pl", r.ID); target.Status != "queued" {
		t.Fatal("migration target accessor was not detached")
	}
}

func assertUnionTransitionGuards(t *testing.T, doc ontology.Document) {
	t.Helper()
	workflow := doc.Workflows["issue"]
	if workflow.Slots[ontology.Close] != "done" || workflow.Slots[ontology.Claim] != "in_progress" {
		t.Fatal("union rewrote lifecycle slots")
	}
	foundClaim, foundClose := false, false
	for _, rule := range workflow.Transitions {
		if rule.To == "shared" {
			foundClaim = true
			if !reflect.DeepEqual(rule.From, []string{"todo", "queued", "tableonly"}) || !reflect.DeepEqual(rule.Guards, []ontology.Guard{{Kind: ontology.NotTemplate}, {Kind: ontology.NoOpenBlockers}, {Kind: ontology.Permission}}) {
				t.Fatalf("alias claim weakened template guards: %+v", rule)
			}
		}
		if rule.To == "shipped" {
			foundClose = true
			if len(rule.From) != 0 || !reflect.DeepEqual(rule.Guards, []ontology.Guard{{Kind: ontology.NotTemplate}, {Kind: ontology.NoOpenChildren}, {Kind: ontology.NoOpenBlockers}, {Kind: ontology.Permission}}) {
				t.Fatalf("alias close weakened template guards: %+v", rule)
			}
		}
	}
	if !foundClaim || !foundClose {
		t.Fatal("union lacks guarded incoming alias transitions")
	}
}

func TestStrictConfigUnionStoredAndYAMLIndependent(t *testing.T) {
	for _, test := range []struct {
		name, stored string
		yaml         any
	}{
		{"stored-only", "queued:active", nil}, {"yaml-only", "", []any{"queued:active"}},
		{"compatible-overlap", "queued:active", []any{"queued:active"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := strictUnionFixture(t, test.stored, test.yaml)
			if test.name != "yaml-only" {
				f.addConfig("config", "types.custom", "decision", "public")
			}
			if test.name != "stored-only" {
				for i := range f.config {
					if f.config[i].Input.Family == "beads-dir-config" {
						f.config[i].Values["types.custom"] = []any{"decision"}
					}
				}
			}
			f.manifest.Ontology = []OntologyDirective{typeMapping("decision")}
			a, err := Read(bytes.NewReader(strictUnionBytes(t, f, nil)))
			if err != nil {
				t.Fatal(err)
			}
			m, _ := a.ImportMigration()
			if doc := m.Document(); len(doc.Statuses) != 7 || doc.Statuses["queued"].Category != ontology.Unstarted || len(doc.Types) != 2 || doc.Types["decision"].Workflow != "issue" {
				t.Fatal("independent status/type layer omitted from installed union")
			}
		})
	}
}

func TestStrictConfigUnionExplicitResolutionRetainsConflictingLayers(t *testing.T) {
	for _, stored := range []string{"queued:active", "queued"} {
		f := strictUnionFixture(t, stored, []any{"queued:wip"})
		f.manifest.Ontology = []OntologyDirective{statusMapping("queued", "failed")}
		a, err := Read(bytes.NewReader(strictUnionBytes(t, f, nil)))
		if err != nil {
			t.Fatal(err)
		}
		m, _ := a.ImportMigration()
		if got := m.Document().Statuses["queued"]; got.Category != ontology.Completed || got.Outcome != ontology.Failure {
			t.Fatalf("operator-selected target was not installed: %+v", got)
		}
		decls := m.Declarations()
		var categories []string
		for _, decl := range decls {
			categories = append(categories, decl.Category)
		}
		want := []string{strings.TrimPrefix(strings.TrimPrefix(stored, "queued"), ":"), "wip"}
		sort.Strings(categories)
		sort.Strings(want)
		if !reflect.DeepEqual(categories, want) {
			t.Fatalf("resolution rewrote raw layers: %+v", decls)
		}
		for _, r := range a.Records("config") {
			if r.ID == "config/status.custom" && r.Source.Fields["value"] != stored {
				t.Fatal("stored source changed")
			}
		}
		data, _ := a.Member("config/sources.json")
		if !bytes.Contains(data, []byte(`"queued:wip"`)) {
			t.Fatal("YAML source changed")
		}
	}
}

func TestStrictConfigUnionNamedRefusals(t *testing.T) {
	tests := []struct {
		name, code, entry string
		change            func(*mergeFixture)
	}{
		{"missing migration", "migration-required", "queued", func(f *mergeFixture) { f.manifest.Verification["ontology"] = verificationLimits["ontology"] }},
		{"conflicting categories", "category-conflict", "queued", func(f *mergeFixture) { setUnionYAML(f, map[string]any{"status.custom": []any{"queued:wip"}}) }},
		{"unspecified DB", "category-required", "queued", func(f *mergeFixture) { setStoredStatus(f, "queued") }},
		{"unspecified YAML", "category-required", "queued", func(f *mergeFixture) { setUnionYAML(f, map[string]any{"status.custom": []any{"queued"}}) }},
		{"normalized conflict", "category-conflict", "queued", func(f *mergeFixture) { f.addArchive(t, "custom_statuses", Row{"name": "queued", "category": "done"}) }},
		{"absent type mapping", "mapping-required", "decision", func(f *mergeFixture) { f.addConfig("config", "types.custom", "decision", "public") }},
		{"unknown type target", "unknown-target", "map-decision", func(f *mergeFixture) {
			f.manifest.Ontology = []OntologyDirective{typeMapping("decision")}
			f.manifest.Ontology[0].To = "imagined"
		}},
		{"unknown status target", "unknown-target", "map-queued", func(f *mergeFixture) { f.manifest.Ontology = []OntologyDirective{statusMapping("queued", "imagined")} }},
		{"duplicate directive ID", "duplicate-directive", "map-queued", func(f *mergeFixture) {
			d := statusMapping("queued", "todo")
			f.manifest.Ontology = []OntologyDirective{d, d}
		}},
		{"ambiguous directives", "overlapping-directive", "queued", func(f *mergeFixture) {
			d := statusMapping("queued", "todo")
			e := statusMapping("queued", "done")
			e.ID = "different"
			f.manifest.Ontology = []OntologyDirective{d, e}
		}},
		{"narrow declaration mapping", "malformed-declaration", "map-queued", func(f *mergeFixture) {
			d := statusMapping("queued", "todo")
			d.Target.ID = "pl-1"
			f.manifest.Ontology = []OntologyDirective{d}
		}},
		{"unused directive", "unused-directive", "map-other", func(f *mergeFixture) { f.manifest.Ontology = []OntologyDirective{statusMapping("other", "todo")} }},
		{"invalid category", "malformed-declaration", "queued", func(f *mergeFixture) { setStoredStatus(f, "queued:invented") }},
		{"empty category", "malformed-declaration", "queued:", func(f *mergeFixture) { setStoredStatus(f, "queued:") }},
		{"malformed status value", "malformed-declaration", "status.custom", func(f *mergeFixture) { setStoredStatus(f, map[string]any{"category": "active"}) }},
		{"malformed types JSON", "malformed-declaration", "types.custom", func(f *mergeFixture) { f.addConfig("config", "types.custom", `["decision",false]`, "public") }},
		{"dotted and nested", "malformed-declaration", "status.custom", func(f *mergeFixture) {
			setUnionYAML(f, map[string]any{"status.custom": "queued:active", "status": map[string]any{"custom": []any{"queued:active"}}})
		}},
		{"duplicate same-layer name", "duplicate-declaration", "queued", func(f *mergeFixture) { setStoredStatus(f, "queued:active,queued:wip") }},
		{"duplicate normalized name", "duplicate-declaration", "dupe", func(f *mergeFixture) {
			f.addArchive(t, "custom_statuses", Row{"name": "dupe", "category": "active"})
			f.addArchive(t, "custom_statuses", Row{"name": "dupe", "category": "done"})
		}},
		{"undeclared record status", "undeclared-entry", "phantom", func(f *mergeFixture) { f.records["bead"][0].Source.Fields["status"] = "phantom" }},
		{"undeclared legacy spelling", "undeclared-entry", "frozen", func(f *mergeFixture) { f.records["bead"][0].Source.Fields["status"] = "frozen" }},
		{"undeclared record type", "undeclared-entry", "phantom", func(f *mergeFixture) { f.records["bead"][0].Source.Fields["issue_type"] = "phantom" }},
		{"native key conflict", "ontology-conflict", "todo", func(f *mergeFixture) { setStoredStatus(f, "todo:wip") }},
		{"project ownership absent", "project-scope", "configuration union", func(f *mergeFixture) {
			f.manifest.Projects = append(f.manifest.Projects, Project{Prefix: "other", IDMode: "sequential", ChildCounters: map[string]uint64{}})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := strictUnionFixture(t, "queued:active", nil)
			test.change(f)
			archive, err := Read(bytes.NewReader(strictUnionBytes(t, f, nil)))
			var refused *MigrationRefusal
			if archive != nil || !errors.As(err, &refused) || refused.Code != test.code || refused.Entry != test.entry {
				t.Fatalf("named refusal: archive=%v err=%v, want %s/%s", archive, err, test.code, test.entry)
			}
			if test.code == "category-conflict" && (!strings.Contains(err.Error(), "active") || !strings.Contains(err.Error(), "queued")) {
				t.Fatal("conflict did not name the affected raw entries")
			}
		})
	}
}

func setStoredStatus(f *mergeFixture, value any) {
	for i := range f.records["config"] {
		r := &f.records["config"][i]
		if r.ID == "config/status.custom" {
			r.Source.Fields["value"], r.Body["value"] = value, value
		}
	}
}

func setUnionYAML(f *mergeFixture, values map[string]any) {
	for i := range f.config {
		if f.config[i].Input.Family == "beads-dir-config" {
			f.config[i].Input.State, f.config[i].Values = "json-config", values
		}
	}
}

func TestStrictConfigUnionDoesNotRelaxOtherFamilyOrBodyPolicies(t *testing.T) {
	for _, table := range []string{"routes", "interactions", "federation_peers"} {
		t.Run(table, func(t *testing.T) {
			f := strictUnionFixture(t, "queued:active", nil)
			f.addArchive(t, table, Row{})
			if a, err := Read(bytes.NewReader(strictUnionBytes(t, f, nil))); a != nil || err == nil || !strings.Contains(err.Error(), "unsupported or operational source record") {
				t.Fatalf("strict migration weakened nonempty family %s: %v", table, err)
			}
		})
	}
	for _, name := range []string{"conditional action", "source schema", "body", "secret"} {
		t.Run(name, func(t *testing.T) {
			f := strictUnionFixture(t, "queued:active", nil)
			f.addArchive(t, "custom_statuses", Row{"name": "tableonly", "category": "active"})
			mutate := func(m *Manifest, members map[string][]byte) {
				if name == "conditional action" {
					for i := range m.Sources {
						if m.Sources[i].Family == "custom_statuses" {
							m.Sources[i].Action = "archive"
						}
					}
				}
			}
			if name == "source schema" {
				f.records["beads-archive"][len(f.records["beads-archive"])-1].Source.Fields["extra"] = true
			}
			if name == "body" {
				f.records["bead"][0].Body["status_category"] = "started"
			}
			if name == "secret" {
				f.addConfig("config", "api_key", "raw-secret-control", "public")
			}
			if a, err := Read(bytes.NewReader(strictUnionBytes(t, f, mutate))); a != nil || err == nil {
				t.Fatal("strict migration waived raw mapping/census/credential validation")
			}
		})
	}
	f := strictUnionFixture(t, "queued:active", nil)
	a, err := Read(bytes.NewReader(strictUnionBytes(t, f, func(m *Manifest, members map[string][]byte) {
		members[SourceHistoryMember] = []byte("{}\n")
		m.Files[SourceHistoryMember] = File{SHA256: digest(members[SourceHistoryMember]), Size: int64(len(members[SourceHistoryMember]))}
	})))
	var refused *MigrationRefusal
	if a != nil || !errors.As(err, &refused) || refused.Code != "history-scope" {
		t.Fatalf("strict union widened source-history admission: %v", err)
	}
}

func TestStrictConfigUnionNativeObservationEnvelope(t *testing.T) {
	raw := []byte("status:\n  custom: [queued:active]\n")
	for _, broken := range []string{"", "hash", "size", "bytes", "parsed", "duplicate path"} {
		t.Run(broken, func(t *testing.T) {
			f := strictUnionFixture(t, "", nil)
			observation := map[string]any{
				"path": "/synthetic/.beads/config.yaml", "state": "file", "sha256": digest(raw), "size": len(raw),
				"bytes":  map[string]any{"$bytes": base64.StdEncoding.EncodeToString(raw)},
				"values": map[string]any{"status": map[string]any{"custom": []any{"queued:active"}}},
			}
			switch broken {
			case "hash":
				observation["sha256"] = strings.Repeat("0", 64)
			case "size":
				observation["size"] = len(raw) + 1
			case "bytes":
				observation["bytes"] = map[string]any{"$bytes": "bad!"}
			case "parsed":
				delete(observation, "values")
			}
			observations := []any{observation}
			if broken == "duplicate path" {
				observations = append(observations, observation)
			}
			setUnionYAML(f, map[string]any{"observations": observations})
			data := strictUnionBytes(t, f, nil)
			a, err := Read(bytes.NewReader(data))
			if broken != "" {
				var refused *MigrationRefusal
				if a != nil || !errors.As(err, &refused) || refused.Code != "malformed-declaration" {
					t.Fatalf("bad captured envelope admitted: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			m, _ := a.ImportMigration()
			if m.Document().Statuses["queued"].Category != ontology.Unstarted || len(m.Declarations()) != 1 {
				t.Fatal("captured YAML values omitted")
			}
			retained, _ := a.Member("config/sources.json")
			if !bytes.Contains(retained, []byte(base64.StdEncoding.EncodeToString(raw))) {
				t.Fatal("raw YAML byte envelope lost")
			}
		})
	}
}

func TestStrictConfigUnionNativeCommentOnlyYAML(t *testing.T) {
	for _, control := range []string{
		"comments", "empty", "whitespace", "yml", "substantive", "scalar", "document marker",
		"JSON", "NUL", "invalid UTF8", "control character", "hash", "size", "bytes",
		"duplicate path", "null parsed values", "non-object parsed values",
		"CR comments", "CRLF comments", "CR substantive", "CRLF substantive",
		"Unicode next line", "Unicode line separator", "Unicode paragraph separator",
	} {
		t.Run(control, func(t *testing.T) {
			f := strictUnionFixture(t, "", nil)
			raw, configPath := []byte("# Native defaults\n  # issue-prefix: example\n\n"), "/synthetic/.beads/config.yaml"
			wantOK := control == "comments" || control == "empty" || control == "whitespace" || control == "yml" || control == "CR comments" || control == "CRLF comments"
			switch control {
			case "empty":
				raw = nil
			case "whitespace":
				raw = []byte(" \t\r\n \n")
			case "yml":
				configPath = "/synthetic/.beads/config.yml"
			case "substantive":
				raw = []byte("# defaults\nstatus:\n  custom: [queued:active]\n")
			case "scalar":
				raw = []byte("null\n")
			case "document marker":
				raw = []byte("---\n# defaults\n")
			case "JSON":
				configPath, raw = "/synthetic/.beads/config.json", []byte("{}\n")
			case "NUL":
				raw = []byte("# defaults\x00\n")
			case "invalid UTF8":
				raw = []byte{'#', ' ', 0xff, '\n'}
			case "control character":
				raw = []byte("# defaults\x01\n")
			case "CR comments":
				raw = []byte("# defaults\r  # issue-prefix: example\r \t\r")
			case "CRLF comments":
				raw = []byte("# defaults\r\n  # issue-prefix: example\r\n")
			case "CR substantive":
				raw = []byte("# defaults\rstatus:\r  custom: [queued:active]\r")
			case "CRLF substantive":
				raw = []byte("# defaults\r\nstatus:\r\n  custom: [queued:active]\r\n")
			case "Unicode next line":
				raw = []byte("# defaults\u0085status: active\n")
			case "Unicode line separator":
				raw = []byte("# defaults\u2028status: active\n")
			case "Unicode paragraph separator":
				raw = []byte("# defaults\u2029status: active\n")
			}
			observation := map[string]any{
				"path": configPath, "state": "file", "sha256": digest(raw), "size": len(raw),
				"bytes": map[string]any{"$bytes": base64.StdEncoding.EncodeToString(raw)},
			}
			switch control {
			case "hash":
				observation["sha256"] = strings.Repeat("0", 64)
			case "size":
				observation["size"] = len(raw) + 1
			case "bytes":
				observation["bytes"] = map[string]any{"$bytes": "bad!"}
			case "null parsed values":
				observation["values"] = nil
			case "non-object parsed values":
				observation["values"] = false
			}
			observations := []any{observation}
			if control == "duplicate path" {
				observations = append(observations, observation)
			}
			setUnionYAML(f, map[string]any{"observations": observations})
			a, err := Read(bytes.NewReader(strictUnionBytes(t, f, nil)))
			if !wantOK {
				var refused *MigrationRefusal
				if a != nil || !errors.As(err, &refused) || refused.Code != "malformed-declaration" {
					t.Fatalf("unparsed configuration/envelope admitted: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			migration, _ := a.ImportMigration()
			retained, _ := a.Member("config/sources.json")
			if len(migration.Declarations()) != 0 || !bytes.Contains(retained, []byte(base64.StdEncoding.EncodeToString(raw))) {
				t.Fatal("empty configuration invented declarations or lost captured bytes")
			}
		})
	}
}

func TestStrictConfigUnionOpaqueDirectoryProvenance(t *testing.T) {
	for _, control := range []string{
		"config file", "empty YAML object", "empty JSON object",
		"missing config values", "null config values", "non-object config values",
		"opaque config directory", "opaque entries", "opaque nonempty entries", "opaque parsed values",
		"opaque bytes", "opaque hash", "opaque size", "unmarked opaque directory",
	} {
		t.Run(control, func(t *testing.T) {
			f := strictUnionFixture(t, "", nil)
			raw := []byte("status:\n  custom: [queued:active]\n")
			configPath, storePath := "/synthetic/.beads/config.yaml", "/synthetic/store"
			parsed := map[string]any{"status": map[string]any{"custom": []any{"queued:active"}}}
			wantDeclarations, wantEntry := 1, ""
			if control == "empty YAML object" || control == "empty JSON object" {
				raw, parsed, wantDeclarations = nil, map[string]any{}, 0
				if control == "empty JSON object" {
					configPath, raw = "/synthetic/.beads/config.json", []byte("{}\n")
				}
			}
			config := map[string]any{
				"path": configPath, "state": "file", "sha256": digest(raw), "size": len(raw),
				"bytes": map[string]any{"$bytes": base64.StdEncoding.EncodeToString(raw)}, "values": parsed,
			}
			opaque := map[string]any{"path": storePath, "state": "directory", "contents": "not-read", "mode": 0o700}
			switch control {
			case "missing config values":
				delete(config, "values")
				wantEntry = configPath
			case "null config values":
				config["values"], wantEntry = nil, configPath
			case "non-object config values":
				config["values"], wantEntry = false, configPath
			case "opaque config directory":
				config = map[string]any{"path": configPath, "state": "directory", "contents": "not-read"}
				wantEntry = configPath
			case "opaque entries":
				opaque["entries"], wantEntry = []any{}, storePath
			case "opaque nonempty entries":
				opaque["entries"], wantEntry = []any{config}, storePath
			case "opaque parsed values":
				opaque["values"], wantEntry = map[string]any{"status.custom": "hidden:done"}, storePath
			case "opaque bytes":
				opaque["bytes"], wantEntry = map[string]any{"$bytes": ""}, storePath
			case "opaque hash":
				opaque["sha256"], wantEntry = "not-a-file-hash", storePath
			case "opaque size":
				opaque["size"], wantEntry = 0, storePath
			case "unmarked opaque directory":
				delete(opaque, "contents")
				wantEntry = storePath
			}
			for i := range f.config {
				entry := &f.config[i]
				switch entry.Input.Family {
				case "beads-dir-config":
					entry.Input.State, entry.Values = "json-config", map[string]any{"observations": []any{config}}
				case "store-files-and-quiescence":
					entry.Input.State = "json-config"
					entry.Values = map[string]any{"observations": []any{opaque}, "operator_quiesced": true, "sql_clean": true, "filesystem_atomicity": "not-verified"}
				}
			}
			var wantSources []byte
			data := strictUnionBytes(t, f, func(_ *Manifest, members map[string][]byte) {
				wantSources = append([]byte(nil), members["config/sources.json"]...)
			})
			a, err := Read(bytes.NewReader(data))
			if wantEntry != "" {
				var refused *MigrationRefusal
				if a != nil || !errors.As(err, &refused) || refused.Code != "malformed-declaration" || refused.Entry != wantEntry {
					t.Fatalf("malformed config or opaque evidence admitted: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			m, ok := a.ImportMigration()
			if !ok || len(m.Declarations()) != wantDeclarations {
				t.Fatal("opaque provenance changed the effective declaration set")
			}
			status, declared := m.Document().Statuses["queued"]
			if declared != (wantDeclarations == 1) || declared && status.Category != ontology.Unstarted {
				t.Fatal("true parsed configuration was replaced by opaque provenance")
			}
			retained, _ := a.Member("config/sources.json")
			if !bytes.Equal(retained, wantSources) {
				t.Fatal("raw directory/config provenance changed")
			}
		})
	}
}

func TestStrictConfigUnionConfigDirectoryShapes(t *testing.T) {
	for _, family := range []string{"beads-dir-config", "project-config", "project-local-config"} {
		for _, control := range []string{
			"enumerated file", "enumerated empty", "opaque missing entries",
			"opaque empty entries", "opaque nonempty entries", "unknown contents", "null contents",
			"directory values", "directory null values", "directory bytes", "directory hash", "directory size",
		} {
			t.Run(family+"/"+control, func(t *testing.T) {
				f := strictUnionFixture(t, "", nil)
				directoryPath := "/synthetic/config-directory"
				raw := []byte("status:\n  custom: [queued:active]\n")
				file := map[string]any{
					"path": directoryPath + "/config.yaml", "state": "file", "sha256": digest(raw), "size": len(raw),
					"bytes":  map[string]any{"$bytes": base64.StdEncoding.EncodeToString(raw)},
					"values": map[string]any{"status": map[string]any{"custom": []any{"queued:active"}}},
				}
				directory := map[string]any{"path": directoryPath, "state": "directory", "entries": []any{file}}
				wantDeclarations, refuse := 1, true
				switch control {
				case "enumerated file":
					refuse = false
				case "enumerated empty":
					directory["entries"], wantDeclarations, refuse = []any{}, 0, false
				case "opaque missing entries":
					directory["contents"] = "not-read"
					delete(directory, "entries")
				case "opaque empty entries":
					directory["contents"], directory["entries"] = "not-read", []any{}
				case "opaque nonempty entries":
					directory["contents"] = "not-read"
				case "unknown contents":
					directory["contents"] = "captured"
				case "null contents":
					directory["contents"] = nil
				case "directory values":
					directory["values"] = map[string]any{}
				case "directory null values":
					directory["values"] = nil
				case "directory bytes":
					directory["bytes"] = map[string]any{"$bytes": ""}
				case "directory hash":
					directory["sha256"] = digest(nil)
				case "directory size":
					directory["size"] = 0
				}
				wantOrigin := ""
				for i := range f.config {
					entry := &f.config[i]
					if entry.Input.Family == family {
						entry.Input.State, entry.Values = "json-config", map[string]any{"observations": []any{directory}}
						wantOrigin = family + ":" + entry.Input.Path + "@" + directoryPath
					}
				}
				if wantOrigin == "" {
					t.Fatal("missing independent config-family input")
				}
				var wantSources []byte
				data := strictUnionBytes(t, f, func(_ *Manifest, members map[string][]byte) {
					wantSources = append([]byte(nil), members["config/sources.json"]...)
				})
				a, err := Read(bytes.NewReader(data))
				if refuse {
					var refused *MigrationRefusal
					if a != nil || !errors.As(err, &refused) || refused.Code != "malformed-declaration" || refused.Entry != directoryPath {
						t.Fatalf("contradictory directory admitted: %v", err)
					}
					for _, origin := range refused.Origins {
						if origin == wantOrigin {
							return
						}
					}
					t.Fatalf("directory refusal lost exact input origin: %+v", refused)
				}
				if err != nil {
					t.Fatal(err)
				}
				migration, ok := a.ImportMigration()
				if !ok || len(migration.Declarations()) != wantDeclarations {
					t.Fatal("enumerated config directory changed its declaration set")
				}
				status, declared := migration.Document().Statuses["queued"]
				if declared != (wantDeclarations == 1) || declared && status.Category != ontology.Unstarted {
					t.Fatal("enumerated config file did not supply its declared status")
				}
				retained, _ := a.Member("config/sources.json")
				if !bytes.Equal(retained, wantSources) {
					t.Fatal("enumerated directory provenance changed")
				}
			})
		}
	}
}

// The pinned source reserves seven built-in names; its parser permits frozen
// as a custom name. The strict declaration must survive the legacy spelling.
func TestStrictConfigUnionDeclaredLegacySpelling(t *testing.T) {
	f := strictUnionFixture(t, "frozen:active", nil)
	f.records["bead"][0].Source.Fields["status"] = "frozen"
	a, err := Read(bytes.NewReader(strictUnionBytes(t, f, nil)))
	if err != nil {
		t.Fatal(err)
	}
	m, _ := a.ImportMigration()
	target, ok := m.Target("pl", f.records["bead"][0].ID)
	if !ok || target.Status != "frozen" || m.Document().Statuses["frozen"].Category != ontology.Unstarted {
		t.Fatal("legacy spelling replaced an explicitly declared custom status")
	}
}

func TestB27AuxiliarySourceInventoryNamesIndependentTablesAndRefusals(t *testing.T) {
	f := newMergeFixture(t, "pl")
	a, err := Read(bytes.NewReader(f.bytes(t, nil)))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"routes":               "refuse-if-nonempty",
		"interactions":         "refuse-if-nonempty",
		"issue_snapshots":      "archive",
		"compaction_snapshots": "archive",
		"provenance_events":    "archive",
		"events":               "archive",
	}
	got := map[string]Disposition{}
	for _, source := range a.Manifest().Sources {
		if _, needed := want[source.Family]; needed {
			if _, duplicate := got[source.Family]; duplicate {
				t.Fatalf("duplicate independent source disposition %q", source.Family)
			}
			got[source.Family] = source
		}
	}
	for family, action := range want {
		source, ok := got[family]
		if !ok || source.Action != action || source.Rows != 0 || source.Reason == "" {
			t.Fatalf("independent source %q was not explicitly inventoried: %+v", family, source)
		}
	}

	// A nonempty independent table is rejected before the archive can become a
	// restore plan. Exercise each refusal family separately so a generic
	// refuse-if-nonempty branch cannot masquerade as a complete inventory.
	for family := range want {
		if want[family] != "refuse-if-nonempty" {
			continue
		}
		t.Run(family, func(t *testing.T) {
			fixture := newMergeFixture(t, "pl")
			fixture.addArchive(t, family, Row{"synthetic": family})
			archive, err := Read(bytes.NewReader(fixture.bytes(t, nil)))
			if archive != nil || err == nil || !strings.Contains(err.Error(), "unsupported or operational source record") {
				t.Fatalf("nonempty independent source %q was not refused before admission: archive=%v err=%v", family, archive, err)
			}
		})
	}
}

func TestB27NonemptyAuxiliarySourcesSurviveReadAndMerge(t *testing.T) {
	// These are independent consumer vectors, not a native producer recording.
	rows := func(prefix string) map[string]Row {
		return map[string]Row{
			"issue_snapshots": {
				"id": prefix + "-snapshot", "issue_id": prefix + "-1", "snapshot_time": "2026-11-03T23:00:00Z",
				"compaction_level": json.Number("2"), "original_size": json.Number("9007199254740993"), "compressed_size": json.Number("37"),
				"original_content": "Snapshot λ\n" + prefix, "archived_events": "{\"revision\":9007199254740993, \"optional\":null}\n",
			},
			"compaction_snapshots": {
				"id": prefix + "-compaction", "issue_id": prefix + "-1", "compaction_level": json.Number("3"),
				"snapshot_json": "{ \"title\":\"雪\", \"ratio\":1.0, \"optional\":null }\n", "created_at": "2026-11-03T23:00:01Z",
			},
			"provenance_events": {
				"id": prefix + "-provenance", "issue_id": prefix + "-1", "kind": "import", "actor": "unverified-source-actor",
				"ref": prefix + "-external", "ref_kind": "spec", "payload": "{\"literal\":9007199254740993,\"text\":\"λ\"}\n",
				"source": "independent-fixture", "occurred_at": nil, "created_at": "2026-11-03T23:00:02Z",
			},
			"events": {
				"id": prefix + "-event", "issue_id": prefix + "-1", "event_type": "updated", "actor": "unverified-source-actor",
				"old_value": nil, "new_value": "{ \"ratio\":1.0, \"large\":9007199254740993 }", "comment": "Event λ\n" + prefix, "created_at": "2026-11-03T23:00:03Z",
			},
		}
	}
	for _, family := range []string{"issue_snapshots", "compaction_snapshots", "provenance_events", "events"} {
		t.Run(family, func(t *testing.T) {
			fixtures := []*mergeFixture{newMergeFixture(t, "alpha"), newMergeFixture(t, "beta")}
			want := map[string]Record{}
			inputs := make([][]byte, len(fixtures))
			check := func(a *Archive, count int) {
				t.Helper()
				var dispositions []Disposition
				for _, d := range a.Manifest().Sources {
					if d.Family == family {
						dispositions = append(dispositions, d)
					}
				}
				if len(dispositions) != 1 || dispositions[0].Action != "archive" || dispositions[0].Rows != count || dispositions[0].Reason == "" {
					t.Fatalf("nonempty independent source census lost: %+v", dispositions)
				}
				got := map[string]Record{}
				for _, record := range a.Records("beads-archive") {
					if record.Source.Table != family {
						continue
					}
					if _, duplicate := got[record.ID]; duplicate || !equalJSON(record, want[record.ID]) {
						t.Fatalf("source row, opaque bytes or verification flags changed: %+v", record)
					}
					got[record.ID] = record
				}
				if len(got) != count || a.Manifest().History != "available-unverified" {
					t.Fatalf("archive row loss or invented verified history: count=%d manifest=%+v", len(got), a.Manifest())
				}
			}
			for i, prefix := range []string{"alpha", "beta"} {
				f := fixtures[i]
				f.addArchive(t, family, rows(prefix)[family])
				records := f.records["beads-archive"]
				record := records[len(records)-1]
				want[record.ID] = record
				inputs[i] = f.bytes(t, nil)
				a, err := Read(bytes.NewReader(inputs[i]))
				if err != nil {
					t.Fatal(err)
				}
				check(a, 1)
			}
			merged, err := Merge(bytes.NewReader(inputs[0]), bytes.NewReader(inputs[1]))
			if err != nil {
				t.Fatal(err)
			}
			a, err := Read(bytes.NewReader(merged))
			if err != nil {
				t.Fatal(err)
			}
			check(a, 2)
			reversed, err := Merge(bytes.NewReader(inputs[1]), bytes.NewReader(inputs[0]))
			if err != nil || !bytes.Equal(merged, reversed) {
				t.Fatalf("input order changed retained archive: %v", err)
			}
			t.Run("census-mismatch-refused", func(t *testing.T) {
				bad := fixtures[0].bytes(t, func(m *Manifest, _ map[string][]byte) {
					for i := range m.Sources {
						if m.Sources[i].Family == family {
							m.Sources[i].Rows++
						}
					}
				})
				if archive, err := Read(bytes.NewReader(bad)); archive != nil || err == nil || !strings.Contains(err.Error(), "disposition/action/count mismatch") {
					t.Fatalf("false nonempty source count admitted: archive=%v err=%v", archive, err)
				}
			})
		})
	}
}

func TestB27UnresolvedDependencyIsTypedAndRetainsExactEndpoint(t *testing.T) {
	f := newMergeFixture(t, "pl")
	f.records["edge"][0].Source.Fields["depends_on_issue_id"] = "pl-missing"
	f.records["edge"][0].Body["to"] = "pl-missing"
	archive, err := Read(bytes.NewReader(f.bytes(t, nil)))
	var refusal *UnresolvedDependencyError
	if archive != nil || !errors.As(err, &refusal) {
		t.Fatalf("unresolved dependency was not a typed refusal: archive=%v err=%v", archive, err)
	}
	if refusal.EdgeID != "pl-edge" || refusal.Project != "pl" || refusal.Source != "pl-1" || refusal.Target != "pl-missing" || refusal.TargetKind != "depends_on_issue_id" {
		t.Fatalf("typed unresolved dependency lost source details: %+v", refusal)
	}
}
