package s1b25b28_test

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This is an authored regression archive, never a recording, native inventory
// or producer success artifact. The retained source tar is read, never changed.
type configUnionInput struct {
	manifest dumpformat.Manifest
	records  map[string][]dumpformat.Record
	sources  []map[string]any
}

func configUnionJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return append(data, '\n')
}

func addUnionConfig(in *configUnionInput, key, value string) {
	in.records["config"] = append(in.records["config"], dumpformat.Record{
		Kind: "config", ID: "config/" + key,
		Body:   dumpformat.Row{"key": "config/" + key, "value": value, "classification": "public"},
		Source: dumpformat.Provenance{Table: "config", Fields: dumpformat.Row{"key": key, "value": value}},
	})
}

func addUnionArchive(t *testing.T, in *configUnionInput, table string, fields dumpformat.Row) {
	t.Helper()
	in.records["beads-archive"] = append(in.records["beads-archive"], dumpformat.Record{
		Kind: "beads-archive", ID: table + "/" + digest(configUnionJSON(t, fields)),
		Body:   dumpformat.Row{"read_only": true, "redactable": true, "history_verified": false, "attribution_verified": false},
		Source: dumpformat.Provenance{Table: table, Fields: fields},
	})
}

func unionTypeDirective(name string) dumpformat.OntologyDirective {
	return dumpformat.OntologyDirective{ID: "map-" + name, Kind: "map_type", Target: dumpformat.OntologyTarget{Project: "demo", Type: name}, From: name, To: "issue"}
}

func configUnionArchive(t *testing.T, change func(*configUnionInput)) []byte {
	t.Helper()
	base, err := dumpformat.Read(bytes.NewReader(exporterArchive(t)))
	if err != nil {
		t.Fatal(err)
	}
	in := &configUnionInput{manifest: base.Manifest(), records: map[string][]dumpformat.Record{}}
	in.manifest.History = "unavailable"
	in.manifest.Projects[0].IDMode, in.manifest.Projects[0].HashLength = "hash", 1
	in.manifest.Verification["ontology"] = dumpformat.StrictUnionOntologyVerification
	in.manifest.Ontology = []dumpformat.OntologyDirective{unionTypeDirective("decision"), unionTypeDirective("incident"), unionTypeDirective("proposal")}
	for _, kind := range in.manifest.Kinds {
		for _, row := range base.Records(kind.Name) {
			if row.Source.Table != "issue_versions" {
				in.records[kind.Name] = append(in.records[kind.Name], row)
			}
		}
	}
	// The expected source state is explicitly enumerated, independently of the
	// migration compiler and its output document.
	for i := range in.records["bead"] {
		r := &in.records["bead"][i]
		status, typ, category, outcome := "", "", "", ""
		switch r.ID {
		case "demo-7":
			status, typ, category = "queued", "decision", "unstarted"
		case "demo-7.4":
			status, typ, category = "shared", "incident", "started"
		case "demo-8":
			status, typ, category, outcome = "shipped", "proposal", "completed", "success"
		default:
			t.Fatalf("unexpected authored source ID %s", r.ID)
		}
		r.Source.Fields["status"], r.Source.Fields["issue_type"] = status, typ
		r.Body["type"], r.Body["source_type"], r.Body["status_category"] = typ, typ, category
		delete(r.Body, "result")
		if outcome != "" {
			r.Body["result"] = outcome
		}
	}
	edge := &in.records["edge"][0]
	edge.Source.Fields["issue_id"], edge.Source.Fields["depends_on_issue_id"], edge.Source.Fields["type"] = "demo-7", "demo-8", "blocks"
	edge.Body["from"], edge.Body["to"], edge.Body["type"] = "demo-7", "demo-8", "blocks"
	addUnionConfig(in, "status.custom", "queued:active,shared:wip")
	addUnionConfig(in, "types.custom", "decision")
	addUnionArchive(t, in, "custom_statuses", dumpformat.Row{"name": "tableonly", "category": "active"})
	addUnionArchive(t, in, "custom_types", dumpformat.Row{"name": "proposal"})
	config, _ := base.Member("config/sources.json")
	decoder := json.NewDecoder(bytes.NewReader(config))
	decoder.UseNumber()
	if err := decoder.Decode(&in.sources); err != nil {
		t.Fatal(err)
	}
	for _, entry := range in.sources {
		input := entry["input"].(map[string]any)
		if input["family"] == "beads-dir-config" {
			input["state"] = "json-config"
			input["path"] = "/authored/config-union/.beads/config.yaml"
			entry["values"] = map[string]any{
				"status": map[string]any{"custom": []any{"shared:wip", "shipped:done", "cold:frozen"}},
				"types":  map[string]any{"custom": []any{"decision", "incident"}},
			}
		}
	}
	if change != nil {
		change(in)
	}
	return encodeConfigUnionArchive(t, in)
}

func encodeConfigUnionArchive(t *testing.T, in *configUnionInput) []byte {
	t.Helper()
	m := in.manifest
	m.Kinds, m.Files = nil, map[string]dumpformat.File{}
	members, counts := map[string][]byte{}, map[string]int{}
	for kind, rows := range in.records {
		if len(rows) == 0 {
			continue
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].Project+"\x00"+rows[i].ID < rows[j].Project+"\x00"+rows[j].ID })
		var content []byte
		for _, row := range rows {
			content = append(content, configUnionJSON(t, row)...)
			counts[row.Source.Table]++
			if kind == "bead" {
				counts["labels"] += len(row.Body["labels"].([]any))
			}
		}
		path := "records/" + kind + ".ndjson"
		members[path] = content
		m.Kinds = append(m.Kinds, dumpformat.Kind{Name: kind, Version: 1, File: path, Count: len(rows)})
	}
	sort.Slice(m.Kinds, func(i, j int) bool { return m.Kinds[i].Name < m.Kinds[j].Name })
	sort.Slice(in.sources, func(i, j int) bool {
		a, b := in.sources[i]["input"].(map[string]any), in.sources[j]["input"].(map[string]any)
		pa, pb := configUnionPrecedence(t, a["precedence"]), configUnionPrecedence(t, b["precedence"])
		if pa != pb {
			return pa < pb
		}
		return a["family"].(string)+"\x00"+a["path"].(string) < b["family"].(string)+"\x00"+b["path"].(string)
	})
	members["config/sources.json"] = configUnionJSON(t, in.sources)
	external := map[string]bool{}
	var sources []dumpformat.Disposition
	for _, entry := range in.sources {
		input := entry["input"].(map[string]any)
		family := input["family"].(string)
		external[family] = true
		d := dumpformat.Disposition{Family: family, Action: "manifest/rebuild", Reason: "requested source absent at declared path"}
		if input["state"] == "json-config" {
			d.Rows, d.Action, d.Reason = len(entry["values"].(map[string]any)), "archive", "explicit JSON config; secrets are references; precedence retained, effective config not inferred"
		}
		sources = append(sources, d)
	}
	for _, d := range m.Sources {
		if external[d.Family] {
			continue
		}
		if d.Family != "repo_mtimes" {
			d.Rows = counts[d.Family]
		}
		if d.Family == "issues.row_lock" || d.Family == "issues.is_blocked" {
			d.Rows = counts["issues"]
		}
		if d.Family == "custom_statuses" || d.Family == "custom_types" {
			d.Action = "carry"
		}
		sources = append(sources, d)
	}
	sort.Slice(sources, func(i, j int) bool {
		return sources[i].Family+"\x00"+sources[i].Reason < sources[j].Family+"\x00"+sources[j].Reason
	})
	m.Sources = sources
	var paths []string
	for path, data := range members {
		m.Files[path] = dumpformat.File{SHA256: digest(data), Size: int64(len(data))}
		paths = append(paths, path)
	}
	sort.Strings(paths)
	members["manifest.json"] = configUnionJSON(t, m)
	var out bytes.Buffer
	writer := tar.NewWriter(&out)
	for _, path := range append([]string{"manifest.json"}, paths...) {
		data := members[path]
		if err := writer.WriteHeader(&tar.Header{Name: path, Mode: 0600, Size: int64(len(data)), Typeflag: tar.TypeReg, Format: tar.FormatUSTAR}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func configUnionPrecedence(t *testing.T, value any) int64 {
	t.Helper()
	number, ok := value.(json.Number)
	if !ok {
		t.Fatalf("authored source precedence is not a captured number: %v", value)
	}
	result, err := number.Int64()
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func configUnionProfile(t *testing.T, data []byte) driver.RestoreImportProfile {
	t.Helper()
	inspection, err := converterroundtrip.InspectImport(context.Background(), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return driver.RestoreImportProfile{
		Admission:     driver.RestoreImportAdmission{Mode: "authenticated", Actor: acceptancePrincipal, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)},
		SourceStoreID: inspection.SourceStoreID, Method: "offline.restore.import", IdempotencyKey: strings.Repeat("b", 32),
		ArchiveDigest: inspection.ArchiveSHA256, DerivedDigest: inspection.DerivedSHA256, HistoryMode: driver.RestoreHistoryModeSnapshot,
	}
}

func TestB27ConfigUnionStoredOntologyAndReopenAcrossDrivers(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite", "postgres"} {
		for _, resolution := range []string{"compatible", "conflict-mapped", "unspecified-mapped"} {
			t.Run(kind+"/"+resolution, func(t *testing.T) {
				data := configUnionArchive(t, func(in *configUnionInput) {
					if resolution == "compatible" {
						return
					}
					if resolution == "conflict-mapped" {
						appendUnionYAMLStatus(in, "queued:wip")
					} else {
						setUnionStoredStatuses(in, "queued,shared:wip")
					}
					in.manifest.Ontology = append(in.manifest.Ontology, dumpformat.OntologyDirective{
						ID: "resolve-queued", Kind: "map_status", Target: dumpformat.OntologyTarget{Project: "demo", Type: "issue"}, From: "queued", To: "todo",
					})
				})
				profile := configUnionProfile(t, data)
				s := openStack(t, kind, nil)
				if _, err := converterroundtrip.RestoreImportWithLease(s.ctx, bytes.NewReader(data), s.store, s.lease, profile); err != nil {
					t.Fatal(err)
				}
				assertStoredConfigUnion(t, s, data, profile)
				before := configUnionSnapshotState(t, s, data, profile)
				if len(before.Changesets) != 1 || before.Changesets[0].Kind != model.ChangesetRestore {
					t.Fatalf("import fabricated source history: %+v", before.Changesets)
				}
				if kind != "memory" {
					s.reopen()
					assertStoredConfigUnion(t, s, data, profile)
					if !reflect.DeepEqual(before, configUnionSnapshotState(t, s, data, profile)) {
						t.Fatal("configuration/ontology/history changed after real SQL reopen")
					}
				}
				// Reverse has a deliberately separate profile boundary. This
				// assertion prevents an import success from becoming a reverse claim.
				state := configUnionSnapshotState(t, s, data, profile)
				result, err := converterroundtrip.ReverseRestored(s.ctx, s.store)
				var refusal converterroundtrip.ReverseRefusal
				if !errors.As(err, &refusal) || refusal.Reason != "missing_source_evidence" || len(result.Result.JSONL) != 0 || !reflect.DeepEqual(state, configUnionSnapshotState(t, s, data, profile)) {
					t.Fatalf("unreleased strict reverse path was not closed: %v", err)
				}
			})
		}
	}
}

func assertStoredConfigUnion(t *testing.T, s *stack, data []byte, p driver.RestoreImportProfile) {
	t.Helper()
	a, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	meta, err := s.store.Meta(s.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.store.Read(s.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	page, err := r.Query(s.ctx, driver.Query{Limit: 1000})
	if err != nil || page.NextCursor != nil {
		t.Fatalf("incomplete source/config census: %v", err)
	}
	live := map[string]driver.Record{}
	for _, row := range page.Items {
		if _, duplicate := live[row.Ref.ID]; duplicate {
			t.Fatal("duplicate stored identity")
		}
		live[row.Ref.ID] = row
	}
	wantIDs := map[string]bool{
		"restore/archive":                             true,
		"instance_config/restore-import/binding":      true,
		"instance_config/restore-import/project/demo": true,
	}
	wantRecords := 3 // archive evidence, import binding, one project binding
	for _, kind := range a.Manifest().Kinds {
		wantRecords += kind.Count
		for _, source := range a.Records(kind.Name) {
			id := source.ID
			if kind.Name != "bead" {
				id = "source/" + kind.Name + "/" + source.ID
			}
			wantIDs[id] = true
			row, ok := live[id]
			if !ok || row.Kind != kind.Name {
				t.Fatalf("lost source %s/%s", kind.Name, id)
			}
			if kind.Name == "bead" {
				doc, err := pipeline.DecodeDocument(row.Body)
				if err != nil {
					t.Fatal(err)
				}
				want := map[string]struct{ typ, status string }{"demo-7": {"decision", "queued"}, "demo-7.4": {"incident", "shared"}, "demo-8": {"proposal", "shipped"}}[source.ID]
				if doc.Type != want.typ || doc.Fields["status"] != want.status {
					t.Fatalf("installed record differs from union: %+v", doc)
				}
				var fields dumpformat.Row
				decodeUnionJSON(t, doc.Metadata["beads.source_fields"], &fields)
				if !sameUnionJSON(fields, source.Source.Fields) {
					t.Fatal("full raw issue source changed")
				}
				var rawStatus string
				decodeUnionJSON(t, doc.Metadata["beads.source.status"], &rawStatus)
				if rawStatus != source.Source.Fields["status"] {
					t.Fatal("raw custom status lost")
				}
			} else {
				var got dumpformat.Row
				decodeUnionJSON(t, row.Body, &got)
				want := dumpformat.Row{}
				for key, value := range source.Body {
					want[key] = value
				}
				want["source_id"], want["source_table"], want["source"] = source.ID, source.Source.Table, source.Source.Fields
				if content, eligible := source.Body["value"].(string); kind.Name == "memory" && source.Body["classification"] == "public" && eligible {
					key, ok := source.Body["key"].(string)
					if !ok || strings.TrimSpace(key) == "" {
						t.Fatal("public source memory lacks its admitted key")
					}
					want = dumpformat.Row{
						"id": id, "key": strings.TrimSpace(key), "content": content,
						"metadata": map[string]any{
							"plug.restore.source_id": source.ID, "plug.restore.source_table": source.Source.Table,
							"plug.restore.source": source.Source.Fields, "plug.restore.archive_sha256": a.SHA256(),
						},
					}
				}
				if !sameUnionJSON(got, want) {
					t.Fatalf("complete source/config body changed: %s", id)
				}
			}
		}
	}
	if len(live) != wantRecords {
		t.Fatalf("extra/missing stored records: %d, want %d", len(live), wantRecords)
	}
	for id, row := range live {
		if !wantIDs[id] {
			t.Fatalf("unexpected record %s/%s", row.Kind, id)
		}
	}
	if got, err := r.Blob(s.ctx, driver.Hash(digest(data))); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("raw source tar changed: %v", err)
	}
	for path, file := range a.Manifest().Files {
		want, _ := a.Member(path)
		if got, err := r.Blob(s.ctx, driver.Hash(file.SHA256)); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("source member bytes changed at %s: %v", path, err)
		}
	}
	ontologyRows, err := r.DerivedRows(s.ctx, driver.DerivedOntology, "")
	if err != nil || len(ontologyRows) != 1 || ontologyRows[0].Key != "builtin" {
		t.Fatalf("installed ontology missing: %v", err)
	}
	doc, err := ontology.Decode(ontologyRows[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	categories := map[string]ontology.Category{}
	outcomes := map[string]ontology.Outcome{}
	for name, status := range doc.Statuses {
		categories[name], outcomes[name] = status.Category, status.Outcome
	}
	wantCategories := map[string]ontology.Category{
		"todo": ontology.Unstarted, "in_progress": ontology.Started, "done": ontology.Completed, "failed": ontology.Completed, "canceled": ontology.Canceled, "backlog": ontology.Backlog,
		"queued": ontology.Unstarted, "shared": ontology.Started, "shipped": ontology.Completed, "cold": ontology.Backlog, "tableonly": ontology.Unstarted,
	}
	wantOutcomes := map[string]ontology.Outcome{"todo": "", "in_progress": "", "done": ontology.Success, "failed": ontology.Failure, "canceled": "", "backlog": "", "queued": "", "shared": "", "shipped": ontology.Success, "cold": "", "tableonly": ""}
	if !reflect.DeepEqual(categories, wantCategories) || !reflect.DeepEqual(outcomes, wantOutcomes) || len(doc.Types) != 4 || len(doc.Workflows) != 1 || len(doc.Workflows["issue"].Statuses) != 11 {
		t.Fatalf("installed union document incomplete: %+v", doc)
	}
	for _, name := range []string{"issue", "decision", "incident", "proposal"} {
		if typ, ok := doc.Types[name]; !ok || typ.Workflow != "issue" {
			t.Fatalf("type %s absent from installed union", name)
		}
	}
	// These predicates are evaluated from the stored document, after reopen
	// too. They do not assert production resolver wiring or public commands.
	snapshot, problems := ontology.Compile(doc, nil, nil)
	if len(problems) != 0 || !snapshot.Effective("demo").ReadyEligible("decision", "queued") || !snapshot.Effective("demo").SatisfiesBlocks("shipped") || snapshot.Effective("demo").SatisfiesBlocks("cold") {
		t.Fatalf("stored union predicates disagree: %v", problems)
	}
	readyRows, err := r.DerivedRows(s.ctx, driver.DerivedReadyState, "")
	if err != nil {
		t.Fatal(err)
	}
	ready := map[string]map[string]bool{}
	for _, row := range readyRows {
		var state map[string]bool
		decodeUnionJSON(t, row.Value, &state)
		ready[row.Key] = state
	}
	wantReady := map[string]map[string]bool{"demo-7": {"ready": true, "blocked": false}, "demo-7.4": {"ready": false, "blocked": false}, "demo-8": {"ready": false, "blocked": false}}
	if len(readyRows) != 3 || !reflect.DeepEqual(ready, wantReady) {
		t.Fatalf("derived state used a different ontology: %v", ready)
	}
	var evidence struct {
		Profile       string `json:"profile"`
		ArchiveSHA256 string `json:"archive_sha256"`
		DerivedSHA256 string `json:"derived_sha256"`
	}
	decodeUnionJSON(t, live["restore/archive"].Body, &evidence)
	if evidence.Profile != "beads-compat-union-head/1" || evidence.ArchiveSHA256 != digest(data) || evidence.DerivedSHA256 != p.DerivedDigest {
		t.Fatalf("wrong strict image binding: %+v", evidence)
	}
	principal, _ := driver.PrincipalKey(p.Admission.Actor)
	receipt, err := r.IdempotencyByKey(s.ctx, principal, p.AgentLabel(), p.Method, p.IdempotencyKey)
	if err != nil || receipt == nil {
		t.Fatalf("successful import receipt missing: %v", err)
	}
}

type configUnionSnapshotObservation struct {
	durableRelationState
	Receipt driver.IdempotencyRecord
	Blobs   map[string][]byte
}

// A snapshot retains its current image and restore receipt, not source log
// history below its floor. Observe those independently and require the typed
// refusal instead of using a nil changeset cursor as a complete-history oracle.
func configUnionSnapshotState(t *testing.T, s *stack, data []byte, p driver.RestoreImportProfile) configUnionSnapshotObservation {
	t.Helper()
	state := configUnionSnapshotObservation{
		durableRelationState: durableRelationState{Versions: map[string][]driver.Version{}, Derived: map[driver.DerivedTable][]driver.DerivedRow{}},
		Blobs:                map[string][]byte{},
	}
	var err error
	state.Meta, err = s.store.Meta(s.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if p.HistoryMode != driver.RestoreHistoryModeSnapshot || state.Meta.StoreID != p.SourceStoreID || state.Meta.Head.String() != "1" || state.Meta.HistoryFloor.String() != "1" {
		t.Fatalf("strict snapshot current-image boundary changed: %+v", state.Meta)
	}
	r, err := s.store.Read(s.ctx, driver.View{Incarnation: state.Meta.Incarnation, At: &state.Meta.Head})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	page, err := r.Query(s.ctx, driver.Query{Limit: 1000})
	if err != nil || page.NextCursor != nil {
		t.Fatalf("incomplete strict snapshot record census: %v", err)
	}
	state.Records = page.Items
	for _, row := range page.Items {
		versions, err := r.Versions(s.ctx, row.Ref, driver.PageReq{Limit: 1000})
		if err != nil || versions.NextCursor != nil {
			t.Fatalf("incomplete strict snapshot versions: %v", err)
		}
		state.Versions[row.Ref.ID] = versions.Items
	}
	beforeFloor := driver.Cursor{Incarnation: state.Meta.Incarnation, Revision: model.Natural64(0), Method: "changesets"}
	for _, cursor := range []*driver.Cursor{nil, &beforeFloor} {
		changes, err := r.Changesets(s.ctx, cursor, 1000)
		var truncated driver.ErrTruncated
		if !errors.As(err, &truncated) || truncated.Floor != state.Meta.HistoryFloor || len(changes) != 0 {
			t.Fatalf("strict snapshot invented unavailable pre-floor history: cursor=%+v changes=%+v err=%v", cursor, changes, err)
		}
	}
	afterFloor := driver.Cursor{Incarnation: state.Meta.Incarnation, Revision: state.Meta.HistoryFloor, Method: "changesets"}
	if changes, err := r.Changesets(s.ctx, &afterFloor, 1000); err != nil || len(changes) != 0 {
		t.Fatalf("strict snapshot has unexpected post-floor history: changes=%+v err=%v", changes, err)
	}
	principal, err := driver.PrincipalKey(p.Admission.Actor)
	if err != nil {
		t.Fatal(err)
	}
	request, err := p.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := r.IdempotencyByKey(s.ctx, principal, p.AgentLabel(), p.Method, p.IdempotencyKey)
	if err != nil || receipt == nil || receipt.ResponsePending || receipt.RequestHash != request || receipt.Commit.Revision != state.Meta.Head {
		t.Fatalf("strict snapshot receipt missing or changed: %+v %v", receipt, err)
	}
	change, err := r.ChangesetByKey(s.ctx, principal, p.AgentLabel(), p.Method, p.IdempotencyKey)
	if err != nil || change == nil || change.Kind != model.ChangesetRestore || change.Revision != state.Meta.Head || change.Incarnation != state.Meta.Incarnation || change.IdempotencyKey != p.IdempotencyKey || !reflect.DeepEqual(change.Principal, p.Admission.Actor) || !reflect.DeepEqual(*change, receipt.Changeset) {
		t.Fatalf("strict snapshot restore changeset binding changed: %+v %v", change, err)
	}
	state.Receipt = *receipt
	// This is the actual key-bound restore row, not a reconstructed source log.
	state.Changesets = []model.Changeset{*change}
	for _, table := range []driver.DerivedTable{driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins, driver.DerivedReadyState, driver.DerivedReferenceIndex, driver.DerivedSortKey} {
		state.Derived[table], err = r.DerivedRows(s.ctx, table, "")
		if err != nil {
			t.Fatal(err)
		}
	}
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if archive.Manifest().History != "unavailable" {
		t.Fatal("strict snapshot control substituted source history")
	}
	state.Blobs["archive.tar"], err = r.Blob(s.ctx, driver.Hash(digest(data)))
	if err != nil || !bytes.Equal(state.Blobs["archive.tar"], data) {
		t.Fatalf("strict snapshot archive bytes changed: %v", err)
	}
	files := archive.Manifest().Files
	manifest, ok := archive.Member("manifest.json")
	if !ok {
		t.Fatal("strict snapshot source manifest missing")
	}
	files["manifest.json"] = dumpformat.File{SHA256: digest(manifest), Size: int64(len(manifest))}
	for name, file := range files {
		want, present := archive.Member(name)
		state.Blobs[name], err = r.Blob(s.ctx, driver.Hash(file.SHA256))
		if !present || err != nil || !bytes.Equal(state.Blobs[name], want) {
			t.Fatalf("strict snapshot member bytes changed at %s: %v", name, err)
		}
	}
	return state
}

func sameUnionJSON(a, b any) bool {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return bytes.Equal(left, right)
}

func decodeUnionJSON(t *testing.T, data []byte, target any) {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		t.Fatal(err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Fatalf("trailing stored JSON: %v", err)
	}
}

func appendUnionYAMLStatus(in *configUnionInput, value string) {
	for _, entry := range in.sources {
		if entry["input"].(map[string]any)["family"] == "beads-dir-config" {
			status := entry["values"].(map[string]any)["status"].(map[string]any)
			status["custom"] = append(status["custom"].([]any), value)
		}
	}
}

func setUnionStoredStatuses(in *configUnionInput, value string) {
	for i := range in.records["config"] {
		r := &in.records["config"][i]
		if r.ID == "config/status.custom" {
			r.Body["value"], r.Source.Fields["value"] = value, value
		}
	}
}

type configUnionObservedStore struct {
	driver.Store
	writes int
}

func (s *configUnionObservedStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	s.writes++
	return s.Store.Write(ctx, lease)
}

func TestB27ConfigUnionRefusalHasNoWriteHistoryReceiptOrPublicationAcrossDrivers(t *testing.T) {
	valid := configUnionArchive(t, nil)
	profile := configUnionProfile(t, valid)
	controls := []struct {
		name, code, entry string
		change            func(*configUnionInput)
	}{
		{"conflict", "category-conflict", "queued", func(in *configUnionInput) { appendUnionYAMLStatus(in, "queued:wip") }},
		{"unspecified", "category-required", "queued", func(in *configUnionInput) { setUnionStoredStatuses(in, "queued,shared:wip") }},
		{"absent mapping", "mapping-required", "decision", func(in *configUnionInput) { in.manifest.Ontology = in.manifest.Ontology[1:] }},
		{"unknown target", "unknown-target", "map-decision", func(in *configUnionInput) { in.manifest.Ontology[0].To = "invented" }},
		{"duplicate directive", "duplicate-directive", "map-decision", func(in *configUnionInput) {
			in.manifest.Ontology = append(in.manifest.Ontology, in.manifest.Ontology[0])
		}},
		{"ambiguous directive", "overlapping-directive", "decision", func(in *configUnionInput) {
			d := in.manifest.Ontology[0]
			d.ID = "ambiguous"
			in.manifest.Ontology = append(in.manifest.Ontology, d)
		}},
		{"unused directive", "unused-directive", "map-unused", func(in *configUnionInput) {
			in.manifest.Ontology = append(in.manifest.Ontology, unionTypeDirective("unused"))
		}},
		{"undeclared status", "undeclared-entry", "phantom", func(in *configUnionInput) { in.records["bead"][0].Source.Fields["status"] = "phantom" }},
		{"malformed category", "malformed-declaration", "queued", func(in *configUnionInput) { setUnionStoredStatuses(in, "queued:invented,shared:wip") }},
		{"malformed YAML", "malformed-declaration", "status.custom", func(in *configUnionInput) {
			for _, e := range in.sources {
				if e["input"].(map[string]any)["family"] == "beads-dir-config" {
					e["values"].(map[string]any)["status"] = map[string]any{"custom": false}
				}
			}
		}},
		{"duplicate declaration", "duplicate-declaration", "queued", func(in *configUnionInput) { setUnionStoredStatuses(in, "queued:active,queued:wip,shared:wip") }},
		{"duplicate normalized declaration", "duplicate-declaration", "tableonly", func(in *configUnionInput) {
			addUnionArchive(t, in, "custom_statuses", dumpformat.Row{"name": "tableonly", "category": "wip"})
		}},
	}
	for _, kind := range []string{"memory", "sqlite", "postgres"} {
		t.Run(kind, func(t *testing.T) {
			s := openStack(t, kind, nil)
			target := &configUnionObservedStore{Store: s.store}
			before := relationState(t, s)
			var refusedProfiles []driver.RestoreImportProfile
			for _, control := range controls {
				t.Run(control.name, func(t *testing.T) {
					data := configUnionArchive(t, control.change)
					bound := profile
					bound.ArchiveDigest = digest(data)
					report, err := converterroundtrip.RestoreImportWithLease(s.ctx, bytes.NewReader(data), target, s.lease, bound)
					var refused *dumpformat.MigrationRefusal
					if !errors.As(err, &refused) || refused.Code != control.code || refused.Entry != control.entry || target.writes != 0 || report.Archive != nil || !report.Restored.Head.IsZero() {
						t.Fatalf("named refusal escaped before-write boundary: report=%+v writes=%d err=%v", report, target.writes, err)
					}
					if !reflect.DeepEqual(before, relationState(t, s)) {
						t.Fatal("refusal changed records, versions, changesets/events or derived state")
					}
					assertUnionRefusalUnpublished(t, s, bound)
					refusedProfiles = append(refusedProfiles, bound)
				})
			}
			if kind != "memory" {
				s.reopen()
				if !reflect.DeepEqual(before, relationState(t, s)) {
					t.Fatal("refusal left durable data visible after SQL reopen")
				}
				for _, bound := range refusedProfiles {
					assertUnionRefusalUnpublished(t, s, bound)
				}
			}
		})
	}
}

func TestB27ConfigUnionHistoryRequiresExplicitSnapshotAcrossDrivers(t *testing.T) {
	data := configUnionArchive(t, nil)
	snapshotProfile := configUnionProfile(t, data)
	for _, kind := range []string{"memory", "sqlite", "postgres"} {
		t.Run(kind, func(t *testing.T) {
			s := openStack(t, kind, nil)
			target := &configUnionObservedStore{Store: s.store}
			before := relationState(t, s)
			for _, control := range []struct {
				name   string
				mode   driver.RestoreHistoryMode
				reason driver.RestoreHistoryReason
			}{
				{"default-full", "", driver.RestoreHistoryReasonUnavailable},
				{"explicit-full", driver.RestoreHistoryModeFull, driver.RestoreHistoryReasonUnavailable},
				{"partial", driver.RestoreHistoryModePartial, driver.RestoreHistoryReasonNotYetSupported},
			} {
				t.Run(control.name, func(t *testing.T) {
					profile := snapshotProfile
					profile.HistoryMode = control.mode
					report, err := converterroundtrip.RestoreImportWithLease(s.ctx, bytes.NewReader(data), target, s.lease, profile)
					var refused driver.ErrRestoreHistoryRefused
					wantMode := control.mode
					if wantMode == "" {
						wantMode = driver.RestoreHistoryModeFull
					}
					if !errors.As(err, &refused) || refused.Mode != wantMode || refused.History != "unavailable" || refused.Reason != control.reason || target.writes != 0 || report.Archive == nil || !report.Restored.Head.IsZero() {
						t.Fatalf("strict config history refusal escaped before-write boundary: report=%+v writes=%d err=%v", report, target.writes, err)
					}
					if !reflect.DeepEqual(before, relationState(t, s)) {
						t.Fatal("history refusal changed records, versions, changesets/events or derived state")
					}
					assertUnionRefusalUnpublished(t, s, profile)
				})
			}
			if kind != "memory" {
				s.reopen()
				if !reflect.DeepEqual(before, relationState(t, s)) {
					t.Fatal("history refusal left durable data visible after SQL reopen")
				}
				assertUnionRefusalUnpublished(t, s, snapshotProfile)
			}
		})
	}
}

// This separate authored input satisfies the old current-image guards. A
// comment/allocator/relation refusal must not mask missing strict admission.
func configUnionLegacyArchive(t *testing.T) []byte {
	t.Helper()
	return configUnionArchive(t, func(in *configUnionInput) {
		in.manifest.Projects = []dumpformat.Project{{Prefix: "pl", IDMode: "hash", HashLength: 32, ChildCounters: map[string]uint64{}}}
		for i := range in.manifest.Ontology {
			in.manifest.Ontology[i].Target.Project = "pl"
		}
		rekey := strings.NewReplacer(
			"demo-7.4", "pl-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			"demo-7", "pl-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"demo-8", "pl-cccccccccccccccccccccccccccccccc",
		)
		delete(in.records, "comment")
		for kind, records := range in.records {
			var retained []dumpformat.Record
			for _, record := range records {
				switch record.Source.Table {
				case "events", "issue_versions", "issue_counter", "child_counters":
					continue
				}
				var rebound dumpformat.Record
				decoder := json.NewDecoder(strings.NewReader(rekey.Replace(string(configUnionJSON(t, record)))))
				decoder.UseNumber()
				if err := decoder.Decode(&rebound); err != nil {
					t.Fatal(err)
				}
				if rebound.Project == "demo" {
					rebound.Project = "pl"
				}
				if kind == "beads-archive" {
					rebound.ID = rebound.Source.Table + "/" + digest(configUnionJSON(t, rebound.Source.Fields))
				}
				retained = append(retained, rebound)
			}
			in.records[kind] = retained
		}
	})
}

func TestB27ConfigUnionLegacyWrappersRequireExplicitSnapshotAcrossDrivers(t *testing.T) {
	data := configUnionLegacyArchive(t)
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	manifest := archive.Manifest()
	wantProject := dumpformat.Project{Prefix: "pl", IDMode: "hash", HashLength: 32, ChildCounters: map[string]uint64{}}
	if manifest.History != "unavailable" || len(archive.Records("comment")) != 0 || !reflect.DeepEqual(manifest.Projects, []dumpformat.Project{wantProject}) {
		t.Fatal("strict control does not satisfy the other legacy history/comment/allocator guards")
	}
	wantTargets := map[string]dumpformat.ImportTarget{
		"pl-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": {Type: "decision", Status: "queued"},
		"pl-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb": {Type: "incident", Status: "shared"},
		"pl-cccccccccccccccccccccccccccccccc": {Type: "proposal", Status: "shipped"},
	}
	migration, ok := archive.ImportMigration()
	if !ok || len(archive.Records("bead")) != len(wantTargets) {
		t.Fatal("control lacks its complete admitted strict migration")
	}
	for _, bead := range archive.Records("bead") {
		want, known := wantTargets[bead.ID]
		got, present := migration.Target("pl", bead.ID)
		if !known || bead.Project != "pl" || !present || got != want {
			t.Fatal("strict control lost its independent hash32 identity/type/status targets")
		}
	}
	if len(archive.Records("edge")) != 1 {
		t.Fatal("strict control lacks its supported relation")
	}
	edge := archive.Records("edge")[0].Body
	if edge["target_kind"] != "depends_on_issue_id" || edge["type"] != "blocks" || edge["from"] != "pl-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" || edge["to"] != "pl-cccccccccccccccccccccccccccccccc" {
		t.Fatal("strict control contains a relation outside legacy support")
	}
	// Inspection succeeds without selecting a policy or opening any store.
	profile := configUnionProfile(t, data)
	verification := converterroundtrip.Verification{ArchiveSHA256: profile.ArchiveDigest, DerivedSHA256: profile.DerivedDigest}
	if verification.ArchiveSHA256 != digest(data) || len(verification.DerivedSHA256) != 64 || profile.SourceStoreID != driver.StoreID(manifest.StoreID) {
		t.Fatal("inspection did not bind the complete strict control")
	}
	for _, kind := range []string{"memory", "sqlite", "postgres"} {
		for _, entrypoint := range []string{"Restore", "RestoreWithLease", "explicit-snapshot"} {
			t.Run(kind+"/"+entrypoint, func(t *testing.T) {
				s := openStack(t, kind, nil)
				target := &configUnionObservedStore{Store: s.store}
				if entrypoint == "explicit-snapshot" {
					report, err := converterroundtrip.RestoreImportWithLease(s.ctx, bytes.NewReader(data), target, s.lease, profile, verification)
					if err != nil || target.writes != 1 || report.Archive == nil || report.Restored.Head.String() != "1" {
						t.Fatalf("same strict input with explicit snapshot did not commit: report=%+v writes=%d err=%v", report, target.writes, err)
					}
					for id, want := range wantTargets {
						row, present := readRecord(t, s, id)
						var document pipeline.Document
						if !present || json.Unmarshal(row.Body, &document) != nil || document.Project != "pl" || document.Type != want.Type || document.Fields["status"] != want.Status {
							t.Fatalf("explicit snapshot lost the expected strict record %s", id)
						}
					}
					state := configUnionSnapshotState(t, s, data, profile)
					if len(state.Changesets) != 1 || state.Changesets[0].Kind != model.ChangesetRestore {
						t.Fatal("explicit strict snapshot fabricated history")
					}
					if kind != "memory" {
						s.reopen()
						if !reflect.DeepEqual(state, configUnionSnapshotState(t, s, data, profile)) {
							t.Fatal("explicit strict snapshot changed after SQL reopen")
						}
					}
					return
				}
				if entrypoint == "Restore" {
					// Let this wrapper acquire its own lease if the admission guard
					// regresses; a held fixture lease must not mask Store.Write.
					if err := s.lease.Release(s.ctx); err != nil {
						t.Fatal(err)
					}
					s.lease = nil
				}
				before := relationState(t, s)
				assertUnionLegacyRefusalUnpublished(t, s, profile, archive)
				var report converterroundtrip.Report
				var restoreErr error
				if entrypoint == "Restore" {
					report, restoreErr = converterroundtrip.Restore(s.ctx, bytes.NewReader(data), target, verification)
				} else {
					report, restoreErr = converterroundtrip.RestoreWithLease(s.ctx, bytes.NewReader(data), target, s.lease, verification)
				}
				var refused driver.ErrRestoreHistoryRefused
				if !errors.As(restoreErr, &refused) || refused.Mode != driver.RestoreHistoryModeFull || refused.History != "unavailable" || refused.Reason != driver.RestoreHistoryReasonUnavailable || target.writes != 0 || report.Archive == nil || !report.Restored.Head.IsZero() || report.Recovered || report.IdempotencyKey != "" || report.RequestHash != "" || !reflect.DeepEqual(report.Target, before.Meta) {
					t.Fatalf("legacy strict admission escaped its typed before-write refusal: report=%+v writes=%d err=%v", report, target.writes, restoreErr)
				}
				if !reflect.DeepEqual(before, relationState(t, s)) {
					t.Fatal("legacy strict refusal changed full observed durable state")
				}
				assertUnionLegacyRefusalUnpublished(t, s, profile, archive)
				if kind != "memory" {
					s.reopen()
					if !reflect.DeepEqual(before, relationState(t, s)) {
						t.Fatal("legacy strict refusal left durable state after SQL reopen")
					}
					assertUnionLegacyRefusalUnpublished(t, s, profile, archive)
				}
			})
		}
	}
}

func assertUnionLegacyRefusalUnpublished(t *testing.T, s *stack, p driver.RestoreImportProfile, archive *dumpformat.Archive) {
	t.Helper()
	assertUnionRefusalUnpublished(t, s, p)
	meta, err := s.store.Meta(s.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.store.Read(s.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	principal, err := driver.PrincipalKey(model.PrincipalRef{Kind: "system", ID: "plug-converter"})
	if err != nil {
		t.Fatal(err)
	}
	key := "restore/" + p.ArchiveDigest
	if receipt, err := r.IdempotencyByKey(s.ctx, principal, "", "offline.restore", key); err != nil || receipt != nil {
		t.Fatalf("legacy strict refusal published recovery receipt: %+v %v", receipt, err)
	}
	if change, err := r.ChangesetByKey(s.ctx, principal, "", "offline.restore", key); err != nil || change != nil {
		t.Fatalf("legacy strict refusal published recovery log/events: %+v %v", change, err)
	}
	files := archive.Manifest().Files
	manifest, ok := archive.Member("manifest.json")
	if !ok {
		t.Fatal("strict control lost its manifest")
	}
	files["manifest.json"] = dumpformat.File{SHA256: digest(manifest), Size: int64(len(manifest))}
	for name, file := range files {
		if _, err := r.Blob(s.ctx, driver.Hash(file.SHA256)); !errors.Is(err, driver.ErrNotFound) {
			t.Fatalf("legacy strict refusal published source member blob %s: %v", name, err)
		}
	}
}

func assertUnionRefusalUnpublished(t *testing.T, s *stack, p driver.RestoreImportProfile) {
	t.Helper()
	meta, err := s.store.Meta(s.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.store.Read(s.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	principal, _ := driver.PrincipalKey(p.Admission.Actor)
	if receipt, err := r.IdempotencyByKey(s.ctx, principal, p.AgentLabel(), p.Method, p.IdempotencyKey); err != nil || receipt != nil {
		t.Fatalf("refused import published receipt: %+v %v", receipt, err)
	}
	if changeset, err := r.ChangesetByKey(s.ctx, principal, p.AgentLabel(), p.Method, p.IdempotencyKey); err != nil || changeset != nil {
		t.Fatalf("refused import published log/events: %+v %v", changeset, err)
	}
	if _, err := r.Blob(s.ctx, driver.Hash(p.ArchiveDigest)); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("refused import published archive blob: %v", err)
	}
}
