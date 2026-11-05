package dumpformat

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Build disposable wire archives independently of the merger's serializer.
// All values and paths are synthetic; no source or target store is opened.
type mergeFixture struct {
	manifest Manifest
	records  map[string][]Record
	config   []externalEntry
}

// Keep the external schema spelling in this existing source-provenance
// fixture adapter. The contract regression still exercises the exact raw arm.
func setFixtureWorkflowTarget(record *Record, target string) {
	record.Source.Fields["depends_on_issue_id"] = nil
	record.Source.Fields["depends_on_wisp_id"] = target
	record.Body["target_kind"] = "depends_on_wisp_id"
}

func newMergeFixture(t *testing.T, prefix string) *mergeFixture {
	t.Helper()
	f := &mergeFixture{
		manifest: Manifest{
			Format: Format, Schema: 1, Reference: BeadsReference,
			StoreID: "store-" + prefix, Machine: "machine-" + prefix,
			MainVersion: 72, IgnoredVersion: 28, Scope: Scope, History: "available-unverified",
			Projects: []Project{{Prefix: prefix, IDMode: "sequential", LastID: 9, ChildCounters: map[string]uint64{prefix + "-1": 5}}},
			Warnings: []string{
				"not a complete store migration or Plug restore certificate",
				"source history and attribution are unverified; no Plug versions, changesets or live leases invented",
			},
			Verification: map[string]string{},
		},
		records: map[string][]Record{},
	}
	for key, value := range verificationLimits {
		f.manifest.Verification[key] = value
	}
	for lane, table := range map[string]string{"main": "schema_migrations", "ignored": "ignored_schema_migrations"} {
		for version, hash := range migrationHashes[lane] {
			fields := Row{"version": json.Number(strconv.Itoa(version)), "content_hash": hash}
			if lane == "ignored" {
				fields["applied_at"] = "2026-01-01T00:00:00Z"
			}
			f.addArchive(t, table, fields)
		}
	}
	for _, id := range []string{prefix + "-1", prefix + "-1.2", prefix + "-2"} {
		fields := Row{}
		for _, column := range strings.Fields(issueColumns) {
			if column != "row_lock" && column != "is_blocked" {
				fields[column] = nil
			}
		}
		fields["id"], fields["title"], fields["status"], fields["issue_type"] = id, "Synthetic "+id, "open", "task"
		fields["priority"] = json.Number("2")
		fields["content_hash"] = "source-content-digest-" + id
		fields["metadata"] = map[string]any{"large_number": json.Number("9007199254740993"), "explicit_null": nil}
		body := Row{"type": "issue", "source_type": "task", "title": fields["title"], "status_category": "unstarted", "labels": []any{"example"}}
		for _, column := range []string{"description", "design", "acceptance_criteria", "notes", "priority", "assignee", "owner", "created_at", "created_by", "updated_at", "closed_at", "due_at", "defer_until", "started_at", "metadata", "spec_id", "external_ref", "pinned"} {
			body[column] = fields[column]
		}
		f.records["bead"] = append(f.records["bead"], Record{Kind: "bead", ID: id, Project: prefix, Body: body, Source: Provenance{Table: "issues", Fields: fields}})
	}
	comment := Row{"id": prefix + "-comment", "issue_id": prefix + "-1", "author": "synthetic", "text": "Comment", "created_at": "2026-01-01"}
	f.records["comment"] = []Record{{Kind: "comment", ID: prefix + "-comment", Project: prefix, Body: comment, Source: Provenance{Table: "comments", Fields: comment}}}
	edge := Row{"id": prefix + "-edge", "issue_id": prefix + "-1", "depends_on_issue_id": prefix + "-2", "depends_on_wisp_id": nil, "depends_on_external": nil, "type": "blocks", "created_at": "2026-01-01", "created_by": "synthetic", "metadata": nil, "thread_id": ""}
	f.records["edge"] = []Record{{Kind: "edge", ID: prefix + "-edge", Project: prefix, Body: Row{"from": prefix + "-1", "to": prefix + "-2", "target_kind": "depends_on_issue_id", "type": "blocks", "metadata": nil, "thread_id": ""}, Source: Provenance{Table: "dependencies", Fields: edge}}}
	f.addConfig("config", "shared.token", map[string]any{"$secret": "config/shared.token"}, "secret")
	f.addConfig("config", "kv.memory."+prefix, "Synthetic memory", "public")
	f.addArchive(t, "issue_counter", Row{"prefix": prefix, "last_id": json.Number("9")})
	f.addArchive(t, "child_counters", Row{"parent_id": prefix + "-1", "last_child": json.Number("5")})
	f.addArchive(t, "issue_versions", Row{"issue_id": prefix + "-1", "revision": json.Number("9007199254740993"), "epoch": "source-epoch-" + prefix, "durable_state": "unchanged history", "change_actor": "synthetic", "change_agent": nil, "change_message": nil, "change_at": "2026-01-01", "removed_at": nil, "removed_reason": nil, "attribution_status": "unknown", "removed_restriction": nil})
	for _, family := range externalFamilies {
		var entry externalEntry
		entry.Input.Family, entry.Input.Path, entry.Input.State = family, "/synthetic/"+prefix+"/"+family, "absent"
		if family == "project-config" {
			entry.Input.State, entry.Input.Precedence = "json-config", 2
			entry.Values = map[string]any{"project": prefix, "nested": map[string]any{"api_key": map[string]any{"$secret": "synthetic/" + prefix}}}
		}
		f.config = append(f.config, entry)
	}
	return f
}

func (f *mergeFixture) addArchive(t *testing.T, table string, fields Row) {
	t.Helper()
	data := fixtureJSON(t, fields)
	f.records["beads-archive"] = append(f.records["beads-archive"], Record{
		Kind: "beads-archive", ID: table + "/" + digest(data),
		Body:   Row{"read_only": true, "redactable": true, "history_verified": false, "attribution_verified": false},
		Source: Provenance{Table: table, Fields: fields},
	})
}

func (f *mergeFixture) addConfig(table, key string, value any, classification string) {
	kind, id := "config", table+"/"+key
	if strings.HasPrefix(key, "kv.memory.") {
		kind, id = "memory", strings.TrimPrefix(key, "kv.memory.")
	}
	f.records[kind] = append(f.records[kind], Record{Kind: kind, ID: id,
		Body:   Row{"key": id, "value": value, "classification": classification},
		Source: Provenance{Table: table, Fields: Row{"key": key, "value": value}},
	})
}

func (f *mergeFixture) bytes(t *testing.T, mutate func(*Manifest, map[string][]byte)) []byte {
	t.Helper()
	m := f.manifest
	m.Files, m.Kinds = map[string]File{}, []Kind{}
	counts := map[string]int{}
	files := map[string][]byte{}
	for kind, records := range f.records {
		if len(records) == 0 {
			continue
		}
		rows := append([]Record(nil), records...)
		sort.Slice(rows, func(i, j int) bool { return recordKey(rows[i]) < recordKey(rows[j]) })
		var data []byte
		for _, record := range rows {
			data = append(data, fixtureJSON(t, record)...)
			counts[record.Source.Table]++
			if record.Kind == "bead" {
				counts["labels"] += len(record.Body["labels"].([]any))
			}
		}
		path := "records/" + kind + ".ndjson"
		files[path] = data
		m.Kinds = append(m.Kinds, Kind{Name: kind, Version: 1, File: path, Count: len(rows)})
	}
	sort.Slice(m.Kinds, func(i, j int) bool { return m.Kinds[i].Name < m.Kinds[j].Name })
	entries := append([]externalEntry(nil), f.config...)
	sort.Slice(entries, func(i, j int) bool {
		a, b := entries[i].Input, entries[j].Input
		if a.Precedence != b.Precedence {
			return a.Precedence < b.Precedence
		}
		return a.Family+"\x00"+a.Path < b.Family+"\x00"+b.Path
	})
	files["config/sources.json"] = fixtureJSON(t, entries)
	for table := range tableColumns {
		m.Sources = append(m.Sources, Disposition{Family: table, Rows: counts[table], Action: actionFor(table), Reason: "synthetic source census"})
	}
	for _, family := range []string{"issues.row_lock", "issues.is_blocked"} {
		m.Sources = append(m.Sources, Disposition{Family: family, Rows: counts["issues"], Action: "manifest/rebuild", Reason: "excluded operational field"})
	}
	for _, entry := range entries {
		d := Disposition{Family: entry.Input.Family, Action: "manifest/rebuild", Reason: "requested source absent at declared path"}
		if entry.Input.State == "json-config" {
			d.Rows, d.Action, d.Reason = len(entry.Values), "archive", "explicit JSON config; secrets are references; precedence retained, effective config not inferred"
		}
		m.Sources = append(m.Sources, d)
	}
	sort.Slice(m.Sources, func(i, j int) bool {
		a, b := m.Sources[i], m.Sources[j]
		return a.Family+"\x00"+a.Reason < b.Family+"\x00"+b.Reason
	})
	for path, data := range files {
		m.Files[path] = File{Size: int64(len(data)), SHA256: digest(data)}
	}
	if mutate != nil {
		mutate(&m, files)
	}
	files["manifest.json"] = fixtureJSON(t, m)
	return fixtureTar(t, files)
}

func fixtureJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return append(data, '\n')
}

func fixtureTar(t *testing.T, files map[string][]byte) []byte {
	t.Helper()
	paths := []string{}
	for path := range files {
		if path != "manifest.json" {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	var data bytes.Buffer
	tw := tar.NewWriter(&data)
	for _, path := range append([]string{"manifest.json"}, paths...) {
		content := files[path]
		if err := tw.WriteHeader(&tar.Header{Name: path, Mode: 0o600, Size: int64(len(content)), Typeflag: tar.TypeReg, ModTime: time.Unix(0, 0), Format: tar.FormatUSTAR}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(content); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return data.Bytes()
}
