package reverseconverter

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func fixtureDump() Dump {
	stamp := "2026-01-01T00:00:00Z"
	updated := "2026-01-02T00:00:00Z"
	issue := func(id, title, status, category, issueType string, priority int, result string, labels []any) Record {
		body := map[string]any{
			"type":            "issue",
			"source_type":     issueType,
			"title":           title,
			"status_category": category,
			"priority":        priority,
			"created_at":      stamp,
			"updated_at":      updated,
			"labels":          labels,
			"metadata":        map[string]any{"component": "fixture"},
		}
		if result != "" {
			body["result"] = result
		}
		return Record{
			Kind: "bead", ID: id, Project: "demo", Body: body,
			Source: Provenance{Table: "issues", Fields: map[string]any{
				"id": id, "issue_type": issueType, "title": title, "status": status,
				"priority": priority, "created_at": stamp, "updated_at": updated,
			}},
		}
	}

	records := map[string][]Record{
		"bead": {
			issue("demo-3", "Done", "closed", "completed", "feature", 3, "success", []any{}),
			issue("demo-1", "Open", "open", "unstarted", "task", 1, "", []any{"zeta", "alpha"}),
			issue("demo-2", "Started", "in_progress", "started", "bug", 2, "", []any{}),
		},
		"edge": {{
			Kind: "edge", ID: "edge-1", Project: "demo",
			Body:   map[string]any{"from": "demo-2", "to": "demo-1", "target_kind": "depends_on_issue_id", "type": "blocks", "metadata": map[string]any{"why": "fixture"}, "thread_id": "thread-1"},
			Source: Provenance{Table: "dependencies", Fields: map[string]any{"id": "edge-1", "issue_id": "demo-2", "depends_on_issue_id": "demo-1", "created_at": "2026-01-01T00:00:00Z", "created_by": "fixture"}},
		}},
		"comment": {{
			Kind: "comment", ID: "comment-1", Project: "demo",
			Body:   map[string]any{"id": "comment-1", "issue_id": "demo-1", "author": "fixture", "text": "A preserved comment", "created_at": "2026-01-02T00:00:00Z"},
			Source: Provenance{Table: "comments", Fields: map[string]any{"id": "comment-1", "issue_id": "demo-1"}},
		}},
		"memory": {{
			Kind: "memory", ID: "fixture", Project: "", Body: map[string]any{"key": "fixture", "value": "memory value", "classification": "public"},
			Source: Provenance{Table: "config", Fields: map[string]any{"key": "kv.memory.fixture", "value": "memory value"}},
		}},
		"config": {{
			Kind: "config", ID: "config/github.token", Project: "", Body: map[string]any{"key": "config/github.token", "value": map[string]any{"$secret": "config/github.token"}, "classification": "secret"},
			Source: Provenance{Table: "config", Fields: map[string]any{"key": "github.token", "value": map[string]any{"$secret": "config/github.token"}}},
		}},
		"beads-archive": {{
			Kind: "beads-archive", ID: "issue_versions/1", Project: "", Body: map[string]any{"read_only": true, "redactable": true, "history_verified": false, "attribution_verified": false},
			Source: Provenance{Table: "issue_versions", Fields: map[string]any{"issue_id": "demo-1", "revision": 1}},
		}},
	}
	return Dump{
		Manifest: Manifest{
			Format: PlugDumpFormat, Schema: 1, Reference: BeadsReference,
			StoreID: "fixture-store", Machine: "fixture", MainVersion: 72, IgnoredVersion: 28, Scope: "offline-head-and-source-archives",
			History: "available-unverified", Warnings: []string{"fixture warning"},
			Kinds: []Kind{
				{Name: "bead", Version: 1, File: "records/bead.ndjson", Count: 3},
				{Name: "beads-archive", Version: 1, File: "records/beads-archive.ndjson", Count: 1},
				{Name: "comment", Version: 1, File: "records/comment.ndjson", Count: 1},
				{Name: "config", Version: 1, File: "records/config.ndjson", Count: 1},
				{Name: "edge", Version: 1, File: "records/edge.ndjson", Count: 1},
				{Name: "memory", Version: 1, File: "records/memory.ndjson", Count: 1},
			},
		},
		Records: records,
	}
}

func cloneDump(input Dump) Dump {
	output := input
	output.Manifest.Kinds = append([]Kind(nil), input.Manifest.Kinds...)
	output.Manifest.Warnings = append([]string(nil), input.Manifest.Warnings...)
	output.Records = make(map[string][]Record, len(input.Records))
	for kind, records := range input.Records {
		output.Records[kind] = make([]Record, len(records))
		for i, record := range records {
			output.Records[kind][i] = record
			output.Records[kind][i].Body = cloneMap(record.Body)
			output.Records[kind][i].Source.Fields = cloneMap(record.Source.Fields)
		}
	}
	return output
}

func TestConvertIsDeterministicAndReportsLoss(t *testing.T) {
	first := fixtureDump()
	second := cloneDump(first)
	second.Records["bead"][0], second.Records["bead"][2] = second.Records["bead"][2], second.Records["bead"][0]
	second.Records["edge"] = append([]Record(nil), second.Records["edge"]...)

	a, err := Convert(first)
	if err != nil {
		t.Fatalf("first conversion: %v", err)
	}
	b, err := Convert(second)
	if err != nil {
		t.Fatalf("second conversion: %v", err)
	}
	if a.Digest == "" || a.Digest != b.Digest || !bytes.Equal(a.JSONL, b.JSONL) {
		t.Fatalf("conversion is not deterministic: %q != %q", a.Digest, b.Digest)
	}
	if a.Report.OutputDigest != a.Digest || a.Report.ArchiveRows != 1 || a.Report.ConfigAsMemories != 1 {
		t.Fatalf("unexpected report: %+v", a.Report)
	}
	if len(a.Report.LostPostSwitchHistory) != 2 || len(a.Report.ConfigReferences) != 1 {
		t.Fatalf("loss/reference report: %+v", a.Report)
	}

	lines := bytes.Split(bytes.TrimSpace(a.JSONL), []byte{'\n'})
	if len(lines) != 6 { // header, three issues, one memory, one config reference
		t.Fatalf("JSONL lines=%d, want 6:\n%s", len(lines), a.JSONL)
	}
	var header map[string]any
	if err := json.Unmarshal(lines[0], &header); err != nil || header["_schema"] != BeadsJSONL {
		t.Fatalf("header=%s err=%v", lines[0], err)
	}
	var closed map[string]any
	if err := json.Unmarshal(lines[1+2], &closed); err != nil {
		t.Fatal(err)
	}
	if closed["id"] != "demo-3" || closed["status"] != "closed" || closed["issue_type"] != "feature" {
		t.Fatalf("closed projection=%v", closed)
	}
	if metadata, ok := closed["metadata"].(map[string]any); !ok || metadata["_plug_status_outcome"] != "success" {
		t.Fatalf("closed outcome was not carried in metadata: %v", closed["metadata"])
	}
	if !strings.Contains(string(a.JSONL), `"key":"plug.config/github.token"`) || !strings.Contains(string(a.JSONL), `\"$secret\":\"config/github.token\"`) {
		t.Fatalf("secret reference was not preserved in memory representation:\n%s", a.JSONL)
	}
}

func TestConvertArchivesUnsupportedSourceFields(t *testing.T) {
	dump := cloneDump(fixtureDump())
	archived := map[string]any{
		"estimated_minutes": json.Number("30"), "no_history": true,
		"compaction_level": json.Number("2"), "compacted_at": "2026-01-01T00:00:00Z",
		"compacted_at_commit": "synthetic-commit", "original_size": json.Number("0"),
		"current_revision": json.Number("9007199254740993"), "content_hash": "synthetic-content-hash",
		"source_repo": "", "storage_class": "versioned",
	}
	for key, value := range archived {
		dump.Records["bead"][0].Source.Fields[key] = value
	}
	before := cloneDump(dump)
	result, err := Convert(dump)
	if err != nil {
		t.Fatalf("archivable source field refused: %v", err)
	}
	if !reflect.DeepEqual(dump, before) {
		t.Fatal("conversion changed the source dump")
	}
	for key := range archived {
		if !containsString(result.Report.ArchivedSourceFields, "bead/demo-3/source/"+key) {
			t.Fatalf("archived source field %s missing from report: %+v", key, result.Report)
		}
	}
	for _, line := range bytes.Split(bytes.TrimSpace(result.JSONL), []byte{'\n'}) {
		var issue issueLine
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.UseNumber()
		if err := decoder.Decode(&issue); err != nil || issue.ID != "demo-3" {
			continue
		}
		encoded, ok := issue.Metadata["_plug_source_fields"].(string)
		if !ok {
			t.Fatalf("source archive must be a lossless JSON string: %#v", issue.Metadata)
		}
		var fields map[string]any
		decoder = json.NewDecoder(strings.NewReader(encoded))
		decoder.UseNumber()
		if err := decoder.Decode(&fields); err != nil || !reflect.DeepEqual(fields, archived) {
			t.Fatalf("archived source field missing from issue metadata: %#v", issue.Metadata)
		}
		return
	}
	t.Fatal("archived issue was not emitted")
}

func TestConvertRefusesWorkflowAndUnknownSourceFields(t *testing.T) {
	for field, value := range map[string]any{
		"payload": map[string]any{"state": "running"}, "wi" + "sp_type": "run",
		"work_type": "workflow", "role_type": "agent", "ephemeral": true,
		"is_template": true, "target": "demo-1", "unknown_column": "unaccounted",
	} {
		t.Run(field, func(t *testing.T) {
			dump := cloneDump(fixtureDump())
			dump.Records["bead"][0].Source.Fields[field] = value
			result, err := Convert(dump)
			var refusal *Refusal
			if !errors.As(err, &refusal) || !strings.Contains(strings.Join(refusal.Reasons, "\n"), field) || len(result.JSONL) != 0 || len(refusal.Report.UnsupportedFields) == 0 {
				t.Fatalf("source field was not refused without output: result=%+v err=%v", result, err)
			}
		})
	}
}

func TestConvertRefusesSourceArchiveMetadataCollision(t *testing.T) {
	dump := cloneDump(fixtureDump())
	dump.Records["bead"][0].Source.Fields["no_history"] = true
	dump.Records["bead"][0].Body["metadata"].(map[string]any)["_plug_source_fields"] = "existing"
	result, err := Convert(dump)
	if err == nil || !strings.Contains(err.Error(), "reserves _plug_source_fields") || len(result.JSONL) != 0 {
		t.Fatalf("reserved metadata was overwritten: result=%+v err=%v", result, err)
	}
}

func TestConvertRefusesEmptyMemoryAndConfigValues(t *testing.T) {
	for _, kind := range []string{"memory", "config"} {
		t.Run(kind, func(t *testing.T) {
			dump := cloneDump(fixtureDump())
			record := &dump.Records[kind][0]
			if kind == "config" {
				record.ID = "config/display.theme"
				record.Body["key"], record.Body["classification"] = record.ID, "public"
				record.Source.Fields["key"] = "display.theme"
			}
			record.Body["value"], record.Source.Fields["value"] = "", ""
			result, err := Convert(dump)
			var refusal *Refusal
			if !errors.As(err, &refusal) || !strings.Contains(err.Error(), "empty memory value") || len(result.JSONL) != 0 || !containsString(refusal.Report.UnsupportedFields, kind+"/"+record.ID+"/value") {
				t.Fatalf("empty value was not refused with a report: result=%+v err=%v", result, err)
			}
		})
	}
}

func TestConvertRefusesUnsupportedAndDuplicateInputs(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Dump)
		want string
	}{
		{
			name: "unsupported body field",
			edit: func(d *Dump) { d.Records["bead"][0].Body["await_type"] = "human" },
			want: "unsupported field",
		},
		{
			name: "unsupported kind",
			edit: func(d *Dump) {
				kind := "wi" + "sp"
				d.Records[kind] = []Record{{Kind: kind, ID: kind + "-1", Body: map[string]any{}, Source: Provenance{Table: kind + "s", Fields: map[string]any{}}}}
				d.Manifest.Kinds = append(d.Manifest.Kinds, Kind{Name: kind, Version: 1, File: "records/" + kind + ".ndjson", Count: 1})
			},
			want: "invalid or unsorted kind registry",
		},
		{
			name: "duplicate edge",
			edit: func(d *Dump) {
				d.Records["edge"] = append(d.Records["edge"], d.Records["edge"][0])
				d.Manifest.Kinds[4].Count = 2
			},
			want: "duplicate edge",
		},
		{
			name: "external link",
			edit: func(d *Dump) {
				d.Records["edge"][0].Body["target_kind"] = "depends_on_external"
				d.Records["edge"][0].Body["to"] = "external:tracker/1"
			},
			want: "unsupported external target",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dump := cloneDump(fixtureDump())
			test.edit(&dump)
			_, err := Convert(dump)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Convert error=%v, want %q", err, test.want)
			}
			var refusal *Refusal
			if !errors.As(err, &refusal) || refusal.Report.InputDigest == "" {
				t.Fatalf("error did not carry structured report: %T %+v", err, err)
			}
		})
	}
}

func TestConvertRefusesMalformedTypedAndMismatchedSourceFields(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Dump)
		want string
	}{
		{
			name: "typed priority",
			edit: func(d *Dump) { d.Records["bead"][0].Body["priority"] = "2" },
			want: "invalid priority",
		},
		{
			name: "typed metadata",
			edit: func(d *Dump) { d.Records["bead"][0].Body["metadata"] = "not-an-object" },
			want: "metadata is not an object",
		},
		{
			name: "edge source target",
			edit: func(d *Dump) { d.Records["edge"][0].Source.Fields["depends_on_issue_id"] = "demo-3" },
			want: "source target mismatch",
		},
		{
			name: "config source key",
			edit: func(d *Dump) { d.Records["config"][0].Source.Fields["key"] = "github.other" },
			want: "identity does not match source key",
		},
		{
			name: "config source value",
			edit: func(d *Dump) {
				d.Records["config"][0].Source.Fields["value"] = map[string]any{"$secret": "config/other"}
			},
			want: "source/body value mismatch",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dump := cloneDump(fixtureDump())
			test.edit(&dump)
			_, err := Convert(dump)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Convert error=%v, want %q", err, test.want)
			}
		})
	}
}

func TestConvertCarriesEdgeIdentityEnvelope(t *testing.T) {
	result, err := Convert(fixtureDump())
	if err != nil {
		t.Fatal(err)
	}
	var issue issueLine
	for _, line := range bytes.Split(bytes.TrimSpace(result.JSONL), []byte{'\n'}) {
		var candidate issueLine
		if err := json.Unmarshal(line, &candidate); err == nil && candidate.ID == "demo-2" {
			issue = candidate
			break
		}
	}
	if len(issue.Dependencies) != 1 {
		t.Fatalf("demo-2 dependencies=%+v", issue.Dependencies)
	}
	var envelope map[string]any
	if err := json.Unmarshal([]byte(issue.Dependencies[0].Metadata), &envelope); err != nil {
		t.Fatalf("edge metadata is not JSON: %v", err)
	}
	if envelope["_plug_edge_id"] != "edge-1" {
		t.Fatalf("edge identity envelope=%v", envelope)
	}
	if metadata, ok := envelope["_plug_metadata"].(map[string]any); !ok || metadata["why"] != "fixture" {
		t.Fatalf("source edge metadata was not nested in envelope: %v", envelope)
	}
	if result.Projection.Edges[0].ID != "edge-1" {
		t.Fatalf("projection edge identity=%+v", result.Projection.Edges)
	}
}

func TestReadUsesCanonicalArchiveValidator(t *testing.T) {
	records := []byte(`{"kind":"bead","id":"demo-1","project":"demo","body":{"type":"issue","source_type":"task","title":"one","status_category":"unstarted","priority":1,"created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"},"source":{"table":"issues","fields":{"status":"open"}}}` + "\n")
	// The old reader accepted this shallow fixture despite its missing
	// canonical disclosures, allocator inventory, source census and schema.
	archive := makeArchive(t, records, 0o600, tar.TypeReg, "bead")
	trailing := append(append([]byte(nil), archive...), make([]byte, 1536)...)
	trailing[len(archive)] = 1 // keep two zero end blocks after the hidden data
	for _, test := range []struct {
		name string
		data []byte
		want string
	}{
		{"shallow archive", archive, "missing incomplete-conversion disclosures"},
		{"mode", makeArchive(t, records, 0o644, tar.TypeReg, "bead"), "noncanonical tar member"},
		{"symlink", makeArchive(t, records, 0o600, tar.TypeSymlink, "bead"), "noncanonical tar member"},
		{"unknown kind member", makeArchive(t, records, 0o600, tar.TypeReg, "unknown"), "unknown archive member"},
		{"truncated end blocks", archive[:len(archive)-512], "invalid tar length or end blocks"},
		{"partial block", archive[:len(archive)-1], "invalid tar length or end blocks"},
		{"hidden trailing data", trailing, "unaccounted trailing archive data"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ReadBytes(test.data)
			if err == nil || !strings.Contains(err.Error(), "canonical plug-dump/1 validation failed: "+test.want) {
				t.Fatalf("Read error=%v, want canonical refusal %q", err, test.want)
			}
			result, err := ConvertArchive(test.data)
			var refusal *Refusal
			if !errors.As(err, &refusal) || len(result.JSONL) != 0 {
				t.Fatalf("ConvertArchive produced output for invalid input: result=%+v err=%v", result, err)
			}
		})
	}
}

func makeArchive(t *testing.T, records []byte, mode int64, typeflag byte, kind string) []byte {
	t.Helper()
	fileDigest := sha256.Sum256(records)
	configData := []byte("[]\n")
	configDigest := sha256.Sum256(configData)
	manifest := Manifest{
		Format: PlugDumpFormat, Schema: 1, Reference: BeadsReference,
		StoreID: "archive-fixture", Machine: "test-machine", MainVersion: 72, IgnoredVersion: 28,
		Scope: "offline-head-and-source-archives", History: "unavailable",
		Projects: []Project{}, Sources: []json.RawMessage{},
		Warnings: []string{"fixture warning"}, Verification: map[string]string{"capture": "fixture"},
		Kinds: []Kind{{Name: kind, Version: 1, File: "records/" + kind + ".ndjson", Count: 1}},
		Files: map[string]File{"config/sources.json": {SHA256: hex.EncodeToString(configDigest[:]), Size: int64(len(configData))}, "records/" + kind + ".ndjson": {SHA256: hex.EncodeToString(fileDigest[:]), Size: int64(len(records))}},
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	tw := tar.NewWriter(&output)
	writeMember := func(name string, data []byte, memberMode int64, memberType byte) {
		header := &tar.Header{Name: name, Mode: memberMode, Size: int64(len(data)), Uid: 0, Gid: 0, ModTime: time.Unix(0, 0).UTC(), Typeflag: memberType, Format: tar.FormatUSTAR}
		if memberType == tar.TypeSymlink {
			header.Size = 0
			header.Linkname = "records/bead.ndjson"
		}
		if err := tw.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if memberType == tar.TypeReg && len(data) > 0 {
			if _, err := tw.Write(data); err != nil {
				t.Fatal(err)
			}
		}
	}
	writeMember("manifest.json", manifestBytes, 0o600, tar.TypeReg)
	writeMember("config/sources.json", configData, 0o600, tar.TypeReg)
	writeMember("records/"+kind+".ndjson", records, mode, typeflag)
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestProjectionIsJSONStable(t *testing.T) {
	result, err := Convert(fixtureDump())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(result.Projection)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Projection
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Projection, decoded) {
		t.Fatalf("projection JSON changed semantics: %#v != %#v", result.Projection, decoded)
	}
	if len(encoded) == 0 {
		t.Fatal("unreachable")
	}
}
