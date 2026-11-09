package reverseconverter

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/ontologymigration"
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

func TestConvertPreservesOpaqueAgentStateMetadata(t *testing.T) {
	const state = "opaque/v1:phase=ready\x00"
	dump := cloneDump(fixtureDump())
	dump.Records["bead"][0].Source.Fields["agent_state"] = state
	result, err := Convert(dump)
	if err != nil {
		t.Fatalf("agent_state was refused: %v", err)
	}
	if containsString(result.Report.UnsupportedFields, "bead/demo-3/agent_state") || containsString(result.Report.ArchivedSourceFields, "bead/demo-3/source/agent_state") {
		t.Fatalf("agent_state was classified as loss: %+v", result.Report)
	}
	for _, line := range bytes.Split(bytes.TrimSpace(result.JSONL), []byte{'\n'}) {
		var issue issueLine
		if err := json.Unmarshal(line, &issue); err != nil || issue.ID != "demo-3" {
			continue
		}
		if got, ok := issue.Metadata[beadsAgentStateMetadataKey].(string); !ok || got != state {
			t.Fatalf("agent_state metadata=%#v, want exact %q", issue.Metadata[beadsAgentStateMetadataKey], state)
		}
		return
	}
	t.Fatal("agent_state issue was not emitted")
}

func TestConvertRefusesAgentStateTypeOrMetadataConflict(t *testing.T) {
	tests := []struct {
		name  string
		value any
		setup func(map[string]any)
		want  string
	}{
		{name: "non-string source", value: json.Number("1"), want: "source field \"agent_state\" is not a string"},
		{name: "metadata conflict", value: "source-state", setup: func(metadata map[string]any) { metadata[beadsAgentStateMetadataKey] = "metadata-state" }, want: "conflicts with metadata"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dump := cloneDump(fixtureDump())
			dump.Records["bead"][0].Source.Fields["agent_state"] = tt.value
			if tt.setup != nil {
				tt.setup(dump.Records["bead"][0].Body["metadata"].(map[string]any))
			}
			result, err := Convert(dump)
			var refusal *Refusal
			if !errors.As(err, &refusal) || !strings.Contains(err.Error(), tt.want) || len(result.JSONL) != 0 {
				t.Fatalf("agent_state validation=%+v err=%v", result, err)
			}
		})
	}
}

func TestConvertPreservesOpaqueAwaitIDMetadata(t *testing.T) {
	const awaitID = "opaque-await/v1:owner=worker\x00"
	dump := cloneDump(fixtureDump())
	dump.Records["bead"][0].Source.Fields["await_id"] = awaitID
	result, err := Convert(dump)
	if err != nil {
		t.Fatalf("await_id was refused: %v", err)
	}
	if containsString(result.Report.UnsupportedFields, "bead/demo-3/await_id") || containsString(result.Report.ArchivedSourceFields, "bead/demo-3/source/await_id") {
		t.Fatalf("await_id was classified as loss: %+v", result.Report)
	}
	for _, line := range bytes.Split(bytes.TrimSpace(result.JSONL), []byte{'\n'}) {
		var issue issueLine
		if err := json.Unmarshal(line, &issue); err != nil || issue.ID != "demo-3" {
			continue
		}
		if got, ok := issue.Metadata[beadsAwaitIDMetadataKey].(string); !ok || got != awaitID {
			t.Fatalf("await_id metadata=%#v, want exact %q", issue.Metadata[beadsAwaitIDMetadataKey], awaitID)
		}
		return
	}
	t.Fatal("await_id issue was not emitted")
}

func TestConvertPreservesEveryOpaqueIssueFieldByteForByte(t *testing.T) {
	want := map[string]any{
		"agent_state": "opaque-agent\x00state",
		"await_id":    "opaque-await\x00id",
		"await_type":  "human",
		"ephemeral":   json.Number("1"),
		"event_kind":  nil,
		"hook_bead":   "demo-1",
		"is_template": false,
		"mol_type":    "formula",
		"payload":     map[string]any{"state": "waiting", "number": json.Number("9007199254740993")},
		"role_bead":   "demo-2",
		"role_type":   "agent",
		"target":      "demo-3",
		"timeout_ns":  json.Number("9007199254740993"),
		"waiters":     []any{"demo-1", nil},
		"wisp_type":   "run",
		"work_type":   "workflow",
	}
	dump := cloneDump(fixtureDump())
	for field, value := range want {
		dump.Records["bead"][0].Source.Fields[field] = value
	}
	result, err := Convert(dump)
	if err != nil {
		t.Fatalf("opaque fields were refused: %v", err)
	}
	var rawIssue struct {
		ID       string                     `json:"id"`
		Metadata map[string]json.RawMessage `json:"metadata"`
	}
	for _, line := range bytes.Split(bytes.TrimSpace(result.JSONL), []byte{'\n'}) {
		var candidate struct {
			ID       string                     `json:"id"`
			Metadata map[string]json.RawMessage `json:"metadata"`
		}
		if err := json.Unmarshal(line, &candidate); err == nil && candidate.ID == "demo-3" {
			rawIssue = candidate
			break
		}
	}
	if rawIssue.ID == "" {
		t.Fatal("opaque issue was not emitted")
	}
	for field, value := range want {
		key := "beads." + field
		got, ok := rawIssue.Metadata[key]
		encoded, marshalErr := json.Marshal(value)
		if marshalErr != nil || !ok || !bytes.Equal(got, encoded) {
			t.Fatalf("opaque metadata %s=%s want %s (present=%v marshal=%v)", key, got, encoded, ok, marshalErr)
		}
		if containsString(result.Report.UnsupportedFields, "bead/demo-3/"+field) || containsString(result.Report.ArchivedSourceFields, "bead/demo-3/source/"+field) {
			t.Fatalf("opaque field %s was classified as loss: %+v", field, result.Report)
		}
	}
}

func TestConvertRefusesAwaitIDTypeOrMetadataConflict(t *testing.T) {
	tests := []struct {
		name  string
		value any
		setup func(map[string]any)
		want  string
	}{
		{name: "non-string source", value: json.Number("1"), want: "source field \"await_id\" is not a string"},
		{name: "metadata conflict", value: "source-await", setup: func(metadata map[string]any) { metadata[beadsAwaitIDMetadataKey] = "metadata-await" }, want: "conflicts with metadata"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dump := cloneDump(fixtureDump())
			dump.Records["bead"][0].Source.Fields["await_id"] = tt.value
			if tt.setup != nil {
				tt.setup(dump.Records["bead"][0].Body["metadata"].(map[string]any))
			}
			result, err := Convert(dump)
			var refusal *Refusal
			if !errors.As(err, &refusal) || !strings.Contains(err.Error(), tt.want) || len(result.JSONL) != 0 {
				t.Fatalf("await_id validation=%+v err=%v", result, err)
			}
		})
	}
}

func TestConvertRefusesWorkflowAndUnknownSourceFields(t *testing.T) {
	for field, value := range map[string]any{"unknown_column": "unaccounted"} {
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
			name: "external arm with mismatched local source",
			edit: func(d *Dump) {
				d.Records["edge"][0].Body["target_kind"] = "depends_on_external"
				d.Records["edge"][0].Body["to"] = "external:tracker/1"
			},
			want: "source target mismatch",
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

func unknownTypeDump(issueType string) Dump {
	dump := cloneDump(fixtureDump())
	bead := &dump.Records["bead"][0]
	bead.Body["source_type"] = issueType
	bead.Source.Fields["issue_type"] = issueType
	return dump
}

func allowTypeDirective(issueType string) ontologymigration.Directive {
	return ontologymigration.Directive{
		ID: "preserve-" + issueType, Kind: ontologymigration.MapType,
		Target: ontologymigration.Target{Project: "demo", Type: issueType},
		From:   issueType, To: "issue",
	}
}

func TestConvertUnknownTypeRequiresDirective(t *testing.T) {
	result, err := Convert(unknownTypeDump("decision"))
	var refusal *Refusal
	if !errors.As(err, &refusal) || !strings.Contains(err.Error(), "unsupported kind \"decision\"") || len(result.JSONL) != 0 {
		t.Fatalf("unknown type was not refused without output: result=%+v err=%v", result, err)
	}
}

func TestConvertUnknownTypeDirectivePreservesSourceType(t *testing.T) {
	dump := unknownTypeDump("decision")
	directive := allowTypeDirective("decision")
	first, err := ConvertWithDirectives(dump, []ontologymigration.Directive{directive})
	if err != nil {
		t.Fatalf("directive-authorized decision was refused: %v", err)
	}
	second, err := ConvertWithDirectives(dump, []ontologymigration.Directive{directive})
	if err != nil || first.Digest == "" || first.Digest != second.Digest || !bytes.Equal(first.JSONL, second.JSONL) {
		t.Fatalf("directive conversion was not deterministic: first=%+v second=%+v err=%v", first, second, err)
	}
	if len(first.Report.TypeMappings) != 1 || first.Report.TypeMappings[0].ID != directive.ID || first.Report.TypeMappings[0].From != "decision" || first.Report.TypeMappings[0].To != "issue" {
		t.Fatalf("type mapping evidence=%+v", first.Report.TypeMappings)
	}
	var found issueLine
	for _, line := range bytes.Split(bytes.TrimSpace(first.JSONL), []byte{'\n'}) {
		var candidate issueLine
		if err := json.Unmarshal(line, &candidate); err == nil && candidate.ID == "demo-3" {
			found = candidate
			break
		}
	}
	if found.ID != "demo-3" || found.IssueType != "decision" {
		t.Fatalf("source type was rewritten: %+v", found)
	}
}

func TestConvertUnknownTypeDirectiveSupportsCustomAndExactRows(t *testing.T) {
	dump := unknownTypeDump("architecture")
	directive := allowTypeDirective("architecture")
	directive.Target.ID = "demo-3"
	result, err := ConvertWithDirectives(dump, []ontologymigration.Directive{directive})
	if err != nil || len(result.Report.TypeMappings) != 1 {
		t.Fatalf("custom exact-row type was not preserved: result=%+v err=%v", result, err)
	}
}

func TestConvertTypeDirectiveRejectsIrrelevantMembers(t *testing.T) {
	tests := map[string]func(*ontologymigration.Directive){
		"field":      func(d *ontologymigration.Directive) { d.Field = "custom" },
		"value":      func(d *ontologymigration.Directive) { d.Value = 1 },
		"conversion": func(d *ontologymigration.Directive) { d.Conversion = "string_to_integer" },
		"violation":  func(d *ontologymigration.Directive) { d.Violation = "digest" },
		"reason":     func(d *ontologymigration.Directive) { d.Reason = "migration note" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			directive := allowTypeDirective("decision")
			mutate(&directive)
			result, err := ConvertWithDirectives(unknownTypeDump("decision"), []ontologymigration.Directive{directive})
			if err == nil || !strings.Contains(err.Error(), "not valid for map_type") || len(result.JSONL) != 0 {
				t.Fatalf("irrelevant map_type member was accepted: result=%+v err=%v", result, err)
			}
		})
	}
}

func TestConvertTypeDirectiveValidationAndUsage(t *testing.T) {
	tests := []struct {
		name       string
		input      Dump
		directives []ontologymigration.Directive
		want       string
	}{
		{name: "missing target project", input: unknownTypeDump("decision"), directives: []ontologymigration.Directive{{ID: "bad", Kind: ontologymigration.MapType, Target: ontologymigration.Target{Type: "decision"}, From: "decision", To: "issue"}}, want: "project"},
		{name: "wrong source", input: unknownTypeDump("decision"), directives: []ontologymigration.Directive{{ID: "bad", Kind: ontologymigration.MapType, Target: ontologymigration.Target{Project: "demo", Type: "other"}, From: "decision", To: "issue"}}, want: "source"},
		{name: "same target", input: unknownTypeDump("decision"), directives: []ontologymigration.Directive{{ID: "bad", Kind: ontologymigration.MapType, Target: ontologymigration.Target{Project: "demo", Type: "decision"}, From: "decision", To: "decision"}}, want: "change"},
		{name: "wrong kind", input: unknownTypeDump("decision"), directives: []ontologymigration.Directive{{ID: "bad", Kind: ontologymigration.MapStatus, Target: ontologymigration.Target{Project: "demo", Type: "decision"}, From: "decision", To: "issue"}}, want: "map_type"},
		{name: "unused", input: fixtureDump(), directives: []ontologymigration.Directive{allowTypeDirective("unused")}, want: "unused"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ConvertWithDirectives(test.input, test.directives)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("directive error=%v, want %q", err, test.want)
			}
		})
	}

	dump := unknownTypeDump("decision")
	conflict := []ontologymigration.Directive{allowTypeDirective("decision"), {ID: "other", Kind: ontologymigration.MapType, Target: ontologymigration.Target{Project: "demo", Type: "decision"}, From: "decision", To: "issue-2"}}
	if _, err := ConvertWithDirectives(dump, conflict); err == nil || !strings.Contains(err.Error(), "conflicting") {
		t.Fatalf("overlapping directives accepted: %v", err)
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

func TestReadConvertCanonicalArchiveMapsPinnedSQLRepresentations(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  bool
	}{
		{name: "numeric zero", value: json.Number("0"), want: false},
		{name: "numeric one", value: json.Number("1"), want: true},
		{name: "textual zero", value: "0", want: false},
		{name: "textual one", value: "1", want: true},
		{name: "boolean false", value: false, want: false},
		{name: "boolean true", value: true, want: true},
		{name: "explicit null retains omission", value: nil, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := canonicalPinnedArchive(t, test.value)
			beforeBytes := append([]byte(nil), data...)
			dump, err := ReadBytes(data)
			if err != nil {
				t.Fatalf("ReadBytes: %v", err)
			}
			beforeDigest := digestDump(dump)
			var sourcePinned any
			for _, record := range dump.Records["bead"] {
				if record.ID == "demo-7" {
					if !reflect.DeepEqual(record.Body["pinned"], test.value) {
						t.Fatalf("body pinned=%#v, want %#v", record.Body["pinned"], test.value)
					}
					sourcePinned = record.Source.Fields["pinned"]
				}
			}
			if !reflect.DeepEqual(sourcePinned, test.value) {
				t.Fatalf("source pinned=%#v, want %#v", sourcePinned, test.value)
			}

			result, err := Convert(dump)
			if err != nil {
				t.Fatalf("Convert: %v", err)
			}
			if result.Report.InputDigest != beforeDigest {
				t.Fatalf("input digest changed: got %q want %q", result.Report.InputDigest, beforeDigest)
			}
			if !bytes.Equal(data, beforeBytes) {
				t.Fatal("ReadBytes/Convert mutated the archive input")
			}

			var got issueLine
			for _, line := range bytes.Split(bytes.TrimSpace(result.JSONL), []byte{'\n'}) {
				var candidate issueLine
				if err := json.Unmarshal(line, &candidate); err == nil && candidate.ID == "demo-7" {
					got = candidate
					break
				}
			}
			if got.ID != "demo-7" || got.Pinned != test.want {
				t.Fatalf("converted pinned=%v, want %v; JSONL=%s", got.Pinned, test.want, result.JSONL)
			}
			if raw, textual := test.value.(string); textual {
				var archive map[string]any
				encoded, ok := got.Metadata["_plug_source_fields"].(string)
				if !ok || json.Unmarshal([]byte(encoded), &archive) != nil || archive["pinned"] != raw {
					t.Fatal("textual SQL encoding disappeared from source metadata")
				}
			}
			if test.want && !bytes.Contains(result.JSONL, []byte(`"pinned":true`)) {
				t.Fatalf("numeric 1 did not emit pinned:true: %s", result.JSONL)
			}
		})
	}
}

func TestReadConvertCanonicalArchiveRejectsUnsupportedPinnedValues(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{name: "two", value: json.Number("2")},
		{name: "negative one", value: json.Number("-1")},
		{name: "fraction", value: json.Number("0.5")},
		{name: "long near one", value: json.Number("1.0000000000000000001")},
		{name: "exponent", value: json.Number("1e0")},
		{name: "string leading zero", value: "01"},
		{name: "string double zero", value: "00"},
		{name: "string leading whitespace", value: " 1"},
		{name: "string trailing whitespace", value: "1 "},
		{name: "string decimal", value: "1.0"},
		{name: "string exponent", value: "1e0"},
		{name: "string false", value: "false"},
		{name: "string true", value: "true"},
		{name: "object", value: map[string]any{"value": 1}},
		{name: "array", value: []any{1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := canonicalPinnedArchive(t, test.value)
			dump, err := ReadBytes(data)
			if err != nil {
				t.Fatalf("ReadBytes should validate source/body equality: %v", err)
			}
			result, err := Convert(dump)
			var refusal *Refusal
			if !errors.As(err, &refusal) || !strings.Contains(err.Error(), "pinned") || len(result.JSONL) != 0 {
				t.Fatalf("unsupported pinned value was not refused without output: result=%+v err=%v", result, err)
			}
		})
	}
}

func canonicalPinnedArchive(t *testing.T, pinned any) []byte {
	t.Helper()
	original := canonicalFixtureBytes(t)
	archive, err := dumpformat.Read(bytes.NewReader(original))
	if err != nil {
		t.Fatalf("read canonical fixture: %v", err)
	}
	manifest := archive.Manifest()
	beads := archive.Records("bead")
	var beadData bytes.Buffer
	for _, record := range beads {
		if record.ID == "demo-7" {
			record.Body["pinned"] = pinned
			record.Source.Fields["pinned"] = pinned
		}
		encoded, err := json.Marshal(record)
		if err != nil {
			t.Fatalf("marshal bead %s: %v", record.ID, err)
		}
		beadData.Write(encoded)
		beadData.WriteByte('\n')
	}

	members := make(map[string][]byte, len(manifest.Files))
	for name := range manifest.Files {
		member, ok := archive.Member(name)
		if !ok {
			t.Fatalf("canonical fixture missing member %q", name)
		}
		members[name] = member
	}
	members["records/bead.ndjson"] = beadData.Bytes()
	sum := sha256.Sum256(beadData.Bytes())
	manifest.Files["records/bead.ndjson"] = dumpformat.File{SHA256: hex.EncodeToString(sum[:]), Size: int64(beadData.Len())}
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	manifestData = append(manifestData, '\n')

	names := make([]string, 0, len(members))
	for name := range members {
		names = append(names, name)
	}
	sort.Strings(names)
	var output bytes.Buffer
	tw := tar.NewWriter(&output)
	writeMember := func(name string, data []byte) {
		header := &tar.Header{Name: name, Mode: 0o600, Size: int64(len(data)), Uid: 0, Gid: 0, ModTime: time.Unix(0, 0).UTC(), Typeflag: tar.TypeReg, Format: tar.FormatUSTAR}
		if err := tw.WriteHeader(header); err != nil {
			t.Fatalf("write %s header: %v", name, err)
		}
		if _, err := tw.Write(data); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	writeMember("manifest.json", manifestData)
	for _, name := range names {
		writeMember(name, members[name])
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close archive: %v", err)
	}
	return output.Bytes()
}

func canonicalFixtureBytes(t *testing.T) []byte {
	t.Helper()
	path := filepath.Join("..", "..", "integration", "converterroundtrip", "testdata", "synthetic-plug-dump-1.tar")
	if root := os.Getenv("TEST_SRCDIR"); root != "" {
		path = filepath.Join(root, os.Getenv("TEST_WORKSPACE"), "internal/integration/converterroundtrip/testdata/synthetic-plug-dump-1.tar")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read canonical fixture %s: %v", path, err)
	}
	return data
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

// Typed output retention is distinct from native importer qualification.
func TestReverseRetainsExplicitExternalTargetBeforeLocalLookup(t *testing.T) {
	for _, target := range []string{"external:tracker/1", "demo-1"} {
		t.Run(target, func(t *testing.T) {
			dump := fixtureDump()
			edge := &dump.Records["edge"][0]
			edge.Body["target_kind"], edge.Body["to"] = "depends_on_external", target
			delete(edge.Source.Fields, "depends_on_issue_id")
			edge.Source.Fields["depends_on_external"] = target
			result, err := Convert(dump)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Projection.Edges) != 1 || result.Projection.Edges[0].TargetKind != "depends_on_external" || result.Projection.Edges[0].To != target {
				t.Fatalf("typed identity lost: %+v", result.Projection.Edges)
			}
			var dependency map[string]any
			for _, line := range bytes.Split(result.JSONL, []byte{'\n'}) {
				if len(line) == 0 {
					continue
				}
				var row map[string]any
				if err := json.Unmarshal(line, &row); err != nil {
					t.Fatal(err)
				}
				if deps, ok := row["dependencies"].([]any); ok && len(deps) != 0 {
					dependency = deps[0].(map[string]any)
				}
			}
			if dependency["depends_on_external"] != target {
				t.Fatalf("external field not retained: %v", dependency)
			}
			if _, flattened := dependency["depends_on_id"]; flattened {
				t.Fatal("external identity flattened into local target")
			}
		})
	}
}
