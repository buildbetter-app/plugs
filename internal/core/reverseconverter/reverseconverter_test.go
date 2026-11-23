package reverseconverter

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	result, err := ConvertWithOptions(dump, Options{All: true})
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

func TestConvertMemoryIdentityPreservesVerbatimAndRefusesBlank(t *testing.T) {
	for _, tc := range []struct {
		name string
		key  string
		want bool
	}{
		{"spaced-unicode", " \t É cafe\u0301.🧭 \t ", true},
		{"empty", "", false},
		{"ascii-whitespace", " \t ", false},
		{"unicode-whitespace", "\u2003\u00a0\t", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dump := cloneDump(fixtureDump())
			record := &dump.Records["memory"][0]
			record.ID, record.Body["key"], record.Source.Fields["key"] = tc.key, tc.key, "kv.memory."+tc.key
			before := cloneDump(dump)
			result, err := Convert(dump)
			if !reflect.DeepEqual(dump, before) {
				t.Fatal("conversion mutated the source memory")
			}
			if !tc.want {
				var refusal *Refusal
				if !errors.As(err, &refusal) || !strings.Contains(err.Error(), "incomplete identity or provenance") || len(result.JSONL) != 0 || result.Digest != "" {
					t.Fatalf("blank memory identity produced output: result=%+v err=%v", result, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("verbatim spaced key was refused: %v", err)
			}
			found := false
			for _, line := range bytes.Split(bytes.TrimSpace(result.JSONL), []byte{'\n'}) {
				var row map[string]any
				if err := json.Unmarshal(line, &row); err != nil {
					t.Fatal(err)
				}
				if row["_type"] == "memory" && row["key"] == tc.key {
					if row["value"] != "memory value" || found {
						t.Fatalf("verbatim memory value or uniqueness changed: %v", row)
					}
					found = true
				}
			}
			if !found {
				t.Fatalf("reverse output lost the exact spaced key: %s", result.JSONL)
			}
		})
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

func TestConvertBeadsDestinationPreservesUserMetadataAndUnwrapsEdges(t *testing.T) {
	dump := cloneDump(fixtureDump())
	dump.Records["bead"][0].Body["metadata"] = map[string]any{
		"custom": map[string]any{"text": "kept", "number": json.Number("9007199254740993")},
	}
	dump.Records["edge"][0].Body["metadata"] = map[string]any{"why": "kept", "number": json.Number("9007199254740993")}
	result, err := ConvertWithOptions(dump, Options{BeadsDestination: true})
	if err != nil {
		t.Fatal(err)
	}
	var issue issueLine
	for _, line := range bytes.Split(bytes.TrimSpace(result.JSONL), []byte{'\n'}) {
		var candidate issueLine
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.UseNumber()
		if err := decoder.Decode(&candidate); err == nil && candidate.ID == "demo-3" {
			issue = candidate
			break
		}
	}
	if !reflect.DeepEqual(issue.Metadata, dump.Records["bead"][0].Body["metadata"]) {
		t.Fatalf("issue metadata changed: got=%#v", issue.Metadata)
	}
	for key := range issue.Metadata {
		if strings.HasPrefix(key, "beads.") || strings.HasPrefix(key, "_plug_") {
			t.Fatalf("Plug bookkeeping leaked into Beads metadata: %q", key)
		}
	}
	if len(issue.Dependencies) != 0 {
		t.Fatalf("unexpected dependencies on demo-3: %+v", issue.Dependencies)
	}
	for _, line := range bytes.Split(bytes.TrimSpace(result.JSONL), []byte{'\n'}) {
		var candidate issueLine
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.UseNumber()
		if err := decoder.Decode(&candidate); err == nil && candidate.ID == "demo-2" {
			if candidate.Dependencies[0].Metadata != `{"number":9007199254740993,"why":"kept"}` {
				t.Fatalf("edge metadata changed: %q", candidate.Dependencies[0].Metadata)
			}
			return
		}
	}
	t.Fatal("edge owner was not emitted")
}

func TestConvertBeadsDestinationPreservesNullMetadata(t *testing.T) {
	dump := cloneDump(fixtureDump())
	dump.Records["bead"][0].Body["metadata"] = nil
	dump.Records["edge"][0].Body["metadata"] = nil
	result, err := ConvertWithOptions(dump, Options{BeadsDestination: true})
	if err != nil {
		t.Fatal(err)
	}
	issueNull, edgeNull := false, false
	for _, line := range bytes.Split(bytes.TrimSpace(result.JSONL), []byte{'\n'}) {
		if bytes.Contains(line, []byte(`"id":"demo-3"`)) && bytes.Contains(line, []byte(`"metadata":null`)) {
			issueNull = true
		}
		if bytes.Contains(line, []byte(`"id":"demo-2"`)) && bytes.Contains(line, []byte(`"metadata":null`)) {
			edgeNull = true
		}
	}
	if !issueNull || !edgeNull {
		t.Fatalf("null metadata was not retained: %s", result.JSONL)
	}
}

func TestConvertBeadsDestinationUnwrapsExistingEdgeEnvelope(t *testing.T) {
	dump := cloneDump(fixtureDump())
	for i := range dump.Manifest.Kinds {
		if dump.Manifest.Kinds[i].Name == "edge" {
			dump.Manifest.Kinds[i].Version = dumpformat.EdgeVersionTypedDependencies
		}
	}
	envelope := map[string]any{"_plug_edge_id": "edge-1", "_plug_metadata": map[string]any{}}
	dump.Records["edge"][0].Body["metadata"] = envelope
	dump.Records["edge"][0].Source.Fields["metadata"] = envelope
	result, err := ConvertWithOptions(dump, Options{BeadsDestination: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range bytes.Split(bytes.TrimSpace(result.JSONL), []byte{'\n'}) {
		var candidate issueLine
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.UseNumber()
		if err := decoder.Decode(&candidate); err == nil && candidate.ID == "demo-2" {
			if len(candidate.Dependencies) != 1 || candidate.Dependencies[0].Metadata != "{}" {
				t.Fatalf("existing edge envelope was not unwrapped: %+v", candidate.Dependencies)
			}
			return
		}
	}
	t.Fatal("edge owner was not emitted")
}

func TestConvertBeadsDestinationPreservesMappedNativeTypes(t *testing.T) {
	for _, issueType := range []string{"rig", "merge-request", "molecule"} {
		t.Run(issueType, func(t *testing.T) {
			dump := unknownTypeDump(issueType)
			// Native source-plane projection may expose a portable body alias
			// while the authenticated physical field retains the custom type.
			dump.Records["bead"][0].Body["source_type"] = "task"
			result, err := ConvertWithOptions(dump, Options{
				BeadsDestination: true,
				Directives:       []ontologymigration.Directive{allowTypeDirective(issueType)},
			})
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Contains(result.JSONL, []byte(`"issue_type":"`+issueType+`"`)) {
				t.Fatalf("custom issue type was rewritten: %s", result.JSONL)
			}
			if bytes.Contains(result.JSONL, []byte(`"_plug_native_type"`)) {
				t.Fatalf("native type provenance leaked: %s", result.JSONL)
			}
		})
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

func versionedDependencyDump(kind string, metadata any) Dump {
	dump := cloneDump(fixtureDump())
	for i := range dump.Manifest.Kinds {
		if dump.Manifest.Kinds[i].Name == "edge" {
			dump.Manifest.Kinds[i].Version = dumpformat.EdgeVersionTypedDependencies
		}
	}
	edge := &dump.Records["edge"][0]
	edge.Body["type"], edge.Source.Fields["type"] = kind, kind
	edge.Body["metadata"], edge.Source.Fields["metadata"] = metadata, metadata
	edge.Source.Fields["thread_id"] = edge.Body["thread_id"]
	return dump
}

func TestReverseFixed19LocalKindsPreserveAllDependencyFields(t *testing.T) {
	kinds := []string{"blocks", "parent-child", "conditional-blocks", "waits-for", "related", "discovered-from", "replies-to", "relates-to", "duplicates", "supersedes", "authored-by", "assigned-to", "approved-by", "attests", "tracks", "until", "caused-by", "validates", "delegated-from"}
	for _, kind := range kinds {
		for _, version := range []int{dumpformat.EdgeVersionTypedDependencies, dumpformat.EdgeVersionLocalOpaque} {
			t.Run(fmt.Sprintf("%s/v%d", kind, version), func(t *testing.T) {
				const raw = " {\"gate\":\"all-children\",\"also_blocks\":true,\"opaque\":[9007199254740993,1.0,null]} \n"
				dump := versionedDependencyDump(kind, raw)
				dump.Manifest.Kinds[4].Version = version
				edge := &dump.Records["edge"][0]
				edge.Source.Fields["depends_on_wisp_id"], edge.Source.Fields["depends_on_external"] = nil, nil
				before := cloneDump(dump)
				result, err := Convert(dump)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, line := range bytes.Split(bytes.TrimSpace(result.JSONL), []byte{'\n'}) {
					var issue issueLine
					if err := json.Unmarshal(line, &issue); err != nil {
						t.Fatal(err)
					}
					if issue.ID != "demo-2" {
						continue
					}
					if len(issue.Dependencies) != 1 {
						t.Fatal("fixed dependency omitted or duplicated")
					}
					got := issue.Dependencies[0]
					if got.ID != edge.ID || got.IssueID != edge.Body["from"] || got.DependsOnID != edge.Body["to"] || got.DependsOnExternal != "" || got.Type != kind || got.ThreadID != edge.Body["thread_id"] || got.CreatedAt != edge.Source.Fields["created_at"] || got.CreatedBy != edge.Source.Fields["created_by"] {
						t.Fatalf("fixed dependency fields changed: %+v", got)
					}
					var envelope map[string]any
					if err := json.Unmarshal([]byte(got.Metadata), &envelope); err != nil || envelope["_plug_edge_id"] != edge.ID || envelope["_plug_metadata"] != raw {
						t.Fatalf("fixed dependency lost its identity envelope or SQL-text bytes: %#v %v", envelope, err)
					}
					found = true
				}
				if !found || !reflect.DeepEqual(dump, before) {
					t.Fatal("fixed dependency reverse mutated or omitted source")
				}
				if kind != "blocks" && kind != "parent-child" && kind != "related" && kind != "discovered-from" {
					dump.Manifest.Kinds[4].Version = 1
					got, err := Convert(dump)
					var refusal *Refusal
					if !errors.As(err, &refusal) || len(got.JSONL) != 0 {
						t.Fatal("legacy v1 emitted a newly typed fixed local dependency")
					}
				}
			})
		}
	}
}

func TestConvertVersionedDependenciesKeepRawMetadataAndOneEnvelope(t *testing.T) {
	const sql = " {\"gate\":\"any-children\",\"also_blocks\":true,\"spawner_id\":\"demo-1\",\"opaque\":{\"large\":9007199254740993,\"literal\":1.0}} \n"
	const existing = " {\"_plug_edge_id\":\"edge-1\",\"_plug_metadata\":\" {\\\"gate\\\":\\\"any-children\\\",\\\"opaque\\\":9007199254740993} \\n\"} \n"
	for _, test := range []struct {
		name     string
		kind     string
		raw      any
		envelope bool
	}{
		{"legacy NULL", "waits-for", nil, false},
		{"explicit empty", "waits-for", map[string]any{}, false},
		{"empty gate object", "waits-for", map[string]any{"gate": ""}, false},
		{"empty gate SQL", "waits-for", " {\"gate\":\"\"} \n", false},
		{"empty gate object envelope", "waits-for", map[string]any{"_plug_edge_id": "edge-1", "_plug_metadata": map[string]any{"gate": ""}}, true},
		{"empty gate SQL envelope", "waits-for", " {\"_plug_edge_id\":\"edge-1\",\"_plug_metadata\":\" {\\\"gate\\\":\\\"\\\"} \\n\"} \n", true},
		{"SQL text", "waits-for", sql, false},
		{"typed object", "waits-for", map[string]any{"gate": "all-children", "also_blocks": false, "spawner_id": "demo-1", "opaque": json.Number("9007199254740993")}, false},
		{"conditional array", "conditional-blocks", []any{json.Number("9007199254740993"), json.Number("1.0"), nil}, false},
		{"conditional SQL", "conditional-blocks", ` {"opaque":[9007199254740993,1.0,null]} `, false},
		{"fixed tracks", "tracks", map[string]any{"opaque": json.Number("9007199254740993")}, false},
		{"existing SQL envelope", "waits-for", existing, true},
		{"existing typed envelope", "waits-for", map[string]any{"_plug_edge_id": "edge-1", "_plug_metadata": map[string]any{"gate": "any-children", "opaque": json.Number("9007199254740993")}}, true},
		{"existing ID-only envelope", "waits-for", ` {"_plug_edge_id":"edge-1"} `, true},
	} {
		for _, version := range []int{dumpformat.EdgeVersionTypedDependencies, dumpformat.EdgeVersionLocalOpaque} {
			t.Run(fmt.Sprintf("%s/v%d", test.name, version), func(t *testing.T) {
				dump := versionedDependencyDump(test.kind, test.raw)
				dump.Manifest.Kinds[4].Version = version
				before := cloneDump(dump)
				result, err := Convert(dump)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, raw := range bytes.Split(bytes.TrimSpace(result.JSONL), []byte{'\n'}) {
					var issue issueLine
					if json.Unmarshal(raw, &issue) != nil || issue.ID != "demo-2" {
						continue
					}
					if len(issue.Dependencies) != 1 || issue.Dependencies[0].Type != test.kind || issue.Dependencies[0].ID != "edge-1" || issue.Dependencies[0].ThreadID != "thread-1" {
						t.Fatalf("dependency identity/type/thread changed: %#v", issue.Dependencies)
					}
					encoded := issue.Dependencies[0].Metadata
					if test.envelope {
						want, ok := test.raw.(string)
						if !ok {
							want, err = canonicalJSON(test.raw)
							if err != nil {
								t.Fatal(err)
							}
						}
						if encoded != want {
							t.Fatalf("existing flat envelope changed: %q want %q", encoded, want)
						}
					} else {
						var envelope map[string]any
						decoder := json.NewDecoder(strings.NewReader(encoded))
						decoder.UseNumber()
						if err := decoder.Decode(&envelope); err != nil || envelope["_plug_edge_id"] != "edge-1" {
							t.Fatalf("invalid identity envelope: %#v %v", envelope, err)
						}
						payload, present := envelope["_plug_metadata"]
						if present != (test.raw != nil) || !reflect.DeepEqual(payload, test.raw) {
							t.Fatalf("source payload type/value changed: %#v want %#v", payload, test.raw)
						}
					}
					found = true
				}
				if !found || !reflect.DeepEqual(dump, before) {
					t.Fatal("conversion omitted dependency or mutated retained raw source")
				}
			})
		}
	}
}

func TestConvertVersionedDependenciesRetainRefusalBoundaries(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*Dump)
	}{
		{"v1 waits", func(d *Dump) { d.Manifest.Kinds[4].Version = 1 }},
		{"v1 conditional", func(d *Dump) {
			d.Manifest.Kinds[4].Version = 1
			d.Records["edge"][0].Body["type"] = "conditional-blocks"
			d.Records["edge"][0].Source.Fields["type"] = "conditional-blocks"
		}},
		{"v1 tracks", func(d *Dump) {
			d.Manifest.Kinds[4].Version = 1
			d.Records["edge"][0].Body["type"] = "tracks"
			d.Records["edge"][0].Source.Fields["type"] = "tracks"
		}},
		{"unknown edge version", func(d *Dump) { d.Manifest.Kinds[4].Version = 4 }},
		{"zero edge version", func(d *Dump) { d.Manifest.Kinds[4].Version = 0 }},
		{"other kind version", func(d *Dump) { d.Manifest.Kinds[0].Version = 2 }},
		{"custom kind", func(d *Dump) {
			d.Records["edge"][0].Body["type"] = "custom"
			d.Records["edge"][0].Source.Fields["type"] = "custom"
		}},
		{"wisp", func(d *Dump) { d.Records["edge"][0].Body["target_kind"] = "depends_on_wisp_id" }},
		{"dangling", func(d *Dump) {
			d.Records["edge"][0].Body["to"] = "demo-missing"
			d.Records["edge"][0].Source.Fields["depends_on_issue_id"] = "demo-missing"
		}},
		{"multiple target arms", func(d *Dump) { d.Records["edge"][0].Source.Fields["depends_on_external"] = "external:demo" }},
		{"missing raw column", func(d *Dump) { delete(d.Records["edge"][0].Body, "metadata") }},
		{"missing source column", func(d *Dump) { delete(d.Records["edge"][0].Source.Fields, "metadata") }},
		{"source metadata changed", func(d *Dump) { d.Records["edge"][0].Source.Fields["metadata"] = map[string]any{"gate": "any-children"} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			dump := versionedDependencyDump("waits-for", nil)
			test.mutate(&dump)
			result, err := Convert(dump)
			var refusal *Refusal
			if !errors.As(err, &refusal) || len(result.JSONL) != 0 {
				t.Fatalf("invalid versioned dependency emitted output: %v", err)
			}
		})
	}
	for i, metadata := range []any{
		`{"gate":"all-children","gate":"any-children"}`, `{} {}`, `{"x":"\ud800"}`,
		map[string]any{"gate": nil}, map[string]any{"gate": " "}, map[string]any{"gate": "other"},
		map[string]any{"gate": true}, map[string]any{"gate": json.Number("1")},
		map[string]any{"also_blocks": nil}, map[string]any{"also_blocks": json.Number("1")},
		map[string]any{"spawner_id": json.Number("1")},
		map[string]any{"_plug_edge_id": "wrong", "_plug_metadata": map[string]any{}},
		map[string]any{"_plug_metadata": map[string]any{}},
		map[string]any{"_plug_edge_id": "edge-1", "_plug_metadata": nil},
		map[string]any{"_plug_edge_id": "edge-1", "extra": true},
		map[string]any{"opaque": map[string]any{"_plug_edge_id": "nested"}},
		map[string]any{"_plug_edge_id": "edge-1", "_plug_metadata": map[string]any{"_plug_edge_id": "edge-1"}},
	} {
		for _, version := range []int{dumpformat.EdgeVersionTypedDependencies, dumpformat.EdgeVersionLocalOpaque} {
			t.Run(fmt.Sprintf("invalid_metadata/v%d/%d", version, i), func(t *testing.T) {
				dump := versionedDependencyDump("waits-for", metadata)
				dump.Manifest.Kinds[4].Version = version
				result, err := Convert(dump)
				var refusal *Refusal
				if !errors.As(err, &refusal) || len(result.JSONL) != 0 {
					t.Fatalf("ambiguous versioned metadata emitted output: %#v %v", metadata, err)
				}
			})
		}
	}
}

func TestConvertPreservesSQLTextMetadataNumbersWithoutChangingSource(t *testing.T) {
	const raw = " {\"large\":9007199254740993,\"literal\":1.0,\"null\":null,\"nested\":{\"value\":[1,null]}} \n"
	dump := cloneDump(fixtureDump())
	dump.Records["bead"][0].Body["metadata"] = raw
	dump.Records["bead"][0].Source.Fields["metadata"] = raw
	before := cloneDump(dump)
	result, err := Convert(dump)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range bytes.Split(bytes.TrimSpace(result.JSONL), []byte{'\n'}) {
		var issue struct {
			ID       string                     `json:"id"`
			Metadata map[string]json.RawMessage `json:"metadata"`
		}
		if json.Unmarshal(line, &issue) != nil || issue.ID != dump.Records["bead"][0].ID {
			continue
		}
		for key, want := range map[string]string{"large": "9007199254740993", "literal": "1.0", "null": "null", "nested": `{"value":[1,null]}`} {
			if string(issue.Metadata[key]) != want {
				t.Fatalf("SQL metadata %s=%s want %s", key, issue.Metadata[key], want)
			}
		}
		if !reflect.DeepEqual(dump, before) {
			t.Fatal("JSONL conversion changed the retained SQL metadata text")
		}
		return
	}
	t.Fatal("SQL metadata issue was not emitted")
}

func TestConvertRefusesMalformedSQLTextMetadataWithoutOutput(t *testing.T) {
	for _, raw := range []string{"null", "[]", "1", `{"nested":{"x":1,"x":2}}`, "{} {}", `{"x":"\ud800"}`} {
		dump := cloneDump(fixtureDump())
		dump.Records["bead"][0].Body["metadata"] = raw
		dump.Records["bead"][0].Source.Fields["metadata"] = raw
		result, err := Convert(dump)
		if err == nil || len(result.JSONL) != 0 {
			t.Fatalf("malformed SQL metadata emitted a partial result: %q %v", raw, err)
		}
	}
}

func TestReverseArchiveRetainsSeparatePhysicalSourceContract(t *testing.T) {
	original := canonicalFixtureBytes(t)
	baseline, err := ConvertArchive(original)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		marker any
		valid  bool
	}{
		{"known", dumpformat.IssueSourceFieldsContract, true},
		{"unknown", "beads-issues-source-fields/2", false},
		{"empty", "", false},
		{"null", nil, false},
		{"number", json.Number("1"), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			reader, writer := tar.NewReader(bytes.NewReader(original)), tar.NewWriter(&output)
			for {
				header, err := reader.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				member, err := io.ReadAll(reader)
				if err != nil {
					t.Fatal(err)
				}
				if header.Name == "manifest.json" {
					var manifest map[string]any
					decoder := json.NewDecoder(bytes.NewReader(member))
					decoder.UseNumber()
					if err := decoder.Decode(&manifest); err != nil {
						t.Fatal(err)
					}
					manifest["source_fields_contract"] = test.marker
					member, err = json.Marshal(manifest)
					if err != nil {
						t.Fatal(err)
					}
					member = append(member, '\n')
					header.Size = int64(len(member))
				}
				if err := writer.WriteHeader(header); err != nil {
					t.Fatal(err)
				}
				if _, err := writer.Write(member); err != nil {
					t.Fatal(err)
				}
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			dump, err := ReadBytes(output.Bytes())
			if !test.valid {
				if err == nil {
					t.Fatal("invalid source contract admitted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if dump.Manifest.SourceFieldsContract != dumpformat.IssueSourceFieldsContract || manifestEdgeVersion(dump.Manifest) != 1 {
				t.Fatal("source-field contract lost or conflated with edge version")
			}
			result, err := Convert(dump)
			if err != nil || !bytes.Equal(result.JSONL, baseline.JSONL) {
				t.Fatalf("marker alone changed ordinary JSONL projection: %v", err)
			}
		})
	}
}

func TestReverseDetachedUnknownPhysicalSourceContractRefuses(t *testing.T) {
	dump := fixtureDump()
	dump.Manifest.SourceFieldsContract = "beads-issues-source-fields/2"
	result, err := Convert(dump)
	if err == nil || len(result.JSONL) != 0 {
		t.Fatal("unknown detached physical contract returned output")
	}
}

func TestVersionedReverseKeepsCurrentTypedExternalTarget(t *testing.T) {
	for _, test := range []struct {
		kind string
		raw  any
	}{
		{"waits-for", map[string]any{"gate": "any-children", "also_blocks": true}},
		{"conditional-blocks", `{"opaque":9007199254740993}`},
		{"custom-external", []any{json.Number("9007199254740993"), nil}},
	} {
		t.Run(test.kind, func(t *testing.T) {
			dump := versionedDependencyDump(test.kind, test.raw)
			edge := &dump.Records["edge"][0]
			// Deliberately collide with a real local issue ID: the typed arm
			// must remain external before any local lookup or scheduling.
			edge.Body["target_kind"], edge.Body["to"] = "depends_on_external", "demo-1"
			delete(edge.Source.Fields, "depends_on_issue_id")
			edge.Source.Fields["depends_on_external"] = "demo-1"
			result, err := Convert(dump)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Projection.Edges) != 1 || result.Projection.Edges[0].TargetKind != "depends_on_external" || result.Projection.Edges[0].To != "demo-1" {
				t.Fatal("versioned reverse changed typed external projection")
			}
			decoder := json.NewDecoder(bytes.NewReader(result.JSONL))
			found := false
			for {
				var issue issueLine
				if err := decoder.Decode(&issue); err == io.EOF {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				if issue.ID != "demo-2" {
					continue
				}
				if len(issue.Dependencies) != 1 || issue.Dependencies[0].DependsOnID != "" || issue.Dependencies[0].DependsOnExternal != "demo-1" || issue.Dependencies[0].Type != test.kind {
					t.Fatalf("external JSONL arm changed: %#v", issue.Dependencies)
				}
				var envelope map[string]any
				metadata := json.NewDecoder(strings.NewReader(issue.Dependencies[0].Metadata))
				metadata.UseNumber()
				if err := metadata.Decode(&envelope); err != nil {
					t.Fatal(err)
				}
				if envelope["_plug_edge_id"] != "edge-1" || !reflect.DeepEqual(envelope["_plug_metadata"], test.raw) {
					t.Fatal("external metadata source representation changed")
				}
				found = true
			}
			if !found {
				t.Fatal("external dependency owner omitted")
			}
		})
	}
}

func TestConvertV3LocalOpaqueDependencyKeepsIdentityAndRawMetadata(t *testing.T) {
	const raw = " {\"large\":9007199254740993,\"literal\":1.0,\"null\":null} "
	dump := versionedDependencyDump("local-opaque", raw)
	for i := range dump.Manifest.Kinds {
		if dump.Manifest.Kinds[i].Name == "edge" {
			dump.Manifest.Kinds[i].Version = dumpformat.EdgeVersionLocalOpaque
		}
	}
	before := cloneDump(dump)
	result, err := Convert(dump)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(result.JSONL))
	decoder.UseNumber()
	found := false
	for {
		var issue issueLine
		if err := decoder.Decode(&issue); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if issue.ID != "demo-2" {
			continue
		}
		if len(issue.Dependencies) != 1 || issue.Dependencies[0].Type != "local-opaque" || issue.Dependencies[0].DependsOnID != "demo-1" || issue.Dependencies[0].DependsOnExternal != "" || issue.Dependencies[0].ThreadID != "thread-1" {
			t.Fatalf("local opaque dependency changed wire identity: %#v", issue.Dependencies)
		}
		var envelope map[string]any
		metadata := json.NewDecoder(strings.NewReader(issue.Dependencies[0].Metadata))
		metadata.UseNumber()
		if err := metadata.Decode(&envelope); err != nil || envelope["_plug_edge_id"] != "edge-1" || envelope["_plug_metadata"] != raw {
			t.Fatalf("local opaque metadata envelope changed: %#v %v", envelope, err)
		}
		found = true
	}
	if !found || !reflect.DeepEqual(dump, before) {
		t.Fatal("v3 conversion omitted the edge or mutated source values")
	}
}

func TestConvertV3LocalOpaqueRetainsNegativeAndExternalBoundaries(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*Dump)
	}{
		{"v2-local-custom", func(d *Dump) { d.Manifest.Kinds[4].Version = dumpformat.EdgeVersionTypedDependencies }},
		{"unknown-version", func(d *Dump) { d.Manifest.Kinds[4].Version = 4 }},
		{"v1-fixed-local-kind", func(d *Dump) {
			d.Manifest.Kinds[4].Version = 1
			d.Records["edge"][0].Body["type"] = "replies-to"
			d.Records["edge"][0].Source.Fields["type"] = "replies-to"
		}},
		{"malformed-metadata", func(d *Dump) {
			d.Records["edge"][0].Body["metadata"] = "{} {}"
			d.Records["edge"][0].Source.Fields["metadata"] = "{} {}"
		}},
		{"wisp-target", func(d *Dump) { d.Records["edge"][0].Body["target_kind"] = "depends_on_wisp_id" }},
		{"dangling-local-target", func(d *Dump) {
			d.Records["edge"][0].Body["to"] = "demo-missing"
			d.Records["edge"][0].Source.Fields["depends_on_issue_id"] = "demo-missing"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			dump := versionedDependencyDump("local-opaque", map[string]any{"opaque": true})
			for i := range dump.Manifest.Kinds {
				if dump.Manifest.Kinds[i].Name == "edge" {
					dump.Manifest.Kinds[i].Version = dumpformat.EdgeVersionLocalOpaque
				}
			}
			test.mutate(&dump)
			result, err := Convert(dump)
			var refusal *Refusal
			if !errors.As(err, &refusal) || len(result.JSONL) != 0 {
				t.Fatalf("invalid v3 local opaque row emitted output: %v", err)
			}
		})
	}
	dump := versionedDependencyDump("external-opaque", []any{json.Number("9007199254740993"), nil})
	for i := range dump.Manifest.Kinds {
		if dump.Manifest.Kinds[i].Name == "edge" {
			dump.Manifest.Kinds[i].Version = dumpformat.EdgeVersionLocalOpaque
		}
	}
	edge := &dump.Records["edge"][0]
	delete(edge.Source.Fields, "depends_on_issue_id")
	edge.Source.Fields["depends_on_external"], edge.Body["target_kind"], edge.Body["to"] = "demo-1", "depends_on_external", "demo-1"
	result, err := Convert(dump)
	if err != nil || len(result.Projection.Edges) != 1 || result.Projection.Edges[0].TargetKind != "depends_on_external" {
		t.Fatalf("v3 external arm changed: %v", err)
	}
}
