package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

// These values are private evidence only. Presence is independent of SQL NULL,
// and the archive's JSON representation remains exact (including json.Number).
// A JSON type here does not assert a SQL column's declared type.
type readbackCell struct {
	Present bool   `json:"present"`
	Type    string `json:"json_type"`
	Value   any    `json:"value"`
}

func exactReadbackCell(value any, present bool) readbackCell {
	kind := "absent"
	if present {
		switch value.(type) {
		case nil:
			kind = "null"
		case json.Number:
			kind = "number"
		case string:
			kind = "string"
		case bool:
			kind = "boolean"
		case map[string]any:
			kind = "object"
		case []any:
			kind = "array"
		default:
			kind = "unsupported"
		}
	}
	return readbackCell{Present: present, Type: kind, Value: value}
}

type readbackCellRefusal struct {
	Domain   string       `json:"domain"`
	Project  string       `json:"project"`
	Table    string       `json:"table"`
	Row      string       `json:"row"`
	Field    string       `json:"field"`
	Expected readbackCell `json:"expected"`
	Actual   readbackCell `json:"actual"`
}

type readbackFieldCount struct {
	Domain string `json:"domain"`
	Field  string `json:"field"`
	Rows   int    `json:"rows"`
}

type readbackIdentityRefusal struct {
	Code     string       `json:"code"`
	Table    string       `json:"table"`
	Field    string       `json:"field"`
	RawID    string       `json:"raw_id"`
	Envelope readbackCell `json:"envelope_id"`
}

type beadsReadbackRefusal struct {
	Schema               string                   `json:"schema"`
	Reason               string                   `json:"reason"`
	SourceSHA256         string                   `json:"source_sha256"`
	SubmittedSHA256      string                   `json:"submitted_jsonl_sha256"`
	ProducedSHA256       string                   `json:"produced_archive_sha256,omitempty"`
	AcceptedSHA256       string                   `json:"accepted_archive_sha256,omitempty"`
	ImporterCommit       string                   `json:"importer_commit"`
	ImporterTree         string                   `json:"importer_tree"`
	ImporterBinarySHA256 string                   `json:"importer_binary_sha256"`
	ImporterBuildRun     string                   `json:"importer_build_run"`
	CaptureCommit        string                   `json:"capture_commit"`
	CaptureTree          string                   `json:"capture_tree"`
	CaptureBinarySHA256  string                   `json:"capture_binary_sha256"`
	CaptureBuildRun      string                   `json:"capture_build_run"`
	Validation           string                   `json:"validation"`
	ValidationError      string                   `json:"validation_error,omitempty"`
	Identity             *readbackIdentityRefusal `json:"identity_refusal,omitempty"`
	NativeComparison     string                   `json:"native_comparison"`
	CellRefusals         []readbackCellRefusal    `json:"cell_refusals"`
	FieldRows            []readbackFieldCount     `json:"field_rows"`
}

func retainBeadsReadbackRefusal(output string, source *dumpformat.Archive, submitted, produced []byte, actual *dumpformat.Archive, validationErr error, result *outcome) {
	refusal := beadsReadbackRefusal{
		Schema: "plug-public-readback-refusal/1", Reason: result.Reason,
		SourceSHA256: source.SHA256(), SubmittedSHA256: hash(submitted),
		ImporterCommit: result.BeadsCommit, ImporterTree: result.BeadsTree,
		ImporterBinarySHA256: result.BeadsBinarySHA256, ImporterBuildRun: result.BeadsBuildRun,
		CaptureCommit: result.BeadsCaptureCommit, CaptureTree: result.BeadsCaptureTree,
		CaptureBinarySHA256: result.BeadsCaptureBinarySHA256, CaptureBuildRun: result.BeadsCaptureBuildRun,
		Validation: "FAIL", NativeComparison: "NOT_MEASURED",
		CellRefusals: []readbackCellRefusal{}, FieldRows: []readbackFieldCount{},
	}
	if produced != nil {
		refusal.ProducedSHA256 = hash(produced)
	}
	if validationErr != nil {
		refusal.ValidationError = validationErr.Error()
		var identity *dumpformat.EdgeMetadataIdentityError
		if errors.As(validationErr, &identity) {
			refusal.Identity = &readbackIdentityRefusal{Code: "edge_metadata_identity_mismatch", Table: identity.SourceTable, Field: "metadata._plug_edge_id", RawID: identity.EdgeID, Envelope: exactReadbackCell(identity.EnvelopeID, identity.EnvelopePresent)}
		}
	} else if actual != nil {
		refusal.Validation, refusal.NativeComparison = "PASS", "MEASURED"
		refusal.AcceptedSHA256 = actual.SHA256()
		refusal.CellRefusals = nativeReadbackCellRefusals(source, actual)
		metadata, err := metadataReadbackCellRefusals(submitted, actual)
		if err != nil {
			result.Status, result.Reason, result.BeadsRefusalSHA256 = "BLOCK", "beads_readback_evidence_refused", ""
			return
		}
		refusal.CellRefusals = append(refusal.CellRefusals, metadata...)
		refusal.FieldRows = readbackRefusalCounts(refusal.CellRefusals)
	}
	data, err := json.Marshal(refusal)
	if err != nil || writePrivate(output, "beads-readback-refusal.json", data) != nil {
		result.Status, result.Reason, result.BeadsRefusalSHA256 = "BLOCK", "beads_readback_evidence_refused", ""
		return
	}
	result.BeadsRefusalSHA256 = hash(data)
}

// Diagnostics supplement the unchanged nativeOpaqueLosses oracle. They never
// determine acceptance, rewrite values or inspect a consumer-rejected archive.
func nativeReadbackCellRefusals(source, actual *dumpformat.Archive) []readbackCellRefusal {
	rows := map[string]dumpformat.Record{}
	for _, row := range actual.Records("bead") {
		rows[row.ID] = row
	}
	refusals := []readbackCellRefusal{}
	for _, row := range source.Records("bead") {
		stored, exists := rows[row.ID]
		if !exists {
			refusals = append(refusals, readbackCellRefusal{Domain: "native", Project: row.Project, Table: row.Source.Table, Row: row.ID, Field: "missing_issue", Expected: exactReadbackCell(row.Source.Fields, true), Actual: exactReadbackCell(nil, false)})
			continue
		}
		for _, field := range dumpformat.OpaqueIssueFields() {
			want, present := row.Source.Fields[field]
			got, captured := stored.Source.Fields[field]
			if present != captured || !sameJSON(want, got) {
				refusals = append(refusals, readbackCellRefusal{Domain: "native", Project: row.Project, Table: row.Source.Table, Row: row.ID, Field: field, Expected: exactReadbackCell(want, present), Actual: exactReadbackCell(got, captured)})
			}
		}
	}
	return refusals
}

func metadataReadbackCellRefusals(submitted []byte, actual *dumpformat.Archive) ([]readbackCellRefusal, error) {
	rows := map[string]dumpformat.Record{}
	for _, row := range actual.Records("bead") {
		rows[row.ID] = row
	}
	decoder := json.NewDecoder(bytes.NewReader(submitted))
	decoder.UseNumber()
	refusals := []readbackCellRefusal{}
	for {
		var line map[string]any
		if err := decoder.Decode(&line); err != nil {
			if err == io.EOF {
				return refusals, nil
			}
			return nil, err
		}
		if line["_type"] != "issue" {
			continue
		}
		id, _ := line["id"].(string)
		row, exists := rows[id]
		if !exists {
			continue // The authoritative comparator reports the missing row.
		}
		want, included := line["metadata"]
		got, captured := row.Source.Fields["metadata"]
		if sameJSON(want, got) && included == captured {
			continue
		}
		expected, expectedObject := want.(map[string]any)
		stored, storedObject := got.(map[string]any)
		if !expectedObject || !storedObject {
			refusals = append(refusals, readbackCellRefusal{Domain: "metadata", Project: row.Project, Table: row.Source.Table, Row: id, Field: "metadata", Expected: exactReadbackCell(want, included), Actual: exactReadbackCell(got, captured)})
			continue
		}
		keys := map[string]bool{}
		for key := range expected {
			keys[key] = true
		}
		for key := range stored {
			keys[key] = true
		}
		names := make([]string, 0, len(keys))
		for key := range keys {
			names = append(names, key)
		}
		sort.Strings(names)
		for _, key := range names {
			value, present := expected[key]
			observed, presentActual := stored[key]
			if present != presentActual || !sameJSON(value, observed) {
				refusals = append(refusals, readbackCellRefusal{Domain: "metadata", Project: row.Project, Table: row.Source.Table, Row: id, Field: key, Expected: exactReadbackCell(value, present), Actual: exactReadbackCell(observed, presentActual)})
			}
		}
	}
}

func readbackRefusalCounts(refusals []readbackCellRefusal) []readbackFieldCount {
	type key struct{ domain, field string }
	type row struct{ project, table, id string }
	counts := map[key]map[row]bool{}
	for _, refusal := range refusals {
		k := key{refusal.Domain, refusal.Field}
		if counts[k] == nil {
			counts[k] = map[row]bool{}
		}
		counts[k][row{refusal.Project, refusal.Table, refusal.Row}] = true
	}
	result := []readbackFieldCount{}
	for k, rows := range counts {
		result = append(result, readbackFieldCount{Domain: k.domain, Field: k.field, Rows: len(rows)})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Domain != result[j].Domain {
			return result[i].Domain < result[j].Domain
		}
		return result[i].Field < result[j].Field
	})
	return result
}
