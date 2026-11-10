package reverseconverter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

// ValidateNativeImportJSONL repeats the physical preflight at the target
// boundary, including callers that obtained lossless JSONL without selecting
// Options.NativeImport. No target process or database is opened.
func ValidateNativeImportJSONL(data []byte) error {
	if len(data) == 0 || len(data) > MaxArchiveSize {
		return refusal("invalid native import JSONL size")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var rows []Record
	for {
		var line map[string]any
		if err := decoder.Decode(&line); err == io.EOF {
			break
		} else if err != nil {
			return refusal("invalid native import JSONL")
		}
		if line["_type"] != BeadsJSONLIssueType {
			continue
		}
		metadata, _ := line["metadata"].(map[string]any)
		if line["metadata"] != nil && metadata == nil {
			return refusal("invalid native import metadata object")
		}
		fields := map[string]any{}
		for _, name := range dumpformat.OpaqueIssueFields() {
			if value, ok := metadata["beads."+name]; ok {
				fields[name] = value
			}
		}
		if value, exists := metadata["_plug_source_fields"]; exists {
			raw, ok := value.(string)
			if !ok {
				return refusal("invalid native source archive")
			}
			archive := json.NewDecoder(strings.NewReader(raw))
			archive.UseNumber()
			var saved map[string]any
			if archive.Decode(&saved) != nil || archive.Decode(new(any)) != io.EOF {
				return refusal("invalid native source archive")
			}
			for name, value := range saved {
				if _, duplicate := fields[name]; duplicate {
					return refusal("duplicate native source archive field")
				}
				fields[name] = value
			}
		}
		table := dumpformat.DurableEntityTable
		if line[dumpformat.EphemeralPlaneWireField] == true {
			table = dumpformat.EphemeralEntityTable
		}
		rows = append(rows, Record{ID: text(line["id"]), Source: Provenance{Table: table, Fields: fields}})
	}
	return physicalImportRefusal(rows, true)
}

// PhysicalFieldCount identifies populated source cells the native JSONL
// importer cannot restore. Values and source IDs never appear in refusals.
type PhysicalFieldCount struct {
	Table string `json:"table"`
	Field string `json:"field"`
	Rows  int    `json:"rows"`
}

// PhysicalFieldRefusal prevents a native import from silently replacing
// populated SQL cells with importer defaults. JSONL metadata is not a physical
// column destination; an exact restore writer must establish its own contract.
type PhysicalFieldRefusal struct {
	Fields []PhysicalFieldCount `json:"fields"`
}

func (e *PhysicalFieldRefusal) Error() string {
	parts := make([]string, len(e.Fields))
	for i, field := range e.Fields {
		parts[i] = fmt.Sprintf("%s/%s rows=%d", field.Table, field.Field, field.Rows)
	}
	return "native reverse cannot carry populated source fields: " + strings.Join(parts, ", ")
}

// These are the fields actually emitted to native issueLine destinations.
// An opaque metadata copy and a reserved archive string are not destinations.
var nativeIssueFields = map[string]bool{
	"id": true, "title": true, "description": true, "design": true,
	"acceptance_criteria": true, "notes": true, "spec_id": true,
	"status": true, "priority": true, "issue_type": true, "assignee": true,
	"owner": true, "created_at": true, "created_by": true, "updated_at": true,
	"started_at": true, "closed_at": true, "close_reason": true,
	"closed_by_session": true, "due_at": true, "defer_until": true,
	"external_ref": true, "source_system": true, "metadata": true,
	"pinned": true, "ephemeral": true, "no_history": true,
}

func physicalImportRefusal(entities []Record, all bool) error {
	counts := map[[2]string]int{}
	for _, row := range entities {
		ephemeral, ok := sourceFlag(row.Source.Fields, "ephemeral")
		if ok && ephemeral && !all {
			continue // The explicit export selection retains its omission report.
		}
		for name, value := range row.Source.Fields {
			if physicalDefault(name, value) {
				continue
			}
			if nativeIssueFields[name] && nativeCellEncoding(name, value) {
				continue
			}
			counts[[2]string{row.Source.Table, name}]++
		}
	}
	if len(counts) == 0 {
		return nil
	}
	fields := make([]PhysicalFieldCount, 0, len(counts))
	for key, rows := range counts {
		fields = append(fields, PhysicalFieldCount{Table: key[0], Field: key[1], Rows: rows})
	}
	sort.Slice(fields, func(i, j int) bool {
		if fields[i].Table != fields[j].Table {
			return fields[i].Table < fields[j].Table
		}
		return fields[i].Field < fields[j].Field
	})
	return &PhysicalFieldRefusal{Fields: fields}
}

func nativeCellEncoding(field string, value any) bool {
	if field == "pinned" || field == "ephemeral" || field == "no_history" {
		// A native boolean does not carry a populated SQL TEXT flag's type.
		_, textual := value.(string)
		return !textual
	}
	return true
}

func physicalDefault(field string, value any) bool {
	if value == nil || value == "" {
		return true
	}
	if field == "is_template" || field == "pinned" || field == "ephemeral" || field == "no_history" {
		flag, ok := parsePinned(value)
		return ok && !flag
	}
	if field == "timeout_ns" || field == "compaction_level" || field == "original_size" || field == "estimated_minutes" || field == "current_revision" || field == "participation_generation" {
		n, ok := integer(value)
		return ok && n == 0
	}
	if field == "waiters" {
		switch v := value.(type) {
		case []any:
			return len(v) == 0
		case []string:
			return len(v) == 0
		}
	}
	return false
}

// Detached callers must not introduce already-rounded floating point values.
// Wire decoders use json.Number; reject floats recursively instead of
// fabricating an exact integer from an inexact representation.
func exactNumbers(value any) bool {
	return exactNumberValue(reflect.ValueOf(value))
}

func exactNumberValue(value reflect.Value) bool {
	if !value.IsValid() {
		return true
	}
	switch value.Kind() {
	case reflect.Float32, reflect.Float64:
		return false
	case reflect.Interface, reflect.Pointer:
		return value.IsNil() || exactNumberValue(value.Elem())
	case reflect.Map:
		iter := value.MapRange()
		for iter.Next() {
			if !exactNumberValue(iter.Value()) {
				return false
			}
		}
	case reflect.Array, reflect.Slice:
		for i := 0; i < value.Len(); i++ {
			if !exactNumberValue(value.Index(i)) {
				return false
			}
		}
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			if !exactNumberValue(value.Field(i)) {
				return false
			}
		}
	}
	return true
}
