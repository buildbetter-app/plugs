// Package reverseconverter converts a validated Plug head-state dump into the
// Beads JSONL interchange accepted by the pinned bd importer.
//
// The package owns only the rollback direction. Read uses the canonical,
// read-only dumpformat validator; it has no restore or storage dependency.
// Callers may also pass detached, already validated records to Convert.
package reverseconverter

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

const (
	PlugDumpFormat = "plug-dump/1"
	BeadsJSONL     = "beads-jsonl/1"
	BeadsReference = "9d6fff760b7a314b476539198504d77ce0959656"
	MaxArchiveSize = 128 << 20
)

// Project and Kind mirror the versioned plug-dump/1 manifest fields needed by
// the reverse path. They intentionally contain no restore or storage methods.
type Project struct {
	Prefix        string            `json:"prefix"`
	IDMode        string            `json:"id_mode"`
	HashLength    int               `json:"hash_length"`
	LastID        uint64            `json:"last_id"`
	ChildCounters map[string]uint64 `json:"child_counters"`
}

type Kind struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
	File    string `json:"file"`
	Count   int    `json:"count"`
}

type File struct {
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

type Manifest struct {
	Format                     string            `json:"format"`
	Schema                     int               `json:"schema"`
	Reference                  string            `json:"beads_reference"`
	StoreID                    string            `json:"store_id"`
	Machine                    string            `json:"machine"`
	MainVersion                int               `json:"main_version"`
	IgnoredVersion             int               `json:"ignored_version"`
	Scope                      string            `json:"scope"`
	History                    string            `json:"history"`
	Projects                   []Project         `json:"projects"`
	Kinds                      []Kind            `json:"kind_registry"`
	Files                      map[string]File   `json:"files"`
	Sources                    []json.RawMessage `json:"sources"`
	Warnings                   []string          `json:"warnings"`
	Verification               map[string]string `json:"verification"`
	SourceFieldsContract       string            `json:"source_fields_contract,omitempty"`
	SourcePlaneAddressContract string            `json:"source_plane_address_contract,omitempty"`
}

type Provenance struct {
	Table  string         `json:"table"`
	Fields map[string]any `json:"fields"`
}

type Record struct {
	Kind    string         `json:"kind"`
	ID      string         `json:"id"`
	Project string         `json:"project"`
	Body    map[string]any `json:"body"`
	Source  Provenance     `json:"source"`
}

// ConfigMemoryPrefix namespaces configuration references in the Beads memory
// plane. Beads memories are string-valued, so a configuration record whose
// value is a structured secret reference is encoded as canonical JSON under
// this reserved prefix. The original Plug key remains in the value and in
// Report.ConfigReferences; no secret value is resolved.
const ConfigMemoryPrefix = "plug.config/"

// Dump is the detached, validated input to Convert. Records are grouped by
// kind, but their order is not trusted; Convert sorts every emitted relation.
type Dump struct {
	Manifest Manifest
	Records  map[string][]Record
	Members  map[string][]byte
}

// Projection is the semantic subset expected to survive a bd import/readback.
// It is also useful to independent callers without parsing the JSONL bytes.
type Projection struct {
	Issues   []IssueProjection   `json:"issues"`
	Edges    []EdgeProjection    `json:"edges"`
	Comments []CommentProjection `json:"comments"`
	Memories []MemoryProjection  `json:"memories"`
}

type IssueProjection struct {
	ID       string `json:"id"`
	Kind     string `json:"kind"`
	Status   string `json:"status"`
	Category string `json:"category"`
	Outcome  string `json:"outcome,omitempty"`
}

type EdgeProjection struct {
	TargetKind string `json:"target_kind,omitempty"`
	ID         string `json:"id"`
	From       string `json:"from"`
	To         string `json:"to"`
	Type       string `json:"type"`
}

type CommentProjection struct {
	ID      string `json:"id"`
	IssueID string `json:"issue_id"`
}

type MemoryProjection struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// TypeMapping records an explicit ontology map_type directive that authorized
// a source type outside Plug's built-in reverse-converter vocabulary. The
// source type is intentionally retained in the emitted Beads JSONL; To names
// the target Plug ontology type for the caller's separate migration plan.
type TypeMapping struct {
	ID      string `json:"id"`
	Project string `json:"project"`
	From    string `json:"from"`
	To      string `json:"to"`
}

// Report makes every intentional loss visible. A nonempty refusal is returned
// as *Refusal and includes this same report, so callers never receive a silent
// partial import.
type Report struct {
	CoreKindLosses        []StateLoss     `json:"core_kind_losses,omitempty"`
	NamespaceLosses       []NamespaceLoss `json:"namespace_losses,omitempty"`
	OmittedEphemeralRows  []string        `json:"omitted_ephemeral_rows,omitempty"`
	InputDigest           string          `json:"input_digest"`
	OutputDigest          string          `json:"output_digest,omitempty"`
	LostPostSwitchHistory []string        `json:"lost_post_switch_history,omitempty"`
	PlugOnlyKinds         []string        `json:"plug_only_kinds,omitempty"`
	TypeMappings          []TypeMapping   `json:"type_mappings,omitempty"`
	Blobs                 []string        `json:"blobs,omitempty"`
	Principals            []string        `json:"principals,omitempty"`
	Warnings              []string        `json:"warnings,omitempty"`
	UnsupportedFields     []string        `json:"unsupported_fields,omitempty"`
	ArchivedSourceFields  []string        `json:"archived_source_fields,omitempty"`
	ConfigReferences      []string        `json:"config_references,omitempty"`
	ArchiveRows           int             `json:"archive_rows,omitempty"`
	ConfigAsMemories      int             `json:"config_as_memories,omitempty"`
}

// Result is a complete conversion. JSONL always ends in a newline when there
// is at least a header, and Digest is SHA-256 over those exact bytes.
type Result struct {
	JSONL      []byte
	Digest     string
	Projection Projection
	Report     Report
}

// Refusal is a typed fail-closed conversion error. Reasons are stable enough
// for a caller to display, while Report carries the structured evidence.
type Refusal struct {
	Reasons []string
	Report  Report
}

func (e *Refusal) Error() string {
	if len(e.Reasons) == 0 {
		return "reverse conversion refused"
	}
	return "reverse conversion refused: " + e.Reasons[0]
}

func refusal(format string, args ...any) *Refusal {
	return &Refusal{Reasons: []string{fmt.Sprintf(format, args...)}}
}

func addUnique(values *[]string, value string) {
	if value == "" {
		return
	}
	for _, existing := range *values {
		if existing == value {
			return
		}
	}
	*values = append(*values, value)
}

func normalizeReport(report *Report) {
	sort.Strings(report.OmittedEphemeralRows)
	sort.Strings(report.LostPostSwitchHistory)
	sort.Strings(report.PlugOnlyKinds)
	sort.Slice(report.TypeMappings, func(i, j int) bool {
		a, b := report.TypeMappings[i], report.TypeMappings[j]
		if a.ID != b.ID {
			return a.ID < b.ID
		}
		if a.Project != b.Project {
			return a.Project < b.Project
		}
		if a.From != b.From {
			return a.From < b.From
		}
		return a.To < b.To
	})
	sort.Strings(report.Blobs)
	sort.Strings(report.Principals)
	sort.Strings(report.Warnings)
	sort.Strings(report.UnsupportedFields)
	sort.Strings(report.ArchivedSourceFields)
	sort.Strings(report.ConfigReferences)
}

func digestBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func cloneMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = cloneValue(value)
	}
	return out
}

func cloneValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		return cloneMap(value)
	case []any:
		out := make([]any, len(value))
		for i, item := range value {
			out[i] = cloneValue(item)
		}
		return out
	default:
		return value
	}
}

func canonicalJSON(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal value: %w", err)
	}
	return string(data), nil
}
