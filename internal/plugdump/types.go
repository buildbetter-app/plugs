// Package plugdump maps a frozen Beads read set into a deterministic plug-dump/1
// archive. It does not restore data or claim complete historical reconstruction.
package plugdump

import (
	"context"
	"encoding/json"
)

const Format = "plug-dump/1"
const BeadsReference = "9d6fff760b7a314b476539198504d77ce0959656"

const (
	// EdgeVersionTypedDependencies is the explicit successor producer
	// contract. Format/schema/reference alone never enable local waits-for,
	// conditional-blocks or tracks semantics.
	EdgeVersionTypedDependencies = 2
	// EdgeVersionLocalOpaque is the strict successor of the typed dependency
	// contract. It is selected only when a stored local dependency has a
	// non-well-known kind that the v2 archive cannot represent.
	EdgeVersionLocalOpaque = 3
	// IssueSourceFieldsContract binds the producer's complete physical issue
	// row image. Recognition is separate from native SQL import capability.
	IssueSourceFieldsContract = "beads-issues-source-fields/1"
	// SourcePlaneAddressContract enables physical dependency identities scoped
	// by project and source table. Omission retains the legacy address rules.
	SourcePlaneAddressContract = "beads-source-planes/1"
	// StatusCategoryContract preserves native names and Beads lifecycle categories.
	StatusCategoryContract = "beads-status-categories/1"
)

// Row retains every source column, including SQL NULL and lossless numbers.
type Row map[string]any

// Table is an unfiltered read of one source family. Columns are required even
// for an empty table, so an empty result cannot conceal schema drift.
type Table struct {
	Columns []string
	Rows    []Row
}

// Source is implemented over the existing storage.RawDBAccessor read seam.
// Implementations must return the complete frozen read set or an error.
type Source interface {
	Tables(context.Context) ([]string, error)
	ReadTable(context.Context, string) (Table, error)
}

// Inventory supplies the offline capture's identity and external-file census.
// These declarations are operator evidence, never an independent freeze proof.
type Inventory struct {
	NativeHistoryContract      string              `json:"native_history_contract,omitempty"`
	SourceFamilyContract       string              `json:"source_family_contract,omitempty"`
	StatusCategoryContract     string              `json:"status_category_contract,omitempty"`
	SourcePlaneAddressContract string              `json:"source_plane_address_contract,omitempty"`
	Version                    string              `json:"version"`
	StoreID                    string              `json:"store_id"`
	Machine                    string              `json:"machine"`
	Projects                   []Project           `json:"projects"`
	Ontology                   []OntologyDirective `json:"ontology"`
	Quiesced                   bool                `json:"quiesced"`
	RemotesMerged              bool                `json:"remotes_merged"`
	WorkingSetClean            bool                `json:"working_set_clean"`
	History                    string              `json:"history"` // unavailable or available-unverified
	External                   []ExternalInput     `json:"external"`
	ConfigPolicy               map[string]string   `json:"config_policy"` // public or secret, every DB config/metadata key
}

type ProjectStatus struct {
	Name     string `json:"name"`
	Category string `json:"category"`
}

type Project struct {
	Statuses       []ProjectStatus   `json:"statuses,omitempty"`
	Prefix         string            `json:"prefix"`
	IDMode         string            `json:"id_mode"`
	HashLength     int               `json:"hash_length"`
	HashNamespaces []string          `json:"hash_namespaces,omitempty"`
	LastID         uint64            `json:"last_id"`
	ChildCounters  map[string]uint64 `json:"child_counters"`
}

// OntologyDirective is the closed-world map_type declaration used to admit a
// Beads issue type that is not one of the built-in exporter types. The target
// type is the original Beads spelling; To is the Plug kind emitted by this
// archive. A target ID may narrow a declaration to one exact source row.
// This mirrors the Plug ontology migration directive shape without importing
// Plug's implementation into Beads.
type OntologyDirective struct {
	ID     string         `json:"id"`
	Kind   string         `json:"kind"`
	Target OntologyTarget `json:"target"`
	From   string         `json:"from"`
	To     string         `json:"to"`
}

type OntologyTarget struct {
	Project string `json:"project"`
	Type    string `json:"type"`
	ID      string `json:"id,omitempty"`
}

// The initial reader accepts absent external sources, or an explicitly
// requested JSON object of public values and {$secret:name} references.
// Other formats refuse; they are not silently omitted.
type ExternalInput struct {
	Family     string `json:"family"`
	Path       string `json:"path"`
	State      string `json:"state"` // absent or json-config
	Precedence int    `json:"precedence"`
}

type Record struct {
	Kind    string         `json:"kind"`
	ID      string         `json:"id"`
	Project string         `json:"project"`
	Body    map[string]any `json:"body"`
	Source  Provenance     `json:"source"`
}

type Provenance struct {
	Table  string `json:"table"`
	Fields Row    `json:"fields"`
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

type Disposition struct {
	Family  string `json:"family"`
	Rows    int    `json:"rows"`
	Action  string `json:"action"`
	Reason  string `json:"reason"`
	History string `json:"history,omitempty"`
}

type Manifest struct {
	NativeHistoryContract      string              `json:"native_history_contract,omitempty"`
	SourceFamilyContract       string              `json:"source_family_contract,omitempty"`
	StatusCategoryContract     string              `json:"status_category_contract,omitempty"`
	SourcePlaneAddressContract string              `json:"source_plane_address_contract,omitempty"`
	Format                     string              `json:"format"`
	Schema                     int                 `json:"schema"`
	Reference                  string              `json:"beads_reference"`
	StoreID                    string              `json:"store_id"`
	Machine                    string              `json:"machine"`
	MainVersion                int                 `json:"main_version"`
	IgnoredVersion             int                 `json:"ignored_version"`
	Scope                      string              `json:"scope"`
	History                    string              `json:"history"`
	Projects                   []Project           `json:"projects"`
	Ontology                   []OntologyDirective `json:"ontology,omitempty"`
	Kinds                      []Kind              `json:"kind_registry"`
	Files                      map[string]File     `json:"files"`
	Sources                    []Disposition       `json:"sources"`
	Warnings                   []string            `json:"warnings"`
	Verification               map[string]string   `json:"verification"`
	SourceFieldsContract       string              `json:"source_fields_contract,omitempty"`
}

func marshal(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}
