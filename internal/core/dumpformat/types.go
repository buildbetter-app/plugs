// Package dumpformat reads the versioned plug-dump/1 archive emitted by the
// Beads exporter. It is deliberately read-only: parsing an archive never
// opens a store or creates a revision.
package dumpformat

const (
	Format          = "plug-dump/1"
	BeadsReference  = "9d6fff760b7a314b476539198504d77ce0959656"
	Scope           = "offline-head-and-source-archives"
	MaxArchiveBytes = 128 << 20
)

// Row retains source JSON values, including explicit nulls and lossless
// numbers. The exporter uses this shape for both a typed record body and its
// source provenance.
type Row map[string]any

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

type Disposition struct {
	Family string `json:"family"`
	Rows   int    `json:"rows"`
	Action string `json:"action"`
	Reason string `json:"reason"`
}

type Manifest struct {
	Format         string            `json:"format"`
	Schema         int               `json:"schema"`
	Reference      string            `json:"beads_reference"`
	StoreID        string            `json:"store_id"`
	Machine        string            `json:"machine"`
	MainVersion    int               `json:"main_version"`
	IgnoredVersion int               `json:"ignored_version"`
	Scope          string            `json:"scope"`
	History        string            `json:"history"`
	Projects       []Project         `json:"projects"`
	Kinds          []Kind            `json:"kind_registry"`
	Files          map[string]File   `json:"files"`
	Sources        []Disposition     `json:"sources"`
	Warnings       []string          `json:"warnings"`
	Verification   map[string]string `json:"verification"`
}

type Provenance struct {
	Table  string `json:"table"`
	Fields Row    `json:"fields"`
}

type Record struct {
	Kind    string     `json:"kind"`
	ID      string     `json:"id"`
	Project string     `json:"project"`
	Body    Row        `json:"body"`
	Source  Provenance `json:"source"`
}

// Archive owns validated data. Accessors return detached copies; a caller
// cannot turn a validated record into an unchecked restore request.
type Archive struct {
	manifest Manifest
	members  map[string][]byte
	byKind   map[string][]Record
}

func (a *Archive) Manifest() Manifest {
	m := a.manifest
	m.Projects = append([]Project(nil), m.Projects...)
	for i := range m.Projects {
		c := make(map[string]uint64, len(m.Projects[i].ChildCounters))
		for key, value := range m.Projects[i].ChildCounters {
			c[key] = value
		}
		m.Projects[i].ChildCounters = c
	}
	m.Kinds = append([]Kind(nil), m.Kinds...)
	m.Sources = append([]Disposition(nil), m.Sources...)
	m.Warnings = append([]string(nil), m.Warnings...)
	m.Files = make(map[string]File, len(a.manifest.Files))
	for key, value := range a.manifest.Files {
		m.Files[key] = value
	}
	m.Verification = make(map[string]string, len(a.manifest.Verification))
	for key, value := range a.manifest.Verification {
		m.Verification[key] = value
	}
	return m
}

func (a *Archive) Member(path string) ([]byte, bool) {
	data, ok := a.members[path]
	return append([]byte(nil), data...), ok
}

func (a *Archive) Records(kind string) []Record {
	rows := append([]Record(nil), a.byKind[kind]...)
	for i := range rows {
		rows[i].Body = cloneRow(rows[i].Body)
		rows[i].Source.Fields = cloneRow(rows[i].Source.Fields)
	}
	return rows
}

func cloneRow(row Row) Row {
	result := make(Row, len(row))
	for key, value := range row {
		result[key] = cloneValue(value)
	}
	return result
}

func cloneValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(value))
		for key, item := range value {
			result[key] = cloneValue(item)
		}
		return result
	case []any:
		result := make([]any, len(value))
		for i, item := range value {
			result[i] = cloneValue(item)
		}
		return result
	default:
		return value // JSON scalars, including json.Number, are immutable.
	}
}
