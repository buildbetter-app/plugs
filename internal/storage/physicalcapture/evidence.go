package physicalcapture

import (
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"
	"unicode/utf8"
)

// EvidenceCell preserves the exact detached driver type and value. In
// particular SQL NULL, JSON-column string "null", empty text and empty binary
// are different observations. Integers never pass through a JSON number.
type EvidenceCell struct {
	Kind  string `json:"kind"`
	Value string `json:"value,omitempty"`
}

type EvidenceTable struct {
	Name    string           `json:"name"`
	Columns []EvidenceColumn `json:"columns"`
	Rows    [][]EvidenceCell `json:"rows"`
}

type EvidenceColumn struct {
	Name    string `json:"name"`
	SQLType string `json:"sql_type"`
}

type EvidenceImage struct {
	CommitID      string          `json:"commit_id"`
	ReadOnlyScope bool            `json:"read_only_scope"`
	Tables        []EvidenceTable `json:"tables"`
}

type EvidenceWorkingRoot struct {
	Format          string          `json:"format"`
	Database        string          `json:"database"`
	HeadCommitID    string          `json:"head_commit_id"`
	WorkingRootHash string          `json:"working_root_hash"`
	ReadOnlyScope   bool            `json:"read_only_scope"`
	Complete        bool            `json:"complete"`
	Tables          []EvidenceTable `json:"tables"`
}

// Evidence is a separate physical observation beside the normalized archive.
// It does not invent SQL defaults/indexes/constraints or portable history.
type Evidence struct {
	Format        string              `json:"format"`
	ArchiveSHA256 string              `json:"archive_sha256"`
	StoreID       string              `json:"store_id"`
	Projects      []string            `json:"projects"`
	SQLDefaults   string              `json:"sql_defaults"`
	Working       EvidenceWorkingRoot `json:"working"`
	Images        []EvidenceImage     `json:"images"`
}

func evidenceCell(value driver.Value) (EvidenceCell, error) {
	cell := EvidenceCell{}
	switch v := value.(type) {
	case nil:
		cell.Kind = "sql-null"
	case string:
		cell.Kind, cell.Value = "string", v
		if !utf8.ValidString(v) {
			cell.Kind, cell.Value = "string-bytes", base64.StdEncoding.EncodeToString([]byte(v))
		}
	case []byte:
		cell.Kind, cell.Value = "bytes", base64.StdEncoding.EncodeToString(v)
	case bool:
		cell.Kind, cell.Value = "bool", strconv.FormatBool(v)
	case int8:
		cell.Kind, cell.Value = "int8", strconv.FormatInt(int64(v), 10)
	case int16:
		cell.Kind, cell.Value = "int16", strconv.FormatInt(int64(v), 10)
	case int32:
		cell.Kind, cell.Value = "int32", strconv.FormatInt(int64(v), 10)
	case int64:
		cell.Kind, cell.Value = "int64", strconv.FormatInt(v, 10)
	case int:
		cell.Kind, cell.Value = "int", strconv.FormatInt(int64(v), 10)
	case uint8:
		cell.Kind, cell.Value = "uint8", strconv.FormatUint(uint64(v), 10)
	case uint16:
		cell.Kind, cell.Value = "uint16", strconv.FormatUint(uint64(v), 10)
	case uint32:
		cell.Kind, cell.Value = "uint32", strconv.FormatUint(uint64(v), 10)
	case uint64:
		cell.Kind, cell.Value = "uint64", strconv.FormatUint(v, 10)
	case uint:
		cell.Kind, cell.Value = "uint", strconv.FormatUint(uint64(v), 10)
	case float32:
		cell.Kind, cell.Value = "float32-bits", strconv.FormatUint(uint64(math.Float32bits(v)), 16)
	case float64:
		cell.Kind, cell.Value = "float64-bits", strconv.FormatUint(math.Float64bits(v), 16)
	case time.Time:
		cell.Kind, cell.Value = "time", v.Format(time.RFC3339Nano)
	default:
		return EvidenceCell{}, fmt.Errorf("unsupported physical evidence cell type")
	}
	return cell, nil
}

func EvidenceTables(tables []Table) ([]EvidenceTable, error) {
	result := make([]EvidenceTable, 0, len(tables))
	for _, table := range tables {
		out := EvidenceTable{Name: table.Name, Columns: make([]EvidenceColumn, 0, len(table.Columns)), Rows: make([][]EvidenceCell, 0, len(table.Rows))}
		for _, column := range table.Columns {
			out.Columns = append(out.Columns, EvidenceColumn{Name: column.Name, SQLType: column.DatabaseTypeName})
		}
		for _, row := range table.Rows {
			if len(row) != len(table.Columns) {
				return nil, fmt.Errorf("physical evidence row width differs")
			}
			cells := make([]EvidenceCell, len(row))
			for i, value := range row {
				cell, err := evidenceCell(value)
				if err != nil {
					return nil, err
				}
				cells[i] = cell
			}
			out.Rows = append(out.Rows, cells)
		}
		result = append(result, out)
	}
	return result, nil
}

func MarshalEvidence(evidence Evidence) ([]byte, error) {
	return json.Marshal(evidence)
}
