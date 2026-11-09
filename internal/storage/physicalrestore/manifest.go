// Package physicalrestore is the restore-only storage facade. It does not use
// types.Issue, ordinary import/update semantics, or untyped NativeTable values.
package physicalrestore

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	embedded "github.com/dolthub/driver/v2"
	"github.com/steveyegge/beads/internal/storage/physicalcapture"
)

const Format = "beads-physical-restore/1"

// Manifest binds every physical column, its exact SQL type and every cell.
// Only explicit snapshot mode is implemented; strict history is refused rather
// than silently lowered. There is no logical-to-physical inference or bypass.
type Manifest struct {
	Format      string  `json:"format"`
	HistoryMode string  `json:"history_mode"`
	Tables      []Table `json:"tables"`
}
type Column struct {
	Name    string `json:"name"`
	SQLType string `json:"sql_type"`
}
type Table struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
	Rows    [][]Cell `json:"rows"`
}

// Data is an exact textual encoding under Type, not a dynamically decoded JSON
// number. "null" has no data. "string" and "bytes" are distinct; numeric widths
// are explicit. JSON columns use string cells containing exact JSON source text
// plus the bound SQL type. This never relocates or stringifies metadata fields.
type Cell struct {
	Type string `json:"type"`
	Data string `json:"data,omitempty"`
}

// Error exposes no driver internals or cell values to command/tool callers.
type Error struct {
	Kind, Table, Column string
	Row                 int
	Cause               error
}

func (e *Error) Error() string {
	return fmt.Sprintf("physical restore %s table=%s column=%s row=%d: %v", e.Kind, e.Table, e.Column, e.Row, e.Cause)
}
func (e *Error) Unwrap() error { return e.Cause }

// Read refuses unknown fields, duplicate JSON keys, non-UTF8 input and trailing
// data before any destination is opened. Integer data never visits float64.
func Read(reader io.Reader) (Manifest, error) {
	data, err := io.ReadAll(io.LimitReader(reader, (64<<20)+1))
	if err != nil {
		return Manifest{}, err
	}
	if len(data) > 64<<20 || !utf8.Valid(data) {
		return Manifest{}, errors.New("invalid or oversized physical manifest")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := checkValue(decoder); err != nil {
		return Manifest{}, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return Manifest{}, errors.New("trailing manifest data")
	}
	decoder = json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, err
	}
	if _, err := decode(manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}
func checkValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if delim, ok := token.(json.Delim); ok {
		switch delim {
		case '{':
			seen := map[string]bool{}
			for decoder.More() {
				key, err := decoder.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				if !ok || seen[name] {
					return errors.New("duplicate manifest key")
				}
				seen[name] = true
				if err := checkValue(decoder); err != nil {
					return err
				}
			}
		case '[':
			for decoder.More() {
				if err := checkValue(decoder); err != nil {
					return err
				}
			}
		default:
			return errors.New("invalid manifest delimiter")
		}
		_, err = decoder.Token()
	}
	return err
}

// Restore owns only the driver's restore operation on an idle dedicated
// connection. The caller retains connection/DB lifetime. Source buffers do not
// escape into the transaction; byte cells are independently owned.
func Restore(ctx context.Context, conn *sql.Conn, manifest Manifest) error {
	tables, err := decode(manifest)
	if err != nil {
		return err
	}
	if conn == nil {
		return errors.New("physical restore connection required")
	}
	err = embedded.RestorePhysicalSnapshot(ctx, conn, tables)
	var failure *embedded.PhysicalRestoreError
	if errors.As(err, &failure) {
		return &Error{failure.Kind, failure.Table, failure.Column, failure.Row, failure.Cause}
	}
	return err
}
func decode(manifest Manifest) ([]embedded.PhysicalTable, error) {
	if manifest.Format != Format {
		return nil, &Error{Kind: "format", Row: -1, Cause: errors.New("unsupported physical restore format")}
	}
	if manifest.HistoryMode != "snapshot" {
		return nil, &Error{Kind: "history-unavailable", Row: -1, Cause: errors.New("strict native history replay is not implemented; explicit snapshot required")}
	}
	if len(manifest.Tables) == 0 {
		return nil, &Error{Kind: "manifest", Row: -1, Cause: errors.New("at least one physical table required")}
	}
	tables := make([]embedded.PhysicalTable, len(manifest.Tables))
	for ti, table := range manifest.Tables {
		out := embedded.PhysicalTable{Name: table.Name, Columns: make([]embedded.PhysicalColumn, len(table.Columns)), Rows: make([][]driver.Value, len(table.Rows))}
		for ci, column := range table.Columns {
			out.Columns[ci] = embedded.PhysicalColumn{Name: column.Name, DatabaseTypeName: column.SQLType}
		}
		for ri, row := range table.Rows {
			if len(row) != len(table.Columns) {
				return nil, &Error{Kind: "row-width", Table: table.Name, Row: ri, Cause: errors.New("every physical column must be present")}
			}
			out.Rows[ri] = make([]driver.Value, len(row))
			for ci, cell := range row {
				value, err := decodeCell(cell)
				if err != nil {
					return nil, &Error{Kind: "cell-type", Table: table.Name, Column: table.Columns[ci].Name, Row: ri, Cause: err}
				}
				if strings.EqualFold(table.Columns[ci].SQLType, "json") {
					// Existing capture's Value() conflates SQL NULL and top-level JSON null.
					// Never pretend that the untyped nil determines the original distinction.
					if value == nil {
						return nil, &Error{Kind: "ambiguous-json-null", Table: table.Name, Column: table.Columns[ci].Name, Row: ri, Cause: errors.New("capture cannot distinguish SQL NULL from JSON null")}
					}
					text, ok := value.(string)
					if !ok {
						return nil, &Error{Kind: "cell-type", Table: table.Name, Column: table.Columns[ci].Name, Row: ri, Cause: errors.New("JSON column requires exact detached JSON text")}
					}
					decoder := json.NewDecoder(strings.NewReader(text))
					decoder.UseNumber()
					if err := checkValue(decoder); err != nil {
						return nil, &Error{Kind: "json", Table: table.Name, Column: table.Columns[ci].Name, Row: ri, Cause: errors.New("invalid or duplicate JSON cell")}
					}
					if _, err := decoder.Token(); err != io.EOF {
						return nil, &Error{Kind: "json", Table: table.Name, Column: table.Columns[ci].Name, Row: ri, Cause: errors.New("trailing JSON cell")}
					}
				}
				out.Rows[ri][ci] = value
			}
		}
		tables[ti] = out
	}
	return tables, nil
}
func decodeCell(cell Cell) (driver.Value, error) {
	bad := errors.New("unsupported or noncanonical physical cell encoding")
	switch cell.Type {
	case "null":
		if cell.Data != "" {
			return nil, bad
		}
		return nil, nil
	case "string":
		if !utf8.ValidString(cell.Data) {
			return nil, bad
		}
		return cell.Data, nil
	case "bytes":
		value, err := base64.StdEncoding.Strict().DecodeString(cell.Data)
		if err != nil {
			return nil, bad
		}
		return append([]byte{}, value...), nil
	case "bool":
		if cell.Data == "true" {
			return true, nil
		}
		if cell.Data == "false" {
			return false, nil
		}
		return nil, bad
	case "time":
		value, err := time.Parse(time.RFC3339Nano, cell.Data)
		if err != nil {
			return nil, bad
		}
		return value, nil
	case "int", "int8", "int16", "int32", "int64":
		bits := 64
		switch cell.Type {
		case "int":
			bits = strconv.IntSize
		case "int8":
			bits = 8
		case "int16":
			bits = 16
		case "int32":
			bits = 32
		}
		n, err := strconv.ParseInt(cell.Data, 10, bits)
		if err != nil || strconv.FormatInt(n, 10) != cell.Data {
			return nil, bad
		}
		switch cell.Type {
		case "int":
			return int(n), nil
		case "int8":
			return int8(n), nil
		case "int16":
			return int16(n), nil
		case "int32":
			return int32(n), nil
		}
		return n, nil
	case "uint", "uint8", "uint16", "uint32", "uint64":
		bits := 64
		switch cell.Type {
		case "uint":
			bits = strconv.IntSize
		case "uint8":
			bits = 8
		case "uint16":
			bits = 16
		case "uint32":
			bits = 32
		}
		n, err := strconv.ParseUint(cell.Data, 10, bits)
		if err != nil || strconv.FormatUint(n, 10) != cell.Data {
			return nil, bad
		}
		switch cell.Type {
		case "uint":
			return uint(n), nil
		case "uint8":
			return uint8(n), nil
		case "uint16":
			return uint16(n), nil
		case "uint32":
			return uint32(n), nil
		}
		return n, nil
	case "float32", "float64":
		bits := 64
		if cell.Type == "float32" {
			bits = 32
		}
		n, err := strconv.ParseFloat(cell.Data, bits)
		if err != nil || math.IsNaN(n) || math.IsInf(n, 0) {
			return nil, bad
		}
		if bits == 32 {
			return float32(n), nil
		}
		return n, nil
	default:
		return nil, bad
	}
}

// FromImage requires the ORIGINAL typed capture port. NativeTable alone cannot
// supply SQL types or recover binary/timestamp/SQL NULL distinctions. Values
// that the capture port cannot distinguish are refused here, never inferred.
func FromImage(image physicalcapture.Image) (Manifest, error) {
	manifest := Manifest{Format: Format, HistoryMode: "snapshot", Tables: make([]Table, len(image.Tables))}
	for ti, table := range image.Tables {
		out := Table{Name: table.Name, Columns: make([]Column, len(table.Columns)), Rows: make([][]Cell, len(table.Rows))}
		for ci, column := range table.Columns {
			out.Columns[ci] = Column{Name: column.Name, SQLType: column.DatabaseTypeName}
		}
		for ri, row := range table.Rows {
			if len(row) != len(table.Columns) {
				return Manifest{}, errors.New("capture row width differs")
			}
			out.Rows[ri] = make([]Cell, len(row))
			for ci, value := range row {
				cell, err := encodeCell(value)
				if err != nil {
					return Manifest{}, &Error{Kind: "capture-cell", Table: table.Name, Column: table.Columns[ci].Name, Row: ri, Cause: err}
				}
				out.Rows[ri][ci] = cell
			}
		}
		manifest.Tables[ti] = out
	}
	if _, err := decode(manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}
func encodeCell(value driver.Value) (Cell, error) {
	switch v := value.(type) {
	case nil:
		return Cell{Type: "null"}, nil
	case string:
		if !utf8.ValidString(v) {
			return Cell{}, errors.New("non-UTF8 physical text")
		}
		return Cell{Type: "string", Data: v}, nil
	case []byte:
		return Cell{Type: "bytes", Data: base64.StdEncoding.EncodeToString(v)}, nil
	case bool:
		return Cell{Type: "bool", Data: strconv.FormatBool(v)}, nil
	case time.Time:
		return Cell{Type: "time", Data: v.UTC().Format(time.RFC3339Nano)}, nil
	case int:
		return Cell{Type: "int", Data: strconv.FormatInt(int64(v), 10)}, nil
	case int8:
		return Cell{Type: "int8", Data: strconv.FormatInt(int64(v), 10)}, nil
	case int16:
		return Cell{Type: "int16", Data: strconv.FormatInt(int64(v), 10)}, nil
	case int32:
		return Cell{Type: "int32", Data: strconv.FormatInt(int64(v), 10)}, nil
	case int64:
		return Cell{Type: "int64", Data: strconv.FormatInt(v, 10)}, nil
	case uint:
		return Cell{Type: "uint", Data: strconv.FormatUint(uint64(v), 10)}, nil
	case uint8:
		return Cell{Type: "uint8", Data: strconv.FormatUint(uint64(v), 10)}, nil
	case uint16:
		return Cell{Type: "uint16", Data: strconv.FormatUint(uint64(v), 10)}, nil
	case uint32:
		return Cell{Type: "uint32", Data: strconv.FormatUint(uint64(v), 10)}, nil
	case uint64:
		return Cell{Type: "uint64", Data: strconv.FormatUint(v, 10)}, nil
	case float32:
		return Cell{Type: "float32", Data: strconv.FormatFloat(float64(v), 'g', -1, 32)}, nil
	case float64:
		return Cell{Type: "float64", Data: strconv.FormatFloat(v, 'g', -1, 64)}, nil
	default:
		return Cell{}, errors.New("unsupported capture cell type")
	}
}
