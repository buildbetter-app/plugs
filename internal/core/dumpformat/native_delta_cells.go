package dumpformat

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

func nativeDeltaBase64(s string) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil || base64.StdEncoding.EncodeToString(b) != s {
		return nil, fmt.Errorf("noncanonical native delta base64")
	}
	return b, nil
}

func nativeDeltaCellValue(cell NativeDeltaCell, sqlType string) (any, error) {
	s := cell.Value
	switch cell.Kind {
	case "sql-null":
		if s != "" {
			return nil, fmt.Errorf("SQL NULL has value")
		}
		return nil, nil
	case "secret-reference":
		return map[string]any{"$secret": s}, nil
	case "string", "bytes", "string-bytes":
		if cell.Kind != "string" {
			b, err := nativeDeltaBase64(s)
			if err != nil {
				return nil, err
			}
			if cell.Kind == "bytes" && (strings.Contains(strings.ToUpper(sqlType), "BLOB") || strings.Contains(strings.ToUpper(sqlType), "BINARY")) {
				return map[string]any{"$bytes": s}, nil
			}
			if !utf8.Valid(b) {
				return nil, fmt.Errorf("native delta refuses lossy text")
			}
			s = string(b)
			if cell.Kind == "string-bytes" {
				return nil, fmt.Errorf("string-bytes requires non-UTF8 text and refuses normalized conversion")
			}
		}
		if !utf8.ValidString(s) {
			return nil, fmt.Errorf("native delta invalid text")
		}
		if strings.EqualFold(sqlType, "JSON") {
			var v any
			if err := decodeJSON([]byte(s), &v); err != nil {
				return nil, err
			}
			return v, nil
		}
		return s, nil
	case "bool":
		if s == "true" {
			return true, nil
		}
		if s == "false" {
			return false, nil
		}
		return nil, fmt.Errorf("noncanonical boolean cell")
	case "time":
		v, err := time.Parse(time.RFC3339Nano, s)
		if err != nil || v.Format(time.RFC3339Nano) != s {
			return nil, fmt.Errorf("noncanonical time cell")
		}
		return v.UTC().Format(time.RFC3339Nano), nil
	case "float32-bits", "float64-bits":
		width := 64
		if cell.Kind == "float32-bits" {
			width = 32
		}
		bits, err := strconv.ParseUint(s, 16, width)
		if err != nil || strconv.FormatUint(bits, 16) != s {
			return nil, fmt.Errorf("noncanonical float bits")
		}
		var v any
		if width == 32 {
			f := math.Float32frombits(uint32(bits))
			if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
				return nil, fmt.Errorf("nonfinite normalized cell")
			}
			v = f
		} else {
			f := math.Float64frombits(bits)
			if math.IsNaN(f) || math.IsInf(f, 0) {
				return nil, fmt.Errorf("nonfinite normalized cell")
			}
			v = f
		}
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		return json.Number(b), nil
	}
	width := 64
	signed := false
	switch cell.Kind {
	case "int":
		signed = true
	case "int8":
		signed = true
		width = 8
	case "int16":
		signed = true
		width = 16
	case "int32":
		signed = true
		width = 32
	case "int64":
		signed = true
	case "uint":
	case "uint8":
		width = 8
	case "uint16":
		width = 16
	case "uint32":
		width = 32
	case "uint64":
	default:
		return nil, fmt.Errorf("unsupported native delta cell kind")
	}
	if signed {
		n, err := strconv.ParseInt(s, 10, width)
		if err != nil || strconv.FormatInt(n, 10) != s {
			return nil, fmt.Errorf("noncanonical signed cell")
		}
	} else {
		n, err := strconv.ParseUint(s, 10, width)
		if err != nil || strconv.FormatUint(n, 10) != s {
			return nil, fmt.Errorf("noncanonical unsigned cell")
		}
	}
	return json.Number(s), nil
}

// Compare decimal JSON numbers without conversion to binary floating point or
// expansion of exponents. Physical integer/float width remains in row hashes.
func nativeDeltaNumberIdentity(s string) (string, string) {
	negative := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	exp := new(big.Int)
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		exp.SetString(s[i+1:], 10)
		s = s[:i]
	}
	if i := strings.IndexByte(s, '.'); i >= 0 {
		exp.Sub(exp, big.NewInt(int64(len(s)-i-1)))
		s = s[:i] + s[i+1:]
	}
	s = strings.TrimLeft(s, "0")
	if s == "" {
		return "0", "0"
	}
	trimmed := strings.TrimRight(s, "0")
	exp.Add(exp, big.NewInt(int64(len(s)-len(trimmed))))
	if negative {
		trimmed = "-" + trimmed
	}
	return trimmed, exp.String()
}

func nativeDeltaValuesEqual(a, b any) bool {
	if x, ok := a.(json.Number); ok {
		y, ok := b.(json.Number)
		if !ok {
			return false
		}
		ac, ae := nativeDeltaNumberIdentity(string(x))
		bc, be := nativeDeltaNumberIdentity(string(y))
		return ac == bc && ae == be
	}
	switch x := a.(type) {
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for k, v := range x {
			w, ok := y[k]
			if !ok || !nativeDeltaValuesEqual(v, w) {
				return false
			}
		}
		return true
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !nativeDeltaValuesEqual(x[i], y[i]) {
				return false
			}
		}
		return true
	case nil:
		return b == nil
	case string:
		y, ok := b.(string)
		return ok && x == y
	case bool:
		y, ok := b.(bool)
		return ok && x == y
	}
	return false
}

func validateNativeDeltaRow(table NativeDeltaTableSchema, row NativeDeltaRow, policy map[string]string) (Row, []byte, error) {
	key, err := nativeDeltaBase64(row.Key)
	if err != nil || len(key) == 0 {
		return nil, nil, fmt.Errorf("invalid native delta key")
	}
	normalized, err := nativeDeltaBase64(row.NormalizedJSON)
	if err != nil || len(normalized) == 0 || normalized[len(normalized)-1] != '\n' {
		return nil, nil, fmt.Errorf("invalid native delta normalized row")
	}
	var fields Row
	if err := decodeJSON(normalized, &fields); err != nil {
		return nil, nil, err
	}
	canonical, err := json.Marshal(fields)
	if err != nil || !bytes.Equal(append(canonical, '\n'), normalized) {
		return nil, nil, fmt.Errorf("noncanonical normalized row")
	}
	if len(fields) != len(table.Columns) || len(row.Cells) != len(table.Columns) {
		return nil, nil, fmt.Errorf("native delta cell/column census differs")
	}
	for i, column := range table.Columns {
		actual, present := fields[column.Name]
		if !present {
			return nil, nil, fmt.Errorf("native delta row column missing")
		}
		cell := row.Cells[i]
		if cell.Kind == "secret-reference" {
			k, _ := fields["key"].(string)
			if (table.Name != "config" && table.Name != "metadata") || column.Name != "value" || k == "" || cell.Value != table.Name+"/"+k || (!secretKey(k) && policy[table.Name+"/"+k] != "secret") {
				return nil, nil, fmt.Errorf("native delta secret reference context differs")
			}
		}
		expected, err := nativeDeltaCellValue(cell, column.SQLType)
		if err != nil {
			return nil, nil, err
		}
		if !nativeDeltaValuesEqual(expected, actual) {
			return nil, nil, fmt.Errorf("native delta typed/normalized cell differs")
		}
		if strings.HasPrefix(cell.Kind, "float") {
			if fmt.Sprint(expected) != fmt.Sprint(actual) {
				return nil, nil, fmt.Errorf("native delta noncanonical normalized float")
			}
		}
	}
	if table.Name == "config" || table.Name == "metadata" || table.Name == "local_metadata" {
		if err := validateNativeDeltaSecretReferences(table.Name, map[string]any{"key": fields["key"], "value": fields["value"]}); err != nil {
			return nil, nil, err
		}
		for k, v := range fields {
			if k != "key" && k != "value" {
				if err := validateNativeDeltaSecretReferences(table.Name, map[string]any{k: v}); err != nil {
					return nil, nil, err
				}
			}
		}
	} else {
		if err := validateNativeDeltaSecretReferences(table.Name, map[string]any(fields)); err != nil {
			return nil, nil, err
		}
	}
	pk, err := NativeDeltaPrimaryKey(table, row.Cells)
	if err != nil {
		return nil, nil, err
	}
	if pk != nil && !bytes.Equal(pk, key) {
		return nil, nil, fmt.Errorf("native delta primary key differs")
	}
	return fields, key, nil
}

// Only a verified v2 SQL row may preserve the producer's declared absence of
// this optional peer credential. Nested objects, config and all other fields
// retain the existing reference rules; no credential value is admitted here.
func validateNativeDeltaSecretReferences(family string, values map[string]any) error {
	// Config/metadata rows carry the credential name in `key`, while physical
	// native tables carry it in the column name. Keep this native-only wrapper
	// separate from validateSecretReferences so portable behavior is unchanged.
	if family == "config" || family == "metadata" || family == "local_metadata" {
		logicalKey, hasKey := values["key"].(string)
		if hasKey {
			if secretKey(logicalKey) {
				if err := validateNativeCredentialValue(family, logicalKey, values["value"]); err != nil {
					return err
				}
			} else if err := validateNativeNestedCredentialValue(family, values["value"]); err != nil {
				return err
			}
			// `key` and `value` are the row envelope, not credential fields. The
			// logical key above is the only field that decides whether the value is
			// stored or an admitted secret reference.
			for key, value := range values {
				if key == "key" || key == "value" {
					continue
				}
				if err := validateNativeNestedCredentialValue(family, value); err != nil {
					return err
				}
			}
			return nil
		}
	}
	for key, value := range values {
		if family == "federation_peers" && key == "password_encrypted" && nativeDeltaEmptyPeerCredential(value) {
			continue
		}
		if err := validateNativeCredentialValue(family, key, value); err != nil {
			return err
		}
	}
	return nil
}

// NativeStoredCredentialError is the counts-only refusal for a real native
// credential value. It deliberately never stores or formats the value.
type NativeStoredCredentialError struct {
	Code   string `json:"code"`
	Family string `json:"family"`
	// Field is a stable public category, never the producer's config key or
	// nested JSON path. Stored names can contain secrets and are counts-only
	// refusal data, so they must not escape through diagnostics.
	Field string `json:"field"`
	Count int    `json:"count"`
}

func (e *NativeStoredCredentialError) Error() string {
	code := e.Code
	if code == "" {
		code = "native_stored_credential"
	}
	return fmt.Sprintf("%s: family=%s field=%s count=%d", code, e.Family, e.Field, e.Count)
}

func nativeStoredCredential(family, field string) error {
	// Only fixed schema fields are public diagnostic labels. Dynamic logical
	// keys and JSON path components never enter the exported error.
	label := "credential"
	if family == "config" || family == "metadata" || family == "local_metadata" {
		label = "value"
	} else if family == "federation_peers" && field == "password_encrypted" {
		label = "password_encrypted"
	}
	return &NativeStoredCredentialError{Code: "native_stored_credential", Family: family, Field: label, Count: 1}
}

func validateNativeCredentialValue(family, field string, value any) error {
	if ref, ok := value.(map[string]any); ok && len(ref) == 1 {
		if secret, present := ref["$secret"]; present {
			name, valid := secret.(string)
			if !valid || name == "" {
				return fmt.Errorf("invalid secret reference")
			}
			return nil
		}
	}
	if secretKey(field) {
		return nativeStoredCredential(family, field)
	}
	return validateNativeNestedCredentialValue(family, value)
}

func validateNativeNestedCredentialValue(family string, value any) error {
	switch value := value.(type) {
	case map[string]any:
		if ref, ok := value["$secret"]; ok {
			name, valid := ref.(string)
			if !valid || name == "" || len(value) != 1 {
				return fmt.Errorf("invalid secret reference")
			}
			return nil
		}
		for key, item := range value {
			// Classify the borrowed local key. Counts-only diagnostics need no
			// source path, so the walk never allocates growing key-path copies.
			if secretKey(key) {
				if err := validateNativeCredentialValue(family, key, item); err != nil {
					return err
				}
				continue
			}
			if err := validateNativeNestedCredentialValue(family, item); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range value {
			if err := validateNativeNestedCredentialValue(family, item); err != nil {
				return err
			}
		}
	}
	return nil
}

func nativeDeltaEmptyPeerCredential(value any) bool {
	switch value := value.(type) {
	case nil:
		return true
	case string:
		return value == ""
	case map[string]any:
		data, ok := value["$bytes"].(string)
		return ok && len(value) == 1 && data == ""
	}
	return false
}
