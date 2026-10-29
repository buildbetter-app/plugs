package plugdump

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/steveyegge/beads/internal/config"
)

// Queryer is satisfied by sql.Tx. The command begins a read-only transaction
// through storage.RawDBAccessor and always rolls it back after gathering data.
type Queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type SQLSource struct{ DB Queryer }

func (s SQLSource) Tables(ctx context.Context) ([]string, error) {
	rows, err := s.DB.QueryContext(ctx, "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		// Dolt's virtual system tables are inventoried as external capture
		// evidence. User tables, including migration residue, are never hidden.
		names = append(names, name)
	}
	return names, rows.Err()
}

func (s SQLSource) ReadTable(ctx context.Context, name string) (Table, error) {
	if _, ok := tableColumns[name]; !ok {
		return Table{}, fmt.Errorf("unsupported table")
	}
	// name comes exclusively from the closed census registry above.
	rows, err := s.DB.QueryContext(ctx, "SELECT * FROM `"+name+"`") //nolint:gosec // closed allowlist, no caller SQL
	if err != nil {
		return Table{}, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return Table{}, err
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return Table{}, err
	}
	table := Table{Columns: columns}
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return Table{}, err
		}
		row := Row{}
		for i, value := range values {
			converted, err := sqlValue(value, columnTypes[i].DatabaseTypeName())
			if err != nil {
				return Table{}, fmt.Errorf("column %s: %w", columns[i], err)
			}
			row[columns[i]] = converted
		}
		table.Rows = append(table.Rows, row)
	}
	return table, rows.Err()
}

func sqlValue(value any, dbType string) (any, error) {
	if value == nil {
		return nil, nil
	}
	if stamp, ok := value.(time.Time); ok {
		return stamp.UTC().Format(time.RFC3339Nano), nil
	}
	if data, ok := value.([]byte); ok {
		if strings.Contains(strings.ToUpper(dbType), "BLOB") || strings.Contains(strings.ToUpper(dbType), "BINARY") {
			return map[string]string{"$bytes": base64.StdEncoding.EncodeToString(data)}, nil
		}
		if !utf8.Valid(data) {
			return nil, fmt.Errorf("non-UTF-8 text refuses lossy conversion")
		}
		value = string(data)
	}
	if text, ok := value.(string); ok {
		if !utf8.ValidString(text) {
			return nil, fmt.Errorf("non-UTF-8 text refuses lossy conversion")
		}
		if !strings.EqualFold(dbType, "JSON") {
			return text, nil
		}
		var decoded any
		if err := decodeJSON([]byte(text), &decoded); err != nil {
			return nil, fmt.Errorf("invalid JSON cell")
		}
		return decoded, nil
	}
	return value, nil
}

// ReadInventory refuses duplicate keys, trailing data and unknown members.
func ReadInventory(reader io.Reader) (Inventory, error) {
	data, err := io.ReadAll(io.LimitReader(reader, 4<<20+1))
	if err != nil {
		return Inventory{}, err
	}
	if len(data) > 4<<20 {
		return Inventory{}, fmt.Errorf("inventory exceeds 4 MiB")
	}
	var inventory Inventory
	err = decodeJSON(data, &inventory)
	return inventory, err
}

func decodeJSON(data []byte, target any) error {
	if !utf8.Valid(data) {
		return fmt.Errorf("JSON must be UTF-8")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if err := checkJSONValue(d); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return fmt.Errorf("trailing JSON data")
	}
	d = json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	d.DisallowUnknownFields()
	return d.Decode(target)
}

func checkJSONValue(d *json.Decoder) error {
	token, err := d.Token()
	if err != nil {
		return fmt.Errorf("invalid JSON")
	}
	if delim, ok := token.(json.Delim); ok {
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				keyToken, err := d.Token()
				if err != nil {
					return fmt.Errorf("invalid JSON object")
				}
				key, ok := keyToken.(string)
				if !ok || seen[key] {
					return fmt.Errorf("duplicate or invalid JSON key")
				}
				seen[key] = true
				if err := checkJSONValue(d); err != nil {
					return err
				}
			}
		case '[':
			for d.More() {
				if err := checkJSONValue(d); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("invalid JSON delimiter")
		}
		_, err = d.Token()
	}
	return err
}

func readExternal(inputs []ExternalInput) ([]byte, []Disposition, error) {
	seenFamily, seenPath := map[string]bool{}, map[string]bool{}
	allowed := map[string]bool{}
	for _, family := range externalFamilies {
		allowed[family] = true
	}
	inputs = append([]ExternalInput(nil), inputs...)
	sort.Slice(inputs, func(i, j int) bool {
		if inputs[i].Precedence != inputs[j].Precedence {
			return inputs[i].Precedence < inputs[j].Precedence
		}
		if inputs[i].Family != inputs[j].Family {
			return inputs[i].Family < inputs[j].Family
		}
		return inputs[i].Path < inputs[j].Path
	})
	var config []map[string]any
	var dispositions []Disposition
	for _, input := range inputs {
		if !allowed[input.Family] || !filepath.IsAbs(input.Path) || input.Precedence < 0 || seenPath[input.Family+"\x00"+input.Path] {
			return nil, nil, fmt.Errorf("unknown, duplicate or unaccounted external input")
		}
		seenFamily[input.Family] = true
		seenPath[input.Family+"\x00"+input.Path] = true
		info, err := os.Lstat(input.Path)
		switch input.State {
		case "absent":
			if !os.IsNotExist(err) {
				return nil, nil, fmt.Errorf("external family %s is present or unreadable; absent declaration refused", input.Family)
			}
			config = append(config, map[string]any{"input": input})
			dispositions = append(dispositions, Disposition{input.Family, 0, "manifest/rebuild", "requested source absent at declared path"})
		case "json-config":
			if err != nil || !info.Mode().IsRegular() || info.Size() > 4<<20 {
				return nil, nil, fmt.Errorf("external family %s must be a regular JSON file <=4 MiB", input.Family)
			}
			data, err := os.ReadFile(input.Path)
			if err != nil {
				return nil, nil, fmt.Errorf("external family %s unreadable", input.Family)
			}
			var values map[string]any
			if err := decodeJSON(data, &values); err != nil || values == nil {
				return nil, nil, fmt.Errorf("external family %s is not a supported JSON config object", input.Family)
			}
			if err := validateSecretReferences(values); err != nil {
				return nil, nil, fmt.Errorf("external family %s: %w", input.Family, err)
			}
			config = append(config, map[string]any{"input": input, "values": values})
			dispositions = append(dispositions, Disposition{input.Family, len(values), "archive", "explicit JSON config; secrets are references; precedence retained, effective config not inferred"})
		default:
			return nil, nil, fmt.Errorf("external family %s has unsupported input state", input.Family)
		}
	}
	for _, family := range externalFamilies {
		if !seenFamily[family] {
			return nil, nil, fmt.Errorf("unaccounted external family %s", family)
		}
	}
	data, err := marshal(config)
	return data, dispositions, err
}

func secretKey(key string) bool {
	// Keep dump classification aligned with the existing configuration
	// security predicate. Retain dump-specific substring classifications,
	// while the shared predicate covers api-key/apikey,
	// passwd/privatekey and equivalent recognized names.
	lower := strings.ToLower(key)
	return config.IsSecretKey(key) || strings.Contains(lower, "dsn") || strings.Contains(lower, "access_key")
}

func validateSecretReferences(value any) error {
	switch value := value.(type) {
	case map[string]any:
		if ref, ok := value["$secret"]; ok {
			name, ok := ref.(string)
			if !ok || name == "" || len(value) != 1 {
				return fmt.Errorf("invalid secret reference")
			}
			return nil
		}
		for key, item := range value {
			if secretKey(key) {
				ref, ok := item.(map[string]any)
				if !ok || len(ref) != 1 || ref["$secret"] == nil {
					return fmt.Errorf("credential values/digests require secret references")
				}
			}
			if err := validateSecretReferences(item); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range value {
			if err := validateSecretReferences(item); err != nil {
				return err
			}
		}
	}
	return nil
}
