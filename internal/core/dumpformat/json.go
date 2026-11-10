package dumpformat

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// encoding/json accepts missing/null scalar fields and case-insensitive struct
// keys. The wire contract requires exact, explicitly present typed members.
// Source JSON values are interfaces so explicit SQL nulls remain legal there.
func checkShape(value any, typ reflect.Type) error {
	if typ.Kind() == reflect.Interface {
		return nil
	}
	if value == nil {
		return fmt.Errorf("null typed JSON member")
	}
	// Per-commit migration cursors have explicitly nullable slots. Their custom
	// decoder enforces uint64 values after this exact required-field shape;
	// all other typed fields keep the existing null refusal.
	if typ == reflect.TypeOf(NativeCommitSchemaProfile{}) {
		typ = reflect.TypeOf(nativeCommitSchemaWire{})
	}
	switch typ.Kind() {
	case reflect.Struct:
		object, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("expected JSON object")
		}
		fields := map[string]reflect.StructField{}
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			name := strings.Split(f.Tag.Get("json"), ",")[0]
			if name == "-" {
				continue
			}
			fields[name] = f
			if _, present := object[name]; !present && !strings.Contains(f.Tag.Get("json"), ",omitempty") {
				return fmt.Errorf("missing typed JSON member %q", name)
			}
		}
		for name, item := range object {
			f, ok := fields[name]
			if !ok {
				return fmt.Errorf("unknown typed JSON member")
			}
			if err := checkShape(item, f.Type); err != nil {
				return fmt.Errorf("member %s: %w", name, err)
			}
		}
	case reflect.Map:
		object, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("expected JSON object")
		}
		for _, item := range object {
			if err := checkShape(item, typ.Elem()); err != nil {
				return err
			}
		}
	case reflect.Slice:
		array, ok := value.([]any)
		if !ok {
			return fmt.Errorf("expected JSON array")
		}
		for _, item := range array {
			if err := checkShape(item, typ.Elem()); err != nil {
				return err
			}
		}
	case reflect.String:
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected JSON string")
		}
	case reflect.Int, reflect.Int64, reflect.Uint64:
		if _, ok := value.(json.Number); !ok {
			return fmt.Errorf("expected JSON integer")
		}
	}
	return nil
}

// The standard decoder replaces unpaired UTF-16 escapes with U+FFFD. Refuse
// that lossy repair while allowing valid surrogate pairs and literal UTF-8.
func checkUnicodeEscapes(data []byte) error {
	quoted := false
	unit := func(at int) (uint64, error) {
		if at+4 > len(data) {
			return 0, fmt.Errorf("invalid Unicode escape")
		}
		n, err := strconv.ParseUint(string(data[at:at+4]), 16, 16)
		if err != nil {
			return 0, fmt.Errorf("invalid Unicode escape")
		}
		return n, nil
	}
	for i := 0; i < len(data); i++ {
		if data[i] == '"' {
			quoted = !quoted
			continue
		}
		if !quoted || data[i] != '\\' {
			continue
		}
		i++
		if i >= len(data) {
			return fmt.Errorf("invalid JSON escape")
		}
		if data[i] != 'u' {
			continue
		}
		n, err := unit(i + 1)
		if err != nil {
			return err
		}
		i += 4
		if n >= 0xdc00 && n <= 0xdfff {
			return fmt.Errorf("unpaired Unicode surrogate")
		}
		if n >= 0xd800 && n <= 0xdbff {
			if i+6 >= len(data) || data[i+1] != '\\' || data[i+2] != 'u' {
				return fmt.Errorf("unpaired Unicode surrogate")
			}
			low, err := unit(i + 3)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return fmt.Errorf("unpaired Unicode surrogate")
			}
			i += 6
		}
	}
	return nil
}
