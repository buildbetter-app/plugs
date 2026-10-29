package dumpformat

import (
	"encoding/json"
	"fmt"
	"reflect"
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
