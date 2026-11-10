package dumpformat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// D2 explicitly permits null pointers and requires their field presence. This
// decoder is separate so legacy JSON/null admission rules stay unchanged.
func decodeNativeDeltaJSON(raw []byte, target any) error {
	var shape any
	if err := decodeJSON(raw, &shape); err != nil {
		return err
	}
	if err := checkNativeDeltaShape(shape, reflect.TypeOf(target).Elem()); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	d.DisallowUnknownFields()
	return d.Decode(target)
}

func checkNativeDeltaShape(value any, typ reflect.Type) error {
	if typ.Kind() == reflect.Interface {
		return nil
	}
	if typ.Kind() == reflect.Pointer {
		if value == nil {
			return nil
		}
		return checkNativeDeltaShape(value, typ.Elem())
	}
	if value == nil {
		return fmt.Errorf("null native delta typed field")
	}
	switch typ.Kind() {
	case reflect.Struct:
		object, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("native delta object required")
		}
		known := map[string]reflect.StructField{}
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			tag := f.Tag.Get("json")
			name := strings.Split(tag, ",")[0]
			if name == "-" {
				continue
			}
			if name == "" {
				name = f.Name
			}
			known[name] = f
			if _, ok := object[name]; !ok && !strings.Contains(tag, ",omitempty") {
				return fmt.Errorf("native delta missing field %s", name)
			}
		}
		for name, item := range object {
			f, ok := known[name]
			if !ok {
				return fmt.Errorf("native delta unknown field %s", name)
			}
			if err := checkNativeDeltaShape(item, f.Type); err != nil {
				return fmt.Errorf("native delta %s: %w", name, err)
			}
		}
	case reflect.Slice:
		items, ok := value.([]any)
		if !ok {
			return fmt.Errorf("native delta array required")
		}
		for _, item := range items {
			if err := checkNativeDeltaShape(item, typ.Elem()); err != nil {
				return err
			}
		}
	case reflect.Map:
		items, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("native delta object required")
		}
		for _, item := range items {
			if err := checkNativeDeltaShape(item, typ.Elem()); err != nil {
				return err
			}
		}
	case reflect.String:
		if _, ok := value.(string); !ok {
			return fmt.Errorf("native delta string required")
		}
	case reflect.Bool:
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("native delta boolean required")
		}
	case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, ok := value.(json.Number)
		if !ok {
			return fmt.Errorf("native delta uint required")
		}
		v, err := strconv.ParseUint(string(n), 10, typ.Bits())
		if err != nil || strconv.FormatUint(v, 10) != string(n) {
			return fmt.Errorf("noncanonical native delta uint")
		}
	case reflect.Int, reflect.Int32, reflect.Int64:
		n, ok := value.(json.Number)
		if !ok {
			return fmt.Errorf("native delta integer required")
		}
		v, err := strconv.ParseInt(string(n), 10, typ.Bits())
		if err != nil || strconv.FormatInt(v, 10) != string(n) {
			return fmt.Errorf("noncanonical native delta integer")
		}
	}
	return nil
}
