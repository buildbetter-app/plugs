package ontology

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// encoding/json's usual struct decoder accepts duplicate and case-insensitive
// members and treats null scalars as zero values. Configuration cannot do that:
// a misspelled guard or a duplicate definition must not silently replace a rule.
func uniqueValue(dec *json.Decoder) (any, error) {
	token, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch token {
	case json.Delim('{'):
		object := map[string]any{}
		for dec.More() {
			member, err := dec.Token()
			if err != nil {
				return nil, err
			}
			key, ok := member.(string)
			if !ok {
				return nil, fmt.Errorf("expected object key")
			}
			if _, exists := object[key]; exists {
				return nil, fmt.Errorf("duplicate member %q", key)
			}
			value, err := uniqueValue(dec)
			if err != nil {
				return nil, err
			}
			object[key] = value
		}
		end, err := dec.Token()
		if err != nil {
			return nil, err
		}
		if end != json.Delim('}') {
			return nil, fmt.Errorf("expected object end")
		}
		return object, nil
	case json.Delim('['):
		array := []any{}
		for dec.More() {
			value, err := uniqueValue(dec)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		end, err := dec.Token()
		if err != nil {
			return nil, err
		}
		if end != json.Delim(']') {
			return nil, fmt.Errorf("expected array end")
		}
		return array, nil
	default:
		return token, nil
	}
}

func checkShape(value any, typ reflect.Type, path string) error {
	if value == nil {
		return fmt.Errorf("%s: null is not allowed", path)
	}
	if typ.Kind() == reflect.Interface {
		return nil
	} // default's JSON value
	if typ.Kind() == reflect.Pointer {
		return checkShape(value, typ.Elem(), path)
	}
	switch typ.Kind() {
	case reflect.Struct:
		object, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("%s: expected object", path)
		}
		fields := map[string]reflect.Type{}
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			fields[strings.Split(field.Tag.Get("json"), ",")[0]] = field.Type
		}
		for _, name := range keys(object) {
			fieldType, ok := fields[name]
			if !ok {
				return fmt.Errorf("%s.%s: unknown member", path, name)
			}
			if err := checkShape(object[name], fieldType, path+"."+name); err != nil {
				return err
			}
		}
	case reflect.Map:
		object, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("%s: expected object", path)
		}
		for _, name := range keys(object) {
			if err := checkShape(object[name], typ.Elem(), path+"."+name); err != nil {
				return err
			}
		}
	case reflect.Slice:
		array, ok := value.([]any)
		if !ok {
			return fmt.Errorf("%s: expected array", path)
		}
		for i, element := range array {
			if err := checkShape(element, typ.Elem(), fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	}
	return nil
}
