package model

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
)

// decodeStrict is the one JSON boundary used by every custom wire codec in
// this sketch. OpenRPC closes these objects, so ordinary json.Unmarshal is not
// sufficient: it silently drops unknown members and accepts a missing object
// as its zero value. The required list is checked separately because Go's
// zero-value fields cannot distinguish an omitted member from an explicit
// zero. Nested values invoke their own strict codecs through json.Decoder.
func decodeStrict(data []byte, dst any, required ...string) error {
	var fields map[string]json.RawMessage
	if err := decodeJSON(data, &fields); err != nil {
		return err
	}
	if fields == nil {
		return fmt.Errorf("expected JSON object")
	}
	for _, name := range required {
		if _, ok := fields[name]; !ok {
			return fmt.Errorf("missing required JSON member %q", name)
		}
	}
	if err := checkWireFields(fields, reflect.TypeOf(dst).Elem()); err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	dec.UseNumber()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return err
	}
	return nil
}

// encoding/json matches field names without regard to case and treats null
// as a no-op for scalar fields. Neither behavior matches JSON Schema. Check
// the exact member names and typed collection elements before decoding. Only
// sponsor is nullable; RawMessage and any deliberately retain arbitrary JSON.
func checkWireFields(fields map[string]json.RawMessage, typ reflect.Type) error {
	allowed := map[string]reflect.StructField{}
	var collect func(reflect.Type)
	collect = func(t reflect.Type) {
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if field.Anonymous {
				collect(field.Type)
				continue
			}
			name := strings.Split(field.Tag.Get("json"), ",")[0]
			if name != "-" && name != "" {
				allowed[name] = field
			}
		}
	}
	collect(typ)
	for name, raw := range fields {
		field, ok := allowed[name]
		if !ok {
			return fmt.Errorf("unknown JSON member %q", name)
		}
		if err := checkWireValue(raw, field.Type, field.Tag.Get("wire") == "nullable"); err != nil {
			return fmt.Errorf("JSON member %q: %w", name, err)
		}
	}
	return nil
}

func checkWireValue(raw json.RawMessage, typ reflect.Type, nullable bool) error {
	if typ == reflect.TypeFor[json.RawMessage]() || typ.Kind() == reflect.Interface {
		return nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		if nullable {
			return nil
		}
		return fmt.Errorf("null is not allowed")
	}
	if typ.Kind() == reflect.Pointer {
		return checkWireValue(raw, typ.Elem(), false)
	}
	if reflect.PointerTo(typ).Implements(reflect.TypeFor[json.Unmarshaler]()) {
		// The nested custom codec checks its own tagged schema.
		return nil
	}
	switch typ.Kind() {
	case reflect.Slice:
		var values []json.RawMessage
		if err := decodeJSON(raw, &values); err != nil {
			return err
		}
		for _, value := range values {
			if err := checkWireValue(value, typ.Elem(), false); err != nil {
				return err
			}
		}
	case reflect.Map:
		var values map[string]json.RawMessage
		if err := decodeJSON(raw, &values); err != nil {
			return err
		}
		for _, value := range values {
			if err := checkWireValue(value, typ.Elem(), false); err != nil {
				return err
			}
		}
	case reflect.Struct:
		var fields map[string]json.RawMessage
		if err := decodeJSON(raw, &fields); err != nil {
			return err
		}
		return checkWireFields(fields, typ)
	}
	return nil
}

func decodeJSON(data []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return err
	}
	return nil
}
