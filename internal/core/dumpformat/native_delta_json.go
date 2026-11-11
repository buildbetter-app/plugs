package dumpformat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"
)

// D2 explicitly permits null pointers and requires their field presence. This
// decoder is separate so legacy JSON/null admission rules stay unchanged.
func decodeNativeDeltaJSON(raw []byte, target any) error {
	return decodeNativeDeltaJSONWithBudget(raw, target, nil)
}

func decodeNativeDeltaJSONWithBudget(raw []byte, target any, budget *NativeDeltaBudget, phases ...string) error {
	if !utf8.Valid(raw) {
		return fmt.Errorf("JSON must be UTF-8")
	}
	if err := checkUnicodeEscapes(raw); err != nil {
		return err
	}
	var release func()
	reserve := func(phases ...string) error {
		if budget == nil {
			return nil
		}
		scope := budget.Scope()
		release = scope.Release
		counter := deltaSizeCounter{}
		if err := counter.raw(raw); err != nil {
			scope.Release()
			return err
		}
		for _, phase := range phases {
			n := counter.n
			if strings.HasSuffix(phase, "input") {
				n = uint64(len(raw))
			}
			if err := scope.ChargeBytes(phase, n); err != nil {
				scope.Release()
				return err
			}
		}
		return nil
	}
	if err := reserve("json_validation_input", "json_validation_tokens"); err != nil {
		return err
	}
	check := json.NewDecoder(bytes.NewReader(raw))
	check.UseNumber()
	if err := checkValue(check, 0); err != nil {
		if release != nil {
			release()
		}
		return err
	}
	if _, err := check.Token(); err != io.EOF {
		if release != nil {
			release()
		}
		return fmt.Errorf("trailing JSON data")
	}
	if release != nil {
		release()
		release = nil
	}
	if err := reserve("json_shape_input", "json_shape_candidate", "json_typed_input"); err != nil {
		return err
	}
	if release != nil {
		defer release()
	}
	var shape any
	shapeDecoder := json.NewDecoder(bytes.NewReader(raw))
	shapeDecoder.UseNumber()
	if err := shapeDecoder.Decode(&shape); err != nil {
		return err
	}
	if err := checkNativeDeltaShape(shape, reflect.TypeOf(target).Elem()); err != nil {
		return err
	}
	var output *NativeDeltaBudget
	if budget != nil {
		output = budget.Scope()
		defer output.Release()
		counter := deltaSizeCounter{}
		if err := counter.raw(raw); err != nil {
			return err
		}
		phase := "json_typed_candidate"
		if len(phases) != 0 {
			phase = phases[0]
		}
		if err := output.ChargeBytes(phase, counter.n+1); err != nil {
			return err
		}
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return err
	}
	if output != nil {
		// Scratch ends here, but the detached typed target belongs to the
		// caller's owner from before Decode until that owner drops it.
		output.Retain()
	}
	return nil
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
