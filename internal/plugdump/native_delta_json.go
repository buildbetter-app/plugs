package plugdump

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"unicode/utf8"
)

var nativeDeltaJSONNumber = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

// Count the exact encoding/json size before its whole-value encode buffer is
// allocated. Only the closed wire types and their ordinary JSON values enter
// this function. The caller includes the terminating newline in the budget.
func nativeDeltaJSONSize(value any, limit uint64) (uint64, error) {
	var size uint64
	add := func(n uint64) error {
		if n > limit-size {
			return ErrSourceHistoryUnavailable{Reason: "table-size-limit"}
		}
		size += n
		return nil
	}
	var stringSize func(string) error
	stringSize = func(s string) error {
		if err := add(2); err != nil {
			return err
		}
		for len(s) > 0 {
			c := s[0]
			n, width := uint64(1), 1
			switch {
			case c == '"' || c == '\\' || c == '\n' || c == '\r' || c == '\t' || c == '\b' || c == '\f':
				n = 2
			case c < 0x20 || c == '<' || c == '>' || c == '&':
				n = 6
			case c >= utf8.RuneSelf:
				r, w := utf8.DecodeRuneInString(s)
				width, n = w, uint64(w)
				if r == utf8.RuneError && w == 1 || r == '\u2028' || r == '\u2029' {
					n = 6
				}
			}
			if err := add(n); err != nil {
				return err
			}
			s = s[width:]
		}
		return nil
	}
	var visit func(reflect.Value) error
	visit = func(v reflect.Value) error {
		if !v.IsValid() {
			return add(4)
		}
		if v.Type() == reflect.TypeFor[json.Number]() {
			// Validate the number without copying its arbitrarily long spelling.
			s := v.String()
			if !nativeDeltaJSONNumber.MatchString(s) {
				return fmt.Errorf("invalid delta JSON number")
			}
			return add(uint64(len(s)))
		}
		switch v.Kind() {
		case reflect.Interface, reflect.Pointer:
			if v.IsNil() {
				return add(4)
			}
			return visit(v.Elem())
		case reflect.String:
			return stringSize(v.String())
		case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
			// Scalar numeric/bool encodings are bounded by their machine width.
			data, err := json.Marshal(v.Interface())
			if err != nil {
				return err
			}
			return add(uint64(len(data)))
		case reflect.Slice, reflect.Array:
			if v.Kind() == reflect.Slice && v.IsNil() {
				return add(4)
			}
			if v.Type().Elem().Kind() == reflect.Uint8 && v.Kind() == reflect.Slice {
				return add(2 + ((uint64(v.Len())+2)/3)*4)
			}
			if err := add(2); err != nil {
				return err
			}
			for i := 0; i < v.Len(); i++ {
				if i > 0 {
					if err := add(1); err != nil {
						return err
					}
				}
				if err := visit(v.Index(i)); err != nil {
					return err
				}
			}
			return nil
		case reflect.Map:
			if v.IsNil() {
				return add(4)
			}
			if v.Type().Key().Kind() != reflect.String {
				return fmt.Errorf("invalid delta JSON map key")
			}
			if err := add(2); err != nil {
				return err
			}
			i := 0
			it := v.MapRange()
			for it.Next() {
				separator := uint64(1)
				if i > 0 {
					separator++
				}
				if err := add(separator); err != nil {
					return err
				}
				if err := stringSize(it.Key().String()); err != nil {
					return err
				}
				if err := visit(it.Value()); err != nil {
					return err
				}
				i++
			}
			return nil
		case reflect.Struct:
			if err := add(2); err != nil {
				return err
			}
			count := 0
			for i := 0; i < v.NumField(); i++ {
				field := v.Type().Field(i)
				if !field.IsExported() {
					continue
				}
				name, options, _ := strings.Cut(field.Tag.Get("json"), ",")
				if name == "-" {
					continue
				}
				if options == "omitempty" && nativeDeltaJSONEmpty(v.Field(i)) {
					continue
				}
				if name == "" {
					name = field.Name
				}
				separator := uint64(1)
				if count > 0 {
					separator++
				}
				if err := add(separator); err != nil {
					return err
				}
				if err := stringSize(name); err != nil {
					return err
				}
				if err := visit(v.Field(i)); err != nil {
					return err
				}
				count++
			}
			return nil
		}
		return fmt.Errorf("unsupported delta JSON value")
	}
	if err := visit(reflect.ValueOf(value)); err != nil {
		return 0, err
	}
	return size, nil
}

func nativeDeltaJSONEmpty(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.Interface, reflect.Pointer:
		return v.IsZero()
	}
	return false
}
