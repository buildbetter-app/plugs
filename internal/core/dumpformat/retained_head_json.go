package dumpformat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// retainedHeadJSON walks the typed capture envelope in its retained bytes.
// Inventory and history containers are traversed in wire order. Only a
// complete row or scalar reaches the strict decoder as a single value.
type retainedHeadJSON struct {
	rest   []byte
	member string
	owner  *NativeDeltaBudget
}

func decodeRetainedHeadJSON(raw []byte, member string, owner *NativeDeltaBudget, target any) (map[string]json.RawMessage, error) {
	reader := retainedHeadJSON{rest: raw, member: member, owner: owner}
	fields := map[string]json.RawMessage{}
	if err := reader.decode(reflect.ValueOf(target).Elem(), "$", 0, 0, fields); err != nil {
		return nil, err
	}
	reader.peek()
	if len(reader.rest) != 0 {
		return nil, fmt.Errorf("trailing retained head JSON")
	}
	return fields, nil
}

func (r *retainedHeadJSON) decode(target reflect.Value, path string, index uint64, depth int, fields map[string]json.RawMessage) error {
	first := r.peek()
	if depth >= driver.NativeHeadJSONDepth && (first == '{' || first == '[') {
		return driver.LocateReverseJSONLimit(&driver.ReverseJSONLimitError{
			Resource: "nesting_depth", Measured: uint64(depth + 1), Limit: driver.NativeHeadJSONDepth,
		}, r.member, path, index)
	}
	if first == 'n' {
		return fmt.Errorf("null typed JSON member")
	}
	switch target.Kind() {
	case reflect.Struct:
		switch target.Interface().(type) {
		case Manifest, Project, SourceHistory, ProjectSourceHistory, SourceHistoryRange, SourceHistoryFrame:
			return r.object(target, path, index, depth, fields)
		}
	case reflect.Map:
		if target.Type().Elem().Kind() != reflect.Interface {
			return r.object(target, path, index, depth, nil)
		}
	case reflect.Slice:
		if !r.consume('[') {
			return fmt.Errorf("expected retained head array")
		}
		if !r.consume(']') {
			for i := uint64(0); ; i++ {
				entry := reflect.New(target.Type().Elem()).Elem()
				if err := r.decode(entry, path+"[]", i, depth+1, nil); err != nil {
					return err
				}
				target.Set(reflect.Append(target, entry))
				if r.consume(']') {
					break
				}
				if !r.consume(',') {
					return fmt.Errorf("invalid retained head array")
				}
			}
		}
		if target.IsNil() {
			target.Set(reflect.MakeSlice(target.Type(), 0, 0))
		}
		return nil
	}
	raw := r.value()
	footprint, err := driver.ReverseJSONElementFootprint(raw, depth)
	if err != nil {
		return driver.LocateReverseJSONLimit(err, r.member, path, index)
	}
	if err := r.owner.ChargeBytes("head_decoded_elements", footprint); err != nil {
		return err
	}
	return decodeRetainedValue(raw, target.Addr().Interface(), depth, footprint, r.owner)
}

func (r *retainedHeadJSON) object(target reflect.Value, path string, index uint64, depth int, fields map[string]json.RawMessage) error {
	if !r.consume('{') {
		return fmt.Errorf("expected retained head object")
	}
	seen := map[string]bool{}
	isMap := target.Kind() == reflect.Map
	if isMap {
		target.Set(reflect.MakeMap(target.Type()))
	}
	fieldIndex := map[string]int{}
	if !isMap {
		for i := range target.NumField() {
			name, _, _ := strings.Cut(target.Type().Field(i).Tag.Get("json"), ",")
			fieldIndex[name] = i
		}
	}
	done := r.consume('}')
	for i := uint64(0); !done; i++ {
		if r.peek() != '"' {
			return fmt.Errorf("invalid retained head member key")
		}
		key := r.value()
		footprint, err := driver.ReverseJSONElementFootprint(key, depth+1)
		if err != nil {
			return driver.LocateReverseJSONLimit(err, r.member, path+".<id>", i)
		}
		if err := r.owner.ChargeBytes("head_decoded_elements", footprint+96); err != nil {
			return err
		}
		var name string
		if err := decodeRetainedValue(key, &name, depth+1, footprint, r.owner); err != nil {
			return err
		}
		if seen[name] || !r.consume(':') {
			return fmt.Errorf("invalid or duplicate retained head member")
		}
		seen[name] = true
		if isMap {
			value := reflect.New(target.Type().Elem()).Elem()
			if err := r.decode(value, path+".<id>", i, depth+1, nil); err != nil {
				return err
			}
			target.SetMapIndex(reflect.ValueOf(name), value)
		} else {
			field, ok := fieldIndex[name]
			if !ok {
				return fmt.Errorf("unknown typed JSON member")
			}
			before := r.rest
			if err := r.decode(target.Field(field), path+"."+name, index, depth+1, nil); err != nil {
				return err
			}
			if fields != nil {
				fields[name] = bytes.TrimSpace(before[:len(before)-len(r.rest)])
			}
		}
		done = r.consume('}')
		if !done && !r.consume(',') {
			return fmt.Errorf("invalid retained head object")
		}
	}
	for name, field := range fieldIndex {
		if !seen[name] && !strings.HasSuffix(target.Type().Field(field).Tag.Get("json"), ",omitempty") {
			return fmt.Errorf("missing typed JSON member %q", name)
		}
	}
	if _, scoped := seen["project_histories"]; scoped && len(seen) != 2 {
		return fmt.Errorf("invalid project-scoped history members")
	}
	if _, scoped := seen["project_histories"]; scoped && !seen["format"] {
		return fmt.Errorf("missing typed JSON member %q", "format")
	}
	return nil
}

func decodeRetainedValue(raw []byte, target any, depth int, footprint uint64, owner *NativeDeltaBudget) error {
	if owner != nil {
		scratch := owner.Scope()
		defer scratch.Release()
		// The strict decoder builds a generic shape before typed decoding.
		// Reserve that shape plus two independently growing decoder buffers.
		if err := scratch.ChargeBytes("retained_json_decode_scratch", footprint+4*uint64(len(raw))); err != nil {
			return err
		}
	}
	return decodeJSONAtDepth(raw, target, depth)
}

// decodeAny walks a span already admitted for JSON syntax and depth. The
// input stays borrowed and each scalar is decoded directly into the graph.
func (r *retainedHeadJSON) decodeAny() (any, error) {
	switch r.peek() {
	case '{':
		r.take(1)
		object := make(map[string]any)
		for !r.consume('}') {
			var key string
			if err := json.Unmarshal(r.value(), &key); err != nil {
				return nil, err
			}
			if _, duplicate := object[key]; duplicate {
				return nil, fmt.Errorf("duplicate or invalid JSON key")
			}
			r.consume(':')
			value, err := r.decodeAny()
			if err != nil {
				return nil, err
			}
			object[key] = value
			r.consume(',')
		}
		return object, nil
	case '[':
		r.take(1)
		array := make([]any, 0)
		for !r.consume(']') {
			value, err := r.decodeAny()
			if err != nil {
				return nil, err
			}
			array = append(array, value)
			r.consume(',')
		}
		return array, nil
	case '"':
		var value string
		err := json.Unmarshal(r.value(), &value)
		return value, err
	case 't', 'f', 'n':
		var value any
		err := json.Unmarshal(r.value(), &value)
		return value, err
	default:
		return json.Number(r.value()), nil
	}
}

func (r *retainedHeadJSON) consume(want byte) bool {
	if r.peek() == want {
		r.rest = r.rest[1:]
		return true
	}
	return false
}

func (r *retainedHeadJSON) peek() byte {
	r.rest = bytes.TrimLeft(r.rest, " \t\r\n")
	if len(r.rest) == 0 {
		return 0
	}
	return r.rest[0]
}

// value frames a borrowed span. The strict decoder validates its grammar.
func (r *retainedHeadJSON) value() []byte {
	r.rest = bytes.TrimLeft(r.rest, " \t\r\n")
	depth := 0
	quoted, escaped := false, false
	for i, b := range r.rest {
		switch {
		case escaped:
			escaped = false
		case quoted:
			escaped = b == '\\'
			if b == '"' {
				quoted = false
				if depth == 0 {
					return r.take(i + 1)
				}
			}
		case b == '"':
			quoted = true
		case b == '{' || b == '[':
			depth++
		case b == '}' || b == ']':
			if depth == 0 {
				return r.take(i)
			}
			depth--
			if depth == 0 {
				return r.take(i + 1)
			}
		case depth == 0 && (jsonSpace(b) || b == ',' || b == ':'):
			return r.take(i)
		}
	}
	return r.take(len(r.rest))
}

func (r *retainedHeadJSON) take(n int) []byte {
	raw := r.rest[:n]
	r.rest = r.rest[n:]
	return raw
}
