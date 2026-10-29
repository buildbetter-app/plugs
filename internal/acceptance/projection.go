package acceptance

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// CanonicalJSON sorts object keys only. Array order, null, strings, and exact
// JSON number spellings survive; this is not a JCS implementation.
func CanonicalJSON(raw []byte) (json.RawMessage, error) {
	v, err := decode(raw)
	if err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

// Project selects required RFC 6901 pointers. Missing values are errors rather
// than silently becoming null. The caller declares all normalization by naming
// the compared paths; semantic ordering is never sorted away.
func Project(raw []byte, pointers ...string) (json.RawMessage, error) {
	root, err := decode(raw)
	if err != nil {
		return nil, err
	}
	projection := make(map[string]any, len(pointers))
	if len(pointers) == 0 {
		return nil, fmt.Errorf("projection needs at least one required path")
	}
	for _, pointer := range pointers {
		if _, exists := projection[pointer]; exists {
			return nil, fmt.Errorf("duplicate projection path %q", pointer)
		}
		value, err := at(root, pointer)
		if err != nil {
			return nil, err
		}
		projection[pointer] = value
	}
	return json.Marshal(projection)
}

// EqualJSON compares stable JSON meaning without rounding integers to float64.
func EqualJSON(want, got []byte) error {
	w, err := CanonicalJSON(want)
	if err != nil {
		return fmt.Errorf("expected JSON: %w", err)
	}
	g, err := CanonicalJSON(got)
	if err != nil {
		return fmt.Errorf("actual JSON: %w", err)
	}
	if !bytes.Equal(w, g) {
		return fmt.Errorf("JSON differs\nwant: %s\n got: %s", w, g)
	}
	return nil
}

func at(root any, pointer string) (any, error) {
	if pointer == "" {
		return root, nil
	}
	if !strings.HasPrefix(pointer, "/") {
		return nil, fmt.Errorf("invalid JSON pointer %q", pointer)
	}
	value := root
	for _, part := range strings.Split(pointer[1:], "/") {
		var key strings.Builder
		for i := 0; i < len(part); i++ {
			if part[i] != '~' {
				key.WriteByte(part[i])
				continue
			}
			i++
			if i == len(part) || (part[i] != '0' && part[i] != '1') {
				return nil, fmt.Errorf("invalid escape in JSON pointer %q", pointer)
			}
			if part[i] == '0' {
				key.WriteByte('~')
			} else {
				key.WriteByte('/')
			}
		}
		name := key.String()
		switch node := value.(type) {
		case map[string]any:
			var ok bool
			value, ok = node[name]
			if !ok {
				return nil, fmt.Errorf("missing required path %q", pointer)
			}
		case []any:
			i, err := strconv.Atoi(name)
			if err != nil || i < 0 || i >= len(node) || strconv.Itoa(i) != name {
				return nil, fmt.Errorf("invalid array index in path %q", pointer)
			}
			value = node[i]
		default:
			return nil, fmt.Errorf("cannot traverse required path %q", pointer)
		}
	}
	return value, nil
}

// decode rejects duplicate members, including nested ones. Ordinary Unmarshal
// would silently hide conflicting fields in an acceptance response.
func decode(raw []byte) (any, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	value, err := readValue(d)
	if err != nil {
		return nil, err
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, fmt.Errorf("trailing JSON input")
	}
	return value, nil
}

func readValue(d *json.Decoder) (any, error) {
	token, err := d.Token()
	if err != nil {
		return nil, err
	}
	delim, compound := token.(json.Delim)
	if !compound {
		return token, nil
	}
	switch delim {
	case '{':
		object := make(map[string]any)
		for d.More() {
			keyToken, err := d.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, fmt.Errorf("invalid object member")
			}
			if _, exists := object[key]; exists {
				return nil, fmt.Errorf("duplicate JSON member %q", key)
			}
			object[key], err = readValue(d)
			if err != nil {
				return nil, err
			}
		}
		if _, err := d.Token(); err != nil {
			return nil, err
		}
		return object, nil
	case '[':
		array := []any{}
		for d.More() {
			value, err := readValue(d)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		if _, err := d.Token(); err != nil {
			return nil, err
		}
		return array, nil
	default:
		return nil, fmt.Errorf("unexpected JSON delimiter %q", delim)
	}
}
