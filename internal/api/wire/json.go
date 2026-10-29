package wire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"unicode/utf8"
)

// DecodeJSON keeps numbers lossless and rejects duplicate object members. It is
// the shared byte boundary for envelopes, parameters and handler results.
func DecodeJSON(data []byte) (any, error) {
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("invalid UTF-8")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	v, err := decodeValue(d, 0)
	if err != nil {
		return nil, err
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, fmt.Errorf("expected one JSON value")
	}
	return v, nil
}

func decodeValue(d *json.Decoder, depth int) (any, error) {
	if depth > 128 {
		return nil, fmt.Errorf("JSON nesting exceeds 128")
	}
	t, err := d.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := t.(json.Delim)
	if !ok {
		return t, nil
	}
	switch delim {
	case '{':
		out := map[string]any{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return nil, err
			}
			name, ok := key.(string)
			if !ok {
				return nil, fmt.Errorf("expected object member")
			}
			if _, exists := out[name]; exists {
				return nil, fmt.Errorf("duplicate member %q", name)
			}
			v, err := decodeValue(d, depth+1)
			if err != nil {
				return nil, err
			}
			out[name] = v
		}
		if _, err = d.Token(); err != nil {
			return nil, err
		}
		return out, nil
	case '[':
		out := []any{}
		for d.More() {
			v, err := decodeValue(d, depth+1)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		if _, err = d.Token(); err != nil {
			return nil, err
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unexpected delimiter")
	}
}

// DecodeInto preserves json.Number values in application-owned maps.
func DecodeInto(data []byte, dst any) error {
	if _, err := DecodeJSON(data); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	d.DisallowUnknownFields()
	return d.Decode(dst)
}
