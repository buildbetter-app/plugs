package model

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
)

// dependencyMetadataTokenEqual compares native metadata values without adding a
// dependency to the SDK/offline plugin host. Objects ignore key order, numbers
// use IEEE 754 doubles, and JSON string escapes use encoding/json's Unicode
// replacement semantics. Duplicate keys, invalid JSON and numeric overflow fall
// back to exact text equality. The original stored token is never rewritten.
//
// This deliberately fixes pinned JCS parser quirks: top-level whitespace is
// insignificant, whitespace cannot join number digits, and invalid surrogate
// pairs use Go's U+FFFD replacement rather than JCS's single DecodeRune result.
func dependencyMetadataTokenEqual(left, right string) bool {
	if left == right {
		return true
	}
	var textA, textB string
	a, b := bytes.TrimSpace([]byte(left)), bytes.TrimSpace([]byte(right))
	isTextA := len(a) != 0 && a[0] == '"'
	isTextB := len(b) != 0 && b[0] == '"'
	if isTextA != isTextB {
		return false
	}
	if isTextA {
		if json.Unmarshal(a, &textA) != nil || json.Unmarshal(b, &textB) != nil {
			return false
		}
		left, right = textA, textB
	}
	valueA, errA := dependencyMetadataValue(left)
	valueB, errB := dependencyMetadataValue(right)
	if errA != nil || errB != nil {
		return left == right
	}
	return reflect.DeepEqual(valueA, valueB)
}

// dependencyMetadataValue rejects duplicate object keys rather than letting
// encoding/json's map decoding silently keep the last value.
func dependencyMetadataValue(text string) (any, error) {
	decoder := json.NewDecoder(bytes.NewBufferString(text))
	value, err := dependencyMetadataElement(decoder)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errors.New("metadata must contain exactly one JSON value")
	}
	return value, nil
}

func dependencyMetadataElement(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, compound := token.(json.Delim)
	if !compound {
		return token, nil
	}
	switch delimiter {
	case '{':
		value := map[string]any{}
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			name, ok := key.(string)
			if !ok {
				return nil, errors.New("metadata object key must be a string")
			}
			if _, exists := value[name]; exists {
				return nil, errors.New("duplicate metadata object key")
			}
			element, err := dependencyMetadataElement(decoder)
			if err != nil {
				return nil, err
			}
			value[name] = element
		}
		if _, err := decoder.Token(); err != nil {
			return nil, err
		}
		return value, nil
	case '[':
		value := []any{}
		for decoder.More() {
			element, err := dependencyMetadataElement(decoder)
			if err != nil {
				return nil, err
			}
			value = append(value, element)
		}
		if _, err := decoder.Token(); err != nil {
			return nil, err
		}
		return value, nil
	default:
		return nil, errors.New("unexpected metadata delimiter")
	}
}
