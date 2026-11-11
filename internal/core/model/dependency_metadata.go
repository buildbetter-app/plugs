package model

import (
	"encoding/json"
	"math/big"
	"strings"
)

// dependencyMetadataTokenEqual compares the represented native metadata value.
// A native SQL text cell is a JSON string containing JSON, so its contents need
// a second decode. Other opaque edge members never enter this comparison.
// Equality retains the original token; it does not rewrite captured source data.
func dependencyMetadataTokenEqual(left, right string) bool {
	if left == right {
		return true
	}
	var a, b any
	if decodeJSON([]byte(left), &a) != nil || decodeJSON([]byte(right), &b) != nil {
		return false
	}
	textA, isTextA := a.(string)
	textB, isTextB := b.(string)
	if isTextA != isTextB {
		return false
	}
	if isTextA {
		if textA == textB {
			return true
		}
		if decodeJSON([]byte(textA), &a) != nil || decodeJSON([]byte(textB), &b) != nil {
			return false // Invalid metadata text falls back to its exact value.
		}
	}
	return dependencyMetadataValueEqual(a, b)
}

func dependencyMetadataValueEqual(a, b any) bool {
	switch left := a.(type) {
	case nil:
		return b == nil
	case bool:
		right, ok := b.(bool)
		return ok && left == right
	case string:
		right, ok := b.(string)
		return ok && left == right
	case json.Number:
		right, ok := b.(json.Number)
		return ok && dependencyMetadataNumber(left) == dependencyMetadataNumber(right)
	case []any:
		right, ok := b.([]any)
		if !ok || len(left) != len(right) {
			return false
		}
		for i := range left {
			if !dependencyMetadataValueEqual(left[i], right[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		right, ok := b.(map[string]any)
		if !ok || len(left) != len(right) {
			return false
		}
		for key, value := range left {
			other, present := right[key]
			if !present || !dependencyMetadataValueEqual(value, other) {
				return false
			}
		}
		return true
	}
	return false
}

// JSON numbers compare as decimal values without a float conversion. Keeping
// the exponent symbolic also bounds work for a short token such as 1e999999999.
// The caller supplies only numbers already validated by the JSON decoder.
func dependencyMetadataNumber(number json.Number) string {
	text := string(number)
	negative := strings.HasPrefix(text, "-")
	text = strings.TrimPrefix(text, "-")
	exponent := new(big.Int)
	if at := strings.IndexAny(text, "eE"); at >= 0 {
		exponent.SetString(text[at+1:], 10)
		text = text[:at]
	}
	if at := strings.IndexByte(text, '.'); at >= 0 {
		exponent.Sub(exponent, big.NewInt(int64(len(text)-at-1)))
		text = text[:at] + text[at+1:]
	}
	text = strings.TrimLeft(text, "0")
	if text == "" {
		return "0"
	}
	coefficient := strings.TrimRight(text, "0")
	exponent.Add(exponent, big.NewInt(int64(len(text)-len(coefficient))))
	if negative {
		coefficient = "-" + coefficient
	}
	return coefficient + "e" + exponent.String()
}
