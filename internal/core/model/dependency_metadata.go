package model

import (
	"bytes"
	"encoding/json"

	"github.com/gowebpki/jcs"
)

// dependencyMetadataTokenEqual compares the represented native metadata value
// with the same pinned JCS implementation as native DependencyMetadataEqual.
// A native SQL text cell is a JSON string containing JSON. Decode that wrapper
// without re-encoding its contents; physical null and other values stay distinct.
// Equality retains the original token, including its original number spelling.
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
	canonicalA, errA := jcs.Transform([]byte(left))
	canonicalB, errB := jcs.Transform([]byte(right))
	if errA != nil || errB != nil {
		return left == right // Native falls back to the original metadata text.
	}
	return bytes.Equal(canonicalA, canonicalB)
}
