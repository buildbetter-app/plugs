package ontology

import (
	"encoding/json"
	"strconv"
	"unicode/utf8"
)

// Await members retain the public native gate data independently of a gate
// resolver or a bead's type. Waiters are an ordered whole value, not a set.
const (
	maxAwaitTypeCharacters = 32
	maxAwaitIDCharacters   = 255
	maxWaitersJSONBytes    = 65535
)

func isAwaitMember(name string) bool {
	switch name {
	case "await_type", "await_id", "timeout", "waiters":
		return true
	default:
		return false
	}
}

func validateAwaitMembers(before, after *BeadState, facts GuardFacts) []Violation {
	var violations []Violation
	for _, name := range []string{"await_type", "await_id", "timeout", "waiters"} {
		value, present := after.Fields[name]
		// The native public patch exposes await_id and waiters only. A create
		// may carry all four; an unrelated edit preserves the original data.
		if before != nil && (name == "await_type" || name == "timeout") {
			old, existed := before.Fields[name]
			if existed != present || !sameValue(old, value) || hasPath(facts.Touched, "fields."+name) {
				violations = append(violations, invalid("fields."+name, "await member is create-only")...)
				continue
			}
		}
		if !present {
			continue
		}
		valid := false
		switch name {
		case "await_type", "await_id":
			limit := maxAwaitTypeCharacters
			if name == "await_id" {
				limit = maxAwaitIDCharacters
			}
			text, ok := value.(string)
			valid = ok && utf8.ValidString(text) && utf8.RuneCountInString(text) <= limit
		case "timeout":
			// time.Duration's public representation is an int64 nanosecond
			// count. Never round through float64 or expand an exponent.
			data, err := json.Marshal(value)
			if err == nil {
				n, err := strconv.ParseInt(string(data), 10, 64)
				valid = err == nil && n >= 0
			}
		case "waiters":
			if validValue(Field{Type: "strings"}, value) {
				data, err := json.Marshal(value)
				valid = err == nil && len(data) <= maxWaitersJSONBytes
			}
		}
		if !valid {
			violations = append(violations, invalid("fields."+name, "invalid await member type or bound")...)
		}
	}
	return violations
}

func awaitMemberSchemas() map[string]any {
	return map[string]any{
		"await_type": map[string]any{"type": "string", "maxLength": maxAwaitTypeCharacters, "x-plug-create-only": true},
		"await_id":   map[string]any{"type": "string", "maxLength": maxAwaitIDCharacters},
		"timeout":    map[string]any{"type": "integer", "minimum": 0, "maximum": int64(9223372036854775807), "x-plug-create-only": true, "description": "Nanoseconds"},
		"waiters":    map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "x-plug-max-json-bytes": maxWaitersJSONBytes},
	}
}
