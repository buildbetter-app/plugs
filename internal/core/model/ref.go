package model

import (
	"encoding/json"
	"fmt"
)

type Ref struct {
	// Presence selects the oneOf arm, including an explicit empty id or key.
	// Use the constructors to select an empty arm in native Go code.
	ID       string       `json:"id,omitempty"`
	Branch   Natural      `json:"branch,omitempty"`
	Key      string       `json:"key,omitempty"`
	selected refSelection `json:"-"`
}

type refSelection uint8

const (
	refUnselected refSelection = iota
	refID
	refKey
)

func RefByID(id string, branch Natural) Ref { return Ref{ID: id, Branch: branch, selected: refID} }
func RefByKey(key string) Ref               { return Ref{Key: key, selected: refKey} }

func (r Ref) selection() refSelection {
	if r.selected != refUnselected {
		return r.selected
	}
	if r.ID != "" || !r.Branch.IsZero() {
		return refID
	}
	if r.Key != "" {
		return refKey
	}
	return refUnselected
}

// IsID preserves the selected form even when the selected string is empty.
// Callers validate the reference before using it as an identity or lock key.
func (r Ref) IsID() bool { return r.selection() == refID }

func (r Ref) Validate() error {
	if err := r.Branch.Validate(); err != nil {
		return fmt.Errorf("ref branch: %w", err)
	}
	switch r.selection() {
	case refID:
		if r.Key != "" {
			return fmt.Errorf("ref cannot contain both id and key")
		}
	case refKey:
		if r.ID != "" || !r.Branch.IsZero() {
			return fmt.Errorf("ref key cannot carry an id or branch")
		}
	default:
		return fmt.Errorf("ref must contain exactly one of id or key")
	}
	return nil
}

func (r Ref) MarshalJSON() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	if r.IsID() {
		return json.Marshal(struct {
			ID     string  `json:"id"`
			Branch Natural `json:"branch,omitempty"`
		}{r.ID, r.Branch})
	}
	return json.Marshal(struct {
		Key string `json:"key"`
	}{r.Key})
}

func (r *Ref) UnmarshalJSON(data []byte) error {
	type wireRef Ref
	var value wireRef
	if err := decodeStrict(data, &value); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := decodeJSON(data, &fields); err != nil {
		return err
	}
	_, hasID := fields["id"]
	_, hasKey := fields["key"]
	if hasID == hasKey {
		return fmt.Errorf("ref must contain exactly one of id or key")
	}
	if hasKey {
		if _, hasBranch := fields["branch"]; hasBranch {
			return fmt.Errorf("ref key cannot carry a branch")
		}
	}
	decoded := Ref(value)
	if hasID {
		decoded.selected = refID
	} else {
		decoded.selected = refKey
	}
	if err := decoded.Validate(); err != nil {
		return err
	}
	*r = decoded
	return nil
}

type ResourceRef struct {
	Kind string `json:"kind"`
	Ref  Ref    `json:"ref"`
}
type FieldSet []string
