package model

import (
	"encoding/json"
	"fmt"
)

// PreconditionByRevision selects the record revision compare-and-set arm.
func PreconditionByRevision(ref Ref, revision Revision) Precondition {
	return Precondition{Ref: ref, Revision: &revision}
}

// PreconditionByHash preserves the field/hash arm even for empty strings,
// which are valid in the schema. The pipeline checks the actual field hash.
func PreconditionByHash(ref Ref, field, hash string) Precondition {
	return Precondition{Ref: ref, Field: field, Hash: hash, hashSelected: true}
}

func (p Precondition) Validate() error {
	if err := p.Ref.Validate(); err != nil {
		return fmt.Errorf("precondition ref: %w", err)
	}
	hasHash := p.hashSelected || p.Field != "" || p.Hash != ""
	if (p.Revision != nil) == hasHash {
		return fmt.Errorf("precondition must select exactly one revision or field/hash arm")
	}
	if p.Revision != nil {
		return p.Revision.Validate()
	}
	return nil
}

func (p Precondition) MarshalJSON() ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	if p.Revision != nil {
		return json.Marshal(struct {
			Ref      Ref      `json:"ref"`
			Revision Revision `json:"revision"`
		}{p.Ref, *p.Revision})
	}
	return json.Marshal(struct {
		Ref   Ref    `json:"ref"`
		Field string `json:"field"`
		Hash  string `json:"hash"`
	}{p.Ref, p.Field, p.Hash})
}

func (p *Precondition) UnmarshalJSON(data []byte) error {
	type wire Precondition
	var value wire
	if err := decodeStrict(data, &value, "ref"); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := decodeJSON(data, &fields); err != nil {
		return err
	}
	_, revision := fields["revision"]
	_, field := fields["field"]
	_, hash := fields["hash"]
	if revision {
		if field || hash {
			return fmt.Errorf("revision precondition cannot carry field/hash")
		}
	} else if !field || !hash {
		return fmt.Errorf("field/hash precondition requires both members")
	}
	decoded := Precondition(value)
	decoded.hashSelected = field
	if err := decoded.Validate(); err != nil {
		return err
	}
	*p = decoded
	return nil
}

// MaxChangeSetOps is the canonical atomic batch bound.
const MaxChangeSetOps = 1000

func (c ChangeSet) Validate() error {
	if c.Ops == nil {
		return fmt.Errorf("changeset ops must be an array")
	}
	if len(c.Ops) > MaxChangeSetOps {
		return fmt.Errorf("changeset has more than %d operations", MaxChangeSetOps)
	}
	for i, op := range c.Ops {
		if err := op.Validate(); err != nil {
			return fmt.Errorf("operation %d: %w", i, err)
		}
	}
	for i, p := range c.Expect {
		if err := p.Validate(); err != nil {
			return fmt.Errorf("precondition %d: %w", i, err)
		}
	}
	return nil
}

func (c ChangeSet) MarshalJSON() ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	type wire ChangeSet
	return json.Marshal(wire(c))
}

func (c *ChangeSet) UnmarshalJSON(data []byte) error {
	type wire ChangeSet
	var value wire
	if err := decodeStrict(data, &value, "ops"); err != nil {
		return err
	}
	decoded := ChangeSet(value)
	if err := decoded.Validate(); err != nil {
		return err
	}
	*c = decoded
	return nil
}

func (o Op) Validate() error {
	_, err := o.MarshalJSON()
	return err
}
