package model

import (
	"bytes"
	"encoding/json"
	"fmt"
)

func (r Record) Validate() error {
	if err := r.Ref.Validate(); err != nil {
		return err
	}
	if err := r.Revision.Validate(); err != nil {
		return err
	}
	if r.Body == nil {
		return fmt.Errorf("record body must be an object")
	}
	return nil
}
func (r Record) MarshalJSON() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	type wire Record
	return json.Marshal(wire(r))
}
func (r *Record) UnmarshalJSON(data []byte) error {
	type wire Record
	var value wire
	if err := decodeStrict(data, &value, "ref", "kind", "revision", "body"); err != nil {
		return err
	}
	decoded := Record(value)
	if err := decoded.Validate(); err != nil {
		return err
	}
	*r = decoded
	return nil
}

func (r OpResult) Validate() error {
	if err := r.Ref.Validate(); err != nil {
		return err
	}
	if err := r.Revision.Validate(); err != nil {
		return err
	}
	if len(r.Record) > 0 {
		var value map[string]json.RawMessage
		if err := decodeJSON(r.Record, &value); err != nil {
			return err
		}
		if value == nil {
			return fmt.Errorf("operation result record must be an object")
		}
	}
	return nil
}
func (r OpResult) MarshalJSON() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	type wire OpResult
	return json.Marshal(wire(r))
}
func (r *OpResult) UnmarshalJSON(data []byte) error {
	type wire OpResult
	var value wire
	if err := decodeStrict(data, &value, "ref", "revision"); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := decodeJSON(data, &fields); err != nil {
		return err
	}
	if raw, exists := fields["replaced"]; exists && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return fmt.Errorf("operation result replaced must be a boolean when present")
	}
	decoded := OpResult(value)
	if err := decoded.Validate(); err != nil {
		return err
	}
	*r = decoded
	return nil
}

func (c Commit) Validate() error {
	if err := c.Revision.Validate(); err != nil {
		return err
	}
	if c.Results == nil || c.Warnings == nil {
		return fmt.Errorf("commit results and warnings must be arrays")
	}
	for i, r := range c.Results {
		if err := r.Validate(); err != nil {
			return fmt.Errorf("result %d: %w", i, err)
		}
	}
	for i, w := range c.Warnings {
		if err := w.Code.Validate(); err != nil {
			return fmt.Errorf("warning %d: %w", i, err)
		}
	}
	if c.LeaseTtlMs != nil {
		if err := c.LeaseTtlMs.Validate(); err != nil {
			return err
		}
		if c.LeaseTtlMs.IsZero() {
			return fmt.Errorf("lease TTL must be positive")
		}
	}
	return nil
}
func (c Commit) MarshalJSON() ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	type wire Commit
	return json.Marshal(wire(c))
}
func (c *Commit) UnmarshalJSON(data []byte) error {
	type wire Commit
	var value wire
	if err := decodeStrict(data, &value, "revision", "results", "warnings"); err != nil {
		return err
	}
	decoded := Commit(value)
	if err := decoded.Validate(); err != nil {
		return err
	}
	*c = decoded
	return nil
}

func (v Violation) MarshalJSON() ([]byte, error) {
	type wire Violation
	return json.Marshal(wire(v))
}
func (v *Violation) UnmarshalJSON(data []byte) error {
	type wire Violation
	var value wire
	if err := decodeStrict(data, &value, "guard", "message"); err != nil {
		return err
	}
	*v = Violation(value)
	return nil
}

// ControlEvent uses the same flat tagged envelope whether encoded through
// Event or directly. Details are never silently discarded by encoding/json.
func (c ControlEvent) MarshalJSON() ([]byte, error) {
	return (Event{Category: EventControl, Control: &c}).MarshalJSON()
}
func (c *ControlEvent) UnmarshalJSON(data []byte) error {
	var event Event
	if err := event.UnmarshalJSON(data); err != nil {
		return err
	}
	if event.Control == nil {
		return fmt.Errorf("expected control event")
	}
	*c = *event.Control
	return nil
}

func (e Event) Validate() error        { _, err := e.MarshalJSON(); return err }
func (p PrincipalRef) Validate() error { return validatePrincipal(p) }
func (c EventCause) Validate() error   { return validateEventCause(c) }

func (k OpKind) Validate() error {
	switch k {
	case OpCreate, OpPatch, OpClose, OpReparent, OpLink, OpUnlink, OpComment, OpAttach, OpDelete, OpSetMeta:
		return nil
	}
	return fmt.Errorf("unknown operation kind %q", k)
}
func (k OpKind) MarshalJSON() ([]byte, error) {
	if err := k.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(string(k))
}
func (k *OpKind) UnmarshalJSON(data []byte) error {
	var text string
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fmt.Errorf("null operation kind")
	}
	if err := decodeJSON(data, &text); err != nil {
		return err
	}
	value := OpKind(text)
	if err := value.Validate(); err != nil {
		return err
	}
	*k = value
	return nil
}

func (k ChangesetKind) Validate() error {
	switch k {
	case ChangesetWrite, ChangesetDerived, ChangesetRevert, ChangesetMigration, ChangesetRedact, ChangesetRestore:
		return nil
	}
	return fmt.Errorf("unknown changeset kind %q", k)
}
func (k ChangesetKind) MarshalJSON() ([]byte, error) {
	if err := k.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(string(k))
}
func (k *ChangesetKind) UnmarshalJSON(data []byte) error {
	var text string
	if err := decodeJSON(data, &text); err != nil {
		return err
	}
	value := ChangesetKind(text)
	if err := value.Validate(); err != nil {
		return err
	}
	*k = value
	return nil
}
