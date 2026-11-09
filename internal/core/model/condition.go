package model

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"unicode/utf8"
)

// AuditResource identifies a resource whose CURRENT read policy protects the
// descriptor. Audit identities are resolved, nonempty ID refs on branch zero;
// display keys and caller-supplied unresolved references are not log identities.
type AuditResource struct {
	Kind string `json:"kind"`
	Ref  Ref    `json:"ref"`
}

type EventImage struct {
	Ref      Ref      `json:"ref"`
	Revision Revision `json:"revision"`
}

// ConditionalEdge is a directed edge stored in the dependent's source image.
// (Dependent, Blocker, Kind) is its identity; Revision pins its membership.
type ConditionalEdge struct {
	Dependent Ref      `json:"dependent"`
	Blocker   Ref      `json:"blocker"`
	Kind      string   `json:"kind"`
	Revision  Revision `json:"revision"`
}

// OntologyPin names the protected ontology document containing the base and
// overlays. Digest pins the effective compilation used for this project. A nil
// Project explicitly means the server base (a projectless blocker).
type OntologyPin struct {
	Ref      Ref      `json:"ref"`
	Revision Revision `json:"revision"`
	Digest   string   `json:"digest"`
	Project  *Ref     `json:"project,omitempty"`
}

// ConditionDecision stores the resolved interpretation, never a live lookup.
// Canceled has no outcome; completed has success/failure. Only completed/failure
// satisfies conditional-blocks. Terminal and Satisfied are checked assertions.
type ConditionDecision struct {
	Status    string `json:"status"`
	Category  string `json:"category"`
	Outcome   string `json:"outcome"`
	Terminal  bool   `json:"terminal"`
	Satisfied bool   `json:"satisfied"`
}

type ConditionUnmetDetails struct {
	Dependent     EventImage        `json:"dependent"`
	BlockerBefore EventImage        `json:"blockerBefore"`
	BlockerAfter  EventImage        `json:"blockerAfter"`
	Edge          ConditionalEdge   `json:"edge"`
	Ontology      OntologyPin       `json:"ontology"`
	Before        ConditionDecision `json:"before"`
	After         ConditionDecision `json:"after"`
}

const maxAuditText = 1024

func auditText(value, name string) error {
	if value == "" || !utf8.ValidString(value) || utf8.RuneCountInString(value) > maxAuditText {
		return fmt.Errorf("%s must contain 1..%d Unicode characters", name, maxAuditText)
	}
	return nil
}

func auditRef(ref Ref) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	if !ref.IsID() || !ref.Branch.IsZero() {
		return fmt.Errorf("audit reference must be a resolved branch-zero ID")
	}
	return auditText(ref.ID, "audit ID")
}

func sameAuditRef(a, b Ref) bool {
	return a.ID == b.ID && a.IsID() && b.IsID() && a.Branch.IsZero() && b.Branch.IsZero()
}

func (r AuditResource) Validate() error {
	switch r.Kind {
	case "bead", "ontology", "plugin", "view", "memory", "sync", "derived", "project":
	default:
		return fmt.Errorf("unknown audit resource kind %q", r.Kind)
	}
	return auditRef(r.Ref)
}
func (r AuditResource) MarshalJSON() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	type wire AuditResource
	return json.Marshal(wire(r))
}
func (r *AuditResource) UnmarshalJSON(data []byte) error {
	type wire AuditResource
	var v wire
	if err := decodeStrict(data, &v, "kind", "ref"); err != nil {
		return err
	}
	d := AuditResource(v)
	if err := d.Validate(); err != nil {
		return err
	}
	*r = d
	return nil
}

func (i EventImage) Validate() error {
	if err := auditRef(i.Ref); err != nil {
		return err
	}
	return i.Revision.Validate()
}
func (i EventImage) MarshalJSON() ([]byte, error) {
	if err := i.Validate(); err != nil {
		return nil, err
	}
	type wire EventImage
	return json.Marshal(wire(i))
}
func (i *EventImage) UnmarshalJSON(data []byte) error {
	type wire EventImage
	var v wire
	if err := decodeStrict(data, &v, "ref", "revision"); err != nil {
		return err
	}
	d := EventImage(v)
	if err := d.Validate(); err != nil {
		return err
	}
	*i = d
	return nil
}

func (e ConditionalEdge) Validate() error {
	if err := auditRef(e.Dependent); err != nil {
		return err
	}
	if err := auditRef(e.Blocker); err != nil {
		return err
	}
	if sameAuditRef(e.Dependent, e.Blocker) || e.Kind != "conditional-blocks" {
		return fmt.Errorf("invalid conditional edge")
	}
	return e.Revision.Validate()
}
func (e ConditionalEdge) MarshalJSON() ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	type wire ConditionalEdge
	return json.Marshal(wire(e))
}
func (e *ConditionalEdge) UnmarshalJSON(data []byte) error {
	type wire ConditionalEdge
	var v wire
	if err := decodeStrict(data, &v, "dependent", "blocker", "kind", "revision"); err != nil {
		return err
	}
	d := ConditionalEdge(v)
	if err := d.Validate(); err != nil {
		return err
	}
	*e = d
	return nil
}

func (p OntologyPin) Validate() error {
	if err := auditRef(p.Ref); err != nil {
		return err
	}
	if err := p.Revision.Validate(); err != nil {
		return err
	}
	if p.Project != nil {
		if err := auditRef(*p.Project); err != nil {
			return err
		}
	}
	digest, err := hex.DecodeString(p.Digest)
	if err != nil || len(digest) != 32 || hex.EncodeToString(digest) != p.Digest {
		return fmt.Errorf("ontology digest must be canonical lowercase SHA-256 hex")
	}
	return nil
}
func (p OntologyPin) MarshalJSON() ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	type wire OntologyPin
	return json.Marshal(wire(p))
}
func (p *OntologyPin) UnmarshalJSON(data []byte) error {
	type wire OntologyPin
	var v wire
	if err := decodeStrict(data, &v, "ref", "revision", "digest"); err != nil {
		return err
	}
	d := OntologyPin(v)
	if err := d.Validate(); err != nil {
		return err
	}
	*p = d
	return nil
}

func (d ConditionDecision) Validate() error {
	if err := auditText(d.Status, "status"); err != nil {
		return err
	}
	terminal, satisfied := false, false
	switch d.Category {
	case "triage", "backlog", "unstarted", "started", "active", "wip", "frozen":
		if d.Outcome != "" {
			return fmt.Errorf("nonterminal status has an outcome")
		}
	case "canceled":
		terminal = true
		if d.Outcome != "" {
			return fmt.Errorf("canceled status has an outcome")
		}
	case "completed", "done":
		terminal = true
		if d.Outcome != "success" && d.Outcome != "failure" {
			return fmt.Errorf("completed status needs success or failure outcome")
		}
		if d.Category == "done" && d.Outcome != "success" {
			return fmt.Errorf("native done category requires success")
		}
		satisfied = d.Outcome == "failure"
	default:
		return fmt.Errorf("unknown status category %q", d.Category)
	}
	if d.Terminal != terminal || d.Satisfied != satisfied {
		return fmt.Errorf("inconsistent terminal/condition decision")
	}
	return nil
}
func (d ConditionDecision) MarshalJSON() ([]byte, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	type wire ConditionDecision
	return json.Marshal(wire(d))
}
func (d *ConditionDecision) UnmarshalJSON(data []byte) error {
	type wire ConditionDecision
	var v wire
	if err := decodeStrict(data, &v, "status", "category", "outcome", "terminal", "satisfied"); err != nil {
		return err
	}
	decoded := ConditionDecision(v)
	if err := decoded.Validate(); err != nil {
		return err
	}
	*d = decoded
	return nil
}

func (d ConditionUnmetDetails) Validate() error {
	for _, image := range []EventImage{d.Dependent, d.BlockerBefore, d.BlockerAfter} {
		if err := image.Validate(); err != nil {
			return err
		}
	}
	if err := d.Edge.Validate(); err != nil {
		return err
	}
	if err := d.Ontology.Validate(); err != nil {
		return err
	}
	if err := d.Before.Validate(); err != nil {
		return err
	}
	if err := d.After.Validate(); err != nil {
		return err
	}
	if !sameAuditRef(d.BlockerBefore.Ref, d.BlockerAfter.Ref) || !sameAuditRef(d.Edge.Dependent, d.Dependent.Ref) || !sameAuditRef(d.Edge.Blocker, d.BlockerAfter.Ref) || d.Edge.Revision.String() != d.Dependent.Revision.String() {
		return fmt.Errorf("conditional edge/source image identity mismatch")
	}
	if d.Before.Status == d.After.Status || !d.After.Terminal || d.After.Satisfied {
		return fmt.Errorf("condition-unmet requires a changed status entering an unsatisfied terminal outcome")
	}
	if compareAuditRevision(d.BlockerBefore.Revision, d.BlockerAfter.Revision) >= 0 {
		return fmt.Errorf("condition-unmet requires a later blocker image")
	}
	if compareAuditRevision(d.Dependent.Revision, d.BlockerAfter.Revision) > 0 || compareAuditRevision(d.Ontology.Revision, d.BlockerAfter.Revision) > 0 {
		return fmt.Errorf("condition source image is from a future revision")
	}
	return nil
}

// Inputs have already passed Natural.Validate. Compare symbolic magnitudes;
// even 1e1000000000 never expands into a billion-digit allocation.
func compareAuditRevision(a, b Revision) int {
	left, right := a.String(), b.String()
	if left == right {
		return 0
	}
	if left == "0" {
		return -1
	}
	if right == "0" {
		return 1
	}
	magnitude := func(value string) (string, *big.Int) {
		coefficient, exponent, _ := strings.Cut(value, "e")
		size := new(big.Int)
		if exponent != "" {
			size.SetString(exponent, 10)
		}
		size.Add(size, big.NewInt(int64(len(coefficient))))
		return coefficient, size
	}
	l, lm := magnitude(left)
	r, rm := magnitude(right)
	if cmp := lm.Cmp(rm); cmp != 0 {
		return cmp
	}
	for i := 0; i < max(len(l), len(r)); i++ {
		lc, rc := byte('0'), byte('0')
		if i < len(l) {
			lc = l[i]
		}
		if i < len(r) {
			rc = r[i]
		}
		if lc < rc {
			return -1
		}
		if lc > rc {
			return 1
		}
	}
	return 0
}
func (d ConditionUnmetDetails) MarshalJSON() ([]byte, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	type wire ConditionUnmetDetails
	return json.Marshal(wire(d))
}
func (d *ConditionUnmetDetails) UnmarshalJSON(data []byte) error {
	type wire ConditionUnmetDetails
	var v wire
	if err := decodeStrict(data, &v, "dependent", "blockerBefore", "blockerAfter", "edge", "ontology", "before", "after"); err != nil {
		return err
	}
	decoded := ConditionUnmetDetails(v)
	if err := decoded.Validate(); err != nil {
		return err
	}
	*d = decoded
	return nil
}
