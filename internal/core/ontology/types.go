// Package ontology compiles project workflows and evaluates their fixed rules.
// It performs no I/O. Callers supply observations from their tracked snapshot
// and recheck graph facts in the write transaction.
package ontology

import "github.com/buildbetter-app/plugs/internal/core/model"

type Category string

const (
	Triage    Category = "triage"
	Backlog   Category = "backlog"
	Unstarted Category = "unstarted"
	Started   Category = "started"
	Completed Category = "completed"
	Canceled  Category = "canceled"
	// Native Beads categories retain their original wire spelling. Their
	// behavior is shared with the corresponding kernel category, never inferred
	// from a status name.
	Active Category = "active"
	WIP    Category = "wip"
	Done   Category = "done"
	Frozen Category = "frozen"
)

func (c Category) Terminal() bool      { return c.Completed() || c == Canceled }
func (c Category) ReadyEligible() bool { return c == Unstarted || c == Active }
func (c Category) Started() bool       { return c == Started || c == WIP }
func (c Category) Completed() bool     { return c == Completed || c == Done }
func (c Category) Backlog() bool       { return c == Backlog || c == Frozen }

type Outcome string

const (
	Success Outcome = "success"
	Failure Outcome = "failure"
)

type Slot string

const (
	Claim  Slot = "claim"
	Close  Slot = "close"
	Reopen Slot = "reopen"
	Fail   Slot = "fail"
	Cancel Slot = "cancel"
	Defer  Slot = "defer"
)

type GuardKind string

const (
	FieldsRequired   GuardKind = "fields_required"
	EvidenceRequired GuardKind = "evidence_required"
	NoOpenChildren   GuardKind = "no_open_children"
	NoOpenBlockers   GuardKind = "no_open_blockers"
	NotTemplate      GuardKind = "not_template"
	GatesResolved    GuardKind = "gates_resolved"
	Permission       GuardKind = "permission"
)

// Document definitions are replaced whole by key, never recursively merged.
// Fixed v1 templates, priorities, dependencies and role policy are intentionally
// absent: Decode refuses attempts to configure those kernel-owned definitions.
type Document struct {
	Statuses  map[string]Status   `json:"statuses,omitempty"`
	Workflows map[string]Workflow `json:"workflows,omitempty"`
	Types     map[string]Type     `json:"types,omitempty"`
	GateKinds map[string]GateKind `json:"gate_kinds,omitempty"`
}

type Status struct {
	Name     string   `json:"name"`
	Category Category `json:"category"`
	Outcome  Outcome  `json:"outcome,omitempty"`
	Archived bool     `json:"archived,omitempty"`
}

type Workflow struct {
	Statuses    []string        `json:"statuses,omitempty"`
	Initial     string          `json:"initial,omitempty"`
	Slots       map[Slot]string `json:"slots,omitempty"`
	Transitions []Rule          `json:"transitions,omitempty"`
	Archived    bool            `json:"archived,omitempty"`
}

type Rule struct {
	From   []string `json:"from,omitempty"` // omitted means any different workflow status
	To     string   `json:"to"`
	Guards []Guard  `json:"guards,omitempty"`
}

type Guard struct {
	Kind   GuardKind `json:"kind"`
	Fields []string  `json:"fields,omitempty"`
	Kinds  []string  `json:"kinds,omitempty"` // evidence or gate kinds
}

type Type struct {
	Name          string           `json:"name"`
	Workflow      string           `json:"workflow"`
	Fields        map[string]Field `json:"fields,omitempty"`
	ReadyEligible *bool            `json:"ready_eligible,omitempty"` // nil infers eligibility from category
	Archived      bool             `json:"archived,omitempty"`
}

// GateKind describes the resolver and counting policy for a graph gate. The
// resolver is intentionally an opaque plugin identity except for the built-in
// manual resolver, whose approval semantics are implemented by core. Params
// is open so a resolver can retain its own configuration; core consumes only
// min_approvals for manual gates.
type GateKind struct {
	Resolver  string         `json:"resolver"`
	Params    map[string]any `json:"params,omitempty"`
	Count     string         `json:"count,omitempty"` // humans or principals
	Timeout   string         `json:"timeout,omitempty"`
	OnTimeout string         `json:"on_timeout,omitempty"`
	Archived  bool           `json:"archived,omitempty"`
}

// Field is a deliberately small typed schema, not an executable expression.
type Field struct {
	Type     string   `json:"type"` // string, integer, number, boolean, enum, strings
	Values   []string `json:"values,omitempty"`
	Required bool     `json:"required,omitempty"`
	Default  any      `json:"default,omitempty"`
	Owner    string   `json:"owner,omitempty"` // installed mutator identity
}

// PluginCatalog names available field owners; compilation marks a type with
// an unavailable owner degraded, so runtime writes fail closed.
type PluginCatalog map[string]bool

type Problem struct {
	Code    model.ErrorCode `json:"code"`
	Path    string          `json:"path"`
	Message string          `json:"message"`
}

type Violation struct {
	Code    model.ErrorCode `json:"code"`
	Field   string          `json:"field,omitempty"`
	Guard   GuardKind       `json:"guard,omitempty"`
	Message string          `json:"message"`
}

// BeadState is an ontology view of a record, not a second storage model.
// Metadata is deliberately separate and never accepted as a typed field.
// BeadState is the ontology view of a record. Fields preserves raw priority and
// template alongside custom typed fields; Status is extracted by the adapter.
type BeadState struct {
	ID       string         `json:"id,omitempty"`
	Project  string         `json:"project,omitempty"`
	Type     string         `json:"type"`
	Status   string         `json:"status"`
	Template bool           `json:"template,omitempty"`
	Fields   map[string]any `json:"fields,omitempty"`
}

// GuardFacts are authoritative observations, not client-provided arguments.
// Nil means unobserved and fails a guard that needs the fact. Evidence and
// Gates map kind -> observed satisfied; a missing kind is unobserved/unsatisfied.
// The permission result must be for action transition and the target status.
type GuardFacts struct {
	// TargetStatus comes only from a typed create/close operation or trusted
	// named lifecycle lowering, never from a generic fields.status patch.
	TargetStatus   *string
	NoOpenChildren *bool
	NoOpenBlockers *bool
	Evidence       map[string]bool
	Gates          map[string]bool
	AuthzInstalled bool
	Permission     *bool
	SetBy          map[string]string // fields.<key> -> mutator identity
	Touched        model.FieldSet    // trusted operation paths, including same-value status patches
}

type Effective struct {
	document Document
	degraded map[string]bool
}

type Snapshot struct {
	base     Effective
	projects map[string]Effective
	digest   string
}

// Effective falls back to the server base for projects without an overlay.
func (s Snapshot) Effective(project string) Effective {
	if e, ok := s.projects[project]; ok {
		return e
	}
	return s.base
}

// Digest identifies compilation inputs; it is not a protected apply permit.
func (s Snapshot) Digest() string { return s.digest }

func (e Effective) Status(key string) (Status, bool) {
	s, ok := e.document.Statuses[key]
	return s, ok
}

func (e Effective) Category(status string) (Category, bool) {
	s, ok := e.Status(status)
	return s.Category, ok
}

// SatisfiesBlocks evaluates a blocker in its own project's effective ontology.
// Archival does not rewrite a stored status's category or terminal outcome.
func (e Effective) SatisfiesBlocks(status string) bool {
	s, ok := e.Status(status)
	return ok && s.Category.Terminal()
}

func (e Effective) SatisfiesConditionalBlocks(status string) bool {
	s, ok := e.Status(status)
	return ok && s.Category == Completed && s.Outcome == Failure
}

// Document returns a detached description. Mutating it cannot change checks or
// the digest of an already compiled snapshot.
func (e Effective) Document() Document {
	doc, _ := cloneDocument(e.document)
	return doc
}

// ReadyEligible supplies the status component of readiness. The service still
// evaluates blockers, leases and its other readiness predicates.
func (e Effective) ReadyEligible(typ, status string) bool {
	t, ok := e.document.Types[typ]
	if !ok || t.Archived || e.degraded[typ] || (t.ReadyEligible != nil && !*t.ReadyEligible) {
		return false
	}
	w := e.document.Workflows[t.Workflow]
	s, ok := e.Status(status)
	return ok && !s.Archived && !w.Archived && contains(w.Statuses, status) && s.Category.ReadyEligible()
}

func contains[T comparable](values []T, value T) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}
