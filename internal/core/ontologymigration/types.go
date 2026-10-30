// Package ontologymigration builds read-only, digest-bound ontology migration
// plans. It has no storage transaction, apply, authorization, or locking API.
package ontologymigration

import (
	"context"
	"encoding/json"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
)

const MaxAffectedRows = 5000

type Kind string

const (
	MapStatus    Kind = "map_status"
	MapType      Kind = "map_type"
	SetField     Kind = "set_field"
	ConvertField Kind = "convert_field"
	DropField    Kind = "drop_field"
	Grandfather  Kind = "grandfather"
)

// Target selects original row images, before any directives run. Project and
// Type are mandatory; an empty ID selects all rows of that project and type.
// Overlapping writes to the same row member are refused, even if identical.
type Target struct {
	Project string `json:"project"`
	Type    string `json:"type"`
	ID      string `json:"id,omitempty"`
}

// Directive is an internal typed contract, not an expansion of ontology.plan's
// wire schema. Each kind rejects irrelevant members. Grandfather selects one
// exact violation ID on one exact row and requires a recorded reason.
type Directive struct {
	ID         string `json:"id"`
	Kind       Kind   `json:"kind"`
	Target     Target `json:"target"`
	From       string `json:"from,omitempty"`
	To         string `json:"to,omitempty"`
	Field      string `json:"field,omitempty"` // bare custom field key
	Value      any    `json:"value,omitempty"`
	Conversion string `json:"conversion,omitempty"`
	Violation  string `json:"violation,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

type Reference struct {
	Revision model.Revision `json:"revision"`
	Digest   string         `json:"digest"`
}

type Version struct {
	Revision model.Revision
	Snapshot ontology.Snapshot
}

type Request struct {
	Current    Version
	Candidate  Version
	Directives []Directive
}

// Row preserves the complete ontology view, including unknown custom fields
// and raw fixed priority/template values. The adapter must not project away
// invalid fields. Fields, ID, project, type, status and revision are required.
type Row struct {
	Revision model.Revision     `json:"revision"`
	State    ontology.BeadState `json:"state"`
}

// Preserve an explicitly observed empty fields object across copy boundaries;
// BeadState's public description otherwise omits empty maps.
func (row Row) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Revision model.Revision `json:"revision"`
		State    struct {
			ontology.BeadState
			Fields map[string]any `json:"fields"`
		} `json:"state"`
	}{row.Revision, struct {
		ontology.BeadState
		Fields map[string]any `json:"fields"`
	}{row.State, row.State.Fields}})
}

// Inventory is an independently obtained count and content digest of ALL bead
// row images at one storage snapshot, not a claim that a filtered list is valid.
// Scan must use exactly that snapshot. Build checks count, digest, identities,
// revisions and ontology provenance; omission, duplicates and mixed snapshots
// refuse. A real adapter must obtain this receipt from its authoritative read.
type Inventory struct {
	DataRevision model.Revision `json:"dataRevision"`
	Ontology     Reference      `json:"ontology"`
	Count        model.Natural  `json:"count"`
	RowsDigest   string         `json:"rowsDigest"`
}

type RowSource interface {
	Inventory(context.Context) (Inventory, error)
	Scan(context.Context, Inventory, func(Row) error) error
}

// GuardSource is needed only when affected rows have target guards requiring
// graph/permission observations. Observations must describe the final proposal
// at Inventory's snapshot. Missing facts refuse; an observed false guard is a
// violation. This optional read-only extension never receives a WriteTx.
type GuardSource interface {
	Observe(context.Context, Inventory, Change) (Observation, error)
}

type Observation struct {
	Candidate Reference           `json:"candidate"`
	RowDigest string              `json:"rowDigest"`
	Facts     ontology.GuardFacts `json:"facts"`
	// Permission guards require an explicitly observed posture, including
	// an affirmative observation that no authorizer is installed. It must
	// agree with Facts.AuthzInstalled; a zero-valued Facts cannot allow.
	AuthzInstalled *bool `json:"authzInstalled,omitempty"`
}

// Binding identifies an actual bound validating pre callback. Digest identifies
// its immutable implementation/configuration. Empty Project means all projects;
// Type is mandatory. This list comes from the trusted host, not an RPC caller.
type Binding struct {
	ID      string `json:"id"`
	Project string `json:"project,omitempty"`
	Type    string `json:"type"`
	Digest  string `json:"digest"`
}

// PreInventory must include an explicit non-nil bindings list, even when empty.
// Its digest commits to the complete candidate binding manifest. No previously
// asserted validation result is accepted: Build invokes Validate itself.
type PreInventory struct {
	Candidate Reference `json:"candidate"`
	Bindings  []Binding `json:"bindings"`
	Digest    string    `json:"digest"`
}

type ValidatingPre interface {
	Inventory(context.Context, Reference) (PreInventory, error)
	Validate(context.Context, Binding, Change) ([]ontology.Violation, error)
}

type Change struct {
	Candidate Reference `json:"candidate"`
	Before    Row       `json:"before"`
	After     Row       `json:"after"`
}

type Refusal struct {
	Code      model.ErrorCode `json:"code"`
	Reason    string          `json:"reason,omitempty"`
	Row       string          `json:"row,omitempty"`
	Directive string          `json:"directive,omitempty"`
	Message   string          `json:"message"`
}

type Finding struct {
	ID          string             `json:"id"`
	Violation   ontology.Violation `json:"violation"`
	Subscriber  string             `json:"subscriber,omitempty"`
	GuardDigest string             `json:"guardDigest,omitempty"`
	Disposition string             `json:"disposition,omitempty"` // grandfather directive ID
}

type PreRun struct {
	Binding    string               `json:"binding"`
	Violations []ontology.Violation `json:"violations"`
}

type RowPlan struct {
	Before       Row             `json:"before"`
	After        Row             `json:"after"`
	BeforeStatus ontology.Status `json:"beforeStatus"`
	AfterStatus  ontology.Status `json:"afterStatus"`
	Affected     bool            `json:"affected"`
	Directives   []string        `json:"directives"`
	Defaults     []string        `json:"defaults"`
	Findings     []Finding       `json:"findings"`
	PreRuns      []PreRun        `json:"preRuns"`
	Observation  *Observation    `json:"observation,omitempty"`
}

type Report struct {
	Format             string                   `json:"format"`
	PlanDigest         string                   `json:"planDigest"`
	Current            Reference                `json:"current"`
	Candidate          Reference                `json:"candidate"`
	Compilation        ontology.CompilationPlan `json:"compilation"`
	Inventory          Inventory                `json:"inventory"`
	ObservedRowsDigest string                   `json:"observedRowsDigest"`
	ObservedRowCount   int                      `json:"observedRowCount"`
	PreInventory       PreInventory             `json:"preInventory"`
	Directives         []Directive              `json:"directives"`
	Rows               []RowPlan                `json:"rows"`
	Affected           int                      `json:"affected"`
	Grandfathered      int                      `json:"grandfathered"` // violations, not rows
	Refusals           []Refusal                `json:"refusals"`
}

// Plan owns detached input and result images. Report returns a fresh deep copy;
// callback inputs and caller edits cannot change an already built plan.
type Plan struct{ report Report }

func (p Plan) Digest() string { return p.report.PlanDigest }

func (p Plan) Ready() bool {
	if p.Digest() == "" || len(p.report.Refusals) != 0 {
		return false
	}
	for _, row := range p.report.Rows {
		for _, finding := range row.Findings {
			if finding.Disposition == "" {
				return false
			}
		}
	}
	return true
}

func (p Plan) Report() Report {
	copy, _ := detached(p.report) // Only already encodable owned data is stored.
	return copy
}
