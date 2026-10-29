package model

import "time"

type ChangesetKind string

const (
	ChangesetWrite     ChangesetKind = "write"
	ChangesetDerived   ChangesetKind = "derived"
	ChangesetRevert    ChangesetKind = "revert"
	ChangesetMigration ChangesetKind = "migration"
	ChangesetRedact    ChangesetKind = "redact"
	ChangesetRestore   ChangesetKind = "restore"
)

type Changeset struct {
	Revision       Revision       `json:"revision"`
	Branch         Branch         `json:"branch"`
	Incarnation    string         `json:"incarnation"`
	Kind           ChangesetKind  `json:"kind"`
	Time           time.Time      `json:"time"`
	Principal      PrincipalRef   `json:"principal"`
	Chain          []string       `json:"chain"`
	IdempotencyKey string         `json:"idempotencyKey,omitempty"`
	Mutators       []string       `json:"mutators,omitempty"`
	Changes        []ChangeRecord `json:"changes"`
	// Events is a complete ordered event plan, atomically stored with this row.
	// Nil means a legacy row: event indexes remain ChangeRecord indexes.
	Events []EventDescriptor `json:"events,omitempty"`
}
type ChangeRecord struct {
	Ref          Ref      `json:"ref"`
	Op           OpKind   `json:"op"`
	FromRevision Revision `json:"fromRevision"`
	ChangedPaths []string `json:"changedPaths"`
}
