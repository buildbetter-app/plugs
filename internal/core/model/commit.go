package model

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type Commit struct {
	Revision Revision   `json:"revision"`
	Results  []OpResult `json:"results"`
	Warnings []Warning  `json:"warnings"`
	// Dry-run responses also carry these fields. They are omitted on a real
	// commit unless the caller requested a dry run.
	WouldCommit    bool        `json:"wouldCommit,omitempty"`
	Violations     []Violation `json:"violations,omitempty"`
	LeaseExpiresAt *time.Time  `json:"leaseExpiresAt,omitempty"`
	LeaseTtlMs     *Natural    `json:"leaseTtlMs,omitempty"`
}
type OpResult struct {
	Ref      Ref               `json:"ref"`
	Revision Revision          `json:"revision"`
	Record   json.RawMessage   `json:"record,omitempty"`
	SetBy    map[string]string `json:"setBy,omitempty"`
}
type Warning struct {
	Code    WarningCode    `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func (w Warning) MarshalJSON() ([]byte, error) {
	if !validWarningCode(w.Code) {
		return nil, fmt.Errorf("unknown warning code %q", w.Code)
	}
	type wireWarning Warning
	return json.Marshal(wireWarning(w))
}

func (w *Warning) UnmarshalJSON(data []byte) error {
	type wireWarning Warning
	var value wireWarning
	if err := decodeStrict(data, &value, "code", "message"); err != nil {
		return err
	}
	if !validWarningCode(value.Code) {
		return fmt.Errorf("unknown warning code %q", value.Code)
	}
	*w = Warning(value)
	return nil
}

type Violation struct {
	Guard   string         `json:"guard"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

// Writer is the only core entry point for graph writes. Named verbs lower to
// this same pipeline; tx.apply supplies ChangeSet directly.
type Writer interface {
	Apply(ctx context.Context, by PrincipalRef, cs ChangeSet, meta WriteMeta) (Commit, error)
}
type WriteMeta struct {
	// Method is the named operation that produced this write.
	Method string
	// TargetStatus is trusted named close/reopen lowering context, never a
	// generic patch capability. Typed tx operations carry their own selector.
	TargetStatus *string
	// GateAction and GateState/Details are trusted lowering context for the
	// canonical gate service. They are carried through the existing Writer
	// pipeline so idempotency lookup happens before the gate is evaluated.
	GateAction     string
	GateState      string
	GateDetails    map[string]any
	AgentLabel     string
	Project        string
	IdempotencyKey string
	// ClaimNextIdentity is the canonical queue selector identity. It is set
	// only by the claimNext domain seam and participates in the request hash so
	// a reused key cannot silently change view or filter parameters.
	ClaimNextIdentity string
	DryRun            bool
	Return            string // full or minimal
	Traceparent       string
	Agent             string
}
