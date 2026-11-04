// Package port contains the core-owned synchronous callback boundaries.
package port

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// WriteOutcome carries response-only execution information outside the durable
// Commit. Replayed means this invocation returned a full-scope stored receipt;
// equal revisions, unchanged writes and dry runs do not imply replay.
type WriteOutcome struct {
	Commit   model.Commit
	Replayed bool
	// NullResult marks the cataloged claimNext terminal outcome. Its durable
	// receipt still carries a commit revision, but the RPC result is JSON null.
	// Keeping this transient avoids conflating a real empty-result commit from
	// another named write with a terminal queue response.
	NullResult bool
}

// HeartbeatOutcome is the response-only result of a quiet lease refresh. A
// heartbeat never publishes a graph commit, version, event, or ordinary
// idempotency receipt; its replay bit belongs to the checkpoint-backed lease
// receipt and is therefore kept separate from WriteOutcome.
type HeartbeatOutcome struct {
	LeaseExpiresAt time.Time
	Replayed       bool
}

// OutcomeWriter is an additive host seam. model.Writer.Apply remains available
// for callers that do not need transport replay metadata.
type OutcomeWriter interface {
	ApplyWithOutcome(context.Context, model.PrincipalRef, model.ChangeSet, model.WriteMeta) (WriteOutcome, error)
}

// ClaimNextReadyProbe runs after a claimNext empty outcome has acquired the
// transaction head. It observes the ready set through that write transaction
// so a candidate becoming ready between the live walk and receipt commit can
// force the caller to restart its walk before a durable null is published.
type ClaimNextReadyProbe func(context.Context, driver.Reader) (bool, error)

// ErrClaimNextReady asks the claimNext service to release the transaction and
// restart from a fresh ready snapshot. It is never a wire-facing error.
var ErrClaimNextReady = errors.New("claimNext ready candidate observed")

// SnapshotReader has no write handle. Pre reads are pinned to BaseRevision
// and recorded in the commit guard, including missing records and queries.
// Post reads are pinned to the committed revision.
type SnapshotReader interface {
	Snapshot() driver.View
	Get(context.Context, []model.Ref) ([]driver.Record, error)
	Query(context.Context, driver.Query) (driver.Page[driver.Record], error)
	Observe(context.Context, []driver.LockKey) ([]driver.LockState, error)
}

// HierarchySnapshotReader is an optional trusted-core snapshot capability.
// Children and selected closure groups join the same Writer read set as Get
// and Observe, including empty membership, missing groups and exact row reads.
// Unsupported source coverage must refuse; a generic Query is not a fallback.
type HierarchySnapshotReader interface {
	SnapshotReader
	driver.HierarchyChildrenExcludingReader
	driver.InverseSourceLinksReader
	HierarchyClosureRows(context.Context, []model.Ref) ([]driver.DerivedRow, error)
}

// SelectionSnapshotReader is a future trusted-core capability. Implementors
// must track every consumed source/control and absence plus predicate scopes,
// preserving callback budgets. W3 defines this seam but enables no callers or
// forwarding through the Writer; an untracked driver selector is insufficient.
// Complete read authority is represented by driver.SelectionBoundary. Its
// constructor is a schema/ownership boundary, not permission to omit an
// installed host callback: the owning host must admit the plan from its
// actual immutable composition or return bounded check-only. Source/control
// witnesses and the view revision remain part of the final publication fence.
type SelectionSnapshotReader interface {
	SnapshotReader
	driver.SelectionReader
}

type Change struct {
	Resource model.ResourceRef
	Op       model.Op
	Before   json.RawMessage
	After    json.RawMessage
	SetBy    map[string]string
}

type ProposedChange struct {
	RequestID    string
	Principal    model.PrincipalRef
	BaseRevision model.Revision
	DryRun       bool
	// SameClaim is core-proven: one empty claim patch, the same live holder
	// and anonymous agent label. It preserves the lease, with no transition.
	SameClaim bool
	Meta      model.WriteMeta
	Changes   []Change
	// Expect carries canonical precondition targets into the authorization
	// boundary. A stale result stays private until those targets are allowed.
	Expect []model.Precondition
}

type CommittedChange struct {
	RequestID string
	Principal model.PrincipalRef
	Revision  model.Revision
	Meta      model.WriteMeta
	Changes   []Change
}

// Paths use fields.<name>. A null value clears that field. A patch may only
// target an already proposed, live record and a declared owned field.
type Patch struct {
	Resource model.ResourceRef
	Paths    map[string]json.RawMessage
}

type Verdict struct {
	Allow      bool
	Violations []model.Violation
}

type Mutator interface {
	Mutate(context.Context, ProposedChange, SnapshotReader) ([]Patch, error)
}

// TimedMutator lets a host that owns an execution pool start the callback
// budget after it acquires a runtime. Ordinary mutators remain wrapped by the
// pipeline's legacy invoke boundary.
type TimedMutator interface {
	MutateWithTimeout(context.Context, time.Duration, ProposedChange, SnapshotReader) ([]Patch, error)
}

type Validator interface {
	Validate(context.Context, ProposedChange, SnapshotReader) (Verdict, error)
}

// TimedValidator is the validator counterpart to TimedMutator.
type TimedValidator interface {
	ValidateWithTimeout(context.Context, time.Duration, ProposedChange, SnapshotReader) (Verdict, error)
}

type PostSubscriber interface {
	After(context.Context, CommittedChange, SnapshotReader) error
}

// TimedPostSubscriber lets a pooled host keep queue wait outside its callback
// execution budget while retaining the normal post callback bound.
type TimedPostSubscriber interface {
	AfterWithTimeout(context.Context, time.Duration, CommittedChange, SnapshotReader) error
}

// Ontology supplies defaults and named slot selection, then validates the
// complete post-image and fixed guards. Its reads join the same commit guard.
// A service must not validate transitions before Apply's idempotency lookup.
type Ontology interface {
	Normalize(context.Context, ProposedChange, SnapshotReader) ([]Patch, error)
	Validate(context.Context, ProposedChange, SnapshotReader) error
}

// ConditionalDecision interprets both blocker images in the blocker's own
// project ontology. The pin identifies the protected source read through the
// supplied tracked snapshot, including its effective compilation digest.
type ConditionalDecision struct {
	Ontology model.OntologyPin
	Before   model.ConditionDecision
	After    model.ConditionDecision
}

// OntologyAudit is an optional trusted extension, separate from existing
// Ontology implementations. It classifies final images; it never discovers
// edges, constructs descriptors, or writes storage. Unsupported source/pinning
// configurations must return an error rather than inventing an interpretation.
type OntologyAudit interface {
	DecideConditional(context.Context, Change, SnapshotReader) (ConditionalDecision, error)
}

// EventContext is trusted host provenance, never ChangeSet input. Without a
// source the Writer uses a new root request and an empty authorization chain;
// an installed authorizer requires a source so its chain is not fabricated.
type EventContext struct {
	Cause model.EventCause
	Chain []string
}

type EventContextSource interface {
	EventContext(context.Context, ProposedChange, SnapshotReader) (EventContext, error)
}

type AuthorizationStage string

const (
	AuthorizeBefore AuthorizationStage = "before"
	AuthorizeAfter  AuthorizationStage = "after"
	AuthorizeReplay AuthorizationStage = "replay"
)

// Authorizer is optional. With none installed the pipeline allows the caller.
// Replay is checked against current facts without rerunning pre/post callbacks.
type Authorizer interface {
	Authorize(context.Context, AuthorizationStage, ProposedChange, SnapshotReader) error
}
