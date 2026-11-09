// Package viewops executes the bounded, read-only A7 view surface.
//
// The package owns no storage transaction and no authorization policy. The
// composition root supplies a read-only store, the active lease, the current
// read authorizer, and (optionally) a saved-view reader.
package viewops

import (
	"context"
	"time"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const (
	// OrdinaryCandidateCap is the maximum number of candidates that remain
	// after current read authorization for an ordinary view.
	OrdinaryCandidateCap = 20_000
	// TypeSummaryCandidateCap is the narrow #20 exception. It applies only to
	// an explicitly present fields=[] type-count summary.
	TypeSummaryCandidateCap = 100_000
	MaxGroups               = 100
	DefaultOutputBudget     = 100
)

// ReadStore is deliberately narrower than driver.Store. View execution never
// receives a write transaction.
type ReadStore interface {
	Meta(context.Context) (driver.Meta, error)
	Read(context.Context, driver.View) (driver.Reader, error)
}

// AuthorizeRead is the core read-authorization seam. A nil callback means the
// host has no authorization plugin and all authenticated reads are allowed.
// A forbidden/not_found model error denies only that candidate; all other
// errors fail closed and abort the view. The project argument is the exact
// stored body.project value, never an inferred parent or caller default.
type AuthorizeRead func(context.Context, model.PrincipalRef, model.ResourceRef, string) error

// AuthorizeReadBatch is an optional trusted count adapter for the same current
// read policy as AuthorizeRead. It returns one decision per input, in order,
// and must fail closed on unavailable authority. Historical content must not
// select historical authority. No decision may survive this call.
type AuthorizeReadBatch func(context.Context, model.PrincipalRef, []model.ResourceRef) ([]bool, error)

// ViewReader resolves saved definitions for read-only execution. Save/delete
// are intentionally not part of this package until their protected write path
// is implemented.
type ViewReader interface {
	GetView(context.Context, model.PrincipalRef, string) (Definition, error)
}

// ReadinessState is the small derived-row projection consumed by a view. The
// reader receives the same pinned snapshot used for the view scan, so a
// pending fan-out can never make membership stale or reorder a page.
type ReadinessState struct {
	Pending bool
}

type ReadinessReader interface {
	Pending(context.Context, driver.Reader, []model.Ref) (map[string]ReadinessState, error)
}

// ReadinessSource is an expressive alias for composition roots that expose
// the same pinned-snapshot port under a source-oriented name.
type ReadinessSource = ReadinessReader

// DirectBlockerState is the producer's three-valued direct-edge verdict.
// Unknown is deliberately distinct from an active unsatisfied blocker.
type DirectBlockerState string

const (
	DirectBlockerSatisfied   DirectBlockerState = "satisfied"
	DirectBlockerUnsatisfied DirectBlockerState = "unsatisfied"
	DirectBlockerUnknown     DirectBlockerState = "unknown"
)

// DirectBlockerEdge is the redacted, consumer-facing projection of one
// producer edge. Project and raw status are intentionally absent.
type DirectBlockerEdge struct {
	TargetKind model.LinkTargetKind `json:"targetKind,omitempty"`
	Blocker    model.Ref            `json:"blocker"`
	Kind       string               `json:"kind"`
	State      DirectBlockerState   `json:"state"`
	Reason     string               `json:"reason,omitempty"`
}

// DirectBlockerVerdict is the bounded verdict returned by a configured host
// adapter. The adapter never authorizes targets; viewops does that below.
type DirectBlockerVerdict struct {
	Edges []DirectBlockerEdge
}

// DirectBlockerSource evaluates direct edges against the reader already pinned
// by view.explain. The limit is fixed by the consumer and is not RPC input.
type DirectBlockerSource func(context.Context, driver.Reader, model.Ref, int) (DirectBlockerVerdict, error)

const DirectBlockerExplainLimit = 100

// SelectionConfig binds the complete current chain on every Run. A separate
// AuthorizeRead callback is an additional host restriction and forces the exact
// ordinary fallback; it can never be certified by this chain-only adapter.
type SelectionConfig struct {
	Bind          func(context.Context, model.PrincipalRef, string) (*SelectionAuthority, error)
	AuthorizeRead AuthorizeRead
}

// SelectionAuthority lives for one request. Reader is CURRENT authority, while
// payloads and membership use the separately pinned content reader. A nil
// Boundary explicitly means check_only. Check and Fence remain mandatory even
// for unavailable acceleration, empty results and historical content.
type SelectionAuthority struct {
	Reader   driver.Reader
	Boundary *driver.SelectionBoundary
	Binding  string
	Check    func(context.Context, []model.Ref) ([]bool, error)
	Fence    func(context.Context) error
}

type Config struct {
	Selection          *SelectionConfig
	AuthorizeRead      AuthorizeRead
	AuthorizeReadBatch AuthorizeReadBatch
	Views              ViewReader
	Readiness          ReadinessReader
	DirectBlockers     DirectBlockerSource
	// StaleOntologySource selects the configured protected ontology from the
	// same pinned reader as an age query. If configured, missing/invalid source
	// refuses and never falls back to the process-local resolver.
	StaleOntologySource service.OntologySourceLocator
	// StaleDefaultOntology is the exact ontology instance used by the Writer
	// and Service on a no-plugin host. Only its immutable built-in default mode
	// may serve a stale query without a protected, pinned source.
	StaleDefaultOntology *service.Ontology
	// Now is sampled once for the first page of an age query. Continuations
	// reuse its signed cutoff; nil uses the production wall clock.
	Now              func() time.Time
	ProjectionStatus coreevents.ProjectionStatusReader
	// ProjectionBinding is the durable checkpoint binding whose status is
	// surfaced with view results and view.watch frames. "core" is the stable
	// default used by the projection composition root.
	ProjectionBinding string
	OntologyRevision  model.Revision
	// PolicyVersion is the host's current authorization generation. It is
	// carried into watch cursors; an empty value is valid for an immutable
	// no-plugin installation.
	PolicyVersion string
	// EpicProgress is a trusted, optional host capability. The ordinary view
	// and hierarchy-write authorization boundaries are unchanged when absent.
	EpicProgress *EpicProgressConfig
	// ParentProjection applies current authority to ordinary item/group parents.
	ParentProjection *ParentProjectionConfig
}

// EpicProgressConfig supplies current authority over a complete, pinned direct
// child set. AuthorizeScope must require read access to the parent (returning
// not_found for a hidden parent) and return only readable child refs, checked
// in each child's own project. It must never grant access through the link.
// AuthorizeScope receives PINNED CONTENT and must itself fence one current
// authority snapshot, including for asOf content. ValidateAuthority checks a
// fresh CURRENT snapshot's caller, facts, ACL/context and generations against
// the returned binding after status bucketing, before any counts escape.
type EpicProgressConfig struct {
	// AnonymousOnly is an explicit immutable no-plugin host mode. It refuses
	// authenticated principals and cannot be combined with a read authorizer.
	ParentProjection  *ParentProjectionConfig
	AnonymousOnly     bool
	ValidateAuthority func(context.Context, model.PrincipalRef, string) error
	AuthorizeScope    func(context.Context, model.PrincipalRef, driver.Reader, model.Ref, []model.Ref) (EpicProgressAuthority, error)
	OntologySource    service.OntologySourceLocator
}

// EpicProgressAuthority is host-private. It does not add a wire result member
// or grant. EP2's Checker.AuthorizeEpicProgress is adapted to this projection.
type EpicProgressAuthority struct {
	Visible       []model.Ref
	Hidden        model.Natural
	PolicyBinding string
}

// Keep the resolver contract visible from the view package while the shared
// event/watch layer owns the transport-neutral binding type.
type WatchBinding = coreevents.ViewBinding

type Service struct {
	store ReadStore
	lease driver.ActiveLease
	cfg   Config
}

// Filter is the canonical shared filter shape. The query member uses the
// admitted equality grammar implemented by this slice; unsupported grammar is
// refused explicitly.
type Filter struct {
	Query     string   `json:"query,omitempty"`
	Resources []string `json:"resources,omitempty"`
	Types     []string `json:"types,omitempty"`
	Events    []string `json:"events,omitempty"`
	ViewID    string   `json:"viewId,omitempty"`
}

// Definition preserves presence for fields and budgetTokens through nil
// slices/pointers. In particular, non-nil empty Fields is the explicit
// fields=[] required by decision #20.
type Definition struct {
	Name         string         `json:"name,omitempty"`
	Filter       *Filter        `json:"filter"`
	Fields       []string       `json:"fields,omitempty"`
	GroupBy      []string       `json:"groupBy,omitempty"`
	Sort         []string       `json:"sort,omitempty"`
	BudgetTokens *model.Natural `json:"budgetTokens,omitempty"`
}

type AsOf struct {
	Revision    model.Revision `json:"revision"`
	Incarnation string         `json:"incarnation,omitempty"`
}

// Cursor is trusted only after viewrpc verifies its signature. Core still
// checks every binding field before using the position.
type Cursor struct {
	Version          int            `json:"version"`
	Method           string         `json:"method"`
	PrincipalDigest  string         `json:"principal"`
	DefinitionDigest string         `json:"definition"`
	ViewID           string         `json:"viewId,omitempty"`
	OntologyRevision model.Revision `json:"ontology"`
	Incarnation      string         `json:"incarnation"`
	Revision         model.Revision `json:"revision"`
	Index            uint64         `json:"index"`
	AuthorityBinding string         `json:"authority,omitempty"`
	// StaleResumeDigest identifies the last emitted visible row by a fixed-size
	// digest of its ID. The signed definition and snapshot recover its sort
	// position without embedding unbounded sort values or denied-row indexes.
	StaleResumeDigest string     `json:"staleResume,omitempty"`
	StaleCutoff       *time.Time `json:"staleCutoff,omitempty"`
}

// StaleFilter is an execution-only view.run option. Saved definitions and
// the shared Filter used by ready/claimNext/watch have no age semantics.
type StaleFilter struct {
	Days int `json:"days"`
	// Status is a pointer so the wire layer can distinguish an omitted
	// status (the default active categories) from an explicitly supplied
	// empty status (an exact, unknown status that matches nothing).
	Status *string `json:"status,omitempty"`
}

type RunRequest struct {
	// CountOnly counts the complete ordinary predicate set without item paging.
	// It is an execution option and never part of a saved Definition.
	CountOnly  bool
	Stale      *StaleFilter
	ViewID     string
	Definition *Definition
	Cursor     *Cursor
	AsOf       *AsOf
	Project    string
	Agent      string
}

type Bead struct {
	ID               string         `json:"id"`
	Type             string         `json:"type"`
	Project          string         `json:"project,omitempty"`
	Revision         model.Revision `json:"revision"`
	Status           string         `json:"status,omitempty"`
	Fields           map[string]any `json:"fields,omitempty"`
	Metadata         map[string]any `json:"metadata,omitempty"`
	Parent           string         `json:"parent,omitempty"`
	ParentHidden     bool           `json:"-"`
	Labels           []string       `json:"labels,omitempty"`
	Assignee         string         `json:"assignee,omitempty"`
	HiddenBlockers   int            `json:"hiddenBlockers,omitempty"`
	ReadinessPending bool           `json:"readinessPending,omitempty"`
}

type Elision struct {
	Count         int
	Reason        string
	ContinueIndex uint64
}

type Group struct {
	Key    string
	Count  int
	Items  []Bead
	Elided *Elision
}

type Result struct {
	// Count is present, including zero, only for an explicit CountOnly run.
	Count      *int                         `json:"count,omitempty"`
	Items      []Bead                       `json:"items"`
	NextCursor *Cursor                      `json:"nextCursor,omitempty"`
	AsOf       AsOf                         `json:"asOf"`
	Groups     []Group                      `json:"groups,omitempty"`
	Projection *coreevents.ProjectionStatus `json:"projection,omitempty"`
}

type ExplainRequest struct {
	ViewID  string
	ItemID  string
	Project string
	Agent   string
}

type ExplainResult struct {
	ViewID           string                       `json:"viewId"`
	ItemID           string                       `json:"itemId"`
	Included         bool                         `json:"included"`
	Reasons          []string                     `json:"reasons"`
	HiddenBlockers   int                          `json:"hiddenBlockers,omitempty"`
	ReadinessPending bool                         `json:"readinessPending,omitempty"`
	DirectBlockers   *DirectBlockerResult         `json:"directBlockers,omitempty"`
	Projection       *coreevents.ProjectionStatus `json:"projection,omitempty"`
}

// DirectBlockerResult is present only when the optional source is configured
// and the dependent is readable and included. Inner fields stay present for an
// evaluated empty verdict so unavailable and empty remain distinguishable.
type DirectBlockerResult struct {
	Edges       []DirectBlockerEdge `json:"edges"`
	HiddenCount int                 `json:"hiddenCount"`
}
