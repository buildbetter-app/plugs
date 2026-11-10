// Package service owns domain reads and named verbs. Transports map these
// values to their contract; graph writes go through model.Writer only.
package service

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// Gate request/result names are re-exported from the core port so domain
// services and RPC adapters share one canonical shape.
type GateApproveRequest = port.GateApproveRequest
type GateReportRequest = port.GateReportRequest
type GateResult = port.GateResult
type GateOutcome = port.GateOutcome
type GateService = port.GateService

// AsOf selects either a revision address or an instant. Instant is input-only;
// returned pages and PagePosition always carry the resolved Revision. When
// Instant is present, Revision must retain its unset Go value.
type AsOf struct {
	Revision    model.Revision
	Incarnation string
	Instant     *time.Time
}

type Memory struct {
	ID       string
	Key      string
	Content  string
	Metadata map[string]any
	Revision model.Revision
}

type MemoryPage struct {
	Items []Memory
	AsOf  AsOf
	Next  *PagePosition
}

type MemoryGetRequest struct {
	ID   model.Ref
	AsOf *AsOf
}

type MemoryListRequest struct {
	Query    string
	Limit    int
	Cursor   string
	Position *PagePosition
}

type MemorySaveRequest struct {
	Key      string
	Content  string
	Metadata map[string]any
	Expect   []model.Precondition
}

type MemoryDeleteRequest struct {
	ID     model.Ref
	Expect []model.Precondition
}

type WhoAmIResult struct {
	Principal      model.PrincipalRef
	Sponsor        *model.PrincipalRef
	Caveats        map[string]any
	DefaultProject string
}

type BriefRequest struct {
	Project      string
	BudgetTokens int
}

type BriefResult struct {
	WhoAmI          WhoAmIResult
	Claims          []Bead
	ReadyTop        []Bead
	RecentChanges   []map[string]any
	OntologySummary map[string]any
	Conventions     map[string]any
	Memories        []Memory
}

// MemoryReadAuthorizer evaluates current read visibility for one memory. It
// is deliberately a service boundary: policy code sees a domain Memory and
// never receives a driver reader or transaction.
type MemoryReadAuthorizer interface {
	CanReadMemory(context.Context, model.PrincipalRef, Memory) (bool, error)
}

type MemoryReadAuthorizerFunc func(context.Context, model.PrincipalRef, Memory) (bool, error)

func (f MemoryReadAuthorizerFunc) CanReadMemory(ctx context.Context, by model.PrincipalRef, memory Memory) (bool, error) {
	return f(ctx, by, memory)
}

// PagePosition is domain state, never an untrusted wire token. RPC signs and
// validates its method/principal/filter binding before passing it to service.
// Ordinary Ready and MemoryList continue at Offset in the live ordered list;
// AsOf identifies the last observed page and incarnation. Workflow Ready
// offsets additionally bind the provider snapshot. History pins every page.
type PagePosition struct {
	Offset                int
	AsOf                  AsOf
	AuthorizationRevision model.Revision
	AuthorizationBound    bool
	// Workflow marks the second phase of bead.ready pagination. Core beads are
	// emitted first; the signed transport cursor then continues through the
	// stable plugin-owned workflow ordering without resetting the core offset.
	Workflow bool
	// WorkflowSnapshot also binds an authorization-filtered aggregate offset,
	// whose position does not use the raw provider-phase Workflow flag.
	WorkflowSnapshot *WorkflowPageSnapshot
	// ReadyMembership binds guarded aggregate paging to the complete
	// post-authorization order, including ordinary core rows.
	ReadyMembership string
	// A trusted internal Ready drain may reuse decoded source bytes. The
	// guarded public position is built separately and never carries this state.
	readyGraph *readyGraphSelection
}

// WorkflowPageSnapshot identifies one atomic provider ready observation. Its
// deadline includes currently excluded claims: expiry can change membership
// without changing the persisted revision. It is internal signed cursor state.
type WorkflowPageSnapshot struct {
	Revision   model.Revision `json:"revision"`
	ValidUntil *time.Time     `json:"validUntil,omitempty"`
}

func (s *WorkflowPageSnapshot) Valid() bool {
	return s == nil || (s.Revision.Validate() == nil && (s.ValidUntil == nil || !s.ValidUntil.IsZero()))
}

func (s *WorkflowPageSnapshot) Equal(other *WorkflowPageSnapshot) bool {
	if s == nil || other == nil {
		return s == other
	}
	if !s.Valid() || !other.Valid() || s.Revision.String() != other.Revision.String() {
		return false
	}
	if s.ValidUntil == nil || other.ValidUntil == nil {
		return s.ValidUntil == other.ValidUntil
	}
	return s.ValidUntil.Equal(*other.ValidUntil)
}

type Filter struct {
	Query            string
	Resources        []string
	Types            []string
	Events           []string
	ViewID           string
	IncludeEphemeral *bool
}

// EventProvider is the transport-neutral seam used by the core service to
// expose the persisted event reader. The provider owns cursor signing and
// current read authorization; the service only supplies the authenticated
// principal and the wire filter values.
type EventProvider interface {
	List(context.Context, EventProviderRequest) (EventPage, error)
}

type EventProviderRequest struct {
	Principal model.PrincipalRef
	Filter    EventFilter
	After     string
	Limit     int
}

type EventFilter struct {
	Types     []string
	Resources []string
}

type EventPage struct {
	Events []model.Event
	Cursor string
}

type EventListRequest struct {
	Filter *Filter
	After  string
	Limit  int
}

type EventReader interface {
	EventList(context.Context, model.PrincipalRef, EventListRequest) (EventPage, error)
}

type HistoryReader interface {
	HistoryGet(context.Context, model.PrincipalRef, HistoryVersionRequest) (HistoryVersion, error)
	HistoryDiff(context.Context, model.PrincipalRef, HistoryDiffRequest) (HistoryDiff, error)
}

type HealthReader interface {
	Health(context.Context, model.PrincipalRef) (driver.StoreHealth, error)
}

type HistoryVersionRequest struct {
	Ref  model.Ref
	AsOf AsOf
}

type HistoryVersion struct {
	Ref       model.Ref
	Revision  model.Revision
	Base      bool
	Patch     []map[string]any
	Body      map[string]any
	CreatedAt time.Time
	Principal model.PrincipalRef
	Deleted   bool
}

type HistoryDiffRequest struct {
	Ref          model.Ref
	FromRevision model.Revision
	ToRevision   model.Revision
}

type JSONPatchOperation struct {
	Op    string
	Path  string
	Value any
}

type HistoryDiff struct {
	Patch        []JSONPatchOperation
	ChangedPaths []string
}

type GetRequest struct {
	ID   model.Ref
	AsOf *AsOf
	// Project is the effective transport scope for plugin-owned reads.
	Project string
	// Agent carries the transport's anonymous-agent label to plugin-owned
	// standard reads. Authenticated principals ignore it for ownership.
	Agent string
}

type ReadyRequest struct {
	Project string
	// Agent carries the transport's anonymous-agent label to plugin-owned
	// standard ready listings and cursor continuations.
	Agent    string
	ViewID   string
	Filter   *Filter
	Limit    int
	Cursor   string
	Position *PagePosition
	// ClaimNextSeed is an internal queue-ordering seed. It is never accepted
	// from the wire; claimNext derives it from the selector and request key so
	// equal-priority candidates can spread without changing bead.ready order.
	ClaimNextSeed string
	// authorizationFilter is a composition-root capability used by the guarded
	// RPC adapter. The core service applies it to each complete ready candidate
	// before cutting a page; it is never populated from wire input.
	authorizationFilter ReadyAuthorizer
	// Only the trusted guarded capability requests complete internal order metadata.
	readySourceBinding bool
	readyGraph         *readyGraphSelection
}

// ReadyAuthorizer is the narrow host-to-core read seam for a compiled finite
// authorization predicate. The reader is the exact snapshot used to build
// the ready graph, so an adapter can join its fact reads to that snapshot.
// Check-only authorization deliberately leaves this nil and uses the guarded
// post-filter cap instead.
type ReadyAuthorizer func(context.Context, driver.Reader, Bead) (bool, error)

type CreateRequest struct {
	Type   string
	Fields map[string]any
	Parent *model.Ref
}

type UpdateRequest struct {
	ID     model.Ref
	Fields map[string]any
	Expect []model.Precondition
}

type ClaimRequest struct {
	ID     model.Ref
	Expect []model.Precondition
}

// HeartbeatRequest identifies the claimed bead whose replica-local lease is
// being refreshed. The idempotency key and anonymous agent label remain in
// WriteMeta, matching every other named write.
type HeartbeatRequest struct {
	ID model.Ref
}

type HeartbeatResult struct {
	LeaseExpiresAt time.Time
}

// ClaimNextRequest describes the ready-set selection for the atomic queue
// claim. The RPC layer supplies the project and filter after schema/cursor
// validation; the service keeps the same bounded ready vocabulary as Ready.
type ClaimNextRequest struct {
	Project string
	ViewID  string
	Filter  *Filter
}

// ClaimNextRequestIdentity returns a canonical, selector-only identity for a
// claimNext request. Filter lists are sets in the ready query, so ordering and
// duplicate values do not change the identity. A nil filter and an empty
// filter are equivalent.
func ClaimNextRequestIdentity(req ClaimNextRequest) (string, error) {
	type normalizedFilter struct {
		Query            string   `json:"query"`
		Resources        []string `json:"resources"`
		Types            []string `json:"types"`
		Events           []string `json:"events"`
		ViewID           string   `json:"viewId"`
		IncludeEphemeral bool     `json:"includeEphemeral"`
	}
	normalize := func(values []string) []string {
		if len(values) == 0 {
			return nil
		}
		seen := make(map[string]struct{}, len(values))
		out := make([]string, 0, len(values))
		for _, value := range values {
			if _, ok := seen[value]; ok {
				continue
			}
			seen[value] = struct{}{}
			out = append(out, value)
		}
		sort.Strings(out)
		return out
	}
	value := normalizedFilter{IncludeEphemeral: true}
	if req.Filter != nil {
		includeEphemeral := true
		if req.Filter.IncludeEphemeral != nil {
			includeEphemeral = *req.Filter.IncludeEphemeral
		}
		value = normalizedFilter{
			Query: req.Filter.Query, Resources: normalize(req.Filter.Resources),
			Types: normalize(req.Filter.Types), Events: normalize(req.Filter.Events),
			ViewID: req.Filter.ViewID, IncludeEphemeral: includeEphemeral,
		}
	}
	data, err := json.Marshal(struct {
		ViewID string           `json:"viewId"`
		Filter normalizedFilter `json:"filter"`
	}{ViewID: req.ViewID, Filter: value})
	return string(data), err
}

type CommentRequest struct {
	ID     model.Ref
	Body   string
	Expect []model.Precondition
}

type CloseRequest struct {
	ID     model.Ref
	Reason string
	Expect []model.Precondition
}

// LinkRequest is the named dependency write lowered to the canonical LinkOp
// in a ChangeSet. Expect applies to the source bead, which is the only record
// mutated by a dependency operation.
type LinkRequest struct {
	TargetKind model.LinkTargetKind
	From       model.Ref
	To         model.Ref
	Kind       string
	Metadata   *model.LinkMetadata
	Expect     []model.Precondition
}

// The named lifecycle requests intentionally share the close shape. Their
// method name selects the ontology slot; callers cannot choose a target status
// through request fields.
type ReopenRequest struct {
	ID     model.Ref
	Reason string
	Expect []model.Precondition
}

type DeferRequest struct {
	ID     model.Ref
	Reason string
	Expect []model.Precondition
}

type CancelRequest struct {
	ID     model.Ref
	Reason string
	Expect []model.Precondition
}

type FailRequest struct {
	ID     model.Ref
	Reason string
	Expect []model.Precondition
}

type HistoryRequest struct {
	Project  string
	AsOf     AsOf
	Filter   *Filter
	Limit    int
	Cursor   string
	Position *PagePosition
}

type Bead struct {
	ID       string
	Type     string
	Project  string
	Revision model.Revision
	Status   string
	Fields   map[string]any
	Metadata map[string]any
	Parent   string
	// ParentHidden is projection state, never a stored relationship or grant.
	// When true the transport emits only parent:{exists:true}.
	ParentHidden     bool
	Labels           []string
	Links            []workflowplugin.RelationView
	Blocked          bool
	ChildCounter     uint64
	Assignee         string
	HiddenBlockers   int
	ReadinessPending bool
	CreatedAt        time.Time
	UpdatedAt        time.Time
	// Workflow fields are present only for plugin-owned ephemeral items. They
	// make the plane and kind explicit at the standard-tool boundary.
	Plane         string
	Kind          string
	Plugin        string
	WorkflowRunID string
}

type Lease struct {
	Principal model.PrincipalRef
	Agent     string
	ExpiresAt time.Time
	TTL       time.Duration
	Epoch     model.Natural
}

type GetResult struct {
	Bead        Bead
	Attachments []model.Attachment
	Lease       *Lease
	Comments    []Comment
}

// Comment keeps verified actor/target time separate from untrusted import
// provenance. A nil Comments slice preserves the legacy omitted result.
type Comment struct {
	Body         string
	Principal    model.PrincipalRef
	Time         time.Time
	Origin       string
	SourceAuthor string
	SourceTime   *time.Time
}

type BeadPage struct {
	Items      []Bead
	NextCursor string
	AsOf       AsOf
	Next       *PagePosition
	// Retained even on the final page so guarded aggregation can sign its own
	// continuation after current-policy filtering.
	WorkflowSnapshot *WorkflowPageSnapshot
	ReadyMembership  string
}

type ClaimResult struct {
	Commit         model.Commit
	LeaseExpiresAt time.Time
	LeaseTTL       time.Duration
}
