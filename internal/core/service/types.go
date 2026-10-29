// Package service owns domain reads and named verbs. Transports map these
// values to their contract; graph writes go through model.Writer only.
package service

import (
	"context"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type AsOf struct {
	Revision    model.Revision
	Incarnation string
}

// PagePosition is domain state, never an untrusted wire token. RPC signs and
// validates its method/principal/filter binding before passing it to service.
// Ready continues at Offset in the live ordered list; AsOf identifies the last
// observed page and incarnation. History instead pins every page to AsOf.
type PagePosition struct {
	Offset int
	AsOf   AsOf
}

type Filter struct {
	Query     string
	Resources []string
	Types     []string
	Events    []string
	ViewID    string
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
}

type ReadyRequest struct {
	Project  string
	ViewID   string
	Filter   *Filter
	Limit    int
	Cursor   string
	Position *PagePosition
}

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
	ID               string
	Type             string
	Project          string
	Revision         model.Revision
	Status           string
	Fields           map[string]any
	Metadata         map[string]any
	Parent           string
	Labels           []string
	Assignee         string
	HiddenBlockers   int
	ReadinessPending bool
	CreatedAt        time.Time
	UpdatedAt        time.Time
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
}

type BeadPage struct {
	Items      []Bead
	NextCursor string
	AsOf       AsOf
	Next       *PagePosition
}

type ClaimResult struct {
	Commit         model.Commit
	LeaseExpiresAt time.Time
	LeaseTTL       time.Duration
}
