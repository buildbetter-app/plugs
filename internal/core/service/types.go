// Package service owns domain reads and named verbs. Transports map these
// values to their contract; graph writes go through model.Writer only.
package service

import (
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

type AsOf struct {
	Revision    model.Revision
	Incarnation string
}

type Filter struct {
	Query     string
	Resources []string
	Types     []string
	Events    []string
	ViewID    string
}

type GetRequest struct {
	ID   model.Ref
	AsOf *AsOf
}

type ReadyRequest struct {
	Project string
	ViewID  string
	Filter  *Filter
	Limit   int
	Cursor  string
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

type HistoryRequest struct {
	Project string
	AsOf    AsOf
	Filter  *Filter
	Limit   int
	Cursor  string
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
}

type ClaimResult struct {
	Commit         model.Commit
	LeaseExpiresAt time.Time
	LeaseTTL       time.Duration
}
