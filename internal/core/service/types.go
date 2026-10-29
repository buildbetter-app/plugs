// Package service owns domain reads and named verbs. Transports map these
// values to their contract; graph writes go through model.Writer only.
package service

import (
	"encoding/json"
	"sort"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
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
	Offset                int
	AsOf                  AsOf
	AuthorizationRevision model.Revision
	AuthorizationBound    bool
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
		Query     string   `json:"query"`
		Resources []string `json:"resources"`
		Types     []string `json:"types"`
		Events    []string `json:"events"`
		ViewID    string   `json:"viewId"`
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
	value := normalizedFilter{}
	if req.Filter != nil {
		value = normalizedFilter{
			Query: req.Filter.Query, Resources: normalize(req.Filter.Resources),
			Types: normalize(req.Filter.Types), Events: normalize(req.Filter.Events),
			ViewID: req.Filter.ViewID,
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
