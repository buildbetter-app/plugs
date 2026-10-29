// Package collab owns bounded, ephemeral collaboration sessions. It imports
// core/service; the graph, writer, and core never depend on collaboration state.
package collab

import (
	"context"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

const (
	SessionTTL              = 5 * time.Minute
	MaxSessions             = 1024
	MaxSessionsPerPrincipal = 64
	MaxSessionsPerSponsor   = 256
	MaxWorkingOn            = 64
	MaxResourceBytes        = 1024
	MaxPrincipalBytes       = 16384
)

// BeadReader is the actual domain service boundary, never a graph copy.
type BeadReader interface {
	Get(context.Context, model.PrincipalRef, service.GetRequest) (service.GetResult, error)
}

// ReadPolicy must evaluate CURRENT permissions for the authenticated principal
// on the actual service result. It must honor cancellation, deny unsupported
// consumers, and return an error when its authoritative context/facts are absent.
// No implementation or policy permit is inferred from a successful service.Get.
type ReadPolicy interface {
	AdmitPresence(context.Context, model.PrincipalRef, string) (bool, error)
	CanRead(context.Context, model.PrincipalRef, service.Bead) (bool, error)
}

// ActiveLease is supplied by the composition root's real storage lease.
type ActiveLease interface{ Lost() <-chan struct{} }

type Limits struct {
	Sessions, PerPrincipal, PerSponsor int
}

type Config struct {
	Service BeadReader
	Lease   ActiveLease
	// Required: read current host installation state, never a caller assertion.
	AuthorizationInstalled func() bool
	Policy                 ReadPolicy
	Limits                 Limits
	// Clock is for deterministic expiry assertions. Production uses time.Now.
	Clock func() time.Time
	// IdleTTL may shorten, but never widen, the canonical five-minute ceiling.
	IdleTTL time.Duration
}

type Session struct {
	SessionID  string             `json:"sessionId"`
	Principal  model.PrincipalRef `json:"principal"`
	OpenedAt   time.Time          `json:"openedAt"`
	OnBehalfOf string             `json:"onBehalfOf,omitempty"`
}

type Presence struct {
	SessionID  string             `json:"sessionId"`
	Principal  model.PrincipalRef `json:"principal"`
	Focus      string             `json:"focus,omitempty"`
	WorkingOn  []string           `json:"workingOn,omitempty"`
	OnBehalfOf string             `json:"onBehalfOf,omitempty"`
}

// Caller comes only from trusted rpc.Call fields. Agent is an unverified
// anonymous client label; it never replaces Principal or a sponsor.
type Caller struct {
	Principal model.PrincipalRef
	Agent     string
}

type SetRequest struct {
	SessionID string
	Project   string
	// Nil preserves focus; an explicit empty string clears it.
	Focus      *string
	WorkingOn  []string
	OnBehalfOf *string
}

func failure(code model.ErrorCode, hint string) error {
	return &model.Error{Data: model.ErrorData{Code: code, Hint: hint, Retryable: code == "busy" || code == "authz_unavailable" || code == "too_many_sessions"}}
}
