// Package authz supplies the finite authorization chain and recorded-fact ports.
// It contains no installed policy and never opens a storage transaction.
package authz

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

type Effect string

const (
	Allow      Effect = "allow"
	Deny       Effect = "deny"
	NoDecision Effect = "no_decision"
)

type Action string

const (
	Read              Action = "read"
	Modify            Action = "modify"
	ImportBulkHistory Action = "importBulkHistory"
	Transition        Action = "transition"
	OverrideGuard     Action = "override_guard"
)

type ResourceKind string

const (
	Bead    ResourceKind = "bead"
	Project ResourceKind = "project"
	History ResourceKind = "history"
)

type ScopeKind string

const (
	AuthorityScope ScopeKind = "authority"
	TeamScope      ScopeKind = "team"
	ProjectScope   ScopeKind = "project"
)

type SubjectRef struct{ Kind, ID string }
type Scope struct {
	Kind ScopeKind
	Ref  model.Ref
}
type Grant struct {
	Subject SubjectRef
	Scope   Scope
	Role    string
}

// Namespace is pinned installation configuration, never caller-selected scope.
type Namespace struct {
	StoreID, Incarnation, InstallationID string
	Authority                            model.Ref
}

func ValidID(s string) bool {
	return s != "" && utf8.ValidString(s) && strings.TrimSpace(s) == s && !strings.ContainsRune(s, 0)
}

// CanonicalRef narrows the wire Ref contract only for this finite component.
func CanonicalRef(r model.Ref) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if !r.IsID() || !r.Branch.IsZero() || !ValidID(r.ID) {
		return errors.New("authz requires a nonempty ID-arm branch-zero Ref")
	}
	return nil
}

func (n Namespace) Validate() error {
	if !ValidID(n.StoreID) || !ValidID(n.Incarnation) || !ValidID(n.InstallationID) {
		return errors.New("invalid authorization namespace")
	}
	return CanonicalRef(n.Authority)
}

func sameNamespace(a, b Namespace) bool {
	return a.Validate() == nil && b.Validate() == nil && a.StoreID == b.StoreID && a.Incarnation == b.Incarnation && a.InstallationID == b.InstallationID && a.Authority.ID == b.Authority.ID
}

func (s Scope) Validate() error {
	if s.Kind != AuthorityScope && s.Kind != TeamScope && s.Kind != ProjectScope {
		return fmt.Errorf("invalid scope kind %q", s.Kind)
	}
	return CanonicalRef(s.Ref)
}

func (s SubjectRef) Validate() error {
	if (s.Kind != "user" && s.Kind != "team") || !ValidID(s.ID) {
		return errors.New("invalid grant subject")
	}
	return nil
}

// Caller is a core-authenticated snapshot binding. VerificationID is an opaque,
// nonsecret receipt identity, not a token or an assertion of human presence.
// CheckContext must validate this exact principal/sponsor/caveats and captured
// epoch against authoritative current generations, activity and expiry.
type Caller struct {
	Principal      model.PrincipalRef
	VerificationID string
	CapturedEpoch  model.Revision
}

type Resource struct {
	Kind ResourceKind
	Ref  model.Ref
}
type CheckRequest struct {
	Caller   Caller
	Action   Action
	Resource Resource
	Method   string
	// Transition is trusted host context, never RPC input. Strings are values,
	// so freezing a request also detaches this exact target-status binding.
	Transition TransitionContext
}

type TransitionContext struct {
	Project, Type        string
	FromStatus, ToStatus string
}
type ListRequest struct {
	Caller       Caller
	Action       Action
	ResourceKind ResourceKind
	Method       string
}

type PolicyError struct {
	PolicyID string
	Effect   Effect
	Message  string
}
type Decision struct {
	Effect  Effect
	Reasons []string
	Errors  []PolicyError
}

// Dependency has a persistent version/lock identity, including a missing value
// and deleted value. Source revisions for point hits are additionally recorded
// by Slice. Incarnation is checked independently; versions never cross it.
type Dependency struct{ Kind, Key string }

// Slice is the read-only seam to the host's pinned snapshot. Record MUST join
// that snapshot's guard before a fact read; errors must never become absence.
// Implementations record source records/projection integrity for all hits.
// Groups is complete (or errors), includes inactive teams, and records empty
// enumerations. ProjectForBead reads the exact stored body.project and validates
// its projection against the immutable registry; there is no parent/default
// inheritance. A missing mapping returns Found=false, never a global scope.
type Slice interface {
	Namespace() Namespace
	Record(context.Context, Dependency) error
	CheckContext(context.Context, Caller) (bool, error)
	HasGrant(context.Context, SubjectRef, Scope, string) (bool, error)
	Groups(context.Context, SubjectRef) ([]model.Ref, error)
	TeamActive(context.Context, model.Ref) (bool, error)
	ProjectActive(context.Context, model.Ref) (bool, error)
	ProjectForBead(context.Context, model.Ref) (ProjectBinding, error)
}

// SourceBeadSlice is an optional trusted-host identity binding for targets in a
// separately guarded source namespace. It admits only exact installed targets;
// the host must retain the provider's presence/ownership and namespace fences.
// It supplies no policy permit. Ordinary slices retain slash-free bead IDs.
type SourceBeadSlice interface {
	SourceBeadReference(model.Ref) bool
}

func sourceBeadReference(s Slice, ref model.Ref) bool {
	bound, ok := s.(SourceBeadSlice)
	return ok && bound.SourceBeadReference(ref)
}

func validBeadReference(s Slice, ref model.Ref) bool {
	return !strings.Contains(ref.ID, "/") || sourceBeadReference(s, ref)
}

type ProjectBinding struct {
	Name  string
	Ref   model.Ref
	Found bool
}

type RuleSet interface {
	Eval(context.Context, Slice, []CheckRequest) ([]Decision, error)
	Compile(ListRequest) (FilterTemplate, error)
}

// Pass may append a restriction (AND); it cannot replace authenticated context.
type Step struct {
	Effect      Effect
	Restriction *FilterExpr
	Reason      string
}
type Link interface {
	// Inputs are isolated copies; a link cannot replace the host's caller,
	// sponsor, caveats or target. Returned restrictions only intersect.
	Check(context.Context, Slice, []CheckRequest) ([]Step, error)
	// Filter is the exact set passing this link's restrictions (Pass or Allow).
	// It does not distinguish those effects; compilation also needs AllowFilter.
	// A check-only link returns ErrCheckOnly; lists/claims cannot silently cap.
	Filter(ListRequest) (FilterExpr, error)
}

// TerminalLink compiles the exact subset on which a code link returns Allow,
// before applying its restrictions. Chain.Compile requires this capability
// from every link. A nonterminal link, including any link followed by rules,
// must return literal False for the bound request or the chain is check-only.
// Only the last link of a code-only chain may contribute this subset as a
// permit. Passing a restriction alone never manufactures a permit.
type TerminalLink interface {
	AllowFilter(ListRequest) (FilterExpr, error)
}

var ErrCheckOnly = errors.New("authz check-only link cannot compile")

type Error struct {
	Code      string
	Retryable bool
	Cause     error
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return e.Code + ": " + e.Cause.Error()
	}
	return e.Code
}
func (e *Error) Unwrap() error { return e.Cause }

func unavailable(err error) error {
	return &Error{Code: "authz_unavailable", Retryable: true, Cause: err}
}
