// Package authz contains the policy-language part of the authorization spike.
//
// The spike deliberately keeps the data contract small: principals carry their
// type and sponsor, a sponsor carries the projects in which they are involved,
// and resources have a Cedar entity type and identifier.  The SQL benchmark
// can use the FilterTemplate's SQL fragment while this package checks that its
// result agrees with cedar-go on individual requests.
package authz

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	cedar "github.com/cedar-policy/cedar-go"
	cedarast "github.com/cedar-policy/cedar-go/x/exp/ast"
)

// CP7RelationshipSchema is the relationship-schema spelling of the three
// policies used by the spike.  It is intentionally concrete enough to compare
// readability with Cedar, while remaining independent of a relationship
// engine's implementation or syntax.
const CP7RelationshipSchema = `
entity User {
  admin: Bool,
  involved_in: Set<Project>
}
entity Agent { sponsor: User }
entity Project
entity Bead
entity History

relation User.admin -> importBulkHistory on History
relation User -> read on Bead
relation Agent -> modify on Project
  when project in agent.sponsor.involved_in
`

// CP7CedarPolicies is the Cedar source for CP7.  It uses the two-hierarchy
// contract from authorization-options.md: normal entity parents are available
// through Cedar's "in", and relationship data is an attribute of the sponsor.
const CP7CedarPolicies = `
@id("admin_bulk_history")
permit (
  principal is User,
  action == Action::"importBulkHistory",
  resource is History
) when { principal.admin };

@id("user_read_all_beads")
permit (
  principal is User,
  action == Action::"read",
  resource is Bead
);

@id("agent_modify_sponsored_project")
permit (
  principal is Agent,
  action == Action::"modify",
  resource is Project
) when { resource in principal.sponsor.involvedIn };
`

// PolicySpec keeps both representations together for the readability and
// subset-compiler portions of the spike.
type PolicySpec struct {
	Name         string
	Relationship string
	Cedar        string
	Read         bool
	Description  string
}

// CP7Policies is the ordered list of real policies in the spike.  A copy is
// returned by Policies so callers cannot mutate the package-level slice.
var CP7Policies = []PolicySpec{
	{
		Name:         "admin_bulk_history",
		Relationship: "relation User.admin -> importBulkHistory on History",
		Cedar:        `permit (principal is User, action == Action::"importBulkHistory", resource is History) when { principal.admin };`,
		Description:  "Only admin users can import bulk history.",
	},
	{
		Name:         "user_read_all_beads",
		Relationship: "relation User -> read on Bead",
		Cedar:        `permit (principal is User, action == Action::"read", resource is Bead);`,
		Read:         true,
		Description:  "All users can read all beads.",
	},
	{
		Name:         "agent_modify_sponsored_project",
		Relationship: "relation Agent -> modify on Project when project in agent.sponsor.involved_in",
		Cedar:        `permit (principal is Agent, action == Action::"modify", resource is Project) when { resource in principal.sponsor.involvedIn };`,
		Description:  "Agents can modify only projects their sponsoring user is involved in.",
	},
}

// Policies returns a copy of CP7Policies.
func Policies() []PolicySpec { return append([]PolicySpec(nil), CP7Policies...) }

// Principal is the Cedar principal and its authorization-contract facts.
type Principal struct {
	ID               string
	Kind             string // User or Agent.
	Admin            bool
	SponsorID        string
	InvolvedProjects []string
}

// Resource identifies a Cedar resource. Kind is one of Bead, Project, or
// History for the CP7 policies.
type Resource struct {
	ID       string
	Kind     string
	ParentID string // optional Cedar parent, useful to the SQL benchmark
}

// EvaluationRequest is the request shared by Cedar and the compiled subset.
type EvaluationRequest struct {
	Principal Principal
	Action    string
	Resource  Resource
}

// FilterTemplate is a plan-time compiled policy fragment. Predicate is bound
// to a principal at request time, preserving the compile-at-plan/bind-at-
// request rule. SQL is an illustrative push-down shape for the benchmark.
type FilterTemplate struct {
	PolicyID     string
	Action       string
	ResourceKind string
	SQL          string
	predicate    func(Principal, Resource) bool
}

// Match applies a compiled template to one principal/resource pair.
func (f FilterTemplate) Match(principal Principal, resource Resource) bool {
	return f.predicate != nil && f.predicate(principal, resource)
}

// CompiledList is the OR of all matching permit templates for an action and
// resource kind.  It preserves the order of the input resource slice.
type CompiledList struct {
	Action       string
	ResourceKind string
	Templates    []FilterTemplate
}

// Match reports whether any compiled policy permits the request.
func (l CompiledList) Match(principal Principal, resource Resource) bool {
	for _, template := range l.Templates {
		if template.Match(principal, resource) {
			return true
		}
	}
	return false
}

// Filter evaluates a compiled list against resources. The returned slice is a
// new slice and may be safely changed by the caller.
func (l CompiledList) Filter(principal Principal, resources []Resource) []Resource {
	out := make([]Resource, 0, len(resources))
	for _, resource := range resources {
		if l.Match(principal, resource) {
			out = append(out, resource)
		}
	}
	return out
}

// CP7PolicySet contains parsed cedar-go policies and their checked subset
// templates.
type CP7PolicySet struct {
	Cedar     *cedar.PolicySet
	Policies  map[string]*cedar.Policy
	Templates map[string]FilterTemplate
}

// ParseCP7 parses all three policies through cedar-go v1.8.0 and checks that
// each policy belongs to the subset understood by the list compiler.
func ParseCP7() (*CP7PolicySet, error) {
	result := &CP7PolicySet{
		Cedar:     cedar.NewPolicySet(),
		Policies:  make(map[string]*cedar.Policy, len(CP7Policies)),
		Templates: make(map[string]FilterTemplate, len(CP7Policies)),
	}
	for _, spec := range CP7Policies {
		var policy cedar.Policy
		if err := policy.UnmarshalCedar([]byte(spec.Cedar)); err != nil {
			return nil, fmt.Errorf("parse policy %q: %w", spec.Name, err)
		}
		result.Cedar.Add(cedar.PolicyID(spec.Name), &policy)
		result.Policies[spec.Name] = &policy
		template, err := CompilePolicySubset(spec.Name, &policy)
		if err != nil {
			return nil, fmt.Errorf("compile policy %q: %w", spec.Name, err)
		}
		result.Templates[spec.Name] = template
	}
	return result, nil
}

// ParseAndCompileCP7 is an explicit alias useful to benchmark drivers.
func ParseAndCompileCP7() (*CP7PolicySet, error) { return ParseCP7() }

// CompilePolicySubset checks the Cedar AST against the list-filter subset and
// returns a principal-symbolic template.  The supported fragment is:
//
//	permit, principal is T, action == Action::"a", resource is R
//	optional condition principal.admin
//	optional condition resource in principal.sponsor.involvedIn
//
// Conditions are deliberately recognized structurally instead of by matching
// source text, so formatting and policy serialization cannot evade the check.
func CompilePolicySubset(policyID string, policy *cedar.Policy) (FilterTemplate, error) {
	if policy == nil {
		return FilterTemplate{}, errors.New("nil policy")
	}
	ast := policy.AST()
	if ast.Effect != cedarast.EffectPermit {
		return FilterTemplate{}, errors.New("forbid policies are not list-filter templates")
	}

	principal, ok := ast.Principal.(cedarast.ScopeTypeIs)
	if !ok {
		return FilterTemplate{}, errors.New("principal must use an entity type")
	}
	action, ok := ast.Action.(cedarast.ScopeTypeEq)
	if !ok || action.Entity.Type != "Action" {
		return FilterTemplate{}, errors.New("action must be an Action entity equality")
	}
	resource, ok := ast.Resource.(cedarast.ScopeTypeIs)
	if !ok {
		return FilterTemplate{}, errors.New("resource must use an entity type")
	}
	if len(ast.Conditions) > 1 {
		return FilterTemplate{}, errors.New("only one condition is in the subset")
	}

	condition := ""
	if len(ast.Conditions) == 1 {
		if ast.Conditions[0].Condition != cedarast.ConditionWhen {
			return FilterTemplate{}, errors.New("unless conditions are not in the subset")
		}
		switch node := ast.Conditions[0].Body.(type) {
		case cedarast.NodeTypeAccess:
			if !isPrincipalAdmin(node) {
				return FilterTemplate{}, errors.New("condition is not principal.admin")
			}
			condition = "principal.admin"
		case cedarast.NodeTypeIn:
			if !isSponsoredProjectMembership(node) {
				return FilterTemplate{}, errors.New("condition is not resource in principal.sponsor.involvedIn")
			}
			condition = "resource in principal.sponsor.involvedIn"
		default:
			return FilterTemplate{}, fmt.Errorf("condition node %T is outside the subset", ast.Conditions[0].Body)
		}
	}

	template := FilterTemplate{
		PolicyID:     policyID,
		Action:       string(action.Entity.ID),
		ResourceKind: string(resource.Type),
		SQL:          sqlForSubset(string(principal.Type), string(action.Entity.ID), string(resource.Type), condition),
	}
	switch condition {
	case "":
		template.predicate = func(p Principal, r Resource) bool {
			return p.Kind == string(principal.Type) && r.Kind == string(resource.Type)
		}
	case "principal.admin":
		template.predicate = func(p Principal, r Resource) bool {
			return p.Kind == string(principal.Type) && p.Admin && r.Kind == string(resource.Type)
		}
	case "resource in principal.sponsor.involvedIn":
		template.predicate = func(p Principal, r Resource) bool {
			if p.Kind != string(principal.Type) || r.Kind != string(resource.Type) {
				return false
			}
			for _, projectID := range p.InvolvedProjects {
				if projectID == r.ID {
					return true
				}
			}
			return false
		}
	}
	return template, nil
}

func sqlForSubset(principalKind, action, resourceKind, condition string) string {
	base := fmt.Sprintf("action = '%s' AND resource_kind = '%s' AND principal_kind = '%s'", action, resourceKind, principalKind)
	switch condition {
	case "principal.admin":
		return base + " AND principal_admin"
	case "resource in principal.sponsor.involvedIn":
		return base + " AND EXISTS (SELECT 1 FROM plugin_grant g WHERE g.subject = principal_sponsor AND g.scope = resource_id AND g.role = 'involved')"
	default:
		return base
	}
}

func isPrincipalAdmin(node cedarast.NodeTypeAccess) bool {
	variable, ok := node.Arg.(cedarast.NodeTypeVariable)
	return ok && variable.Name == "principal" && node.Value == "admin"
}

func isSponsoredProjectMembership(node cedarast.NodeTypeIn) bool {
	left, ok := node.Left.(cedarast.NodeTypeVariable)
	if !ok || left.Name != "resource" {
		return false
	}
	right, ok := node.Right.(cedarast.NodeTypeAccess)
	if !ok || right.Value != "involvedIn" {
		return false
	}
	sponsor, ok := right.Arg.(cedarast.NodeTypeAccess)
	if !ok || sponsor.Value != "sponsor" {
		return false
	}
	principal, ok := sponsor.Arg.(cedarast.NodeTypeVariable)
	return ok && principal.Name == "principal"
}

// CompileList binds no principal. This is the plan-time operation; the
// returned templates bind a principal only when Match or Filter is called.
func (s *CP7PolicySet) CompileList(action, resourceKind string) (CompiledList, error) {
	if s == nil {
		return CompiledList{}, errors.New("nil policy set")
	}
	list := CompiledList{Action: action, ResourceKind: resourceKind}
	for _, template := range s.Templates {
		if template.Action == action && template.ResourceKind == resourceKind {
			list.Templates = append(list.Templates, template)
		}
	}
	sort.Slice(list.Templates, func(i, j int) bool { return list.Templates[i].PolicyID < list.Templates[j].PolicyID })
	return list, nil
}

// Filter compiles the relevant list shape and evaluates it for principal. An
// empty list is a valid deny-all shape: no CP7 policy covers that action/kind.
func (s *CP7PolicySet) Filter(principal Principal, action string, resources []Resource) ([]Resource, error) {
	kind := ""
	if len(resources) > 0 {
		kind = resources[0].Kind
	}
	list, err := s.CompileList(action, kind)
	if err != nil {
		return nil, err
	}
	return list.Filter(principal, resources), nil
}

// Authorize evaluates a request through cedar-go and returns its decision and
// diagnostic. Callers can inspect Diagnostic.Errors to distinguish a missing
// entity/attribute from an ordinary deny.
func (s *CP7PolicySet) Authorize(req EvaluationRequest) (cedar.Decision, cedar.Diagnostic, error) {
	if s == nil || s.Cedar == nil {
		return cedar.Deny, cedar.Diagnostic{}, errors.New("nil policy set")
	}
	entities := entitiesFor(req)
	decision, diagnostic := cedar.Authorize(s.Cedar, entities, cedar.Request{
		Principal: entityUID(req.Principal.Kind, req.Principal.ID),
		Action:    entityUID("Action", req.Action),
		Resource:  entityUID(req.Resource.Kind, req.Resource.ID),
	})
	return decision, diagnostic, nil
}

// AuthorizeRequest is a convenience form that returns only allow/deny and an
// error. The diagnostic form remains available through Authorize.
func (s *CP7PolicySet) AuthorizeRequest(req EvaluationRequest) (bool, error) {
	decision, diag, err := s.Authorize(req)
	if err != nil {
		return false, err
	}
	if len(diag.Errors) > 0 {
		return false, fmt.Errorf("cedar evaluation error: %s", diag.Errors[0].Message)
	}
	return decision == cedar.Allow, nil
}

// DifferentialMismatch identifies one compiled-list/Cedar disagreement.
type DifferentialMismatch struct {
	Principal Principal
	Action    string
	Resource  Resource
	Compiled  bool
	Cedar     cedar.Decision
}

func (m DifferentialMismatch) Error() string {
	return fmt.Sprintf("compiled decision %v differs from Cedar %v for %s/%s %s/%s action %q", m.Compiled, m.Cedar, m.Principal.Kind, m.Principal.ID, m.Resource.Kind, m.Resource.ID, m.Action)
}

// DifferentialCheck compares the compiled list result to cedar-go's per-item
// decision for every supplied principal, action, and resource. It is intended
// for a small deterministic sample in tests and for each storage driver.
func DifferentialCheck(s *CP7PolicySet, principals []Principal, actions []string, resources []Resource) error {
	if s == nil {
		return errors.New("nil policy set")
	}
	for _, principal := range principals {
		for _, action := range actions {
			for _, resource := range resources {
				list, err := s.CompileList(action, resource.Kind)
				if err != nil {
					return err
				}
				compiled := list.Match(principal, resource)
				decision, diag, err := s.Authorize(EvaluationRequest{Principal: principal, Action: action, Resource: resource})
				if err != nil {
					return err
				}
				if len(diag.Errors) != 0 {
					return fmt.Errorf("cedar diagnostic for %s/%s %s/%s: %s", principal.Kind, principal.ID, resource.Kind, resource.ID, diag.Errors[0].Message)
				}
				if (decision == cedar.Allow) != compiled {
					return DifferentialMismatch{Principal: principal, Action: action, Resource: resource, Compiled: compiled, Cedar: decision}
				}
			}
		}
	}
	return nil
}

func entityUID(kind, id string) cedar.EntityUID {
	return cedar.NewEntityUID(cedar.EntityType(kind), cedar.String(id))
}

func entitiesFor(req EvaluationRequest) cedar.EntityMap {
	entities := cedar.EntityMap{}
	principalUID := entityUID(req.Principal.Kind, req.Principal.ID)
	resourceUID := entityUID(req.Resource.Kind, req.Resource.ID)
	actionUID := entityUID("Action", req.Action)
	entities[actionUID] = cedar.Entity{UID: actionUID}
	entities[resourceUID] = cedar.Entity{UID: resourceUID}
	if req.Resource.ParentID != "" {
		parentUID := entityUID("Project", req.Resource.ParentID)
		entities[parentUID] = cedar.Entity{UID: parentUID}
		entities[resourceUID] = cedar.Entity{UID: resourceUID, Parents: cedar.NewEntityUIDSet(parentUID)}
	}

	involved := make([]cedar.Value, 0, len(req.Principal.InvolvedProjects))
	for _, id := range req.Principal.InvolvedProjects {
		uid := entityUID("Project", id)
		involved = append(involved, uid)
		if _, ok := entities[uid]; !ok {
			entities[uid] = cedar.Entity{UID: uid}
		}
	}
	userID := req.Principal.ID
	if req.Principal.Kind == "Agent" {
		userID = req.Principal.SponsorID
	}
	if userID != "" {
		userUID := entityUID("User", userID)
		entities[userUID] = cedar.Entity{
			UID: userUID,
			Attributes: cedar.NewRecord(cedar.RecordMap{
				"admin":      cedar.Boolean(req.Principal.Admin),
				"involvedIn": cedar.NewSet(involved...),
			}),
		}
		if req.Principal.Kind == "Agent" {
			entities[principalUID] = cedar.Entity{
				UID: principalUID,
				Attributes: cedar.NewRecord(cedar.RecordMap{
					"sponsor": userUID,
				}),
			}
		}
	} else {
		entities[principalUID] = cedar.Entity{UID: principalUID}
	}
	return entities
}

// Ensure a malformed Cedar policy cannot accidentally be accepted by an
// implementation that only invokes CompilePolicySubset directly.
func init() {
	for _, spec := range CP7Policies {
		if strings.TrimSpace(spec.Name) == "" || strings.TrimSpace(spec.Cedar) == "" {
			panic("invalid CP7 policy specification")
		}
	}
}
