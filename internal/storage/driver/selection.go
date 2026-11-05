package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

const (
	// Version 2 adds explicit optional presence and the kernel boolean template.
	// Older annotation layouts are unavailable; never reinterpret their bytes.
	SelectionIndexVersion = 2
	SelectionOrdinaryCap  = 20000
	SelectionSummaryCap   = 100000
	SelectionGroupCap     = 100
	SelectionMaxDepth     = 32
	SelectionMaxNodes     = 4096
)

// SelectionReader is optional and internal. A selector is pinned to current
// authority at reader creation; historical content has its own revision. Core
// must still fence fresh authority before publishing. This is not a terminal
// after-Head freshness primitive or a public query/SQL interface.
//
// Acceleration requires version-compatible COMPLETE bead projection coverage
// at BOTH selected revisions: exactly one source-valid counting row per live
// bead, including unrelated/hidden sources. Missing, duplicate or stale rows,
// unbuilt ranges and incompatible layouts refuse BEFORE boolean evaluation,
// even for empty/probe requests. SQL maintains this proof atomically with ALL
// record, derived and control writes; it cannot infer coverage by setting a
// marker, inspecting final bodies or building only the graph source indexes.
// Initial population, deletes, imports/restores and invalidation must preserve
// the same temporal/certified-versus-unbuilt source-index contract. Memory's
// full census is the differential oracle, not a proposed SQL scan fallback.
// Children/Incoming use the EXISTING parent/link indexes, not derived edge
// copies, and require their independent source domains at the selected revision.
// Coverage is node-relevant and eager: Children requires Parent coverage;
// Incoming requires Links coverage. Every referenced arm is checked even under
// false/NOT, empty inputs or a zero-row probe. Membership uses content revision;
// Authorization and HostExpression use authority revision. A staged explicit
// Parent declaration suffices for Children; only Incoming requires full Links.
// Plans without either relation node do not require those source domains.
// Only emitted thin keys are returned; SQL payload lookahead is forbidden.
type SelectionReader interface {
	Select(context.Context, SelectionPlan) (SelectionResult, error)
}

type SelectionMode string

const (
	SelectionPageMode  SelectionMode = "page"
	SelectionCountMode SelectionMode = "count"
	// SelectionTypeSummary is reserved for a trusted compiler's fully compiled
	// explicit fields=[]/groupBy=type request. No caller is enabled by this API.
	SelectionTypeSummary SelectionMode = "type_summary"
)

type SelectionGroup string

const SelectionGroupType SelectionGroup = "type"

type SelectionPlan struct {
	Incarnation                        string
	Branch                             model.Branch
	ContentRevision, AuthorityRevision model.Revision
	// ContentCommitted selects the committed image even when historical content
	// has the SAME revision token as a transaction overlay. Authority remains
	// the pinned current overlay. It does not remove source/control evidence.
	ContentCommitted          bool
	Membership, Authorization SelectionExpr
	Boundary                  *SelectionBoundary
	// Excluded is the existing prepared-source suppression plane: canonical
	// keys are excluded from committed AND staged candidates before membership,
	// cap, page and aggregates. It does not erase authority/control evidence.
	// Core prepared replacement images are NOT included by this interface.
	// Exact aggregates over those images require a complete pre-cap merge in a
	// future trusted adapter; suppression alone cannot claim that result.
	Excluded []model.Ref
	Order    []SelectionOrder
	Mode     SelectionMode
	GroupBy  SelectionGroup
	// Cap may tighten, but never expand, the mode's default maximum.
	Cap  int
	Page *SelectionPage
}
type SelectionPage struct {
	Limit  int
	Offset uint64
	After  *SelectionKeyset
}
type SelectionColumn string

const (
	SelectionID       SelectionColumn = "id"
	SelectionType     SelectionColumn = "type"
	SelectionProject  SelectionColumn = "project"
	SelectionStatus   SelectionColumn = "status"
	SelectionTitle    SelectionColumn = "title"
	SelectionRevision SelectionColumn = "revision"
	SelectionParent   SelectionColumn = "parent"
	SelectionTemplate SelectionColumn = "template"
	SelectionPriority SelectionColumn = "priority"
)

type SelectionOrder struct {
	Column SelectionColumn
	Desc   bool
}

// Exactly one value arm is populated, including false, zero and empty string.
type SelectionValue struct {
	String *string
	Bool   *bool
	Int    *int64
	Ref    *model.Ref
}

func SelectionString(s string) SelectionValue { return SelectionValue{String: &s} }
func SelectionBool(b bool) SelectionValue     { return SelectionValue{Bool: &b} }
func SelectionInt(n int64) SelectionValue     { return SelectionValue{Int: &n} }
func SelectionRef(r model.Ref) SelectionValue { return SelectionValue{Ref: &r} }

type SelectionOp string

const (
	SelectionTrue           SelectionOp = "true"
	SelectionFalse          SelectionOp = "false"
	SelectionAll            SelectionOp = "all"
	SelectionAny            SelectionOp = "any"
	SelectionNot            SelectionOp = "not"
	SelectionCompareOp      SelectionOp = "compare"
	SelectionRefs           SelectionOp = "refs"
	SelectionExistsOp       SelectionOp = "exists"
	SelectionResourceExists SelectionOp = "resource_exists"
	SelectionChildrenOp     SelectionOp = "children"
	SelectionIncomingOp     SelectionOp = "incoming"
)

type SelectionExpr struct {
	Op      SelectionOp
	Args    []SelectionExpr
	Compare *SelectionCompare
	Refs    []model.Ref
	Exists  *SelectionExists
	Sources *SelectionSources
}

// Children uses Refs as canonical parents and forbids Kinds. Incoming uses
// the union of closed target/kind pairs. Empty refs/kinds means no membership,
// after all coverage checks; neither empty vector broadens the relation.
type SelectionSources struct {
	Refs  []model.Ref
	Kinds []IncomingLinkKind
}
type SelectionCompare struct {
	Column SelectionColumn
	Op     string // eq | contains (literal, case-sensitive title) | absent
	Value  *SelectionValue
}
type SelectionExists struct {
	Row   DerivedRowKey
	Kind  SelectionFactKind
	Tests []SelectionFactTest
}
type SelectionFactColumn string

const (
	SelectionFactRef         SelectionFactColumn = "ref"
	SelectionFactTarget      SelectionFactColumn = "target"
	SelectionFactSubject     SelectionFactColumn = "subject"
	SelectionFactScope       SelectionFactColumn = "scope"
	SelectionFactScopeKind   SelectionFactColumn = "scope_kind"
	SelectionFactProjectName SelectionFactColumn = "project_name"
	SelectionFactRole        SelectionFactColumn = "role"
	SelectionFactName        SelectionFactColumn = "name"
	SelectionFactValue       SelectionFactColumn = "value"
	SelectionFactActive      SelectionFactColumn = "active"
	SelectionFactDepth       SelectionFactColumn = "depth"
	SelectionFactLinkKind    SelectionFactColumn = "link_kind"
)

type SelectionFactTest struct {
	Column    SelectionFactColumn
	Value     *SelectionValue
	Candidate SelectionColumn
}
type SelectionFactKind string

const (
	SelectionLabelFact    SelectionFactKind = "label"
	SelectionAncestorFact SelectionFactKind = "ancestor"
	SelectionLinkFact     SelectionFactKind = "link"
	SelectionGrantFact    SelectionFactKind = "grant"
	// This separate, versioned complete domain never upgrades generic grant
	// rows (including an empty set) into authoritative fixed-scope metadata.
	// It supports direct Bead-project grant leaves and exact scope literals.
	// It does not represent the same-team member/active/involved join; a full
	// policy requiring that join remains check-only/unavailable, even if the
	// installed facts currently contain no team grants.
	SelectionScopedGrantFact SelectionFactKind = "scoped_grant_v1"
	SelectionProjectFact     SelectionFactKind = "project"
	SelectionTeamFact        SelectionFactKind = "team"
	SelectionOntologyFact    SelectionFactKind = "ontology"
	SelectionReadyFact       SelectionFactKind = "ready"
	SelectionPluginFact      SelectionFactKind = "plugin"
)

type SelectionScopeKind string

const (
	SelectionAuthorityScope SelectionScopeKind = "authority"
	SelectionTeamScope      SelectionScopeKind = "team"
	SelectionProjectScope   SelectionScopeKind = "project"
)

// A complete fact set belongs to ONE existing logical row. An empty set is
// known absence only with a valid envelope and source observation. Absent or
// incompatible coverage is an error even below NOT or an unreachable branch.
type SelectionFacts struct {
	// Kind declares exactly the domain for which Rows is complete, even empty.
	Kind SelectionFactKind
	Rows []SelectionFact
}
type SelectionFact struct {
	Kind                              SelectionFactKind
	Ref, Target                       *model.Ref
	Link                              *SourceLink
	Subject, Scope, Role, Name, Value string
	Active                            bool
	Depth                             uint64
	// Only scoped_grant_v1 owns these fields. The producer derives them from
	// the SAME complete installed source/registry as the unchanged opaque
	// Subject/Scope/Role. Storage validates shape and source binding; it does
	// not decode those opaque strings or prove redundant semantic agreement.
	// Omit these newly added zero fields when serializing legacy generic facts,
	// whose logical row Value may be encoding/json of this exact struct.
	ScopeKind SelectionScopeKind `json:",omitempty"`
	ScopeRef  *model.Ref         `json:",omitempty"` // Canonical fixed-scope metadata, not a query column.
	// ProjectName is nonnil for a project scope and is its exact immutable
	// registry NAME. Name != ScopeRef.ID is valid. It is nil for authority/team.
	// Nil is never equal to an empty string. Matching CandidateProject covers
	// Bead targets only: Project targets use their canonical Ref and History
	// has no targetProject. There is no global wildcard or bead scope kind.
	ProjectName *string `json:",omitempty"`
}
type SelectionBead struct {
	Ref                 model.Ref
	Project             *string
	Parent              *model.Ref
	Type, Status, Title string
	Template            bool
	Priority            int64
	MembershipLeaf      string
	// Absent declares optional semantic fields that are not present. Values
	// for an absent field must be zero. Nil explicitly declares value fields
	// present, including empty/0. Null/wrong-type values are not coerced: a
	// producer unable to preserve semantics must withhold annotation/coverage.
	// Project and Parent retain their separate pointer presence representation.
	Absent []SelectionColumn
}
type SelectionSource struct {
	Key      LockKey
	Revision model.Revision
	Present  bool
	Digest   string
}

// DerivedSelectionRow carries a certified annotation without changing its
// existing logical table/key/value identity. Activation/publication owns family
// completeness separately; a slice of rows alone is never a coverage marker.
type DerivedSelectionRow struct {
	Table DerivedTable
	Row   DerivedRow
}

func (r DerivedSelectionRow) Validate() error {
	if r.Row.Index == nil {
		return selectionError("certified row requires annotation")
	}
	return r.Row.Index.Validate(r.Table, r.Row)
}
func (r DerivedSelectionRow) Clone() DerivedSelectionRow {
	return DerivedSelectionRow{Table: r.Table, Row: r.Row.Clone()}
}

type SelectionIndex struct {
	Version     uint8
	Row         DerivedRowKey
	Incarnation string
	Branch      model.Branch
	ValueDigest string
	Source      SelectionSource
	Bead        *SelectionBead
	Facts       *SelectionFacts
	Queue       *QueueFacts `json:",omitempty"`
}
type SelectionKey struct {
	Ref            model.Ref
	Revision       model.Revision
	Order          []SelectionValue
	MembershipLeaf string
}
type SelectionKeyset struct {
	Binding string
	Key     SelectionKey
}
type SelectionTypeGroup struct {
	Type          string
	Count, Elided uint64
}
type SelectionResult struct {
	Keys   []SelectionKey
	Total  uint64
	Groups []SelectionTypeGroup
	More   bool
	Next   *SelectionKeyset
}

// ErrSelectionCap proves only cap+1 membership, never an exact partial count.
type ErrSelectionCap struct {
	Limit  int
	Groups bool
}

func (e ErrSelectionCap) Error() string {
	return fmt.Sprintf("selection cap exceeded: %d (groups=%t)", e.Limit, e.Groups)
}

func SelectionDigest(data []byte) string {
	s := sha256.Sum256(data)
	return hex.EncodeToString(s[:])
}
func SelectionSameRef(a, b model.Ref) bool {
	x, ex := EncodeLockKey(LockKey{Kind: LockRecord, Ref: &a})
	y, ey := EncodeLockKey(LockKey{Kind: LockRecord, Ref: &b})
	return ex == nil && ey == nil && x == y
}
func selectionError(s string) error { return fmt.Errorf("%w: %s", ErrSelectionInvalid, s) }

func valueKind(v SelectionValue) (string, error) {
	n, kind := 0, ""
	if v.String != nil {
		n++
		kind = "string"
	}
	if v.Bool != nil {
		n++
		kind = "bool"
	}
	if v.Int != nil {
		n++
		kind = "int"
	}
	if v.Ref != nil {
		n++
		kind = "ref"
		if err := ValidateHierarchyRef(*v.Ref); err != nil {
			return "", err
		}
	}
	if n != 1 {
		return "", selectionError("value must have exactly one arm")
	}
	return kind, nil
}
func columnKind(c SelectionColumn) (string, error) {
	switch c {
	case SelectionID, SelectionType, SelectionProject, SelectionStatus, SelectionTitle, SelectionRevision:
		return "string", nil
	case SelectionTemplate:
		return "bool", nil
	case SelectionParent:
		return "ref", nil
	case SelectionPriority:
		return "int", nil
	default:
		return "", selectionError("unknown bead column")
	}
}
func factColumns(kind SelectionFactKind) (map[SelectionFactColumn]string, error) {
	switch kind {
	case SelectionLabelFact:
		return map[SelectionFactColumn]string{SelectionFactRef: "ref", SelectionFactName: "string", SelectionFactValue: "string"}, nil
	case SelectionAncestorFact:
		return map[SelectionFactColumn]string{SelectionFactRef: "ref", SelectionFactTarget: "ref", SelectionFactDepth: "int"}, nil
	case SelectionLinkFact:
		return map[SelectionFactColumn]string{SelectionFactRef: "ref", SelectionFactTarget: "ref", SelectionFactLinkKind: "string"}, nil
	case SelectionGrantFact:
		return map[SelectionFactColumn]string{SelectionFactSubject: "string", SelectionFactScope: "string", SelectionFactRole: "string"}, nil
	case SelectionScopedGrantFact:
		return map[SelectionFactColumn]string{SelectionFactSubject: "string", SelectionFactScope: "string", SelectionFactRole: "string", SelectionFactScopeKind: "string", SelectionFactProjectName: "string"}, nil
	case SelectionProjectFact, SelectionTeamFact:
		return map[SelectionFactColumn]string{SelectionFactName: "string", SelectionFactActive: "bool"}, nil
	case SelectionOntologyFact, SelectionPluginFact:
		return map[SelectionFactColumn]string{SelectionFactName: "string", SelectionFactValue: "string"}, nil
	case SelectionReadyFact:
		return map[SelectionFactColumn]string{SelectionFactRef: "ref", SelectionFactName: "string", SelectionFactActive: "bool"}, nil
	default:
		return nil, selectionError("unknown fact layout")
	}
}
func factTable(kind SelectionFactKind, table DerivedTable) bool {
	switch kind {
	case SelectionLabelFact, SelectionProjectFact, SelectionTeamFact:
		return table == DerivedAttribute
	case SelectionAncestorFact:
		return table == DerivedAncestorClosure || table == DerivedCutClosure
	case SelectionLinkFact:
		return table == DerivedReferenceIndex
	case SelectionGrantFact, SelectionScopedGrantFact:
		return table == DerivedGrant
	case SelectionOntologyFact:
		return table == DerivedOntology
	case SelectionReadyFact:
		return table == DerivedReadyState
	case SelectionPluginFact:
		return table == DerivedPlugins
	default:
		return false
	}
}

func validateExpr(e SelectionExpr, depth int, nodes *int) error {
	*nodes++
	if depth > SelectionMaxDepth || *nodes > SelectionMaxNodes {
		return selectionError("expression budget exceeded")
	}
	used := SelectionExpr{Op: e.Op}
	switch e.Op {
	case SelectionTrue, SelectionFalse, SelectionResourceExists:
	case SelectionAll, SelectionAny, SelectionNot:
		if e.Op == SelectionNot && len(e.Args) != 1 {
			return selectionError("not requires one child")
		}
		if len(e.Args) > SelectionMaxNodes {
			return selectionError("too many children")
		}
		used.Args = e.Args
		for _, child := range e.Args {
			if err := validateExpr(child, depth+1, nodes); err != nil {
				return err
			}
		}
	case SelectionCompareOp:
		if e.Compare == nil {
			return selectionError("missing comparison")
		}
		used.Compare = e.Compare
		c := e.Compare
		kind, err := columnKind(c.Column)
		if err != nil {
			return err
		}
		switch c.Op {
		case "absent":
			if c.Value != nil || !selectionOptionalColumn(c.Column) {
				return selectionError("invalid absence comparison")
			}
		case "eq", "contains":
			if c.Value == nil {
				return selectionError("missing comparison operand")
			}
			vk, err := valueKind(*c.Value)
			if err != nil {
				return err
			}
			if vk != kind || (c.Op == "contains" && c.Column != SelectionTitle) {
				return selectionError("comparison type mismatch")
			}
		default:
			return selectionError("unknown comparison operator")
		}
	case SelectionRefs:
		if len(e.Refs) > SelectionMaxNodes {
			return selectionError("refs vector too large")
		}
		used.Refs = e.Refs
		for _, ref := range e.Refs {
			if err := ValidateHierarchyRef(ref); err != nil {
				return err
			}
		}
	case SelectionExistsOp:
		if e.Exists == nil {
			return selectionError("missing exists")
		}
		used.Exists = e.Exists
		x := e.Exists
		if err := x.Row.Validate(); err != nil {
			return err
		}
		cols, err := factColumns(x.Kind)
		if err != nil {
			return err
		}
		if !factTable(x.Kind, x.Row.Table) || len(x.Tests) > SelectionMaxNodes {
			return selectionError("incompatible exists layout")
		}
		for _, t := range x.Tests {
			want, ok := cols[t.Column]
			if !ok {
				return selectionError("unknown fact column")
			}
			if (t.Value == nil) == (t.Candidate == "") {
				return selectionError("fact operand must have one arm")
			}
			var got string
			if t.Value != nil {
				got, err = valueKind(*t.Value)
			} else if t.Candidate == SelectionID && want == "ref" {
				got = "ref"
			} else {
				got, err = columnKind(t.Candidate)
			}
			if err != nil {
				return err
			}
			if got != want {
				return selectionError("fact operand type mismatch")
			}
			if x.Kind == SelectionScopedGrantFact {
				// Only the registry name has a candidate mapping in this closed
				// Bead selector. Opaque identity/role and scope kind are exact
				// trusted constants, never synthesized from a candidate ID/name.
				if t.Candidate != "" && (t.Column != SelectionFactProjectName || t.Candidate != SelectionProject) {
					return selectionError("unsupported scoped grant candidate mapping")
				}
				if t.Column == SelectionFactScopeKind && !selectionScopeKind(SelectionScopeKind(*t.Value.String)) {
					return selectionError("unknown scoped grant scope kind")
				}
			}
		}
	case SelectionChildrenOp, SelectionIncomingOp:
		if e.Sources == nil {
			return selectionError("missing source membership")
		}
		used.Sources = e.Sources
		if e.Op == SelectionChildrenOp {
			if e.Sources.Kinds != nil {
				return selectionError("children has incoming kinds")
			}
			if _, err := NormalizeHierarchyRequest(e.Sources.Refs, 0); err != nil {
				return selectionError(err.Error())
			}
		} else if _, err := NormalizeInverseSourceLinksRequest(e.Sources.Refs, e.Sources.Kinds, nil, 0); err != nil {
			return selectionError(err.Error())
		}
	default:
		return selectionError("unknown expression")
	}
	if !reflect.DeepEqual(used, e) {
		return selectionError("unused expression arm")
	}
	return nil
}

func (p SelectionPlan) EffectiveCap() int {
	if p.Cap != 0 {
		return p.Cap
	}
	if p.Mode == SelectionTypeSummary {
		return SelectionSummaryCap
	}
	return SelectionOrdinaryCap
}

// Validate validates EVERY branch before any boolean or storage evaluation.
func (p SelectionPlan) Validate() error {
	err := p.validate()
	if err != nil && !errors.Is(err, ErrSelectionInvalid) && !errors.Is(err, ErrSelectionCheckOnly) {
		return fmt.Errorf("%w: %v", ErrSelectionInvalid, err)
	}
	return err
}

func (p SelectionPlan) validate() error {
	if p.Incarnation == "" || p.Branch.Validate() != nil || !p.Branch.IsZero() {
		return selectionError("invalid snapshot identity")
	}
	c, err := p.ContentRevision.Uint64()
	if err != nil {
		return err
	}
	a, err := p.AuthorityRevision.Uint64()
	if err != nil {
		return err
	}
	if c > a {
		return selectionError("content is newer than authority")
	}
	nodes := 0
	if err := validateExpr(p.Membership, 0, &nodes); err != nil {
		return err
	}
	if err := validateExpr(p.Authorization, 0, &nodes); err != nil {
		return err
	}
	boundary, err := p.boundary()
	if err != nil {
		return err
	}
	if len(p.Excluded) > HierarchyMaxExcluded {
		return selectionError("source exclusion vector too large")
	}
	for _, ref := range p.Excluded {
		if err := ValidateHierarchyRef(ref); err != nil {
			return selectionError(err.Error())
		}
	}
	if len(p.Order) > 16 {
		return selectionError("too many order columns")
	}
	for _, o := range p.Order {
		if _, err := columnKind(o.Column); err != nil {
			return err
		}
		if o.Column == SelectionParent || o.Column == SelectionTemplate {
			return selectionError("unsupported order column")
		}
	}
	maximum := SelectionOrdinaryCap
	switch p.Mode {
	case SelectionPageMode:
		if p.Page == nil || (p.GroupBy != "" && p.GroupBy != SelectionGroupType) {
			return selectionError("page requires page request and closed group")
		}
	case SelectionCountMode:
		if p.Page != nil || p.GroupBy != "" {
			return selectionError("count cannot page or group")
		}
	case SelectionTypeSummary:
		if p.Page != nil || p.GroupBy != SelectionGroupType {
			return selectionError("summary requires only type groups")
		}
		maximum = SelectionSummaryCap
	default:
		return selectionError("unknown mode")
	}
	if p.Cap < 0 || p.EffectiveCap() > maximum {
		return selectionError("invalid membership cap")
	}
	if p.Page != nil {
		if p.Page.Limit < 0 || p.Page.Limit > 10000 {
			return selectionError("invalid page limit")
		}
		if p.Page.After != nil {
			if p.Page.Offset != 0 {
				return selectionError("keyset and offset are exclusive")
			}
			k := p.Page.After
			if err := ValidateHierarchyRef(k.Key.Ref); err != nil {
				return err
			}
			if _, err := k.Key.Revision.Uint64(); err != nil {
				return err
			}
			if len(k.Key.Order) != len(p.Order) {
				return selectionError("keyset order arity")
			}
			for i, v := range k.Key.Order {
				got, err := valueKind(v)
				if err != nil {
					return err
				}
				want, _ := columnKind(p.Order[i].Column)
				if got != want {
					return selectionError("keyset order type")
				}
			}
			b, err := p.Binding()
			if err != nil {
				return err
			}
			if k.Binding != b {
				return selectionError("keyset binding mismatch")
			}
		}
	}
	if boundary.Host == SelectionHostCheckOnly {
		return SelectionCheckOnlyError{CandidateCap: SelectionCheckOnlyCap}
	}
	return nil
}
func (p SelectionPlan) Binding() (string, error) {
	// Paging state is not membership. Limits and offsets may change while the
	// same immutable selection is traversed.
	p.Page = nil
	nodes := 0
	if err := validateExpr(p.Membership, 0, &nodes); err != nil {
		return "", err
	}
	if err := validateExpr(p.Authorization, 0, &nodes); err != nil {
		return "", err
	}
	if _, err := p.boundary(); err != nil {
		return "", err
	}
	seal, err := p.Boundary.GobEncode()
	if err != nil {
		return "", err
	}
	p.Boundary = nil
	encoded, err := selectionBytes(struct {
		Plan     SelectionPlan
		Boundary []byte
	}{p, seal})
	if err != nil {
		return "", err
	}
	return SelectionDigest(encoded), nil
}

func canonicalDigest(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 32 && hex.EncodeToString(b) == s
}
func (index *SelectionIndex) Validate(table DerivedTable, row DerivedRow) error {
	if index == nil {
		return nil
	}
	if index.Version != SelectionIndexVersion || index.Row.Table != table || index.Row.Key != row.Key || index.Incarnation == "" || index.Branch.Validate() != nil || !index.Branch.IsZero() {
		return selectionError("index envelope mismatch")
	}
	if err := index.Row.Validate(); err != nil {
		return err
	}
	if index.ValueDigest != SelectionDigest(row.Value) {
		return selectionError("index logical value digest mismatch")
	}
	if err := index.Source.Validate(); err != nil {
		return err
	}
	if (index.Bead == nil) == (index.Facts == nil) {
		return selectionError("index must have one layout")
	}
	if index.Queue != nil {
		if index.Bead == nil || index.Queue.Version != QueueFactsVersion {
			return selectionError("invalid queue annotation layout")
		}
		for _, link := range index.Queue.Links {
			targetKind, err := normalizeQueueTargetKind(link.TargetKind)
			if err != nil {
				return selectionError("invalid queue link target kind")
			}
			if err := validateQueueTargetKind(targetKind, link.To); err != nil || validateQueueLinkKind(link.Kind) != nil {
				return selectionError("invalid queue link")
			}
			if targetKind != "external" {
				value := SourceLink{Kind: link.Kind, To: link.To}
				if validateFact(DerivedReferenceIndex, SelectionFact{Kind: SelectionLinkFact, Ref: &index.Bead.Ref, Link: &value}) != nil {
					return selectionError("invalid queue link")
				}
			}
			if link.Metadata != nil {
				if _, err := link.Metadata.Normalized(); err != nil {
					return selectionError("invalid queue link metadata")
				}
			}
		}
	}
	if index.Bead != nil {
		b := index.Bead
		if table != DerivedSortKey || !index.Source.Present || index.Source.Key.Kind != LockRecord || index.Source.Key.Ref == nil || !SelectionSameRef(*index.Source.Key.Ref, b.Ref) {
			return selectionError("bead source mismatch")
		}
		if err := ValidateHierarchyRef(b.Ref); err != nil {
			return err
		}
		if b.Parent != nil {
			if err := ValidateHierarchyRef(*b.Parent); err != nil {
				return err
			}
		}
		seen := map[SelectionColumn]bool{}
		for _, c := range b.Absent {
			if seen[c] {
				return selectionError("duplicate absent field")
			}
			seen[c] = true
			switch c {
			case SelectionStatus:
				if b.Status != "" {
					return selectionError("absent status has value")
				}
			case SelectionTitle:
				if b.Title != "" {
					return selectionError("absent title has value")
				}
			case SelectionTemplate:
				if b.Template {
					return selectionError("absent template has value")
				}
			case SelectionPriority:
				if b.Priority != 0 {
					return selectionError("absent priority has value")
				}
			default:
				return selectionError("invalid absent field")
			}
		}
	} else {
		if !factTable(index.Facts.Kind, table) {
			return selectionError("fact completeness domain mismatch")
		}
		if len(index.Facts.Rows) > SelectionSummaryCap || (!index.Source.Present && len(index.Facts.Rows) != 0) {
			return selectionError("invalid complete fact set")
		}
		for _, f := range index.Facts.Rows {
			if f.Kind != index.Facts.Kind {
				return selectionError("mixed fact completeness domains")
			}
			if err := validateFact(table, f); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateFact(table DerivedTable, f SelectionFact) error {
	if !factTable(f.Kind, table) {
		return selectionError("fact table mismatch")
	}
	used := SelectionFact{Kind: f.Kind}
	switch f.Kind {
	case SelectionLabelFact:
		used.Ref, used.Name, used.Value = f.Ref, f.Name, f.Value
		if f.Ref == nil {
			return selectionError("label ref missing")
		}
	case SelectionAncestorFact:
		used.Ref, used.Target, used.Depth = f.Ref, f.Target, f.Depth
		if f.Ref == nil || f.Target == nil || (f.Ref.ID == f.Target.ID) != (f.Depth == 0) || f.Depth > uint64(1<<63-1) {
			return selectionError("ancestor shape")
		}
	case SelectionLinkFact:
		used.Ref, used.Link = f.Ref, f.Link
		if f.Ref == nil || f.Link == nil {
			return selectionError("link shape")
		}
		if err := ValidateGraphSourceRecord(Record{Ref: *f.Ref, Kind: "bead", Body: []byte(`{}`)}, nil, []SourceLink{*f.Link}); err != nil {
			return err
		}
	case SelectionGrantFact:
		used.Subject, used.Scope, used.Role = f.Subject, f.Scope, f.Role
		if f.Subject == "" || f.Role == "" {
			return selectionError("grant shape")
		}
	case SelectionScopedGrantFact:
		used.Subject, used.Scope, used.Role = f.Subject, f.Scope, f.Role
		used.ScopeKind, used.ScopeRef, used.ProjectName = f.ScopeKind, f.ScopeRef, f.ProjectName
		if f.Subject == "" || f.Scope == "" || f.Role == "" || !selectionScopeKind(f.ScopeKind) || f.ScopeRef == nil {
			return selectionError("scoped grant shape")
		}
		if err := f.ScopeRef.Validate(); err != nil || !f.ScopeRef.IsID() || !f.ScopeRef.Branch.IsZero() || !selectionScopeText(f.ScopeRef.ID) {
			return selectionError("scoped grant requires canonical fixed-scope ref")
		}
		if f.ScopeKind == SelectionProjectScope {
			if f.ProjectName == nil || !selectionScopeText(*f.ProjectName) {
				return selectionError("project scope requires exact registry name")
			}
		} else if f.ProjectName != nil {
			return selectionError("nonproject scope has registry project name")
		}
	case SelectionProjectFact, SelectionTeamFact:
		used.Name, used.Active = f.Name, f.Active
		if f.Name == "" {
			return selectionError("activity name missing")
		}
	case SelectionOntologyFact, SelectionPluginFact:
		used.Name, used.Value = f.Name, f.Value
		if f.Name == "" {
			return selectionError("fact name missing")
		}
	case SelectionReadyFact:
		used.Ref, used.Name, used.Active = f.Ref, f.Name, f.Active
		if f.Ref == nil {
			return selectionError("ready ref missing")
		}
	default:
		return selectionError("unknown fact")
	}
	if !reflect.DeepEqual(used, f) {
		return selectionError("unused fact field")
	}
	for _, r := range []*model.Ref{f.Ref, f.Target} {
		if r != nil {
			if err := ValidateHierarchyRef(*r); err != nil {
				return err
			}
		}
	}
	return nil
}

func selectionScopeKind(kind SelectionScopeKind) bool {
	return kind == SelectionAuthorityScope || kind == SelectionTeamScope || kind == SelectionProjectScope
}

func selectionScopeText(value string) bool {
	return value != "" && utf8.ValidString(value) && strings.TrimSpace(value) == value && !strings.ContainsRune(value, 0)
}

// Clone owns all slices, pointers and logical bytes at the storage boundary.
func (row DerivedRow) Clone() DerivedRow {
	out := DerivedRow{Key: row.Key, Value: bytes.Clone(row.Value)}
	if row.Index != nil {
		x := *row.Index
		if x.Queue != nil {
			q := *x.Queue
			q.Links = append([]QueueLink(nil), q.Links...)
			for i := range q.Links {
				if q.Links[i].Metadata != nil {
					v := *q.Links[i].Metadata
					q.Links[i].Metadata = &v
				}
			}
			x.Queue = &q
		}
		if x.Source.Key.Ref != nil {
			v := *x.Source.Key.Ref
			x.Source.Key.Ref = &v
		}
		if x.Source.Key.Derived != nil {
			v := *x.Source.Key.Derived
			x.Source.Key.Derived = &v
		}
		if x.Bead != nil {
			b := *x.Bead
			b.Absent = append([]SelectionColumn(nil), b.Absent...)
			if b.Project != nil {
				v := *b.Project
				b.Project = &v
			}
			if b.Parent != nil {
				v := *b.Parent
				b.Parent = &v
			}
			x.Bead = &b
		}
		if x.Facts != nil {
			f := &SelectionFacts{Kind: x.Facts.Kind, Rows: append([]SelectionFact(nil), x.Facts.Rows...)}
			for i := range f.Rows {
				r := &f.Rows[i]
				if r.Ref != nil {
					v := *r.Ref
					r.Ref = &v
				}
				if r.Target != nil {
					v := *r.Target
					r.Target = &v
				}
				if r.Link != nil {
					v := *r.Link
					r.Link = &v
				}
				if r.ScopeRef != nil {
					v := *r.ScopeRef
					r.ScopeRef = &v
				}
				if r.ProjectName != nil {
					v := *r.ProjectName
					r.ProjectName = &v
				}
			}
			x.Facts = f
		}
		out.Index = &x
	}
	return out
}

func selectionOptionalColumn(c SelectionColumn) bool {
	switch c {
	case SelectionProject, SelectionParent, SelectionStatus, SelectionTitle, SelectionTemplate, SelectionPriority:
		return true
	}
	return false
}
