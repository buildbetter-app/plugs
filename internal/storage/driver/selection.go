package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"reflect"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

const (
	SelectionOrdinaryCap = 20000
	SelectionSummaryCap  = 100000
	SelectionGroupCap    = 100
	SelectionMaxDepth    = 32
	SelectionMaxNodes    = 4096
)

// SelectionReader is optional and internal. A selector is pinned to current
// authority at reader creation; historical content has its own revision. Core
// must still fence fresh authority before publishing. This is not a terminal
// after-Head freshness primitive or a public query/SQL interface.
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
	Membership, Authorization          SelectionExpr
	Order                              []SelectionOrder
	Mode                               SelectionMode
	GroupBy                            SelectionGroup
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
)

type SelectionExpr struct {
	Op      SelectionOp
	Args    []SelectionExpr
	Compare *SelectionCompare
	Refs    []model.Ref
	Exists  *SelectionExists
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
	SelectionFactRef      SelectionFactColumn = "ref"
	SelectionFactTarget   SelectionFactColumn = "target"
	SelectionFactSubject  SelectionFactColumn = "subject"
	SelectionFactScope    SelectionFactColumn = "scope"
	SelectionFactRole     SelectionFactColumn = "role"
	SelectionFactName     SelectionFactColumn = "name"
	SelectionFactValue    SelectionFactColumn = "value"
	SelectionFactActive   SelectionFactColumn = "active"
	SelectionFactDepth    SelectionFactColumn = "depth"
	SelectionFactLinkKind SelectionFactColumn = "link_kind"
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
	SelectionProjectFact  SelectionFactKind = "project"
	SelectionTeamFact     SelectionFactKind = "team"
	SelectionOntologyFact SelectionFactKind = "ontology"
	SelectionReadyFact    SelectionFactKind = "ready"
	SelectionPluginFact   SelectionFactKind = "plugin"
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
}
type SelectionBead struct {
	Ref                           model.Ref
	Project                       *string
	Parent                        *model.Ref
	Type, Status, Title, Template string
	Priority                      int64
	MembershipLeaf                string
}
type SelectionSource struct {
	Key      LockKey
	Revision model.Revision
	Present  bool
	Digest   string
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
func selectionError(s string) error { return fmt.Errorf("%w: selection %s", ErrUnsupported, s) }

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
	case SelectionID, SelectionType, SelectionProject, SelectionStatus, SelectionTitle, SelectionRevision, SelectionTemplate:
		return "string", nil
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
	case SelectionGrantFact:
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
			if c.Value != nil || (c.Column != SelectionProject && c.Column != SelectionParent) {
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
	// There are no maps/interfaces in this closed tree. Gob retains opaque
	// string bytes that JSON would replace when they are not UTF-8.
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(p); err != nil {
		return "", err
	}
	return SelectionDigest(b.Bytes()), nil
}

func canonicalDigest(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 32 && hex.EncodeToString(b) == s
}
func (index *SelectionIndex) Validate(table DerivedTable, row DerivedRow) error {
	if index == nil {
		return nil
	}
	if index.Version != 1 || index.Row.Table != table || index.Row.Key != row.Key || index.Incarnation == "" || index.Branch.Validate() != nil || !index.Branch.IsZero() {
		return selectionError("index envelope mismatch")
	}
	if err := index.Row.Validate(); err != nil {
		return err
	}
	if index.ValueDigest != SelectionDigest(row.Value) {
		return selectionError("index logical value digest mismatch")
	}
	if err := index.Source.Key.Validate(); err != nil {
		return err
	}
	if index.Source.Key.Ref != nil && !index.Source.Key.Ref.Branch.IsZero() {
		return selectionError("source branch mismatch")
	}
	if _, err := index.Source.Revision.Uint64(); err != nil {
		return err
	}
	data := index.Source.Key.Kind == LockRecord || index.Source.Key.Kind == LockDerived
	if index.Source.Present && data {
		if !canonicalDigest(index.Source.Digest) {
			return selectionError("invalid source digest")
		}
	} else if index.Source.Digest != "" {
		return selectionError("digest without source bytes")
	}
	if (index.Bead == nil) == (index.Facts == nil) {
		return selectionError("index must have one layout")
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

// Clone owns all slices, pointers and logical bytes at the storage boundary.
func (row DerivedRow) Clone() DerivedRow {
	out := DerivedRow{Key: row.Key, Value: bytes.Clone(row.Value)}
	if row.Index != nil {
		x := *row.Index
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
			}
			x.Facts = f
		}
		out.Index = &x
	}
	return out
}
