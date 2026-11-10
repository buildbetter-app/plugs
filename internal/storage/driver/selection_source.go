package driver

import (
	"bytes"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

const SelectionBackfillPageSize = 128

// DerivedRowRange is a core-declared opaque byte-prefix input. Storage must
// enforce each Max and the projection's total page budget before decoding.
type DerivedRowRange struct {
	Table  DerivedTable
	Prefix []byte
	Max    int
}

// SelectionProjectionFamily identifies one independently versioned projection.
// The empty Kind denotes the one counting bead row in DerivedSortKey.
// These are exactly the source-built core families, not every admitted fact
// table. The three fact ranges apply to record-sourced envelopes. Independently
// complete derived-source facts, CutClosure Ancestor and record-sourced
// Grant/Project/Team retain exact envelope admission; startup does not certify
// those domains. Bead coverage is required unconditionally at both planes.
type SelectionProjectionFamily struct {
	Table   DerivedTable
	Kind    SelectionFactKind
	Version uint8
}

func (f SelectionProjectionFamily) Validate() error {
	if f.Version != SelectionIndexVersion {
		return fmt.Errorf("%w: incompatible selection family layout", ErrSelectionUnavailable)
	}
	switch {
	case f.Table == DerivedSortKey && f.Kind == "":
	case f.Table == DerivedAttribute && f.Kind == SelectionLabelFact:
	case f.Table == DerivedReferenceIndex && f.Kind == SelectionLinkFact:
	case f.Table == DerivedAncestorClosure && f.Kind == SelectionAncestorFact:
	default:
		return fmt.Errorf("%w: unsupported selection projection family", ErrSelectionUnavailable)
	}
	return nil
}

func (f SelectionProjectionFamily) Matches(row DerivedSelectionRow) bool {
	index := row.Row.Index
	if index == nil || row.Table != f.Table || index.Version != f.Version {
		return false
	}
	if f.Kind == "" {
		return index.Bead != nil
	}
	return index.Facts != nil && index.Facts.Kind == f.Kind
}

// SelectionCoverage certifies a closed revision interval for exactly one
// incarnation, branch and family layout. No candidate census, graph marker,
// present final image or empty request substitutes for this temporal proof.
type SelectionCoverage struct {
	Family         SelectionProjectionFamily
	Incarnation    string
	Branch         model.Branch
	First, Through model.Revision
}

func (c SelectionCoverage) Require(family SelectionProjectionFamily, incarnation string, branch model.Branch, at model.Revision) error {
	first, e1 := c.First.Uint64()
	through, e2 := c.Through.Uint64()
	revision, e3 := at.Uint64()
	if family.Validate() != nil || c.Family != family || incarnation == "" || c.Incarnation != incarnation || !c.Branch.IsZero() || !branch.IsZero() || e1 != nil || e2 != nil || e3 != nil || first > through || revision < first || revision > through {
		return fmt.Errorf("%w: selection family revision range unavailable", ErrSelectionUnavailable)
	}
	return nil
}

// GraphSourceProjection supplies selection-only startup work alongside the
// source page. Decode receives ONLY that record's declared logical ranges from
// the same pinned snapshot. Neither callback may read storage. Families are
// complete only after every source and inter-page publication has been covered;
// pages never certify. Final activation starts at the actual final Head.
// An Ancestor family also requires final typed Parent/Ancestor chain agreement
// over the complete current projection. SQL engines prove that relation in SQL;
// the reference engine uses ValidateSelectionParentChains, without source JSON.
type GraphSourceProjection struct {
	Families    []SelectionProjectionFamily
	MaxPageRows int
	Inputs      func(Record) ([]DerivedRowRange, error)
	Decode      func(Record, []DerivedRow) ([]DerivedSelectionRow, error)
}

func (p GraphSourceProjection) Validate() error {
	if p.Inputs == nil || p.Decode == nil || p.MaxPageRows <= 0 || len(p.Families) == 0 {
		return fmt.Errorf("%w: incomplete selection backfill declaration", ErrSelectionUnavailable)
	}
	seen := map[SelectionProjectionFamily]bool{}
	for _, family := range p.Families {
		if err := family.Validate(); err != nil {
			return err
		}
		if seen[family] {
			return fmt.Errorf("%w: duplicate selection backfill family", ErrSelectionUnavailable)
		}
		seen[family] = true
	}
	return nil
}

// SelectionProjectionPage owns one bounded source page and its declared ranges.
// Engines read Ranges in one batch at the SAME snapshot as Records, then call
// Decode. Successful pages are staging data only; they never certify a range.
type SelectionProjectionPage struct {
	Records  []Record
	Ranges   []DerivedRowRange
	metadata []GraphSourceMetadata
	owners   []int
	project  GraphSourceProjection
}

func NewSelectionProjectionPage(records []Record, metadata []GraphSourceMetadata, project GraphSourceProjection) (*SelectionProjectionPage, error) {
	if err := project.Validate(); err != nil {
		return nil, err
	}
	if len(records) > SelectionBackfillPageSize || len(records) != len(metadata) {
		return nil, fmt.Errorf("%w: invalid selection source page", ErrSelectionUnavailable)
	}
	project.Families = append([]SelectionProjectionFamily(nil), project.Families...)
	p := &SelectionProjectionPage{project: project}
	for i, record := range records {
		record.Body = bytes.Clone(record.Body)
		p.Records = append(p.Records, record)
		owned := GraphSourceMetadata{}
		for _, row := range metadata[i].Selection {
			owned.Selection = append(owned.Selection, row.Clone())
		}
		p.metadata = append(p.metadata, owned)
		inputs, err := project.Inputs(record)
		if err != nil {
			return nil, err
		}
		for _, input := range inputs {
			if input.Max <= 0 || input.Max > project.MaxPageRows || len(p.Ranges) >= SelectionBackfillPageSize {
				return nil, fmt.Errorf("%w: invalid selection input bound", ErrSelectionUnavailable)
			}
			if err := (DerivedRowKey{Table: input.Table, Key: string(input.Prefix) + "!"}).Validate(); err != nil {
				return nil, err
			}
			input.Prefix = bytes.Clone(input.Prefix)
			p.Ranges = append(p.Ranges, input)
			p.owners = append(p.owners, i)
		}
	}
	return p, nil
}

// Decode enforces both bounds BEFORE invoking core, then verifies complete
// per-source family coverage and exact source bindings on core's owned output.
func (p *SelectionProjectionPage) Decode(inputs [][]DerivedRow, incarnation string) ([][]DerivedSelectionRow, error) {
	if len(inputs) != len(p.Ranges) {
		return nil, fmt.Errorf("%w: incomplete selection input batch", ErrSelectionUnavailable)
	}
	current := make([][]DerivedRow, len(p.Records))
	total := 0
	for i, rows := range inputs {
		input := p.Ranges[i]
		total += len(rows)
		if len(rows) > input.Max || total > p.project.MaxPageRows {
			return nil, fmt.Errorf("%w: selection input range/page overflow", ErrSelectionUnavailable)
		}
		seen := map[string]bool{}
		for _, row := range rows {
			if !bytes.HasPrefix([]byte(row.Key), input.Prefix) || seen[row.Key] {
				return nil, fmt.Errorf("%w: invalid selection input identity", ErrSelectionUnavailable)
			}
			seen[row.Key] = true
			current[p.owners[i]] = append(current[p.owners[i]], row.Clone())
		}
	}
	result := make([][]DerivedSelectionRow, len(p.Records))
	identities := map[DerivedRowKey]bool{}
	for i, record := range p.Records {
		rows, err := p.project.Decode(record, current[i])
		if err != nil {
			return nil, err
		}
		rows = append(p.metadata[i].Selection, rows...)
		covered := map[SelectionProjectionFamily]bool{}
		for _, row := range rows {
			if err := row.Validate(); err != nil || row.Row.Index == nil {
				return nil, fmt.Errorf("%w: selection backfill row unavailable", ErrSelectionUnavailable)
			}
			index := row.Row.Index
			source := index.Source
			if index.Incarnation != incarnation || !index.Branch.IsZero() || source.Key.Kind != LockRecord || source.Key.Ref == nil || !SelectionSameRef(*source.Key.Ref, record.Ref) || !source.Present || source.Revision.String() != record.Revision.String() || source.Digest != SelectionDigest(record.Body) {
				return nil, fmt.Errorf("%w: selection backfill source mismatch", ErrSelectionUnavailable)
			}
			matched := false
			for _, family := range p.project.Families {
				if family.Matches(row) {
					covered[family], matched = true, true
				}
			}
			identity := DerivedRowKey{Table: row.Table, Key: row.Row.Key}
			if !matched || identities[identity] {
				return nil, fmt.Errorf("%w: duplicate or undeclared selection output", ErrSelectionUnavailable)
			}
			identities[identity] = true
			result[i] = append(result[i], row.Clone())
		}
		if len(covered) != len(p.project.Families) {
			return nil, fmt.Errorf("%w: incomplete selection source families", ErrSelectionUnavailable)
		}
	}
	return result, nil
}

// ValidateSelectionParentChains is the closed reference proof over typed core
// metadata. Self/contiguous depths alone cannot detect a stale closure after an
// ancestor's Parent changes without touching a descendant. Every depth target
// must be the prior target's current Parent; the last target must be a live root.
// Engines may prove the identical relation with scalar joins instead of loading
// this reference representation. Neither path decodes bodies or opaque keys.
func ValidateSelectionParentChains(rows []DerivedSelectionRow) error {
	beads := map[string]SelectionBead{}
	ancestors := map[string]map[uint64]model.Ref{}
	refID := func(ref model.Ref) string {
		id, _ := EncodeLockKey(LockKey{Kind: LockRecord, Ref: &ref})
		return id
	}
	for _, row := range rows {
		if row.Validate() != nil || row.Row.Index == nil {
			return fmt.Errorf("%w: chain projection missing", ErrSelectionUnavailable)
		}
		index := row.Row.Index
		if index.Bead != nil {
			id := refID(index.Bead.Ref)
			if _, exists := beads[id]; exists {
				return fmt.Errorf("%w: duplicate chain source", ErrSelectionUnavailable)
			}
			beads[id] = *index.Bead
		}
		if row.Table == DerivedAncestorClosure && index.Facts != nil && index.Facts.Kind == SelectionAncestorFact {
			for _, fact := range index.Facts.Rows {
				if fact.Ref == nil || fact.Target == nil {
					return fmt.Errorf("%w: missing typed chain identity", ErrSelectionUnavailable)
				}
				id := refID(*fact.Ref)
				if ancestors[id] == nil {
					ancestors[id] = map[uint64]model.Ref{}
				}
				if _, exists := ancestors[id][fact.Depth]; exists {
					return fmt.Errorf("%w: duplicate typed chain depth", ErrSelectionUnavailable)
				}
				ancestors[id][fact.Depth] = *fact.Target
			}
		}
	}
	for id, bead := range beads {
		chain := ancestors[id]
		if len(chain) == 0 {
			return fmt.Errorf("%w: missing typed self chain", ErrSelectionUnavailable)
		}
		expected := bead.Ref
		for depth := uint64(0); depth < uint64(len(chain)); depth++ {
			actual, found := chain[depth]
			if !found || !SelectionSameRef(actual, expected) {
				return fmt.Errorf("%w: closure disagrees with current Parent chain", ErrSelectionUnavailable)
			}
			target, live := beads[refID(actual)]
			if !live || (target.Parent == nil) != (depth+1 == uint64(len(chain))) {
				return fmt.Errorf("%w: incomplete current Parent chain", ErrSelectionUnavailable)
			}
			if target.Parent != nil {
				expected = *target.Parent
			}
		}
	}
	for id := range ancestors {
		if _, live := beads[id]; !live {
			return fmt.Errorf("%w: orphan typed closure", ErrSelectionUnavailable)
		}
	}
	return nil
}
