package driver

import (
	"context"
	"fmt"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

const (
	SourceLinkMaxKindBytes = 32
	SourceLinkMaxTargets   = HierarchyMaxParents
	SourceLinkMaxKinds     = 4
	SourceLinkMaxExcluded  = HierarchyMaxExcluded
	SourceLinkMaxLimit     = HierarchyMaxLimit
)

// SourceLink is a canonical outgoing membership fact supplied by trusted core
// from the exact record Body. Kind preserves all valid dependency kinds,
// including opaque custom/association spellings and embedded NUL bytes. Link
// metadata and its interpretation remain in Body; storage never decodes it.
type SourceLink struct {
	Kind string
	To   model.Ref
}

// IncomingLinkKind is the closed domain required by hierarchy guards. It does
// not classify forward scheduling edges or admit arbitrary custom-kind reads.
type IncomingLinkKind string

const (
	IncomingLinkParentChild       IncomingLinkKind = "parent-child"
	IncomingLinkBlocks            IncomingLinkKind = "blocks"
	IncomingLinkConditionalBlocks IncomingLinkKind = "conditional-blocks"
	IncomingLinkWaitsFor          IncomingLinkKind = "waits-for"
)

func (kind IncomingLinkKind) Validate() error {
	switch kind {
	case IncomingLinkParentChild, IncomingLinkBlocks, IncomingLinkConditionalBlocks, IncomingLinkWaitsFor:
		return nil
	default:
		return fmt.Errorf("%w: inverse source-link kind %q is outside the covered domain", ErrUnsupported, kind)
	}
}

// GraphSourceWriter is an optional trusted-core WriteTx capability. One call
// stages the exact bead image, its canonical Parent and the COMPLETE outgoing
// Links declaration together. Nil parent explicitly means root; nil/empty
// links explicitly declares no outgoing links. An ordinary or Parent-only Put
// supplies no link declaration. Core derives both metadata arguments from the
// same decoded document and validates agreement with Body before publication.
//
// Success owns copies of Body, parent and every link. Repeated graph writes
// replace all staged components atomically; errors leave them all unchanged.
// Duplicate facts may collapse for membership only; Body remains byte-exact.
// Delete removes the image and all source facts together. Record/graph update
// locks, exact bead/graph bumps, Head/header and final lease fences still apply.
//
// Source images, Parent index, outgoing/inverse facts, independent versioned
// link coverage, history, head/log/receipt publish in the existing transaction.
// Parent coverage never certifies link coverage. Activation requires a proven
// empty durable target under Head and complete typed images. Ordinary and
// Parent-only bead Put must refuse ready link coverage before staging and must
// never reuse an earlier staged link declaration. Legacy unbuilt stores remain
// unbuilt; gaps and imported history cannot be certified from final images.
type GraphSourceWriter interface {
	PutGraphRecord(ctx context.Context, record Record, parent *model.Ref, links []SourceLink) error
}

// InverseSourceLinksReader selects source Records with at least one outgoing
// fact in the union of the requested target/kind pairs. Canonical Parent is
// exclusively HierarchyChildren's relation. Empty targets OR kinds select no
// sources, after the same input, snapshot and coverage checks. Empty kinds
// never means all kinds. Complete outgoing publication does not authorize an
// all-kinds selector or establish bounded general-delete coverage.
//
// Exclude canonical source keys from committed AND driver-staged selection,
// then deduplicate keys across all target/kind pairs BEFORE materializing any
// Body or applying ONE global limit+1. Return at most limit exact source images,
// sorted by encoded record LockKey, with caller-owned Bodies. More describes
// unique eligible sources beyond that page. Zero returns no records and uses
// one unique lookahead source to determine More. Prepared core images are
// merged by core; caller exclusions take precedence over driver staging.
//
// Images/facts belong to the same bound incarnation, branch and revision,
// including historical intervals and staged committed revision tokens. Missing,
// incompatible, incomplete or stale link-domain coverage returns ErrUnsupported
// even for empty/probe requests. Incarnation/history-floor errors retain their
// existing meaning. Cancellation, closure, malformed images or any error returns
// no records and More=false. Unsupported indexed historical work refuses rather
// than scanning sources/versions or treating incomplete membership as absence.
//
// Core records graph/blocking before EVERY lookup, including empty/More/error,
// and exact source observations for returned Records. Storage never broadens
// Query, decodes JSON, uses closure as membership, or repeats larger prefixes
// to compensate for exclusions. All indexed candidate and image work is bounded.
type InverseSourceLinksReader interface {
	InverseSourceLinks(ctx context.Context, targets []model.Ref, kinds []IncomingLinkKind, excluded []model.Ref, limit int) ([]Record, bool, error)
}

// NormalizeInverseSourceLinksRequest validates every bounded input without
// interpreting empty vectors as an all-relations request or mutating input.
func NormalizeInverseSourceLinksRequest(targets []model.Ref, kinds []IncomingLinkKind, excluded []model.Ref, limit int) (int, error) {
	if len(kinds) > SourceLinkMaxKinds {
		return 0, fmt.Errorf("%w: inverse source-link kind vector exceeds %d", ErrUnsupported, SourceLinkMaxKinds)
	}
	limit, err := NormalizeHierarchyExcludingRequest(targets, excluded, limit)
	if err != nil {
		return 0, fmt.Errorf("inverse source links: %w", err)
	}
	for i, kind := range kinds {
		if err := kind.Validate(); err != nil {
			return 0, fmt.Errorf("inverse source-link kind %d: %w", i, err)
		}
	}
	return limit, nil
}

// ValidateGraphSourceRecord checks storage envelopes only. Core owns the
// complete outgoing-list and Parent agreement with the exact Body. All valid
// custom kinds survive verbatim; only the reader's requested domain is closed.
func ValidateGraphSourceRecord(record Record, parent *model.Ref, links []SourceLink) error {
	if err := ValidateHierarchyRecord(record, parent); err != nil {
		return err
	}
	for i, link := range links {
		if link.Kind == "" || len(link.Kind) > SourceLinkMaxKindBytes || !utf8.ValidString(link.Kind) {
			return fmt.Errorf("source link %d: kind must be nonempty UTF-8 of at most %d bytes", i, SourceLinkMaxKindBytes)
		}
		if err := ValidateHierarchyRef(link.To); err != nil {
			return fmt.Errorf("source link %d target: %w", i, err)
		}
	}
	return nil
}
