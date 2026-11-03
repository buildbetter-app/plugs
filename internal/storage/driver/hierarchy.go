package driver

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// HierarchyMaxParents bounds the request-local parent vector. The bound is
// part of the storage contract so every backend can do its limit arithmetic
// before constructing a query or a heap.
const HierarchyMaxParents = 1024

// HierarchyMaxLimit is the largest page accepted by HierarchyChildren.
const HierarchyMaxLimit = 10000

// HierarchyMaxExcluded bounds caller-supplied source exclusions per lookup.
const HierarchyMaxExcluded = 1000

// HierarchySourceWriter is an optional WriteTx capability for trusted core.
// PutHierarchyRecord stages a bead and its supplied direct-parent metadata in
// one operation. Nil is an explicit root; an ordinary Put supplies no metadata.
// Core derives parent from the same decoded document it serializes as Body and
// validates live parent existence, cycles and agreement before publication.
// Storage validates only the canonical Ref envelope and retains exact Body
// bytes; it never decodes Parent from JSON, infers it from an ID or consults
// derived closure rows to establish membership.
//
// The transaction must hold this record's update/skipLocked lock and the
// graph/blocking update/skipLocked lock, and atomically bump graph/blocking.
// Existing exact bead sentinel, Head, header-lock and lease rules still apply.
// Success owns copies of record.Body and parent. Repeated typed writes replace
// both staged components; Delete removes both. An untyped Put must never reuse
// an earlier parent declaration. Errors leave the staged image unchanged.
//
// Coverage and source images publish atomically with records, versions, log,
// head and receipts. Certification starts only from a proven empty durable
// target under Head with complete typed bead metadata. Legacy unbuilt stores
// may continue untyped writes without certification. A certified store refuses
// every untyped bead Put before staging and requires graph/blocking for bead
// deletion. It never bridges a stale coverage gap or certifies imported history
// from the final source images alone.
type HierarchySourceWriter interface {
	PutHierarchyRecord(context.Context, Record, *model.Ref) error
}

// HierarchyChildrenReader is an optional Reader/WriteTx capability. Its bound
// branch, incarnation and revision select live direct children of canonical
// resolved parents. Duplicate parent inputs select each child once; empty
// input selects no rows. Output sorts bytewise by the encoded record LockKey,
// uses exact source bytes and revisions, and owns every returned Body.
//
// One global storage-side limit+1 selection determines More; at most limit
// records are returned. Zero is an existence probe: return no records and use
// one lookahead image to determine More, after the same coverage validation.
// Indexed parent/revision predicates run before fetching images. No mode may
// discover membership through generic Query, closure rows,
// a scan of records/versions or unbounded Go sorting. A mode that cannot bound
// indexed work returns ErrUnsupported. Coverage absent, unbuilt, incompatible,
// stale or outside the certified range also returns ErrUnsupported, including
// for empty requests. Existing incarnation/history-floor errors still apply.
// Cancellation, closure, malformed images and all other errors return no
// records and More=false, never a partial page.
//
// WriteTx readers exclude every staged child key from committed selection
// before its limit, then merge typed creates, replacements, moves and deletes
// under the same global limit. Staged images retain committed revision tokens
// and do not change Snapshot. A move between requested parents appears once.
// This overlay is driver-staged state; prepared core overlays have no exclusion
// parameter or other representation in this interface.
// Core's observing wrapper records graph/blocking before every lookup, even an
// empty or More page, and retains individual returned source observations.
type HierarchyChildrenReader interface {
	HierarchyChildren(context.Context, []model.Ref, int) ([]Record, bool, error)
}

// HierarchyChildrenExcludingReader is an optional extension for a core reader
// with prepared source changes that are not yet staged in WriteTx. It preserves
// the HierarchyChildren contract, including coverage checks and zero-row probes.
// An empty excluded vector has exactly the same meaning as HierarchyChildren.
//
// Each excluded Ref suppresses its canonical child key from both committed and
// driver-staged images before selection, image fetching and the ONE global
// limit+1. Caller exclusions take precedence over a staged replacement or move;
// excluded children never consume a result or lookahead slot or set More.
// Input order and duplicate exclusions do not change membership. All inputs
// are validated, including for empty parents or a zero-row probe. Oversized
// exclusion vectors return ErrUnsupported before selecting any source images.
//
// Core merges its own prepared images and records the same graph/blocking and
// source observations. This interface does not publish those images. A reader
// without this capability cannot emulate it with repeated larger prefixes:
// that can exceed the caller's physical source-image budget.
type HierarchyChildrenExcludingReader interface {
	HierarchyChildrenReader
	HierarchyChildrenExcluding(ctx context.Context, parents, excluded []model.Ref, limit int) ([]Record, bool, error)
}

// NormalizeHierarchyRequest validates the bounded request arithmetic shared by
// all drivers. A zero limit requests an existence probe; negative and oversized
// values are rejected.
func NormalizeHierarchyRequest(parents []model.Ref, limit int) (int, error) {
	if len(parents) > HierarchyMaxParents {
		return 0, fmt.Errorf("%w: hierarchy parent vector exceeds %d", ErrUnsupported, HierarchyMaxParents)
	}
	if limit < 0 || limit > HierarchyMaxLimit {
		return 0, fmt.Errorf("hierarchy limit must be between 0 and %d", HierarchyMaxLimit)
	}
	for i, parent := range parents {
		if err := ValidateHierarchyRef(parent); err != nil {
			return 0, fmt.Errorf("hierarchy parent %d: %w", i, err)
		}
	}
	return limit, nil
}

// NormalizeHierarchyExcludingRequest validates request-local bounds and every
// excluded identity without sorting, deduplicating or changing caller input.
func NormalizeHierarchyExcludingRequest(parents, excluded []model.Ref, limit int) (int, error) {
	if len(excluded) > HierarchyMaxExcluded {
		return 0, fmt.Errorf("%w: hierarchy exclusion vector exceeds %d", ErrUnsupported, HierarchyMaxExcluded)
	}
	limit, err := NormalizeHierarchyRequest(parents, limit)
	if err != nil {
		return 0, err
	}
	for i, ref := range excluded {
		if err := ValidateHierarchyRef(ref); err != nil {
			return 0, fmt.Errorf("hierarchy exclusion %d: %w", i, err)
		}
	}
	return limit, nil
}

// ValidateHierarchyRef accepts only a resolved branch-zero ID in the existing
// bead/closure sentinel domain. It preserves the ID verbatim, including opaque
// delimiter-bearing spellings, and never infers parentage from its spelling.
func ValidateHierarchyRef(ref model.Ref) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	if !ref.IsID() || !ref.Branch.IsZero() {
		return fmt.Errorf("%w: hierarchy ref must be a resolved branch-zero id", ErrUnsupported)
	}
	return validateSentinelPart("hierarchy id", ref.ID)
}

// ValidateHierarchyRecord validates only storage-level identity and byte
// requirements. It intentionally does not inspect JSON semantics: pipeline is
// authoritative for Parent decoding and supplies the resolved parent here.
func ValidateHierarchyRecord(record Record, parent *model.Ref) error {
	if err := ValidateHierarchyRef(record.Ref); err != nil {
		return fmt.Errorf("hierarchy child ref: %w", err)
	}
	if record.Kind != "bead" {
		return fmt.Errorf("%w: typed hierarchy publication requires a bead", ErrUnsupported)
	}
	if !json.Valid(record.Body) {
		return fmt.Errorf("hierarchy child body is not valid JSON")
	}
	if parent == nil {
		return nil
	}
	if err := ValidateHierarchyRef(*parent); err != nil {
		return fmt.Errorf("hierarchy parent ref: %w", err)
	}
	if parent.ID == record.Ref.ID {
		return fmt.Errorf("hierarchy child cannot parent itself")
	}
	return nil
}
