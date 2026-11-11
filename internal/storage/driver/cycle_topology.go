package driver

import (
	"context"
	"fmt"
	"github.com/buildbetter-app/plugs/internal/core/model"
)

// CycleTopologyEdge is a local directed membership fact, independent of bead
// hydration. External relations are excluded by trusted-core publication.
// Parallel facts may collapse; the cycle report also collapses endpoints.
type CycleTopologyEdge struct {
	From, To model.Ref
	Kind     string
}

// CycleTopologyReader enumerates COMPLETE outgoing local topology at the
// reader's pinned incarnation/revision. It never resurrects ordinary Delete:
// that operation removes source facts. Independent facts can outlive damaged
// hydration only in explicitly corrupted disposable diagnostic stores.
// Coverage is checked on every page, including empty pages. Missing, stale or
// incompatible coverage and staged write readers refuse with ErrUnsupported.
// Any error returns a zero page. Cursor order is deterministic and independent
// of the report limit. Callers must consume every page before counting cycles.
type CycleTopologyReader interface {
	CycleTopology(context.Context, *Cursor, int) (Page[CycleTopologyEdge], error)
}

// CycleTopologyOffset binds diagnostic pagination to one pinned snapshot.
func CycleTopologyOffset(cursor *Cursor, incarnation string, at model.Revision, limit int) (uint64, error) {
	if limit < 1 || limit > 1000 {
		return 0, fmt.Errorf("cycle topology page limit must be 1..1000")
	}
	if cursor == nil {
		return 0, nil
	}
	if cursor.Method != "cycle-topology" || cursor.Incarnation != incarnation || cursor.Revision.String() != at.String() || cursor.FilterDigest != "" || cursor.PrincipalDigest != "" || !cursor.OntologyRevision.IsZero() {
		return 0, fmt.Errorf("cycle topology cursor snapshot mismatch")
	}
	return cursor.Index.Uint64()
}
func CycleTopologyCursor(incarnation string, at model.Revision, offset uint64) *Cursor {
	return &Cursor{Method: "cycle-topology", Incarnation: incarnation, Revision: at, Index: model.Natural64(offset)}
}
