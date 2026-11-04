package pipeline

import (
	"context"
	"errors"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// GraphValidityReadFact is a semantic certificate, separate from typed source
// membership coverage. Trusted core bumps it ONLY after validating the complete
// resulting graph, or preserving an already certified graph with bounded checks.
// Its revision must equal graph/blocking in the same incarnation and snapshot.
// Any graph publication which does not maintain it makes it stale automatically.
// The version names the exact invariant set below; changing that set requires a
// new fact, not accepting an older certificate under stronger semantics.
const GraphValidityReadFact driver.ReadFact = "core/graph-validity/v1"

func GraphValidityLockKey() driver.LockKey {
	return driver.LockKey{Kind: driver.LockReadFact, Fact: GraphValidityReadFact}
}

func (p *prepared) graphIsCertified(ctx context.Context) (bool, error) {
	states, err := p.snapshot.observe(ctx, []driver.LockKey{GraphValidityLockKey(), driver.GraphBlockingSentinelLock})
	if err != nil {
		return false, err
	}
	return states[0].Present && states[1].Present && !states[1].Revision.IsZero() && states[0].Revision.String() == states[1].Revision.String(), nil
}

// A certificate also needs the active typed-write fence. Otherwise a legacy
// untyped writer could change a bead without graph/blocking, and later startup
// membership activation could make the old certificate appear current again.
func (p *prepared) graphCoverageActive(ctx context.Context) (bool, error) {
	if _, _, err := p.snapshot.HierarchyChildren(ctx, nil, 0); err != nil {
		if errors.Is(err, driver.ErrUnsupported) {
			return false, nil
		}
		return false, err
	}
	if _, _, err := p.snapshot.InverseSourceLinks(ctx, nil, nil, nil, 0); err != nil {
		if errors.Is(err, driver.ErrUnsupported) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ValidateGraphDocuments checks the complete relevant graph, not just the
// sources selected by a hierarchy edit. Native restore uses the same validator
// as ordinary writes before publishing its semantic certificate. Callers own
// decoding, exact record/body identity, and the pinned publication boundary.
func ValidateGraphDocuments(ctx context.Context, docs map[string]Document) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkParentGraph(parentGraph(docs)); err != nil {
		return err
	}
	for _, doc := range docs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if doc.Parent != nil && driver.ValidateHierarchyRef(*doc.Parent) != nil {
			return failure("ontology_guard_failed", "invalid hierarchy source parent")
		}
		for _, link := range doc.Links {
			if !validDependencyKind(link.Kind) || driver.ValidateHierarchyRef(link.To) != nil {
				return failure("ontology_guard_failed", "invalid hierarchy source link")
			}
			if _, ok := docs[link.To.ID]; !ok {
				return failure("ontology_guard_failed", "unlink all incoming and outgoing edges before deletion")
			}
		}
	}
	visiting, done := map[string]bool{}, map[string]bool{}
	var walk func(string) error
	walk = func(id string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if visiting[id] {
			return failure("ontology_guard_failed", "blocking link would form a cycle")
		}
		if done[id] {
			return nil
		}
		visiting[id] = true
		if parent := docs[id].Parent; parent != nil {
			if err := walk(parent.ID); err != nil {
				return err
			}
		}
		for _, link := range docs[id].Links {
			if dependencyIsScheduling(link.Kind) {
				if err := walk(link.To.ID); err != nil {
					return err
				}
			}
		}
		delete(visiting, id)
		done[id] = true
		return nil
	}
	for id := range docs {
		if err := walk(id); err != nil {
			return err
		}
	}
	return checkBlockingHierarchy(docs)
}
