package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/readyindex"
)

// ReadyProjectionEvaluator is an optional trusted ontology adapter. Local
// certifies that this source cannot make another source's eligibility depend
// on time, heartbeat metadata, pending work or a nonlocal readiness relation.
// Canonical parents are safe only while the complete source set is Local:
// every ancestor then has a known non-backlog status and no live blockers.
type ReadyProjectionState struct {
	Eligible, Local bool
	Binding         string
}
type ReadyProjectionEvaluator interface {
	ReadyProjectionState(Document) (ReadyProjectionState, error)
}

type ReadyProjectionPlan struct{ delta *readyindex.Delta }

// PrepareReadyProjection is also the typed fixture publication seam. Sources
// are FINAL validated graph images; each deletion carries its actual old kind.
// Call before Lock/Head on the SAME pinned reader, include every returned lock,
// recheck all observations, and Publish alongside source H+1. Skipped raw writes
// leave coverage unavailable; this function never certifies a populated store
// from its final images or changes existing write/closure admission limits.
func PrepareReadyProjection(ctx context.Context, r interface {
	Snapshot() driver.View
	Observe(context.Context, []driver.LockKey) ([]driver.LockState, error)
}, sources []driver.Record, deleted []driver.Record, evaluator ReadyProjectionEvaluator) (*ReadyProjectionPlan, error) {
	if evaluator == nil && (len(sources) != 0 || len(deleted) != 0) {
		return &ReadyProjectionPlan{}, nil
	}
	mutations := make([]readyindex.Mutation, 0, len(sources)+len(deleted))
	for _, record := range sources {
		if record.Kind != "bead" {
			continue
		}
		doc, err := DecodeDocument(record.Body)
		if err != nil || doc.ID != record.Ref.ID {
			return nil, fmt.Errorf("invalid Ready projection source")
		}
		st, err := evaluator.ReadyProjectionState(doc)
		if err != nil {
			return nil, err
		}
		status, _ := doc.Fields["status"].(string)
		priority, violations := ontology.Priority(ontology.BeadState{Fields: doc.Fields})
		if len(violations) != 0 {
			return nil, fmt.Errorf("invalid Ready priority")
		}
		item := driver.ReadyMembershipItem{ID: doc.ID, Type: doc.Type, Project: doc.Project, Status: status, Revision: model.Natural64(0), Fields: doc.Fields}
		createdAt, _ := doc.CreationTime()
		mutations = append(mutations, readyindex.Mutation{Ref: record.Ref, Item: &item, Priority: int64(priority), Project: doc.Project, Type: doc.Type, Binding: st.Binding, Eligible: st.Eligible, Local: st.Local, CreatedAt: createdAt})
	}
	for _, record := range deleted {
		if record.Kind == "bead" {
			mutations = append(mutations, readyindex.Mutation{Ref: record.Ref})
		}
	}
	delta, err := readyindex.Prepare(ctx, r, mutations)
	if err != nil {
		if errors.Is(err, driver.ErrUnsupported) {
			return &ReadyProjectionPlan{}, nil
		}
		return nil, err
	}
	return &ReadyProjectionPlan{delta: delta}, nil
}

func (p *ReadyProjectionPlan) Available() bool {
	return p != nil && p.delta != nil && p.delta.Available()
}
func (p *ReadyProjectionPlan) LockKeys() []driver.LockKey {
	if p == nil || p.delta == nil {
		return nil
	}
	return p.delta.Locks()
}

// Skip drops optional maintenance after the lock census while completing its
// declared predicate updates. It writes no nodes and does not advance coverage.
func (p *ReadyProjectionPlan) Skip(ctx context.Context, tx driver.WriteTx) error {
	if p == nil || p.delta == nil {
		return nil
	}
	return p.delta.BumpPredicates(ctx, tx)
}
func (p *ReadyProjectionPlan) Publish(ctx context.Context, tx driver.WriteTx, revision model.Revision) (driver.ReadyOrderWork, error) {
	if p == nil || p.delta == nil {
		return driver.ReadyOrderWork{}, nil
	}
	// An intervening unmaintained publication may advance Head without
	// changing optional metadata. Refuse that coverage before staging; the
	// ordinary source transaction still commits and the old marker stays stale.
	view := tx.Snapshot()
	if view.At == nil || p.delta.BaseRevision().String() != view.At.String() {
		return driver.ReadyOrderWork{}, p.Skip(ctx, tx)
	}
	return p.delta.Publish(ctx, tx, revision)
}
func (p *prepared) prepareReadyProjection(ctx context.Context, o any) (*ReadyProjectionPlan, error) {
	evaluator, ok := o.(ReadyProjectionEvaluator)
	if !ok {
		return &ReadyProjectionPlan{}, nil
	}
	ids := make([]string, 0, len(p.changed))
	for id, changed := range p.changed {
		if changed {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	sources, deleted := []driver.Record{}, []driver.Record{}
	for _, id := range ids {
		pending := p.writes[id]
		record := driver.Record{Ref: pending.ref, Kind: pending.kind}
		if pending.deleted {
			deleted = append(deleted, record)
			continue
		}
		body, err := pending.body()
		if err != nil {
			return nil, err
		}
		record.Body = body
		sources = append(sources, record)
	}
	// Optional metadata has its own optimistic read set. Keep actual work in
	// the existing budget, but merge observations only when maintaining a
	// complete projection. Corrupt/unavailable optional state cannot add stale
	// read conflicts or become a new ordinary-write refusal.
	isolated := &snapshotReader{reader: p.snapshot.reader, reads: &readSet{states: map[string]driver.LockState{}}}
	reader := readyProjectionReader{parent: p.snapshot, isolated: isolated}
	plan, err := PrepareReadyProjection(ctx, reader, sources, deleted, evaluator)
	if err != nil || !plan.Available() {
		return plan, err
	}
	p.snapshot.reads.mu.Lock()
	for key, state := range isolated.reads.states {
		p.snapshot.reads.states[key] = state
	}
	p.snapshot.reads.mu.Unlock()
	return plan, nil
}

type readyProjectionReader struct{ parent, isolated *snapshotReader }

func (r readyProjectionReader) Snapshot() driver.View { return r.parent.Snapshot() }
func (r readyProjectionReader) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	if err := r.parent.budget(len(keys), 0); err != nil {
		return nil, err
	}
	return r.isolated.observe(ctx, keys)
}
