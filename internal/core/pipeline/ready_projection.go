package pipeline

import (
	"context"
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
	if evaluator == nil {
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
		mutations = append(mutations, readyindex.Mutation{Ref: record.Ref, Item: &item, Priority: int64(priority), Project: doc.Project, Type: doc.Type, Binding: st.Binding, Eligible: st.Eligible, Local: st.Local})
	}
	for _, record := range deleted {
		if record.Kind == "bead" {
			mutations = append(mutations, readyindex.Mutation{Ref: record.Ref})
		}
	}
	delta, err := readyindex.Prepare(ctx, r, mutations)
	if err != nil {
		return nil, err
	}
	return &ReadyProjectionPlan{delta: delta}, nil
}
func (p *ReadyProjectionPlan) LockKeys() []driver.LockKey {
	if p == nil || p.delta == nil {
		return nil
	}
	return p.delta.Locks()
}
func (p *ReadyProjectionPlan) Publish(ctx context.Context, tx driver.WriteTx, revision model.Revision) (driver.ReadyOrderWork, error) {
	if p == nil || p.delta == nil {
		return driver.ReadyOrderWork{}, nil
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
	return PrepareReadyProjection(ctx, p.snapshot, sources, deleted, evaluator)
}
