// Package migrationpre contains the connected read-only composition used by
// the ontology migration acceptance tests. It adapts the native plugin host's
// validating subscriber port to the migration planner without adding a write
// or protected-apply seam.
package migrationpre

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/ontologymigration"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// ReadStore is the storage surface needed by a validating-pre adapter. The
// adapter never receives a WriteTx or a lease and therefore cannot publish a
// migration as a side effect of planning.
type ReadStore interface {
	Meta(context.Context) (driver.Meta, error)
	Read(context.Context, driver.View) (driver.Reader, error)
}

// NativePre is the trusted host composition for one or more native validating
// subscriber bindings. Inventory captures one immutable storage view. Every
// callback is then run through pluginhost's real validator adapter against
// that same view, while the candidate before/after images come from the
// planner's detached rows.
type NativePre struct {
	store      ReadStore
	validators map[string]port.Validator

	mu        sync.RWMutex
	bindings  []ontologymigration.Binding
	reader    driver.Reader
	view      driver.View
	candidate ontologymigration.Reference
}

// NewNativePre binds the supplied ontology manifest entries to validators
// registered by the native host. Binding IDs must match actual host bindings;
// a missing validator is rejected instead of becoming a successful stub.
func NewNativePre(store ReadStore, host *pluginhost.Host, bindings []ontologymigration.Binding) (*NativePre, error) {
	if store == nil || host == nil {
		return nil, fmt.Errorf("native migration pre requires a read store and native host")
	}
	if bindings == nil {
		return nil, fmt.Errorf("native migration pre requires an explicit binding list")
	}
	validators := map[string]port.Validator{}
	for _, binding := range host.Validators() {
		if binding.Validator == nil {
			return nil, fmt.Errorf("native host binding %q has no validator", binding.ID)
		}
		validators[binding.ID] = binding.Validator
	}
	for _, binding := range bindings {
		if binding.ID == "" || binding.Type == "" || validators[binding.ID] == nil {
			return nil, fmt.Errorf("migration binding %q is not an actual native validator", binding.ID)
		}
	}
	return &NativePre{store: store, validators: validators, bindings: cloneBindings(bindings)}, nil
}

func cloneBindings(bindings []ontologymigration.Binding) []ontologymigration.Binding {
	return append([]ontologymigration.Binding(nil), bindings...)
}

// Inventory returns the complete trusted manifest and pins a reader at the
// current committed storage head. The planner separately enumerates all rows;
// this adapter only supplies the callback's pinned read boundary.
func (p *NativePre) Inventory(ctx context.Context, candidate ontologymigration.Reference) (ontologymigration.PreInventory, error) {
	if err := ctx.Err(); err != nil {
		return ontologymigration.PreInventory{}, err
	}
	meta, err := p.store.Meta(ctx)
	if err != nil {
		return ontologymigration.PreInventory{}, err
	}
	at := meta.Head
	reader, err := p.store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &at})
	if err != nil {
		return ontologymigration.PreInventory{}, err
	}
	p.mu.Lock()
	old := p.reader
	p.reader = reader
	p.view = reader.Snapshot()
	p.candidate = candidate
	bindings := cloneBindings(p.bindings)
	p.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	digest, err := ontologymigration.BindingsDigest(candidate, bindings)
	if err != nil {
		return ontologymigration.PreInventory{}, err
	}
	return ontologymigration.PreInventory{Candidate: candidate, Bindings: bindings, Digest: digest}, nil
}

// Validate calls the actual pluginhost validator. The host sees the detached
// proposed row images and a reader whose snapshot is the view captured by
// Inventory; it never receives the planner's mutable row or a write handle.
func (p *NativePre) Validate(ctx context.Context, binding ontologymigration.Binding, change ontologymigration.Change) ([]ontology.Violation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.RLock()
	validator := p.validators[binding.ID]
	reader := p.reader
	view := p.view
	p.mu.RUnlock()
	if validator == nil || reader == nil || view.At == nil {
		return nil, fmt.Errorf("native validating-pre binding %q is not pinned", binding.ID)
	}
	before, err := rowBody(change.Before)
	if err != nil {
		return nil, err
	}
	after, err := rowBody(change.After)
	if err != nil {
		return nil, err
	}
	ref := model.Ref{ID: change.After.State.ID}
	if err := ref.Validate(); err != nil {
		return nil, fmt.Errorf("native validating-pre row has invalid ref: %w", err)
	}
	op := model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Kind: model.OpPatch, Target: ref, Fields: map[string]any{}}}
	proposal := port.ProposedChange{
		RequestID:    "ontology.plan",
		Principal:    model.PrincipalRef{Kind: "system", ID: "ontology-planner"},
		BaseRevision: *view.At,
		DryRun:       true,
		Changes: []port.Change{{
			Resource: model.ResourceRef{Kind: "bead", Ref: ref},
			Op:       op,
			Before:   before,
			After:    after,
		}},
	}
	verdict, err := validator.Validate(ctx, proposal, &pinnedReader{reader: reader})
	if err != nil {
		return nil, err
	}
	violations := make([]ontology.Violation, 0, len(verdict.Violations))
	for _, violation := range verdict.Violations {
		code := model.ErrorCode(violation.Guard)
		if code.Validate() != nil {
			code = "ontology_guard_failed"
		}
		field := ""
		if value, ok := violation.Details["field"].(string); ok {
			field = value
		}
		violations = append(violations, ontology.Violation{Code: code, Field: field, Message: violation.Message})
	}
	if !verdict.Allow && len(violations) == 0 {
		violations = append(violations, ontology.Violation{Code: "ontology_guard_failed", Message: "native validating-pre rejected the candidate"})
	}
	return violations, nil
}

// SetBindingDigest simulates a trusted host manifest change for the guard
// invalidation acceptance. It does not change the registered callback.
func (p *NativePre) SetBindingDigest(id, digest string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.bindings {
		if p.bindings[i].ID == id {
			p.bindings[i].Digest = digest
			return nil
		}
	}
	return fmt.Errorf("unknown migration binding %q", id)
}

// Close releases the pinned callback reader. It is safe to call repeatedly.
func (p *NativePre) Close() error {
	p.mu.Lock()
	reader := p.reader
	p.reader = nil
	p.mu.Unlock()
	if reader == nil {
		return nil
	}
	return reader.Close()
}

func rowBody(row ontologymigration.Row) (json.RawMessage, error) {
	fields, err := cloneMap(row.State.Fields)
	if err != nil {
		return nil, err
	}
	if fields == nil {
		fields = map[string]any{}
	}
	fields["status"] = row.State.Status
	return json.Marshal(pipeline.Document{ID: row.State.ID, Project: row.State.Project, Type: row.State.Type, Fields: fields})
}

func cloneMap(value map[string]any) (map[string]any, error) {
	if value == nil {
		return nil, nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var copy map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&copy); err != nil {
		return nil, err
	}
	return copy, nil
}

// pinnedReader converts a real driver snapshot to the core callback port.
type pinnedReader struct{ reader driver.Reader }

func (r *pinnedReader) Snapshot() driver.View { return r.reader.Snapshot() }
func (r *pinnedReader) Get(ctx context.Context, refs []model.Ref) ([]driver.Record, error) {
	return r.reader.Get(ctx, refs)
}
func (r *pinnedReader) Query(ctx context.Context, query driver.Query) (driver.Page[driver.Record], error) {
	return r.reader.Query(ctx, query)
}
func (r *pinnedReader) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	return r.reader.Observe(ctx, keys)
}

var _ ontologymigration.ValidatingPre = (*NativePre)(nil)
var _ port.SnapshotReader = (*pinnedReader)(nil)
