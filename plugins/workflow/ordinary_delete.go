package workflow

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
)

// OrdinaryDeleteImage is an unpublished unlink on its workflow endpoint.
// The ordinary writer keeps the complete original request and sole receipt.
type OrdinaryDeleteImage struct {
	Ref           model.Ref
	Before, After json.RawMessage
	Generation    model.Revision
}

type OrdinaryDeletePreparation struct {
	Namespace driver.CheckpointExpectation
	State     *PreparedState
}

// PrepareOrdinaryDelete applies explicit mixed unlinks in request order, then
// refuses deletion while any workflow reference still names that ordinary ID.
// It only reads the namespace and prepares bytes; it never publishes or GC's.
func (p *Provider) PrepareOrdinaryDelete(ctx context.Context, by model.PrincipalRef, meta model.WriteMeta, deletes []model.Ref, apply func(func(model.Op) (*OrdinaryDeleteImage, error)) error) (OrdinaryDeletePreparation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.persistent {
		return OrdinaryDeletePreparation{}, fail("plugin_durable_state_unavailable", "ephemeral workflows require persistent checkpoint storage")
	}
	value, revision, err := p.readState(ctx)
	if err != nil {
		return OrdinaryDeletePreparation{}, err
	}
	host, ok := p.state.(interface {
		PrepareCheckpointExpectation(context.Context, string, pluginapi.Revision) (driver.CheckpointExpectation, error)
	})
	if !ok || apply == nil {
		return OrdinaryDeletePreparation{}, driver.ErrUnsupported
	}
	expectation, err := host.PrepareCheckpointExpectation(ctx, stateKey, revision)
	if err != nil {
		return OrdinaryDeletePreparation{}, err
	}
	targets := map[string]bool{}
	for _, ref := range deletes {
		if err := validateRef(ref); err != nil || workflowTarget(ref) {
			return OrdinaryDeletePreparation{}, fail("invalid_params", "delete requires an ordinary branch-zero ID")
		}
		targets[ref.ID] = true
	}
	changed := false
	err = apply(func(op model.Op) (*OrdinaryDeleteImage, error) {
		if op.Kind == model.OpDelete && op.Delete.Target.IsID() {
			id := op.Delete.Target.ID
			for _, item := range value.Items {
				if item.Parent != nil && item.Parent.ID == id {
					return nil, fail("ontology_guard_failed", "unlink workflow children before deletion")
				}
				for _, relations := range [][]relationState{item.Links, item.Inbound} {
					for _, relation := range relations {
						if relation.From == id || relation.To.ID == id {
							return nil, fail("ontology_guard_failed", "unlink workflow relations before deletion")
						}
					}
				}
			}
		}
		if op.Kind == model.OpClose && ordinaryBlocked(value, []string{op.Close.Target.ID}, meta)[op.Close.Target.ID] {
			return nil, fail("ontology_guard_failed", "an active workflow dependency is not satisfied")
		}
		if op.Kind != model.OpUnlink || op.Link.TargetKind == model.LinkTargetExternal {
			return nil, nil
		}
		link := op.Link
		fromWorkflow, toWorkflow := workflowTarget(link.From), workflowTarget(link.To)
		if !fromWorkflow && !toWorkflow {
			return nil, nil
		}
		if fromWorkflow == toWorkflow || link.Metadata != nil {
			return nil, fail("unsupported", "delete batches support explicit incident mixed unlinks")
		}
		if err := validateRef(link.From); err != nil {
			return nil, err
		}
		if err := validateRef(link.To); err != nil {
			return nil, err
		}
		ordinary, active := link.From, link.To
		if fromWorkflow {
			ordinary, active = link.To, link.From
		}
		if !targets[ordinary.ID] {
			return nil, fail("unsupported", "mixed unlink must name an ordinary deletion in the same batch")
		}
		item, exists := value.Items[active.ID]
		if !exists {
			return nil, fail("not_found", "workflow relation endpoint was not found")
		}
		if err := p.authorized(item, by, meta); err != nil {
			return nil, err
		}
		before, err := json.Marshal(p.itemViewIn(value, item))
		if err != nil {
			return nil, err
		}
		relation := relationState{From: link.From.ID, To: link.To, Kind: strings.TrimSpace(link.LinkKind)}
		found := false
		if fromWorkflow {
			item.Links, found = removeDeleteRelations(item.Links, relation)
			if link.LinkKind == "parent-child" && item.Parent != nil && *item.Parent == ordinary {
				item.Parent, found = nil, true
			}
		} else {
			item.Inbound, found = removeDeleteRelations(item.Inbound, relation)
		}
		if !found {
			return nil, fail("not_found", "workflow relation was not found")
		}
		item.Generation++
		value.Items[item.ID] = item
		changed = true
		after, err := json.Marshal(p.itemViewIn(value, item))
		if err != nil {
			return nil, err
		}
		return &OrdinaryDeleteImage{Ref: active, Before: before, After: after, Generation: model.Natural64(item.Generation)}, nil
	})
	if err != nil {
		return OrdinaryDeletePreparation{}, err
	}
	result := OrdinaryDeletePreparation{Namespace: expectation}
	if changed {
		data, err := json.Marshal(value)
		if err != nil {
			return OrdinaryDeletePreparation{}, err
		}
		prepared := PreparedState{state: p.state, key: stateKey, value: data, expect: revision}
		result.State = &prepared
	}
	return result, nil
}

func removeDeleteRelations(values []relationState, edge relationState) ([]relationState, bool) {
	var retained []relationState
	found := false
	for _, value := range values {
		if value.From == edge.From && value.To == edge.To && value.Kind == edge.Kind {
			found = true
		} else {
			retained = append(retained, value)
		}
	}
	return retained, found
}

// AuthorizeOrdinaryDeleteReplay rechecks live workflow ownership after the
// canonical receipt lookup. Completed endpoints disclose no stored payload.
func (p *Provider) AuthorizeOrdinaryDeleteReplay(ctx context.Context, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	value, _, err := p.readState(ctx)
	if err != nil {
		return err
	}
	for _, op := range cs.Ops {
		if op.Kind != model.OpUnlink || op.Link.TargetKind == model.LinkTargetExternal {
			continue
		}
		for _, ref := range []model.Ref{op.Link.From, op.Link.To} {
			if item, exists := value.Items[ref.ID]; exists {
				if err := p.authorized(item, by, meta); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
