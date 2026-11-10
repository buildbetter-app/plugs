package workflow

import (
	"context"
	"encoding/json"
	"maps"
	"strconv"
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
	Results   map[int]model.OpResult
}

// PrepareOrdinaryDelete applies explicit mixed unlinks in request order, then
// refuses deletion while any workflow reference still names that ordinary ID.
// It only reads the namespace and prepares bytes; it never publishes or GC's.
func (p *Provider) PrepareOrdinaryDelete(ctx context.Context, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta, apply func(func(model.Op) (*OrdinaryDeleteImage, error)) error) (OrdinaryDeletePreparation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.persistent && p.source == nil {
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
	for _, op := range cs.Ops {
		if op.Kind != model.OpDelete {
			continue
		}
		ref := op.Delete.Target
		if err := validateRef(ref); err != nil || workflowTarget(ref) {
			return OrdinaryDeletePreparation{}, fail("invalid_params", "delete requires an ordinary branch-zero ID")
		}
		targets[ref.ID] = true
	}
	changed := false
	results := map[int]model.OpResult{}
	requestDigest, err := ordinaryDeleteDigest(cs, meta)
	if err != nil {
		return OrdinaryDeletePreparation{}, err
	}
	position := -1
	err = apply(func(op model.Op) (*OrdinaryDeleteImage, error) {
		position++
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
		results[position] = model.OpResult{Ref: active}
		return &OrdinaryDeleteImage{Ref: active, Before: before, After: after, Generation: model.Natural64(item.Generation)}, nil
	})
	if err != nil {
		return OrdinaryDeletePreparation{}, err
	}
	// Each ordinal returns the endpoint's final post-image, even if another
	// unlink later in the same request changes that endpoint again. Ordered
	// change images remain separate input to authorization and validation.
	for position := range cs.Ops {
		result, exists := results[position]
		if !exists {
			continue
		}
		item, exists := value.Items[result.Ref.ID]
		if !exists {
			return OrdinaryDeletePreparation{}, fail("internal_error", "prepared workflow endpoint was removed")
		}
		result.Revision = model.Natural64(item.Generation)
		result.SetBy = map[string]string{"links": "request"}
		if meta.Return != "minimal" {
			result.Record, err = json.Marshal(p.itemViewIn(value, item))
			if err != nil {
				return OrdinaryDeletePreparation{}, err
			}
		}
		results[position] = result
		if !meta.DryRun {
			if err := p.storeOrdinaryDeleteResult(&value, by, meta, requestDigest, position, item, result); err != nil {
				return OrdinaryDeletePreparation{}, err
			}
		}
	}
	result := OrdinaryDeletePreparation{Namespace: expectation, Results: results}
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

// The core receipt retains each original position's identity and revision.
// Active post-images stay in per-position plugin receipts, so promotion and
// completion erase them through the same lifecycle as other workflow results.
func ordinaryDeleteResultKey(scope string, position int) string {
	return scope + "\x00ordinary-delete/" + strconv.Itoa(position)
}

func ordinaryDeleteDigest(cs model.ChangeSet, meta model.WriteMeta) (string, error) {
	format := meta.Return
	if format == "" {
		format = "full"
	}
	return digest(struct {
		ChangeSet model.ChangeSet
		Return    string
	}{cs, format})
}

func (p *Provider) storeOrdinaryDeleteResult(value *persisted, by model.PrincipalRef, meta model.WriteMeta, requestDigest string, position int, item itemState, result model.OpResult) error {
	scope, principal, principalHash, agent, project, err := receiptKey(p, by, meta)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return err
	}
	receipt := receiptState{Principal: principal, PrincipalHash: principalHash, Agent: agent, Project: project, Plugin: p.plugin, Instance: p.instance, Incarnation: p.incarnation, Method: meta.Method, Key: meta.IdempotencyKey, Digest: requestDigest, RunID: item.RunID, ItemID: item.ID, ExpiresAt: p.now().Add(driver.IdempotencyWindow), Result: encoded}
	value.Receipts[ordinaryDeleteResultKey(scope, position)] = receipt
	p.appendAudit(value, "workflow.relation", receipt)
	return nil
}

// ProjectOrdinaryDeleteResults runs only after the canonical writer succeeds
// or replays its receipt. It rechecks current endpoint visibility and joins
// only matching plugin-owned post-images, without a write, GC or new receipt.
func (p *Provider) ProjectOrdinaryDeleteResults(ctx context.Context, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta, commit model.Commit) (model.Commit, error) {
	hasWorkflow := false
	for _, result := range commit.Results {
		hasWorkflow = hasWorkflow || workflowTarget(result.Ref)
	}
	if !hasWorkflow {
		return commit, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	value, _, err := p.readState(ctx)
	if err != nil {
		return model.Commit{}, err
	}
	principal, principalHash, agent, project, scope, err := p.receiptScope(by, meta)
	if err != nil {
		return model.Commit{}, err
	}
	d, err := ordinaryDeleteDigest(cs, meta)
	if err != nil {
		return model.Commit{}, err
	}
	commit.Results = append([]model.OpResult(nil), commit.Results...)
	for position, result := range commit.Results {
		if !workflowTarget(result.Ref) {
			continue
		}
		receipt, exists := value.Receipts[ordinaryDeleteResultKey(scope, position)]
		if !exists || receipt.Principal != principal || receipt.PrincipalHash != principalHash || receipt.Agent != agent || receipt.Project != project || receipt.Plugin != p.plugin || receipt.Instance != p.instance || receipt.Incarnation != p.incarnation || receipt.Method != meta.Method || receipt.Key != meta.IdempotencyKey || receipt.ItemID != result.Ref.ID || receipt.Digest != d {
			return model.Commit{}, fail("internal_error", "workflow transaction result does not match its receipt")
		}
		var stored model.OpResult
		if err := json.Unmarshal(receipt.Result, &stored); err != nil || stored.Ref != result.Ref || stored.Revision != result.Revision || !maps.Equal(stored.SetBy, result.SetBy) {
			return model.Commit{}, fail("internal_error", "workflow transaction result identity changed")
		}
		if item, live := value.Items[result.Ref.ID]; live {
			if item.RunID != receipt.RunID {
				return model.Commit{}, fail("internal_error", "workflow transaction endpoint identity changed")
			}
			if err := p.authorized(item, by, meta); err != nil {
				return model.Commit{}, err
			}
		} else if len(stored.Record) != 0 {
			return model.Commit{}, fail("internal_error", "removed workflow endpoint retained a result payload")
		}
		if meta.Return == "minimal" {
			stored.Record = nil
		}
		commit.Results[position] = stored
	}
	return commit, nil
}
