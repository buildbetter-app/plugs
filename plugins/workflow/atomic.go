package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
)

// PreparedState carries an unpublished provider image. Only a trusted host
// namespace adapter can turn it into an ordinary writer transaction participant.
// No driver or transaction handle is passed to plugin code.
type PreparedState struct {
	state  pluginapi.DurableState
	key    string
	value  json.RawMessage
	expect pluginapi.Revision
}

func (s PreparedState) Checkpoint(ctx context.Context) (driver.CheckpointMutation, error) {
	adapter, ok := s.state.(interface {
		PrepareCheckpoint(context.Context, string, any, pluginapi.Revision) (driver.CheckpointMutation, error)
	})
	if !ok {
		return driver.CheckpointMutation{}, driver.ErrUnsupported
	}
	return adapter.PrepareCheckpoint(ctx, s.key, s.value, s.expect)
}

type stagedState struct {
	base             pluginapi.DurableState
	key              string
	value            json.RawMessage
	expect, revision pluginapi.Revision
	written          bool
}

func (s *stagedState) Persistent() bool {
	marker, ok := s.base.(interface{ Persistent() bool })
	return ok && marker.Persistent()
}
func (s *stagedState) Read(ctx context.Context, key string, dst any) (bool, pluginapi.Revision, error) {
	if !s.written {
		return s.base.Read(ctx, key, dst)
	}
	if key != s.key {
		return s.base.Read(ctx, key, dst)
	}
	if len(s.value) == 0 {
		return false, s.revision, nil
	}
	err := json.Unmarshal(s.value, dst)
	return err == nil, s.revision, err
}
func (s *stagedState) Get(ctx context.Context, key string, dst any) (bool, error) {
	found, _, err := s.Read(ctx, key, dst)
	return found, err
}
func (s *stagedState) Revision(ctx context.Context, key string) (pluginapi.Revision, error) {
	if s.written && s.key == key {
		return s.revision, nil
	}
	return s.base.Revision(ctx, key)
}
func (s *stagedState) Put(ctx context.Context, key string, value any, expect pluginapi.Revision) (pluginapi.Revision, error) {
	current, err := s.Revision(ctx, key)
	if err != nil {
		return "", err
	}
	if current != expect {
		return "", driver.ErrCheckpointConflict
	}
	if s.written && s.key != key {
		return "", fmt.Errorf("workflow move spans more than one provider key")
	}
	number, err := expect.Uint64()
	if err != nil || number == ^uint64(0) {
		return "", fmt.Errorf("workflow generation exhausted")
	}
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	if !s.written {
		s.expect = expect
	}
	s.key, s.value, s.revision, s.written = key, data, pluginapi.Natural64(number+1), true
	return s.revision, nil
}
func (s *stagedState) Delete(context.Context, string, pluginapi.Revision) error {
	return driver.ErrUnsupported
}
func (s *stagedState) List(context.Context, string, int) ([]pluginapi.DurableEntry, error) {
	return nil, driver.ErrUnsupported
}
func (p *Provider) staged() (*Provider, *stagedState) {
	state := &stagedState{base: p.state}
	return &Provider{state: state, now: p.now, persistent: p.persistent, plugin: p.plugin, instance: p.instance, incarnation: p.incarnation, userContentGuard: p.userContentGuard}, state
}
func (s *stagedState) prepared() PreparedState {
	return PreparedState{state: s.base, key: s.key, value: append(json.RawMessage(nil), s.value...), expect: s.expect}
}

// PromoteAtomic holds the provider's mutation fence until the trusted writer
// has atomically published the ordinary target and this prepared checkpoint.
// On any refusal, the entire prepared image is discarded.
func (p *Provider) PromoteAtomic(ctx context.Context, by model.PrincipalRef, req PromoteRequest, meta model.WriteMeta, publish func(ItemView, PromoteResult, PreparedState) error) (PromoteResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	staged, state := p.staged()
	value, _, err := staged.load(ctx)
	if err != nil {
		return PromoteResult{}, err
	}
	var replay PromoteResult
	if ok, err := staged.checkReplay(value, by, meta, req, &replay); err != nil {
		return PromoteResult{}, err
	} else if ok {
		return replay, nil
	}
	item, ok := value.Items[req.Item.ID]
	if !ok {
		return PromoteResult{}, fail("not_found", "workflow item was not found")
	}
	if err := staged.authorized(item, by, meta); err != nil {
		return PromoteResult{}, err
	}
	result, err := staged.Promote(ctx, by, req, meta)
	if err != nil {
		return PromoteResult{}, err
	}
	if publish == nil {
		return PromoteResult{}, driver.ErrUnsupported
	}
	// Migration carries the stored explicit flag. Ready/Get views additionally
	// derive blockers from relationships, which must never become sticky data.
	if err := publish(itemViewAt(item, p.now()), result, state.prepared()); err != nil {
		return PromoteResult{}, err
	}
	return result, nil
}

// DemoteAtomic obtains the durable source through the trusted service, never
// from the caller projection. Replay identity remains the original request.
func (p *Provider) DemoteAtomic(ctx context.Context, by model.PrincipalRef, req DemoteRequest, meta model.WriteMeta, hydrate func() (DemoteRequest, error), publish func(DemoteResult, PreparedState) error) (DemoteResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	staged, state := p.staged()
	value, _, err := staged.load(ctx)
	if err != nil {
		return DemoteResult{}, err
	}
	var replay DemoteResult
	if ok, err := staged.checkReplay(value, by, meta, req, &replay); err != nil {
		return DemoteResult{}, err
	} else if ok {
		return replay, nil
	}
	if hydrate == nil || publish == nil {
		return DemoteResult{}, driver.ErrUnsupported
	}
	image, err := hydrate()
	if err != nil {
		return DemoteResult{}, err
	}
	result, err := staged.Demote(ctx, by, image, meta)
	if err != nil {
		return DemoteResult{}, err
	}
	// Replace the internal hydrated-image digest with the public request digest.
	value, revision, err := staged.load(ctx)
	if err != nil {
		return DemoteResult{}, err
	}
	if _, err := staged.storeReceipt(value, by, meta, req, result, result.Run.ID, result.Item.ID); err != nil {
		return DemoteResult{}, err
	}
	if err := staged.save(ctx, value, revision); err != nil {
		return DemoteResult{}, err
	}
	if image.Status == "closed" {
		// A one-item completed source is already a completed checklist. Erase
		// its operational body before either plane is published, retaining only
		// terminal replay identity under the original public request digest.
		closeMeta := meta
		closeMeta.Method = "bead.close"
		if _, err := staged.Close(ctx, by, model.RefByID(result.Item.ID, model.Natural64(0)), "", closeMeta); err != nil {
			return DemoteResult{}, err
		}
		value, _, err = staged.load(ctx)
		if err != nil {
			return DemoteResult{}, err
		}
		// Terminal receipts intentionally omit payload fields. Decode into a
		// fresh value so JSON omission cannot retain the hydrated live image.
		var terminal DemoteResult
		if ok, err := staged.checkReplay(value, by, meta, req, &terminal); err != nil {
			return DemoteResult{}, err
		} else if !ok {
			return DemoteResult{}, fmt.Errorf("completed demotion lost its terminal receipt")
		}
		result = terminal
	}
	if err := publish(result, state.prepared()); err != nil {
		return DemoteResult{}, err
	}
	return result, nil
}
