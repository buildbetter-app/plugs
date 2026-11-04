// Package events adapts durable event delivery to storage and opaque cursors.
// Transports can use these adapters without owning the dispatcher or its lease.
package events

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	core "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// ReadStore exposes metadata and read snapshots. Checkpoint writes use the
// optional driver capability, never graph transactions or a delivery-owned map.
type ReadStore interface {
	Meta(context.Context) (driver.Meta, error)
	Read(context.Context, driver.View) (driver.Reader, error)
}

type Log struct{ Store ReadStore }

func (l Log) Head(ctx context.Context) (core.Head, error) {
	meta, err := l.Store.Meta(ctx)
	return core.Head{Incarnation: meta.Incarnation, Revision: meta.Head, Floor: meta.HistoryFloor}, err
}
func (l Log) Scan(ctx context.Context, after core.Position, limit int) ([]model.Changeset, error) {
	if err := after.Validate(); err != nil {
		return nil, err
	}
	reader, err := l.Store.Read(ctx, driver.View{Incarnation: after.Incarnation})
	if err != nil {
		return nil, boundaryError(ctx, l.Store, after.Incarnation, err)
	}
	defer reader.Close()
	var cursor *driver.Cursor
	if !after.Revision.IsZero() {
		// Engines explicitly check their supported revision domain. This
		// conversion refuses overflow rather than silently narrowing cursors.
		revision, err := after.Revision.Uint64()
		if err != nil {
			return nil, core.ErrCursorInvalid
		}
		previous := model.Natural64(revision - 1)
		meta, err := l.Store.Meta(ctx)
		if err != nil {
			return nil, err
		}
		if meta.Incarnation != after.Incarnation {
			return nil, &core.BoundaryError{Code: "incarnation_changed", From: after.Incarnation, Head: core.Head{Incarnation: meta.Incarnation, Revision: meta.Head, Floor: meta.HistoryFloor}, Reason: "failover"}
		}
		cmp, err := core.CompareNatural(previous, meta.HistoryFloor)
		if err != nil {
			return nil, err
		}
		if cmp >= 0 {
			cursor = &driver.Cursor{Incarnation: after.Incarnation, Revision: previous, Index: model.Natural64(0)}
		}
	}
	sets, err := reader.Changesets(ctx, cursor, limit)
	if err != nil {
		// A failing source must never expose an apparently usable prefix.
		return nil, boundaryError(ctx, l.Store, after.Incarnation, err)
	}
	// A driver's ordered scan alone does not prove continuity. Validate the
	// complete page before delivery can stop at its visible-event limit or
	// hide a missing revision behind an authorization/resource filter.
	snapshot := reader.Snapshot()
	if snapshot.At == nil || snapshot.Incarnation != after.Incarnation {
		return nil, fmt.Errorf("invalid committed changeset snapshot")
	}
	head, err := snapshot.At.Uint64()
	if err != nil {
		return nil, fmt.Errorf("invalid committed changeset snapshot: %w", err)
	}
	var previous uint64
	if cursor != nil {
		previous, err = cursor.Revision.Uint64()
		if err != nil {
			return nil, err
		}
	}
	for _, set := range sets {
		revision, err := set.Revision.Uint64()
		if err != nil || set.Incarnation != snapshot.Incarnation || revision == 0 || revision > head || revision-1 != previous {
			return nil, fmt.Errorf("invalid committed changeset continuity")
		}
		previous = revision
	}
	// A full limited page may end before the snapshot head. A short page
	// must reach that pinned head, even if a concurrent append moved Meta.
	if len(sets) < limit && previous < head {
		return nil, fmt.Errorf("incomplete committed changeset page")
	}
	return sets, nil
}

func boundaryError(ctx context.Context, store ReadStore, from string, err error) error {
	if err == nil {
		return nil
	}
	code, _, mapped := driver.ErrorMapping(err)
	if !mapped || (code != "incarnation_changed" && code != "cursor_expired") {
		return err
	}
	meta, metaErr := store.Meta(ctx)
	if metaErr != nil {
		return err
	}
	reason := "failover"
	if code == "cursor_expired" {
		reason = "history_floor"
	}
	return &core.BoundaryError{Code: string(code), From: from, Head: core.Head{Incarnation: meta.Incarnation, Revision: meta.Head, Floor: meta.HistoryFloor}, Reason: reason}
}

type Checkpoints struct {
	store driver.CheckpointStore
	lease driver.ActiveLease
}

func NewCheckpoints(store any, lease driver.ActiveLease) (*Checkpoints, error) {
	persistence, ok := store.(driver.CheckpointStore)
	if !ok {
		return nil, driver.ErrUnsupported
	}
	if lease == nil {
		return nil, fmt.Errorf("checkpoint adapter requires active lease")
	}
	return &Checkpoints{store: persistence, lease: lease}, nil
}
func checkpointKey(binding string) string { return "events:" + binding }
func (c *Checkpoints) Load(ctx context.Context, binding string) (core.State, uint64, error) {
	var state core.State
	if err := driver.ValidateCheckpointKey(binding); err != nil {
		return state, 0, err
	}
	record, err := c.store.LoadCheckpoint(ctx, checkpointKey(binding))
	if err != nil {
		return state, 0, err
	}
	if record.Generation == 0 {
		if len(record.Payload) != 0 {
			return state, 0, fmt.Errorf("invalid absent checkpoint payload")
		}
		return state, 0, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(record.Payload))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&state); err != nil {
		return state, record.Generation, fmt.Errorf("invalid persisted event checkpoint: %w", err)
	}
	if err = decoder.Decode(new(any)); err != io.EOF {
		return state, record.Generation, fmt.Errorf("invalid trailing checkpoint data")
	}
	if err = state.Position.Validate(); err != nil {
		return state, record.Generation, err
	}
	if state.Failure != nil {
		if state.Failure.Key.Binding != binding || state.Failure.Key.Validate() != nil || state.Failure.Attempts == 0 {
			return state, record.Generation, fmt.Errorf("invalid persisted event failure")
		}
	}
	return state, record.Generation, nil
}
func (c *Checkpoints) Save(ctx context.Context, binding string, generation uint64, state core.State) (uint64, error) {
	if err := driver.ValidateCheckpointKey(binding); err != nil {
		return generation, err
	}
	if err := state.Position.Validate(); err != nil {
		return generation, err
	}
	data, err := json.Marshal(state)
	if err != nil {
		return generation, err
	}
	record, err := c.store.SaveCheckpoint(ctx, c.lease, checkpointKey(binding), generation, data)
	return record.Generation, err
}

// Description is supplied by a host resolver when the committed operation has
// richer slot/mutator/causal metadata. It is never a public plugin event input.
type Description struct {
	Type  model.DurableEventType
	SetBy map[string]string
	Cause model.EventCause
}
type Describer interface {
	Describe(context.Context, model.Changeset, int, string) (Description, error)
}

type HistoryMaterializer struct {
	Store     ReadStore
	Describer Describer
}

func (m HistoryMaterializer) Materialize(ctx context.Context, set model.Changeset, index int) (model.DurableEvent, error) {
	if set.Events != nil {
		return m.materializeDescriptor(ctx, set, index)
	}
	var event model.DurableEvent
	if index < 0 || index >= len(set.Changes) {
		return event, core.ErrCursorInvalid
	}
	change := set.Changes[index]
	for _, path := range change.ChangedPaths {
		if !validJSONPointer(path) {
			return event, fmt.Errorf("committed changed path is not a JSON Pointer: %w", driver.ErrUnsupported)
		}
	}
	if !change.Ref.Branch.IsZero() {
		return event, driver.ErrUnsupported
	}
	before, err := m.image(ctx, set.Incarnation, change.Ref, change.FromRevision)
	if err != nil {
		return event, err
	}
	var after *driver.Record
	if change.Op != model.OpDelete {
		after, err = m.image(ctx, set.Incarnation, change.Ref, set.Revision)
		if err != nil {
			return event, err
		}
	}
	if before == nil && after == nil {
		return event, fmt.Errorf("committed change has no retained image")
	}
	kind := ""
	if before != nil {
		kind = before.Kind
	}
	if after != nil {
		kind = after.Kind
	}
	var description Description
	if m.Describer != nil {
		description, err = m.Describer.Describe(ctx, set, index, kind)
	} else {
		description, err = describeBasic(set, index, kind)
	}
	if err != nil {
		return event, err
	}
	beforeBody, err := body(before)
	if err != nil {
		return event, err
	}
	afterBody, err := body(after)
	if err != nil {
		return event, err
	}
	ref := change.Ref.ID
	if !change.Ref.IsID() {
		ref = change.Ref.Key
	}
	changed := append([]string{}, change.ChangedPaths...)
	return model.DurableEvent{Type: description.Type, Resource: kind + "/" + ref, Changed: changed, Before: beforeBody, After: afterBody, Principal: set.Principal, Chain: append([]string{}, set.Chain...), SetBy: description.SetBy, Cause: description.Cause, Derived: set.Kind == model.ChangesetDerived}, nil
}

func validJSONPointer(path string) bool {
	if path != "" && path[0] != '/' {
		return false
	}
	for i := 0; i < len(path); i++ {
		if path[i] == '~' {
			i++
			if i == len(path) || (path[i] != '0' && path[i] != '1') {
				return false
			}
		}
	}
	return true
}

func (m HistoryMaterializer) image(ctx context.Context, incarnation string, ref model.Ref, revision model.Revision) (*driver.Record, error) {
	if revision.IsZero() {
		return nil, nil
	}
	reader, err := m.Store.Read(ctx, driver.View{Incarnation: incarnation, At: &revision})
	if err != nil {
		return nil, boundaryError(ctx, m.Store, incarnation, err)
	}
	defer reader.Close()
	records, err := reader.Get(ctx, []driver.Ref{ref})
	if errors.Is(err, driver.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}
	if len(records) != 1 {
		return nil, fmt.Errorf("point read returned invalid record count")
	}
	return &records[0], nil
}
func body(record *driver.Record) (map[string]any, error) {
	if record == nil {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(record.Body))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if value == nil {
		return nil, fmt.Errorf("event image must be a JSON object")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("trailing event image data")
	}
	return value, nil
}
func describeBasic(set model.Changeset, index int, kind string) (Description, error) {
	change := set.Changes[index]
	switch change.Op {
	case model.OpCreate, model.OpPatch, model.OpSetMeta, model.OpReparent, model.OpLink, model.OpUnlink, model.OpComment, model.OpAttach, model.OpDelete:
	default:
		return Description{}, fmt.Errorf("unknown committed operation: %w", driver.ErrUnsupported)
	}
	// The current log sketch does not carry per-field mutator ownership or
	// nested causal provenance. Refuse to fabricate that audit information.
	if len(set.Mutators) > 0 || set.Principal.Kind == "plugin" {
		return Description{}, fmt.Errorf("committed mutator/causal provenance unavailable: %w", driver.ErrUnsupported)
	}
	d := Description{SetBy: map[string]string{}, Cause: model.EventCause{Root: set.Incarnation + ":" + set.Revision.String(), Chain: []string{}}}
	for _, path := range change.ChangedPaths {
		d.SetBy[strings.TrimPrefix(path, "/")] = "request"
	}
	if set.Kind == model.ChangesetDerived {
		d.Type = model.EventDerivedUpdated
		return d, nil
	}
	switch kind {
	case "bead":
		switch change.Op {
		case model.OpCreate:
			d.Type = model.EventBeadCreated
		case model.OpPatch, model.OpSetMeta:
			for _, path := range change.ChangedPaths {
				if path == "" || path == "/" || path == "/status" || strings.HasPrefix(path, "/status/") || path == "/fields" || path == "/fields/status" || strings.HasPrefix(path, "/fields/status/") || path == "/lease" || strings.HasPrefix(path, "/lease/") {
					return Description{}, fmt.Errorf("committed slot provenance unavailable: %w", driver.ErrUnsupported)
				}
			}
			d.Type = model.EventBeadUpdated
		case model.OpReparent:
			d.Type = model.EventBeadReparented
		case model.OpLink:
			d.Type = model.EventBeadLinked
		case model.OpUnlink:
			d.Type = model.EventBeadUnlinked
		case model.OpComment:
			d.Type = model.EventBeadCommented
		case model.OpAttach:
			d.Type = model.EventBeadAttached
		case model.OpDelete:
			d.Type = model.EventBeadDeleted
		}
	case "memory":
		if change.Op == model.OpDelete {
			d.Type = model.EventMemoryDeleted
		} else if change.Op == model.OpCreate || change.Op == model.OpPatch || change.Op == model.OpSetMeta {
			d.Type = model.EventMemorySaved
		}
	case "view":
		if change.Op == model.OpDelete {
			d.Type = model.EventViewDeleted
		} else if change.Op == model.OpCreate || change.Op == model.OpPatch || change.Op == model.OpSetMeta {
			d.Type = model.EventViewSaved
		}
	case "ontology":
		if change.Op == model.OpCreate || change.Op == model.OpPatch {
			d.Type = model.EventOntologyApplied
		}
	case "plugins", "plugin_set":
		if change.Op == model.OpCreate || change.Op == model.OpPatch {
			d.Type = model.EventPluginApplied
		}
	}
	if d.Type == "" {
		return Description{}, fmt.Errorf("no canonical event for committed operation: %w", driver.ErrUnsupported)
	}
	return d, nil
}
