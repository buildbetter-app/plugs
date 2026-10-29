package service

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/port"
)

// Ontology adapts pure workflow rules to the pipeline's tracked snapshot port.
// The local slice has no authorization provider or gate engine. Permission is
// inert without a provider; gates remain unobserved and fail closed if required.
type Ontology struct {
	resolve func(string) ontology.Effective
}

func NewOntology(resolve func(string) ontology.Effective) *Ontology {
	if resolve == nil {
		base := ontology.Default()
		resolve = func(string) ontology.Effective { return base }
	}
	return &Ontology{resolve: resolve}
}

func (o *Ontology) effective(project string) ontology.Effective { return o.resolve(project) }

// A detached JSON proposal represents a missing raw-message image as null.
func hasDocument(raw []byte) bool {
	return len(raw) != 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func violationsError(violations []ontology.Violation) error {
	if len(violations) == 0 {
		return nil
	}
	return &model.Error{Data: model.ErrorData{Code: violations[0].Code, Details: map[string]any{"violations": violations}}}
}

func (o *Ontology) Normalize(ctx context.Context, proposal port.ProposedChange, _ port.SnapshotReader) ([]port.Patch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	patches := []port.Patch{}
	for _, change := range proposal.Changes {
		if change.Resource.Kind != "bead" || !hasDocument(change.After) {
			continue
		}
		doc, err := decodeDocument(change.After)
		if err != nil {
			return nil, err
		}
		before := state(doc)
		after := before
		e := o.effective(doc.Project)
		if !hasDocument(change.Before) {
			var violations []ontology.Violation
			after, violations = ontology.Defaults(e, before)
			if err := violationsError(violations); err != nil {
				return nil, err
			}
		} else if slot, lifecycle := ontology.SlotForMethod(proposal.Meta.Method); lifecycle && change.Op.Kind == model.OpPatch {
			target, ok := e.SlotStatus(doc.Type, slot)
			if !ok {
				return nil, failure("invalid_params", "lifecycle slot is unavailable for this type")
			}
			after.Status = target
		}
		paths := map[string]json.RawMessage{}
		if _, exists := doc.Fields["priority"]; !exists && !hasDocument(change.Before) {
			paths["fields.priority"] = json.RawMessage("2")
		}
		if after.Status != before.Status {
			raw, err := json.Marshal(after.Status)
			if err != nil {
				return nil, err
			}
			paths["fields.status"] = raw
		}
		for name, value := range after.Fields {
			old, exists := before.Fields[name]
			if !exists || !reflect.DeepEqual(old, value) {
				raw, err := json.Marshal(value)
				if err != nil {
					return nil, err
				}
				paths["fields."+name] = raw
			}
		}
		if len(paths) != 0 {
			patches = append(patches, port.Patch{Resource: change.Resource, Paths: paths})
		}
	}
	return patches, nil
}

func (o *Ontology) Validate(ctx context.Context, proposal port.ProposedChange, reader port.SnapshotReader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var graph map[string]storedBead
	if _, lifecycle := ontology.SlotForMethod(proposal.Meta.Method); lifecycle {
		var err error
		graph, err = scanBeads(ctx, reader)
		if err != nil {
			return err
		}
		// Guards evaluate the whole atomic post-image, including dependencies
		// changed in the same changeset. The query tracks graph/blocking, so a
		// concurrent insertion cannot turn an observed absence into an allow.
		for _, change := range proposal.Changes {
			if change.Resource.Kind != "bead" {
				continue
			}
			if !hasDocument(change.After) {
				delete(graph, change.Resource.Ref.ID)
				continue
			}
			doc, err := decodeDocument(change.After)
			if err != nil {
				return failure("internal_error", "invalid proposed bead")
			}
			graph[doc.ID] = storedBead{doc: doc}
		}
	}
	for _, change := range proposal.Changes {
		if change.Resource.Kind != "bead" || !hasDocument(change.After) {
			continue
		}
		doc, err := decodeDocument(change.After)
		if err != nil {
			return err
		}
		after := state(doc)
		var before *ontology.BeadState
		if hasDocument(change.Before) {
			old, err := decodeDocument(change.Before)
			if err != nil {
				return failure("internal_error", "invalid prior bead")
			}
			v := state(old)
			before = &v
		}
		facts := ontology.GuardFacts{SetBy: change.SetBy}
		// Include the request's original paths even if a patch writes the same
		// value. Final-image comparison alone would let direct status edits pass.
		if change.Op.Patch != nil {
			for name := range change.Op.Patch.Fields {
				facts.Touched = append(facts.Touched, "fields."+name)
			}
		}
		if graph != nil {
			noBlockers, err := o.noOpenBlockers(doc, graph)
			if err != nil {
				return err
			}
			// Every graph row was read through the tracked query and strictly
			// decoded. An unsupported parent relation would have refused the
			// scan, so this is an observed absence in the supported envelope.
			noChildren := true
			facts.NoOpenBlockers, facts.NoOpenChildren = &noBlockers, &noChildren
			facts.Evidence = map[string]bool{}
			for _, attachment := range doc.Attachments {
				if attachment.URI != "" || attachment.ContentRef != "" {
					facts.Evidence[attachment.Kind] = true
				}
			}
		}
		// The slot patch and optional reason comment share the final target
		// post-image; neither introduces an independent status transition.
		if err := violationsError(ontology.Validate(o.effective(doc.Project), before, &after, proposal.Meta.Method, facts)); err != nil {
			return err
		}
	}
	return nil
}

var _ port.Ontology = (*Ontology)(nil)
