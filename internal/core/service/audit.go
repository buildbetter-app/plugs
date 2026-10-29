package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
)

// OntologySource is the internal body of one protected ontology record. It is
// deliberately composed of the existing pure ontology types; it is not a
// plugin or transport resource schema. The digest is always computed from the
// decoded documents and is never accepted from the stored body.
type OntologySource struct {
	Base     ontology.Document            `json:"base"`
	Overlays map[string]ontology.Document `json:"overlays,omitempty"`
	Plugins  ontology.PluginCatalog       `json:"plugins,omitempty"`
}

// OntologySourceLocator selects the protected source record for a bead's
// project. The returned reference is still validated and read by the audit
// adapter; a locator cannot supply a digest or a decision.
type OntologySourceLocator func(project string) (model.Ref, error)

// StaticOntologySource is a small locator for deployments whose one protected
// ontology record contains both the base and all project overlays. It remains
// durable and pinned because the adapter reads the record on every decision.
func StaticOntologySource(ref model.Ref) OntologySourceLocator {
	return func(string) (model.Ref, error) { return ref, nil }
}

// TrackedOntologyAudit is the service-owned implementation of the optional
// conditional audit port. Normal lifecycle validation remains on Ontology and
// must be installed separately with pipeline.WithOntology.
type TrackedOntologyAudit struct {
	locate OntologySourceLocator
}

// NewOntologyAudit constructs an adapter that obtains source records through a
// tracked SnapshotReader. A nil locator is retained as an explicit refusal,
// rather than silently falling back to NewOntology's static resolver.
func NewOntologyAudit(locator OntologySourceLocator) *TrackedOntologyAudit {
	return &TrackedOntologyAudit{locate: locator}
}

// NewTrackedOntologyAudit is an explicit spelling for callers that want to
// emphasize that decisions are read from a tracked snapshot.
func NewTrackedOntologyAudit(locator OntologySourceLocator) *TrackedOntologyAudit {
	return NewOntologyAudit(locator)
}

var _ port.OntologyAudit = (*TrackedOntologyAudit)(nil)

// MarshalOntologySource is a test/composition helper for the internal
// protected-record body. It does not add a public resource or digest field.
func MarshalOntologySource(source OntologySource) ([]byte, error) {
	return json.Marshal(source)
}

func protectedOntologyRef(ref model.Ref) bool {
	return ref.IsID() && ref.ID != "" && ref.Branch.IsZero() && (ref.ID == "ontology" || strings.HasPrefix(ref.ID, "ontology/"))
}

func sameResolvedRef(left, right model.Ref) bool {
	return left.IsID() && right.IsID() && left.ID == right.ID && left.Branch.String() == right.Branch.String()
}

func auditError(message string) error {
	return failure("invalid_params", message)
}

// DecideConditional resolves both images in the blocker's own project. Every
// source read goes through reader.Get so the pipeline's commit guard can reject
// an ontology change that races this decision.
func (a *TrackedOntologyAudit) DecideConditional(ctx context.Context, change port.Change, reader port.SnapshotReader) (port.ConditionalDecision, error) {
	if err := ctx.Err(); err != nil {
		return port.ConditionalDecision{}, err
	}
	if a == nil || a.locate == nil {
		return port.ConditionalDecision{}, auditError("conditional audit has no durable ontology source locator")
	}
	if reader == nil {
		return port.ConditionalDecision{}, auditError("conditional audit requires a tracked snapshot reader")
	}
	if change.Resource.Kind != "bead" || !change.Resource.Ref.IsID() || change.Resource.Ref.ID == "" || !change.Resource.Ref.Branch.IsZero() || change.Op.Kind != model.OpPatch {
		return port.ConditionalDecision{}, auditError("conditional audit requires a resolved bead patch")
	}
	before, err := pipeline.DecodeDocument(change.Before)
	if err != nil {
		return port.ConditionalDecision{}, auditError("conditional audit has an invalid blocker before image")
	}
	after, err := pipeline.DecodeDocument(change.After)
	if err != nil {
		return port.ConditionalDecision{}, auditError("conditional audit has an invalid blocker after image")
	}
	if before.ID != change.Resource.Ref.ID || after.ID != change.Resource.Ref.ID || before.ID != after.ID {
		return port.ConditionalDecision{}, auditError("conditional audit images do not identify the blocker")
	}
	if before.Type != after.Type || before.Project != after.Project {
		return port.ConditionalDecision{}, auditError("conditional audit requires unchanged blocker type and project")
	}
	beforeStatus, ok := before.Fields["status"].(string)
	if !ok || beforeStatus == "" {
		return port.ConditionalDecision{}, auditError("conditional audit before image has no status")
	}
	afterStatus, ok := after.Fields["status"].(string)
	if !ok || afterStatus == "" {
		return port.ConditionalDecision{}, auditError("conditional audit after image has no status")
	}
	if err := ctx.Err(); err != nil {
		return port.ConditionalDecision{}, err
	}
	sourceRef, err := a.locate(after.Project)
	if err != nil {
		return port.ConditionalDecision{}, err
	}
	if err := sourceRef.Validate(); err != nil || !protectedOntologyRef(sourceRef) {
		return port.ConditionalDecision{}, auditError("ontology source locator returned an unprotected branch-zero ID")
	}
	rows, err := reader.Get(ctx, []model.Ref{sourceRef})
	if err != nil {
		return port.ConditionalDecision{}, err
	}
	if err := ctx.Err(); err != nil {
		return port.ConditionalDecision{}, err
	}
	if len(rows) != 1 || !sameResolvedRef(rows[0].Ref, sourceRef) || rows[0].Kind != "ontology" {
		return port.ConditionalDecision{}, auditError("ontology source record is missing or has an unsupported kind")
	}
	source, err := decodeOntologySource(rows[0].Body)
	if err != nil {
		return port.ConditionalDecision{}, auditError("unsupported protected ontology source: " + err.Error())
	}
	snapshot, problems := ontology.Compile(source.Base, source.Overlays, source.Plugins)
	if len(problems) != 0 {
		return port.ConditionalDecision{}, auditError("protected ontology source does not compile")
	}
	effective := snapshot.Effective(after.Project)
	beforeDecision, err := conditionDecision(effective, before.Type, beforeStatus)
	if err != nil {
		return port.ConditionalDecision{}, err
	}
	afterDecision, err := conditionDecision(effective, after.Type, afterStatus)
	if err != nil {
		return port.ConditionalDecision{}, err
	}
	var project *model.Ref
	if after.Project != "" {
		value := model.Ref{ID: after.Project}
		if err := value.Validate(); err != nil {
			return port.ConditionalDecision{}, auditError("blocker project is not a valid protected ontology project")
		}
		project = &value
	}
	pin := model.OntologyPin{Ref: sourceRef, Revision: rows[0].Revision, Digest: snapshot.Digest(), Project: project}
	if err := pin.Validate(); err != nil {
		return port.ConditionalDecision{}, auditError("protected ontology pin is invalid")
	}
	return port.ConditionalDecision{Ontology: pin, Before: beforeDecision, After: afterDecision}, nil
}

func conditionDecision(effective ontology.Effective, typ, status string) (model.ConditionDecision, error) {
	if _, violations := effective.Schema(typ, status); len(violations) != 0 {
		return model.ConditionDecision{}, auditError("ontology does not define an active blocker type/status")
	}
	definition, ok := effective.Status(status)
	if !ok || definition.Archived {
		return model.ConditionDecision{}, auditError("ontology status is missing or archived")
	}
	decision := model.ConditionDecision{
		Status:    status,
		Category:  string(definition.Category),
		Outcome:   string(definition.Outcome),
		Terminal:  definition.Category.Terminal(),
		Satisfied: effective.SatisfiesConditionalBlocks(status),
	}
	if err := decision.Validate(); err != nil {
		return model.ConditionDecision{}, auditError("ontology produced an invalid condition decision")
	}
	return decision, nil
}

// decodeOntologySource accepts either the internal {base, overlays, plugins}
// envelope or a plain ontology.Document for the base-only case. Both forms
// use the pure decoder; unknown members, duplicate JSON keys and arbitrary
// digest/project assertions are refused.
func decodeOntologySource(raw []byte) (OntologySource, error) {
	value, err := uniqueJSON(raw)
	if err != nil {
		return OntologySource{}, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return OntologySource{}, fmt.Errorf("source must be a JSON object")
	}
	if _, hasBase := object["base"]; !hasBase {
		data, err := json.Marshal(object)
		if err != nil {
			return OntologySource{}, err
		}
		base, err := ontology.Decode(data)
		if err != nil {
			return OntologySource{}, err
		}
		return OntologySource{Base: base}, nil
	}
	for key := range object {
		if key != "base" && key != "overlays" && key != "plugins" {
			return OntologySource{}, fmt.Errorf("unknown source member %q", key)
		}
	}
	baseValue, ok := object["base"].(map[string]any)
	if !ok {
		return OntologySource{}, fmt.Errorf("base must be an ontology document")
	}
	baseData, err := json.Marshal(baseValue)
	if err != nil {
		return OntologySource{}, err
	}
	base, err := ontology.Decode(baseData)
	if err != nil {
		return OntologySource{}, err
	}
	source := OntologySource{Base: base}
	if overlaysValue, present := object["overlays"]; present {
		overlays, ok := overlaysValue.(map[string]any)
		if !ok {
			return OntologySource{}, fmt.Errorf("overlays must be an object")
		}
		source.Overlays = map[string]ontology.Document{}
		for project, value := range overlays {
			document, ok := value.(map[string]any)
			if !ok || project == "" {
				return OntologySource{}, fmt.Errorf("overlay %q must be a nonempty project ontology document", project)
			}
			data, err := json.Marshal(document)
			if err != nil {
				return OntologySource{}, err
			}
			decoded, err := ontology.Decode(data)
			if err != nil {
				return OntologySource{}, fmt.Errorf("overlay %q: %w", project, err)
			}
			source.Overlays[project] = decoded
		}
	}
	if pluginsValue, present := object["plugins"]; present {
		plugins, ok := pluginsValue.(map[string]any)
		if !ok {
			return OntologySource{}, fmt.Errorf("plugins must be an object")
		}
		source.Plugins = ontology.PluginCatalog{}
		for name, value := range plugins {
			available, ok := value.(bool)
			if !ok || name == "" {
				return OntologySource{}, fmt.Errorf("plugin catalog entry %q must be a boolean", name)
			}
			source.Plugins[name] = available
		}
	}
	return source, nil
}

func uniqueJSON(raw []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	value, err := uniqueJSONValue(dec)
	if err != nil {
		return nil, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("source has trailing JSON")
		}
		return nil, err
	}
	return value, nil
}

func uniqueJSONValue(dec *json.Decoder) (any, error) {
	token, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch token {
	case json.Delim('{'):
		object := map[string]any{}
		for dec.More() {
			member, err := dec.Token()
			if err != nil {
				return nil, err
			}
			key, ok := member.(string)
			if !ok {
				return nil, fmt.Errorf("source object key is not a string")
			}
			if _, exists := object[key]; exists {
				return nil, fmt.Errorf("duplicate source member %q", key)
			}
			value, err := uniqueJSONValue(dec)
			if err != nil {
				return nil, err
			}
			object[key] = value
		}
		if _, err := dec.Token(); err != nil {
			return nil, err
		}
		return object, nil
	case json.Delim('['):
		values := []any{}
		for dec.More() {
			value, err := uniqueJSONValue(dec)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		if _, err := dec.Token(); err != nil {
			return nil, err
		}
		return values, nil
	default:
		return token, nil
	}
}
