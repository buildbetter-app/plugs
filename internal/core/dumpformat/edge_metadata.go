package dumpformat

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// EdgeMetadataIdentityError retains the rejected envelope identity without
// adding source values to the refusal text. EnvelopeID is the detached JSON
// value, including exact json.Number spellings; EnvelopePresent distinguishes
// an absent ID from JSON null. SourceTable is empty for standalone inspection.
type EdgeMetadataIdentityError struct {
	SourceTable     string
	EdgeID          string
	EnvelopeID      any
	EnvelopePresent bool
}

func (*EdgeMetadataIdentityError) Error() string {
	return "edge metadata envelope identity mismatch"
}

// SourceEdgeMetadata validates the declared edge wire contract and returns
// only its scheduling predicate. Body.metadata and Source.Fields.metadata
// remain the exact input value, including SQL JSON text and number spellings.
// The v1 external vocabulary keeps its existing strict waits projection.
func SourceEdgeMetadata(version int, edgeID, kind string, value any) (*model.LinkMetadata, error) {
	projection, err := InspectSourceEdgeMetadata(version, edgeID, kind, value)
	return projection.Predicate, err
}

func sourceTableEdgeMetadata(version int, table, edgeID, kind string, value any) (*model.LinkMetadata, error) {
	predicate, err := SourceEdgeMetadata(version, edgeID, kind, value)
	var identity *EdgeMetadataIdentityError
	if errors.As(err, &identity) {
		identity.SourceTable = table
	}
	return predicate, err
}

// EdgeMetadataProjection separates a detached, unwrapped source value from
// its typed predicate. Envelope identifies an already validated flat envelope;
// reverse conversion can retain the original representation without wrapping
// it again. Value preserves an SQL-text payload as text, not a decoded object.
type EdgeMetadataProjection struct {
	Predicate *model.LinkMetadata
	Value     any
	Envelope  bool
}

// InspectSourceEdgeMetadata applies the same validation used by the reader
// and planner. It never changes the input or reinterprets unknown nested text.
func InspectSourceEdgeMetadata(version int, edgeID, kind string, value any) (EdgeMetadataProjection, error) {
	result := EdgeMetadataProjection{Value: cloneValue(value)}
	if version == 1 {
		if kind == "waits-for" {
			predicate, err := SourceWaitsForMetadata(value)
			if err != nil {
				return EdgeMetadataProjection{}, err
			}
			result.Predicate = predicate
			return result, nil
		}
		return result, nil
	}
	if !IsTypedEdgeVersion(version) {
		return EdgeMetadataProjection{}, fmt.Errorf("unsupported edge metadata version %d", version)
	}
	if value == nil {
		return result, nil // SQL NULL selects the legacy waits predicate.
	}
	decoded, err := sourceJSONValue(value)
	if err != nil {
		return EdgeMetadataProjection{}, err
	}
	if object, ok := decoded.(map[string]any); ok {
		reserved := false
		for key := range object {
			reserved = reserved || key == "_plug_edge_id" || key == "_plug_metadata"
		}
		if reserved {
			envelopeID, present := object["_plug_edge_id"]
			id, ok := envelopeID.(string)
			if !ok || id == "" || id != edgeID {
				return EdgeMetadataProjection{}, &EdgeMetadataIdentityError{
					EdgeID: edgeID, EnvelopeID: envelopeID, EnvelopePresent: present,
				}
			}
			for key := range object {
				if key != "_plug_edge_id" && key != "_plug_metadata" {
					return EdgeMetadataProjection{}, fmt.Errorf("invalid edge metadata envelope member %q", key)
				}
			}
			payload, present := object["_plug_metadata"]
			result.Envelope = true
			result.Value = cloneValue(payload)
			if !present {
				return result, nil // ID-only is absent metadata, never explicit {}.
			}
			if payload == nil {
				return EdgeMetadataProjection{}, fmt.Errorf("edge metadata envelope must omit a null payload")
			}
			decoded = payload
			if _, sqlText := payload.(string); sqlText && kind == "waits-for" {
				decoded, err = sourceJSONValue(payload)
				if err != nil {
					return EdgeMetadataProjection{}, err
				}
			}
		}
	}
	if err := rejectReservedEdgeMetadata(decoded); err != nil {
		return EdgeMetadataProjection{}, err
	}
	if kind != "waits-for" {
		return result, nil // Conditional metadata is valid JSON, with no predicate.
	}
	object, ok := decoded.(map[string]any)
	if !ok || object == nil {
		return EdgeMetadataProjection{}, fmt.Errorf("waits-for metadata must be a JSON object")
	}
	metadata := model.LinkMetadata{Gate: "all-children"}
	if raw, present := object["gate"]; present {
		gate, ok := raw.(string)
		if !ok {
			return EdgeMetadataProjection{}, fmt.Errorf("waits-for gate must be all-children or any-children")
		}
		// The producer also admits an explicit empty gate. Apply the existing
		// canonical default only to the predicate; Value and both raw mirrors
		// retain the original empty string and its source representation.
		predicate, err := (model.LinkMetadata{Gate: gate}).Normalized()
		if err != nil {
			return EdgeMetadataProjection{}, fmt.Errorf("waits-for gate must be all-children or any-children")
		}
		metadata.Gate = predicate.Gate
	}
	if raw, present := object["also_blocks"]; present {
		alsoBlocks, ok := raw.(bool)
		if !ok {
			return EdgeMetadataProjection{}, fmt.Errorf("waits-for also_blocks must be boolean")
		}
		metadata.AlsoBlocks = alsoBlocks
	}
	if raw, present := object["spawner_id"]; present {
		if _, ok := raw.(string); !ok {
			return EdgeMetadataProjection{}, fmt.Errorf("waits-for spawner_id must be text")
		}
	}
	// Unknown nonreserved keys and spawner_id stay opaque in the raw mirrors.
	// In particular, spawner_id cannot substitute for the typed edge target.
	result.Predicate = &metadata
	return result, nil
}

func sourceJSONValue(value any) (any, error) {
	var data []byte
	if text, ok := value.(string); ok {
		data = []byte(text)
	} else {
		var err error
		data, err = json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("source edge metadata: %w", err)
		}
	}
	var decoded any
	if err := decodeJSON(data, &decoded); err != nil {
		return nil, fmt.Errorf("source edge metadata: %w", err)
	}
	return decoded, nil
}

func rejectReservedEdgeMetadata(value any) error {
	switch value := value.(type) {
	case map[string]any:
		for key, child := range value {
			if key == "_plug_edge_id" || key == "_plug_metadata" {
				return fmt.Errorf("nested or reserved edge metadata member %q", key)
			}
			if err := rejectReservedEdgeMetadata(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range value {
			if err := rejectReservedEdgeMetadata(child); err != nil {
				return err
			}
		}
	}
	return nil
}
