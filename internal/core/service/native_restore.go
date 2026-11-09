package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// ValidateNativeRestoreRecords applies ordinary final-image field rules to
// beads and structural persisted-image rules to memories in a native restore.
// Native restore is a direct storage publication path, so it cannot rely on the
// ordinary Writer callbacks to perform this check after a transaction opens.
//
// The protected ontology record, when present, is compiled first and supplies
// the effective type/status/field definitions for the restored projects. The
// restore request has no configured source locator, so multiple protected
// sources are ambiguous and must be refused. An archive without one uses the
// built-in ontology, matching the ordinary no-plugin writer. The error names
// the offending record so the restore boundary can expose a typed,
// state-preserving refusal.
func ValidateNativeRestoreRecords(records []driver.Record) error {
	base := ontology.Default()
	resolve := func(string) ontology.Effective { return base }
	var sourceID string
	for _, record := range records {
		if record.Kind != "ontology" {
			continue
		}
		if sourceID != "" {
			return nativeRestoreRecordError(record, fmt.Errorf("multiple protected ontology sources (%s and %s) without a selected source identity", sourceID, record.Ref.ID))
		}
		sourceID = record.Ref.ID
		source, err := DecodeOntologySource(record.Body)
		if err != nil {
			return nativeRestoreRecordError(record, fmt.Errorf("invalid ontology source: %w", err))
		}
		snapshot, problems := ontology.Compile(source.Base, source.Overlays, source.Plugins)
		if len(problems) != 0 {
			return nativeRestoreRecordError(record, fmt.Errorf("ontology source does not compile: %s", problems[0].Message))
		}
		resolve = snapshot.Effective
	}

	for _, record := range records {
		var err error
		switch record.Kind {
		case "bead":
			var doc pipeline.Document
			doc, err = decodeDocument(record.Body)
			if err == nil && doc.ID != record.Ref.ID {
				err = failure("invalid_params", "document ID differs from record reference")
			}
			if err == nil {
				effective := resolve(doc.Project)
				state := state(doc)
				// A native image may contain a bead already in any valid
				// lifecycle status. Validate the final image with the ordinary
				// field/type/status rules, without treating restore as a new
				// lifecycle create transition.
				// Ordinary writes normalize implicit status, priority, template,
				// and field defaults before this final validation. Do the same
				// against a detached candidate so legacy native images that omit
				// those implicit values remain admissible without changing bytes
				// that the restore publishes.
				normalized, defaultsViolations := ontology.Defaults(effective, state)
				if len(defaultsViolations) != 0 {
					err = violationsError(defaultsViolations)
				} else if violations := ontology.Validate(effective, &normalized, &normalized, "bead.update", ontology.GuardFacts{}); len(violations) != 0 {
					err = violationsError(violations)
				}
			}
		case "memory":
			var doc pipeline.MemoryDocument
			// Restore preserves an existing memory image, including legacy
			// empty content. The ordinary creation-time content rule does not
			// apply, but the stored content member must still be a string.
			var shape struct {
				Content *string `json:"content"`
			}
			if json.Unmarshal(record.Body, &shape) != nil || shape.Content == nil {
				err = failure("invalid_params", "memory content must be a string")
			} else {
				doc, err = pipeline.DecodeMemoryDocument(record.Body)
			}
			if err == nil {
				err = memoryRef(record.Ref)
			}
			if err == nil && doc.ID != record.Ref.ID {
				err = failure("invalid_params", "memory document ID differs from record reference")
			}
			if err == nil && strings.TrimSpace(doc.Key) == "" {
				err = failure("invalid_params", "memory key must be nonempty")
			}
			if err == nil {
				if _, cloneErr := cloneMemoryMetadata(doc.Metadata); cloneErr != nil {
					err = failure("invalid_params", "memory metadata must be a JSON object")
				}
			}
		}
		if err != nil {
			return nativeRestoreRecordError(record, err)
		}
	}
	return nil
}

func nativeRestoreRecordError(record driver.Record, err error) error {
	var typed *model.Error
	if errors.As(err, &typed) && typed != nil && typed.Data.Hint != "" {
		return fmt.Errorf("native record %s/%s: %s: %w", record.Kind, record.Ref.ID, typed.Data.Hint, err)
	}
	if errors.As(err, &typed) && typed != nil {
		if violations, ok := typed.Data.Details["violations"].([]ontology.Violation); ok && len(violations) != 0 {
			return fmt.Errorf("native record %s/%s: %s: %w", record.Kind, record.Ref.ID, violations[0].Message, err)
		}
	}
	return fmt.Errorf("native record %s/%s: %w", record.Kind, record.Ref.ID, err)
}
