package authz

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// HierarchyTarget is a validated image, not an authorization grant. The host
// must bind Before to its tracked stored source (or an earlier authorized
// create), resolve aliases, and check the child and new parent independently.
// Parent never supplies the child's project or an inherited permission.
type HierarchyTarget struct {
	Ref     model.Ref
	Project string
	Parent  *model.Ref
}

// HierarchyImage validates the finite component's source shape. A zero revision
// is allowed only for a host-proven, earlier staged creation. Stored hits and
// misses, ontology/closure integrity and the complete final graph remain host
// responsibilities, as they are for ValidateBeadWrite.
func HierarchyImage(record model.Record) (HierarchyTarget, error) {
	if CanonicalRef(record.Ref) != nil || strings.Contains(record.Ref.ID, "/") || record.Kind != "bead" || record.Body == nil || record.Revision.Validate() != nil {
		return HierarchyTarget{}, errors.New("canonical hierarchy bead image required")
	}
	id, ok := record.Body["id"].(string)
	if !ok || id != record.Ref.ID {
		return HierarchyTarget{}, errors.New("hierarchy image identity mismatch")
	}
	project, ok := record.Body["project"].(string)
	if !ok || !ValidID(project) {
		return HierarchyTarget{}, errors.New("explicit child project required")
	}
	kind, ok := record.Body["type"].(string)
	if !ok || !ValidID(kind) {
		return HierarchyTarget{}, errors.New("explicit bead type required")
	}
	if fields, ok := record.Body["fields"].(map[string]any); !ok || fields == nil {
		return HierarchyTarget{}, errors.New("hierarchy fields must be an object")
	}
	target := HierarchyTarget{Ref: record.Ref, Project: project}
	if parent, present := record.Body["parent"]; present && parent != nil {
		data, err := json.Marshal(parent)
		var ref model.Ref
		if err != nil || json.Unmarshal(data, &ref) != nil || CanonicalRef(ref) != nil || strings.Contains(ref.ID, "/") || ref.ID == record.Ref.ID {
			return HierarchyTarget{}, errors.New("canonical non-self parent required")
		}
		target.Parent = &ref
	}
	return target, nil
}

// ValidateHierarchyWrite is deliberately separate from ValidateBeadWrite:
// ordinary update/create/reparent paths gain no permit from this guard. Only
// create and reparent are mapped here. Reparent may change Parent but none of
// #18's other permission-bearing fields. It requires no old-parent right.
// The caller must still pass through the installed chain, including caveats,
// current context and restrictions, before and after mutation and on replay.
func ValidateHierarchyWrite(kind model.OpKind, before *model.Record, after model.Record, setter model.PrincipalRef, caller Caller) (HierarchyTarget, error) {
	if caller.Principal.Kind != "agent" || caller.Principal.Sponsor == nil || caller.Principal.Sponsor.Kind != "user" || !reflect.DeepEqual(setter, caller.Principal) {
		return HierarchyTarget{}, errors.New("hierarchy requires the authenticated sponsored Agent")
	}
	target, err := HierarchyImage(after)
	if err != nil {
		return HierarchyTarget{}, err
	}
	switch kind {
	case model.OpCreate:
		if before != nil {
			return HierarchyTarget{}, errors.New("hierarchy create requires a tracked source miss")
		}
		// No owner/assignee/restricted/label/deputy assignment is introduced by
		// this bounded creation adapter. Ontology fields remain host-guarded.
		for key := range after.Body {
			switch key {
			case "id", "type", "project", "fields", "parent":
			case "createdAt":
				// The trusted Writer stamps this core member before policy runs.
				// It carries no permission and is never a caller assignment.
				text, ok := after.Body[key].(string)
				instant, err := time.Parse(time.RFC3339Nano, text)
				if !ok || err != nil || instant.IsZero() || instant.UTC().Format(time.RFC3339Nano) != text {
					return HierarchyTarget{}, errors.New("invalid core creation instant")
				}
			default:
				return HierarchyTarget{}, errors.New("unsupported hierarchy create member")
			}
		}
	case model.OpReparent:
		if before == nil {
			return HierarchyTarget{}, errors.New("hierarchy move requires a bound child source")
		}
		if _, err := HierarchyImage(*before); err != nil || before.Ref.ID != after.Ref.ID {
			return HierarchyTarget{}, errors.New("invalid hierarchy child source")
		}
		for _, key := range []string{"project", "owner", "assignee", "type", "labels", "restricted"} {
			old, had := before.Body[key]
			next, has := after.Body[key]
			if had != has || !reflect.DeepEqual(old, next) {
				return HierarchyTarget{}, errors.New("permission-bearing hierarchy reassignment is unsupported")
			}
		}
	default:
		return HierarchyTarget{}, errors.New("unsupported hierarchy operation")
	}
	return target, nil
}
