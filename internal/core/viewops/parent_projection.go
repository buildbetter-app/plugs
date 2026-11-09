package viewops

import (
	"context"
	"encoding/json"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// ParentDisclosure carries only an authorized parent identity or existence.
// Policy remains in the trusted adapter; a parent link never grants access.
type ParentDisclosure struct {
	Exists bool
	Ref    *model.Ref
}

type ParentProjectionAuthority struct {
	Parents       map[string]ParentDisclosure
	PolicyBinding string
}

// ParentProjectionConfig projects all emitted ordinary view rows in one
// current authority snapshot against the supplied pinned content reader.
// ValidateAuthority fences the binding after every projection, before publish.
type ParentProjectionConfig struct {
	Project           func(context.Context, model.PrincipalRef, driver.Reader, []model.Ref) (ParentProjectionAuthority, error)
	ValidateAuthority func(context.Context, model.PrincipalRef, string) error
}

// MarshalJSON keeps a visible parent's existing bare-string shape. Hidden
// parents have exactly one member, exists:true; roots omit parent entirely.
func (b Bead) MarshalJSON() ([]byte, error) {
	type plain Bead
	var parent any
	if b.ParentHidden {
		parent = struct {
			Exists bool `json:"exists"`
		}{true}
	} else if b.Parent != "" {
		parent = b.Parent
	}
	return json.Marshal(struct {
		plain
		Parent any `json:"parent,omitempty"`
	}{plain: plain(b), Parent: parent})
}

func (s *Service) projectParents(ctx context.Context, by model.PrincipalRef, content driver.Reader, rows []stored, fields []string, result *Result) error {
	cfg := s.cfg.ParentProjection
	if cfg == nil || (fields != nil && !hasField(fields, "parent")) {
		return s.admit(ctx, by)
	}
	items := make([]*Bead, 0, len(result.Items))
	for i := range result.Items {
		items = append(items, &result.Items[i])
	}
	for i := range result.Groups {
		for j := range result.Groups[i].Items {
			items = append(items, &result.Groups[i].Items[j])
		}
	}
	refs := make([]model.Ref, 0, len(items))
	wanted := make(map[string]bool, len(items))
	for _, item := range items {
		if !wanted[item.ID] {
			wanted[item.ID] = true
			refs = append(refs, model.RefByID(item.ID, model.Natural64(0)))
		}
	}
	if len(refs) == 0 {
		return s.admit(ctx, by)
	}
	authority, err := cfg.Project(ctx, by, content, refs)
	if err != nil {
		return mapped(err)
	}
	if authority.PolicyBinding == "" || len(authority.Parents) != len(refs) {
		return epicUnavailable("parent projection authority is incomplete")
	}
	for _, item := range items {
		disclosure, ok := authority.Parents[item.ID]
		row, exists := findStored(rows, item.ID)
		if !ok || !exists || disclosure.Exists != (row.doc.Parent != nil) {
			return epicUnavailable("parent projection differs from pinned membership")
		}
		item.Parent = ""
		item.ParentHidden = disclosure.Exists && disclosure.Ref == nil
		if ref := disclosure.Ref; ref != nil {
			if !disclosure.Exists || ref.Validate() != nil || !ref.IsID() || !ref.Branch.IsZero() || ref.ID == "" || row.doc.Parent == nil || ref.ID != row.doc.Parent.ID {
				return epicUnavailable("parent projection returned another parent")
			}
			parent, exists := findStored(rows, ref.ID)
			if !exists {
				records, err := content.Get(ctx, []model.Ref{*ref})
				if err != nil {
					return mapped(err)
				}
				if len(records) != 1 || records[0].Kind != "bead" || !driver.SelectionSameRef(records[0].Ref, *ref) {
					return epicUnavailable("parent projection source is unavailable")
				}
				doc, err := decodeDocument(records[0].Body)
				if err != nil || doc.ID != ref.ID {
					return epicUnavailable("parent projection source is invalid")
				}
				parent = stored{record: records[0], doc: doc}
			}
			allowed, err := s.authorize(ctx, by, parent)
			if err != nil {
				return mapped(err)
			}
			if allowed {
				item.Parent = ref.ID
			} else {
				item.ParentHidden = true
			}
		}
	}
	if err := cfg.ValidateAuthority(ctx, by, authority.PolicyBinding); err != nil {
		return mapped(err)
	}
	return s.admit(ctx, by)
}
