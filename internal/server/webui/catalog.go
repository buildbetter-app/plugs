package webui

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"unicode"
)

// Catalog is the host-owned, declarative surface that a UI plugin can add to
// the built-in views. Catalog entries are data: the browser renders their
// labels and fields as text and never evaluates a plugin-supplied expression.
type Catalog struct {
	Panels  []Panel  `json:"panels,omitempty"`
	Columns []Column `json:"columns,omitempty"`
	Buttons []Button `json:"buttons,omitempty"`
}

type Panel struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	View  string `json:"view"`
}

type Column struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	Field string `json:"field"`
	View  string `json:"view"`
}

type Button struct {
	ID     string          `json:"id"`
	Label  string          `json:"label"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params,omitempty"`
}

var catalogIdentifier = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
var catalogField = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]{0,127}$`)

// DefaultCatalog is the small D31 catalog used when a composition does not
// supply additions of its own. It describes the UX decision without granting
// a write or drawing a plugin-owned surface.
func DefaultCatalog() Catalog {
	return Catalog{
		Panels:  []Panel{{ID: "changes", Title: "Changes since I looked", View: "home"}},
		Columns: []Column{{ID: "blocked-by", Title: "Blocked by", Field: "blockedBy", View: "blocked"}},
		Buttons: []Button{{ID: "claim-next", Label: "Claim next", Method: "bead.claimNext"}},
	}
}

// Validate rejects ambiguous or executable catalog data before it reaches the
// browser. Empty catalogs are valid for compositions that only use the core
// page and iframe bridge.
func (c Catalog) Validate() error {
	if len(c.Panels) > 32 || len(c.Columns) > 64 || len(c.Buttons) > 32 {
		return fmt.Errorf("webui: declarative catalog exceeds its entry limit")
	}
	ids := make(map[string]string, len(c.Panels)+len(c.Columns)+len(c.Buttons))
	for i, panel := range c.Panels {
		if err := validateCatalogID(panel.ID, "panel", i); err != nil {
			return err
		}
		if err := validateCatalogText(panel.Title, "panel title", i); err != nil {
			return err
		}
		if err := validateCatalogID(panel.View, "panel view", i); err != nil {
			return err
		}
		if err := rememberCatalogID(ids, panel.ID, "panel"); err != nil {
			return err
		}
	}
	for i, column := range c.Columns {
		if err := validateCatalogID(column.ID, "column", i); err != nil {
			return err
		}
		if err := validateCatalogText(column.Title, "column title", i); err != nil {
			return err
		}
		if !catalogField.MatchString(column.Field) {
			return fmt.Errorf("webui: invalid column field at index %d", i)
		}
		if err := validateCatalogID(column.View, "column view", i); err != nil {
			return err
		}
		if err := rememberCatalogID(ids, column.ID, "column"); err != nil {
			return err
		}
	}
	for i, button := range c.Buttons {
		if err := validateCatalogID(button.ID, "button", i); err != nil {
			return err
		}
		if err := validateCatalogText(button.Label, "button label", i); err != nil {
			return err
		}
		if !validBridgeMethod(button.Method) {
			return fmt.Errorf("webui: invalid button method at index %d", i)
		}
		if bridgeMethodForbidden(button.Method) {
			return fmt.Errorf("webui: button method %q is unavailable to UI extensions", button.Method)
		}
		if len(button.Params) != 0 {
			var params map[string]json.RawMessage
			if err := json.Unmarshal(button.Params, &params); err != nil || params == nil {
				return fmt.Errorf("webui: button params at index %d must be a JSON object", i)
			}
			if err := rejectBridgeFields(params, "button.params"); err != nil {
				return err
			}
		}
		if err := rememberCatalogID(ids, button.ID, "button"); err != nil {
			return err
		}
	}
	return nil
}

func (c Catalog) clone() Catalog {
	clone := Catalog{
		Panels:  append([]Panel(nil), c.Panels...),
		Columns: append([]Column(nil), c.Columns...),
		Buttons: append([]Button(nil), c.Buttons...),
	}
	for i := range clone.Buttons {
		clone.Buttons[i].Params = append(json.RawMessage(nil), clone.Buttons[i].Params...)
	}
	return clone
}

func validateCatalogID(value, kind string, index int) error {
	if !catalogIdentifier.MatchString(value) {
		return fmt.Errorf("webui: invalid %s id at index %d", kind, index)
	}
	return nil
}

func validateCatalogText(value, kind string, index int) error {
	if value == "" || len(value) > 200 || strings.TrimSpace(value) != value || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return fmt.Errorf("webui: invalid %s at index %d", kind, index)
	}
	return nil
}

func rememberCatalogID(ids map[string]string, id, kind string) error {
	if previous, ok := ids[id]; ok {
		return fmt.Errorf("webui: catalog id %q is used by both %s and %s", id, previous, kind)
	}
	ids[id] = kind
	return nil
}
