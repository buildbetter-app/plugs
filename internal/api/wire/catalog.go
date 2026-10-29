// Package wire owns the JSON-RPC contract, schema validation and error catalog.
package wire

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
)

// catalogJSON is the bounded contract snapshot, including the independently
// maintained descriptor inventory. Runtime discovery is generated from the Go
// descriptors of bound handlers, never from this snapshot's method list.
//
//go:embed catalog.json
var catalogJSON []byte

type Param struct {
	Name     string          `json:"name"`
	Required bool            `json:"required"`
	Schema   json.RawMessage `json:"schema"`
}

// MethodDesc is complete for every callable method. Error references and schemas
// use local OpenRPC component pointers from the bounded inventory.
type MethodDesc struct {
	Name        string          `json:"name"`
	Effect      string          `json:"effect"`
	Idempotency string          `json:"idempotency"`
	Permission  string          `json:"permission"`
	Stability   string          `json:"stability"`
	AgentCore   bool            `json:"agentCore"`
	Params      []Param         `json:"params"`
	Result      json.RawMessage `json:"resultSchema"`
	Errors      []string        `json:"errors"`
}

type ErrorDesc struct {
	Code           string `json:"code"`
	JSONRPC        int    `json:"jsonRpc"`
	Retryable      bool   `json:"retryable"`
	Audience       string `json:"audience"`
	WarningCapable bool   `json:"warningCapable"`
}

type Catalog struct {
	version    string
	info       json.RawMessage
	components json.RawMessage
	methods    map[string]MethodDesc
	sources    map[string]json.RawMessage
	errors     map[string]ErrorDesc
	messages   map[string]string
	errorRefs  map[string]string
	errorOrder []string
	root       map[string]any
}

func DefaultCatalog() (*Catalog, error) { return ParseCatalog(catalogJSON) }

// ParseCatalog checks descriptor parity against the independently stored
// OpenRPC methods and pinned inventory digests before admitting any handlers.
func ParseCatalog(data []byte) (*Catalog, error) {
	var doc struct {
		OpenRPC    string            `json:"openrpc"`
		Info       json.RawMessage   `json:"info"`
		Components json.RawMessage   `json:"components"`
		Methods    []json.RawMessage `json:"methods"`
		Errors     []ErrorDesc       `json:"x-error-catalog"`
		Registry   struct {
			Count          int             `json:"methodCount"`
			NameHash       string          `json:"nameSha256"`
			DescriptorHash string          `json:"descriptorSha256"`
			Methods        json.RawMessage `json:"methods"`
		} `json:"x-method-registry"`
	}
	decoded, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	root, ok := decoded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("catalog must be object")
	}
	c := &Catalog{version: doc.OpenRPC, info: doc.Info, components: doc.Components, methods: map[string]MethodDesc{}, sources: map[string]json.RawMessage{}, errors: map[string]ErrorDesc{}, messages: map[string]string{}, errorRefs: map[string]string{}, root: root}
	var entries []MethodDesc
	if err = json.Unmarshal(doc.Registry.Methods, &entries); err != nil {
		return nil, err
	}
	if len(entries) != doc.Registry.Count || len(doc.Methods) != len(entries) {
		return nil, fmt.Errorf("catalog method count mismatch")
	}
	var compact bytes.Buffer
	if err = json.Compact(&compact, doc.Registry.Methods); err != nil {
		return nil, err
	}
	if digest(compact.Bytes()) != doc.Registry.DescriptorHash {
		return nil, fmt.Errorf("catalog descriptor digest mismatch")
	}
	var names bytes.Buffer
	var rawEntries []map[string]json.RawMessage
	if err = json.Unmarshal(doc.Registry.Methods, &rawEntries); err != nil {
		return nil, err
	}
	for i, d := range entries {
		for _, key := range []string{"name", "effect", "idempotency", "permission", "stability", "agentCore", "params", "resultSchema", "errors"} {
			if _, present := rawEntries[i][key]; !present {
				return nil, fmt.Errorf("descriptor %s missing %s", d.Name, key)
			}
		}
		if len(rawEntries[i]) != 9 {
			return nil, fmt.Errorf("unknown descriptor member: %s", d.Name)
		}
		if _, exists := c.methods[d.Name]; exists {
			return nil, fmt.Errorf("duplicate method %q", d.Name)
		}
		if err = d.Validate(); err != nil {
			return nil, err
		}
		c.methods[d.Name] = d
		names.WriteString(d.Name + "\n")
	}
	if digest(names.Bytes()) != doc.Registry.NameHash {
		return nil, fmt.Errorf("catalog name digest mismatch")
	}
	for _, raw := range doc.Methods {
		var m struct {
			Name   string  `json:"name"`
			Params []Param `json:"params"`
			Result struct {
				Schema json.RawMessage `json:"schema"`
			} `json:"result"`
			Errors []struct {
				Ref string `json:"$ref"`
			} `json:"errors"`
			Effect      string `json:"x-effect"`
			Idempotency string `json:"x-idempotency"`
			Permission  string `json:"x-permission"`
			Stability   string `json:"x-stability"`
			AgentCore   *bool  `json:"x-agent-core"`
		}
		if err = json.Unmarshal(raw, &m); err != nil {
			return nil, err
		}
		if _, exists := c.sources[m.Name]; exists {
			return nil, fmt.Errorf("duplicate OpenRPC method %q", m.Name)
		}
		d, exists := c.methods[m.Name]
		if !exists || m.AgentCore == nil {
			return nil, fmt.Errorf("missing descriptor %q", m.Name)
		}
		e := MethodDesc{Name: m.Name, Effect: m.Effect, Idempotency: m.Idempotency, Permission: m.Permission, Stability: m.Stability, AgentCore: *m.AgentCore, Params: m.Params, Result: m.Result.Schema}
		e.Errors = make([]string, 0, len(m.Errors))
		for _, item := range m.Errors {
			e.Errors = append(e.Errors, item.Ref)
		}
		a, _ := json.Marshal(d)
		b, _ := json.Marshal(e)
		va, _ := DecodeJSON(a)
		vb, _ := DecodeJSON(b)
		if !reflect.DeepEqual(va, vb) {
			return nil, fmt.Errorf("descriptor parity mismatch: %s", m.Name)
		}
		c.sources[m.Name] = raw
	}
	var components struct {
		Errors map[string]struct {
			Code    int            `json:"code"`
			Message string         `json:"message"`
			Data    map[string]any `json:"data"`
		} `json:"errors"`
	}
	if err = json.Unmarshal(c.components, &components); err != nil {
		return nil, err
	}
	integers := map[int]bool{}
	for _, d := range doc.Errors {
		if _, exists := c.errors[d.Code]; exists || integers[d.JSONRPC] {
			return nil, fmt.Errorf("duplicate error allocation")
		}
		c.errors[d.Code] = d
		c.errorOrder = append(c.errorOrder, d.Code)
		integers[d.JSONRPC] = true
	}
	for name, e := range components.Errors {
		properties, _ := e.Data["properties"].(map[string]any)
		codeSchema, _ := properties["code"].(map[string]any)
		code, _ := codeSchema["const"].(string)
		d, exists := c.errors[code]
		if !exists || d.JSONRPC != e.Code {
			return nil, fmt.Errorf("error allocation mismatch %q", code)
		}
		c.messages[code] = e.Message
		c.errorRefs[code] = "#/components/errors/" + name + "/data"
	}
	if len(c.messages) != len(c.errors) {
		return nil, fmt.Errorf("missing error components")
	}
	for _, d := range entries {
		for _, ref := range d.Errors {
			if _, err = c.resolve(ref); err != nil {
				return nil, err
			}
		}
	}
	if err := c.checkReferences(root); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Catalog) checkReferences(value any) error {
	switch v := value.(type) {
	case map[string]any:
		if r, ok := v["$ref"]; ok {
			ref, ok := r.(string)
			if !ok {
				return fmt.Errorf("non-string reference")
			}
			if _, err := c.resolve(ref); err != nil {
				return err
			}
		}
		for _, child := range v {
			if err := c.checkReferences(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range v {
			if err := c.checkReferences(child); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Catalog) ComponentsJSON() json.RawMessage {
	return append(json.RawMessage(nil), c.components...)
}

func (d MethodDesc) Validate() error {
	if d.Name == "" || d.Stability == "" || len(d.Result) == 0 || d.Params == nil {
		return fmt.Errorf("incomplete descriptor %q", d.Name)
	}
	if !oneOf(d.Effect, "read", "write", "plan", "restore", "event", "authn") || !oneOf(d.Idempotency, "none", "required") || !oneOf(d.Permission, "none", "read", "write", "approve", "transition", "plan", "admin") {
		return fmt.Errorf("invalid descriptor %q", d.Name)
	}
	if d.Effect != "event" && len(d.Errors) == 0 {
		return fmt.Errorf("missing error catalog for %q", d.Name)
	}
	if oneOf(d.Effect, "write", "restore") && d.Idempotency != "required" {
		return fmt.Errorf("write must require idempotency: %s", d.Name)
	}
	seen := map[string]bool{}
	for _, p := range d.Params {
		if p.Name == "" || seen[p.Name] || len(p.Schema) == 0 {
			return fmt.Errorf("invalid parameter in %s", d.Name)
		}
		seen[p.Name] = true
	}
	seen = map[string]bool{}
	for _, ref := range d.Errors {
		if seen[ref] {
			return fmt.Errorf("duplicate error in %s", d.Name)
		}
		seen[ref] = true
	}
	return nil
}

func oneOf(v string, values ...string) bool {
	for _, s := range values {
		if v == s {
			return true
		}
	}
	return false
}
func digest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

// Descriptor returns an independent copy so callers cannot alter the registry.
func (c *Catalog) Descriptor(name string) (MethodDesc, bool) {
	d, ok := c.methods[name]
	if !ok {
		return d, false
	}
	raw, _ := json.Marshal(d)
	var copy MethodDesc
	_ = json.Unmarshal(raw, &copy)
	return copy, true
}
func (c *Catalog) Descriptors() []MethodDesc {
	names := make([]string, 0, len(c.methods))
	for n := range c.methods {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]MethodDesc, 0, len(names))
	for _, n := range names {
		d, _ := c.Descriptor(n)
		out = append(out, d)
	}
	return out
}
func (c *Catalog) ErrorCatalog() []ErrorDesc {
	out := make([]ErrorDesc, 0, len(c.errors))
	for _, code := range c.errorOrder {
		out = append(out, c.errors[code])
	}
	return out
}

// OpenRPC generates a document from exactly the provided Go descriptors.
func (c *Catalog) GenerateOpenRPC(descriptors []MethodDesc) ([]byte, error) {
	methods := make([]map[string]any, 0, len(descriptors))
	sorted := append([]MethodDesc(nil), descriptors...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	seen := map[string]bool{}
	for _, d := range sorted {
		if err := d.Validate(); err != nil {
			return nil, err
		}
		if seen[d.Name] {
			return nil, fmt.Errorf("duplicate generated method %s", d.Name)
		}
		seen[d.Name] = true
		m := map[string]any{}
		if raw, ok := c.sources[d.Name]; ok {
			_ = json.Unmarshal(raw, &m)
		}
		m["name"] = d.Name
		m["paramStructure"] = "by-name"
		m["params"] = d.Params
		m["result"] = map[string]any{"name": "result", "schema": d.Result}
		errors := make([]map[string]string, 0, len(d.Errors))
		for _, ref := range d.Errors {
			errors = append(errors, map[string]string{"$ref": ref})
		}
		m["errors"] = errors
		m["x-effect"] = d.Effect
		m["x-idempotency"] = d.Idempotency
		m["x-permission"] = d.Permission
		m["x-stability"] = d.Stability
		m["x-agent-core"] = d.AgentCore
		methods = append(methods, m)
	}
	doc := map[string]any{"openrpc": c.version, "info": c.info, "methods": methods, "components": c.components, "x-error-catalog": c.ErrorCatalog()}
	out, err := json.MarshalIndent(doc, "", "  ")
	return append(out, '\n'), err
}
