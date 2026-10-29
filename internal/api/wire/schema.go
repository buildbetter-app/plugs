package wire

import (
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func (c *Catalog) resolve(ref string) (any, error) {
	if !strings.HasPrefix(ref, "#/") {
		return nil, fmt.Errorf("external schema reference %q", ref)
	}
	var value any = c.root
	for _, name := range strings.Split(ref[2:], "/") {
		name = strings.ReplaceAll(strings.ReplaceAll(name, "~1", "/"), "~0", "~")
		m, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("bad reference %s", ref)
		}
		value, ok = m[name]
		if !ok {
			return nil, fmt.Errorf("unknown reference %s", ref)
		}
	}
	return value, nil
}

// ValidateParams enforces the complete named-parameter schema, including
// nested closed objects, required members, unions and exact integer bounds.
func (c *Catalog) ValidateParams(d MethodDesc, raw []byte) error {
	v, err := DecodeJSON(raw)
	if err != nil {
		return err
	}
	m, ok := v.(map[string]any)
	if !ok {
		return fmt.Errorf("params must be an object")
	}
	allowed := map[string]Param{}
	for _, p := range d.Params {
		allowed[p.Name] = p
	}
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		p, ok := allowed[name]
		if !ok {
			return fmt.Errorf("unknown parameter %q", name)
		}
		s, err := DecodeJSON(p.Schema)
		if err != nil {
			return err
		}
		if err = c.validate(s, m[name], "params."+name, true, 0); err != nil {
			return err
		}
	}
	for _, p := range d.Params {
		if _, ok = m[p.Name]; p.Required && !ok {
			return fmt.Errorf("missing parameter %q", p.Name)
		}
	}
	return nil
}

func (c *Catalog) ValidateResult(d MethodDesc, raw []byte) error {
	v, err := DecodeJSON(raw)
	if err != nil {
		return err
	}
	s, err := DecodeJSON(d.Result)
	if err != nil {
		return err
	}
	return c.validate(s, v, "result", false, 0)
}

func (c *Catalog) validate(schema, value any, path string, request bool, depth int) error {
	if depth > 128 {
		return fmt.Errorf("%s: schema recursion limit", path)
	}
	s, ok := schema.(map[string]any)
	if !ok {
		return fmt.Errorf("%s: invalid schema", path)
	}
	if ref, ok := s["$ref"].(string); ok {
		resolved, err := c.resolve(ref)
		if err != nil {
			return err
		}
		return c.validate(resolved, value, path, request, depth+1)
	}
	if request && s["readOnly"] == true {
		return fmt.Errorf("%s: response-only member", path)
	}
	if types, ok := s["type"].([]any); ok {
		for _, typ := range types {
			variant := make(map[string]any, len(s))
			for k, v := range s {
				variant[k] = v
			}
			variant["type"] = typ
			if c.validate(variant, value, path, request, depth+1) == nil {
				return nil
			}
		}
		return fmt.Errorf("%s: expected an allowed type", path)
	}
	if variants, ok := s["oneOf"].([]any); ok {
		matches := 0
		for _, v := range variants {
			if c.validate(v, value, path, request, depth+1) == nil {
				matches++
			}
		}
		if matches != 1 {
			return fmt.Errorf("%s: expected exactly one union arm", path)
		}
	}
	if constant, exists := s["const"]; exists && !reflect.DeepEqual(value, constant) {
		return fmt.Errorf("%s: unexpected constant", path)
	}
	if enum, ok := s["enum"].([]any); ok {
		found := false
		for _, v := range enum {
			if reflect.DeepEqual(v, value) {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("%s: value is outside enum", path)
		}
	}
	t, _ := s["type"].(string)
	switch t {
	case "":
		return nil
	case "null":
		if value != nil {
			return fmt.Errorf("%s: expected null", path)
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("%s: expected boolean", path)
		}
	case "string":
		v, ok := value.(string)
		if !ok {
			return fmt.Errorf("%s: expected string", path)
		}
		if bound, ok := s["minLength"].(json.Number); ok {
			n, _ := bound.Int64()
			if int64(utf8.RuneCountInString(v)) < n {
				return fmt.Errorf("%s: string too short", path)
			}
		}
		if s["format"] == "date-time" {
			if _, err := time.Parse(time.RFC3339Nano, v); err != nil {
				return fmt.Errorf("%s: invalid date-time", path)
			}
		}
	case "number", "integer":
		v, ok := value.(json.Number)
		if !ok {
			return fmt.Errorf("%s: expected %s", path, t)
		}
		if t == "integer" {
			if _, err := integerCanonical(v); err != nil {
				return fmt.Errorf("%s: expected integer", path)
			}
			for _, bound := range []struct {
				key  string
				sign int
			}{{"minimum", -1}, {"maximum", 1}} {
				if n, ok := s[bound.key].(json.Number); ok {
					cmp, err := compareInteger(v, n)
					if err != nil {
						return err
					}
					if cmp == bound.sign {
						return fmt.Errorf("%s: violates %s", path, bound.key)
					}
				}
			}
		}
	case "array":
		v, ok := value.([]any)
		if !ok {
			return fmt.Errorf("%s: expected array", path)
		}
		if bound, ok := s["minItems"].(json.Number); ok {
			n, _ := bound.Int64()
			if int64(len(v)) < n {
				return fmt.Errorf("%s: too few items", path)
			}
		}
		if bound, ok := s["maxItems"].(json.Number); ok {
			n, _ := bound.Int64()
			if int64(len(v)) > n {
				return fmt.Errorf("%s: too many items", path)
			}
		}
		if item, ok := s["items"]; ok {
			for i, val := range v {
				if err := c.validate(item, val, fmt.Sprintf("%s[%d]", path, i), request, depth+1); err != nil {
					return err
				}
			}
		}
	case "object":
		v, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("%s: expected object", path)
		}
		properties, _ := s["properties"].(map[string]any)
		if required, ok := s["required"].([]any); ok {
			for _, name := range required {
				if _, ok := v[name.(string)]; !ok {
					return fmt.Errorf("%s: missing %s", path, name)
				}
			}
		}
		names := make([]string, 0, len(v))
		for name := range v {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if p, ok := properties[name]; ok {
				if err := c.validate(p, v[name], path+"."+name, request, depth+1); err != nil {
					return err
				}
				continue
			}
			if additional, exists := s["additionalProperties"]; exists {
				switch a := additional.(type) {
				case bool:
					if !a {
						return fmt.Errorf("%s: unknown member %q", path, name)
					}
				case map[string]any:
					if err := c.validate(a, v[name], path+"."+name, request, depth+1); err != nil {
						return err
					}
				}
			}
		}
	default:
		return fmt.Errorf("unsupported schema type %q", t)
	}
	return nil
}

// Integer comparison keeps huge exponents symbolic, matching model.Natural's
// unbounded wire domain without allocating a number proportional to its value.
func integerCanonical(n json.Number) (string, error) {
	v := string(n)
	negative := strings.HasPrefix(v, "-")
	v = strings.TrimPrefix(v, "-")
	c, err := model.Natural(v).Canonical()
	if err != nil {
		return "", err
	}
	if negative && c != "0" {
		c = "-" + c
	}
	return c, nil
}
func compareInteger(a, b json.Number) (int, error) {
	x, err := integerCanonical(a)
	if err != nil {
		return 0, err
	}
	y, err := integerCanonical(b)
	if err != nil {
		return 0, err
	}
	xn, yn := strings.HasPrefix(x, "-"), strings.HasPrefix(y, "-")
	if xn != yn {
		if xn {
			return -1, nil
		}
		return 1, nil
	}
	x = strings.TrimPrefix(x, "-")
	y = strings.TrimPrefix(y, "-")
	cmp := 0
	if x == "0" || y == "0" {
		if x != y {
			if x == "0" {
				cmp = -1
			} else {
				cmp = 1
			}
		}
	} else {
		parts := func(v string) (string, *big.Int) {
			digits, exp, has := strings.Cut(v, "e")
			e := new(big.Int)
			if has {
				e.SetString(exp, 10)
			}
			e.Add(e, big.NewInt(int64(len(digits))))
			return digits, e
		}
		xd, xe := parts(x)
		yd, ye := parts(y)
		cmp = xe.Cmp(ye)
		if cmp == 0 {
			n := max(len(xd), len(yd))
			cmp = strings.Compare(xd+strings.Repeat("0", n-len(xd)), yd+strings.Repeat("0", n-len(yd)))
		}
	}
	if xn {
		cmp = -cmp
	}
	return cmp, nil
}
