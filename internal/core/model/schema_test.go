package model

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"
)

// These component shapes are an immutable snapshot of the canonical OpenRPC
// catalog, not generated from the Go types under test. The source hash is in
// the fixture. The small validator below is test-only and checks this subset
// of JSON Schema without adding production dependencies.
//
//go:embed testdata/openrpc-components.json
var componentJSON []byte

func parseJSON(t *testing.T, raw []byte) any {
	t.Helper()
	var v any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func componentSchemas(t *testing.T) map[string]any {
	t.Helper()
	return parseJSON(t, componentJSON).(map[string]any)["schemas"].(map[string]any)
}

func schemaCheck(schemas map[string]any, schema map[string]any, value any) error {
	if ref, ok := schema["$ref"].(string); ok {
		name := strings.TrimPrefix(ref, "#/components/schemas/")
		target, ok := schemas[name].(map[string]any)
		if !ok {
			return fmt.Errorf("missing component %s", name)
		}
		return schemaCheck(schemas, target, value)
	}
	if choices, ok := schema["oneOf"].([]any); ok {
		matches := 0
		for _, choice := range choices {
			if schemaCheck(schemas, choice.(map[string]any), value) == nil {
				matches++
			}
		}
		if matches != 1 {
			return fmt.Errorf("oneOf matched %d arms", matches)
		}
	}
	if constant, ok := schema["const"]; ok && !reflect.DeepEqual(value, constant) {
		return fmt.Errorf("const mismatch")
	}
	if enum, ok := schema["enum"].([]any); ok {
		found := false
		for _, v := range enum {
			found = found || reflect.DeepEqual(v, value)
		}
		if !found {
			return fmt.Errorf("enum mismatch: %v", value)
		}
	}
	if types, ok := schema["type"].([]any); ok {
		for _, kind := range types {
			arm := make(map[string]any, len(schema))
			for key, v := range schema {
				arm[key] = v
			}
			arm["type"] = kind
			if schemaCheck(schemas, arm, value) == nil {
				return nil
			}
		}
		return fmt.Errorf("no allowed type matched")
	}
	switch schema["type"] {
	case "object":
		obj, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("expected object")
		}
		if required, ok := schema["required"].([]any); ok {
			for _, name := range required {
				if _, ok := obj[name.(string)]; !ok {
					return fmt.Errorf("missing %s", name)
				}
			}
		}
		props, _ := schema["properties"].(map[string]any)
		for name, v := range obj {
			property, ok := props[name].(map[string]any)
			if !ok {
				switch additional := schema["additionalProperties"].(type) {
				case bool:
					if !additional {
						return fmt.Errorf("unknown member %s", name)
					}
				case map[string]any:
					property = additional
				}
			}
			if property != nil {
				if err := schemaCheck(schemas, property, v); err != nil {
					return fmt.Errorf("%s: %w", name, err)
				}
			}
		}
	case "array":
		values, ok := value.([]any)
		if !ok {
			return fmt.Errorf("expected array")
		}
		if max, ok := schema["maxItems"].(json.Number); ok {
			bound, _ := max.Int64()
			if int64(len(values)) > bound {
				return fmt.Errorf("array limit")
			}
		}
		if item, ok := schema["items"].(map[string]any); ok {
			for i, v := range values {
				if err := schemaCheck(schemas, item, v); err != nil {
					return fmt.Errorf("element %d: %w", i, err)
				}
			}
		}
	case "string":
		text, ok := value.(string)
		if !ok {
			return fmt.Errorf("expected string")
		}
		if schema["format"] == "date-time" {
			if _, err := time.Parse(time.RFC3339Nano, text); err != nil {
				return err
			}
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected boolean")
		}
	case "null":
		if value != nil {
			return fmt.Errorf("expected null")
		}
	case "integer":
		number, ok := value.(json.Number)
		if !ok {
			return fmt.Errorf("expected integer")
		}
		rational, ok := new(big.Rat).SetString(number.String())
		if !ok || !rational.IsInt() {
			return fmt.Errorf("expected integral number")
		}
		if min, ok := schema["minimum"].(json.Number); ok {
			bound, _ := new(big.Rat).SetString(min.String())
			if rational.Cmp(bound) < 0 {
				return fmt.Errorf("below minimum")
			}
		}
		if max, ok := schema["maximum"].(json.Number); ok {
			bound, _ := new(big.Rat).SetString(max.String())
			if rational.Cmp(bound) > 0 {
				return fmt.Errorf("above maximum")
			}
		}
	}
	return nil
}

func checkComponent(t *testing.T, name string, raw []byte) error {
	t.Helper()
	schemas := componentSchemas(t)
	return schemaCheck(schemas, schemas[name].(map[string]any), parseJSON(t, raw))
}

// Both directions matter: Go must accept all schema-valid fixtures and must
// emit a schema-valid value, including normalization of integral decimals.
func roundtripComponent(t *testing.T, name, raw string, dst any) {
	t.Helper()
	if err := checkComponent(t, name, []byte(raw)); err != nil {
		t.Fatalf("invalid test fixture: %v", err)
	}
	if err := json.Unmarshal([]byte(raw), dst); err != nil {
		t.Fatalf("schema-valid %s rejected: %v\n%s", name, err, raw)
	}
	encoded, err := json.Marshal(dst)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkComponent(t, name, encoded); err != nil {
		t.Fatalf("Go emitted invalid %s: %v\n%s", name, err, encoded)
	}
}

func rejectComponent(t *testing.T, name, raw string, dst any) {
	t.Helper()
	if err := checkComponent(t, name, []byte(raw)); err == nil {
		t.Fatalf("invalid fixture unexpectedly fits schema: %s", raw)
	}
	if err := json.Unmarshal([]byte(raw), dst); err == nil {
		t.Fatalf("accepted invalid %s: %s", name, raw)
	}
}
