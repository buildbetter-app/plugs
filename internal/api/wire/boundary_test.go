package wire

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp/syntax"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestStringSchemaPatternsFailClosed(t *testing.T) {
	c := &Catalog{}
	for _, tc := range []struct {
		name, pattern, value, errorText string
	}{
		{"typed ID matches", `^workflow:item/.`, "workflow:item/one", ""},
		{"ordinary ID fails", `^workflow:item/.`, "pl-one", "does not match pattern"},
		{"empty suffix fails", `^workflow:item/.`, "workflow:item/", "does not match pattern"},
		{"unanchored JSON Schema search", `item`, "workflow:item/one", ""},
		{"anchored prefix rejects embedded match", `^workflow:item/.`, "other/workflow:item/one", "does not match pattern"},
		{"empty pattern matches", ``, "anything", ""},
		{"invalid pattern fails closed", `[`, "workflow:item/one", "invalid schema pattern"},
	} {
		for _, request := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/request=%v", tc.name, request), func(t *testing.T) {
				schema, err := json.Marshal(map[string]any{"type": "string", "pattern": tc.pattern})
				if err != nil {
					t.Fatal(err)
				}
				method := MethodDesc{Params: []Param{{Name: "value", Required: true, Schema: schema}}, Result: schema}
				path := "result"
				if request {
					raw, _ := json.Marshal(map[string]any{"value": tc.value})
					err = c.ValidateParams(method, raw)
					path = "params.value"
				} else {
					raw, _ := json.Marshal(tc.value)
					err = c.ValidateResult(method, raw)
				}
				if tc.errorText == "" {
					if err != nil {
						t.Fatal(err)
					}
					return
				}
				if err == nil || !strings.Contains(err.Error(), tc.errorText) {
					t.Fatalf("pattern validation error=%v, want %q", err, tc.errorText)
				}
				var patternError *StringPatternError
				if !errors.As(err, &patternError) || patternError.Path != path {
					t.Fatalf("pattern validation error=%v, want typed error at %s", err, path)
				}
				var invalidPattern *syntax.Error
				if errors.As(err, &invalidPattern) != (tc.errorText == "invalid schema pattern") {
					t.Fatalf("pattern error cause=%v, invalid pattern=%v", patternError.Cause, invalidPattern)
				}
			})
		}
	}
}

func TestCanonicalErrorMapping(t *testing.T) {
	c, err := DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range c.ErrorCatalog() {
		e := c.MapError(&model.Error{Message: "private implementation details", Data: model.ErrorData{Code: model.ErrorCode(d.Code), Retryable: !d.Retryable}})
		if d.Audience == "internal" || d.Audience == "plugin-host" || oneOf(d.Code, "subscriber_skipped", "post_subscriber_failed", "ontology_drifted", "visibility_lost") {
			if e.Data.Code != "internal_error" {
				t.Errorf("leaked %s", d.Code)
			}
			continue
		}
		if e.Code != d.JSONRPC || e.Data.Retryable != d.Retryable || strings.Contains(e.Message, "private") {
			t.Errorf("noncanonical %s: %+v", d.Code, e)
		}
	}
	for _, err := range []error{fmt.Errorf("database secret"), &model.Error{Data: model.ErrorData{Code: "made_up"}}} {
		e := c.MapError(err)
		if e.Data.Code != "internal_error" || strings.Contains(e.Error(), "secret") {
			t.Fatalf("leaked unknown failure: %v", e)
		}
	}
	e := c.NewError("lease_not_held", "", map[string]any{"expectedEpoch": model.Natural("1e1000000000"), "observedEpoch": model.Natural64(2)})
	if err = c.ValidateError(e); err != nil {
		t.Fatal(err)
	}
	e.Data.Details = map[string]any{"expectedEpoch": -1, "observedEpoch": 2}
	if err = c.ValidateError(e); err == nil {
		t.Fatal("accepted invalid typed details")
	}
}

func TestIdempotencyKeyEncodings(t *testing.T) {
	for _, key := range []string{"550e8400-e29b-41d4-a716-446655440000", "01JB7W3K9QX4T2M8V6RZ0YDC5H", "3fc5d89aba21452eafee4ab934e08632", "RBPn_NJiNEbovU2zokqvcA"} {
		if err := ValidateIdempotencyKey(key); err != nil {
			t.Errorf("%s: %v", key, err)
		}
	}
	for _, key := range []string{"", "w7-1", "sixteencharacters", "550e8400-e29b-11d4-a716-446655440000", "01JB7W3K9QX4T2M8V6RZ0YDC5I"} {
		if err := ValidateIdempotencyKey(key); err == nil {
			t.Errorf("accepted %q", key)
		}
	}
}

func TestJSONBoundary(t *testing.T) {
	for _, raw := range [][]byte{[]byte(`{"a":1,"a":2}`), []byte(`{} {}`), []byte{34, 255, 34}, []byte(strings.Repeat("[", 130) + "0" + strings.Repeat("]", 130))} {
		if _, err := DecodeJSON(raw); err == nil {
			t.Fatalf("accepted invalid boundary: %q", raw)
		}
	}
	v, err := DecodeJSON([]byte(`{"n":9007199254740993}`))
	if err != nil || v.(map[string]any)["n"] != json.Number("9007199254740993") {
		t.Fatal("number precision lost")
	}
}
