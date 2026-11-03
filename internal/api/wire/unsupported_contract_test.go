package wire

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// B08's strict refusal oracle needs a public typed error, independently of
// which operational-close combinations the controller eventually adopts.
// These tests allocate no JSON-RPC integer and execute no workflow transaction.
const unsupportedContractCode model.ErrorCode = "unsupported"
const unsupportedContractRef = "#/components/errors/Unsupported"

func TestUnsupportedModelErrorCodeRoundTrip(t *testing.T) {
	t.Run("validate", func(t *testing.T) {
		if err := unsupportedContractCode.Validate(); err != nil {
			t.Fatalf("B08 typed refusal code is not admitted: %v", err)
		}
	})
	t.Run("marshal", func(t *testing.T) {
		raw, err := json.Marshal(unsupportedContractCode)
		if err != nil || string(raw) != `"unsupported"` {
			t.Fatalf("typed refusal encoding=%s error=%v", raw, err)
		}
	})
	t.Run("unmarshal", func(t *testing.T) {
		var code model.ErrorCode
		if err := json.Unmarshal([]byte(`"unsupported"`), &code); err != nil || code != unsupportedContractCode {
			t.Fatalf("typed refusal decoding=%q error=%v", code, err)
		}
	})
	t.Run("error-data", func(t *testing.T) {
		want := model.ErrorData{Code: unsupportedContractCode, Retryable: false, Hint: "typed refusal", TraceID: "refusal-trace"}
		raw, err := json.Marshal(want)
		if err != nil {
			t.Fatal("typed refusal metadata cannot be encoded:", err)
		}
		var got model.ErrorData
		if err = json.Unmarshal(raw, &got); err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("typed refusal metadata=%+v error=%v", got, err)
		}
	})
}

func TestUnsupportedWireErrorDataSchemasRemainClosed(t *testing.T) {
	c := unsupportedDefaultCatalog(t)
	for _, tc := range []struct {
		name, schema, raw string
		valid             bool
	}{
		{"code", "ErrorCode", `"unsupported"`, true},
		{"error-data", "ErrorData", `{"code":"unsupported","retryable":false}`, true},
		{"unknown-code", "ErrorCode", `"unsupported_unknown"`, false},
		{"case-sensitive-code", "ErrorCode", `"UNSUPPORTED"`, false},
		{"closed-error-data", "ErrorData", `{"code":"unsupported","retryable":false,"extra":true}`, false},
		{"retryable-is-required", "ErrorData", `{"code":"unsupported"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			method := MethodDesc{Result: json.RawMessage(`{"$ref":"#/components/schemas/` + tc.schema + `"}`)}
			if err := c.ValidateResult(method, []byte(tc.raw)); (err == nil) != tc.valid {
				t.Fatalf("schema=%s valid=%v error=%v", tc.schema, tc.valid, err)
			}
		})
	}
}

func TestUnsupportedCanonicalAllocationParity(t *testing.T) {
	c := unsupportedDefaultCatalog(t)
	canonicalRaw := unsupportedCanonicalOpenRPC(t)
	generated, err := c.GenerateOpenRPC(c.Descriptors())
	if err != nil {
		t.Fatal(err)
	}
	canonical := unsupportedDecodeDocument(t, canonicalRaw)
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{"canonical", canonicalRaw},
		{"embedded", catalogJSON},
		{"generated", generated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc := unsupportedDecodeDocument(t, tc.raw)
			var allocation ErrorDesc
			matches := 0
			for _, entry := range doc.Errors {
				if entry.Code == string(unsupportedContractCode) {
					allocation = entry
					matches++
				}
			}
			if matches != 1 {
				t.Fatalf("unsupported must have exactly one canonical allocation; got %d", matches)
			}
			if allocation.Audience != "client" || allocation.Retryable || allocation.WarningCapable || allocation.JSONRPC >= 0 {
				t.Fatalf("unsupported must be a non-retryable client error, not a warning: %+v", allocation)
			}
			for _, entry := range doc.Errors {
				if entry.Code != allocation.Code && entry.JSONRPC == allocation.JSONRPC {
					t.Fatalf("unsupported reuses published allocation for %s", entry.Code)
				}
			}
			component, ok := doc.Components.Errors["Unsupported"]
			if !ok || component.Code != allocation.JSONRPC || component.Message == "" {
				t.Fatalf("named Unsupported component does not match allocation: %+v", component)
			}
			var data struct {
				Properties map[string]struct {
					Const string `json:"const"`
				} `json:"properties"`
				Required             []string `json:"required"`
				AdditionalProperties *bool    `json:"additionalProperties"`
			}
			if err := json.Unmarshal(component.Data, &data); err != nil || data.Properties["code"].Const != string(unsupportedContractCode) {
				t.Fatalf("named component does not pin data.code=unsupported: %s error=%v", component.Data, err)
			}
			if data.AdditionalProperties == nil || *data.AdditionalProperties || !reflect.DeepEqual(data.Required, []string{"code", "retryable"}) {
				t.Fatalf("named error data must retain the closed required envelope: %s", component.Data)
			}
			var codeSchema struct {
				Enum []string `json:"enum"`
			}
			if err := json.Unmarshal(doc.Components.Schemas["ErrorCode"], &codeSchema); err != nil {
				t.Fatal(err)
			}
			matches = 0
			for _, code := range codeSchema.Enum {
				if code == string(unsupportedContractCode) {
					matches++
				}
			}
			if matches != 1 {
				t.Fatalf("closed ErrorCode enum must admit unsupported exactly once; got %d", matches)
			}
			canonicalComponent := canonical.Components.Errors["Unsupported"]
			if component.Code != canonicalComponent.Code || component.Message != canonicalComponent.Message ||
				!reflect.DeepEqual(unsupportedContractJSON(t, component.Data), unsupportedContractJSON(t, canonicalComponent.Data)) ||
				!reflect.DeepEqual(unsupportedContractJSON(t, doc.Components.Schemas["ErrorCode"]), unsupportedContractJSON(t, canonical.Components.Schemas["ErrorCode"])) {
				t.Fatal("canonical, embedded and generated unsupported contracts differ")
			}
			var canonicalAllocation ErrorDesc
			for _, entry := range canonical.Errors {
				if entry.Code == allocation.Code {
					canonicalAllocation = entry
				}
			}
			if allocation != canonicalAllocation {
				t.Fatalf("allocation differs from canonical source: got=%+v want=%+v", allocation, canonicalAllocation)
			}
		})
	}
}

func TestUnsupportedNewErrorAndMapError(t *testing.T) {
	c := unsupportedDefaultCatalog(t)
	canonical := unsupportedDecodeDocument(t, unsupportedCanonicalOpenRPC(t)).Components.Errors["Unsupported"]
	const hint, trace = "typed refusal", "refusal-trace"
	details := map[string]any{"source": "regression"}
	domain := &model.Error{Message: "private implementation failure", Data: model.ErrorData{
		Code: unsupportedContractCode, Retryable: true, Hint: hint, TraceID: trace, Details: details,
	}}
	transport := &Error{Message: "private transport failure", Data: ErrorData{
		Code: string(unsupportedContractCode), Retryable: true, Hint: hint, TraceID: trace, Details: details,
	}}
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"domain", domain},
		{"wrapped-domain", fmt.Errorf("wrapped private domain failure: %w", domain)},
		{"wire", transport},
		{"wrapped-wire", fmt.Errorf("wrapped private transport failure: %w", transport)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := c.MapError(tc.err)
			if got.Data.Code != string(unsupportedContractCode) {
				t.Fatalf("B08 refusal was lost during MapError: got=%s want=unsupported", got.Data.Code)
			}
			if got.Data.Retryable || got.Data.Hint != hint || got.Data.TraceID != trace || !reflect.DeepEqual(got.Data.Details, details) {
				t.Fatalf("canonical retryability or typed refusal metadata changed: %+v", got)
			}
			if got.Code != canonical.Code || got.Message != canonical.Message || strings.Contains(got.Message, "private") {
				t.Fatalf("mapped refusal did not use the canonical allocation/message: %+v", got)
			}
			if err := c.ValidateError(got); err != nil {
				t.Fatal("mapped refusal fails the actual wire schema:", err)
			}
			raw, err := json.Marshal(got.Data)
			if err != nil {
				t.Fatal(err)
			}
			var decoded model.ErrorData
			if err = json.Unmarshal(raw, &decoded); err != nil || decoded.Code != unsupportedContractCode {
				t.Fatalf("public wire refusal cannot be read as typed model metadata: %+v error=%v", decoded, err)
			}
		})
	}
	t.Run("new-error", func(t *testing.T) {
		got := c.NewError(string(unsupportedContractCode), hint, details)
		if got.Data.Code != string(unsupportedContractCode) || got.Data.Retryable || got.Data.Hint != hint || !reflect.DeepEqual(got.Data.Details, details) {
			t.Fatalf("NewError does not preserve the typed public refusal: %+v", got)
		}
		if got.Code != canonical.Code || got.Message != canonical.Message {
			t.Fatalf("NewError differs from the independent canonical allocation: %+v", got)
		}
		if err := c.ValidateError(got); err != nil {
			t.Fatal(err)
		}
	})
}

func TestUnsupportedTxApplyErrorDescriptorParity(t *testing.T) {
	c := unsupportedDefaultCatalog(t)
	descriptor, ok := c.Descriptor("tx.apply")
	if !ok {
		t.Fatal("existing tx.apply descriptor missing")
	}
	generated, err := c.GenerateOpenRPC([]MethodDesc{descriptor})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{"canonical", unsupportedCanonicalOpenRPC(t)},
		{"embedded", catalogJSON},
		{"generated", generated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc := unsupportedDecodeDocument(t, tc.raw)
			matches := 0
			for _, method := range doc.Methods {
				if method.Name != "tx.apply" {
					continue
				}
				matches++
				var refs []string
				unsupportedRefs := 0
				for _, entry := range method.Errors {
					refs = append(refs, entry.Ref)
					if entry.Ref == unsupportedContractRef {
						unsupportedRefs++
					}
				}
				if unsupportedRefs != 1 {
					t.Errorf("tx.apply must advertise exactly one Unsupported error reference; got %d", unsupportedRefs)
				}
				if !reflect.DeepEqual(refs, descriptor.Errors) {
					t.Errorf("tx.apply error attachments differ from the actual descriptor: got=%v want=%v", refs, descriptor.Errors)
				}
			}
			if matches != 1 {
				t.Fatalf("tx.apply method count=%d want=1", matches)
			}
		})
	}
}

func TestUnsupportedUnknownCodesRemainSanitized(t *testing.T) {
	c := unsupportedDefaultCatalog(t)
	for _, code := range []string{"unsupported_unknown", "UNSUPPORTED"} {
		t.Run(code, func(t *testing.T) {
			if err := model.ErrorCode(code).Validate(); err == nil {
				t.Fatal("unsupported allocation opened the model error vocabulary")
			}
			raw, err := json.Marshal(code)
			if err != nil {
				t.Fatal(err)
			}
			var decoded model.ErrorCode
			if err = json.Unmarshal(raw, &decoded); err == nil {
				t.Fatal("unknown code decoded as a registered model error")
			}
			got := c.MapError(&model.Error{Data: model.ErrorData{
				Code: model.ErrorCode(code), Hint: "private", TraceID: "private", Details: map[string]any{"private": true},
			}})
			if got.Data.Code != "internal_error" || got.Data.Hint != "" || got.Data.TraceID != "" || len(got.Data.Details) != 0 {
				t.Fatalf("unknown model error escaped sanitization: %+v", got)
			}
			if err := c.ValidateError(got); err != nil {
				t.Fatal(err)
			}
		})
	}
	if err := model.WarningCode(unsupportedContractCode).Validate(); err == nil {
		t.Fatal("unsupported refusal became a warning")
	}
}

func unsupportedDefaultCatalog(t *testing.T) *Catalog {
	t.Helper()
	c, err := DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func unsupportedCanonicalOpenRPC(t *testing.T) []byte {
	t.Helper()
	path := filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), "engdocs/design/plug/contracts/openrpc.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type unsupportedContractDocument struct {
	Components struct {
		Schemas map[string]json.RawMessage `json:"schemas"`
		Errors  map[string]struct {
			Code    int             `json:"code"`
			Message string          `json:"message"`
			Data    json.RawMessage `json:"data"`
		} `json:"errors"`
	} `json:"components"`
	Errors  []ErrorDesc `json:"x-error-catalog"`
	Methods []struct {
		Name   string `json:"name"`
		Errors []struct {
			Ref string `json:"$ref"`
		} `json:"errors"`
	} `json:"methods"`
}

func unsupportedDecodeDocument(t *testing.T, raw []byte) unsupportedContractDocument {
	t.Helper()
	var doc unsupportedContractDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

func unsupportedContractJSON(t *testing.T, raw []byte) any {
	t.Helper()
	value, err := DecodeJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
