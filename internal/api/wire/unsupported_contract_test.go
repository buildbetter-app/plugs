package wire_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
)

// This independent oracle records the allocation proposed by the root's C1
// author/reviewer. It does not allocate production values or choose which
// operational-close requests reach this refusal. No workflow operation runs.
const unsupportedOracleCode model.ErrorCode = "unsupported"
const unsupportedOracleInteger = -32057
const unsupportedOracleMessage = "operation is unsupported"
const unsupportedOracleRef = "#/components/errors/Unsupported"

func TestUnsupportedNewErrorAndMapError(t *testing.T) {
	c := unsupportedDefaultCatalog(t)
	const hint, trace = "typed refusal", "refusal-trace"
	large := model.Natural("9007199254740993")
	details := map[string]any{"source": "regression"}
	domain := &model.Error{Message: "private implementation failure", Data: model.ErrorData{
		Code: unsupportedOracleCode, Retryable: true, RetryAfterMs: &large, Hint: hint, TraceID: trace, Details: details,
	}}
	transport := &wire.Error{Code: -32603, Message: "private transport failure", Data: wire.ErrorData{
		Code: string(unsupportedOracleCode), Retryable: true, RetryAfterMS: &large, Hint: hint, TraceID: trace, Details: details,
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
			unsupportedRequireLiteralError(t, c, got)
			if got.Data.Hint != hint || got.Data.TraceID != trace || !reflect.DeepEqual(got.Data.Details, details) || got.Data.RetryAfterMS == nil || *got.Data.RetryAfterMS != large {
				t.Fatalf("mapped refusal lost legitimate typed metadata: %+v", got)
			}
			raw, err := json.Marshal(got.Data)
			if err != nil {
				t.Fatal(err)
			}
			var decoded model.ErrorData
			if err = json.Unmarshal(raw, &decoded); err != nil || decoded.Code != unsupportedOracleCode || decoded.Retryable || decoded.Hint != hint || decoded.TraceID != trace || decoded.RetryAfterMs == nil || *decoded.RetryAfterMs != large || !reflect.DeepEqual(decoded.Details, details) {
				t.Fatalf("mapped public refusal did not survive typed model decoding: %+v error=%v", decoded, err)
			}
		})
	}
	t.Run("new-error", func(t *testing.T) {
		got := c.NewError(string(unsupportedOracleCode), hint, details)
		unsupportedRequireLiteralError(t, c, got)
		if got.Data.Hint != hint || !reflect.DeepEqual(got.Data.Details, details) {
			t.Fatalf("NewError lost legitimate refusal metadata: %+v", got)
		}
	})
	t.Run("catalog-allocation", func(t *testing.T) {
		matches := 0
		for _, entry := range c.ErrorCatalog() {
			if entry.Code == string(unsupportedOracleCode) {
				matches++
				if entry.JSONRPC != unsupportedOracleInteger || entry.Retryable || entry.Audience != "client" || entry.WarningCapable {
					t.Errorf("noncanonical unsupported allocation: %+v", entry)
				}
			} else if entry.JSONRPC == unsupportedOracleInteger {
				t.Errorf("unsupported reuses allocation for %s", entry.Code)
			}
		}
		if matches != 1 {
			t.Fatalf("unsupported allocation count=%d want=1", matches)
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
			method := wire.MethodDesc{Result: json.RawMessage(`{"$ref":"#/components/schemas/` + tc.schema + `"}`)}
			if err := c.ValidateResult(method, []byte(tc.raw)); (err == nil) != tc.valid {
				t.Fatalf("schema=%s valid=%v error=%v", tc.schema, tc.valid, err)
			}
		})
	}
	t.Run("named-retryable-const", func(t *testing.T) {
		got := c.NewError(string(unsupportedOracleCode), "", nil)
		unsupportedRequireLiteralError(t, c, got)
		got.Data.Retryable = true
		if err := c.ValidateError(got); err == nil {
			t.Fatal("Unsupported component admitted retryable=true instead of const:false")
		}
	})
}

func TestUnsupportedTxApplyBoundDescriptorParity(t *testing.T) {
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	called := false
	if err = registry.Register("tx.apply", func(context.Context, rpc.Call) (any, error) {
		called = true
		return nil, fmt.Errorf("descriptor inspection must not execute tx.apply")
	}); err != nil {
		t.Fatal(err)
	}
	descriptors := registry.Descriptors()
	if len(descriptors) != 1 || descriptors[0].Name != "tx.apply" {
		t.Fatalf("actual bound descriptors=%+v", descriptors)
	}
	unsupportedRequireRef(t, descriptors[0].Errors)
	generated, err := registry.Catalog().GenerateOpenRPC(descriptors)
	if err != nil {
		t.Fatal(err)
	}
	server := unsupportedRPCServer(t, registry)
	runtimeDocument, err := server.OpenRPC(context.Background(), model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{"canonical", unsupportedRunfile(t, "engdocs/design/plug/contracts/openrpc.json")},
		{"embedded", unsupportedRunfile(t, "internal/api/wire/catalog.json")},
		{"bound-generator", generated},
		{"bound-runtime", runtimeDocument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			methods := unsupportedMethods(t, tc.raw)
			matches := 0
			for _, method := range methods {
				if method.Name != "tx.apply" {
					continue
				}
				matches++
				var refs []string
				for _, entry := range method.Errors {
					refs = append(refs, entry.Ref)
				}
				unsupportedRequireRef(t, refs)
				if !reflect.DeepEqual(refs, descriptors[0].Errors) {
					t.Errorf("error attachments differ from the actual bound descriptor: got=%v want=%v", refs, descriptors[0].Errors)
				}
			}
			if matches != 1 {
				t.Fatalf("tx.apply method count=%d want=1", matches)
			}
		})
	}
	if called {
		t.Fatal("descriptor inspection executed a transaction handler")
	}
}

func TestUnsupportedBootstrapRemainsFourMethods(t *testing.T) {
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	server := unsupportedRPCServer(t, registry)
	raw, err := server.OpenRPC(context.Background(), model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, method := range unsupportedMethods(t, raw) {
		names = append(names, method.Name)
	}
	if !reflect.DeepEqual(names, []string{"rpc.describe", "rpc.discover", "rpc.methods", "server.hello"}) {
		t.Fatalf("unbound transaction leaked into bootstrap discovery: %v", names)
	}
}

func TestUnsupportedUnknownCodesRemainSanitized(t *testing.T) {
	c := unsupportedDefaultCatalog(t)
	large := model.Natural("9007199254740993")
	for _, code := range []string{"unsupported_unknown", "UNSUPPORTED"} {
		if err := model.ErrorCode(code).Validate(); err == nil {
			t.Fatalf("unknown model code %s was admitted", code)
		}
	}
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"unknown-model", &model.Error{Message: "private", Data: model.ErrorData{
			Code: "unsupported_unknown", Retryable: true, RetryAfterMs: &large, Hint: "private", TraceID: "private", Details: map[string]any{"private": true},
		}}},
		{"unknown-wire", &wire.Error{Message: "private", Data: wire.ErrorData{
			Code: "unsupported_unknown", Retryable: true, RetryAfterMS: &large, Hint: "private", TraceID: "private", Details: map[string]any{"private": true},
		}}},
		{"arbitrary", fmt.Errorf("private implementation failure")},
		{"wrapped-arbitrary", fmt.Errorf("wrapped private failure: %w", fmt.Errorf("private underlying failure"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := c.MapError(tc.err)
			if got.Code != -32603 || got.Message != "internal error" || got.Data.Code != "internal_error" || got.Data.Retryable || got.Data.Hint != "" || got.Data.TraceID != "" || got.Data.RetryAfterMS != nil || len(got.Data.Details) != 0 {
				t.Fatalf("unknown error escaped sanitization: %+v", got)
			}
			if err := c.ValidateError(got); err != nil {
				t.Fatal(err)
			}
		})
	}
	if err := model.WarningCode(unsupportedOracleCode).Validate(); err == nil {
		t.Fatal("unsupported refusal became a warning")
	}
}

func unsupportedRequireLiteralError(t *testing.T, c *wire.Catalog, got *wire.Error) {
	t.Helper()
	if got.Code != unsupportedOracleInteger || got.Message != unsupportedOracleMessage || got.Data.Code != string(unsupportedOracleCode) || got.Data.Retryable {
		t.Fatalf("canonical unsupported error lost: got=%+v want=(%d,%q,unsupported,false)", got, unsupportedOracleInteger, unsupportedOracleMessage)
	}
	if err := c.ValidateError(got); err != nil {
		t.Fatal("literal public error fails the actual wire schema:", err)
	}
}

func unsupportedRequireRef(t *testing.T, refs []string) {
	t.Helper()
	matches := 0
	for _, ref := range refs {
		if ref == unsupportedOracleRef {
			matches++
		}
	}
	if matches != 1 {
		t.Errorf("actual tx.apply descriptor must advertise Unsupported exactly once; got %d in %v", matches, refs)
	}
}

func unsupportedDefaultCatalog(t *testing.T) *wire.Catalog {
	t.Helper()
	c, err := wire.DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func unsupportedRPCServer(t *testing.T, registry *rpc.Registry) *rpc.Server {
	t.Helper()
	server, err := rpc.New(registry, rpc.Config{Incarnation: "unsupported-contract", OntologyRevision: model.Natural64(1), CursorKey: bytes.Repeat([]byte{0x32}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func unsupportedRunfile(t *testing.T, relative string) []byte {
	t.Helper()
	path := filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), relative)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type unsupportedMethod struct {
	Name   string `json:"name"`
	Errors []struct {
		Ref string `json:"$ref"`
	} `json:"errors"`
}

func unsupportedMethods(t *testing.T, raw []byte) []unsupportedMethod {
	t.Helper()
	var doc struct {
		Methods []unsupportedMethod `json:"methods"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	return doc.Methods
}
