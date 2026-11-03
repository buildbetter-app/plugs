package rehearsal

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/buildbetter-app/plugs/internal/api/wire"
)

// TranscriptExchange is one canonical JSON-RPC call and its correlated reply.
// Supply sanitized public fixtures only; this tool never records live traffic.
type TranscriptExchange struct {
	Request  json.RawMessage `json:"request"`
	Response json.RawMessage `json:"response"`
}

// ComparePublicTranscripts validates an offline, read-only transcript profile.
// Each side must contain 1..256 ordered single-call exchanges, at most 1 MiB
// per frame and 8 MiB per stream. The wire catalog owns the effect classifier
// and schemas. Writes, authn, restore, plan effects, notifications and batches
// are outside this profile, including identical or dry-run mutating calls.
//
// Equality ignores object member order and whitespace only. It retains every
// request/result/error member, array order, absent/null distinction and exact
// numeric token; no IDs, revisions, warnings or errors are normalized away.
// Diagnostics contain only side, ordinal and fixed reasons, never payloads.
// This is a validation primitive, not staging, caller-adoption or rollback
// qualification. Runtime bundle installation retains its separate validator.
func ComparePublicTranscripts(reference, shadow []TranscriptExchange) error {
	if len(reference) == 0 || len(reference) != len(shadow) || len(reference) > 256 {
		return fmt.Errorf("nonempty equal-length transcripts of at most 256 exchanges required")
	}
	for _, stream := range [][]TranscriptExchange{reference, shadow} {
		total := 0
		for _, exchange := range stream {
			for _, frame := range []json.RawMessage{exchange.Request, exchange.Response} {
				if len(frame) == 0 || len(frame) > 1<<20 {
					return fmt.Errorf("transcript frame missing or exceeds 1 MiB")
				}
				total += len(frame)
			}
		}
		if total > 8<<20 {
			return fmt.Errorf("transcript stream exceeds 8 MiB")
		}
	}
	catalog, err := wire.DefaultCatalog()
	if err != nil {
		return fmt.Errorf("transcript wire catalog unavailable")
	}
	for i := range reference {
		a, err := validatePublicExchange(catalog, reference[i])
		if err != nil {
			return fmt.Errorf("reference transcript step %d: %w", i+1, err)
		}
		b, err := validatePublicExchange(catalog, shadow[i])
		if err != nil {
			return fmt.Errorf("shadow transcript step %d: %w", i+1, err)
		}
		if !reflect.DeepEqual(a.request, b.request) {
			return fmt.Errorf("transcript step %d: request differs", i+1)
		}
		if !reflect.DeepEqual(a.response, b.response) {
			return fmt.Errorf("transcript step %d: response differs", i+1)
		}
	}
	return nil
}

type publicExchange struct{ request, response map[string]any }

func validatePublicExchange(catalog *wire.Catalog, exchange TranscriptExchange) (publicExchange, error) {
	var out publicExchange
	request, err := wire.DecodeJSON(exchange.Request)
	if err != nil {
		return out, fmt.Errorf("invalid request JSON")
	}
	out.request, _ = request.(map[string]any)
	if out.request == nil || out.request["jsonrpc"] != "2.0" {
		return out, fmt.Errorf("single JSON-RPC 2.0 request required")
	}
	for key := range out.request {
		if key != "jsonrpc" && key != "id" && key != "method" && key != "params" {
			return out, fmt.Errorf("unknown request member")
		}
	}
	id, hasID := out.request["id"]
	if !hasID {
		return out, fmt.Errorf("request ID required")
	}
	switch id.(type) {
	case string, json.Number, nil:
	default:
		return out, fmt.Errorf("invalid request ID")
	}
	method, _ := out.request["method"].(string)
	desc, known := catalog.Descriptor(method)
	if !known || desc.Effect != "read" || desc.Idempotency != "none" {
		return out, fmt.Errorf("catalog read-only call required")
	}
	params := []byte(`{}`)
	if value, exists := out.request["params"]; exists {
		params, _ = json.Marshal(value)
	}
	if catalog.ValidateParams(desc, params) != nil {
		return out, fmt.Errorf("invalid request parameters")
	}
	response, err := wire.DecodeJSON(exchange.Response)
	if err != nil {
		return out, fmt.Errorf("invalid response JSON")
	}
	out.response, _ = response.(map[string]any)
	responseID, hasResponseID := out.response["id"]
	if out.response == nil || out.response["jsonrpc"] != "2.0" || !hasResponseID || !reflect.DeepEqual(id, responseID) {
		return out, fmt.Errorf("response version or ID mismatch")
	}
	for key := range out.response {
		if key != "jsonrpc" && key != "id" && key != "result" && key != "error" {
			return out, fmt.Errorf("unknown read response member")
		}
	}
	result, hasResult := out.response["result"]
	errorValue, hasError := out.response["error"]
	if hasResult == hasError {
		return out, fmt.Errorf("exactly one result or error required")
	}
	if hasResult {
		raw, _ := json.Marshal(result)
		if catalog.ValidateResult(desc, raw) != nil {
			return out, fmt.Errorf("invalid response result")
		}
	} else {
		errorObject, ok := errorValue.(map[string]any)
		if !ok || len(errorObject) != 3 {
			return out, fmt.Errorf("invalid typed error envelope")
		}
		if _, ok := errorObject["message"].(string); !ok {
			return out, fmt.Errorf("invalid typed error message")
		}
		// Validate the original data too: decoding optional nulls into Go
		// zero values must not hide schema violations from ValidateError.
		data, _ := json.Marshal(errorObject["data"])
		dataSchema := wire.MethodDesc{Result: json.RawMessage(`{"$ref":"#/components/schemas/ErrorData"}`)}
		if catalog.ValidateResult(dataSchema, data) != nil {
			return out, fmt.Errorf("invalid typed error data")
		}
		raw, _ := json.Marshal(errorValue)
		var remote wire.Error
		if wire.DecodeInto(raw, &remote) != nil {
			return out, fmt.Errorf("invalid typed error")
		}
		canonical := catalog.NewError(remote.Data.Code, "", nil)
		if canonical.Data.Code != remote.Data.Code || canonical.Code != remote.Code || canonical.Data.Retryable != remote.Data.Retryable || catalog.ValidateError(&remote) != nil {
			return out, fmt.Errorf("error violates wire catalog")
		}
	}
	return out, nil
}
