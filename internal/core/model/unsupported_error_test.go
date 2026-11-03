package model

import (
	"encoding/json"
	"testing"
)

func TestUnsupportedErrorDataPreservesTypedMetadata(t *testing.T) {
	const raw = `{"code":"unsupported","retryable":false,"retryAfterMs":9007199254740993,"hint":"raw durable close cannot target workflow items","traceId":"unsupported-close","details":{"operation":"close"}}`
	retryAfter := Natural("9007199254740993")
	want := ErrorData{
		Code:         "unsupported",
		Retryable:    false,
		RetryAfterMs: &retryAfter,
		Hint:         "raw durable close cannot target workflow items",
		TraceID:      "unsupported-close",
		Details:      map[string]any{"operation": "close"},
	}
	t.Run("marshal", func(t *testing.T) {
		encoded, err := json.Marshal(want)
		if err != nil {
			t.Fatalf("cannot encode the required typed unsupported refusal: %v", err)
		}
		if string(encoded) != raw {
			t.Fatalf("typed refusal wire bytes = %s, want %s", encoded, raw)
		}
	})
	t.Run("unmarshal", func(t *testing.T) {
		var decoded ErrorData
		if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
			t.Fatalf("cannot decode the required typed unsupported refusal: %v", err)
		}
		if decoded.Code != want.Code || decoded.Retryable != want.Retryable || decoded.RetryAfterMs == nil || *decoded.RetryAfterMs != retryAfter || decoded.Hint != want.Hint || decoded.TraceID != want.TraceID || len(decoded.Details) != 1 || decoded.Details["operation"] != "close" {
			t.Fatalf("typed refusal metadata changed: %+v", decoded)
		}
	})
}
