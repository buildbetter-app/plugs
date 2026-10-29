package driver

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestDriverErrorMapping(t *testing.T) {
	tests := []struct {
		err     error
		code    model.ErrorCode
		details map[string]any
	}{
		{ErrFenced{ExpectedEpoch: 2, ObservedEpoch: 3}, "lease_not_held", map[string]any{"expectedEpoch": uint64(2), "observedEpoch": uint64(3)}},
		{&ErrFenced{ExpectedEpoch: 2, ObservedEpoch: 3}, "lease_not_held", map[string]any{"expectedEpoch": uint64(2), "observedEpoch": uint64(3)}},
		{ErrTruncated{Floor: model.Natural("9007199254740993")}, "cursor_expired", map[string]any{"floor": model.Natural("9007199254740993")}},
		{&ErrTruncated{Floor: model.Natural("9007199254740993")}, "cursor_expired", map[string]any{"floor": model.Natural("9007199254740993")}},
		{ErrIncarnationChanged{From: "old", To: "new"}, "incarnation_changed", map[string]any{"fromIncarnation": "old", "toIncarnation": "new"}},
		{&ErrIncarnationChanged{From: "old", To: "new"}, "incarnation_changed", map[string]any{"fromIncarnation": "old", "toIncarnation": "new"}},
		{ErrIndeterminate{IdempotencyKey: "request-1"}, "stale_snapshot", map[string]any{"idempotencyKey": "request-1", "outcome": "unknown"}},
		{&ErrIndeterminate{IdempotencyKey: "request-1"}, "stale_snapshot", map[string]any{"idempotencyKey": "request-1", "outcome": "unknown"}},
	}
	for _, tt := range tests {
		t.Run(string(tt.code), func(t *testing.T) {
			code, details, ok := ErrorMapping(fmt.Errorf("storage: %w", tt.err))
			if !ok || code != tt.code || !reflect.DeepEqual(details, tt.details) {
				t.Fatalf("mapping = (%s, %#v, %t)", code, details, ok)
			}
		})
	}
	for _, err := range []error{nil, errors.New("engine failure"), ErrCheckpointConflict} {
		if code, details, ok := ErrorMapping(err); ok || code != "" || details != nil {
			t.Fatalf("unknown error mapped: (%s, %#v, %t)", code, details, ok)
		}
	}
}

func TestTruncatedMappingKeepsLosslessRevision(t *testing.T) {
	floor := model.Natural("18446744073709551617")
	wrapped := errors.Join(errors.New("connection reset"), fmt.Errorf("read history: %w", &ErrTruncated{Floor: floor}))
	code, details, ok := ErrorMapping(wrapped)
	if !ok || code != "cursor_expired" {
		t.Fatalf("joined driver error not mapped: %q, %t", code, ok)
	}
	encoded, err := json.Marshal(details)
	if err != nil || string(encoded) != `{"floor":18446744073709551617}` {
		t.Fatalf("floor narrowed at wire boundary: %s, %v", encoded, err)
	}
	var typed *ErrTruncated
	if !errors.As(wrapped, &typed) || typed.Floor != floor {
		t.Fatal("original typed error was not preserved")
	}
}
