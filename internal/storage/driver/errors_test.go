package driver

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestReverseJSONByteAndNestingLimits(t *testing.T) {
	const depth = NativeHeadJSONDepth
	nested := func(n int) []byte {
		return []byte(strings.Repeat("[", n) + `"\\\" [] {} : ,"` + strings.Repeat("]", n))
	}
	if !json.Valid(nested(depth)) {
		t.Fatal("malformed nesting fixture")
	}
	if err := CheckReverseJSONShape(nested(depth)); err != nil {
		t.Fatalf("legal nesting refused: %v", err)
	}
	var limit *ReverseJSONLimitError
	if err := CheckReverseJSONShape(nested(depth + 1)); !errors.As(err, &limit) ||
		limit.Code() != "reverse_json_limit" || limit.Resource != "nesting_depth" || limit.Measured != depth+1 || limit.Limit != depth {
		t.Fatalf("nesting limit lost typed details: %v", err)
	}
	wire := []byte(`"` + strings.Repeat("x", NativeHeadRecordBytes-1) + `"`)
	if err := CheckReverseJSONShape(wire); !errors.As(err, &limit) ||
		limit.Resource != "value_bytes" || limit.Measured != NativeHeadRecordBytes+1 || limit.Limit != NativeHeadRecordBytes {
		t.Fatalf("byte limit lost typed details: %v", err)
	}
	wire[len(wire)-2] = '"'
	if err := CheckReverseJSONShape(wire[:len(wire)-1]); err != nil {
		t.Fatalf("exact wire boundary failed resource admission: %v", err)
	}
}

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
