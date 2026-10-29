package driver

import (
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
		{&ErrTruncated{Floor: model.Natural("9007199254740993")}, "cursor_expired", map[string]any{"floor": model.Natural("9007199254740993")}},
		{ErrIncarnationChanged{From: "old", To: "new"}, "incarnation_changed", map[string]any{"fromIncarnation": "old", "toIncarnation": "new"}},
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
