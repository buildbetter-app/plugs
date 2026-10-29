package driver

import (
	"errors"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

var (
	ErrNotFound           = errors.New("driver item not found")
	ErrTxClosed           = errors.New("driver transaction closed")
	ErrConflict           = errors.New("driver transaction conflict")
	ErrCheckpointConflict = errors.New("driver checkpoint generation conflict")
	ErrUnsupported        = errors.New("driver capability unsupported")
)

// ErrorMapping exposes only the closed catalog mapping and typed details;
// adapters own JSON-RPC encoding. It recognizes wrapped value and pointer
// driver errors. Unknown errors are never silently mapped to success.
func ErrorMapping(err error) (model.ErrorCode, map[string]any, bool) {
	var mapped interface {
		driverMapping() (model.ErrorCode, map[string]any)
	}
	if !errors.As(err, &mapped) {
		return "", nil, false
	}
	code, details := mapped.driverMapping()
	return code, details, true
}

func (e ErrFenced) driverMapping() (model.ErrorCode, map[string]any) {
	return "lease_not_held", map[string]any{"expectedEpoch": e.ExpectedEpoch, "observedEpoch": e.ObservedEpoch}
}

func (e ErrTruncated) driverMapping() (model.ErrorCode, map[string]any) {
	return "cursor_expired", map[string]any{"floor": e.Floor}
}

func (e ErrIncarnationChanged) driverMapping() (model.ErrorCode, map[string]any) {
	return "incarnation_changed", map[string]any{"fromIncarnation": e.From, "toIncarnation": e.To}
}

func (e ErrIndeterminate) driverMapping() (model.ErrorCode, map[string]any) {
	return "stale_snapshot", map[string]any{"idempotencyKey": e.IdempotencyKey, "outcome": "unknown"}
}
