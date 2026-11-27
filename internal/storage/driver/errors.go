package driver

import (
	"errors"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

var (
	ErrNotFound           = errors.New("driver item not found")
	ErrTxClosed           = errors.New("driver transaction closed")
	ErrConflict           = errors.New("driver transaction conflict")
	ErrCheckpointConflict = errors.New("driver checkpoint generation conflict")
	ErrUnsupported        = errors.New("driver capability unsupported")
)

// ReverseJSONLimitError reports which offline JSON resource exhausted its
// budget before decoding. Decoded bytes are an estimated allocation charge.
type ReverseJSONLimitError struct {
	Resource     string  `json:"resource"`
	Measured     uint64  `json:"measured"`
	Limit        uint64  `json:"limit"`
	Member       string  `json:"member,omitempty"`
	JSONPath     string  `json:"json_path,omitempty"`
	ElementIndex *uint64 `json:"element_index,omitempty"`
}

func (e *ReverseJSONLimitError) Code() string { return "reverse_json_limit" }

func (e *ReverseJSONLimitError) Error() string {
	if e.ElementIndex != nil {
		return fmt.Sprintf("%s: %s measured %d, limit %d at %s %s index %d", e.Code(), e.Resource, e.Measured, e.Limit, e.Member, e.JSONPath, *e.ElementIndex)
	}
	return fmt.Sprintf("%s: %s measured %d, limit %d", e.Code(), e.Resource, e.Measured, e.Limit)
}

// LocateReverseJSONLimit attaches a static or redacted archive member and
// structural path to an admission failure. Index is zero-based in that path.
func LocateReverseJSONLimit(err error, member, path string, index uint64) error {
	var limit *ReverseJSONLimitError
	if !errors.As(err, &limit) || limit.ElementIndex != nil {
		return err
	}
	located := *limit
	located.Member, located.JSONPath, located.ElementIndex = member, path, &index
	return &located
}

// ErrRestoreBindingMismatch identifies an import receipt/profile field that
// disagrees with the durable publication. It remains a driver conflict for
// callers that only understand ErrConflict, while restore boundaries can
// retain the exact semantic cause.
type ErrRestoreBindingMismatch struct {
	Field string
}

func (e ErrRestoreBindingMismatch) Error() string {
	if e.Field == "" {
		return "driver transaction conflict: restore import binding does not match the durable receipt"
	}
	return fmt.Sprintf("driver transaction conflict: restore import binding field %s does not match the durable receipt", e.Field)
}

func (e ErrRestoreBindingMismatch) Unwrap() error { return ErrConflict }

// ErrRestoreCommitConflict identifies a storage publication race after the
// import binding has been admitted. It is retryable at the restore API
// boundary, but it does not add a retry or change the storage transaction.
type ErrRestoreCommitConflict struct {
	Cause error
}

func (e ErrRestoreCommitConflict) Error() string {
	if e.Cause == nil {
		return "restore import commit conflicted with concurrent store state"
	}
	return fmt.Sprintf("restore import commit conflicted with concurrent store state: %v", e.Cause)
}

func (e ErrRestoreCommitConflict) Unwrap() []error {
	if e.Cause == nil {
		return []error{ErrConflict}
	}
	return []error{ErrConflict, e.Cause}
}

// Indeterminate finds a publication outcome even when it is joined with a
// conflict, a refusal or a deadline. Callers must handle it before those errors.
func Indeterminate(err error) (ErrIndeterminate, bool) {
	var outcome interface{ indeterminate() ErrIndeterminate }
	if errors.As(err, &outcome) {
		return outcome.indeterminate(), true
	}
	return ErrIndeterminate{}, false
}

func (e ErrIndeterminate) indeterminate() ErrIndeterminate { return e }

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
