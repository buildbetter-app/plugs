package viewops

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestMappedContextErrorsPreserveIndependentCauses(t *testing.T) {
	unexpected := errors.New("independent query failure")
	typed := &model.Error{Data: model.ErrorData{Code: "internal_error"}}
	for _, other := range []error{unexpected, typed, driver.ErrTruncated{Floor: model.Natural64(2)}} {
		for _, request := range []error{context.Canceled, context.DeadlineExceeded} {
			for _, cause := range []error{
				errors.Join(other, request),
				errors.Join(request, other),
				fmt.Errorf("query: %w", errors.Join(other, request)),
				errors.Join(fmt.Errorf("storage: %w", other), errors.Join(request, request)),
			} {
				got := mapped(cause)
				if got != cause || !errors.Is(got, other) || !errors.Is(got, request) {
					t.Fatalf("mapping erased an independent error: got=%v original=%v", got, cause)
				}
			}
		}
	}
	mixedContexts := fmt.Errorf("query: %w", errors.Join(context.Canceled, context.DeadlineExceeded))
	if mapped(mixedContexts) != mixedContexts {
		t.Fatal("distinct context causes were collapsed")
	}
	for _, request := range []error{context.Canceled, context.DeadlineExceeded} {
		for _, cause := range []error{request, fmt.Errorf("query: %w", request), errors.Join(request), errors.Join(request, fmt.Errorf("read: %w", request))} {
			var got *model.Error
			if !errors.As(mapped(cause), &got) || got.Data.Code != "busy" || !got.Data.Retryable || got.Data.Hint != "view read was canceled or exceeded its deadline" {
				t.Fatalf("single-cause context mapping changed: %v", mapped(cause))
			}
		}
	}
	if mapped(nil) != nil || mapped(unexpected) != unexpected || mapped(typed) != typed {
		t.Fatal("unrelated error identity changed")
	}
	var truncated *model.Error
	if !errors.As(mapped(driver.ErrTruncated{Floor: model.Natural64(2)}), &truncated) || truncated.Data.Code != "cursor_expired" {
		t.Fatal("ordinary typed driver mapping changed")
	}
}

// Match the production SQL normalizer's single-cause error contract without
// replacing its network cause with a context sentinel.
type mappedDeadlineFailure struct{ cause error }

func (e mappedDeadlineFailure) Error() string        { return e.cause.Error() }
func (e mappedDeadlineFailure) Unwrap() error        { return e.cause }
func (e mappedDeadlineFailure) Is(target error) bool { return target == context.DeadlineExceeded }

func TestMappedNormalizedNetworkDeadlinePreservesIndependentCauses(t *testing.T) {
	timeout := &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}
	if !timeout.Timeout() || errors.Is(timeout, context.DeadlineExceeded) {
		t.Fatal("control must be a real network timeout without context classification")
	}
	normalized := mappedDeadlineFailure{cause: timeout}
	if !errors.Is(normalized, timeout) || !errors.Is(normalized, context.DeadlineExceeded) {
		t.Fatal("normalizer must retain its network cause and add deadline classification")
	}
	for _, cause := range []error{
		normalized,
		fmt.Errorf("query: %w", normalized),
		errors.Join(normalized, context.DeadlineExceeded),
		fmt.Errorf("query: %w", errors.Join(normalized, fmt.Errorf("read: %w", normalized))),
		mappedDeadlineFailure{cause: errors.Join(context.DeadlineExceeded, normalized)},
	} {
		var got *model.Error
		if !errors.As(mapped(cause), &got) || got.Data.Code != "busy" || !got.Data.Retryable || got.Data.Hint != "view read was canceled or exceeded its deadline" {
			t.Fatalf("normalized single-cause deadline mapping changed: %v", mapped(cause))
		}
	}
	unexpected := errors.New("independent query failure")
	for _, other := range []error{unexpected, context.Canceled} {
		for _, joined := range []error{
			errors.Join(normalized, other),
			errors.Join(other, normalized),
			errors.Join(normalized, errors.Join(other, context.DeadlineExceeded)),
		} {
			for _, cause := range []error{
				joined,
				fmt.Errorf("query: %w", joined),
				mappedDeadlineFailure{cause: joined},
				fmt.Errorf("query: %w", mappedDeadlineFailure{cause: joined}),
			} {
				got := mapped(cause)
				if got != cause || !errors.Is(got, other) || !errors.Is(got, timeout) || !errors.Is(got, context.DeadlineExceeded) {
					t.Fatalf("normalizer erased another cause: got=%v original=%v", got, cause)
				}
			}
		}
	}
	if mapped(timeout) != timeout || mapped(nil) != nil {
		t.Fatal("raw timeout or nil identity changed")
	}
}
