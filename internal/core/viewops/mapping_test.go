package viewops

import (
	"context"
	"errors"
	"fmt"
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
