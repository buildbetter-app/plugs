package wire

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

type ErrorData struct {
	Code         string         `json:"code"`
	Retryable    bool           `json:"retryable"`
	RetryAfterMS *model.Natural `json:"retryAfterMs,omitempty"`
	Hint         string         `json:"hint,omitempty"`
	TraceID      string         `json:"traceId,omitempty"`
	Details      map[string]any `json:"details,omitempty"`
}

type Error struct {
	Code    int       `json:"code"`
	Message string    `json:"message"`
	Data    ErrorData `json:"data"`
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Data.Code, e.Message) }

func (c *Catalog) ValidateError(e *Error) error {
	ref, ok := c.errorRefs[e.Data.Code]
	if !ok {
		return fmt.Errorf("unknown error code")
	}
	schema, err := c.resolve(ref)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(e.Data)
	if err != nil {
		return err
	}
	value, err := DecodeJSON(raw)
	if err != nil {
		return err
	}
	return c.validate(schema, value, "error.data", false, 0)
}

// NewError uses the closed allocation, including canonical message and retry
// metadata. Host/internal and warning-only codes can never become client errors.
func (c *Catalog) NewError(code, hint string, details map[string]any) *Error {
	d, ok := c.errors[code]
	if !ok || d.Audience == "internal" || d.Audience == "plugin-host" || oneOf(code, "subscriber_skipped", "post_subscriber_failed", "ontology_drifted", "visibility_lost") {
		code = "internal_error"
		d = c.errors[code]
		hint = ""
		details = nil
	}
	return &Error{Code: d.JSONRPC, Message: c.messages[code], Data: ErrorData{Code: code, Retryable: d.Retryable, Hint: hint, Details: details}}
}

// MapError admits typed domain errors and sanitizes arbitrary Go failures.
func (c *Catalog) MapError(err error) *Error {
	var w *Error
	if errors.As(err, &w) {
		out := c.NewError(w.Data.Code, w.Data.Hint, w.Data.Details)
		if out.Data.Code == w.Data.Code {
			out.Data.RetryAfterMS = w.Data.RetryAfterMS
			out.Data.TraceID = w.Data.TraceID
		}
		return out
	}
	var domain *model.Error
	if errors.As(err, &domain) {
		out := c.NewError(string(domain.Data.Code), domain.Data.Hint, domain.Data.Details)
		if out.Data.Code == string(domain.Data.Code) {
			out.Data.RetryAfterMS = domain.Data.RetryAfterMs
			out.Data.TraceID = domain.Data.TraceID
			if domain.NotRetryable {
				out.Data.Retryable = false
			}
		}
		return out
	}
	return c.NewError("internal_error", "", nil)
}
