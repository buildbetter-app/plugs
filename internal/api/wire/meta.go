package wire

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// Meta contains client assertions only; effective identity comes from the
// transport context. Replayed is a server response member and is not accepted.
type Meta struct {
	Project        string        `json:"project,omitempty"`
	IdempotencyKey string        `json:"idempotencyKey,omitempty"`
	DeadlineMS     model.Natural `json:"deadlineMs,omitempty"`
	Traceparent    string        `json:"traceparent,omitempty"`
	DryRun         bool          `json:"dryRun,omitempty"`
	Return         string        `json:"return,omitempty"`
	Agent          string        `json:"agent,omitempty"`
}

var uuid4 = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
var ulid = regexp.MustCompile(`(?i)^[0-7][0-9A-HJKMNP-TV-Z]{25}$`)

// ValidateIdempotencyKey verifies an encoding large enough for caller-generated
// randomness. Randomness itself is the caller's obligation. UUIDv4 and ULID
// are explicitly accepted by the contract; hex and base64url encode >=16 bytes.
func ValidateIdempotencyKey(key string) error {
	if uuid4.MatchString(key) || ulid.MatchString(key) {
		return nil
	}
	if b, err := hex.DecodeString(key); err == nil && len(b) >= 16 {
		return nil
	}
	if !strings.ContainsAny(key, "=+/") {
		if b, err := base64.RawURLEncoding.DecodeString(key); err == nil && len(b) >= 16 {
			return nil
		}
	}
	return fmt.Errorf("idempotencyKey must encode at least 128 bits (UUIDv4, ULID, hex or base64url)")
}
