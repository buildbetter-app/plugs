package pipeline

import (
	"encoding/hex"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// Match the canonical workflow item identity used by the service without
// consulting the operational provider. All such targets are outside the raw
// durable Close plane, whether or not the operational item currently exists.
func canonicalWorkflowCloseTarget(ref model.Ref) bool {
	if ref.Validate() != nil || !ref.IsID() || !ref.Branch.IsZero() {
		return false
	}
	suffix, ok := strings.CutPrefix(ref.ID, "workflow:item/")
	if !ok {
		return false
	}
	raw, err := hex.DecodeString(suffix)
	return err == nil && len(raw) == 16 && hex.EncodeToString(raw) == suffix
}
