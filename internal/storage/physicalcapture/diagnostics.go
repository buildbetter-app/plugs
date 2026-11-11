package physicalcapture

import (
	"errors"

	embedded "github.com/dolthub/driver/v2"
)

// FailureLocation exposes only a fixed stage and an identifier for the caller
// to check against its public schema allowlist. Driver error text and commit
// identity never cross this diagnostic boundary. The original error is retained.
func FailureLocation(err error) (stage, table string) {
	var physical *embedded.PhysicalReadError
	if !errors.As(err, &physical) {
		return "", ""
	}
	switch physical.Kind {
	case "database", "permission", "transaction", "rollback", "occupied-transaction", "unsupported-driver":
		stage = "open"
	case "schema", "unsupported-schema":
		stage = "schema-probe"
	case "table", "census", "census-inconsistent":
		stage = "census"
	case "rows":
		stage = "cell-read"
	default:
		stage = "history-walk"
	}
	return stage, physical.Table
}
