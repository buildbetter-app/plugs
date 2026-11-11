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
		stage = "archive-read"
	case "schema", "unsupported-schema":
		stage = "schema"
	case "table", "census", "census-inconsistent":
		stage = "table-scan"
	case "rows", "inconsistent-store":
		stage = "row-decode"
	default:
		stage = "history-walk"
	}
	return stage, physical.Table
}

// FailureAtCommit compares a locally observed immutable identity without
// exposing that identity through the count-only diagnostic output.
func FailureAtCommit(err error, id string) bool {
	var physical *embedded.PhysicalReadError
	return errors.As(err, &physical) && physical.CommitID == id
}
