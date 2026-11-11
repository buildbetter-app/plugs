package plugdump

import "strings"

// CaptureSchemaVersion is the latest main cursor in the public schema registry.
// Own-version support is separately checked by CaptureSchemaSupported and the
// full paired migration/schema profile validation.
func CaptureSchemaVersion() uint64 { return uint64(len(migrationHashes["main"])) }

// PublicCaptureIdentifiers releases only identifiers in the public, closed
// Beads schema registry. Unknown names are omitted, never echoed or hashed.
func PublicCaptureIdentifiers(table, column string) (string, string) {
	columns, known := tableColumns[table]
	if !known {
		// These system columns are also queried by the capture-only reader.
		columns, known = map[string]string{
			"dolt_ignore":          "pattern ignored",
			"dolt_nonlocal_tables": "table_name target_ref ref_table options",
			"dolt_log":             "commit_hash committer email date message",
			"dolt_status":          "table_name staged status",
		}[table]
	}
	if !known {
		return "", ""
	}
	for _, allowed := range strings.Fields(columns) {
		if column == allowed {
			return table, column
		}
	}
	return table, ""
}
