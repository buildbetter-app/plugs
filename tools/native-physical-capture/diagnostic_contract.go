package main

import (
	"strings"

	"github.com/steveyegge/beads/internal/plugdump"
)

// Source-history refusals are diagnostic codes, not publication boundaries.
// The typed reason's optional source identifier suffix never leaves this map.
func safeSourceHistoryReason(reason string) string {
	prefix, _, _ := strings.Cut(reason, ":")
	switch prefix {
	case "child-counter-parent-unresolved":
		return "child_counter_parent_unresolved"
	case "ancestry-unavailable":
		return "ancestry_unavailable"
	case "commit-ancestry-cycle":
		return "commit_ancestry_cycle"
	case "commit-ancestry-missing":
		return "commit_ancestry_missing"
	case "commit-census-limit-or-empty":
		return "commit_census_limit_or_empty"
	case "commit-census-unavailable":
		return "commit_census_unavailable"
	case "contradictory-boundary-cursor":
		return "contradictory_boundary_cursor"
	case "contradictory-physical-ancestry":
		return "contradictory_physical_ancestry"
	case "contradictory-physical-census":
		return "contradictory_physical_census"
	case "contradictory-presence-probe":
		return "contradictory_presence_probe"
	case "duplicate-commit":
		return "duplicate_commit"
	case "duplicate-historical-table":
		return "duplicate_historical_table"
	case "head-image-differs-versioned-family":
		return "head_image_differs_versioned_family"
	case "head-image-missing-versioned-family":
		return "head_image_missing_versioned_family"
	case "head-unavailable":
		return "head_unavailable"
	case "historical-image-unavailable":
		return "historical_image_unavailable"
	case "historical-schema-unavailable":
		return "historical_schema_unavailable"
	case "historical-schema-unmappable":
		return "historical_schema_unmappable"
	case "image-census-limit":
		return "image_census_limit"
	case "invalid-ancestry":
		return "invalid_ancestry"
	case "invalid-commit":
		return "invalid_commit"
	case "invalid-commit-census":
		return "invalid_commit_census"
	case "invalid-head":
		return "invalid_head"
	case "invalid-image-request":
		return "invalid_image_request"
	case "invalid-native-history-boundary":
		return "invalid_native_history_boundary"
	case "missing-scoped-reader":
		return "missing_scoped_reader"
	case "native-head-image-unavailable":
		return "native_head_image_unavailable"
	case "native-history-boundary":
		return "native_history_boundary"
	case "native-history-head-unavailable":
		return "native_history_head_unavailable"
	case "native-history-reader-unavailable":
		return "native_history_reader_unavailable"
	case "native-image-census-limit":
		return "native_image_census_limit"
	case "native-physical-root-capability-unavailable":
		return "native_physical_root_capability_unavailable"
	case "native-portable-commit-census-mismatch":
		return "native_portable_commit_census_mismatch"
	case "native-table-limit":
		return "native_table_limit"
	case "presence-boundary-crosses-present-branch":
		return "presence_boundary_crosses_present_branch"
	case "row-limit":
		return "row_limit"
	case "source-census-changed":
		return "source_census_changed"
	case "table-size-limit":
		return "table_size_limit"
	case "unauthenticated-history":
		return "unauthenticated_history"
	case "unobserved-parent":
		return "unobserved_parent"
	case "unreachable-commit-census":
		return "unreachable_commit_census"
	case "working-set-not-clean":
		return "working_set_not_clean"
	case "working-set-unavailable":
		return "working_set_unavailable"
	}
	return ""
}

// Diagnostic names are public source constants, never arbitrary reflected
// strings or driver-supplied names. An unknown class is counted, not printed.
// The list includes our wrappers, the pinned physical driver's detached error
// classes, and standard/SQL error classes used by the capture path. It is not
// an assertion that every dependency error class can be enumerated.
func safeGoErrorType(name string) string {
	switch name {
	case "*main.captureFailure", "*main.unsupportedCaptureSchema",
		"*plugdump.NativeCommitFailure",
		"plugdump.ErrSourceHistoryUnavailable", "*plugdump.ErrSourceHistoryUnavailable",
		"plugdump.ErrUnsupportedNativeCommitSchema", "*plugdump.ErrUnsupportedNativeCommitSchema",
		"*embedded.PhysicalReadError", "*embedded.PhysicalHistoryUnavailable",
		"*embedded.PhysicalStoreInconsistent", "*embedded.physicalErrorEvidence",
		"*embedded.physicalCleanupFailure", "*embedded.storedChunkAbsent",
		"*embedded.physicalRowDecodePanic", "*chunks.MissingChunkError",
		"*mysql.MySQLError", "*mysql.SQLError", "*errors.Error",
		"*errors.errorString", "*errors.joinError", "*fmt.wrapError", "*fmt.wrapErrors",
		"*fs.PathError", "*os.LinkError", "*os.SyscallError", "syscall.Errno",
		"context.deadlineExceededError", "runtime.errorString", "runtime.boundsError",
		"*runtime.TypeAssertionError", "*strconv.NumError",
		"*json.SyntaxError", "*json.UnmarshalTypeError", "*json.InvalidUnmarshalError",
		"*json.UnsupportedTypeError", "*json.UnsupportedValueError", "*json.MarshalerError":
		return name
	}
	return ""
}

func safeStage(stage captureStage) captureStage {
	switch stage {
	case stageOpen, stageSchema, stageCensus, stageHistory, stageCell, stageReport:
		return stage
	}
	return stageReport
}

func safeSchemaObservation(observed schemaObservation) schemaObservation {
	// The expected version is always the public registry, not source input.
	result := schemaObservation{ExpectedVersion: plugdump.CaptureSchemaVersion(), RowCountClass: "unmeasured"}
	switch observed.RowCountClass {
	case "empty", "single", "series":
		if observed.FoundVersion != nil {
			result.RowCountClass, result.FoundVersion = observed.RowCountClass, observed.FoundVersion
		}
	}
	return result
}
