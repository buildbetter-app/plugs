package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"reflect"

	"github.com/go-sql-driver/mysql"
	"github.com/steveyegge/beads/internal/plugdump"
	"github.com/steveyegge/beads/internal/storage/physicalcapture"
)

type captureStage string

const (
	stageOpen    captureStage = "open"
	stageSchema  captureStage = "schema-probe"
	stageCensus  captureStage = "census"
	stageHistory captureStage = "history-walk"
	stageCell    captureStage = "cell-read"
)

type schemaObservation struct {
	FoundVersion    *uint64 `json:"found_version,omitempty"`
	ExpectedVersion uint64  `json:"expected_version"`
	RowCountClass   string  `json:"row_count_class"`
}

type captureFailure struct {
	stage         captureStage
	table, column string
	schema        schemaObservation
	cause         error
}

func (*captureFailure) Error() string   { return "native capture failed" }
func (e *captureFailure) Unwrap() error { return e.cause }

type unsupportedCaptureSchema struct{}

func (*unsupportedCaptureSchema) Error() string { return "capture schema unsupported" }

// Only fixed schema identifiers are selected, before inventory/history reads.
// Missing tables are errors, never synthesized version zero or empty data.
func probeSchema(ctx context.Context, conn *sql.Conn) (schemaObservation, error) {
	observation := schemaObservation{ExpectedVersion: plugdump.CaptureSchemaVersion(), RowCountClass: "unmeasured"}
	var count, version uint64
	err := conn.QueryRowContext(ctx, "SELECT COUNT(*), COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&count, &version)
	if err != nil {
		return observation, err
	}
	observation.FoundVersion = &version
	switch count {
	case 0:
		observation.RowCountClass = "empty"
	case 1:
		observation.RowCountClass = "single"
	default:
		observation.RowCountClass = "series"
	}
	return observation, nil
}

type safeCaptureReport struct {
	InnermostGoErrorType  string            `json:"innermost_go_error_type,omitempty"`
	CommitSchemaVersion   *uint64           `json:"commit_schema_version,omitempty"`
	CommitOrdinalFromHead *uint64           `json:"commit_ordinal_from_head,omitempty"`
	Format                string            `json:"format"`
	Stage                 captureStage      `json:"stage"`
	GoErrorTypes          []string          `json:"go_error_types,omitempty"`
	SQLCodes              []uint16          `json:"sql_dolt_codes,omitempty"`
	Schema                schemaObservation `json:"schema_migrations"`
	Table                 string            `json:"table,omitempty"`
	Column                string            `json:"column,omitempty"`
}

// Never call Error, format a value, parse a message or serialize an error.
// Unwrap only to retain Go type names and the driver's typed numeric SQL code.
func safeReport(err error) safeCaptureReport {
	report := safeCaptureReport{Format: "beads-capture-diagnostic/1", Stage: stageOpen,
		Schema: schemaObservation{ExpectedVersion: plugdump.CaptureSchemaVersion(), RowCountClass: "unmeasured"}}
	var failure *captureFailure
	if errors.As(err, &failure) {
		report.Stage, report.Schema = failure.stage, failure.schema
		report.Table, report.Column = plugdump.PublicCaptureIdentifiers(failure.table, failure.column)
	}
	if stage, table := physicalcapture.FailureLocation(err); stage != "" {
		report.Stage = captureStage(stage)
		report.Table, report.Column = plugdump.PublicCaptureIdentifiers(table, "")
	}
	var commitFailure *plugdump.NativeCommitFailure
	if errors.As(err, &commitFailure) {
		report.CommitSchemaVersion = commitFailure.SchemaVersion
		if commitFailure.OrdinalFromHead != 0 {
			ordinal := commitFailure.OrdinalFromHead
			report.CommitOrdinalFromHead = &ordinal
		}
	}
	deepestLeaf := -1
	seenTypes, seenCodes := map[string]bool{}, map[uint16]bool{}
	var walk func(error, int)
	walk = func(cause error, depth int) {
		if cause == nil || depth > 32 {
			return
		}
		name := reflect.TypeOf(cause).String()
		if !seenTypes[name] {
			report.GoErrorTypes = append(report.GoErrorTypes, name)
			seenTypes[name] = true
		}
		if sqlError, ok := cause.(*mysql.MySQLError); ok && !seenCodes[sqlError.Number] {
			report.SQLCodes = append(report.SQLCodes, sqlError.Number)
			seenCodes[sqlError.Number] = true
		}
		hasChild := false
		switch wrapped := cause.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range wrapped.Unwrap() {
				if child != nil {
					hasChild = true
					walk(child, depth+1)
				}
			}
		case interface{ Unwrap() error }:
			if child := wrapped.Unwrap(); child != nil {
				hasChild = true
				walk(child, depth+1)
			}
		}
		if !hasChild && depth > deepestLeaf {
			deepestLeaf = depth
			report.InnermostGoErrorType = name
		}
	}
	walk(err, 0)
	return report
}

func writeSafeReport(output io.Writer, err error) {
	// Encoding this closed report has no data-dependent marshaler. If the
	// destination is unavailable, never fall back to printing the original error.
	_ = json.NewEncoder(output).Encode(safeReport(err))
}

// Stage current SQL reads without parsing error text. NativeInventory performs
// the same reads again in the existing transaction; the preflight localizes a
// failed census or cell read before its higher-level validation runs.
func preflightCurrent(ctx context.Context, source plugdump.SQLSource, observation schemaObservation) error {
	names, err := source.Tables(ctx)
	if err != nil {
		return &captureFailure{stage: stageCensus, schema: observation, cause: err}
	}
	for _, name := range names {
		allowed, _ := plugdump.PublicCaptureIdentifiers(name, "")
		if allowed == "" {
			return &captureFailure{stage: stageCensus, schema: observation, cause: errors.New("unregistered source family")}
		}
		if _, err := source.ReadTable(ctx, name); err != nil {
			return &captureFailure{stage: stageCell, table: name, schema: observation, cause: err}
		}
	}
	return nil
}

type observedNativeSource struct {
	plugdump.OwnVersionCommitSource
	schema schemaObservation
}

func (s observedNativeSource) Tables(ctx context.Context) ([]string, error) {
	names, err := s.SQLSource.Tables(ctx)
	return names, s.failure(stageCensus, "", err)
}

func (s observedNativeSource) ReadTable(ctx context.Context, name string) (plugdump.Table, error) {
	table, err := s.SQLSource.ReadTable(ctx, name)
	return table, s.failure(stageCell, name, err)
}

func (s observedNativeSource) ReadSourceCommits(ctx context.Context) (plugdump.SourceCommitLog, error) {
	log, err := s.SQLSource.ReadSourceCommits(ctx)
	return log, s.failure(stageHistory, "", err)
}

func (s observedNativeSource) ReadSourceIgnore(ctx context.Context) (plugdump.Table, error) {
	table, err := s.SQLSource.ReadSourceIgnore(ctx)
	return table, s.failure(stageCensus, "dolt_ignore", err)
}

func (s observedNativeSource) ReadNativeSourceImage(ctx context.Context, id string) (map[string]plugdump.Table, error) {
	image, err := s.OwnVersionCommitSource.ReadNativeSourceImage(ctx, id)
	return image, s.failure(stageHistory, "", err)
}

func (s observedNativeSource) failure(stage captureStage, table string, err error) error {
	if err == nil {
		return nil
	}
	return &captureFailure{stage: stage, table: table, schema: s.schema, cause: err}
}
