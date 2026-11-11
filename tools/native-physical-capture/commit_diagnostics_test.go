package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/steveyegge/beads/internal/plugdump"
)

type unfamiliarCommitDecodeError struct{ private string }

func (*unfamiliarCommitDecodeError) Error() string { panic("never read a message") }

type commitErrorWrapper struct{ cause error }

func (*commitErrorWrapper) Error() string   { panic("never format a wrapper") }
func (e *commitErrorWrapper) Unwrap() error { return e.cause }

func TestSafePerCommitTaxonomyCountsUnfamiliarTypesAndKeepsNumericContext(t *testing.T) {
	schema := uint64(39)
	cause := &plugdump.NativeCommitFailure{SchemaVersion: &schema, OrdinalFromHead: 7,
		Cause: errors.Join(&commitErrorWrapper{cause: &unfamiliarCommitDecodeError{private: "private-cell-id-and-hash"}}, &mysql.MySQLError{Number: 1105, Message: "private SQL message"})}
	var output bytes.Buffer
	writeSafeReport(&output, &captureFailure{stage: stageHistory, cause: cause})
	for _, private := range []string{"private-cell", "private SQL", "private-cell-id-and-hash"} {
		if bytes.Contains(output.Bytes(), []byte(private)) {
			t.Fatal("private error content leaked")
		}
	}
	var report safeCaptureReport
	if err := json.Unmarshal(output.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.InnermostGoErrorType != "" || report.UnrecognizedGoErrorTypeCount != 2 || report.CommitSchemaVersion == nil || *report.CommitSchemaVersion != 39 || report.CommitOrdinalFromHead == nil || *report.CommitOrdinalFromHead != 7 || !reflect.DeepEqual(report.SQLCodes, []uint16{1105}) {
		t.Fatalf("safe per-commit coordinates lost: %+v", report)
	}
	if strings.Contains(output.String(), "*main.unfamiliarCommitDecodeError") || strings.Contains(output.String(), "*main.commitErrorWrapper") {
		t.Fatal("unregistered type name escaped the closed vocabulary")
	}
}

func TestSafePerCommitTaxonomyDoesNotInferAbsentSchemaOrOrdinal(t *testing.T) {
	report := safeReport(&captureFailure{stage: stageHistory, cause: &unfamiliarCommitDecodeError{private: "secret"}})
	if report.CommitSchemaVersion != nil || report.CommitOrdinalFromHead != nil || report.InnermostGoErrorType != "" || report.UnrecognizedGoErrorTypeCount != 1 {
		t.Fatal("unobserved coordinate inferred")
	}
	schema := ^uint64(0)
	report = safeReport(&plugdump.NativeCommitFailure{SchemaVersion: &schema, OrdinalFromHead: 1, Cause: &messageTrap{}})
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var decoded safeCaptureReport
	if err := json.Unmarshal(data, &decoded); err != nil || decoded.CommitSchemaVersion == nil || *decoded.CommitSchemaVersion != schema || *decoded.CommitOrdinalFromHead != 1 {
		t.Fatal("uint64 schema/HEAD ordinal rounded")
	}
}
