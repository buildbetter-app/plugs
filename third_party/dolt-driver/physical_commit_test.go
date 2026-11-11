package embedded

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/go-sql-driver/mysql"
)

type opaquePhysicalValue struct{ internal any }

type physicalValuer struct{}

func (physicalValuer) Value() (driver.Value, error) { return "hidden conversion", nil }

func TestPhysicalDriverCellBoundary(t *testing.T) {
	t.Run("unsupported_objects_refuse", func(t *testing.T) {
		for _, value := range []any{opaquePhysicalValue{internal: map[string]int{"x": 1}}, &opaquePhysicalValue{}, map[string]int{"x": 1}, []any{1}, physicalValuer{}} {
			if _, err := physicalCell(value); err == nil {
				t.Errorf("non-scalar object escaped boundary: %T", value)
			}
		}
	})
	t.Run("binary_buffer_owned", func(t *testing.T) {
		original := []byte{0, 255}
		value, err := physicalCell(original)
		if err != nil {
			t.Fatal(err)
		}
		original[0] = 9
		if string(value.([]byte)) != string([]byte{0, 255}) {
			t.Fatal("driver returned an engine-owned binary buffer")
		}
	})
	t.Run("exact_scalar_and_unsigned_max", func(t *testing.T) {
		for _, value := range []any{nil, true, "text", int8(-1), int64(math.MinInt64), uint64(math.MaxUint64), float64(0.5), time.Unix(12, 345).UTC()} {
			got, err := physicalCell(value)
			if err != nil || got != value {
				t.Errorf("detached scalar changed: type=%T error=%v", value, err)
			}
		}
	})
}

func TestPhysicalProbeCursorRetainsActualIntegerWidthAndRejectsUnknown(t *testing.T) {
	for _, value := range []driver.Value{int32(39), int64(39), uint64(math.MaxUint64), int32(-1), nil, "39"} {
		image := PhysicalCommitSchema{Tables: []PhysicalTable{{Name: "schema_migrations", Columns: []PhysicalColumn{{Name: "version"}}, Rows: [][]driver.Value{{value}}}}}
		version, err := physicalProbeSchemaVersion(image)
		switch v := value.(type) {
		case int32:
			if v < 0 {
				if err == nil || version != nil {
					t.Fatal("negative cursor became a schema coordinate")
				}
			} else if err != nil || version == nil || *version != uint64(v) {
				t.Fatal("actual INT cursor width was not preserved")
			}
		case int64:
			if err != nil || version == nil || *version != uint64(v) {
				t.Fatal("actual BIGINT cursor changed")
			}
		case uint64:
			if err != nil || version == nil || *version != v {
				t.Fatal("unsigned cursor was rounded")
			}
		default:
			if err == nil || version != nil {
				t.Fatal("unknown cursor became a numeric coordinate")
			}
		}
	}
	if version, err := physicalProbeSchemaVersion(PhysicalCommitSchema{}); err != nil || version != nil {
		t.Fatal("absent cursor was assigned a version")
	}
}

type physicalFailureRows struct {
	rows      []gms.Row
	nextErr   error
	closeErr  error
	closes    int
	nextPanic any
}

func (rows *physicalFailureRows) Next(*gms.Context) (gms.Row, error) {
	if rows.nextPanic != nil {
		panic(rows.nextPanic)
	}
	if len(rows.rows) > 0 {
		row := rows.rows[0]
		rows.rows = rows.rows[1:]
		return row, nil
	}
	if rows.nextErr != nil {
		return nil, rows.nextErr
	}
	return nil, io.EOF
}

func (rows *physicalFailureRows) Close(*gms.Context) error {
	rows.closes++
	return rows.closeErr
}

func TestPhysicalRowsRetainOriginalIterationAndCleanupFailures(t *testing.T) {
	ctx := gms.NewContext(context.Background())
	schema := gms.Schema{&gms.Column{Name: "id", Type: types.Int64}}
	for _, test := range []struct {
		name, reason string
		original     error
		cleanup      error
		codes        []uint16
	}{
		{"missing-hash", "missing_table_chunk", doltdb.ErrHashNotFound, nil, []uint16{}},
		{"missing-file", "missing_table_chunk", &os.PathError{Op: "read", Path: "/public/chunk", Err: fs.ErrNotExist}, nil, []uint16{}},
		{"present-corrupt", "", errors.New("present corrupt row bytes"), nil, []uint16{}},
		{"original-sql-code", "", &mysql.MySQLError{Number: 1146, Message: "public fixture error"}, nil, []uint16{1146}},
		{"missing-with-cleanup-failure", "", doltdb.ErrHashNotFound, errors.New("close failed"), []uint16{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			iter := &physicalFailureRows{rows: []gms.Row{{int64(7)}}, nextErr: test.original, closeErr: test.cleanup}
			rows, err := readPhysicalRows(ctx, schema, iter)
			if rows != nil || !errors.Is(err, test.original) || iter.closes != 1 {
				t.Fatal("immutable iterator lost its original failure, leaked partial rows or did not close exactly once")
			}
			if test.cleanup != nil && !errors.Is(err, test.cleanup) {
				t.Fatal("immutable iterator dropped an actual cleanup failure")
			}
			failure := physicalReadError("rows", "public", "issues", err)
			if PhysicalHistoryUnavailableReason(failure) != test.reason {
				t.Fatal("iterator absence/corruption classification differs from original evidence")
			}
			var evidence *physicalErrorEvidence
			if !errors.As(failure, &evidence) || !reflect.DeepEqual(evidence.SafeSQLCodes(), test.codes) {
				t.Fatal("immutable row diagnostics invented or lost original SQL/Dolt numbers")
			}
			originalType := reflect.TypeOf(test.original).String()
			found := false
			for _, name := range evidence.SafeGoErrorTypes() {
				found = found || name == originalType
			}
			if !found {
				t.Fatal("immutable row diagnostics dropped an original error type")
			}
		})
	}
	// Ordinary driver callers keep the existing translated MySQL error surface.
	original := errors.New("ordinary public SQL failure")
	ordinary := doltRows{sch: schema, rowIter: &physicalFailureRows{nextErr: original}, gmsCtx: ctx}
	err := ordinary.Next(make([]driver.Value, 1))
	var mysqlError *mysql.MySQLError
	if !errors.As(err, &mysqlError) || errors.Is(err, original) {
		t.Fatal("immutable adapter changed ordinary driver error compatibility")
	}
}

func TestPhysicalRowsKeepDriverConversionAndOwnBinaryBuffers(t *testing.T) {
	ctx := gms.NewContext(context.Background())
	schema := gms.Schema{
		&gms.Column{Name: "binary", Type: types.LongBlob},
		&gms.Column{Name: "converted", Type: types.Text},
		&gms.Column{Name: "null", Type: types.Text},
		&gms.Column{Name: "bigint", Type: types.Int64},
	}
	buffer := []byte{0, 255, 128}
	row := gms.Row{buffer, physicalValuer{}, nil, int64(9007199254740993)}
	ordinary := doltRows{sch: schema, rowIter: &physicalFailureRows{rows: []gms.Row{row}}, gmsCtx: ctx}
	want := make([]driver.Value, len(schema))
	if err := ordinary.Next(want); err != nil {
		t.Fatal(err)
	}
	iter := &physicalFailureRows{rows: []gms.Row{row}}
	got, err := readPhysicalRows(ctx, schema, iter)
	if err != nil || len(got) != 1 || !reflect.DeepEqual(got[0], want) || iter.closes != 1 {
		t.Fatal("immutable adapter changed the existing driver conversion")
	}
	buffer[0] = 9
	if got[0][0].([]byte)[0] != 0 {
		t.Fatal("immutable adapter retained an engine-owned binary buffer")
	}
}

type physicalFailurePartition struct{}

func (physicalFailurePartition) Key() []byte { return []byte("public") }

type physicalFailurePartitions struct {
	nextErr, closeErr error
	emit, emitted     bool
	closes            int
	nextPanic         any
}

func (iter *physicalFailurePartitions) Next(*gms.Context) (gms.Partition, error) {
	if iter.nextPanic != nil {
		panic(iter.nextPanic)
	}
	if iter.emit && !iter.emitted {
		iter.emitted = true
		return physicalFailurePartition{}, nil
	}
	if iter.nextErr != nil {
		return nil, iter.nextErr
	}
	return nil, io.EOF
}

func (iter *physicalFailurePartitions) Close(*gms.Context) error {
	iter.closes++
	return iter.closeErr
}

type physicalFailureTable struct {
	partitions *physicalFailurePartitions
	rows       *physicalFailureRows
	rowErr     error
	rowCalls   int
	rowPanic   any
}

func (*physicalFailureTable) Name() string   { return "issues" }
func (*physicalFailureTable) String() string { return "public fixture" }
func (*physicalFailureTable) Schema(*gms.Context) gms.Schema {
	return gms.Schema{&gms.Column{Name: "id", Type: types.Int64}}
}
func (*physicalFailureTable) Collation() gms.CollationID { return gms.Collation_Default }
func (table *physicalFailureTable) Partitions(*gms.Context) (gms.PartitionIter, error) {
	return table.partitions, nil
}
func (table *physicalFailureTable) PartitionRows(*gms.Context, gms.Partition) (gms.RowIter, error) {
	table.rowCalls++
	if table.rowPanic != nil {
		panic(table.rowPanic)
	}
	return table.rows, table.rowErr
}

func TestPhysicalProbeCompleteReaderRefusesAndStopsOnPanicOrLateAbsence(t *testing.T) {
	ctx := gms.NewContext(context.Background())
	original := fmt.Errorf("public late missing sentinel: %w", doltdb.ErrHashNotFound)
	cleanup := errors.New("public close failure")
	for _, stage := range []string{"partition-next", "partition-rows", "row-next", "late-absence"} {
		for _, failedClose := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cleanup_%t", stage, failedClose), func(t *testing.T) {
				partitions := &physicalFailurePartitions{emit: true}
				rows := &physicalFailureRows{}
				table := &physicalFailureTable{partitions: partitions, rows: rows}
				switch stage {
				case "partition-next":
					partitions.nextPanic = original
				case "partition-rows":
					table.rowPanic = original
				case "row-next":
					rows.nextPanic = original
				case "late-absence":
					rows.nextErr = original
				}
				if failedClose {
					partitions.closeErr = cleanup
					rows.closeErr = cleanup
				}
				image, err := readPhysicalProbedTable(ctx, table, "issues")
				var inconsistent *PhysicalStoreInconsistent
				if !errors.As(err, &inconsistent) || !errors.Is(err, original) || !reflect.DeepEqual(image, PhysicalTable{}) || partitions.closes != 1 {
					t.Fatal("probed reader leaked panic/partial image, lost original cause, or failed cleanup")
				}
				if stage != "late-absence" && inconsistent.PanicType != fmt.Sprintf("%T", original) {
					t.Fatal("panic type was lost")
				}
				if failedClose && !errors.Is(err, cleanup) {
					t.Fatal("panic recovery lost a cleanup failure")
				}
				wantRowsClosed := 0
				if stage == "row-next" || stage == "late-absence" {
					wantRowsClosed = 1
				}
				if rows.closes != wantRowsClosed || table.rowCalls > 1 {
					t.Fatal("reader continued or closed an unowned iterator")
				}
				failure := physicalReadError("inconsistent-store", "public", "issues", err)
				if PhysicalHistoryUnavailableReason(failure) != "" || missingPhysicalHistory("rows", err) != "" || !errors.As(failure, &inconsistent) {
					t.Fatal("late missing sentinel was admitted as a history boundary")
				}
			})
		}
	}
}

func TestPhysicalTablePreservesPartitionAndRowCleanupFailures(t *testing.T) {
	ctx := gms.NewContext(context.Background())
	missing := fmt.Errorf("public missing row data: %w", doltdb.ErrHashNotFound)
	cleanup := &mysql.MySQLError{Number: 1205, Message: "public cleanup failure"}
	for _, stage := range []string{"partition-next", "partition-rows", "row-next"} {
		for _, failedClose := range []bool{false, true} {
			name := fmt.Sprintf("%s/close_failure_%t", stage, failedClose)
			t.Run(name, func(t *testing.T) {
				partitions := &physicalFailurePartitions{emit: stage != "partition-next"}
				rows := &physicalFailureRows{}
				table := &physicalFailureTable{partitions: partitions, rows: rows}
				switch stage {
				case "partition-next":
					partitions.nextErr = missing
				case "partition-rows":
					table.rowErr = missing
				case "row-next":
					rows.nextErr = missing
				}
				if failedClose {
					partitions.closeErr = cleanup
				}
				_, err := readPhysicalTable(ctx, table, "issues")
				if !errors.Is(err, missing) || partitions.closes != 1 {
					t.Fatal("table read lost its original error or did not close partitions once")
				}
				wantReason := "missing_table_chunk"
				wantCodes := []uint16{}
				if failedClose {
					wantReason, wantCodes = "", []uint16{1205}
					if !errors.Is(err, cleanup) {
						t.Fatal("table read dropped its actual partition cleanup failure")
					}
				}
				failure := physicalReadError("rows", "public", "issues", err)
				var evidence *physicalErrorEvidence
				if PhysicalHistoryUnavailableReason(failure) != wantReason || !errors.As(failure, &evidence) || !reflect.DeepEqual(evidence.SafeSQLCodes(), wantCodes) {
					t.Fatal("partition cleanup changed the original evidence or admitted a false boundary")
				}
				wantRowCloses := 0
				if stage == "row-next" {
					wantRowCloses = 1
				}
				if rows.closes != wantRowCloses {
					t.Fatal("table read closed an unowned row iterator or failed to close its owned iterator")
				}
			})
		}
	}
	t.Run("cleanup_only_missing_does_not_prove_history_absence", func(t *testing.T) {
		partitions := &physicalFailurePartitions{closeErr: missing}
		_, err := readPhysicalTable(ctx, &physicalFailureTable{partitions: partitions}, "issues")
		if !errors.Is(err, missing) || partitions.closes != 1 || PhysicalHistoryUnavailableReason(physicalReadError("rows", "public", "issues", err)) != "" {
			t.Fatal("cleanup-only failure was lost or mistaken for absent stored history")
		}
	})
	t.Run("row_and_partition_cleanup_causes_preserved", func(t *testing.T) {
		rowCleanup := &mysql.MySQLError{Number: 1213, Message: "public row cleanup failure"}
		partitions := &physicalFailurePartitions{emit: true, closeErr: cleanup}
		rows := &physicalFailureRows{nextErr: missing, closeErr: rowCleanup}
		_, err := readPhysicalTable(ctx, &physicalFailureTable{partitions: partitions, rows: rows}, "issues")
		if !errors.Is(err, missing) || !errors.Is(err, cleanup) || !errors.Is(err, rowCleanup) || rows.closes != 1 || partitions.closes != 1 {
			t.Fatal("table read lost an operation or cleanup cause, or closed more than once")
		}
		failure := physicalReadError("rows", "public", "issues", err)
		var evidence *physicalErrorEvidence
		if PhysicalHistoryUnavailableReason(failure) != "" || !errors.As(failure, &evidence) || !reflect.DeepEqual(evidence.SafeSQLCodes(), []uint16{1205, 1213}) {
			t.Fatal("joined cleanup evidence admitted a boundary or lost an original numeric code")
		}
	})
	t.Run("successful_table_closes_once", func(t *testing.T) {
		partitions := &physicalFailurePartitions{emit: true}
		rows := &physicalFailureRows{rows: []gms.Row{{int64(7)}}}
		table := &physicalFailureTable{partitions: partitions, rows: rows}
		got, err := readPhysicalTable(ctx, table, "issues")
		if err != nil || got.Name != "issues" || len(got.Rows) != 1 || got.Rows[0][0] != int64(7) || table.rowCalls != 1 || rows.closes != 1 || partitions.closes != 1 {
			t.Fatal("successful immutable table conversion changed or failed to close once")
		}
	})
}
