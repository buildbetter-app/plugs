package plugdump

import (
	"context"
	"fmt"
	"time"

	"github.com/steveyegge/beads/internal/captureclock"
)

// Read the genuine SQL expressions used by native decisions. In particular,
// UTC_TIMESTAMP() has second precision even when the engine clock has usecs.
// This is an observation, never a replacement for a business query or result.
const nativeClockQuery = `SELECT
DATE_FORMAT(UTC_TIMESTAMP(6), '%Y-%m-%dT%H:%i:%s.%fZ') AS utc_microseconds,
DATE_FORMAT(UTC_TIMESTAMP(), '%Y-%m-%dT%H:%i:%s.%fZ') AS utc_default,
@@session.time_zone AS session_time_zone, VERSION() AS server_version`

const nativeClockColumnsQuery = `SELECT table_name, column_name, data_type, datetime_precision
FROM information_schema.columns WHERE table_schema = DATABASE()
AND table_name IN ('issues', 'wisps') AND column_name IN ('created_at', 'updated_at', 'defer_until')
ORDER BY table_name, column_name`

type nativeClockEvidence struct {
	Input                     *captureclock.Snapshot `json:"input"`
	GoObserved                time.Time              `json:"go_observed"`
	GoInputResolutionNS       int64                  `json:"go_input_resolution_ns"`
	SQLInputResolutionNS      int64                  `json:"sql_input_resolution_ns"`
	SQLDefaultUTCResolutionNS int64                  `json:"sql_default_utc_timestamp_resolution_ns"`
	SQLQuery                  string                 `json:"sql_query"`
	SQLResult                 Table                  `json:"sql_result"`
	ColumnPrecisionQuery      string                 `json:"column_precision_query"`
	ColumnPrecision           Table                  `json:"column_precision"`
	Scope                     string                 `json:"scope"`
}

func (s SQLSource) observeNativeClock(ctx context.Context, input *captureclock.Snapshot) (*nativeClockEvidence, error) {
	if input == nil {
		return nil, nil
	}
	observed := captureclock.Now().UTC()
	if !observed.Equal(input.GoTime) {
		return nil, fmt.Errorf("native capture Go clock changed during census")
	}
	table, err := s.historyTable(ctx, nativeClockQuery)
	if err != nil {
		return nil, fmt.Errorf("native capture SQL clock readback: %w", err)
	}
	if len(table.Rows) != 1 {
		return nil, fmt.Errorf("native capture SQL clock readback requires one row")
	}
	format := "2006-01-02T15:04:05.000000Z"
	if text(table.Rows[0], "utc_microseconds") != input.SQLTime.Format(format) ||
		text(table.Rows[0], "utc_default") != input.SQLTime.Truncate(time.Second).Format(format) {
		return nil, fmt.Errorf("native capture SQL clock differs from explicit input")
	}
	columns, err := s.historyTable(ctx, nativeClockColumnsQuery)
	if err != nil || len(columns.Rows) != 6 {
		return nil, fmt.Errorf("native capture clock column precision census: %v", err)
	}
	return &nativeClockEvidence{
		Input: input, GoObserved: observed,
		GoInputResolutionNS: 1, SQLInputResolutionNS: 1000,
		SQLDefaultUTCResolutionNS: int64(time.Second),
		SQLQuery:                  nativeClockQuery, SQLResult: table,
		ColumnPrecisionQuery: nativeClockColumnsQuery, ColumnPrecision: columns,
		Scope: "named Go wall-time seams and genuine SQL-server query time; embedded SQL clock unsupported; no elapsed-time override",
	}, nil
}

func unchangedNativeClock(input *captureclock.Snapshot) error {
	final, err := captureclock.Read()
	if err != nil {
		return err
	}
	if input == nil && final == nil {
		return nil
	}
	if input == nil || final == nil || input.Path != final.Path || input.SHA256 != final.SHA256 {
		return fmt.Errorf("native capture clock input changed during census")
	}
	return nil
}
