package nativeprojection

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"time"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func NativeDeltaNormalizedRow(row dumpformat.NativeDeltaRow) (dumpformat.Row, error) {
	raw, err := base64.StdEncoding.DecodeString(row.NormalizedJSON)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var fields dumpformat.Row
	if err := decoder.Decode(&fields); err != nil {
		return nil, err
	}
	return fields, nil
}

func NativeDeltaCurrentRecords(ctx context.Context, archive *dumpformat.Archive, when time.Time) ([]driver.Record, []NativeFamilyReport, error) {
	owner := archive
	archive, release := archive.NativeDeltaAccountingScope()
	defer release()
	var identity dumpformat.SourceIdentity
	var classes map[string]string
	var counts map[string]int
	scopeCount := 0
	err := archive.VisitNativeSourceHistories(ctx, func(scope dumpformat.NativeHistory) error {
		scopeCount++
		if scopeCount != 1 {
			return driver.ErrUnsupported
		}
		if err := NativePolicyRefusal(driver.RestoreHistoryModeFull, archive, scope); err != nil {
			return err
		}
		var err error
		identity, err = NativeTableIdentity(scope)
		if err != nil {
			return err
		}
		classes, counts, err = NativeCurrentClasses(scope)
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	if scopeCount != 1 {
		return nil, nil, driver.ErrUnsupported
	}
	records := []driver.Record{}
	reports := []NativeFamilyReport{}
	seen := map[string]bool{}
	err = archive.VisitNativeDeltaTableStates(ctx, func(state dumpformat.NativeDeltaTableState) error {
		if state.Kind == "commit" || state.Complete {
			return nil
		}
		history := "retained-native-commit"
		var keep func(dumpformat.Row) (bool, error)
		report := NativeFamilyReport{}
		if state.Kind == "current" {
			history = classes[state.Schema.Name]
			if history == "" || seen[state.Schema.Name] || len(state.Rows) != counts[state.Schema.Name] {
				return driver.ErrUnsupported
			}
			seen[state.Schema.Name] = true
			report = NativeFamilyReport{Source: identity, Family: state.Schema.Name, History: history, SourceRows: len(state.Rows)}
			if state.Schema.Name == "leases" {
				keep = func(fields dumpformat.Row) (bool, error) { return NativeLeaseUnexpired(fields, when) }
			}
		} else if state.Kind != "head-checkpoint" {
			return driver.ErrUnsupported
		}
		lowered, projected, err := owner.NativeDeltaTableRecords(state, history, keep)
		if err != nil {
			return err
		}
		records = append(records, lowered...)
		if state.Kind == "current" {
			report.ImportedRows = len(projected.Rows)
			report.ExpiredRows = report.SourceRows - report.ImportedRows
			reports = append(reports, report)
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if len(seen) != len(classes) {
		return nil, nil, fmt.Errorf("native row current census differs")
	}
	sort.Slice(reports, func(i, j int) bool { return reports[i].Family < reports[j].Family })
	return records, reports, nil
}
