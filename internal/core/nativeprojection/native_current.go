package nativeprojection

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const NativeFamilyReportID = "instance_config/restore-import/native-families"

// NativeFamilyReport counts current rows independently of immutable source
// versions. Expired source leases remain evidence and never become current
// imported lease rows.
type NativeFamilyReport struct {
	Source       dumpformat.SourceIdentity `json:"source"`
	Family       string                    `json:"family"`
	History      string                    `json:"history"`
	SourceRows   int                       `json:"source_rows"`
	ImportedRows int                       `json:"imported_rows"`
	ExpiredRows  int                       `json:"expired_rows"`
}

type NativeFamilyEvidence struct {
	Contract    string                `json:"contract"`
	RestoreTime string                `json:"restore_time"`
	Families    []NativeFamilyReport  `json:"families"`
	Schemas     []NativeSchemaReport  `json:"source_schemas,omitempty"`
	Histories   []NativeHistoryReport `json:"source_histories,omitempty"`
	NativeDeltaHistory *driver.NativeDeltaHistoryEvidence `json:"native_delta_history,omitempty"`
}

// NativeHistoryReport separates genuine source incompleteness from complete
// verification of every image actually imported. Events stay a current source
// attachment, never an invented event-to-commit assignment.
type NativeHistoryReport struct {
	Source              dumpformat.SourceIdentity         `json:"source"`
	Complete            bool                              `json:"complete"`
	VerifiedCommitCount uint64                            `json:"verified_commit_count"`
	Coverage            *dumpformat.NativeHistoryCoverage `json:"history_coverage,omitempty"`
}

func NativeHistoryAccounting(archive *dumpformat.Archive) ([]NativeHistoryReport, error) {
	scopes, err := archive.NativeSourceHistoryMetadata()
	if err != nil {
		return nil, err
	}
	bounded := false
	for _, scope := range scopes {
		bounded = bounded || scope.HistoryCoverage != nil
	}
	collection := archive.HasNativeCaptureCollection()
	if !bounded && !collection {
		// Preserve original complete-archive report/receipt bytes.
		return nil, nil
	}
	if collection {
		// These summaries become part of the actual publication and recovery
		// evidence, including when both originals have complete histories.
		// Admit the complete candidate before allocating its result slice.
		for _, scope := range scopes {
			if len(scope.Projects) != 1 {
				return nil, fmt.Errorf("%w: ambiguous collection history owner", driver.ErrUnsupported)
			}
			report := NativeHistoryReport{Source: dumpformat.SourceIdentity{StoreID: scope.StoreID, Project: scope.Projects[0]}, Complete: scope.Complete, VerifiedCommitCount: uint64(scope.ImageCount), Coverage: scope.HistoryCoverage}
			if err := archive.ChargeNativeDeltaJSON("collection_native_history_reports", report); err != nil {
				return nil, err
			}
		}
	}
	reports := make([]NativeHistoryReport, 0, len(scopes))
	for _, scope := range scopes {
		reports = append(reports, NativeHistoryReport{Source: dumpformat.SourceIdentity{StoreID: scope.StoreID, Project: scope.Projects[0]}, Complete: scope.Complete, VerifiedCommitCount: uint64(scope.ImageCount), Coverage: scope.HistoryCoverage})
	}
	return reports, nil
}

func NativeTableIdentity(scope dumpformat.NativeHistory) (dumpformat.SourceIdentity, error) {
	if scope.StoreID == "" || len(scope.Projects) != 1 || scope.Projects[0] == "" {
		return dumpformat.SourceIdentity{}, fmt.Errorf("%w: raw native history requires unambiguous single-project source ownership", driver.ErrUnsupported)
	}
	return dumpformat.SourceIdentity{StoreID: scope.StoreID, Project: scope.Projects[0]}, nil
}

func NativePolicyRefusal(mode driver.RestoreHistoryMode, archive *dumpformat.Archive, scope dumpformat.NativeHistory) error {
	identity, err := NativeTableIdentity(scope)
	if err != nil {
		return err
	}
	if scope.Complete && len(scope.Gaps) == 0 && scope.VerifiedImageCount() != 0 && scope.VerifiedImageCount() == uint64(len(scope.Commits)) {
		return nil
	}
	if scope.HistoryCoverage != nil {
		if err := dumpformat.RequireVerifiedNativeHistory(scope); err != nil {
			return fmt.Errorf("%w: %w", driver.ErrRestoreHistoryRefused{Mode: mode, History: archive.Manifest().History, Reason: driver.RestoreHistoryReasonAvailableUnverified}, err)
		}
		return nil
	}
	gap := dumpformat.IncompleteHistoryError{StoreID: identity.StoreID, Project: identity.Project, Reason: "native immutable history incomplete"}
	family := ""
	if len(scope.Gaps) != 0 {
		first := scope.Gaps[0]
		gap.First, gap.Last, gap.Reason = first.Sequence, first.Sequence, first.Reason
		gap.CommitID, gap.Family = first.CommitID, first.Family
		family = first.Family
	}
	policy := driver.ErrRestoreHistoryRefused{Mode: mode, History: archive.Manifest().History, Reason: driver.RestoreHistoryReasonAvailableUnverified}
	if family != "" {
		return fmt.Errorf("native family %q: %w: %w", family, policy, gap)
	}
	return fmt.Errorf("%w: %w", policy, gap)
}

func NativeImageRecords(identity dumpformat.SourceIdentity, tables map[string]dumpformat.NativeTable, owners ...*dumpformat.Archive) ([]driver.Record, error) {
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)
	rows := make([]driver.Record, 0, len(names))
	for _, name := range names {
		var records []driver.Record
		var err error
		if len(owners) != 0 {
			records, err = owners[0].NativeV1TableRecords(identity, name, "retained-native-commit", tables[name])
		} else {
			records, err = dumpformat.NativeV1TableRecords(identity, name, "retained-native-commit", tables[name])
		}
		if err != nil {
			return nil, err
		}
		rows = append(rows, records...)
	}
	return rows, nil
}

func NativeLeaseUnexpired(fields dumpformat.Row, when time.Time) (bool, error) {
	text, ok := fields["lease_expires_at"].(string)
	if !ok || when.IsZero() {
		return false, fmt.Errorf("%w: current native lease requires exact expiration and bound restore time", driver.ErrUnsupported)
	}
	expires, err := time.Parse(time.RFC3339Nano, text)
	if err != nil || expires.UTC().Format(time.RFC3339Nano) != text {
		return false, fmt.Errorf("%w: invalid current native lease expiration", driver.ErrUnsupported)
	}
	return expires.After(when), nil
}

// NativeCurrentClasses validates the original complete family census without
// cloning rows. Its returned scalar metadata owns no borrowed table data.
func NativeCurrentClasses(scope dumpformat.NativeHistory) (map[string]string, map[string]int, error) {
	classes, counts := map[string]string{}, map[string]int{}
	for _, source := range scope.Sources {
		if source.History == "" {
			continue
		}
		if classes[source.Family] != "" || source.History != dumpformat.FamilyVersioned && source.History != dumpformat.FamilyUnversioned {
			return nil, nil, driver.ErrUnsupported
		}
		value, ok := scope.Current[source.Family]
		if !ok || source.Rows != len(value.Rows) {
			return nil, nil, driver.ErrUnsupported
		}
		classes[source.Family], counts[source.Family] = source.History, source.Rows
	}
	for name, value := range scope.Current {
		if scope.OpaqueNativeCurrentFamily(name) {
			if classes[name] != "" {
				return nil, nil, driver.ErrUnsupported
			}
			classes[name], counts[name] = dumpformat.FamilyOpaqueNative, len(value.Rows)
		}
	}
	if len(classes) != len(scope.Current) {
		return nil, nil, driver.ErrUnsupported
	}
	return classes, counts, nil
}

func NativeCurrentRecords(archive *dumpformat.Archive, when time.Time, contexts ...context.Context) ([]driver.Record, []NativeFamilyReport, error) {
	ctx := NativeOperationContext(contexts)
	if _, delta := archive.NativeDeltaRoot(); delta {
		return NativeDeltaCurrentRecords(ctx, archive, when)
	}
	rows, reports := []driver.Record{}, []NativeFamilyReport{}
	err := archive.VisitNativeSourceHistories(ctx, func(scope dumpformat.NativeHistory) error {
		if err := NativePolicyRefusal(driver.RestoreHistoryModeFull, archive, scope); err != nil {
			return err
		}
		identity, err := NativeTableIdentity(scope)
		if err != nil {
			return err
		}
		foundHead := false
		for _, image := range scope.Images {
			if image.CommitID != scope.Head {
				continue
			}
			foundHead = true
			head, err := NativeImageRecords(identity, image.Tables, archive)
			if err != nil {
				return err
			}
			rows = append(rows, head...)
		}
		if !foundHead {
			return fmt.Errorf("%w: native immutable head image unavailable", driver.ErrUnsupported)
		}
		classes, counts, err := NativeCurrentClasses(scope)
		if err != nil {
			return err
		}
		names := make([]string, 0, len(classes))
		for name := range classes {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if err := ctx.Err(); err != nil {
				return err
			}
			value := scope.Current[name]
			report := NativeFamilyReport{Source: identity, Family: name, History: classes[name], SourceRows: counts[name], ImportedRows: counts[name]}
			if name == "leases" {
				retained := []dumpformat.Row{}
				for _, row := range value.Rows {
					keep, err := NativeLeaseUnexpired(row, when)
					if err != nil {
						return err
					}
					if keep {
						retained = append(retained, row)
					}
				}
				value.Rows = retained // row maps still borrow authenticated source
				report.ImportedRows, report.ExpiredRows = len(retained), report.SourceRows-len(retained)
			}
			records, err := archive.NativeV1TableRecords(identity, name, classes[name], value)
			if err != nil {
				return err
			}
			rows, reports = append(rows, records...), append(reports, report)
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	sort.Slice(reports, func(i, j int) bool {
		a, b := reports[i], reports[j]
		if a.Source.StoreID != b.Source.StoreID {
			return a.Source.StoreID < b.Source.StoreID
		}
		if a.Source.Project != b.Source.Project {
			return a.Source.Project < b.Source.Project
		}
		return a.Family < b.Family
	})
	return rows, reports, nil
}
