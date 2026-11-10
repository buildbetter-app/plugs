package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
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

type nativeFamilyEvidence struct {
	Contract    string                `json:"contract"`
	RestoreTime string                `json:"restore_time"`
	Families    []NativeFamilyReport  `json:"families"`
	Schemas     []NativeSchemaReport  `json:"source_schemas,omitempty"`
	Histories   []NativeHistoryReport `json:"source_histories,omitempty"`
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

func nativeHistoryAccounting(archive *dumpformat.Archive) ([]NativeHistoryReport, error) {
	scopes, err := archive.NativeSourceHistoryMetadata()
	if err != nil {
		return nil, err
	}
	bounded := false
	for _, scope := range scopes {
		bounded = bounded || scope.HistoryCoverage != nil
	}
	if !bounded {
		// Preserve original complete-archive report/receipt bytes.
		return nil, nil
	}
	reports := make([]NativeHistoryReport, 0, len(scopes))
	for _, scope := range scopes {
		reports = append(reports, NativeHistoryReport{Source: dumpformat.SourceIdentity{StoreID: scope.StoreID, Project: scope.Projects[0]}, Complete: scope.Complete, VerifiedCommitCount: uint64(scope.ImageCount), Coverage: scope.HistoryCoverage})
	}
	return reports, nil
}

type nativeTableBody struct {
	Source  dumpformat.SourceIdentity `json:"source_identity"`
	Table   string                    `json:"table"`
	History string                    `json:"history"`
	Columns []string                  `json:"columns"`
	Rows    []dumpformat.Row          `json:"rows"`
}

func nativeTableIdentity(scope dumpformat.NativeHistory) (dumpformat.SourceIdentity, error) {
	if scope.StoreID == "" || len(scope.Projects) != 1 || scope.Projects[0] == "" {
		return dumpformat.SourceIdentity{}, fmt.Errorf("%w: raw native history requires unambiguous single-project source ownership", driver.ErrUnsupported)
	}
	return dumpformat.SourceIdentity{StoreID: scope.StoreID, Project: scope.Projects[0]}, nil
}

func nativePolicyRefusal(mode driver.RestoreHistoryMode, archive *dumpformat.Archive, scope dumpformat.NativeHistory) error {
	identity, err := nativeTableIdentity(scope)
	if err != nil {
		return err
	}
	if scope.Complete && len(scope.Gaps) == 0 && len(scope.Images) != 0 && len(scope.Images) == len(scope.Commits) {
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

func nativeTableRecord(identity dumpformat.SourceIdentity, table, history string, value dumpformat.NativeTable) (driver.Record, error) {
	ref, err := nativeTableRef(identity, table, history)
	if err != nil {
		return driver.Record{}, err
	}
	body, err := json.Marshal(nativeTableBody{Source: identity, Table: table, History: history, Columns: value.Columns, Rows: value.Rows})
	return driver.Record{Ref: ref, Kind: "beads-archive", Body: body}, err
}

func nativeTableRef(identity dumpformat.SourceIdentity, table, history string) (model.Ref, error) {
	tuple, err := json.Marshal([]string{identity.StoreID, identity.Project, table})
	if err != nil {
		return model.Ref{}, err
	}
	prefix := driver.NativeTableHistoryRecordPrefix
	if history != "retained-native-commit" {
		prefix = "source/native-current/"
	}
	return model.RefByID(prefix+hashBytes(tuple), model.Natural64(0)), nil
}

func nativeImageRecords(identity dumpformat.SourceIdentity, tables map[string]dumpformat.NativeTable) ([]driver.Record, error) {
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)
	rows := make([]driver.Record, 0, len(names))
	for _, name := range names {
		row, err := nativeTableRecord(identity, name, "retained-native-commit", tables[name])
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func lowerNativeTableHistory(archive *dumpformat.Archive, profile driver.RestoreImportProfile) (driver.OfflineHistory, error) {
	if profile.SourcePlanes == nil || profile.HistoryMode != driver.RestoreHistoryModeFull {
		return driver.OfflineHistory{}, driver.ErrUnsupported
	}
	scopes, err := archive.NativeSourceHistoriesChecked()
	if err != nil {
		return driver.OfflineHistory{}, err
	}
	if len(scopes) == 0 {
		return driver.OfflineHistory{}, driver.ErrUnsupported
	}
	for _, scope := range scopes {
		if err := nativePolicyRefusal(profile.HistoryMode, archive, scope); err != nil {
			return driver.OfflineHistory{}, err
		}
	}
	sort.Slice(scopes, func(i, j int) bool {
		if scopes[i].StoreID != scopes[j].StoreID {
			return scopes[i].StoreID < scopes[j].StoreID
		}
		return scopes[i].Projects[0] < scopes[j].Projects[0]
	})
	history := driver.OfflineHistory{Source: driver.NativeTableHistorySource, Verified: true, Complete: true, First: 1, Baseline: []driver.Record{}, Gaps: []driver.SourceHistoryGap{}, Batches: []driver.SourceHistoryBatch{}}
	for _, scope := range scopes {
		identity, err := nativeTableIdentity(scope)
		if err != nil {
			return driver.OfflineHistory{}, err
		}
		if scope.HistoryCoverage != nil {
			history.Complete = false
			history.NativeBoundaries = append(history.NativeBoundaries, driver.NativeHistoryBoundary{SourceStoreID: identity.StoreID, Project: identity.Project, VerifiedCommitCount: uint64(len(scope.Images)), BoundarySHA256: dumpformat.NativeHistoryCoverageSHA256(*scope.HistoryCoverage)})
		}
		before := []driver.Record{}
		var beforeTables map[string]dumpformat.NativeTable
		for _, image := range scope.Images {
			// Account the exact full lowered records and their before/after body
			// copies before constructing or retaining a SourceHistoryBatch.
			if err := chargeNativeDeltaBatch(archive, history.Head+1, identity, image, before, beforeTables); err != nil {
				return driver.OfflineHistory{}, err
			}
			after, err := nativeImageRecords(identity, image.Tables)
			if err != nil {
				return driver.OfflineHistory{}, err
			}
			history.Head++
			history.Batches = append(history.Batches, driver.SourceHistoryBatch{SourceStoreID: identity.StoreID, Project: identity.Project, SourceSequence: image.Sequence, Sequence: history.Head, CommitIDs: []string{image.CommitID}, EventIDs: []string{}, Changes: historyDelta(before, after)})
			before = after
			beforeTables = image.Tables
		}
	}
	if err := chargeNativeDeltaEnvelope(archive, history); err != nil {
		return driver.OfflineHistory{}, err
	}
	return history, nil
}

func nativeLeaseUnexpired(fields dumpformat.Row, when time.Time) (bool, error) {
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

func nativeCurrentRecords(archive *dumpformat.Archive, when time.Time) ([]driver.Record, []NativeFamilyReport, error) {
	rows := []driver.Record{}
	reports := []NativeFamilyReport{}
	scopes, err := archive.NativeSourceHistoriesChecked()
	if err != nil {
		return nil, nil, err
	}
	for _, scope := range scopes {
		if err := nativePolicyRefusal(driver.RestoreHistoryModeFull, archive, scope); err != nil {
			return nil, nil, err
		}
		identity, err := nativeTableIdentity(scope)
		if err != nil {
			return nil, nil, err
		}
		foundHead := false
		for _, image := range scope.Images {
			if image.CommitID != scope.Head {
				continue
			}
			foundHead = true
			head, err := nativeImageRecords(identity, image.Tables)
			if err != nil {
				return nil, nil, err
			}
			rows = append(rows, head...)
		}
		if !foundHead {
			return nil, nil, fmt.Errorf("%w: native immutable head image unavailable", driver.ErrUnsupported)
		}
		seen := map[string]bool{}
		for _, source := range scope.Sources {
			if source.History == "" {
				continue
			}
			if source.History != "versioned" && source.History != "unversioned-in-beads-by-design" || seen[source.Family] {
				return nil, nil, fmt.Errorf("%w: ambiguous native family disposition", driver.ErrUnsupported)
			}
			seen[source.Family] = true
			value, ok := scope.Current[source.Family]
			if !ok || source.Rows != len(value.Rows) {
				return nil, nil, fmt.Errorf("%w: required current native family unavailable: %s", driver.ErrUnsupported, source.Family)
			}
			report := NativeFamilyReport{Source: identity, Family: source.Family, History: source.History, SourceRows: len(value.Rows), ImportedRows: len(value.Rows)}
			if source.Family == "leases" {
				retained := []dumpformat.Row{}
				for _, row := range value.Rows {
					keep, err := nativeLeaseUnexpired(row, when)
					if err != nil {
						return nil, nil, err
					}
					if keep {
						retained = append(retained, row)
					}
				}
				value.Rows = retained
				report.ImportedRows, report.ExpiredRows = len(retained), report.SourceRows-len(retained)
			}
			row, err := nativeTableRecord(identity, source.Family, source.History, value)
			if err != nil {
				return nil, nil, err
			}
			rows, reports = append(rows, row), append(reports, report)
		}
		physical := []string{}
		for name := range scope.Current {
			if scope.OpaqueNativeCurrentFamily(name) {
				physical = append(physical, name)
			}
		}
		sort.Strings(physical)
		for _, name := range physical {
			if seen[name] {
				return nil, nil, fmt.Errorf("%w: opaque native family has portable disposition", driver.ErrUnsupported)
			}
			seen[name] = true
			value := scope.Current[name]
			row, err := nativeTableRecord(identity, name, dumpformat.FamilyOpaqueNative, value)
			if err != nil {
				return nil, nil, err
			}
			rows = append(rows, row)
			reports = append(reports, NativeFamilyReport{Source: identity, Family: name, History: dumpformat.FamilyOpaqueNative, SourceRows: len(value.Rows), ImportedRows: len(value.Rows)})
		}
		if len(seen) != len(scope.Current) {
			return nil, nil, fmt.Errorf("%w: incomplete current native family dispositions", driver.ErrUnsupported)
		}
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

// Recovery recomputes accounting from the authenticated original envelope and
// the admitted restore clock. Neither a stored report nor a caller's new clock
// can change which source leases were eligible in the committed import.
func recoverNativeFamilies(ctx context.Context, reader driver.Reader, profile driver.RestoreImportProfile, evidence archiveEvidence, receipt *driver.IdempotencyRecord) ([]NativeFamilyReport, []NativeSchemaReport, []NativeHistoryReport, error) {
	if profile.SourcePlanes == nil || profile.HistoryMode != driver.RestoreHistoryModeFull {
		return nil, nil, nil, nil
	}
	raw, err := reader.Blob(ctx, driver.Hash(evidence.ArchiveSHA256))
	if err != nil || hashBytes(raw) != profile.ArchiveDigest {
		return nil, nil, nil, driver.ErrConflict
	}
	archive, err := readRetainedImportArchive(ctx, reader, raw, evidence)
	if err != nil {
		return nil, nil, nil, nativeDeltaRecoveryError(err)
	}
	if !archive.HasNativeSourceHistories() {
		return nil, nil, nil, nil
	}
	when, err := time.Parse(time.RFC3339Nano, profile.HistoryTime)
	if err != nil {
		return nil, nil, nil, driver.ErrConflict
	}
	_, reports, err := nativeCurrentRecords(archive, when)
	if err != nil {
		return nil, nil, nil, nativeDeltaRecoveryError(err)
	}
	var schemas []NativeSchemaReport
	if archive.Manifest().MainVersion != 72 {
		plan, planErr := buildPlanWithProfile(archive, &profile, when)
		if planErr != nil {
			return nil, nil, nil, nativeDeltaRecoveryError(planErr)
		}
		schemas = plan.nativeSchemas
	}
	histories, err := nativeHistoryAccounting(archive)
	if err != nil {
		return nil, nil, nil, err
	}
	want, err := json.Marshal(nativeFamilyEvidence{Contract: archive.Manifest().SourceFamilyContract, RestoreTime: when.UTC().Format(time.RFC3339Nano), Families: reports, Schemas: schemas, Histories: histories})
	if err != nil {
		return nil, nil, nil, driver.ErrConflict
	}
	var committed json.RawMessage
	for _, result := range receipt.Commit.Results {
		if result.Ref.ID == NativeFamilyReportID {
			committed = result.Record
		}
	}
	rows, err := reader.Get(ctx, []model.Ref{model.RefByID(NativeFamilyReportID, model.Natural64(0))})
	if err != nil || len(rows) != 1 || rows[0].Kind != "instance_config" || !bytes.Equal(rows[0].Body, want) || !bytes.Equal(committed, want) {
		return nil, nil, nil, driver.ErrConflict
	}
	return reports, schemas, histories, nil
}
