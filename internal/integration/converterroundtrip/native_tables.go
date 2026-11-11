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
	Contract           string                             `json:"contract"`
	RestoreTime        string                             `json:"restore_time"`
	Families           []NativeFamilyReport               `json:"families"`
	Schemas            []NativeSchemaReport               `json:"source_schemas,omitempty"`
	Histories          []NativeHistoryReport              `json:"source_histories,omitempty"`
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

func nativeImageRecords(identity dumpformat.SourceIdentity, tables map[string]dumpformat.NativeTable, owners ...*dumpformat.Archive) ([]driver.Record, error) {
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

func lowerNativeTableHistory(archive *dumpformat.Archive, profile driver.RestoreImportProfile, contexts ...context.Context) (driver.OfflineHistory, error) {
	if profile.SourcePlanes == nil {
		return driver.OfflineHistory{}, driver.ErrUnsupported
	}
	if _, delta := archive.NativeDeltaRoot(); delta {
		if profile.HistoryMode == driver.RestoreHistoryModePartial && profile.NativeDeltaHistory != nil {
			return lowerNativeDeltaPartialHistory(nativeOperationContext(contexts), archive, profile)
		}
		if profile.HistoryMode != driver.RestoreHistoryModeFull {
			return driver.OfflineHistory{}, driver.ErrUnsupported
		}
		return lowerNativeDeltaRowHistory(nativeOperationContext(contexts), archive, profile)
	}
	if profile.HistoryMode != driver.RestoreHistoryModeFull {
		return driver.OfflineHistory{}, driver.ErrUnsupported
	}
	if !archive.HasNativeSourceHistories() {
		return driver.OfflineHistory{}, driver.ErrUnsupported
	}
	history := driver.OfflineHistory{Source: driver.NativeTableHistorySource, Verified: true, Complete: true, First: 1, Baseline: []driver.Record{}, Gaps: []driver.SourceHistoryGap{}, Batches: []driver.SourceHistoryBatch{}}
	ctx := nativeOperationContext(contexts)
	err := archive.VisitNativeSourceHistories(ctx, func(scope dumpformat.NativeHistory) error {
		if err := nativePolicyRefusal(profile.HistoryMode, archive, scope); err != nil {
			return err
		}
		identity, err := nativeTableIdentity(scope)
		if err != nil {
			return err
		}
		if scope.HistoryCoverage != nil {
			history.Complete = false
			history.NativeBoundaries = append(history.NativeBoundaries, driver.NativeHistoryBoundary{SourceStoreID: identity.StoreID, Project: identity.Project, VerifiedCommitCount: uint64(len(scope.Images)), BoundarySHA256: dumpformat.NativeHistoryCoverageSHA256(*scope.HistoryCoverage)})
		}
		before := []driver.Record{}
		_, releaseBefore := archive.NativeDeltaAccountingScope()
		defer func() { releaseBefore() }()
		for _, image := range scope.Images {
			if err := ctx.Err(); err != nil {
				return err
			}
			afterOwner, releaseAfter := archive.NativeDeltaAccountingScope()
			after, err := nativeImageRecords(identity, image.Tables, afterOwner)
			if err != nil {
				releaseAfter()
				return err
			}
			changes, err := nativeHistoryDelta(archive, before, after)
			if err != nil {
				releaseAfter()
				return err
			}
			batch := driver.SourceHistoryBatch{SourceStoreID: identity.StoreID, Project: identity.Project, SourceSequence: image.Sequence, Sequence: history.Head + 1, CommitIDs: []string{image.CommitID}, EventIDs: []string{}, Changes: changes}
			// Both rolling images remain admitted until the detached changed
			// bodies have their retained-batch reservation.
			if err := archive.ChargeNativeDeltaJSON("offline_batches", batch); err != nil {
				releaseAfter()
				return err
			}
			history.Head++
			history.Batches = append(history.Batches, batch)
			releaseBefore()
			before, releaseBefore = after, releaseAfter
		}
		return nil
	})
	if err != nil {
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

// nativeCurrentClasses validates the original complete family census without
// cloning rows. Its returned scalar metadata owns no borrowed table data.
func nativeCurrentClasses(scope dumpformat.NativeHistory) (map[string]string, map[string]int, error) {
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

func nativeCurrentRecords(archive *dumpformat.Archive, when time.Time, contexts ...context.Context) ([]driver.Record, []NativeFamilyReport, error) {
	ctx := nativeOperationContext(contexts)
	if _, delta := archive.NativeDeltaRoot(); delta {
		return nativeDeltaCurrentRecords(ctx, archive, when)
	}
	rows, reports := []driver.Record{}, []NativeFamilyReport{}
	err := archive.VisitNativeSourceHistories(ctx, func(scope dumpformat.NativeHistory) error {
		if err := nativePolicyRefusal(driver.RestoreHistoryModeFull, archive, scope); err != nil {
			return err
		}
		identity, err := nativeTableIdentity(scope)
		if err != nil {
			return err
		}
		foundHead := false
		for _, image := range scope.Images {
			if image.CommitID != scope.Head {
				continue
			}
			foundHead = true
			head, err := nativeImageRecords(identity, image.Tables, archive)
			if err != nil {
				return err
			}
			rows = append(rows, head...)
		}
		if !foundHead {
			return fmt.Errorf("%w: native immutable head image unavailable", driver.ErrUnsupported)
		}
		classes, counts, err := nativeCurrentClasses(scope)
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
					keep, err := nativeLeaseUnexpired(row, when)
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

// Recovery recomputes accounting from the authenticated original envelope and
// the admitted restore clock. Neither a stored report nor a caller's new clock
// can change which source leases were eligible in the committed import.
func recoverNativeFamilies(ctx context.Context, reader driver.Reader, profile driver.RestoreImportProfile, evidence archiveEvidence, receipt *driver.IdempotencyRecord) ([]NativeFamilyReport, []NativeSchemaReport, []NativeHistoryReport, *driver.NativeDeltaHistoryEvidence, error) {
	if profile.SourcePlanes == nil || profile.HistoryMode != driver.RestoreHistoryModeFull && profile.NativeDeltaHistory == nil {
		return nil, nil, nil, nil, nil
	}
	raw, err := reader.Blob(ctx, driver.Hash(evidence.ArchiveSHA256))
	if err != nil || hashBytes(raw) != profile.ArchiveDigest {
		return nil, nil, nil, nil, driver.ErrConflict
	}
	archive, err := readRetainedImportArchive(ctx, reader, raw, evidence)
	if err != nil {
		return nil, nil, nil, nil, nativeDeltaRecoveryError(err)
	}
	defer archive.ReleaseNativeDecoded()
	archive, releaseArchive := archive.NativeDeltaAccountingScope()
	defer releaseArchive()
	ctx = archive.WithNativeDeltaAccounting(ctx)
	if !archive.HasNativeSourceHistories() {
		return nil, nil, nil, nil, nil
	}
	when, err := time.Parse(time.RFC3339Nano, profile.HistoryTime)
	if err != nil {
		return nil, nil, nil, nil, driver.ErrConflict
	}
	_, reports, err := nativeCurrentRecords(archive, when, ctx)
	if err != nil {
		return nil, nil, nil, nil, nativeDeltaRecoveryError(err)
	}
	var schemas []NativeSchemaReport
	if archive.Manifest().MainVersion != 72 {
		plan, planErr := buildPlanWithProfile(archive, &profile, when, ctx)
		if planErr != nil {
			return nil, nil, nil, nil, nativeDeltaRecoveryError(planErr)
		}
		schemas = plan.nativeSchemas
	}
	histories, err := nativeHistoryAccounting(archive)
	if err != nil {
		return nil, nil, nil, nil, nativeDeltaRecoveryError(err)
	}
	var policy *driver.NativeDeltaHistoryEvidence
	if profile.NativeDeltaHistory != nil {
		policy, err = nativeDeltaHistoryEvidence(archive, profile)
		if err != nil {
			return nil, nil, nil, nil, nativeDeltaRecoveryError(err)
		}
		if profile.HistoryMode == driver.RestoreHistoryModeSnapshot {
			markers, markerErr := reader.Get(ctx, []model.Ref{model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0))})
			want, encodeErr := json.Marshal(nativeSnapshotHistoryEvidence(policy))
			if markerErr != nil || encodeErr != nil || len(markers) != 1 || markers[0].Kind != "beads-archive" || !bytes.Equal(markers[0].Body, want) {
				return nil, nil, nil, nil, driver.ErrConflict
			}
			var original json.RawMessage
			for _, result := range receipt.Commit.Results {
				if result.Ref.ID == driver.OfflineHistoryEvidenceID {
					original = result.Record
				}
			}
			if !bytes.Equal(original, want) {
				return nil, nil, nil, nil, driver.ErrConflict
			}
		}
	}
	want, err := json.Marshal(nativeFamilyEvidence{Contract: archive.Manifest().SourceFamilyContract, RestoreTime: when.UTC().Format(time.RFC3339Nano), Families: reports, Schemas: schemas, Histories: histories, NativeDeltaHistory: policy})
	if err != nil {
		return nil, nil, nil, nil, driver.ErrConflict
	}
	var committed json.RawMessage
	for _, result := range receipt.Commit.Results {
		if result.Ref.ID == NativeFamilyReportID {
			committed = result.Record
		}
	}
	rows, err := reader.Get(ctx, []model.Ref{model.RefByID(NativeFamilyReportID, model.Natural64(0))})
	if err != nil || len(rows) != 1 || rows[0].Kind != "instance_config" || !bytes.Equal(rows[0].Body, want) || !bytes.Equal(committed, want) {
		return nil, nil, nil, nil, driver.ErrConflict
	}
	return reports, schemas, histories, policy, nil
}
