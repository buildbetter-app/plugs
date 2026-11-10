package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// The archive reader has already authenticated schema, images and row census.
// Refuse unverified native history before creating a disposable target. A
// genuine boundary is explicit; arbitrary gaps and present-history integrity
// failures remain refusals. BindSourceImport repeats this at publication.
func strictNativeHistory(archive *dumpformat.Archive) error {
	for _, scope := range archive.NativeSourceHistories() {
		if len(scope.Projects) != 1 || scope.Projects[0] == "" || scope.StoreID == "" {
			return fmt.Errorf("%w: ambiguous native source/project", driver.ErrUnsupported)
		}
		if scope.HistoryCoverage != nil {
			if err := dumpformat.RequireVerifiedNativeHistory(scope); err != nil {
				return fmt.Errorf("%w: %w", driver.ErrRestoreHistoryRefused{Mode: driver.RestoreHistoryModeFull, History: archive.Manifest().History, Reason: driver.RestoreHistoryReasonAvailableUnverified}, err)
			}
			continue
		}
		if !scope.Complete || len(scope.Gaps) != 0 || len(scope.Images) == 0 || len(scope.Images) != len(scope.Commits) {
			gap := dumpformat.IncompleteHistoryError{StoreID: scope.StoreID, Project: scope.Projects[0], Reason: "native immutable history incomplete"}
			if len(scope.Gaps) != 0 {
				first := scope.Gaps[0]
				gap.First, gap.Last, gap.Reason, gap.CommitID, gap.Family = first.Sequence, first.Sequence, first.Reason, first.CommitID, first.Family
			}
			return fmt.Errorf("%w: %w", driver.ErrRestoreHistoryRefused{Mode: driver.RestoreHistoryModeFull, History: archive.Manifest().History, Reason: driver.RestoreHistoryReasonAvailableUnverified}, gap)
		}
	}
	return nil
}

type nativeBody struct {
	Source  dumpformat.SourceIdentity `json:"source_identity"`
	Table   string                    `json:"table"`
	History string                    `json:"history"`
	Columns []string                  `json:"columns"`
	Rows    []dumpformat.Row          `json:"rows"`
}

func tableRef(source dumpformat.SourceIdentity, table, history string) (model.Ref, error) {
	tuple, err := json.Marshal([]string{source.StoreID, source.Project, table})
	if err != nil {
		return model.Ref{}, err
	}
	prefix := "source/native-current/"
	if history == "retained-native-commit" {
		prefix = driver.NativeTableHistoryRecordPrefix
	}
	return model.RefByID(prefix+digest(tuple), model.Natural64(0)), nil
}

func checkTable(ctx context.Context, reader driver.Reader, source dumpformat.SourceIdentity, table, history string, value dumpformat.NativeTable) error {
	ref, err := tableRef(source, table, history)
	if err != nil {
		return err
	}
	rows, err := reader.Get(ctx, []model.Ref{ref})
	if err != nil {
		return err
	}
	if len(rows) != 1 || rows[0].Kind != "beads-archive" {
		return fmt.Errorf("native table %s: committed record missing", table)
	}
	var actual nativeBody
	if err := decode(rows[0].Body, &actual); err != nil {
		return err
	}
	want := nativeBody{source, table, history, value.Columns, value.Rows}
	if !reflect.DeepEqual(actual, want) {
		return fmt.Errorf("native table %s: columns/cells/multiplicity differ", table)
	}
	return nil
}

func verifyNative(ctx context.Context, store driver.Store, profile driver.RestoreImportProfile, report converterroundtrip.Report, input []byte) (verification, error) {
	var proof verification
	archive, err := dumpformat.Read(bytes.NewReader(input))
	if err != nil {
		return proof, err
	}
	when, err := time.Parse(time.RFC3339Nano, profile.HistoryTime)
	if err != nil {
		return proof, err
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &report.Restored.Head})
	if err != nil {
		return proof, err
	}
	defer reader.Close()
	retained, err := reader.Blob(ctx, driver.Hash(archive.SHA256()))
	if err != nil {
		return proof, err
	}
	if !bytes.Equal(retained, input) {
		return proof, fmt.Errorf("committed original archive differs")
	}
	if report.Archive == nil || !reflect.DeepEqual(report.Archive.NativeSourceHistories(), archive.NativeSourceHistories()) {
		return proof, fmt.Errorf("recovered per-source native images or schema annotations differ")
	}
	proof.ArchiveBlob = true
	plan, err := profile.SourcePlanes.Plan()
	if err != nil {
		return proof, err
	}
	dispositions := map[string]driver.SourcePlaneDispositionV1{}
	for _, d := range plan.Dispositions {
		dispositions[d.Key] = d
	}
	entities := map[driver.SourceEntityKey]driver.SourcePlaneEntityV1{}
	for _, e := range plan.Entities {
		entities[e.Source] = e
	}
	for _, source := range archive.Records("bead") {
		entity := entities[driver.SourceEntityKey{Project: source.Project, Plane: source.Source.Table, ID: source.ID}]
		if entity.TargetKind != "bead" {
			continue
		}
		rows, err := reader.Get(ctx, []model.Ref{model.RefByID(entity.TargetID, model.Natural64(0))})
		if err != nil || len(rows) != 1 {
			return proof, fmt.Errorf("committed opaque target missing: %w", driver.ErrConflict)
		}
		var doc struct {
			Metadata map[string]json.RawMessage `json:"metadata"`
		}
		if err := json.Unmarshal(rows[0].Body, &doc); err != nil {
			return proof, err
		}
		for _, rule := range dispositions[entity.DispositionKey].Fields {
			if rule.Disposition != "opaque-metadata" {
				continue
			}
			want, err := json.Marshal(source.Source.Fields[rule.Source])
			if err != nil || !bytes.Equal(doc.Metadata[rule.Target], want) {
				return proof, fmt.Errorf("%w: committed opaque metadata differs", driver.ErrConflict)
			}
			proof.OpaqueMetadataFields++
		}
	}
	rows, err := reader.Get(ctx, []model.Ref{model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0))})
	if err != nil {
		return proof, err
	}
	if len(rows) != 1 {
		return proof, fmt.Errorf("committed native history evidence missing")
	}
	var evidence driver.HistoryImportEvidence
	if err := decode(rows[0].Body, &evidence); err != nil {
		return proof, err
	}
	wantComplete, images := true, uint64(0)
	var boundaries []driver.NativeHistoryBoundary
	var histories []converterroundtrip.NativeHistoryReport
	for _, scope := range archive.NativeSourceHistories() {
		images += uint64(len(scope.Images))
		if scope.HistoryCoverage != nil {
			wantComplete = false
			boundaries = append(boundaries, driver.NativeHistoryBoundary{SourceStoreID: scope.StoreID, Project: scope.Projects[0], VerifiedCommitCount: uint64(len(scope.Images)), BoundarySHA256: dumpformat.NativeHistoryCoverageSHA256(*scope.HistoryCoverage)})
		}
		histories = append(histories, converterroundtrip.NativeHistoryReport{Source: dumpformat.SourceIdentity{StoreID: scope.StoreID, Project: scope.Projects[0]}, Complete: scope.Complete, VerifiedCommitCount: uint64(len(scope.Images)), Coverage: scope.HistoryCoverage})
	}
	if wantComplete {
		histories = nil
	}
	if evidence.Source != driver.NativeTableHistorySource || evidence.Mode != driver.RestoreHistoryModeFull || evidence.Complete != wantComplete || len(evidence.Gaps) != 0 || evidence.First != 1 || evidence.Head != images || len(evidence.Mapping) != int(images) || !evidence.Floor.IsZero() || !reflect.DeepEqual(evidence.NativeBoundaries, boundaries) || !reflect.DeepEqual(report.NativeHistories, histories) {
		return proof, fmt.Errorf("committed native history coverage differs from the authenticated source")
	}
	for _, scope := range archive.NativeSourceHistories() {
		source := dumpformat.SourceIdentity{StoreID: scope.StoreID, Project: scope.Projects[0]}
		seen := map[string]bool{}
		for _, family := range scope.Sources {
			// The original manifest also contains non-native files/dispositions.
			// Only classified native families have current SQL table images.
			if family.History == "" {
				continue
			}
			if seen[family.Family] {
				return proof, fmt.Errorf("native current family %s duplicated", family.Family)
			}
			seen[family.Family] = true
			value, present := scope.Current[family.Family]
			if !present {
				return proof, fmt.Errorf("current family %s absent", family.Family)
			}
			if family.Family == "leases" {
				kept := []dumpformat.Row{}
				for _, row := range value.Rows {
					text, ok := row["lease_expires_at"].(string)
					if !ok {
						return proof, fmt.Errorf("lease expiry is not text")
					}
					expires, err := time.Parse(time.RFC3339Nano, text)
					if err != nil {
						return proof, err
					}
					if expires.After(when) {
						kept = append(kept, row)
					}
				}
				value.Rows = kept
			}
			if err := checkTable(ctx, reader, source, family.Family, family.History, value); err != nil {
				return proof, err
			}
			proof.CurrentFamilies++
			proof.CurrentRows += len(value.Rows)
		}
		if len(seen) != len(scope.Current) {
			return proof, fmt.Errorf("native current family census incomplete")
		}
		// The source family set can change across migrations. Check both the
		// tables present in this image and the absence of tables found only in
		// other images, so a dropped table cannot leak into later revisions.
		historical := map[string]model.Ref{}
		for _, image := range scope.Images {
			for name := range image.Tables {
				ref, err := tableRef(source, name, "retained-native-commit")
				if err != nil {
					return proof, err
				}
				historical[name] = ref
			}
			if image.SchemaProfile != nil {
				for _, absence := range image.SchemaProfile.Absences {
					if absence.Column != "" {
						continue
					}
					ref, err := tableRef(source, absence.Family, "retained-native-commit")
					if err != nil {
						return proof, err
					}
					historical[absence.Family] = ref
				}
			}
		}
		for _, image := range scope.Images {
			var revision model.Revision
			matches := 0
			for _, mapping := range evidence.Mapping {
				if mapping.SourceStoreID == source.StoreID && mapping.Project == source.Project && mapping.SourceSequence == image.Sequence {
					matches++
					revision = mapping.Revision
					if !reflect.DeepEqual(mapping.CommitIDs, []string{image.CommitID}) {
						return proof, fmt.Errorf("native image commit identity differs")
					}
				}
			}
			if matches != 1 || revision == "" {
				return proof, fmt.Errorf("native image revision missing or duplicated")
			}
			pinned, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &revision})
			if err != nil {
				return proof, err
			}
			absent := []model.Ref{}
			for name, ref := range historical {
				if _, present := image.Tables[name]; !present {
					absent = append(absent, ref)
				}
			}
			if len(absent) != 0 {
				rows, err := pinned.Get(ctx, absent)
				if err != nil {
					_ = pinned.Close()
					return proof, err
				}
				if len(rows) != 0 {
					_ = pinned.Close()
					return proof, fmt.Errorf("native image contains a table absent from the source image: %w", driver.ErrConflict)
				}
			}
			for name, value := range image.Tables {
				if err := checkTable(ctx, pinned, source, name, "retained-native-commit", value); err != nil {
					_ = pinned.Close()
					return proof, err
				}
				proof.HistoryTables++
				proof.HistoryRows += len(value.Rows)
			}
			if err := pinned.Close(); err != nil {
				return proof, err
			}
			proof.HistoryImages++
		}
	}
	return proof, nil
}
