package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This is an explicitly constructed synthetic migration-prefix fixture, not
// an archive from a Beads process. Existing old-image values remain literal.
func native66ReceivingFixture(t *testing.T, project, category string) []byte {
	t.Helper()
	members := tarMembers(t, nativeTableArchiveFixture(t, project, category))
	var m dumpformat.Manifest
	var h dumpformat.NativeHistory
	if err := decodeNativeSchemaFixture(members["manifest.json"], &m); err != nil {
		t.Fatal(err)
	}
	if err := decodeNativeSchemaFixture(members[dumpformat.NativeHistoryMember], &h); err != nil {
		t.Fatal(err)
	}
	p, err := dumpformat.NativeSchemaForVersion(66, 26)
	if err != nil {
		t.Fatal(err)
	}
	h.SchemaProfile = &p
	m.MainVersion, m.IgnoredVersion = 66, 26
	transform := func(name string, row dumpformat.Row) bool {
		for _, absence := range p.Absences {
			if absence.Family != name {
				continue
			}
			if absence.Column == "" {
				return false
			}
			delete(row, absence.Column)
		}
		if name == "schema_migrations" || name == "ignored_schema_migrations" {
			if _, present := row["version"]; !present {
				return true
			}
			version, err := nativeSchemaFixtureVersion(row["version"])
			if err != nil {
				t.Fatal(err)
			}
			limit := uint64(66)
			if name == "ignored_schema_migrations" {
				limit = 26
			}
			return version <= limit
		}
		return true
	}
	for _, tables := range append([]map[string]dumpformat.NativeTable{h.Current}, nativeReceivingImageTables(h.Images)...) {
		for name, table := range tables {
			if !transform(name, dumpformat.Row{}) {
				delete(tables, name)
				continue
			}
			columns := []string{}
			for _, column := range table.Columns {
				keep := true
				for _, absence := range p.Absences {
					if absence.Family == name && absence.Column == column {
						keep = false
					}
				}
				if keep {
					columns = append(columns, column)
				}
			}
			rows := []dumpformat.Row{}
			for _, row := range table.Rows {
				if transform(name, row) {
					rows = append(rows, row)
				}
			}
			tables[name] = dumpformat.NativeTable{Columns: columns, Rows: rows}
		}
	}
	for i := range m.Kinds {
		kind := &m.Kinds[i]
		rows := []dumpformat.Record{}
		for _, line := range bytes.Split(bytes.TrimSpace(members[kind.File]), []byte{'\n'}) {
			if len(line) == 0 {
				continue
			}
			var row dumpformat.Record
			if err := decodeNativeSchemaFixture(line, &row); err != nil {
				t.Fatal(err)
			}
			if !transform(row.Source.Table, row.Source.Fields) {
				continue
			}
			if row.Kind == "beads-archive" {
				row.ID = row.Source.Table + "/" + hashBytes(encode(t, row.Source.Fields))
			}
			rows = append(rows, row)
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
		members[kind.File] = []byte{}
		for _, row := range rows {
			members[kind.File] = append(members[kind.File], encode(t, row)...)
		}
		kind.Count = len(rows)
	}
	sources := []dumpformat.Disposition{}
	for _, source := range m.Sources {
		if !transform(source.Family, dumpformat.Row{}) {
			continue
		}
		if table, ok := h.Current[source.Family]; ok {
			source.Rows = len(table.Rows)
		}
		sources = append(sources, source)
	}
	m.Sources = sources
	members["manifest.json"], members[dumpformat.NativeHistoryMember] = encode(t, m), encode(t, h)
	return nativeReceivingFixtureMembers(t, members)
}

func nativeReceivingImageTables(images []dumpformat.NativeHistoryImage) []map[string]dumpformat.NativeTable {
	result := []map[string]dumpformat.NativeTable{}
	for _, image := range images {
		result = append(result, image.Tables)
	}
	return result
}

func decodeNativeSchemaFixture(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	return decoder.Decode(target)
}

func nativeSchemaFixtureVersion(value any) (uint64, error) {
	return strconv.ParseUint(fmt.Sprint(value), 10, 64)
}

func nativeReceivingFixtureMembers(t *testing.T, members map[string][]byte) []byte {
	t.Helper()
	var manifest dumpformat.Manifest
	decode(t, members["manifest.json"], &manifest)
	for name, raw := range members {
		if name != "manifest.json" {
			manifest.Files[name] = dumpformat.File{Size: int64(len(raw)), SHA256: hashBytes(raw)}
		}
	}
	members["manifest.json"] = encode(t, manifest)
	return writeTarMembers(t, members)
}

func TestNativeSchema66ReceivingCountsOmissionsAndPreservesSources(t *testing.T) {
	left := native66ReceivingFixture(t, "pl", "active")
	right := native66ReceivingFixture(t, "qa", "done")
	data, err := dumpformat.Merge(bytes.NewReader(left), bytes.NewReader(right))
	if err != nil {
		t.Fatal(err)
	}
	original := append([]byte(nil), data...)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			store, lease, reopen := openIdentityTarget(t, engine)
			profile := boundNativeTableProfile(t, data, store, lease)
			report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
			if err != nil {
				t.Fatal("own-version receiving", err)
			}
			assertNative66Receiving(t, store, report)
			before := reverseState(t, store)
			capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := profile.SourcePlanes.Plan()
			if err != nil {
				t.Fatal(err)
			}
			checkpoint, err := capability.LoadCheckpoint(ctx, plan.Providers[0].Namespace)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile); !errors.Is(err, ErrTargetNotEmpty) {
				t.Fatal("repeated import bypassed the populated-target guard", err)
			}
			after, err := capability.LoadCheckpoint(ctx, plan.Providers[0].Namespace)
			if err != nil || !reflect.DeepEqual(checkpoint, after) || !reflect.DeepEqual(before, reverseState(t, store)) {
				t.Fatal("repeated import refusal changed core or namespace", err)
			}
			_, recoveredBefore, found, err := RecoverBoundSourceImport(ctx, store)
			if err != nil || !found || !reflect.DeepEqual(report.Receipt, recoveredBefore.Receipt) || !reflect.DeepEqual(report.NativeSchemas, recoveredBefore.NativeSchemas) || !reflect.DeepEqual(report.NativeFamilies, recoveredBefore.NativeFamilies) || !reflect.DeepEqual(report.SourcePlanes, recoveredBefore.SourcePlanes) || !reflect.DeepEqual(before, reverseState(t, store)) {
				t.Fatal("read-only receipt recovery changed destination or accounting", err)
			}
			store, lease = reopen()
			_, recovered, found, err := RecoverBoundSourceImport(ctx, store)
			if err != nil || !found || !reflect.DeepEqual(report.NativeSchemas, recovered.NativeSchemas) || !reflect.DeepEqual(report.Receipt, recovered.Receipt) {
				t.Fatal("own-version report/receipt lost on reopen", err)
			}
			recovered.Archive = report.Archive
			assertNative66Receiving(t, store, recovered)
			if !bytes.Equal(original, data) {
				t.Fatal("receiving changed input bytes")
			}
		})
	}
}

func assertNative66Receiving(t *testing.T, store driver.Store, report Report) {
	t.Helper()
	ctx := context.Background()
	if len(report.NativeSchemas) != 2 || len(report.NativeFamilies) != 60 || report.Receipt == nil {
		t.Fatal("own-version schema/family receipt missing")
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	rows, err := reader.Get(ctx, []model.Ref{model.RefByID(NativeFamilyReportID, model.Natural64(0)), model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0))})
	if err != nil || len(rows) != 2 {
		t.Fatal("persisted schema/history evidence missing", err)
	}
	var stored nativeFamilyEvidence
	var history driver.HistoryImportEvidence
	for _, row := range rows {
		if row.Ref.ID == NativeFamilyReportID {
			decode(t, row.Body, &stored)
		} else {
			decode(t, row.Body, &history)
		}
	}
	if !reflect.DeepEqual(stored.Schemas, report.NativeSchemas) || history.Source != driver.NativeTableHistorySource || !history.Complete {
		t.Fatal("stored report or strict native history changed")
	}
	restoreAt, err := time.Parse(time.RFC3339Nano, stored.RestoreTime)
	if err != nil || restoreAt.IsZero() {
		t.Fatal("committed lease clock missing", err)
	}
	for _, schema := range report.NativeSchemas {
		if schema.Profile.MainVersion != 66 || schema.Profile.IgnoredVersion != 26 || len(schema.Profile.Absences) != 7 || schema.SchemaDefaultCount != 0 || len(schema.PlugDefaults) != 1 || len(schema.TargetOmissions) != 2 {
			t.Fatal("source absences/defaults/omissions conflated", schema)
		}
		project := schema.Source.Project
		var allocator driver.OfflineAllocator
		decode(t, schema.PlugDefaults[0].Value, &allocator)
		if schema.PlugDefaults[0].Count != 1 || schema.PlugDefaults[0].Field != "target_allocator" || !reflect.DeepEqual(allocator, nativeReceivingExpectedHashAllocator(t, archive, project)) {
			t.Fatal("actual allocator default changed")
		}
		for _, omission := range schema.TargetOmissions {
			if omission.Family != dumpformat.DurableEntityTable || omission.TargetKind != "bead" || omission.Count != 1 || omission.Disposition != "absent-source-column-no-target-slot" || (omission.Column != "current_revision" && omission.Column != "participation_generation") {
				t.Fatal("target omissions not counted from admitted entities", omission)
			}
		}
		actual, err := reader.Get(ctx, []model.Ref{model.RefByID(project+"-42v", model.Natural64(0)), model.RefByID(importProjectRecordID(project), model.Natural64(0))})
		if err != nil || len(actual) != 2 {
			t.Fatal("target/default readback missing", err)
		}
		for _, row := range actual {
			if row.Kind == "instance_config" {
				var binding importProjectMetadata
				decode(t, row.Body, &binding)
				if !reflect.DeepEqual(binding.TargetAllocator, allocator) || binding.SourceIdentity == nil || *binding.SourceIdentity != schema.Source {
					t.Fatal("report default differs from committed external project binding")
				}
			} else {
				var doc pipeline.Document
				decode(t, row.Body, &doc)
				var source dumpformat.Row
				decode(t, doc.Metadata["beads.source_fields"], &source)
				for _, name := range []string{"current_revision", "participation_generation"} {
					if _, present := doc.Fields[name]; present {
						t.Fatal("unmodeled field default invented")
					}
					if _, present := source[name]; present {
						t.Fatal("schema absence inserted in source cells")
					}
				}
			}
		}
	}
	for _, scope := range report.Archive.NativeSourceHistories() {
		identity, err := nativeTableIdentity(scope)
		if err != nil {
			t.Fatal(err)
		}
		for table, value := range scope.Current {
			historyClass := ""
			for _, family := range scope.Sources {
				if family.Family == table {
					historyClass = family.History
				}
			}
			if table == "leases" {
				sourceRows := len(value.Rows)
				kept := []dumpformat.Row{}
				for _, row := range value.Rows {
					expires, err := time.Parse(time.RFC3339Nano, stringValue(row["lease_expires_at"]))
					if err != nil {
						t.Fatal("source lease deadline invalid", err)
					}
					if expires.After(restoreAt) {
						kept = append(kept, row)
					}
				}
				matches := 0
				for _, account := range report.NativeFamilies {
					if account.Source == identity && account.Family == "leases" {
						matches++
						if account.SourceRows != sourceRows || account.ImportedRows != len(kept) || account.ExpiredRows != sourceRows-len(kept) {
							t.Fatal("actual lease accounting differs from committed clock")
						}
					}
				}
				if matches != 1 {
					t.Fatal("lease accounting absent or duplicate")
				}
				value.Rows = kept
			}
			want, err := nativeTableRecord(identity, table, historyClass, value)
			if err != nil {
				t.Fatal(err)
			}
			got, err := reader.Get(ctx, []model.Ref{want.Ref})
			if err != nil || len(got) != 1 {
				t.Fatal("raw own-version current family lost", table, err)
			}
			assertNativeTableValue(t, got[0], identity, table, historyClass, value)
		}
		for _, image := range scope.Images {
			var revision model.Revision
			for _, mapping := range history.Mapping {
				if mapping.SourceStoreID == identity.StoreID && mapping.Project == identity.Project && mapping.SourceSequence == image.Sequence {
					revision = mapping.Revision
					if !reflect.DeepEqual(mapping.CommitIDs, []string{image.CommitID}) {
						t.Fatal("source commit identity changed")
					}
				}
			}
			if revision == "" {
				t.Fatal("source history image missing")
			}
			pinned, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &revision})
			if err != nil {
				t.Fatal(err)
			}
			for table, value := range image.Tables {
				want, err := nativeTableRecord(identity, table, "retained-native-commit", value)
				if err != nil {
					t.Fatal(err)
				}
				got, err := pinned.Get(ctx, []model.Ref{want.Ref})
				if err != nil || len(got) != 1 {
					t.Fatal("source history table missing", table, err)
				}
				assertNativeTableValue(t, got[0], identity, table, "retained-native-commit", value)
			}
			if err := pinned.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// These public and synthetic fixtures supply no adaptive allocator overrides.
// Keep main's explicit hash policy and original counters in the expected value,
// independently of the importer and the new schema-accounting implementation.
func nativeReceivingExpectedHashAllocator(t *testing.T, archive *dumpformat.Archive, project string) driver.OfflineAllocator {
	t.Helper()
	for _, record := range archive.Records("config") {
		if record.Project != "" && record.Project != project {
			continue
		}
		key, _ := record.Source.Fields["key"].(string)
		if key == "min_hash_length" || key == "max_hash_length" || key == "max_collision_prob" {
			t.Fatal("receiving fixture gained an allocator override; retain its literal policy")
		}
	}
	for _, source := range archive.Manifest().Projects {
		if source.Prefix != project {
			continue
		}
		want := driver.NewHashAllocator(project, 3, 8, 0.25)
		want.LastID = source.LastID
		if len(source.ChildCounters) != 0 {
			want.Children = make(map[string]uint64, len(source.ChildCounters))
			for parent, count := range source.ChildCounters {
				want.Children[parent] = count
			}
		}
		return want
	}
	t.Fatal("receiving source project is missing")
	return driver.OfflineAllocator{}
}

func TestNativeSchema66NamedGapRefusesBeforePublication(t *testing.T) {
	members := tarMembers(t, native66ReceivingFixture(t, "pl", "active"))
	var history dumpformat.NativeHistory
	decode(t, members[dumpformat.NativeHistoryMember], &history)
	missing := history.Images[1]
	history.Images = append(history.Images[:1], history.Images[2:]...)
	history.Complete = false
	history.Gaps = []dumpformat.NativeHistoryGap{{Sequence: missing.Sequence, CommitID: missing.CommitID, Family: dumpformat.DurableEntityTable, Reason: "historical-image-unavailable"}}
	members[dumpformat.NativeHistoryMember] = encode(t, history)
	data := nativeReceivingFixtureMembers(t, members)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			store, lease, _ := openIdentityTarget(t, engine)
			plan := nativeHistoryPlan(t, data, store, lease)
			profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: driver.StoreID(plan.SourceStoreID), Method: "offline.restore.import", IdempotencyKey: strings.Repeat("b", 32), ArchiveDigest: plan.ArchiveSHA256, HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: nativeFixtureTime.Format(time.RFC3339Nano)}
			before := reverseState(t, store)
			capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
			if err != nil {
				t.Fatal(err)
			}
			namespace := plan.Providers[0].Namespace
			checkpoint, err := capability.LoadCheckpoint(context.Background(), namespace)
			if err != nil {
				t.Fatal(err)
			}
			_, err = BindSourceImport(context.Background(), bytes.NewReader(data), profile, plan)
			var gap dumpformat.IncompleteHistoryError
			var policy driver.ErrRestoreHistoryRefused
			if !errors.As(err, &gap) || !errors.As(err, &policy) || gap.First != missing.Sequence || gap.CommitID != missing.CommitID || gap.Family != dumpformat.DurableEntityTable {
				t.Fatalf("source schema absence erased real history gap: %v", err)
			}
			after, err := capability.LoadCheckpoint(context.Background(), namespace)
			if err != nil || !reflect.DeepEqual(before, reverseState(t, store)) || !reflect.DeepEqual(checkpoint, after) {
				t.Fatal("gap refusal changed core or namespace", err)
			}
		})
	}
}
