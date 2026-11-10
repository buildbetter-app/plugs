package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

var nativeFixtureTime = time.Date(2026, 11, 9, 20, 0, 0, 0, time.UTC)

// This is declared synthetic evidence, built independently of a native runtime.
// Old physical schemas, an obsolete catalog, duplicate old table rows and
// retained pre-migration events deliberately cannot use portable replay.
func nativeTableArchiveFixture(t *testing.T, project, category string) []byte {
	t.Helper()
	members := tarMembers(t, nativeHistoryFixture(t, project, category))
	var manifest dumpformat.Manifest
	var portable dumpformat.SourceHistory
	decode(t, members["manifest.json"], &manifest)
	decode(t, members[dumpformat.SourceHistoryMember], &portable)
	schemas := map[string]struct {
		Columns []string `json:"columns"`
		History string   `json:"history"`
	}{}
	data, err := os.ReadFile("../../core/dumpformat/testdata/native-tables-synthetic.tar")
	if err != nil {
		t.Fatal(err)
	}
	reference, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal("declared synthetic native schema source", err)
	}
	referenceHistories := reference.NativeSourceHistories()
	if len(referenceHistories) != 1 {
		t.Fatal("synthetic schema source must have one original source")
	}
	for _, source := range referenceHistories[0].Sources {
		if table, ok := referenceHistories[0].Current[source.Family]; ok {
			schemas[source.Family] = struct {
				Columns []string `json:"columns"`
				History string   `json:"history"`
			}{table.Columns, source.History}
		}
	}
	current := map[string]dumpformat.NativeTable{}
	for name, schema := range schemas {
		current[name] = dumpformat.NativeTable{Columns: schema.Columns, Rows: []dumpformat.Row{}}
	}
	var archiveRows []dumpformat.Record
	for _, kind := range manifest.Kinds {
		for _, line := range bytes.Split(bytes.TrimSpace(members[kind.File]), []byte{'\n'}) {
			if len(line) == 0 {
				continue
			}
			var row dumpformat.Record
			decode(t, line, &row)
			table := current[row.Source.Table]
			fields := cloneRow(row.Source.Fields)
			if row.Kind == "bead" {
				fields["row_lock"], fields["is_blocked"] = nil, false
				labels := current["labels"]
				for _, label := range row.Body["labels"].([]any) {
					labels.Rows = append(labels.Rows, dumpformat.Row{"issue_id": row.ID, "label": label})
				}
				current["labels"] = labels
			}
			table.Rows = append(table.Rows, fields)
			current[row.Source.Table] = table
			if row.Kind == "beads-archive" {
				archiveRows = append(archiveRows, row)
			}
		}
	}
	leases := current["leases"]
	for i, expires := range []time.Time{nativeFixtureTime.Add(-time.Nanosecond), nativeFixtureTime, nativeFixtureTime.Add(time.Nanosecond)} {
		fields := dumpformat.Row{"issue_id": project + "-42v", "holder": []string{"expired", "at-boundary", "unexpired"}[i], "granted_at": "2026-11-01T00:00:00Z", "lease_expires_at": expires.Format(time.RFC3339Nano), "heartbeat_at": nil, "granted_node": "public fixture"}
		leases.Rows = append(leases.Rows, fields)
		archiveRows = append(archiveRows, dumpformat.Record{Kind: "beads-archive", ID: "leases/" + hashBytes(encode(t, fields)), Body: dumpformat.Row{"read_only": true, "redactable": true, "history_verified": false, "attribution_verified": false}, Source: dumpformat.Provenance{Table: "leases", Fields: fields}})
	}
	current["leases"] = leases
	canonicalRows := func(table dumpformat.NativeTable) dumpformat.NativeTable {
		sort.SliceStable(table.Rows, func(i, j int) bool { return bytes.Compare(encode(t, table.Rows[i]), encode(t, table.Rows[j])) < 0 })
		return table
	}
	for name, table := range current {
		current[name] = canonicalRows(table)
	}
	native := dumpformat.NativeHistory{Format: "beads-native-tables/1", StoreID: manifest.StoreID, Projects: []string{project}, Head: portable.Commits[len(portable.Commits)-1].ID, Commits: portable.Commits, Current: current, Ignore: dumpformat.NativeTable{Columns: []string{"ignored", "pattern"}, Rows: []dumpformat.Row{}}, Images: []dumpformat.NativeHistoryImage{}, Complete: true, Gaps: []dumpformat.NativeHistoryGap{}}
	for name, schema := range schemas {
		if schema.History == "unversioned-in-beads-by-design" {
			native.Ignore.Rows = append(native.Ignore.Rows, dumpformat.Row{"ignored": true, "pattern": name})
		}
	}
	native.Ignore = canonicalRows(native.Ignore)
	for i, commit := range native.Commits {
		tables := map[string]dumpformat.NativeTable{}
		for name, schema := range schemas {
			if schema.History == "versioned" {
				var table dumpformat.NativeTable
				decode(t, encode(t, current[name]), &table)
				tables[name] = table
			}
		}
		if i == 0 {
			tables["issues"] = dumpformat.NativeTable{Columns: []string{"id", "legacy_value"}, Rows: []dumpformat.Row{{"id": project + "-42v", "legacy_value": json.Number("9007199254740993")}}}
			tables["custom_statuses"] = dumpformat.NativeTable{Columns: []string{"category", "name"}, Rows: []dumpformat.Row{{"category": "wip", "name": "obsolete"}}}
			tables["old_native_table"] = dumpformat.NativeTable{Columns: []string{"id", "value"}, Rows: []dumpformat.Row{{"id": "equal", "value": nil}, {"id": "equal", "value": nil}}}
		}
		if i == 1 {
			old := cloneRow(current["events"].Rows[0])
			old["comment"] = "only in retained pre-migration history"
			tables["events"] = dumpformat.NativeTable{Columns: current["events"].Columns, Rows: []dumpformat.Row{old}}
		}
		native.Images = append(native.Images, dumpformat.NativeHistoryImage{Sequence: uint64(i + 1), CommitID: commit.ID, Tables: tables})
	}
	portable.Events = current["events"].Rows
	portable.Range = dumpformat.SourceHistoryRange{Status: "unverified", Reason: "historical-physical-schema-differs", Baseline: []dumpformat.Record{}, BaselineEvents: []dumpformat.Row{}, Frames: []dumpformat.SourceHistoryFrame{}, Gaps: []dumpformat.HistoryGap{}}
	manifest.History, manifest.SourceFamilyContract = "available-unverified", "beads-source-families/1"
	for i := range manifest.Sources {
		source := &manifest.Sources[i]
		if schema, ok := schemas[source.Family]; ok {
			source.History, source.Rows = schema.History, len(current[source.Family].Rows)
			if schema.History == "unversioned-in-beads-by-design" {
				source.Reason += "; unversioned in Beads by design"
			}
			if source.Family == "leases" || source.Family == "repo_mtimes" {
				source.Action = "archive"
			}
		}
	}
	sort.Slice(archiveRows, func(i, j int) bool { return archiveRows[i].ID < archiveRows[j].ID })
	var raw []byte
	for _, row := range archiveRows {
		raw = append(raw, encode(t, row)...)
	}
	members["records/beads-archive.ndjson"] = raw
	for i := range manifest.Kinds {
		if manifest.Kinds[i].Name == "beads-archive" {
			manifest.Kinds[i].Count = len(archiveRows)
		}
	}
	members[dumpformat.SourceHistoryMember] = encode(t, portable)
	members["history/native-tables.json"] = encode(t, native)
	for path, raw := range members {
		if path != "manifest.json" {
			manifest.Files[path] = dumpformat.File{SHA256: hashBytes(raw), Size: int64(len(raw))}
		}
	}
	members["manifest.json"] = encode(t, manifest)
	return writeTarMembers(t, members)
}

func mergedNativeTableFixture(t *testing.T) []byte {
	t.Helper()
	data, err := dumpformat.Merge(bytes.NewReader(nativeTableArchiveFixture(t, "pl", "active")), bytes.NewReader(nativeTableArchiveFixture(t, "qa", "done")))
	if err != nil {
		t.Fatal("synthetic raw native merge", err)
	}
	return data
}

func boundNativeTableProfile(t *testing.T, data []byte, store driver.Store, lease driver.ActiveLease) driver.RestoreImportProfile {
	t.Helper()
	plan := nativeHistoryPlan(t, data, store, lease)
	p := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: driver.StoreID(plan.SourceStoreID), Method: "offline.restore.import", IdempotencyKey: strings.Repeat("b", 32), ArchiveDigest: plan.ArchiveSHA256, HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: nativeFixtureTime.Format(time.RFC3339Nano)}
	p, err := BindSourceImport(context.Background(), bytes.NewReader(data), p, plan)
	if err != nil {
		t.Fatal("bind raw native history", err)
	}
	return p
}

func TestNativeTablesPublishEverySourceImageAndCurrentFamiliesAcrossDrivers(t *testing.T) {
	data := mergedNativeTableFixture(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			store, lease, reopen := openIdentityTarget(t, engine)
			p := boundNativeTableProfile(t, data, store, lease)
			report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, p)
			if err != nil {
				t.Fatal("publish raw native history", err)
			}
			assertNativeTablesReadback(t, store, p, report)
			store, lease = reopen()
			_, recovered, found, err := RecoverBoundSourceImport(ctx, store)
			if err != nil || !found || !reflect.DeepEqual(recovered.Receipt, report.Receipt) || !reflect.DeepEqual(recovered.NativeFamilies, report.NativeFamilies) {
				t.Fatal("native history original receipt/family accounting lost on reopen", err)
			}
			assertNativeTablesReadback(t, store, p, recovered)
		})
	}
}

func assertNativeTablesReadback(t *testing.T, store driver.Store, profile driver.RestoreImportProfile, report Report) {
	t.Helper()
	ctx := context.Background()
	if report.SourcePlanes == nil || report.Receipt == nil || !report.Restored.HistoryFloor.IsZero() || len(report.NativeFamilies) != 66 {
		t.Fatal("native joined metadata, accounting or receipt missing")
	}
	portable, _ := report.Archive.SourceHistory()
	for _, scoped := range portable.ProjectHistories {
		if scoped.Range.Status != "unverified" || scoped.Range.Complete || len(scoped.Range.Frames) != 0 {
			t.Fatal("native completeness promoted portable history")
		}
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := reader.Get(ctx, []model.Ref{model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0)), model.RefByID(ImportedOntologySourceID, model.Natural64(0))})
	if err != nil || len(rows) != 2 {
		t.Fatal("native history/catalog evidence unavailable", err)
	}
	var evidence driver.HistoryImportEvidence
	var ontology dumpformat.NativeStatusOntology
	for _, row := range rows {
		if row.Ref.ID == driver.OfflineHistoryEvidenceID {
			decode(t, row.Body, &evidence)
		} else {
			decode(t, row.Body, &ontology)
		}
	}
	if evidence.Source != driver.NativeTableHistorySource || ontology.Overlays["pl"].Statuses["shared"].Category != "active" || ontology.Overlays["qa"].Statuses["shared"].Category != "done" {
		t.Fatal("native source proof or per-project installed catalog lost")
	}
	assertNativeHistoryCoverageReadback(t, store, report.Archive, report, evidence)
	for _, family := range report.NativeFamilies {
		if family.History == "unversioned-in-beads-by-design" && family.Family == "events" && (family.SourceRows != 4 || family.ImportedRows != 4 || family.ExpiredRows != 0) {
			t.Fatal("current audit rows were dropped or assigned to history", family)
		}
		if family.Family == "leases" && (family.SourceRows != 3 || family.ImportedRows != 1 || family.ExpiredRows != 2) {
			t.Fatal("lease boundary or expired accounting differs", family)
		}
	}
	for _, scope := range report.Archive.NativeSourceHistories() {
		identity, _ := nativeTableIdentity(scope)
		for _, image := range scope.Images {
			var at model.Revision
			for _, mapping := range evidence.Mapping {
				if mapping.SourceStoreID == scope.StoreID && mapping.Project == identity.Project && mapping.SourceSequence == image.Sequence {
					if len(mapping.EventIDs) != 0 || !reflect.DeepEqual(mapping.CommitIDs, []string{image.CommitID}) {
						t.Fatal("native commit mapping fabricated audit assignment")
					}
					at = mapping.Revision
				}
			}
			if at == "" {
				t.Fatal("native image dropped from publication", image.Sequence)
			}
			pinned, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &at})
			if err != nil {
				t.Fatal(err)
			}
			assertNativeMissingImageTables(t, pinned, identity, scope, image)
			for table, value := range image.Tables {
				want, err := nativeTableRecord(identity, table, "retained-native-commit", value)
				if err != nil {
					t.Fatal(err)
				}
				got, err := pinned.Get(ctx, []model.Ref{want.Ref})
				if err != nil || len(got) != 1 {
					t.Fatal("native historical SQL cells/schema/duplicate rows differ", identity, image.Sequence, table, err)
				}
				assertNativeTableValue(t, got[0], identity, table, "retained-native-commit", value)
			}
			// A source commit never claims the current Plug business image.
			got, err := pinned.Get(ctx, []model.Ref{model.RefByID(identity.Project+"-42v", model.Natural64(0))})
			if err != nil || len(got) != 0 {
				t.Fatal("current portable body was grafted onto a source commit", err)
			}
			if err := pinned.Close(); err != nil {
				t.Fatal(err)
			}
		}
		for table, value := range scope.Current {
			history := ""
			for _, source := range scope.Sources {
				if source.Family == table {
					history = source.History
				}
			}
			if table == "leases" {
				filtered := []dumpformat.Row{}
				for _, row := range value.Rows {
					if row["holder"] == "unexpired" {
						filtered = append(filtered, row)
					}
				}
				value.Rows = filtered
			}
			want, _ := nativeTableRecord(identity, table, history, value)
			got, err := reader.Get(ctx, []model.Ref{want.Ref})
			if err != nil || len(got) != 1 {
				t.Fatal("current family rows/provenance differ", identity, table, err)
			}
			assertNativeTableValue(t, got[0], identity, table, history, value)
		}
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	_, recovered, found, err := RecoverBoundSourceImport(ctx, store)
	if err != nil || !found || !reflect.DeepEqual(recovered.NativeFamilies, report.NativeFamilies) || profile.HistoryTime != nativeFixtureTime.Format(time.RFC3339Nano) {
		t.Fatal("bound native clock/accounting recovery differs", err)
	}
}

func assertNativeMissingImageTables(t *testing.T, reader driver.Reader, identity dumpformat.SourceIdentity, scope dumpformat.NativeHistory, image dumpformat.NativeHistoryImage) {
	t.Helper()
	absent := []model.Ref{}
	names := map[string]dumpformat.NativeTable{}
	for _, other := range scope.Images {
		for name, value := range other.Tables {
			names[name] = value
		}
		if other.SchemaProfile != nil {
			for _, absence := range other.SchemaProfile.Absences {
				if absence.Column == "" {
					// This value is used only to derive the checked source/table
					// reference. No absent table body is written or returned.
					names[absence.Family] = dumpformat.NativeTable{}
				}
			}
		}
	}
	for name, value := range names {
		if _, present := image.Tables[name]; present {
			continue
		}
		row, err := nativeTableRecord(identity, name, "retained-native-commit", value)
		if err != nil {
			t.Fatal(err)
		}
		absent = append(absent, row.Ref)
	}
	if len(absent) == 0 {
		return
	}
	rows, err := reader.Get(context.Background(), absent)
	if err != nil || len(rows) != 0 {
		t.Fatal("native table absent at this commit leaked from another source image", image.Sequence, err)
	}
}

func assertNativeTableValue(t *testing.T, record driver.Record, identity dumpformat.SourceIdentity, table, history string, expected dumpformat.NativeTable) {
	t.Helper()
	var value nativeTableBody
	decode(t, record.Body, &value)
	if record.Kind != "beads-archive" || value.Source != identity || value.Table != table || value.History != history || !reflect.DeepEqual(value.Columns, expected.Columns) || !bytes.Equal(encode(t, value.Rows), encode(t, expected.Rows)) {
		t.Fatal("raw table identity, exact cells or duplicate multiplicity changed", identity, table)
	}
}

func TestNativeTablesIncompleteJoinedPublicationConservesCoreAndNamespace(t *testing.T) {
	data := mergedNativeTableFixture(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			store, lease, _ := openIdentityTarget(t, engine)
			p := boundNativeTableProfile(t, data, store, lease)
			archive, err := dumpformat.Read(bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
			if err != nil {
				t.Fatal(err)
			}
			plan, _ := p.SourcePlanes.Plan()
			namespace := plan.Providers[0].Namespace
			before := savedEmptyJointTarget(t, store, archive, p)
			checkpoint, err := capability.LoadCheckpoint(ctx, namespace)
			if err != nil {
				t.Fatal(err)
			}
			for _, fault := range []string{"missing-binding", "missing-checkpoint", "changed-envelope", "missing-receipt"} {
				t.Run(fault, func(t *testing.T) {
					if err := lease.Renew(ctx); err != nil {
						t.Fatal("renew native refusal test lease", err)
					}
					broken := &incompleteSourceStore{Store: store, WorkflowCheckpointStore: capability, fault: fault}
					if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), broken, lease, p); !errors.Is(err, driver.ErrUnsupported) {
						t.Fatal("native publication accepted incomplete joined state", err)
					}
					got, err := capability.LoadCheckpoint(ctx, namespace)
					if err != nil || !reflect.DeepEqual(checkpoint, got) || !reflect.DeepEqual(before, savedEmptyJointTarget(t, store, archive, p)) {
						t.Fatal("native publication refusal changed core/history/blobs/receipt/namespace", err)
					}
				})
			}
			if err := lease.Renew(ctx); err != nil {
				t.Fatal("renew native publication test lease", err)
			}
			if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, p); err != nil {
				t.Fatal("valid native publication failed after atomic refusals", err)
			}
		})
	}
}

func TestNativeTablesMissingVersionRefusesNamedGapWithoutPublication(t *testing.T) {
	members := tarMembers(t, nativeTableArchiveFixture(t, "pl", "active"))
	var history dumpformat.NativeHistory
	decode(t, members["history/native-tables.json"], &history)
	missing := history.Images[1]
	history.Images = append(history.Images[:1], history.Images[2:]...)
	history.Complete = false
	history.Gaps = []dumpformat.NativeHistoryGap{{Sequence: missing.Sequence, CommitID: missing.CommitID, Family: "issues", Reason: "historical-image-unavailable"}}
	members["history/native-tables.json"] = encode(t, history)
	var manifest dumpformat.Manifest
	decode(t, members["manifest.json"], &manifest)
	raw := members["history/native-tables.json"]
	manifest.Files["history/native-tables.json"] = dumpformat.File{SHA256: hashBytes(raw), Size: int64(len(raw))}
	members["manifest.json"] = encode(t, manifest)
	data := writeTarMembers(t, members)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			store, lease, _ := openIdentityTarget(t, engine)
			plan := nativeHistoryPlan(t, data, store, lease)
			before := reverseState(t, store)
			p := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: driver.StoreID(plan.SourceStoreID), Method: "offline.restore.import", IdempotencyKey: strings.Repeat("b", 32), ArchiveDigest: plan.ArchiveSHA256, HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: nativeFixtureTime.Format(time.RFC3339Nano)}
			_, err := BindSourceImport(context.Background(), bytes.NewReader(data), p, plan)
			var gap dumpformat.IncompleteHistoryError
			var policy driver.ErrRestoreHistoryRefused
			if !errors.As(err, &gap) || !errors.As(err, &policy) || gap.Project != "pl" || gap.StoreID != plan.SourceStoreID || gap.First != missing.Sequence || gap.Last != missing.Sequence || gap.CommitID != missing.CommitID || gap.Family != "issues" || gap.Reason != "historical-image-unavailable" || !reflect.DeepEqual(before, reverseState(t, store)) {
				t.Fatal("native missing image was backfilled, flattened or lost its source gap", err)
			}
		})
	}
}
