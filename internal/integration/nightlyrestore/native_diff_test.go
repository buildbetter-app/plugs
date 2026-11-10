package nightlyrestore_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type nativeImageCensus struct {
	Sequence uint64                                `json:"sequence"`
	Schema   *dumpformat.NativeCommitSchemaProfile `json:"schema_profile"`
	Tables   map[string]string                     `json:"table_sha256"`
}

type nativeRestoreCensus struct {
	Head           model.Revision                           `json:"head"`
	Floor          model.Revision                           `json:"floor"`
	Counts         map[string]int                           `json:"current_counts"`
	IDs            []string                                 `json:"current_ids"`
	Records        map[string]string                        `json:"source_checked_record_sha256"`
	Dependencies   []string                                 `json:"dependencies"`
	SourceCounters []dumpformat.Project                     `json:"source_counters"`
	TargetCounters []driver.OfflineAllocator                `json:"target_allocators"`
	Archive        string                                   `json:"archive_sha256"`
	Members        map[string]string                        `json:"archive_member_sha256"`
	Events         string                                   `json:"events_attachment_sha256"`
	History        driver.HistoryImportEvidence             `json:"history_evidence"`
	Images         []nativeImageCensus                      `json:"verified_images"`
	Versions       map[string][]versionSample               `json:"native_table_versions"`
	Changesets     []model.Changeset                        `json:"changesets"`
	Families       []converterroundtrip.NativeFamilyReport  `json:"current_families"`
	Schemas        []converterroundtrip.NativeSchemaReport  `json:"source_schemas"`
	Histories      []converterroundtrip.NativeHistoryReport `json:"source_histories"`
}

func nativeTableID(t *testing.T, scope dumpformat.NativeHistory, name, prefix string) string {
	t.Helper()
	return prefix + digest(jsonBytes(t, []string{scope.StoreID, scope.Projects[0], name}))
}

func checkNativeTable(t *testing.T, row driver.Record, scope dumpformat.NativeHistory, name, history string, value dumpformat.NativeTable) {
	t.Helper()
	if row.Kind != "beads-archive" {
		t.Fatal("native table evidence missing", name)
	}
	sameJSON(t, "exact native schema/cells/multiplicity "+name, json.RawMessage(row.Body), map[string]any{
		"source_identity": dumpformat.SourceIdentity{StoreID: scope.StoreID, Project: scope.Projects[0]},
		"table":           name, "history": history, "columns": value.Columns, "rows": value.Rows,
	})
}

func nativeSourceCensus(t *testing.T, store driver.Store, a *dumpformat.Archive, p driver.RestoreImportProfile, published converterroundtrip.Report) nativeRestoreCensus {
	t.Helper()
	ctx := context.Background()
	bound, recovered, found, err := converterroundtrip.RecoverBoundSourceImport(ctx, store)
	if err != nil || !found || recovered.Receipt == nil || recovered.Archive == nil {
		t.Fatal("native import receipt recovery failed", err)
	}
	sameJSON(t, "recovered native admission", bound, p)
	sameJSON(t, "recovered native receipt", recovered.Receipt, published.Receipt)
	sameJSON(t, "recovered native family report", recovered.NativeFamilies, published.NativeFamilies)
	sameJSON(t, "recovered native schema report", recovered.NativeSchemas, published.NativeSchemas)
	sameJSON(t, "recovered native history report", recovered.NativeHistories, published.NativeHistories)
	sameJSON(t, "recovered source-plane commitment", recovered.SourcePlanes, published.SourcePlanes)
	sameJSON(t, "retained own-version native input", recovered.Archive.NativeSourceHistories(), a.NativeSourceHistories())
	if !bytes.Equal(recovered.Archive.TarBytes(), a.TarBytes()) {
		t.Fatal("native recovery changed original archive bytes")
	}
	scope := a.NativeSourceHistories()[0]
	meta, err := store.Meta(ctx)
	if err != nil || meta.StoreID != p.SourceStoreID || meta.Head != model.Natural64(uint64(len(scope.Images)+1)) || !meta.HistoryFloor.IsZero() {
		t.Fatal("native import invented revisions, a baseline or a floor", err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows := indexRows(t, allRows(t, r))
	got := nativeRestoreCensus{Head: meta.Head, Floor: meta.HistoryFloor, Counts: map[string]int{}, Records: map[string]string{}, Dependencies: []string{}, SourceCounters: a.Manifest().Projects, Members: map[string]string{}, Archive: a.SHA256(), Families: recovered.NativeFamilies, Schemas: recovered.NativeSchemas, Histories: recovered.NativeHistories}
	for id, row := range rows {
		got.IDs = append(got.IDs, id)
		got.Counts[row.Kind]++
	}
	sort.Strings(got.IDs)
	nativeDecode(t, rows[driver.OfflineHistoryEvidenceID].Body, &got.History)
	var accounting struct {
		RestoreTime string                                   `json:"restore_time"`
		Families    []converterroundtrip.NativeFamilyReport  `json:"families"`
		Schemas     []converterroundtrip.NativeSchemaReport  `json:"source_schemas"`
		Histories   []converterroundtrip.NativeHistoryReport `json:"source_histories"`
	}
	nativeDecode(t, rows[converterroundtrip.NativeFamilyReportID].Body, &accounting)
	if accounting.RestoreTime != p.HistoryTime {
		t.Fatal("committed native accounting lost the admitted restore time")
	}
	sameJSON(t, "committed native families", accounting.Families, got.Families)
	sameJSON(t, "committed native schemas", accounting.Schemas, got.Schemas)
	sameJSON(t, "committed native boundary report", accounting.Histories, got.Histories)
	wantHistory := driver.HistoryImportEvidence{Source: driver.NativeTableHistorySource, Mode: driver.RestoreHistoryModeFull, Complete: scope.Complete, First: 1, Head: uint64(len(scope.Images)), Floor: model.Natural64(0), Gaps: []driver.SourceHistoryGap{}, Mapping: []driver.SourceImportRevision{}}
	for _, image := range scope.Images {
		wantHistory.Mapping = append(wantHistory.Mapping, driver.SourceImportRevision{SourceStoreID: scope.StoreID, Project: scope.Projects[0], SourceSequence: image.Sequence, Sequence: image.Sequence, Revision: model.Natural64(image.Sequence), CommitIDs: []string{image.CommitID}, EventIDs: []string{}})
	}
	if scope.HistoryCoverage != nil {
		wantHistory.NativeBoundaries = []driver.NativeHistoryBoundary{{SourceStoreID: scope.StoreID, Project: scope.Projects[0], VerifiedCommitCount: uint64(len(scope.Images)), BoundarySHA256: digest(jsonBytes(t, scope.HistoryCoverage))}}
		sameJSON(t, "original boundary and verified-count report", got.Histories, []converterroundtrip.NativeHistoryReport{{Source: dumpformat.SourceIdentity{StoreID: scope.StoreID, Project: scope.Projects[0]}, Complete: false, VerifiedCommitCount: uint64(len(scope.Images)), Coverage: scope.HistoryCoverage}})
	} else if len(got.Histories) != 0 {
		t.Fatal("complete legacy original gained a boundary report")
	}
	sameJSON(t, "exact native mapping without invented event assignments", got.History, wantHistory)
	for _, part := range append([]string{"manifest.json"}, sortedNativeMembers(a)...) {
		raw, _ := a.Member(part)
		blob, err := r.Blob(ctx, driver.Hash(digest(raw)))
		if err != nil || !bytes.Equal(blob, raw) {
			t.Fatal("original native member bytes lost", part, err)
		}
		got.Members[part] = digest(blob)
	}
	blob, err := r.Blob(ctx, driver.Hash(a.SHA256()))
	if err != nil || !bytes.Equal(blob, a.TarBytes()) {
		t.Fatal("original native archive blob lost", err)
	}
	attachment, _ := a.Member(dumpformat.SourceHistoryMember)
	got.Events = digest(attachment)
	var portable dumpformat.SourceHistory
	nativeDecode(t, attachment, &portable)
	if portable.Range.Complete || portable.Range.Status != "unverified" || len(portable.Range.Frames) != 0 || len(portable.Events) != len(scope.Current["events"].Rows) {
		t.Fatal("current events became verified portable history")
	}
	if scope.HistoryCoverage != nil && (got.Events != scope.HistoryCoverage.EventsAttachment.SHA256 || uint64(len(portable.Events)) != scope.HistoryCoverage.EventsAttachment.Rows || portable.Range.Reason != "native-history-boundary") {
		t.Fatal("unavailable-history events attachment changed")
	}
	familyHistory := map[string]string{}
	for _, family := range scope.Sources {
		value, present := scope.Current[family.Family]
		if !present {
			continue
		}
		if _, duplicate := familyHistory[family.Family]; duplicate {
			t.Fatal("source current family census has a duplicate", family.Family)
		}
		familyHistory[family.Family] = family.History
		if family.Family == "leases" && len(value.Rows) != 0 {
			t.Fatal("these originals do not qualify positive lease filtering")
		}
		id := nativeTableID(t, scope, family.Family, "source/native-current/")
		checkNativeTable(t, rows[id], scope, family.Family, family.History, value)
		got.Records[id] = digest(rows[id].Body)
	}
	if len(got.Records) != len(scope.Current) || len(got.Families) != len(scope.Current) {
		t.Fatal("current native family census is incomplete")
	}
	for _, family := range got.Families {
		value, present := scope.Current[family.Family]
		history, unreported := familyHistory[family.Family]
		if !present || !unreported || family.History != history || family.SourceRows != len(value.Rows) || family.ImportedRows != len(value.Rows) || family.ExpiredRows != 0 || family.Source != (dumpformat.SourceIdentity{StoreID: scope.StoreID, Project: scope.Projects[0]}) {
			t.Fatal("native source/imported/expired counts differ")
		}
		delete(familyHistory, family.Family)
	}
	if len(familyHistory) != 0 {
		t.Fatal("native current family report omitted a source family")
	}
	nativeBusinessCensus(t, r, rows, a, &got)
	states := []map[string]driver.Record{{}}
	for _, image := range scope.Images {
		revision := model.Natural64(image.Sequence)
		pinned, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &revision})
		if err != nil {
			t.Fatal(err)
		}
		state := indexRows(t, allRows(t, pinned))
		if err := pinned.Close(); err != nil {
			t.Fatal(err)
		}
		if len(state) != len(image.Tables) {
			t.Fatal("native source revision gained missing families or current business records", image.Sequence)
		}
		entry := nativeImageCensus{Sequence: image.Sequence, Schema: image.SchemaProfile, Tables: map[string]string{}}
		for name, value := range image.Tables {
			id := nativeTableID(t, scope, name, driver.NativeTableHistoryRecordPrefix)
			checkNativeTable(t, state[id], scope, name, "retained-native-commit", value)
			entry.Tables[name] = digest(state[id].Body)
		}
		// Exact cardinality and identity checks above also prove zero stored
		// rows for absent-by-source-schema families, including removed events.
		states = append(states, state)
		got.Images = append(got.Images, entry)
	}
	zero := model.Natural64(0)
	empty, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &zero})
	if err != nil {
		t.Fatal(err)
	}
	if len(allRows(t, empty)) != 0 {
		t.Fatal("Plug revision zero gained fabricated pre-boundary source history")
	}
	if err := empty.Close(); err != nil {
		t.Fatal(err)
	}
	when, err := time.Parse(time.RFC3339Nano, p.HistoryTime)
	if err != nil {
		t.Fatal(err)
	}
	var changes map[model.Revision][]model.ChangeRecord
	got.Versions, changes = checkNativeVersions(t, r, states, p, when)
	changes[meta.Head] = []model.ChangeRecord{}
	for id, row := range rows {
		if strings.HasPrefix(id, driver.NativeTableHistoryRecordPrefix) {
			continue
		}
		if row.Revision != meta.Head {
			t.Fatal("current row was backdated into an immutable source image", id)
		}
		changes[meta.Head] = append(changes[meta.Head], model.ChangeRecord{Ref: row.Ref, Op: model.OpCreate, FromRevision: zero, ChangedPaths: []string{""}})
	}
	sort.Slice(changes[meta.Head], func(i, j int) bool { return changes[meta.Head][i].Ref.ID < changes[meta.Head][j].Ref.ID })
	principal, err := driver.PrincipalKey(p.Admission.Actor)
	if err != nil {
		t.Fatal(err)
	}
	tail, err := r.ChangesetByKey(ctx, principal, p.AgentLabel(), p.Method, p.IdempotencyKey)
	if err != nil || tail == nil {
		t.Fatal("native keyed restore changeset missing", err)
	}
	got.Changesets, _ = checkNativeChangesets(t, r, *tail, changes, meta, p, when)
	return got
}

func sortedNativeMembers(a *dumpformat.Archive) []string {
	names := []string{}
	for name := range a.Manifest().Files {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func nativeBusinessCensus(t *testing.T, r driver.Reader, rows map[string]driver.Record, a *dumpformat.Archive, got *nativeRestoreCensus) {
	t.Helper()
	wantDependencies := []string{}
	for _, source := range a.Records("bead") {
		row, present := rows[source.ID]
		if !present || row.Kind != "bead" {
			t.Fatal("current native business identity missing", source.ID)
		}
		doc, err := pipeline.DecodeDocument(row.Body)
		if err != nil || doc.ID != source.ID || doc.Project != source.Project || doc.Type != source.Body["type"] {
			t.Fatal("native business identity/type changed", err)
		}
		sameJSON(t, "all current native business source fields", doc.Metadata["beads.source_fields"], source.Source.Fields)
		sameJSON(t, "exact native status name", doc.Fields["status"], source.Source.Fields["status"])
		sameJSON(t, "exact native labels", append([]string{}, doc.Labels...), source.Body["labels"])
		created, err := time.Parse(time.RFC3339Nano, fmt.Sprint(source.Source.Fields["created_at"]))
		comments := 0
		for _, comment := range a.Records("comment") {
			if comment.Body["issue_id"] == source.ID {
				comments++
			}
		}
		if err != nil || doc.CreatedAt == nil || !doc.CreatedAt.Equal(created) || doc.Lease != nil || len(doc.Attachments) != 0 || len(doc.Comments) != comments {
			t.Fatal("native creation time or current owned fields differ", err)
		}
		for _, name := range []string{"title", "description", "priority", "status"} {
			if value := source.Body[name]; value != nil {
				sameJSON(t, "current native business field "+name, doc.Fields[name], value)
			}
		}
		for _, link := range doc.Links {
			got.Dependencies = append(got.Dependencies, source.ID+"/"+link.Kind+"/"+link.To.ID)
		}
		got.Records[source.ID] = digest(canonicalBody(t, row, model.PrincipalRef{Kind: "user", ID: "n1-test-restore-admin"}))
	}
	if got.Counts["bead"] != len(a.Records("bead")) {
		t.Fatal("native business count gained or lost IDs")
	}
	for _, edge := range a.Records("edge") {
		kind := fmt.Sprint(edge.Body["type"])
		if kind == "related" {
			kind = "relates-to"
		}
		wantDependencies = append(wantDependencies, fmt.Sprint(edge.Body["from"])+"/"+kind+"/"+fmt.Sprint(edge.Body["to"]))
	}
	sort.Strings(wantDependencies)
	sort.Strings(got.Dependencies)
	sameJSON(t, "native current dependency census", got.Dependencies, wantDependencies)
	allocators, ok := r.(driver.AllocatorReader)
	if !ok {
		t.Fatal("native allocator reader missing")
	}
	for _, source := range a.Manifest().Projects {
		actual, err := allocators.LoadAllocator(context.Background(), source.Prefix)
		if err != nil {
			t.Fatal(err)
		}
		want := driver.NewHashAllocator(source.Prefix, 3, 8, 0.25)
		want.LastID, want.Children = source.LastID, source.ChildCounters
		sameJSON(t, "native target hash policy and source counters", actual, want)
		var project struct {
			Metadata map[string]json.RawMessage `json:"metadata"`
			Target   driver.OfflineAllocator    `json:"targetAllocator"`
		}
		nativeDecode(t, rows["instance_config/restore-import/project/"+source.Prefix].Body, &project)
		sameJSON(t, "native retained original source counters", project.Metadata["plug.restore.sourceAllocator"], source)
		sameJSON(t, "native committed target allocator", project.Target, actual)
		got.TargetCounters = append(got.TargetCounters, actual)
	}
}

func checkNightlyNativeInput(t *testing.T, a *dumpformat.Archive, bounded bool) {
	t.Helper()
	scopes := a.NativeSourceHistories()
	if len(scopes) != 1 || a.Manifest().MainVersion != 66 || a.Manifest().IgnoredVersion != 26 {
		t.Fatal("original native source/schema census changed")
	}
	scope := scopes[0]
	sameJSON(t, "unchanged public source project", scope.Projects, []string{"public"})
	wantImages, wantTables, wantRows, wantAbsences, wantEvents := 20, 372, 1351, 542, 1
	wantVersions := map[string]int{"cursor-absent": 2, "39": 5, "40": 2, "41": 1, "43": 1, "54": 1, "55": 1, "61": 2, "62": 1, "63": 1, "64": 1, "66": 2}
	if bounded {
		wantImages, wantTables, wantRows, wantAbsences, wantEvents = 13, 266, 929, 354, 2
		wantVersions = map[string]int{"39": 5, "40": 2, "41": 1, "61": 2, "62": 1, "66": 2}
		coverage := scope.HistoryCoverage
		if coverage == nil || coverage.VerifiedCommitCount != 13 || len(coverage.Boundaries) != 1 || coverage.EventsAttachment.Rows != 2 {
			t.Fatal("original genuine unavailable frontier lost its explicit verified coverage")
		}
		boundary := coverage.Boundaries[0]
		if boundary.Reason != "ghost_parent" || boundary.CommitOrdinalFromHead != 14 || boundary.CommitSchemaVersion != nil || len(boundary.SQLDoltCodes) != 0 {
			t.Fatal("genuine ghost boundary gained fabricated schema or different coordinates")
		}
	} else if scope.HistoryCoverage != nil {
		t.Fatal("complete original gained unavailable history")
	}
	if scope.Complete == bounded || len(scope.Gaps) != 0 || len(scope.Images) != wantImages || len(scope.Commits) != wantImages || len(scope.Current) != 30 || len(scope.Current["events"].Rows) != wantEvents {
		t.Fatal("original native completeness/current/image/event census changed")
	}
	currentRows := 0
	for _, family := range scope.Current {
		currentRows += len(family.Rows)
	}
	if currentRows != 109 {
		t.Fatal("original current native row census changed")
	}
	versions := map[string]int{}
	tables, rows, absences := 0, 0, 0
	for _, image := range scope.Images {
		if image.SchemaProfile == nil {
			t.Fatal("native image lost its own schema profile")
		}
		key := "cursor-absent"
		if v := image.SchemaProfile.MainVersion; v != nil {
			key = fmt.Sprint(*v)
		}
		versions[key]++
		tables += len(image.Tables)
		absences += len(image.SchemaProfile.Absences)
		for _, table := range image.Tables {
			rows += len(table.Rows)
		}
		if key == "62" {
			if _, present := image.Tables["events"]; present {
				t.Fatal("immutable post-0062 root gained the current events attachment")
			}
		}
	}
	sameJSON(t, "original per-commit schema census", versions, wantVersions)
	if tables != wantTables || rows != wantRows || absences != wantAbsences {
		t.Fatal("historical tables/rows/absences differ from the original producer fixture")
	}
}

func TestNightlyNativeBoundaryEventsAndMultiVersionFreshStoreDiffs(t *testing.T) {
	ctx := context.Background()
	originals := nativeOriginals(t)
	type outcome struct {
		Case        string              `json:"case"`
		Driver      string              `json:"driver"`
		Restore     int                 `json:"fresh_restore"`
		Incarnation string              `json:"incarnation"`
		Reopen      string              `json:"reopen"`
		Census      nativeRestoreCensus `json:"census"`
	}
	results := []outcome{}
	t.Cleanup(func() {
		status := "bounded_public_native_history"
		if t.Failed() {
			status = "failed"
		}
		writeNightlyEvidence(t, "nightly-native-history-restore-diffs.json", map[string]any{
			"schema": "plug-nightly-native-history-restore-diffs/v1", "status": status, "container_sha256": nativeContainerSHA, "original_members": nativeOriginalSHA, "results": results,
			"boundaries": []string{"original_generic_public_source_not_private_or_joined_two_project_evidence", "pre_boundary_history_unavailable_not_reconstructed", "first_imported_native_image_is_not_source_creation", "native_images_do_not_create_portable_business_history", "current_events_not_assigned_to_source_commits", "zero_current_leases_no_positive_lease_filtering_credit", "memory_recovery_same_process_only", "old_storage_incarnation_refused_memory.list_rpc_continuation_not_qualified", "scheduled_main_execution_not_proven_by_manual_gate", "one_dispatch_is_not_a_three_night_streak_real_nights_0_of_3"},
		})
	})
	incarnations := map[string]bool{}
	for _, tc := range []struct {
		name, member string
		bounded      bool
	}{{"genuine-shallow-boundary", "public-shallow-boundary-v66.tar", true}, {"genuine-complete-multiversion", "public-multiversion-v66.tar", false}} {
		data := originals[tc.member]
		a := readArchive(t, data)
		checkNightlyNativeInput(t, a, tc.bounded)
		var baseline *nativeRestoreCensus
		for _, engine := range []string{"memory", "sqlite", "postgres"} {
			for iteration := 1; iteration <= 2; iteration++ {
				t.Run(fmt.Sprintf("%s/%s/fresh-%d", tc.name, engine, iteration), func(t *testing.T) {
					request := nativeRequestFor(t, data, engine)
					store, reopen := freshStore(t, engine)
					before, err := store.Meta(ctx)
					if err != nil || !before.Head.IsZero() || !before.HistoryFloor.IsZero() {
						t.Fatal("native destination is not fresh", err)
					}
					lease, err := store.Lease(ctx, "n1-public-native-restore", driver.LeaseTTL)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { lease.Release(ctx) })
					plan := request.SourcePlan
					plan.Target.StoreID, plan.Target.Incarnation = string(before.StoreID), before.Incarnation
					p := driver.RestoreImportProfile{Admission: request.Admission, SourceStoreID: driver.StoreID(request.SourceStoreID), Method: "offline.restore.import", IdempotencyKey: request.IdempotencyKey, ArchiveDigest: request.ArchiveSHA256, HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: time.Now().UTC().Format(time.RFC3339Nano)}
					p, err = converterroundtrip.BindSourceImport(ctx, bytes.NewReader(data), p, plan)
					if err != nil {
						t.Fatal("bind exact native caller policy", err)
					}
					unchanged(t, store, before)
					published, err := converterroundtrip.RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, p)
					if err != nil {
						t.Fatal("publish original native verified history and events", err)
					}
					if err := lease.Release(ctx); err != nil {
						t.Fatal(err)
					}
					if published.Restored.Incarnation == before.Incarnation || incarnations[published.Restored.Incarnation] {
						t.Fatal("independent native restore reused an incarnation")
					}
					incarnations[published.Restored.Incarnation] = true
					stale, err := store.Read(ctx, driver.View{Incarnation: before.Incarnation})
					if err == nil {
						stale.Close()
						t.Fatal("pre-restore native storage reader was admitted")
					}
					var reset driver.ErrIncarnationChanged
					if !errors.As(err, &reset) {
						t.Fatal("native old incarnation lost its typed reset", err)
					}
					got := nativeSourceCensus(t, store, a, p, published)
					if baseline == nil {
						baseline = &got
					} else {
						sameJSON(t, "independent fresh native history/events/schema census", got, *baseline)
					}
					lifetime := "same-process-memory"
					if engine != "memory" {
						if err := store.Close(); err != nil {
							t.Fatal(err)
						}
						sameJSON(t, "durable reopened native comparison", nativeSourceCensus(t, reopen(), a, p, published), got)
						lifetime = "driver-reopened"
					}
					results = append(results, outcome{Case: tc.name, Driver: engine, Restore: iteration, Incarnation: published.Restored.Incarnation, Reopen: lifetime, Census: got})
				})
			}
		}
	}
	if len(results) != 12 {
		t.Fatalf("native nightly matrix incomplete: got %d want 12", len(results))
	}
}

func TestNightlyNativePresentHistoryContradictionsRefuseNoWrites(t *testing.T) {
	ctx := context.Background()
	original := nativeOriginals(t)["public-shallow-boundary-v66.tar"]
	a := readArchive(t, original)
	passed := []string{}
	t.Cleanup(func() {
		status := "pass"
		if t.Failed() {
			status = "failed"
		}
		writeNightlyEvidence(t, "nightly-native-history-refusals.json", map[string]any{"schema": "plug-nightly-native-history-refusals/v1", "status": status, "scope": "declared_damaged_copies_of_public_original_typed_parser_refusals_no_writes", "typed_no_write_controls": passed})
	})
	for _, tc := range []struct {
		name   string
		change func(*dumpformat.NativeHistory)
	}{
		{"falsely-complete", func(h *dumpformat.NativeHistory) { h.Complete = true }},
		{"missing-present-image", func(h *dumpformat.NativeHistory) { h.Images = h.Images[1:] }},
		{"ghost-schema-claim", func(h *dumpformat.NativeHistory) {
			zero := uint64(0)
			h.HistoryCoverage.Boundaries[0].CommitSchemaVersion = &zero
		}},
		{"changed-events-attachment", func(h *dumpformat.NativeHistory) { h.HistoryCoverage.EventsAttachment.SHA256 = strings.Repeat("0", 64) }},
	} {
		damaged := damagedNativeArchive(t, original, tc.change)
		_, expected := dumpformat.Read(bytes.NewReader(damaged))
		var gap dumpformat.IncompleteHistoryError
		if !errors.As(expected, &gap) || gap.Reason == "" {
			t.Fatal("damaged fixture did not reach a typed present-history integrity refusal", expected)
		}
		for _, engine := range []string{"memory", "sqlite", "postgres"} {
			t.Run(engine+"/"+tc.name, func(t *testing.T) {
				store, _ := freshStore(t, engine)
				before, err := store.Meta(ctx)
				if err != nil {
					t.Fatal(err)
				}
				p := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "n1-test-restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: driver.StoreID(a.Manifest().StoreID), Method: "offline.restore.import", IdempotencyKey: strings.Repeat("b", 32), ArchiveDigest: digest(damaged), HistoryMode: driver.RestoreHistoryModeFull}
				_, err = converterroundtrip.RestoreImport(ctx, bytes.NewReader(damaged), store, p)
				var refused dumpformat.IncompleteHistoryError
				if !errors.As(err, &refused) {
					t.Fatal("native caller lost typed present-history refusal", err)
				}
				sameJSON(t, "exact native contradiction refusal", refused, gap)
				unchanged(t, store, before)
				checkNoArchiveWrites(t, store, a)
				if r, err := store.Read(ctx, driver.View{Incarnation: before.Incarnation}); err != nil {
					t.Fatal(err)
				} else {
					if _, err := r.Blob(ctx, driver.Hash(digest(damaged))); err == nil {
						t.Fatal("refused native import retained its damaged archive")
					}
					if err := r.Close(); err != nil {
						t.Fatal(err)
					}
				}
				passed = append(passed, engine+"/"+tc.name+"/"+refused.Reason)
			})
		}
	}
	if len(passed) != 12 {
		t.Fatal("native typed no-write matrix incomplete")
	}
}
