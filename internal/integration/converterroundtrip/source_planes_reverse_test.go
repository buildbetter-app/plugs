package converterroundtrip

import (
	"archive/tar"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This disposable admitted fixture has distinct physical source IDs. The
// existing equal-ID fixture remains a separate anomalous-source refusal test.
func sourceReverseFixture(t *testing.T, ephemeral bool) []byte {
	t.Helper()
	base := jointSourceFixture(t)
	return rewriteData(t, func(members map[string][]byte) {
		reader := tar.NewReader(bytes.NewReader(base))
		for {
			header, err := reader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			members[header.Name], err = io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
		}
		var manifest dumpformat.Manifest
		decode(t, members["manifest.json"], &manifest)
		for _, kind := range manifest.Kinds {
			var rows []dumpformat.Record
			for _, line := range bytes.Split(bytes.TrimSpace(members[kind.File]), []byte{'\n'}) {
				if len(line) == 0 {
					continue
				}
				var row dumpformat.Record
				decode(t, line, &row)
				fields := row.Source.Fields
				switch row.Source.Table {
				case dumpformat.EphemeralEntityTable:
					fields["id"], fields["ephemeral"] = "demo-9", ephemeral
				case dumpformat.EphemeralCommentTable, dumpformat.EphemeralLabelTable:
					fields["issue_id"] = "demo-9"
				case dumpformat.EphemeralChildCounterTable:
					fields["parent_id"] = "demo-9"
				case dumpformat.EphemeralDependencyTable:
					fields["issue_id"], row.Body["from"] = "demo-9", "demo-9"
				case dumpformat.DurableDependencyTable:
					if row.Body["target_kind"] == dumpformat.EphemeralDependencyTargetField {
						fields[dumpformat.EphemeralDependencyTargetField], row.Body["to"] = "demo-9", "demo-9"
					}
				}
				if row.Kind == "beads-archive" {
					row.ID = row.Source.Table + "/" + digestBytes(encode(t, fields))
				}
				rows = append(rows, row)
			}
			sort.Slice(rows, func(i, j int) bool {
				a, b := rows[i], rows[j]
				if kind.Name == "edge" {
					return driver.CompareSourceEdgeKeys(driver.SourceEdgeKey{Project: a.Project, Table: a.Source.Table, ID: a.ID}, driver.SourceEdgeKey{Project: b.Project, Table: b.Source.Table, ID: b.ID}) < 0
				}
				if a.Project != b.Project {
					return a.Project < b.Project
				}
				return a.ID < b.ID
			})
			members[kind.File] = nil
			for _, row := range rows {
				members[kind.File] = append(members[kind.File], encode(t, row)...)
			}
		}
	})
}

func sourceReverseProfile(t *testing.T, data []byte, store driver.Store, lease driver.ActiveLease, ephemeral bool) driver.RestoreImportProfile {
	t.Helper()
	return jointProfileWithPlan(t, data, store, lease, func(plan *driver.SourcePlanePlanV1) {
		key := driver.SourceEntityKey{Project: "demo", Plane: dumpformat.EphemeralEntityTable, ID: "demo-9"}
		plan.Runs[0].Members = []driver.SourceEntityKey{key}
		for i := range plan.Entities {
			entity := &plan.Entities[i]
			if entity.TargetKind == "workflow:item" {
				entity.Route.ActiveTargetID = entity.TargetID
				entity.Route.DurableTargetID = ""
			} else {
				entity.Route.ActiveTargetID = ""
			}
		}
		plan.Dispositions[0].Ephemeral = ephemeral
		for i := range plan.Origins {
			if plan.Origins[i].Owner != nil {
				owner := key
				plan.Origins[i].Owner = &owner
			}
		}
	})
}

func sourceReverseRows(t *testing.T, data []byte) map[string]map[string]any {
	t.Helper()
	result := map[string]map[string]any{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	for {
		var row map[string]any
		if err := decoder.Decode(&row); err == io.EOF {
			return result
		} else if err != nil {
			t.Fatal(err)
		}
		if id, ok := row["id"].(string); ok {
			if result[id] != nil {
				t.Fatal("reverse relabeled or duplicated raw ID", id)
			}
			result[id] = row
		}
	}
}

type sourceReverseSnapshotHook struct {
	driver.Store
	driver.SourcePlaneSnapshotStore
	driver.CheckpointInventoryStore
	afterSnapshot func()
	reads, pins   int
}

func (s *sourceReverseSnapshotHook) ReadCheckpointInventory(ctx context.Context, view driver.View) (driver.CheckpointInventory, error) {
	s.pins++
	reader, err := s.CheckpointInventoryStore.ReadCheckpointInventory(ctx, view)
	if err == nil && s.afterSnapshot != nil {
		s.afterSnapshot()
	}
	return reader, err
}

func (s *sourceReverseSnapshotHook) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	s.reads++
	if s.reads > 1 {
		return nil, errors.New("reverse reopened its ordinary reader")
	}
	return s.Store.Read(ctx, view)
}

func (s *sourceReverseSnapshotHook) ReadSourcePlaneSnapshot(ctx context.Context, view driver.View, keys []string) (driver.SourcePlaneSnapshot, error) {
	s.pins++
	reader, err := s.SourcePlaneSnapshotStore.ReadSourcePlaneSnapshot(ctx, view, keys)
	if err == nil && s.afterSnapshot != nil {
		s.afterSnapshot()
	}
	return reader, err
}

func TestJointSourceReversePinnedDefaultAndAllAcrossThreeStores(t *testing.T) {
	for _, ephemeral := range []bool{false, true} {
		mode := "non-ephemeral-active"
		if ephemeral {
			mode = "ephemeral-active"
		}
		for _, engine := range []string{"memory", "sqlite", "postgres"} {
			t.Run(mode+"/"+engine, func(t *testing.T) {
				ctx := context.Background()
				data := sourceReverseFixture(t, ephemeral)
				var dsn string
				store, lease, _ := openIdentityTarget(t, engine, func(value string) { dsn = value })
				profile := sourceReverseProfile(t, data, store, lease, ephemeral)
				report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
				if err != nil {
					t.Fatal(err)
				}
				capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
				if err != nil {
					t.Fatal(err)
				}
				namespace := report.SourcePlanes.Providers[0].Provider.Namespace
				_, server := sourcePublicService(t, store, lease, profile, report, &sourcePublicPolicy{})
				apply := func(activeTitle, durableTitle string, request int) {
					t.Helper()
					change := model.ChangeSet{Ops: []model.Op{
						{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID("demo-9", model.Natural64(0)), Fields: map[string]any{"title": activeTitle, "labels": []string{"current-active-label"}}}},
						{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID("demo-8", model.Natural64(0)), Fields: map[string]any{"title": durableTitle}}},
					}}
					sourcePublicCall(t, server, profile.Admission.Actor, "tx.apply", request, map[string]any{"changeSet": change}, "")
				}
				apply("current active reverse", "current durable reverse", 901)
				before := savedLabelImport(t, store, report.Archive, profile, "")
				checkpoint, err := capability.LoadCheckpoint(ctx, namespace)
				if err != nil {
					t.Fatal(err)
				}
				var db *sql.DB
				var physical map[string][]string
				if engine != "memory" {
					driverName := engine
					if engine == "postgres" {
						driverName = "pgx"
					}
					db, err = sql.Open(driverName, dsn)
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()
					physical = jointPhysicalSQL(t, db, engine)
				}
				defaults, err := ReverseRestored(ctx, store)
				if err != nil {
					t.Fatal("default joint reverse", err)
				}
				all, err := ReverseRestoredWithOptions(ctx, store, reverseconverter.Options{All: true})
				if err != nil {
					t.Fatal("all-planes joint reverse", err)
				}
				defaultRows, allRows := sourceReverseRows(t, defaults.Result.JSONL), sourceReverseRows(t, all.Result.JSONL)
				if allRows["demo-9"]["title"] != "current active reverse" || allRows["demo-8"]["title"] != "current durable reverse" || allRows["demo-9"][dumpformat.EphemeralPlaneWireField] != true {
					t.Fatal("reverse substituted receipt content or lost physical placement", string(all.Result.JSONL))
				}
				if !sameReverseJSON(allRows["demo-9"]["labels"], []string{"current-active-label"}) || len(all.Result.Report.OmittedEphemeralRows) != 0 {
					t.Fatal("all option lost live labels or omitted a row", all.Result.Report)
				}
				if ephemeral {
					if defaultRows["demo-9"] != nil || len(defaults.Result.Report.OmittedEphemeralRows) != 1 || !strings.Contains(defaults.Result.Report.OmittedEphemeralRows[0], `"demo-9"`) {
						t.Fatal("default must report each omitted physical ephemeral row", defaults.Result.Report)
					}
				} else if defaultRows["demo-9"]["title"] != "current active reverse" || len(defaults.Result.Report.OmittedEphemeralRows) != 0 {
					t.Fatal("default dropped a non-ephemeral active-plane row", defaults.Result.Report)
				}
				if defaults.Snapshot.At == nil || defaults.Snapshot.At.String() != before.Meta.Head.String() || all.Snapshot.At == nil || all.Snapshot.At.String() != before.Meta.Head.String() {
					t.Fatal("reverse lost its pinned core head")
				}
				afterCheckpoint, err := capability.LoadCheckpoint(ctx, namespace)
				if err != nil || !reflect.DeepEqual(checkpoint, afterCheckpoint) || !reflect.DeepEqual(before, savedLabelImport(t, store, report.Archive, profile, "")) {
					t.Fatal("reverse published core/history/receipt/blob/namespace state", err)
				}
				if db != nil && !reflect.DeepEqual(physical, jointPhysicalSQL(t, db, engine)) {
					t.Fatal("reverse changed physical SQL state")
				}
				observed := &sourceReverseSnapshotHook{Store: store, SourcePlaneSnapshotStore: capability, CheckpointInventoryStore: store.(driver.CheckpointInventoryStore), afterSnapshot: func() { apply("later active reverse", "later durable reverse", 902) }}
				captured, err := ReverseRestoredWithOptions(ctx, observed, reverseconverter.Options{All: true})
				if err != nil || observed.reads != 0 || observed.pins != 1 || captured.Result.Digest != all.Result.Digest {
					t.Fatalf("reverse mixed current namespace/core observations: reads=%d pins=%d err=%v", observed.reads, observed.pins, err)
				}
				latest, err := ReverseRestoredWithOptions(ctx, store, reverseconverter.Options{All: true})
				if err != nil || sourceReverseRows(t, latest.Result.JSONL)["demo-9"]["title"] != "later active reverse" || sourceReverseRows(t, latest.Result.JSONL)["demo-8"]["title"] != "later durable reverse" {
					t.Fatal("later observation failed to advance both planes", err)
				}
				if ephemeral {
					sourcePublicCall(t, server, profile.Admission.Actor, "bead.close", 903, map[string]any{"id": model.RefByID("demo-9", model.Natural64(0)), "reason": "terminal omitted source"}, "")
					terminalBaseline := savedLabelImport(t, store, report.Archive, profile, "")
					terminalCheckpoint, err := capability.LoadCheckpoint(ctx, namespace)
					if err != nil {
						t.Fatal(err)
					}
					terminalDefault, err := ReverseRestored(ctx, store)
					if err != nil || len(terminalDefault.Result.Report.OmittedEphemeralRows) != 1 || sourceReverseRows(t, terminalDefault.Result.JSONL)["demo-9"] != nil {
						t.Fatal("terminal ephemeral identity prevented default omission", err)
					}
					for _, edge := range terminalDefault.Result.Projection.Edges {
						if edge.From == "demo-9" || edge.To == "demo-9" {
							t.Fatal("terminal cleanup resurrected a retained edge", edge)
						}
					}
					terminalAll, err := ReverseRestoredWithOptions(ctx, store, reverseconverter.Options{All: true})
					var refused ReverseRefusal
					if !errors.As(err, &refused) || len(terminalAll.Result.JSONL) != 0 {
						t.Fatal("all-planes reconstructed unavailable terminal body", err)
					}
					afterTerminal, err := capability.LoadCheckpoint(ctx, namespace)
					if err != nil || !reflect.DeepEqual(terminalCheckpoint, afterTerminal) || !reflect.DeepEqual(terminalBaseline, savedLabelImport(t, store, report.Archive, profile, "")) {
						t.Fatal("terminal reverse/refusal published state", err)
					}
				}
			})
		}
	}
}

func TestJointSourceReverseEqualRawIDRefusesAcrossThreeStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			data := jointSourceFixture(t)
			store, lease, _ := openIdentityTarget(t, engine)
			profile := jointProfile(t, data, store, lease)
			report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
			if err != nil {
				t.Fatal(err)
			}
			before := savedLabelImport(t, store, report.Archive, profile, "")
			for _, all := range []bool{false, true} {
				result, err := ReverseRestoredWithOptions(ctx, store, reverseconverter.Options{All: all})
				var refusal *reverseconverter.Refusal
				if !errors.As(err, &refusal) || !strings.Contains(err.Error(), "source-plane-contract") || len(result.Result.JSONL) != 0 || result.Result.Digest != "" {
					t.Fatalf("all=%v collision produced ambiguous output: %v", all, err)
				}
			}
			if !reflect.DeepEqual(before, savedLabelImport(t, store, report.Archive, profile, "")) {
				t.Fatal("refused reverse changed core state")
			}
		})
	}
}

func TestProjectNativeStatusForReverseArchivesIncompatibleBuiltin(t *testing.T) {
	projected := reverseconverter.Record{
		Kind: "bead", ID: "review-project-c2d", Project: "review-project",
		Body:   map[string]any{"metadata": map[string]any{}, "status_category": "wip"},
		Source: reverseconverter.Provenance{Table: dumpformat.DurableEntityTable, Fields: map[string]any{"status": "blocked"}},
	}
	dump := reverseconverter.Dump{Manifest: reverseconverter.Manifest{
		SourcePlaneAddressContract: dumpformat.SourcePlaneAddressContract,
		StatusCategoryContract:     dumpformat.StatusCategoryContract,
	}}
	if err := projectNativeStatusForReverse(&projected, dump, true); err != nil {
		t.Fatal(err)
	}
	if got := projected.Body["status_category"]; got != "started" {
		t.Fatalf("native wip category changed to %v", got)
	}
	if got := projected.Source.Fields["status"]; got != "in_progress" {
		t.Fatalf("incompatible native status was emitted as %v", got)
	}
	metadata, ok := projected.Body["metadata"].(map[string]any)
	if !ok || metadata["_plug_native_status"] != "blocked" || metadata["_plug_native_status_category"] != "wip" {
		t.Fatalf("native status provenance missing: %#v", projected.Body["metadata"])
	}
}

// This declared synthetic history proves admitted metadata behavior, not native
// capture or native-history qualification. Every history image carries the same
// user metadata and explicit custom-type mapping as its admitted head.
func nativeReverseMetadataFixture(t *testing.T, metadata map[string]any) []byte {
	t.Helper()
	members := tarMembers(t, nativeHistoryFixture(t, "pl", "wip"))
	var manifest dumpformat.Manifest
	decode(t, members["manifest.json"], &manifest)
	manifest.Ontology = []dumpformat.OntologyDirective{{ID: "metadata-custom-type", Kind: "map_type", Target: dumpformat.OntologyTarget{Project: "pl", Type: "rich_kind"}, From: "rich_kind", To: "issue"}}
	convert := func(row *dumpformat.Record) {
		if row.Kind == "bead" {
			row.Source.Fields["issue_type"], row.Body["source_type"] = "rich_kind", "rich_kind"
			row.Source.Fields["metadata"], row.Body["metadata"] = metadata, metadata
		}
	}
	secondBead := func(row dumpformat.Record) dumpformat.Record {
		var second dumpformat.Record
		decode(t, encode(t, row), &second)
		second.ID, second.Source.Fields["id"] = "pl-43w", "pl-43w"
		second.Body["labels"] = []any{}
		return second
	}
	// Joint reverse projects a current edge collection. Supply an actual
	// admitted relation and its two endpoints, rather than an empty kind entry.
	edge := dumpformat.Record{Kind: "edge", ID: "metadata-edge", Project: "pl",
		Body: dumpformat.Row{"from": "pl-42v", "to": "pl-43w", "target_kind": "depends_on_issue_id", "type": "relates-to", "metadata": map[string]any{"fixture": "metadata-roundtrip"}, "thread_id": ""},
		Source: dumpformat.Provenance{Table: dumpformat.DurableDependencyTable, Fields: dumpformat.Row{
			"id": "metadata-edge", "issue_id": "pl-42v", "depends_on_issue_id": "pl-43w", dumpformat.EphemeralDependencyTargetField: nil, "depends_on_external": nil,
			"type": "relates-to", "metadata": map[string]any{"fixture": "metadata-roundtrip"}, "thread_id": "", "created_at": "2026-01-01T00:00:00Z", "created_by": "fixture-author",
		}}}
	var beads bytes.Buffer
	for _, line := range bytes.Split(bytes.TrimSpace(members["records/bead.ndjson"]), []byte{'\n'}) {
		var row dumpformat.Record
		decode(t, line, &row)
		convert(&row)
		beads.Write(encode(t, row))
		beads.Write(encode(t, secondBead(row)))
	}
	members["records/bead.ndjson"] = beads.Bytes()
	members["records/edge.ndjson"] = encode(t, edge)
	for i := range manifest.Kinds {
		if manifest.Kinds[i].Name == "bead" {
			manifest.Kinds[i].Count = 2
		}
	}
	manifest.Kinds = append(manifest.Kinds, dumpformat.Kind{Name: "edge", Version: dumpformat.EdgeVersionLocalOpaque, File: "records/edge.ndjson", Count: 1})
	sort.Slice(manifest.Kinds, func(i, j int) bool { return manifest.Kinds[i].Name < manifest.Kinds[j].Name })
	for i := range manifest.Sources {
		switch manifest.Sources[i].Family {
		case "issues", "issues.row_lock", "issues.is_blocked":
			manifest.Sources[i].Rows = 2
		case dumpformat.DurableDependencyTable:
			manifest.Sources[i].Rows = 1
		}
	}
	var history dumpformat.SourceHistory
	decode(t, members[dumpformat.SourceHistoryMember], &history)
	for i := range history.Range.Frames {
		var second dumpformat.Record
		for j := range history.Range.Frames[i].Records {
			row := &history.Range.Frames[i].Records[j]
			convert(row)
			if row.Kind == "bead" && row.ID == "pl-42v" {
				second = secondBead(*row)
			}
		}
		if second.Kind == "bead" {
			history.Range.Frames[i].Records = append(history.Range.Frames[i].Records, second, edge)
		}
	}
	members[dumpformat.SourceHistoryMember] = encode(t, history)
	for path, raw := range members {
		if path != "manifest.json" {
			manifest.Files[path] = dumpformat.File{SHA256: hashBytes(raw), Size: int64(len(raw))}
		}
	}
	members["manifest.json"] = encode(t, manifest)
	return writeTarMembers(t, members)
}

func TestB11AdmittedNativeReverseMetadataPreservationAndConflicts(t *testing.T) {
	bookkeeping := map[string]any{"_plug_native_status": "shared", "_plug_native_status_category": "wip", "_plug_native_type": "rich_kind"}
	type metadataCase struct {
		name, conflictKey string
		matching          bool
		conflictValue     any
	}
	cases := []metadataCase{{name: "absent"}, {name: "matching", matching: true}}
	for _, key := range []string{"_plug_native_status", "_plug_native_status_category", "_plug_native_type"} {
		cases = append(cases,
			metadataCase{name: key + "/unequal", conflictKey: key, conflictValue: "admitted-user-value"},
			metadataCase{name: key + "/null", conflictKey: key, conflictValue: nil})
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, tc := range cases {
			t.Run(engine+"/"+tc.name, func(t *testing.T) {
				ctx := context.Background()
				metadata := map[string]any{"user": map[string]any{"array": []any{"kept", nil, true}, "number": json.Number("9007199254740993")}}
				if tc.matching {
					for key, value := range bookkeeping {
						metadata[key] = value
					}
				}
				if tc.conflictKey != "" {
					metadata[tc.conflictKey] = tc.conflictValue
				}
				data := nativeReverseMetadataFixture(t, metadata)
				store, lease, _ := openIdentityTarget(t, engine)
				plan := nativeHistoryPlan(t, data, store, lease)
				plan.Dispositions[0].SourceType = "rich_kind"
				archive, err := dumpformat.Read(bytes.NewReader(data))
				if err != nil {
					t.Fatal("metadata fixture", err)
				}
				records, err := archive.SourcePlaneRecords()
				if err != nil {
					t.Fatal(err)
				}
				for _, record := range records {
					if record.Kind != "edge" {
						continue
					}
					var origins []driver.SourcePlaneOriginV1
					for _, origin := range plan.Origins {
						if origin.Table != record.Table || origin.ID != record.ID {
							origins = append(origins, origin)
						}
					}
					plan.Origins = origins
					from := driver.SourceEntityKey{Project: "pl", Plane: dumpformat.DurableEntityTable, ID: "pl-42v"}
					to := driver.SourceEntityKey{Project: "pl", Plane: dumpformat.DurableEntityTable, ID: "pl-43w"}
					predicate, err := dumpformat.SourceEdgeMetadata(dumpformat.EdgeVersionLocalOpaque, record.ID, "relates-to", map[string]any{"fixture": "metadata-roundtrip"})
					if err != nil {
						t.Fatal(err)
					}
					predicateJSON, err := json.Marshal(predicate)
					if err != nil {
						t.Fatal(err)
					}
					plan.Relations = append(plan.Relations, driver.SourcePlaneRelationV1{Source: driver.SourceEdgeKey{Project: "pl", Table: record.Table, ID: record.ID}, From: from, To: &to, Kind: "relates-to", PredicateContract: "beads-typed-metadata/1", PredicateJSON: predicateJSON, Origin: jointCommitment(record.Raw, "object"), Fields: jointFields(t, record)})
				}
				profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: driver.StoreID(plan.SourceStoreID), Method: "offline.restore.import", IdempotencyKey: strings.Repeat("b", 32), ArchiveDigest: plan.ArchiveSHA256, HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: nativeFixtureTime.Format("2006-01-02T15:04:05.999999999Z07:00")}
				profile, err = BindSourceImport(ctx, bytes.NewReader(data), profile, plan)
				if err != nil {
					t.Fatal("bind admitted user metadata", err)
				}
				report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
				if err != nil {
					t.Fatal("import admitted user metadata", err)
				}
				before := savedLabelImport(t, store, report.Archive, profile, "")
				result, err := ReverseRestored(ctx, store)
				if tc.conflictKey != "" {
					var refusal ReverseRefusal
					if !errors.As(err, &refusal) || refusal.Reason != "conflicting_native_metadata" || len(result.Result.JSONL) != 0 || result.Result.Digest != "" {
						t.Fatalf("conflicting admitted key produced output: %v %#v", err, result.Result)
					}
				} else {
					if err != nil {
						t.Fatal("reverse admitted user metadata", err)
					}
					rows := sourceReverseRows(t, result.Result.JSONL)
					if len(rows) != 2 || rows["pl-42v"] == nil || rows["pl-43w"] == nil {
						t.Fatal("missing admitted beads", string(result.Result.JSONL))
					}
					// Generic reverse also preserves source fidelity metadata:
					// reverseconverter/convert.go:712-745, with textual pinned
					// archived at :611-616. Keep the complete expectation literal.
					want := map[string]any{
						"user":                         map[string]any{"array": []any{"kept", nil, true}, "number": json.Number("9007199254740993")},
						"_plug_native_status":          "shared",
						"_plug_native_status_category": "wip",
						"_plug_native_type":            "rich_kind",
						"beads.agent_state":            nil,
						"beads.await_id":               "",
						"beads.await_type":             "",
						"beads.ephemeral":              json.Number("0"),
						"beads.event_kind":             "",
						"beads.hook_bead":              nil,
						"beads.is_template":            "0",
						"beads.mol_type":               "",
						"beads.payload":                map[string]any{"n": json.Number("9007199254740992"), "null": nil},
						"beads.role_bead":              nil,
						"beads.role_type":              nil,
						"beads.target":                 "",
						"beads.timeout_ns":             json.Number("9007199254740993"),
						"beads.waiters":                "",
						"beads.wisp_type":              "",
						"beads.work_type":              "",
						"_plug_source_fields":          `{"actor":"","compaction_level":"0","content_hash":"8c5b6000065e227a62c898d951bdbd3724ff2f1d475a2b959e742b4f2393405d","no_history":0,"pinned":"0","sender":"","source_repo":"","storage_class":"versioned"}`,
					}
					for _, id := range []string{"pl-42v", "pl-43w"} {
						if !sameReverseJSON(rows[id]["metadata"], want) {
							t.Fatalf("%s admitted metadata changed: got %#v want %#v", id, rows[id]["metadata"], want)
						}
					}
					dependencies, ok := rows["pl-42v"]["dependencies"].([]any)
					if !ok || len(dependencies) != 1 {
						t.Fatal("JSONL lost admitted durable edge", rows["pl-42v"])
					}
					dependency, ok := dependencies[0].(map[string]any)
					if !ok || dependency["issue_id"] != "pl-42v" || dependency["depends_on_id"] != "pl-43w" || dependency["type"] != "relates-to" {
						t.Fatal("JSONL changed admitted durable edge", dependencies)
					}
					if len(result.Result.Projection.Edges) != 1 || result.Result.Projection.Edges[0].From != "pl-42v" || result.Result.Projection.Edges[0].To != "pl-43w" || result.Result.Projection.Edges[0].Type != "relates-to" {
						t.Fatal("admitted durable edge changed", result.Result.Projection.Edges)
					}
					if tc.name == "absent" {
						beadsResult, beadsErr := ReverseRestoredWithOptions(ctx, store, reverseconverter.Options{All: true, BeadsDestination: true})
						if beadsErr != nil {
							t.Fatal("Beads destination reverse", beadsErr)
						}
						beadsRows := sourceReverseRows(t, beadsResult.Result.JSONL)
						for _, id := range []string{"pl-42v", "pl-43w"} {
							row := beadsRows[id]
							if !sameReverseJSON(row["metadata"], metadata) || row["issue_type"] != "rich_kind" {
								t.Fatalf("Beads destination changed source metadata/type for %s: %#v", id, row)
							}
							for key := range row["metadata"].(map[string]any) {
								if strings.HasPrefix(key, "beads.") || strings.HasPrefix(key, "_plug_") {
									t.Fatalf("Beads destination leaked provenance key %q", key)
								}
							}
						}
					}
				}
				if !reflect.DeepEqual(before, savedLabelImport(t, store, report.Archive, profile, "")) {
					t.Fatal("reverse or conflict refusal changed imported state")
				}
			})
		}
	}
}
