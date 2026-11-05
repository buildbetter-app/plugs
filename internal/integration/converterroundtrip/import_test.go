package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// admittedFixtureBytes keeps the checked-in canonical fixture intact and
// produces a detached, canonical in-memory archive with the same shape as the
// server exporter: no verified history, a hash source declaration, and a
// block dependency. This is test data only; it is never written back.
func admittedFixtureBytes(t *testing.T) []byte {
	return admittedFixtureBytesWithOpaqueFields(t, "", "")
}

func admittedFixtureBytesWithAgentState(t *testing.T, agentState string) []byte {
	return admittedFixtureBytesWithOpaqueFields(t, agentState, "")
}

func admittedFixtureBytesWithAgentStateAndAwaitID(t *testing.T, agentState, awaitID string) []byte {
	return admittedFixtureBytesWithFields(t, map[string]any{"agent_state": agentState, "await_id": awaitID})
}

func admittedFixtureBytesWithOpaqueFields(t *testing.T, agentState, awaitID string) []byte {
	return admittedFixtureBytesWithAgentStateAndAwaitID(t, agentState, awaitID)
}

func admittedFixtureBytesWithFields(t *testing.T, opaque map[string]any) []byte {
	t.Helper()
	return rewriteData(t, func(members map[string][]byte) {
		var manifest map[string]any
		decode(t, members["manifest.json"], &manifest)
		manifest["history"] = "unavailable"
		projects := manifest["projects"].([]any)
		project := projects[0].(map[string]any)
		project["id_mode"] = "hash"
		project["hash_length"] = 1
		project["last_id"] = 8
		project["child_counters"] = map[string]any{"demo-7": 4}
		projects = append(projects, map[string]any{
			"prefix":         "empty",
			"id_mode":        "hash",
			"hash_length":    1,
			"last_id":        0,
			"child_counters": map[string]any{},
		})
		manifest["projects"] = projects
		for _, raw := range manifest["sources"].([]any) {
			source := raw.(map[string]any)
			if source["family"] == "issue_versions" {
				source["rows"] = 0
			}
		}

		archiveLines := bytes.Split(bytes.TrimSuffix(members["records/beads-archive.ndjson"], []byte{'\n'}), []byte{'\n'})
		kept := archiveLines[:0]
		for _, line := range archiveLines {
			var row map[string]any
			decode(t, line, &row)
			source := row["source"].(map[string]any)
			if source["table"] == "issue_versions" {
				continue
			}
			kept = append(kept, line)
		}
		members["records/beads-archive.ndjson"] = append(bytes.Join(kept, []byte{'\n'}), '\n')
		for _, raw := range manifest["kind_registry"].([]any) {
			kind := raw.(map[string]any)
			if kind["name"] == "beads-archive" {
				kind["count"] = len(kept)
			}
		}

		var edge map[string]any
		decode(t, bytes.TrimSuffix(members["records/edge.ndjson"], []byte{'\n'}), &edge)
		edge["body"].(map[string]any)["type"] = "blocks"
		edge["source"].(map[string]any)["fields"].(map[string]any)["type"] = "blocks"
		members["records/edge.ndjson"] = encode(t, edge)
		if len(opaque) != 0 {
			changeRecord(t, members, "bead", func(record map[string]any) {
				for field, value := range opaque {
					source(record)[field] = value
				}
			})
		}
		members["manifest.json"] = encode(t, manifest)
	})
}

func admittedImportProfile(t *testing.T, archiveBytes []byte) (driver.RestoreImportProfile, *dumpformat.Archive) {
	t.Helper()
	archive, err := dumpformat.Read(bytes.NewReader(archiveBytes))
	if err != nil {
		t.Fatal(err)
	}
	profile := driver.RestoreImportProfile{
		Admission: driver.RestoreImportAdmission{
			Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"},
			Scope: "", Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64),
		},
		SourceStoreID: driver.StoreID(archive.Manifest().StoreID), Method: "offline.restore.import",
		IdempotencyKey: strings.Repeat("b", 32), ArchiveDigest: archive.SHA256(), DerivedDigest: strings.Repeat("c", 64),
		HistoryMode: driver.RestoreHistoryModeSnapshot,
	}
	restoreTime := time.Date(2026, 10, 30, 19, 0, 0, 0, time.UTC)
	plan, err := buildPlanWithProfile(archive, &profile, restoreTime)
	if err != nil {
		t.Fatal(err)
	}
	profile.DerivedDigest = plan.derivedDigest
	return profile, archive
}

func TestRestoreImportHistoryModesAreExplicitAcrossMemoryAndSQLite(t *testing.T) {
	data := admittedFixtureBytes(t)
	snapshotProfile, _ := admittedImportProfile(t, data)
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			for _, test := range []struct {
				name   string
				mode   driver.RestoreHistoryMode
				reason driver.RestoreHistoryReason
			}{
				{name: "default-full", mode: "", reason: driver.RestoreHistoryReasonUnavailable},
				{name: "partial", mode: driver.RestoreHistoryModePartial, reason: driver.RestoreHistoryReasonNotYetSupported},
			} {
				t.Run(test.name, func(t *testing.T) {
					store, lease, cleanup := openTarget(t, kind)
					defer cleanup()
					before, err := store.Meta(ctx)
					if err != nil {
						t.Fatal(err)
					}
					profile := snapshotProfile
					profile.HistoryMode = test.mode
					_, err = RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
					var refused driver.ErrRestoreHistoryRefused
					wantMode := test.mode
					if wantMode == "" {
						wantMode = driver.RestoreHistoryModeFull
					}
					if !errors.As(err, &refused) || refused.Mode != wantMode || refused.Reason != test.reason {
						t.Fatalf("mode=%q refusal=%T %v", test.mode, err, err)
					}
					after, metaErr := store.Meta(ctx)
					if metaErr != nil || after != before {
						t.Fatalf("refused mode changed target: before=%+v after=%+v err=%v", before, after, metaErr)
					}
				})
			}

			store, lease, cleanup := openTarget(t, kind)
			func() {
				defer cleanup()
				unverifiedData := fixtureBytes(t)
				unverifiedArchive, err := dumpformat.Read(bytes.NewReader(unverifiedData))
				if err != nil {
					t.Fatal(err)
				}
				profile := snapshotProfile
				profile.SourceStoreID = driver.StoreID(unverifiedArchive.Manifest().StoreID)
				profile.ArchiveDigest = unverifiedArchive.SHA256()
				profile.DerivedDigest = strings.Repeat("c", 64)
				profile.HistoryMode = driver.RestoreHistoryModeFull
				_, err = RestoreImportWithLease(ctx, bytes.NewReader(unverifiedData), store, lease, profile)
				var refused driver.ErrRestoreHistoryRefused
				if !errors.As(err, &refused) || refused.Reason != driver.RestoreHistoryReasonAvailableUnverified {
					t.Fatalf("available-unverified full refusal=%T %v", err, err)
				}
			}()

			store, lease, cleanup = openTarget(t, kind)
			defer cleanup()
			before, err := store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			profile := snapshotProfile
			profile.HistoryMode = driver.RestoreHistoryModeSnapshot
			report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
			if err != nil {
				t.Fatalf("snapshot import: %v", err)
			}
			if report.Restored.Head.String() != "1" || report.Restored.HistoryFloor.String() != "1" || report.Restored.Incarnation == before.Incarnation {
				t.Fatalf("snapshot metadata before=%+v restored=%+v", before, report.Restored)
			}
			if _, err := store.Read(ctx, driver.View{Incarnation: before.Incarnation}); err != nil {
				var changed driver.ErrIncarnationChanged
				if !errors.As(err, &changed) {
					t.Fatalf("old incarnation read=%v", err)
				}
			} else {
				t.Fatal("old incarnation read was accepted")
			}
			at := model.Natural64(0)
			_, err = store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &at})
			var truncated driver.ErrTruncated
			if !errors.As(err, &truncated) || truncated.Floor.String() != "1" {
				t.Fatalf("snapshot floor read=%v", err)
			}
		})
	}
}

func TestAdmittedImportRestoresCommentAndServesWorkload(t *testing.T) {
	const agentState = "opaque/v1:phase=ready\x00"
	const awaitID = "opaque-await/v1:owner=worker\x00"
	opaque := map[string]any{
		"agent_state": agentState,
		"await_id":    awaitID,
		"await_type":  "human",
		"ephemeral":   json.Number("1"),
		"event_kind":  nil,
		"hook_bead":   "demo-1",
		"is_template": false,
		"mol_type":    "formula",
		"payload":     map[string]any{"state": "waiting", "number": json.Number("9007199254740993")},
		"role_bead":   "demo-2",
		"role_type":   "agent",
		"target":      "demo-3",
		"timeout_ns":  json.Number("9007199254740993"),
		"waiters":     []any{"demo-1", nil},
		"wisp_type":   "run",
		"work_type":   "workflow",
	}
	data := admittedFixtureBytesWithFields(t, opaque)
	profile, archive := admittedImportProfile(t, data)
	agentStateID := archive.Records("bead")[0].ID
	inspection, err := InspectImport(context.Background(), bytes.NewReader(data))
	if err != nil || inspection.SourceStoreID != profile.SourceStoreID || inspection.ArchiveSHA256 != profile.ArchiveDigest || inspection.DerivedSHA256 != profile.DerivedDigest {
		t.Fatalf("import inspection=%+v err=%v profile=%+v", inspection, err, profile)
	}
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			store, lease, cleanup := openTarget(t, kind)
			defer cleanup()
			ctx := context.Background()
			before, err := store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := store.Read(ctx, driver.View{Incarnation: before.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			page, err := reader.Query(ctx, driver.Query{Limit: 10})
			_ = reader.Close()
			if err != nil || len(page.Items) != 0 || !before.Head.IsZero() {
				t.Fatalf("pre-call census meta=%+v rows=%d err=%v", before, len(page.Items), err)
			}

			report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
			if err != nil {
				t.Fatalf("import restore: %v", err)
			}
			if report.Archive.SHA256() != archive.SHA256() || report.Restored.Head.String() != "1" || report.IdempotencyKey != profile.IdempotencyKey {
				t.Fatalf("restore report=%+v", report)
			}
			recovered, err := RecoverImport(ctx, store, profile, Verification{ArchiveSHA256: profile.ArchiveDigest, DerivedSHA256: profile.DerivedDigest})
			if err != nil || !recovered.Recovered || recovered.Restored.Head.String() != "1" {
				t.Fatalf("import recovery=%+v err=%v", recovered, err)
			}
			altered := profile
			// Keep the G4 lookup tuple unchanged so this refusal exercises the
			// retained full profile hash rather than a principal lookup miss.
			altered.Admission.AdmissionDigest = strings.Repeat("d", 64)
			if _, err = RecoverImport(ctx, store, altered, Verification{ArchiveSHA256: profile.ArchiveDigest, DerivedSHA256: profile.DerivedDigest}); !errors.Is(err, driver.ErrConflict) {
				t.Fatalf("altered import profile accepted: %v", err)
			}
			after, err := store.Meta(ctx)
			if err != nil || after.Head.String() != "1" {
				t.Fatalf("post-call meta=%+v err=%v", after, err)
			}
			reader, err = store.Read(ctx, driver.View{Incarnation: after.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			rows, err := reader.Query(ctx, driver.Query{Filter: map[string]any{"project": "demo"}, Limit: 100})
			protected, protectedErr := reader.Query(ctx, driver.Query{Filter: map[string]any{"kind": "instance_config"}, Limit: 100})
			_ = reader.Close()
			beadRows := 0
			for _, row := range rows.Items {
				if row.Kind == "bead" {
					beadRows++
				}
			}
			projectRecords := map[string]bool{}
			for _, row := range protected.Items {
				projectRecords[row.Ref.ID] = true
			}
			if err != nil || protectedErr != nil || beadRows != 3 || len(rows.Items) != 4 || len(projectRecords) != 3 || !projectRecords[importBindingRecordID] || !projectRecords[importProjectRecordID("demo")] || !projectRecords[importProjectRecordID("empty")] {
				t.Fatalf("restored census beads=%d projectRows=%d total=%d protected=%v err=%v protectedErr=%v", beadRows, len(projectRecords), len(rows.Items), projectRecords, err, protectedErr)
			}
			var imported pipeline.Comment
			foundComment := false
			foundAgentState := false
			foundAwaitID := false
			for _, row := range rows.Items {
				if row.Kind != "bead" {
					continue
				}
				doc, decodeErr := pipeline.DecodeDocument(row.Body)
				if decodeErr != nil {
					t.Fatal(decodeErr)
				}
				// The canonical instant must come from the admitted source row,
				// with its exact source value still retained as provenance.
				var sourceStamp string
				for _, source := range archive.Records("bead") {
					if source.ID == doc.ID {
						sourceStamp, _ = source.Source.Fields["created_at"].(string)
					}
				}
				wantCreatedAt, stampErr := time.Parse(time.RFC3339Nano, sourceStamp)
				var retainedStamp string
				provenanceErr := json.Unmarshal(doc.Metadata["beads.source.created_at"], &retainedStamp)
				if stampErr != nil || provenanceErr != nil || doc.CreatedAt == nil || !doc.CreatedAt.Equal(wantCreatedAt) || retainedStamp != sourceStamp {
					t.Fatalf("source creation instant/provenance not preserved for %s", doc.ID)
				}
				if len(doc.Comments) == 1 {
					imported, foundComment = doc.Comments[0], true
				}
				if doc.ID == agentStateID {
					for field, want := range opaque {
						raw, ok := doc.Metadata["beads."+field]
						encoded, marshalErr := json.Marshal(want)
						if !ok || marshalErr != nil || !bytes.Equal(raw, encoded) {
							t.Fatalf("%s metadata=%s want=%s (present=%v marshal=%v)", field, raw, encoded, ok, marshalErr)
						}
					}
					foundAgentState, foundAwaitID = true, true
				}
			}
			if !foundComment || imported.Origin != "import" || !reflect.DeepEqual(imported.Principal, profile.Admission.Actor) || imported.SourceAuthor != "fixture-commenter" || imported.SourceTime == nil || imported.Time.IsZero() {
				t.Fatalf("imported comment=%+v found=%v", imported, foundComment)
			}
			if !foundAgentState || !foundAwaitID {
				t.Fatalf("opaque metadata bead %s was not fully restored: agent_state=%v await_id=%v", agentStateID, foundAgentState, foundAwaitID)
			}

			ontology := service.NewOntology(nil)
			writer := pipeline.New(store, lease, pipeline.WithOntology(ontology))
			svc := service.New(store, lease, writer, service.WithOntology(ontology))
			claimMeta := model.WriteMeta{Project: "demo", Agent: "restore-workload", IdempotencyKey: strings.Repeat("d", 32)}
			claimed, err := svc.ClaimNextWithOutcome(ctx, profile.Admission.Actor, service.ClaimNextRequest{Project: "demo"}, claimMeta)
			if err != nil || claimed == nil || len(claimed.Commit.Results) != 1 || claimed.Commit.Results[0].Ref.ID != "demo-7" {
				t.Fatalf("claimNext=%+v err=%v", claimed, err)
			}
			applyMeta := model.WriteMeta{Method: "tx.apply", Project: "demo", Agent: "restore-workload", IdempotencyKey: strings.Repeat("e", 32)}
			changes := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID("demo-7", model.Natural64(0)), Fields: map[string]any{"description": "updated after import"}}}}}
			applied, err := writer.Apply(ctx, profile.Admission.Actor, changes, applyMeta)
			if err != nil || len(applied.Results) != 1 {
				t.Fatalf("tx.apply=%+v err=%v", applied, err)
			}
			replayed, err := writer.Apply(ctx, profile.Admission.Actor, changes, applyMeta)
			if err != nil || !reflect.DeepEqual(applied, replayed) {
				t.Fatalf("tx.apply replay applied=%+v replay=%+v err=%v", applied, replayed, err)
			}
			if _, err := reverseconverter.ReadBytes(data); err != nil {
				t.Fatalf("reverse read: %v", err)
			} else if converted, err := reverseconverter.ConvertArchive(data); err != nil {
				t.Fatalf("reverse convert: %v", err)
			} else {
				decoder := json.NewDecoder(bytes.NewReader(converted.JSONL))
				found := false
				for {
					var line struct {
						ID       string                     `json:"id"`
						Metadata map[string]json.RawMessage `json:"metadata"`
					}
					err := decoder.Decode(&line)
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					if line.ID != agentStateID {
						continue
					}
					for field, want := range opaque {
						got, ok := line.Metadata["beads."+field]
						encoded, marshalErr := json.Marshal(want)
						if !ok || marshalErr != nil || !bytes.Equal(got, encoded) {
							t.Fatalf("reverse %s metadata=%s want=%s (present=%v marshal=%v)", field, got, encoded, ok, marshalErr)
						}
					}
					found = true
				}
				if !found {
					t.Fatalf("reverse output omitted agent_state for %s", agentStateID)
				}
			}
		})
	}
}

type tamperedImportStore struct {
	driver.Store
	mode string
}

func (s tamperedImportStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	reader, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return tamperedImportReader{Reader: reader, mode: s.mode}, nil
}

type tamperedImportReader struct {
	driver.Reader
	mode string
}

func (r tamperedImportReader) Get(ctx context.Context, refs []driver.Ref) ([]driver.Record, error) {
	rows, err := r.Reader.Get(ctx, refs)
	if err != nil {
		return nil, err
	}
	if r.mode == "binding" {
		for i := range rows {
			if rows[i].Ref.ID == importBindingRecordID {
				rows[i].Body = []byte(`{"tampered":true}`)
			}
		}
	}
	if r.mode == "project" {
		for i := range rows {
			if strings.HasPrefix(rows[i].Ref.ID, "instance_config/restore-import/project/") {
				rows[i].Body = []byte(`{"tampered":true}`)
				break
			}
		}
	}
	return rows, nil
}

func (r tamperedImportReader) LoadAllocator(ctx context.Context, prefix string) (driver.OfflineAllocator, error) {
	allocators, ok := r.Reader.(driver.AllocatorReader)
	if !ok {
		return driver.OfflineAllocator{}, driver.ErrUnsupported
	}
	value, err := allocators.LoadAllocator(ctx, prefix)
	if err != nil {
		return driver.OfflineAllocator{}, err
	}
	if r.mode == "allocator" {
		value.RandomBytes = 15
	}
	return value, nil
}

func TestRecoverImportRefusesDurableBindingDriftReadOnly(t *testing.T) {
	data := admittedFixtureBytes(t)
	profile, _ := admittedImportProfile(t, data)
	check := Verification{ArchiveSHA256: profile.ArchiveDigest, DerivedSHA256: profile.DerivedDigest}
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			store, lease, cleanup := openTarget(t, kind)
			defer cleanup()
			ctx := context.Background()
			if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile); err != nil {
				t.Fatalf("import restore: %v", err)
			}
			before, err := store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, mode := range []string{"binding", "project", "allocator"} {
				t.Run(mode, func(t *testing.T) {
					_, err := RecoverImport(ctx, tamperedImportStore{Store: store, mode: mode}, profile, check)
					if !errors.Is(err, driver.ErrConflict) {
						t.Fatalf("tampered %s accepted: %v", mode, err)
					}
					after, metaErr := store.Meta(ctx)
					if metaErr != nil || after.Head != before.Head || after.Incarnation != before.Incarnation {
						t.Fatalf("read-only recovery changed meta before=%+v after=%+v err=%v", before, after, metaErr)
					}
				})
			}
		})
	}
}

func TestAdmittedImportRejectsUntrustedProfileBeforeWrite(t *testing.T) {
	data := admittedFixtureBytes(t)
	profile, _ := admittedImportProfile(t, data)
	profile.Admission.Actor = model.PrincipalRef{Kind: "system", ID: "plug-converter"}
	store, lease, cleanup := openTarget(t, "memory")
	defer cleanup()
	_, err := RestoreImportWithLease(context.Background(), bytes.NewReader(data), store, lease, profile)
	if err == nil || !errors.Is(err, driver.ErrConflict) && !strings.Contains(err.Error(), "import admission") {
		t.Fatalf("forged profile error=%v", err)
	}
	meta, metaErr := store.Meta(context.Background())
	if metaErr != nil || !meta.Head.IsZero() {
		t.Fatalf("forged profile mutated target meta=%+v err=%v", meta, metaErr)
	}
}

// Keep json imported in this file's test-only fixture helpers even when the
// compiler inlines the common decode helper differently across Bazel modes.
var _ = json.Valid

// Only the immutable source row supplies creation time. The public memory and
// SQLite regression above independently checks the stored canonical instant,
// exact source provenance and successful post-restore claim.
func TestRestoreCreationTimeRequiresTheActualSourceInstant(t *testing.T) {
	for _, test := range []struct {
		name   string
		value  any
		absent bool
		want   string
		refuse bool
	}{
		{name: "unknown-null", value: nil},
		{name: "unknown-absent", absent: true},
		{name: "offset-instant", value: "2026-01-01T01:00:00.000000001+01:00", want: "2026-01-01T00:00:00.000000001Z"},
		{name: "invalid-instant", value: "not-an-instant", refuse: true},
		{name: "not-text", value: 1, refuse: true},
		{name: "zero-instant", value: "0001-01-01T00:00:00Z", refuse: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Records returns detached rows; the checked-in archive stays exact.
			source := readFixture(t).Records("bead")[0]
			source.Source.Fields["created_at"] = test.value
			if test.absent {
				delete(source.Source.Fields, "created_at")
			}
			// Neither this different mirror nor a later source version proves
			// the missing creation instant. Only the original row field does.
			source.Body["created_at"] = "2040-01-01T00:00:00Z"
			source.Source.Fields["updated_at"] = "2040-01-01T00:00:00Z"
			before, err := json.Marshal(source)
			if err != nil {
				t.Fatal(err)
			}
			at, err := sourceCreationTime(source)
			after, marshalErr := json.Marshal(source)
			if marshalErr != nil || !bytes.Equal(before, after) {
				t.Fatal("source creation provenance was mutated")
			}
			if test.refuse {
				if err == nil {
					t.Fatal("invalid source creation instant accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if test.want == "" {
				if at != nil {
					t.Fatal("unknown creation time was invented")
				}
			} else if at == nil || at.Format(time.RFC3339Nano) != test.want {
				t.Fatalf("creation instant=%v want=%s", at, test.want)
			}
		})
	}
}
