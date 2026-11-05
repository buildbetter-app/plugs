package nightlyrestore_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
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

type census struct {
	Counts       map[string]int            `json:"counts"`
	IDs          []string                  `json:"ids"`
	Records      map[string]string         `json:"record_sha256"`
	Versions     map[string]string         `json:"version_sha256"`
	Dependencies []string                  `json:"dependencies"`
	Source       []dumpformat.Project      `json:"source_allocators"`
	Target       []driver.OfflineAllocator `json:"target_allocators"`
	Members      map[string]string         `json:"archive_member_sha256"`
	Archive      string                    `json:"archive_sha256"`
}

type outcome struct {
	Driver      string  `json:"driver"`
	Restore     int     `json:"fresh_restore"`
	Status      string  `json:"status"`
	Boundary    string  `json:"boundary,omitempty"`
	Incarnation string  `json:"incarnation"`
	Census      *census `json:"census,omitempty"`
}

func same(t *testing.T, family string, got, want any) {
	t.Helper()
	if !bytes.Equal(jsonBytes(t, got), jsonBytes(t, want)) {
		t.Fatalf("%s differs: got %s; want %s", family, jsonBytes(t, got), jsonBytes(t, want))
	}
}

// Derive the adopted public-string shape from the archive, independently of
// restore lowering. Secret and non-string rows retain the existing raw oracle.
func expectedPublicMemory(t *testing.T, source dumpformat.Record, archiveSHA string) (map[string]any, bool) {
	t.Helper()
	content, eligible := source.Body["value"].(string)
	if source.Kind != "memory" || source.Body["classification"] != "public" || !eligible {
		return nil, false
	}
	key, ok := source.Body["key"].(string)
	if !ok || strings.TrimSpace(key) == "" {
		t.Fatal("public source memory lacks its admitted key")
	}
	return map[string]any{
		"id": "source/memory/" + source.ID, "key": strings.TrimSpace(key), "content": content,
		"metadata": map[string]any{
			"plug.restore.source_id": source.ID, "plug.restore.source_table": source.Source.Table,
			"plug.restore.source": source.Source.Fields, "plug.restore.archive_sha256": archiveSHA,
		},
	}, true
}

func profile(t *testing.T, data []byte) driver.RestoreImportProfile {
	t.Helper()
	i, err := converterroundtrip.InspectImport(context.Background(), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return driver.RestoreImportProfile{
		Admission:     driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "n1-test-restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)},
		SourceStoreID: i.SourceStoreID, Method: "offline.restore.import", IdempotencyKey: strings.Repeat("b", 32),
		ArchiveDigest: i.ArchiveSHA256, DerivedDigest: i.DerivedSHA256, HistoryMode: driver.RestoreHistoryModeSnapshot,
	}
}

func allRows(t *testing.T, reader driver.Reader) []driver.Record {
	t.Helper()
	var rows []driver.Record
	var cursor *driver.Cursor
	seen := map[string]bool{}
	for {
		page, err := reader.Query(context.Background(), driver.Query{Limit: 7, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range page.Items {
			if seen[row.Ref.ID] {
				t.Fatal("duplicate identity during complete paginated census")
			}
			seen[row.Ref.ID] = true
			rows = append(rows, row)
		}
		if page.NextCursor == nil {
			return rows
		}
		if len(page.Items) == 0 || len(rows) > 1000 || reflect.DeepEqual(cursor, page.NextCursor) {
			t.Fatal("census pagination did not advance")
		}
		cursor = page.NextCursor
	}
}

func canonicalBody(t *testing.T, row driver.Record, actor model.PrincipalRef) []byte {
	t.Helper()
	if row.Kind == "bead" {
		doc, err := pipeline.DecodeDocument(row.Body)
		if err != nil {
			t.Fatal(err)
		}
		for i := range doc.Comments {
			comment := &doc.Comments[i]
			if comment.Time.IsZero() || comment.SourceTime == nil || comment.SourceTime.IsZero() || comment.Origin != "import" || !reflect.DeepEqual(comment.Principal, actor) {
				t.Fatal("imported comment attribution/time lost")
			}
			// Restore execution times are expected to differ. All source time,
			// text, source author and admitted actor fields remain compared.
			comment.Time = time.Time{}
		}
		return jsonBytes(t, doc)
	}
	var value any
	dec := json.NewDecoder(bytes.NewReader(row.Body))
	dec.UseNumber()
	if err := dec.Decode(&value); err != nil {
		t.Fatal(err)
	}
	return jsonBytes(t, value)
}

func snapshotArchiveMembers(body []byte, archive *dumpformat.Archive, p driver.RestoreImportProfile) (map[string]dumpformat.File, error) {
	var evidence struct {
		Archive string                     `json:"archive_sha256"`
		Derived string                     `json:"derived_sha256"`
		Members map[string]dumpformat.File `json:"members"`
		Profile string                     `json:"profile"`
		Source  string                     `json:"source_store"`
	}
	if err := json.Unmarshal(body, &evidence); err != nil {
		return nil, err
	}
	if evidence.Archive != archive.SHA256() || evidence.Derived != p.DerivedDigest {
		return nil, fmt.Errorf("retained archive/derived digest differs")
	}
	if evidence.Source != string(p.SourceStoreID) {
		return nil, fmt.Errorf("retained archive source_store differs: got %q; want %q", evidence.Source, p.SourceStoreID)
	}
	if evidence.Profile != "builtin-head/1" {
		return nil, fmt.Errorf("retained archive profile differs: got %q; want builtin-head/1", evidence.Profile)
	}
	return evidence.Members, nil
}

func collect(t *testing.T, store driver.Store, archive *dumpformat.Archive, p driver.RestoreImportProfile) census {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if meta.StoreID != p.SourceStoreID || meta.Head.String() != "1" || meta.HistoryFloor.String() != "1" {
		t.Fatalf("restored identity/head/floor: %+v", meta)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got := census{Counts: map[string]int{}, Records: map[string]string{}, Versions: map[string]string{}, Members: map[string]string{}, Source: []dumpformat.Project{}, Target: []driver.OfflineAllocator{}, Dependencies: []string{}, Archive: archive.SHA256()}
	byID := map[string]driver.Record{}
	for _, row := range allRows(t, r) {
		byID[row.Ref.ID] = row
		got.IDs = append(got.IDs, row.Ref.ID)
		got.Counts[row.Kind]++
		if row.Revision.String() != "1" || !row.Ref.Branch.IsZero() {
			t.Fatal("snapshot record has unexpected revision/branch")
		}
		got.Records[row.Ref.ID] = digest(canonicalBody(t, row, p.Admission.Actor))
		v, err := r.Versions(ctx, row.Ref, driver.PageReq{Limit: 2})
		if err != nil || len(v.Items) != 1 || v.NextCursor != nil {
			t.Fatalf("complete restore-version census for %s: %v", row.Ref.ID, err)
		}
		version := v.Items[0]
		if !version.Base || version.Deleted || version.Revision.String() != "1" || !reflect.DeepEqual(version.Ref, row.Ref) || !bytes.Equal(version.Body, row.Body) || version.CreatedAt.IsZero() || !reflect.DeepEqual(version.Principal, p.Admission.Actor) {
			t.Fatalf("restore base version differs from live record %s", row.Ref.ID)
		}
		got.Versions[row.Ref.ID] = got.Records[row.Ref.ID]
	}
	sort.Strings(got.IDs)
	wantCounts := map[string]int{"restore-archive": 1, "instance_config": len(archive.Manifest().Projects) + 1}
	wantIDs := []string{"restore/archive", "instance_config/restore-import/binding"}
	for _, kind := range archive.Manifest().Kinds {
		wantCounts[kind.Name] = kind.Count
		for _, source := range archive.Records(kind.Name) {
			id := source.ID
			if kind.Name != "bead" {
				id = "source/" + kind.Name + "/" + id
			}
			wantIDs = append(wantIDs, id)
			row, ok := byID[id]
			if !ok || row.Kind != kind.Name {
				t.Fatalf("source identity/kind absent: %s", id)
			}
			if kind.Name == "bead" {
				doc, err := pipeline.DecodeDocument(row.Body)
				if err != nil {
					t.Fatal(err)
				}
				same(t, "source fields", doc.Metadata["beads.source_fields"], source.Source.Fields)
				same(t, "labels", append([]string{}, doc.Labels...), source.Body["labels"])
				for _, field := range []string{"title", "description", "priority"} {
					if value := source.Body[field]; value != nil {
						same(t, field, doc.Fields[field], value)
					}
				}
				wantStatus := map[string]string{"open": "todo", "closed": "done", "in_progress": "in_progress"}[fmt.Sprint(source.Source.Fields["status"])]
				if doc.ID != id || doc.Project != source.Project || doc.Type != source.Body["type"] || doc.Fields["status"] != wantStatus || doc.Lease != nil {
					t.Fatal("bead head fields differ from fixture")
				}
				for _, link := range doc.Links {
					got.Dependencies = append(got.Dependencies, id+"/"+link.Kind+"/"+link.To.ID)
				}
			} else {
				if want, projected := expectedPublicMemory(t, source, archive.SHA256()); projected {
					sameJSON(t, "complete public memory source/provenance", json.RawMessage(row.Body), want)
					continue
				}
				if err := snapshotArchiveContent(row.Body, source); err != nil {
					t.Fatal(err)
				}
				var body map[string]json.RawMessage
				if err := json.Unmarshal(row.Body, &body); err != nil {
					t.Fatal(err)
				}
				same(t, "source archival fields", body["source"], source.Source.Fields)
				for key, value := range source.Body {
					same(t, "source body/"+key, body[key], value)
				}
			}
		}
	}
	allocators, ok := r.(driver.AllocatorReader)
	if !ok {
		t.Fatal("allocator readback capability missing")
	}
	for _, source := range archive.Manifest().Projects {
		id := "instance_config/restore-import/project/" + source.Prefix
		wantIDs = append(wantIDs, id)
		var body struct {
			Metadata map[string]json.RawMessage `json:"metadata"`
			Target   driver.OfflineAllocator    `json:"targetAllocator"`
		}
		if err := json.Unmarshal(byID[id].Body, &body); err != nil {
			t.Fatal(err)
		}
		same(t, "source prefix/counters", body.Metadata["plug.restore.sourceAllocator"], source)
		got.Source = append(got.Source, source)
		actual, err := allocators.LoadAllocator(ctx, source.Prefix)
		if err != nil {
			t.Fatal(err)
		}
		want := driver.NewRandomAllocator(source.Prefix)
		same(t, "explicit target allocator policy", actual, want)
		same(t, "protected target allocator", body.Target, actual)
		got.Target = append(got.Target, actual)
	}
	sort.Strings(wantIDs)
	same(t, "counts", got.Counts, wantCounts)
	same(t, "IDs", got.IDs, wantIDs)
	var wantDependencies []string
	for _, edge := range archive.Records("edge") {
		wantDependencies = append(wantDependencies, fmt.Sprint(edge.Body["from"])+"/blocks/"+fmt.Sprint(edge.Body["to"]))
	}
	sort.Strings(wantDependencies)
	sort.Strings(got.Dependencies)
	same(t, "dependencies", got.Dependencies, wantDependencies)
	members, err := snapshotArchiveMembers(byID["restore/archive"].Body, archive, p)
	if err != nil {
		t.Fatal(err)
	}
	wantMembers := archive.Manifest().Files
	manifest, _ := archive.Member("manifest.json")
	wantMembers["manifest.json"] = dumpformat.File{SHA256: digest(manifest), Size: int64(len(manifest))}
	same(t, "archive inventory", members, wantMembers)
	for name, expected := range wantMembers {
		data, err := r.Blob(ctx, driver.Hash(expected.SHA256))
		source, _ := archive.Member(name)
		if err != nil || int64(len(data)) != expected.Size || digest(data) != expected.SHA256 || !bytes.Equal(data, source) {
			t.Fatalf("archive member/blob differs: %s", name)
		}
		got.Members[name] = digest(data)
	}
	principal, _ := driver.PrincipalKey(p.Admission.Actor)
	entry, err := r.ChangesetByKey(ctx, principal, p.AgentLabel(), p.Method, p.IdempotencyKey)
	if err != nil || entry == nil || entry.Kind != model.ChangesetRestore || len(entry.Changes) != len(got.IDs) || entry.Revision.String() != "1" {
		t.Fatal("restore changeset census differs")
	}
	return got
}

func unchanged(t *testing.T, store driver.Store, before driver.Meta) {
	t.Helper()
	after, err := store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	same(t, "refused target metadata", after, before)
	r, err := store.Read(context.Background(), driver.View{Incarnation: after.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if len(allRows(t, r)) != 0 {
		t.Fatal("refused restore wrote records")
	}
	if cs, err := r.Changesets(context.Background(), nil, 10); err != nil || len(cs) != 0 {
		t.Fatal("refused restore wrote changesets")
	}
}

func TestNightlyFreshStoreDiffs(t *testing.T) {
	ctx := context.Background()
	original := publicFixture(t)
	data := snapshotFixture(t, original)
	a, p := readArchive(t, data), profile(t, data)
	results := []outcome{}
	t.Cleanup(func() {
		status := "bounded_current_state"
		if t.Failed() {
			status = "failed"
		}
		report := map[string]any{"schema": "plug-nightly-restore-diffs/v1", "status": status, "source_fixture_sha256": fixtureSHA, "snapshot_fixture_sha256": digest(data), "results": results,
			"boundaries": []string{"original_unverified_history_refused", "snapshot_versions_are_restore_bases_not_source_history", "old_storage_cursor_requires_incarnation_reset", "memory.list_rpc_continuation_not_qualified", "public_synthetic_fixture_only_not_complete_private_census", "one_dispatch_is_not_a_three_night_streak"}}
		encoded := append(jsonBytes(t, report), '\n')
		if dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); dir != "" {
			if err := os.WriteFile(filepath.Join(dir, "nightly-restore-diffs.json"), encoded, 0600); err != nil {
				t.Error(err)
			}
		}
		t.Log(string(encoded))
	})
	var baseline *census
	incarnations := map[string]bool{}
	for _, kind := range []string{"memory", "sqlite", "postgres"} {
		for iteration := 1; iteration <= 2; iteration++ {
			t.Run(fmt.Sprintf("%s/fresh-%d", kind, iteration), func(t *testing.T) {
				store, reopen := freshStore(t, kind)
				before, err := store.Meta(ctx)
				if err != nil || !before.Head.IsZero() {
					t.Fatal("target is not fresh", err)
				}
				unchanged(t, store, before)
				corrupt := append([]byte(nil), data...)
				member, _ := a.Member("records/bead.ndjson")
				offset := bytes.Index(corrupt, member)
				if offset < 0 || len(member) == 0 {
					t.Fatal("fixture has no bead payload for corruption control")
				}
				corrupt[offset+len(member)/2] ^= 1
				if _, err := converterroundtrip.RestoreImport(ctx, bytes.NewReader(corrupt), store, p); err == nil {
					t.Fatal("damaged archive passed digest validation")
				}
				unchanged(t, store, before)
				for _, mode := range []driver.RestoreHistoryMode{"", driver.RestoreHistoryModePartial} {
					strict := p
					strict.HistoryMode = mode
					_, err := converterroundtrip.RestoreImport(ctx, bytes.NewReader(original), store, strict)
					var refused driver.ErrRestoreHistoryRefused
					if !errors.As(err, &refused) || (mode == "" && refused.Reason != driver.RestoreHistoryReasonAvailableUnverified) || (mode != "" && refused.Reason != driver.RestoreHistoryReasonNotYetSupported) {
						t.Fatalf("history boundary is not typed: %T %v", err, err)
					}
					unchanged(t, store, before)
				}
				restored, err := converterroundtrip.RestoreImport(ctx, bytes.NewReader(data), store, p)
				if err != nil {
					t.Fatal(err)
				}
				if restored.Restored.Incarnation == before.Incarnation || incarnations[restored.Restored.Incarnation] {
					t.Fatal("fresh restore did not create a distinct incarnation")
				}
				incarnations[restored.Restored.Incarnation] = true
				if stale, err := store.Read(ctx, driver.View{Incarnation: before.Incarnation}); err == nil {
					stale.Close()
					t.Fatal("pre-restore incarnation was accepted")
				} else {
					var changed driver.ErrIncarnationChanged
					if !errors.As(err, &changed) {
						t.Fatalf("cursor/reset boundary: %v", err)
					}
				}
				current, err := store.Read(ctx, driver.View{Incarnation: restored.Restored.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				_, err = current.Query(ctx, driver.Query{Limit: 1, Cursor: &driver.Cursor{
					Incarnation: before.Incarnation,
					Method:      "query",
				}})
				if closeErr := current.Close(); closeErr != nil {
					t.Fatal(closeErr)
				}
				var staleCursor driver.ErrIncarnationChanged
				if !errors.As(err, &staleCursor) {
					t.Fatalf("stale pagination cursor was not refused with incarnation reset: %v", err)
				}
				got := collect(t, store, a, p)
				if baseline == nil {
					baseline = &got
				} else {
					same(t, "independent fresh-store census/digests", got, *baseline)
				}
				if kind != "memory" {
					if err := store.Close(); err != nil {
						t.Fatal(err)
					}
					same(t, "durable reopen census", collect(t, reopen(), a, p), got)
				}
				results = append(results, outcome{Driver: kind, Restore: iteration, Status: "pass", Incarnation: restored.Restored.Incarnation, Census: &got})
			})
		}
	}
	if len(results) != 6 || baseline == nil {
		t.Fatal("nightly matrix did not complete")
	}
}
