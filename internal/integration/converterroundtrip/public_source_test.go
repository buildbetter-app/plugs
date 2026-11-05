package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func publicSourceFixture(t *testing.T, complete bool) ([]byte, *dumpformat.Archive) {
	t.Helper()
	file, digest := "public-partial-990eb800.tar", "2dd0bc5cb41ee5ae410659fdff91c4ee6b865098b371a2e1be9b819187271e53"
	if complete {
		file, digest = "public-complete-root-990eb800.tar", "a2d933a7a54b53303d33a20d5b96eaac0a85c079c5b08cae553aa2a654d820b5"
	}
	data, err := os.ReadFile("testdata/" + file)
	if err != nil || hashBytes(data) != digest {
		t.Fatalf("producer bytes changed: %v", err)
	}
	a, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	h, ok := a.SourceHistory()
	if !ok || h.Range.Status != "verified" || h.Range.Complete != complete {
		t.Fatal("actual source range changed")
	}
	if complete {
		member, _ := a.Member(dumpformat.SourceHistoryMember)
		if hashBytes(member) != "049b7602c5360e6be2b69ad25a5fae0cb77b7a1fc2af2b18da9a41eb71242b62" || h.Range.First != 1 || h.Range.Head != 7 || len(h.Commits) != 7 || len(h.Commits[0].Parents) != 0 || len(h.Range.Baseline)+len(h.Range.BaselineEvents)+len(h.Range.Gaps) != 0 {
			t.Fatal("real complete-root evidence changed")
		}
		for i, frame := range h.Range.Frames {
			if len(frame.Events) != []int{0, 0, 0, 1, 2, 3, 4}[i] || len(frame.Records) != []int{0, 1, 1, 2, 2, 2, 2}[i] {
				t.Fatal("actual source image/event census changed")
			}
		}
		if len(a.Records("bead")) != 1 || a.Records("bead")[0].ID != "demo-42v" || a.Records("bead")[0].Body["pinned"] != "0" || len(a.Records("comment")) != 0 {
			t.Fatal("actual issue encoding or comment-row boundary changed")
		}
	} else if h.Range.First != 4 || h.Range.Head != 4 || !reflect.DeepEqual(h.Range.Gaps, []dumpformat.HistoryGap{{First: 1, Last: 1, Reason: "not-retained"}, {First: 2, Last: 2, Reason: "unmappable"}, {First: 3, Last: 3, Reason: "not-retained"}}) {
		t.Fatal("actual partial gaps changed")
	}
	for _, bead := range a.Records("bead") {
		payload, _ := bead.Source.Fields["payload"].(map[string]any)
		if payload["n"] != json.Number("9007199254740992") || bead.Source.Fields["timeout_ns"] != json.Number("9007199254740993") {
			t.Fatal("actual stored JSON/BIGINT replaced with requested values")
		}
	}
	return data, a
}

func TestPublicDoltSourceRestoreHistoryAndLiveReverseAllDrivers(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, test := range []struct {
			name        string
			complete    bool
			mode        driver.RestoreHistoryMode
			head, floor string
		}{
			{"complete-default", true, "", "8", "0"},
			{"complete-full", true, driver.RestoreHistoryModeFull, "8", "0"},
			{"complete-snapshot", true, driver.RestoreHistoryModeSnapshot, "1", "1"},
			{"partial", false, driver.RestoreHistoryModePartial, "3", "1"},
			{"partial-snapshot", false, driver.RestoreHistoryModeSnapshot, "1", "1"},
		} {
			t.Run(engine+"/"+test.name, func(t *testing.T) {
				ctx := context.Background()
				data, source := publicSourceFixture(t, test.complete)
				p, _ := admittedImportProfile(t, data)
				p.HistoryMode = test.mode
				when := time.Now().UTC()
				var err error
				if test.mode != driver.RestoreHistoryModeSnapshot {
					p, err = BindHistoryImport(ctx, bytes.NewReader(data), p, when)
					if err != nil {
						t.Fatal(err)
					}
				}
				store, lease, cleanup := openReverseTarget(t, engine)
				defer cleanup()
				old, err := store.Meta(ctx)
				if err != nil {
					t.Fatal(err)
				}
				report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, p)
				if err != nil {
					t.Fatal(err)
				}
				if report.Restored.Head.String() != test.head || report.Restored.HistoryFloor.String() != test.floor || report.Restored.Incarnation == old.Incarnation || string(report.Restored.StoreID) != source.Manifest().StoreID {
					t.Fatalf("target boundary: %+v", report.Restored)
				}
				var stale driver.ErrIncarnationChanged
				if _, err = store.Read(ctx, driver.View{Incarnation: old.Incarnation}); !errors.As(err, &stale) {
					t.Fatalf("stale incarnation accepted: %v", err)
				}
				r, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				assertPublicStoredImage(t, r, source, p.Admission.Actor)
				assertPublicHistoryBinding(t, r, source, p, report.Restored, when)
				// Offline history imports establish live allocator coverage only
				// at their actual final target head, never at source history heads.
				allocatorSnapshot, err := r.(driver.AllocatorSnapshotReader).AllocatorSnapshot(ctx)
				if err != nil || allocatorSnapshot.At != report.Restored.Head || allocatorSnapshot.CoverageFloor != report.Restored.Head || allocatorSnapshot.Incarnation != report.Restored.Incarnation {
					t.Fatalf("offline history final-head allocator coverage: %+v %v", allocatorSnapshot, err)
				}
				allocatorBytes, err := driver.EncodeAllocatorSnapshot(allocatorSnapshot)
				if err != nil {
					t.Fatal(err)
				}
				allocators, ok := r.(driver.AllocatorReader)
				if !ok {
					t.Fatal("allocator readback unavailable")
				}
				for _, project := range source.Manifest().Projects {
					got, err := allocators.LoadAllocator(ctx, project.Prefix)
					if err != nil || !reflect.DeepEqual(got, driver.NewRandomAllocator(project.Prefix)) {
						t.Fatalf("source allocator became live identity: %+v %v", got, err)
					}
					rows, err := r.Get(ctx, []model.Ref{model.RefByID(importProjectRecordID(project.Prefix), model.Natural64(0))})
					var saved importProjectMetadata
					if err != nil || len(rows) != 1 || json.Unmarshal(rows[0].Body, &saved) != nil {
						t.Fatal("protected source allocator evidence missing")
					}
					want, _ := json.Marshal(project)
					if !bytes.Equal(saved.Metadata["plug.restore.sourceAllocator"], want) {
						t.Fatal("source allocator provenance changed")
					}
				}
				r.Close()
				if test.mode != driver.RestoreHistoryModeSnapshot {
					head, _ := report.Restored.Head.Uint64()
					at := model.Natural64(head - 1)
					oldReader, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &at})
					if err != nil {
						t.Fatal(err)
					}
					_, err = oldReader.(driver.AllocatorSnapshotReader).AllocatorSnapshot(ctx)
					oldReader.Close()
					var unavailable *driver.NativeAllocatorSnapshotError
					if !errors.As(err, &unavailable) || unavailable.Reason != driver.NativeAllocatorCoverageUnavailable {
						t.Fatalf("source history fabricated allocator coverage: %v", err)
					}
				}
				h, _ := source.SourceHistory()
				if test.mode == driver.RestoreHistoryModeSnapshot {
					at := model.Natural64(0)
					var truncated driver.ErrTruncated
					if _, err = store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &at}); !errors.As(err, &truncated) || truncated.Floor.String() != "1" {
						t.Fatalf("snapshot pre-floor accepted: %v", err)
					}
				} else {
					if h.Range.First > 1 {
						assertPublicSourceAsOf(t, store, report.Restored, source, h.Range.First-1, 1, p.Admission.Actor)
						at := model.Natural64(0)
						var truncated driver.ErrTruncated
						if _, err = store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &at}); !errors.As(err, &truncated) || truncated.Floor.String() != "1" {
							t.Fatalf("partial pre-floor accepted: %v", err)
						}
					}
					for _, frame := range h.Range.Frames {
						revision := frame.Sequence
						if h.Range.First > 1 {
							revision = frame.Sequence - h.Range.First + 2
						}
						assertPublicSourceAsOf(t, store, report.Restored, source, frame.Sequence, revision, p.Admission.Actor)
					}
				}
				baseline, err := ReverseRestored(ctx, store)
				if err != nil {
					t.Fatal(err)
				}
				assertSourceOpaque(t, baseline.Result.JSONL, source)
				if len(baseline.Result.Report.LostPostSwitchHistory) == 0 {
					t.Fatal("head conversion claimed history replay")
				}
				id := source.Records("bead")[0].ID
				writer := pipeline.New(store, lease, pipeline.WithOntology(service.NewOntology(nil)))
				edit := func(description, key string) {
					t.Helper()
					_, err := writer.Apply(ctx, p.Admission.Actor, model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID(id, model.Natural64(0)), Fields: map[string]any{"description": description}}}}}, model.WriteMeta{Method: "tx.apply", Project: "demo", IdempotencyKey: strings.Repeat(key, 32)})
					if err != nil {
						t.Fatal(err)
					}
				}
				edit("Public live reverse edit", "f")
				at := report.Restored.Head
				historical, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &at})
				if err != nil {
					t.Fatal(err)
				}
				retained, err := historical.(driver.AllocatorSnapshotReader).AllocatorSnapshot(ctx)
				historical.Close()
				if err != nil {
					t.Fatal(err)
				}
				retainedBytes, err := driver.EncodeAllocatorSnapshot(retained)
				if err != nil || !bytes.Equal(allocatorBytes, retainedBytes) {
					t.Fatalf("post-import write lost allocator baseline: %v", err)
				}
				before := reverseState(t, store)
				live, err := ReverseRestored(ctx, store)
				if err != nil || live.Snapshot.At == nil || live.Snapshot.At.String() != before.Meta.Head.String() || live.Snapshot.Incarnation != report.Restored.Incarnation || live.Result.Digest == baseline.Result.Digest {
					t.Fatalf("live as-of boundary failed: %v", err)
				}
				assertPublicReverseDescription(t, live.Result.JSONL, id, "Public live reverse edit")
				assertSourceOpaque(t, live.Result.JSONL, source)
				if !reflect.DeepEqual(before, reverseState(t, store)) {
					t.Fatal("reverse mutated the store")
				}
				captured, err := ReverseRestored(ctx, reverseReadHook{Store: store, afterRead: func() { edit("Concurrent later edit", "e") }})
				if err != nil || captured.Result.Digest != live.Result.Digest || captured.Snapshot.At == nil || *captured.Snapshot.At != *live.Snapshot.At {
					t.Fatalf("reverse mixed live snapshots: %v", err)
				}
				t.Logf("PUBLIC_SOURCE_CONTROL archive=%s engine=%s mode=%s source_complete=%t first=%d head=%d target_head=%s floor=%s incarnation=%s reverse_at=%s reverse_incarnation=%s baseline_sha256=%s live_sha256=%s history_loss=%q beads_reimport=WAIT beads_readback=WAIT", source.SHA256(), engine, p.HistoryMode, h.Range.Complete, h.Range.First, h.Range.Head, report.Restored.Head, report.Restored.HistoryFloor, report.Restored.Incarnation, live.Snapshot.At, live.Snapshot.Incarnation, baseline.Result.Digest, live.Result.Digest, live.Result.Report.LostPostSwitchHistory)
			})
		}
	}
}

func assertPublicSourceAsOf(t *testing.T, store driver.Store, meta driver.Meta, source *dumpformat.Archive, sequence, revision uint64, actor model.PrincipalRef) {
	t.Helper()
	image, err := source.HistoryImage(sequence)
	if err != nil {
		t.Fatal(err)
	}
	at := model.Natural64(revision)
	r, err := store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation, At: &at})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	assertPublicStoredImage(t, r, image, actor)
	t.Logf("PUBLIC_SOURCE_ASOF sequence=%d revision=%s incarnation=%s beads=%d comments=%d", sequence, at, meta.Incarnation, len(image.Records("bead")), len(image.Records("comment")))
}

func assertPublicStoredImage(t *testing.T, r driver.Reader, source *dumpformat.Archive, actor model.PrincipalRef) {
	t.Helper()
	ctx := context.Background()
	rows, err := r.Query(ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000})
	if err != nil || rows.NextCursor != nil || len(rows.Items) != len(source.Records("bead")) {
		t.Fatalf("bead census changed: %v", err)
	}
	for _, bead := range source.Records("bead") {
		stored, err := r.Get(ctx, []model.Ref{model.RefByID(bead.ID, model.Natural64(0))})
		if err != nil || len(stored) != 1 {
			t.Fatalf("source identity missing: %v", err)
		}
		doc, err := pipeline.DecodeDocument(stored[0].Body)
		if err != nil {
			t.Fatal(err)
		}
		for _, field := range []string{"title", "description", "priority"} {
			if !sameReverseJSON(doc.Fields[field], bead.Body[field]) {
				t.Fatalf("source field %s changed", field)
			}
		}
		if !sameReverseJSON(append([]string{}, doc.Labels...), bead.Body["labels"]) {
			t.Fatal("source labels changed")
		}
		for _, field := range dumpformat.OpaqueIssueFields() {
			want, present := bead.Source.Fields[field]
			encoded, err := json.Marshal(want)
			if !present || err != nil || !bytes.Equal(encoded, doc.Metadata["beads."+field]) {
				t.Fatalf("actual opaque %s changed", field)
			}
		}
		raw, _ := json.Marshal(bead.Source.Fields)
		if !bytes.Equal(raw, doc.Metadata["beads.source_fields"]) {
			t.Fatal("raw source field provenance changed")
		}
		for _, comment := range doc.Comments {
			if comment.Origin != "import" || !reflect.DeepEqual(comment.Principal, actor) || comment.SourceAuthor == "" || comment.SourceTime == nil || comment.Time.IsZero() {
				t.Fatal("source comment became Plug authority")
			}
		}
	}
	for _, kind := range []string{"edge", "comment", "memory", "config"} {
		for _, row := range source.Records(kind) {
			got, err := r.Get(ctx, []model.Ref{model.RefByID("source/"+kind+"/"+row.ID, model.Natural64(0))})
			if kind == "memory" && row.Body["classification"] == "public" {
				if content, eligible := row.Body["value"].(string); eligible {
					key, ok := row.Body["key"].(string)
					if !ok || strings.TrimSpace(key) == "" {
						t.Fatal("public source memory lacks its admitted key")
					}
					want := map[string]any{
						"id": "source/memory/" + row.ID, "key": strings.TrimSpace(key), "content": content,
						"metadata": map[string]any{
							"plug.restore.source_id": row.ID, "plug.restore.source_table": row.Source.Table,
							"plug.restore.source": row.Source.Fields, "plug.restore.archive_sha256": source.SHA256(),
						},
					}
					if err != nil || len(got) != 1 {
						t.Fatalf("public memory source missing: %v", err)
					}
					var actual map[string]any
					decode(t, got[0].Body, &actual)
					if !sameReverseJSON(actual, want) {
						t.Fatal("complete public memory source/provenance changed")
					}
					continue
				}
			}
			var body struct {
				Source json.RawMessage `json:"source"`
			}
			want, _ := json.Marshal(row.Source.Fields)
			if err != nil || len(got) != 1 || json.Unmarshal(got[0].Body, &body) != nil || !bytes.Equal(want, body.Source) {
				t.Fatalf("raw %s source changed: %v", kind, err)
			}
		}
	}
}

func assertPublicHistoryBinding(t *testing.T, r driver.Reader, source *dumpformat.Archive, p driver.RestoreImportProfile, meta driver.Meta, when time.Time) {
	t.Helper()
	ctx := context.Background()
	rows, err := r.Get(ctx, []model.Ref{model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0))})
	if err != nil {
		t.Fatal(err)
	}
	if p.HistoryMode == driver.RestoreHistoryModeSnapshot {
		if len(rows) != 0 {
			t.Fatal("snapshot claimed source replay")
		}
		return
	}
	h, _ := source.SourceHistory()
	var evidence driver.HistoryImportEvidence
	if len(rows) != 1 || json.Unmarshal(rows[0].Body, &evidence) != nil {
		t.Fatal("source mapping missing")
	}
	if evidence.Complete != h.Range.Complete || evidence.First != h.Range.First || evidence.Head != h.Range.Head || evidence.Floor != meta.HistoryFloor || len(evidence.Mapping) != len(h.Range.Frames) {
		t.Fatalf("source mapping changed: %+v", evidence)
	}
	gaps := []driver.SourceHistoryGap{}
	for _, g := range h.Range.Gaps {
		gaps = append(gaps, driver.SourceHistoryGap{First: g.First, Last: g.Last, Reason: g.Reason})
	}
	if !reflect.DeepEqual(evidence.Gaps, gaps) {
		t.Fatalf("exact gap reasons changed: %+v", evidence.Gaps)
	}
	seen := map[string]bool{}
	for _, event := range h.Range.BaselineEvents {
		seen[dumpformat.SourceEventID(event)] = true
	}
	for i, frame := range h.Range.Frames {
		ids := []string{}
		for _, event := range frame.Events {
			id := dumpformat.SourceEventID(event)
			if !seen[id] {
				ids = append(ids, id)
			}
			seen[id] = true
		}
		sort.Strings(ids)
		wantRev := frame.Sequence
		if h.Range.First > 1 {
			wantRev = frame.Sequence - h.Range.First + 2
		}
		want := driver.SourceImportRevision{Sequence: frame.Sequence, Revision: model.Natural64(wantRev), CommitIDs: []string{frame.CommitID}, EventIDs: ids}
		if !reflect.DeepEqual(evidence.Mapping[i], want) {
			t.Fatalf("source commit/event identity mapping changed: %+v", evidence.Mapping[i])
		}
	}
	var cursor *driver.Cursor
	if !meta.HistoryFloor.IsZero() {
		cursor = &driver.Cursor{Incarnation: meta.Incarnation, Revision: meta.HistoryFloor}
	}
	entries, err := r.Changesets(ctx, cursor, 100)
	if err != nil || len(entries) != len(h.Range.Frames)+1 {
		t.Fatalf("source changeset census changed: %d %v", len(entries), err)
	}
	for _, entry := range entries {
		if !reflect.DeepEqual(entry.Principal, p.Admission.Actor) || entry.Kind != model.ChangesetRestore || !entry.Time.Equal(when) {
			t.Fatal("source author/time promoted into Plug authority")
		}
	}
	if h.Range.Complete {
		versions, err := r.Versions(ctx, model.RefByID("demo-42v", model.Natural64(0)), driver.PageReq{Limit: 100})
		if err != nil || len(versions.Items) != 3 || !versions.Items[0].Base {
			t.Fatalf("source versions flattened or fabricated: %+v %v", versions, err)
		}
		for i, v := range versions.Items {
			if v.Revision != model.Natural64(uint64(i+4)) || !v.CreatedAt.Equal(when) || !reflect.DeepEqual(v.Principal, p.Admission.Actor) {
				t.Fatal("source version attribution/revision changed")
			}
		}
	}
}

func assertPublicReverseDescription(t *testing.T, data []byte, id, want string) {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(data))
	for {
		var row struct{ ID, Description string }
		if err := d.Decode(&row); err == io.EOF {
			t.Fatal("live bead missing")
			return
		} else if err != nil {
			t.Fatal(err)
		}
		if row.ID == id {
			if row.Description != want {
				t.Fatal("stale source substituted for live readback")
			}
			return
		}
	}
}

func TestPublicDoltPartialStrictRefusalAtomicAllDrivers(t *testing.T) {
	data, _ := publicSourceFixture(t, false)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			store, lease, cleanup := openReverseTarget(t, engine)
			defer cleanup()
			p, _ := admittedImportProfile(t, data)
			for _, populated := range []bool{false, true} {
				if populated {
					writer := pipeline.New(store, lease, pipeline.WithOntology(service.NewOntology(nil)))
					svc := service.New(store, lease, writer)
					if _, err := svc.Create(context.Background(), p.Admission.Actor, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": "Existing target", "status": "todo"}}, model.WriteMeta{Project: "demo", IdempotencyKey: strings.Repeat("a", 32)}); err != nil {
						t.Fatal(err)
					}
				}
				before := snapshot(t, store)
				for _, mode := range []driver.RestoreHistoryMode{"", driver.RestoreHistoryModeFull} {
					p.HistoryMode = mode
					_, err := RestoreImportWithLease(context.Background(), bytes.NewReader(data), store, lease, p)
					var refused driver.ErrRestoreHistoryRefused
					if !errors.As(err, &refused) || refused.Mode != driver.RestoreHistoryModeFull || refused.Reason != driver.RestoreHistoryReasonNonContiguous {
						t.Fatalf("strict source refusal changed: %T %v", err, err)
					}
					if !reflect.DeepEqual(before, snapshot(t, store)) {
						t.Fatal("strict refusal changed records, versions, changesets or target identity")
					}
				}
			}
		})
	}
}
