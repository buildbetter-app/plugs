package s1b17b20

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// This adapter only schedules a real production write between the dump's
// metadata read and reader creation. Every returned row, version, changeset,
// identity and archive byte still comes from the real driver/product path.
type b19WriteBeforeReadStore struct {
	driver.Store
	write func()
}

func (s *b19WriteBeforeReadStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	if s.write != nil {
		write := s.write
		s.write = nil
		write()
	}
	return s.Store.Read(ctx, view)
}

type b19SnapshotProjection struct {
	Body     string
	Revision string
	Versions []string
	Changes  []string
}

type b19SnapshotRecord struct {
	Ref      model.Ref       `json:"ref"`
	Kind     string          `json:"kind"`
	Revision model.Revision  `json:"revision"`
	Body     json.RawMessage `json:"body"`
}

func TestB19DumpRetainsDeclaredSnapshotAcrossAConcurrentWrite(t *testing.T) {
	for _, history := range []string{"full", "head"} {
		t.Run(history, func(t *testing.T) {
			projections := map[string]b19SnapshotProjection{}
			for _, engine := range []string{"memory", "sqlite"} {
				t.Run(engine, func(t *testing.T) {
					h := openB17Harness(t, engine)
					created := b17Commit(t, h, model.ChangeSet{Ops: []model.Op{b17Create()}}, 101)
					ref := created.Results[0].Ref
					b17Commit(t, h, model.ChangeSet{Ops: []model.Op{b17Patch(ref, "declared snapshot")}}, 102)
					before := b17StateOf(t, h, ref)
					baseline, manifest, err := restore.Dump(h.ctx, h.store, history)
					if err != nil {
						t.Fatal(err)
					}
					scheduled := &b19WriteBeforeReadStore{Store: h.store, write: func() {
						commit := b17Commit(t, h, model.ChangeSet{Ops: []model.Op{b17Patch(ref, "later committed state")}}, 103)
						if commit.Revision.String() != "3" {
							t.Fatalf("interleaved write revision=%s", commit.Revision)
						}
					}}
					data, actualManifest, err := restore.Dump(h.ctx, scheduled, history)
					if err != nil {
						t.Fatal(err)
					}
					after := b17StateOf(t, h, ref)
					if scheduled.write != nil || after.Meta.Head.String() != "3" || len(after.Versions) != 3 || len(after.Log) != 3 || b17Document(t, after).Fields["title"] != "later committed state" {
						t.Fatalf("real concurrent write did not land exactly once: %+v", after.Meta)
					}
					if !bytes.Equal(data, baseline) || !reflect.DeepEqual(actualManifest, manifest) {
						b19LogSnapshotDifference(t, baseline, data)
						t.Fatalf("dump mixed a later write into declared head %s: before records/versions/changes=%d/%d/%d, actual=%d/%d/%d; native digests=%s/%s", manifest.NativeHead, manifest.RecordCount, manifest.VersionCount, manifest.ChangesetCount, actualManifest.RecordCount, actualManifest.VersionCount, actualManifest.ChangesetCount, manifest.NativeDigest, actualManifest.NativeDigest)
					}
					archive, err := dumpformat.Read(bytes.NewReader(data))
					if err != nil {
						t.Fatal(err)
					}
					if archive.SHA256() != b17Hash(string(data)) || actualManifest.NativeStoreID != string(before.Meta.StoreID) || actualManifest.NativeIncarnation != before.Meta.Incarnation || actualManifest.NativeHead != before.Meta.Head.String() {
						t.Fatal("archive identity/digest no longer binds the captured source snapshot")
					}
					rows := b19SnapshotMember[b19SnapshotRecord](t, archive, "records/native.ndjson")
					if len(rows) != 1 || rows[0].Ref != ref || rows[0].Revision.String() != "2" || !bytes.Equal(rows[0].Body, before.Body) {
						t.Fatalf("archive body differs from the real earlier state: %+v", rows)
					}
					versions := b19SnapshotMember[driver.Version](t, archive, "versions.ndjson")
					changes := b19SnapshotMember[model.Changeset](t, archive, "changesets.ndjson")
					wantVersions, wantChanges := before.Versions, before.Log
					if history == "head" {
						wantVersions, wantChanges = before.Versions[len(before.Versions)-1:], nil
					}
					if !reflect.DeepEqual(versions, wantVersions) || !reflect.DeepEqual(changes, wantChanges) {
						t.Fatal("archive changed the captured version bodies/patches, attribution or ordered changesets")
					}
					projection := b19SnapshotProjection{Body: string(rows[0].Body), Revision: rows[0].Revision.String()}
					for _, version := range versions {
						projection.Versions = append(projection.Versions, version.Revision.String())
					}
					for _, change := range changes {
						projection.Changes = append(projection.Changes, change.Revision.String())
					}
					projections[engine] = projection
					if history == "head" {
						targetEngine := "memory"
						if engine == "memory" {
							targetEngine = "sqlite"
						}
						b19RestoreCapturedSnapshot(t, targetEngine, data, archive, ref, before)
					}
				})
			}
			if len(projections) == 2 && !reflect.DeepEqual(projections["memory"], projections["sqlite"]) {
				t.Fatalf("paired stores exported different snapshot/history projections: %+v", projections)
			}
		})
	}
}

func b19LogSnapshotDifference(t *testing.T, before, after []byte) {
	t.Helper()
	left, leftErr := dumpformat.Read(bytes.NewReader(before))
	right, rightErr := dumpformat.Read(bytes.NewReader(after))
	if leftErr != nil || rightErr != nil {
		t.Logf("archive validation before=%v after=%v", leftErr, rightErr)
		return
	}
	for _, path := range []string{"records/native.ndjson", "versions.ndjson", "changesets.ndjson"} {
		a, _ := left.NativeMember(path)
		b, _ := right.NativeMember(path)
		if !bytes.Equal(a, b) {
			t.Logf("first changed snapshot member %s: before=%s after=%s", path, a, b)
			return
		}
	}
}

func TestB19DumpEmptySnapshotDoesNotResolveAsLatest(t *testing.T) {
	for _, history := range []string{"full", "head"} {
		for _, engine := range []string{"memory", "sqlite"} {
			t.Run(history+"/"+engine, func(t *testing.T) {
				h := openB17Harness(t, engine)
				baseline, manifest, err := restore.Dump(h.ctx, h.store, history)
				if err != nil || manifest.NativeHead != "0" {
					t.Fatalf("empty source archive=%+v err=%v", manifest, err)
				}
				scheduled := &b19WriteBeforeReadStore{Store: h.store, write: func() {
					commit := b17Commit(t, h, model.ChangeSet{Ops: []model.Op{b17Create()}}, 104)
					if commit.Revision.String() != "1" {
						t.Fatalf("interleaved create revision=%s", commit.Revision)
					}
				}}
				data, actualManifest, err := restore.Dump(h.ctx, scheduled, history)
				if err != nil {
					t.Fatal(err)
				}
				if scheduled.write != nil || !bytes.Equal(data, baseline) || !reflect.DeepEqual(actualManifest, manifest) {
					t.Fatalf("declared empty head 0 resolved as later state: %+v", actualManifest)
				}
				archive, err := dumpformat.Read(bytes.NewReader(data))
				if err != nil {
					t.Fatal(err)
				}
				if len(b19SnapshotMember[b19SnapshotRecord](t, archive, "records/native.ndjson")) != 0 || len(b19SnapshotMember[driver.Version](t, archive, "versions.ndjson")) != 0 || len(b19SnapshotMember[model.Changeset](t, archive, "changesets.ndjson")) != 0 {
					t.Fatal("empty snapshot invented records or history")
				}
				// A separate unpinned dump must observe the real committed create.
				// Thus the empty result above cannot pass because the write failed.
				latestData, latest, err := restore.Dump(h.ctx, h.store, history)
				if err != nil || latest.NativeHead != "1" || latest.RecordCount != 1 || latest.VersionCount != 1 || bytes.Equal(data, latestData) {
					t.Fatalf("latest dump did not observe the concurrent create: %+v err=%v", latest, err)
				}
			})
		}
	}
}

func b19SnapshotMember[T any](t *testing.T, archive *dumpformat.Archive, member string) []T {
	t.Helper()
	data, ok := archive.NativeMember(member)
	if !ok {
		t.Fatalf("archive omitted %s", member)
	}
	var result []T
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var row T
		if err := json.Unmarshal(line, &row); err != nil {
			t.Fatal(err)
		}
		result = append(result, row)
	}
	return result
}

func b19RestoreCapturedSnapshot(t *testing.T, engine string, data []byte, archive *dumpformat.Archive, ref model.Ref, source b17State) {
	t.Helper()
	target := openB17Harness(t, engine)
	before, err := target.store.Meta(target.ctx)
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes, err := json.Marshal(archive.Manifest())
	if err != nil {
		t.Fatal(err)
	}
	var manifest map[string]any
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	presence := restore.Presence{"mode": "presence_check", "challenge": "snapshot-test", "proof": "trusted-test-host"}
	manager := restore.NewManager(target.store, target.lease, restore.PresenceVerifierFunc(func(context.Context, string, string, restore.Presence) error { return nil }))
	status, err := manager.BeginAs(target.ctx, b17Actor, manifest, "snapshot", presence)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ChunkAs(target.ctx, b17Actor, status.RestoreID, 0, data, b17Hash(string(data)), presence); err != nil {
		t.Fatal(err)
	}
	status, err = manager.Finish(target.ctx, status.RestoreID, b17Hash(string(manifestBytes)), b17Actor, model.WriteMeta{Method: "admin.restore.finish", IdempotencyKey: "b19-captured-snapshot"}, presence)
	if err != nil || status.State != "complete" {
		t.Fatalf("snapshot restore=%+v err=%v", status, err)
	}
	check := func() {
		t.Helper()
		meta, err := target.store.Meta(target.ctx)
		if err != nil || meta.StoreID != source.Meta.StoreID || meta.Incarnation != status.Incarnation || meta.Incarnation == before.Incarnation || meta.Incarnation == source.Meta.Incarnation || meta.Head.String() != "1" || meta.HistoryFloor.String() != "1" {
			t.Fatalf("restored snapshot identity/floor=%+v err=%v", meta, err)
		}
		r, err := target.store.Read(target.ctx, driver.View{Incarnation: meta.Incarnation})
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		rows, err := r.Get(target.ctx, []driver.Ref{ref})
		if err != nil || len(rows) != 1 || rows[0].Revision.String() != "1" || !bytes.Equal(rows[0].Body, source.Body) {
			t.Fatalf("restore substituted later content for declared snapshot: rows=%+v err=%v", rows, err)
		}
		doc, err := pipeline.DecodeDocument(rows[0].Body)
		if err != nil || doc.Fields["title"] != "declared snapshot" {
			t.Fatalf("restored product document=%+v err=%v", doc, err)
		}
		versions, err := r.Versions(target.ctx, ref, driver.PageReq{Limit: 10})
		if err != nil || len(versions.Items) != 1 || !versions.Items[0].Base || !bytes.Equal(versions.Items[0].Body, source.Body) {
			t.Fatalf("restored snapshot base=%+v err=%v", versions, err)
		}
		zero := model.Natural64(0)
		var truncated driver.ErrTruncated
		if _, err := target.store.Read(target.ctx, driver.View{Incarnation: meta.Incarnation, At: &zero}); !errors.As(err, &truncated) {
			t.Fatalf("snapshot invented discarded history: %v", err)
		}
		var old driver.ErrIncarnationChanged
		if _, err := target.store.Read(target.ctx, driver.View{Incarnation: source.Meta.Incarnation}); !errors.As(err, &old) {
			t.Fatalf("source incarnation survived snapshot restore: %v", err)
		}
	}
	check()
	if engine == "sqlite" {
		target.close(t)
		target.store, err = (sqlite.Driver{}).Open(target.ctx, driver.Config{DSN: target.path})
		if err != nil {
			t.Fatal(err)
		}
		check()
	}
}
