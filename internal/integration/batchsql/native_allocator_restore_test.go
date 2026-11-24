package batchsql_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func nativeSHA(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }

// Seed real continuation state through the admitted offline transaction. Some
// counters deliberately have no surviving record; no ID census can recover
// these exact values, including values beyond float64's integer range.
func nativeAllocatorSource(t *testing.T, f *fixture) (model.Ref, model.Ref) {
	t.Helper()
	profiles := []driver.OfflineAllocator{
		{Prefix: "demo", Mode: driver.AllocatorSequential, LastID: 41, Children: map[string]uint64{"deleted-project-parent": 9007199254740993, "exhausted-project-parent": ^uint64(0)}},
		driver.NewHashAllocator("hash", 3, 8, 0.25), driver.NewRandomAllocator("random"),
	}
	locks := driver.LockSet{}
	for _, profile := range profiles {
		key, err := driver.AllocatorLock(profile.Prefix)
		if err != nil {
			t.Fatal(err)
		}
		locks = append(locks, driver.LockRequest{Key: key, Mode: driver.LockUpdate})
	}
	sort.Slice(locks, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(locks[i].Key)
		b, _ := driver.EncodeLockKey(locks[j].Key)
		return a < b
	})
	tx, err := f.store.Write(f.ctx, f.lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, err = tx.Lock(f.ctx, locks); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(f.ctx); err != nil {
		t.Fatal(err)
	}
	meta, err := tx.(driver.OfflineRestoreTx).PrepareOfflineRestore(f.ctx, "allocator-native-source")
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.(driver.AllocatorContinuation).RestoreAllocators(f.ctx, profiles); err != nil {
		t.Fatal(err)
	}
	keys := make([]driver.LockKey, len(locks))
	for i := range locks {
		keys[i] = locks[i].Key
	}
	if _, err = tx.Commit(f.ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetRestore, Principal: model.PrincipalRef{Kind: "system", ID: "plug-converter"}, Method: "offline.restore", IdempotencyKey: "native-source-seed", RequestHash: strings.Repeat("a", 64), LockKeys: keys}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	project := hierarchyApply(t, f, hierarchyCreate("project-parent", nil, "demo")).Results[0].Ref
	plain := hierarchyApply(t, f, hierarchyCreate("plain-parent", nil, "")).Results[0].Ref
	if project.ID != "demo-42" {
		t.Fatalf("actual source project allocation: %+v", project)
	}
	for _, p := range []struct {
		ref     model.Ref
		project string
	}{{project, "demo"}, {plain, ""}} {
		first := hierarchyApply(t, f, hierarchyCreate("surviving-child", &p.ref, p.project)).Results[0].Ref
		second := hierarchyApply(t, f, hierarchyCreate("deleted-highest-child", &p.ref, p.project)).Results[0].Ref
		if first.ID != p.ref.ID+".1" || second.ID != p.ref.ID+".2" {
			t.Fatalf("source child allocation: %+v %+v", first, second)
		}
		hierarchyApply(t, f, childDelete(second))
	}
	deleted := hierarchyApply(t, f, hierarchyCreate("deleted-parent", nil, "")).Results[0].Ref
	child := hierarchyApply(t, f, hierarchyCreate("deleted-parent-child", &deleted, "")).Results[0].Ref
	hierarchyApply(t, f, childDelete(child), childDelete(deleted))
	snapshot := observedAllocatorSnapshot(t, f, nil)
	if len(snapshot.Inventory.Projects) != 3 || len(snapshot.Inventory.Parents) != 2 {
		t.Fatalf("source omitted retained state: %+v", snapshot)
	}
	return project, plain
}

func nativeArchiveSnapshot(t *testing.T, data []byte) (*dumpformat.Archive, driver.AllocatorSnapshot) {
	t.Helper()
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	raw, ok := archive.NativeMember(dumpformat.NativeAllocatorMember)
	if !ok {
		t.Fatal("native dump omitted allocator snapshot")
	}
	snapshot, err := driver.DecodeAllocatorSnapshot(raw)
	if err != nil {
		t.Fatal(err)
	}
	manifest := archive.Manifest()
	if manifest.NativeAllocatorProfile != driver.NativeAllocatorVersion || manifest.NativeAllocatorDigest != nativeSHA(raw) || manifest.Files[dumpformat.NativeAllocatorMember].SHA256 != nativeSHA(raw) || manifest.NativeStoreID != string(snapshot.StoreID) || manifest.NativeIncarnation != snapshot.Incarnation || manifest.NativeHead != snapshot.At.String() {
		t.Fatalf("native allocator bindings: %+v %+v", manifest, snapshot)
	}
	return archive, snapshot
}

var nativeTestPresence = restore.Presence{"mode": "presence_check", "challenge": "native-allocator-test", "proof": "explicit-test-verifier"}

func nativeManager(t *testing.T, f *fixture, store driver.Store) *restore.Manager {
	t.Helper()
	t.Log("native allocator acceptance uses an explicit test PresenceVerifier; no production human-presence claim")
	return restore.NewManager(store, f.lease, restore.PresenceVerifierFunc(func(_ context.Context, method, digest string, p restore.Presence) error {
		if method == "" || len(digest) != 64 || !reflect.DeepEqual(p, nativeTestPresence) {
			return fmt.Errorf("native test presence binding refused")
		}
		return nil
	}))
}

func nativeUpload(t *testing.T, f *fixture, manager *restore.Manager, data []byte) (restore.RestoreStatus, string) {
	t.Helper()
	// Read the submitted manifest without pre-validating corrupt input: the
	// production Manager.Finish must classify malformed/hash errors itself.
	var submitted dumpformat.Manifest
	tr := tar.NewReader(bytes.NewReader(data))
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Name == "manifest.json" {
			if err := json.NewDecoder(tr).Decode(&submitted); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	raw, err := json.Marshal(submitted)
	if err != nil {
		t.Fatal(err)
	}
	var manifest map[string]any
	if err = json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	status, err := manager.BeginAs(f.ctx, batchPrincipal, manifest, "snapshot", nativeTestPresence)
	if err != nil {
		t.Fatal(err)
	}
	status, err = manager.ChunkAs(f.ctx, batchPrincipal, status.RestoreID, 0, data, nativeSHA(data), nativeTestPresence)
	if err != nil {
		t.Fatal(err)
	}
	return status, nativeSHA(raw)
}

func nativeFinishMeta() model.WriteMeta {
	return model.WriteMeta{Method: "admin.restore.finish", AgentLabel: "", IdempotencyKey: "native-allocator-restore"}
}

func requireNativeAllocatorImage(t *testing.T, f *fixture, source driver.AllocatorSnapshot) driver.AllocatorSnapshot {
	t.Helper()
	got := observedAllocatorSnapshot(t, f, nil)
	want, err := driver.NewAllocatorSnapshot(driver.Meta{StoreID: source.StoreID, Incarnation: got.Incarnation}, model.Natural64(1), model.Natural64(1), driver.AllocatorInventory{Projects: source.Inventory.Projects, Parents: []driver.ParentCounter{}})
	if err != nil {
		t.Fatal(err)
	}
	for _, parent := range source.Inventory.Parents {
		parent.Incarnation = got.Incarnation
		want.Inventory.Parents = append(want.Inventory.Parents, parent)
	}
	requireAllocatorSnapshotBytes(t, want, got)
	// New-incarnation fact revisions are initialized in the same publication.
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: got.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for _, parent := range got.Inventory.Parents {
		key, err := driver.ParentCounterLock(got.Incarnation, parent.Parent)
		if err != nil {
			t.Fatal(err)
		}
		states, err := r.Observe(f.ctx, []driver.LockKey{key})
		if err != nil || len(states) != 1 || states[0].Revision.String() != "1" {
			t.Fatalf("new incarnation parent fact: %+v %v", states, err)
		}
	}
	return got
}

func TestNativeAllocatorRestoreAllNineRealStorePairs(t *testing.T) {
	for _, sourceEngine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(sourceEngine, func(t *testing.T) {
			source := newFixture(t, sourceEngine)
			project, plain := nativeAllocatorSource(t, source)
			before := childAllocatorDurableState(t, source)
			captured := observedAllocatorSnapshot(t, source, nil)
			data, _, err := restore.Dump(source.ctx, source.store, "head")
			if err != nil {
				t.Fatal(err)
			}
			_, exported := nativeArchiveSnapshot(t, data)
			requireAllocatorSnapshotBytes(t, captured, exported)
			if sourceEngine != "memory" {
				source.reopen(t)
				again, _, err := restore.Dump(source.ctx, source.store, "head")
				if err != nil || !bytes.Equal(data, again) {
					t.Fatalf("source SQL reopen changed exact native bytes: %v", err)
				}
			}
			for _, targetEngine := range []string{"memory", "sqlite", "postgres"} {
				t.Run(targetEngine, func(t *testing.T) {
					target := newFixture(t, targetEngine)
					old, err := target.store.Meta(target.ctx)
					if err != nil {
						t.Fatal(err)
					}
					manager := nativeManager(t, target, target.store)
					status, hash := nativeUpload(t, target, manager, data)
					status, err = manager.Finish(target.ctx, status.RestoreID, hash, batchPrincipal, nativeFinishMeta(), nativeTestPresence)
					if err != nil || status.State != "complete" {
						t.Fatalf("native transfer: %+v %v", status, err)
					}
					restored := requireNativeAllocatorImage(t, target, captured)
					if restored.Incarnation == captured.Incarnation || restored.Incarnation == old.Incarnation || restored.Incarnation != status.Incarnation {
						t.Fatal("restore did not publish a fresh incarnation")
					}
					image := childAllocatorDurableState(t, target)
					if len(image.Records) != len(before.Records) {
						t.Fatal("restore dropped records")
					}
					for id, want := range before.Records {
						got, ok := image.Records[id]
						if !ok || !bytes.Equal(got.Body, want.Body) || got.Kind != want.Kind || got.Revision.String() != "1" {
							t.Fatalf("native record image %s changed: %+v", id, got)
						}
						versions := image.Versions[id]
						if len(versions) != 1 || !versions[0].Base || versions[0].Revision.String() != "1" || !bytes.Equal(versions[0].Body, want.Body) {
							t.Fatalf("snapshot invented/lost base for %s: %+v", id, versions)
						}
					}
					// The floor is exclusive for streaming. Prove revision one separately
					// through the final keyed receipt, never by dropping that boundary row.
					receipt := target.receipt(t, nativeFinishMeta())
					if receipt == nil || receipt.ResponsePending || receipt.RequestHash != nativeSHA(data) || receipt.Changeset.Kind != model.ChangesetRestore || receipt.Changeset.Incarnation != restored.Incarnation || receipt.Changeset.Revision.String() != "1" || !reflect.DeepEqual(receipt.Changeset.Principal, batchPrincipal) || len(receipt.Changeset.Changes) != len(before.Records) || len(receipt.Commit.Results) != len(before.Records) {
						t.Fatalf("native final receipt: %+v", receipt)
					}
					r, err := target.store.Read(target.ctx, driver.View{Incarnation: restored.Incarnation})
					if err != nil {
						t.Fatal(err)
					}
					var truncated driver.ErrTruncated
					log, err := r.Changesets(target.ctx, nil, 100)
					if !errors.As(err, &truncated) || len(log) != 0 {
						t.Fatalf("below-floor native log: %+v %v", log, err)
					}
					log, err = r.Changesets(target.ctx, &driver.Cursor{Incarnation: restored.Incarnation, Revision: model.Natural64(1), Method: "changesets"}, 100)
					if err != nil || len(log) != 0 {
						t.Fatalf("native caught-up log: %+v %v", log, err)
					}
					r.Close()
					zero := model.Natural64(0)
					if historical, err := target.store.Read(target.ctx, driver.View{Incarnation: restored.Incarnation, At: &zero}); !errors.As(err, &truncated) {
						if historical != nil {
							historical.Close()
						}
						t.Fatalf("restore invented allocator/history at zero: %v", err)
					}
					var changed driver.ErrIncarnationChanged
					if historical, err := target.store.Read(target.ctx, driver.View{Incarnation: captured.Incarnation}); !errors.As(err, &changed) {
						if historical != nil {
							historical.Close()
						}
						t.Fatalf("restore retained source lifetime: %v", err)
					}
					replay, err := manager.Finish(target.ctx, status.RestoreID, hash, batchPrincipal, nativeFinishMeta(), nativeTestPresence)
					if err != nil || !reflect.DeepEqual(replay, status) {
						t.Fatalf("native completion recovery: %+v %v", replay, err)
					}
					requireAllocatorSnapshotBytes(t, restored, observedAllocatorSnapshot(t, target, nil))
					if targetEngine != "memory" {
						target.reopen(t)
						requireNativeAllocatorImage(t, target, captured)
						requireSameState(t, image, childAllocatorDurableState(t, target))
						if !reflect.DeepEqual(receipt, target.receipt(t, nativeFinishMeta())) {
							t.Fatal("restore receipt changed across SQL reopen")
						}
					}
					// Exercise the production Writer after the physical target reopen. The
					// highest source children were deleted; reusing .2 would be data loss.
					nextRoot := hierarchyApply(t, target, hierarchyCreate("continued-project-root", nil, "demo")).Results[0].Ref
					nextProject := hierarchyApply(t, target, hierarchyCreate("continued-project-child", &project, "demo")).Results[0].Ref
					nextPlain := hierarchyApply(t, target, hierarchyCreate("continued-plain-child", &plain, "")).Results[0].Ref
					if nextRoot.ID != "demo-43" || nextProject.ID != project.ID+".3" || nextPlain.ID != plain.ID+".3" {
						t.Fatalf("native continuation reused consumed identity: %+v %+v %+v", nextRoot, nextProject, nextPlain)
					}
					for _, prefix := range []string{"hash", "random"} {
						root := hierarchyApply(t, target, hierarchyCreate("continued-"+prefix, nil, prefix)).Results[0].Ref
						child := hierarchyApply(t, target, hierarchyCreate("continued-"+prefix+"-child", &root, prefix)).Results[0].Ref
						if !strings.HasPrefix(root.ID, prefix+"-") || child.ID == root.ID {
							t.Fatalf("restored %s profile failed continuation: %+v %+v", prefix, root, child)
						}
						if prefix == "hash" && child.ID != root.ID+".1" {
							t.Fatalf("restored hash child suffix: %+v", child)
						}
						if prefix == "random" {
							for _, candidate := range []struct{ id, prefix string }{{root.ID, "random-"}, {child.ID, root.ID + "."}} {
								suffix, ok := strings.CutPrefix(candidate.id, candidate.prefix)
								if !ok || len(suffix) != 32 || strings.Trim(suffix, "0123456789abcdef") != "" {
									t.Fatalf("restored random candidate %q must be %s followed by 32 lower-hex digits", candidate.id, candidate.prefix)
								}
							}
						}
					}
					current := observedAllocatorSnapshot(t, target, nil)
					// The complete restore image remains historically readable after writes.
					at := model.Natural64(1)
					requireAllocatorSnapshotBytes(t, restored, observedAllocatorSnapshot(t, target, &at))
					if targetEngine != "memory" {
						target.reopen(t)
						requireAllocatorSnapshotBytes(t, current, observedAllocatorSnapshot(t, target, nil))
						requireAllocatorSnapshotBytes(t, restored, observedAllocatorSnapshot(t, target, &at))
					}
					// The retained live manager may recover completion after writes;
					// SQL recovery uses the durable receipt across reopen above.
					if targetEngine == "memory" {
						replay, err = manager.Finish(target.ctx, status.RestoreID, hash, batchPrincipal, nativeFinishMeta(), nativeTestPresence)
						if err != nil || !reflect.DeepEqual(replay, status) {
							t.Fatalf("native completion replay: %+v %v", replay, err)
						}
					}
					requireAllocatorSnapshotBytes(t, current, observedAllocatorSnapshot(t, target, nil))
					verifyHierarchy(t, target)
					t.Logf("real %s -> %s allocator snapshot transfer, exact bytes/counters, receipt/floor/new incarnation and monotonic Writer continuation", sourceEngine, targetEngine)
				})
			}
			requireSameState(t, before, childAllocatorDurableState(t, source))
			requireAllocatorSnapshotBytes(t, captured, observedAllocatorSnapshot(t, source, nil))
		})
	}
}

type nativeWriteBeforeReadStore struct {
	driver.Store
	write func()
	view  driver.View
}

func (s *nativeWriteBeforeReadStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	s.view = view
	if s.write != nil {
		write := s.write
		s.write = nil
		write()
	}
	return s.Store.Read(ctx, view)
}

func TestNativeAllocatorDumpPinsCapturedHeadAndExplicitZeroAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, empty := range []bool{true, false} {
			for _, history := range []string{"head", "full"} {
				t.Run(fmt.Sprintf("%s/zero=%t/%s", engine, empty, history), func(t *testing.T) {
					source := newFixture(t, engine)
					var parent model.Ref
					if !empty {
						parent = hierarchyApply(t, source, hierarchyCreate("captured-root", nil, "")).Results[0].Ref
						child := hierarchyApply(t, source, hierarchyCreate("captured-deleted-child", &parent, "")).Results[0].Ref
						hierarchyApply(t, source, childDelete(child))
					}
					captured := observedAllocatorSnapshot(t, source, nil)
					baseline, _, err := restore.Dump(source.ctx, source.store, history)
					if err != nil {
						t.Fatal(err)
					}
					wrapped := &nativeWriteBeforeReadStore{Store: source.store, write: func() {
						if empty {
							hierarchyApply(t, source, hierarchyCreate("interleaved-root", nil, ""), hierarchyCreate("interleaved-child", hierarchyRef("interleaved-root"), ""))
						} else {
							hierarchyApply(t, source, hierarchyCreate("interleaved-child", &parent, ""))
						}
					}}
					data, manifest, err := restore.Dump(source.ctx, wrapped, history)
					if err != nil {
						t.Fatal(err)
					}
					if wrapped.write != nil || wrapped.view.At == nil || wrapped.view.At.String() != captured.At.String() || !bytes.Equal(data, baseline) || manifest.NativeHead != captured.At.String() {
						t.Fatalf("native dump escaped captured view: %+v %+v", wrapped.view, manifest)
					}
					_, snapshot := nativeArchiveSnapshot(t, data)
					requireAllocatorSnapshotBytes(t, captured, snapshot)
					live := observedAllocatorSnapshot(t, source, nil)
					if reflect.DeepEqual(live.Inventory, captured.Inventory) {
						t.Fatal("interleaved real allocator write did not occur")
					}
					at := captured.At
					requireAllocatorSnapshotBytes(t, captured, observedAllocatorSnapshot(t, source, &at))
				})
			}
		}
	}
}

func TestNativeAllocatorZeroSnapshotRestoresAcrossAllNinePairs(t *testing.T) {
	for _, sourceEngine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(sourceEngine, func(t *testing.T) {
			source := newFixture(t, sourceEngine)
			captured := observedAllocatorSnapshot(t, source, nil)
			data, _, err := restore.Dump(source.ctx, source.store, "head")
			if err != nil {
				t.Fatal(err)
			}
			if !captured.At.IsZero() || !captured.CoverageFloor.IsZero() {
				t.Fatal("zero fixture is initialized")
			}
			for _, targetEngine := range []string{"memory", "sqlite", "postgres"} {
				t.Run(targetEngine, func(t *testing.T) {
					target := newFixture(t, targetEngine)
					manager := nativeManager(t, target, target.store)
					status, hash := nativeUpload(t, target, manager, data)
					status, err := manager.Finish(target.ctx, status.RestoreID, hash, batchPrincipal, nativeFinishMeta(), nativeTestPresence)
					if err != nil || status.State != "complete" {
						t.Fatalf("zero snapshot transfer: %+v %v", status, err)
					}
					requireNativeAllocatorImage(t, target, captured)
					receipt := target.receipt(t, nativeFinishMeta())
					if receipt == nil || receipt.RequestHash != nativeSHA(data) || receipt.ResponsePending || receipt.Changeset.Revision.String() != "1" || len(receipt.Changeset.Changes) != 0 {
						t.Fatalf("zero restore receipt: %+v", receipt)
					}
					if targetEngine != "memory" {
						target.reopen(t)
						requireNativeAllocatorImage(t, target, captured)
					}
					commit := hierarchyApply(t, target, hierarchyCreate("first-root", nil, ""), hierarchyCreate("first-child", hierarchyRef("first-root"), ""))
					if commit.Revision.String() != "2" || commit.Results[1].Ref.ID != commit.Results[0].Ref.ID+".1" {
						t.Fatalf("zero restored continuation: %+v", commit)
					}
				})
			}
		})
	}
}

// This mutator preserves the real source records and recomputes only transport
// bindings. It deliberately does not fabricate source allocator history.
func nativeMutatedArchive(t *testing.T, input []byte, scenario string) []byte {
	t.Helper()
	members := map[string][]byte{}
	tr := tar.NewReader(bytes.NewReader(input))
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		members[header.Name], err = io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
	}
	var manifest dumpformat.Manifest
	if err := json.Unmarshal(members["manifest.json"], &manifest); err != nil {
		t.Fatal(err)
	}
	raw := members[dumpformat.NativeAllocatorMember]
	switch scenario {
	case "legacy-missing":
		delete(members, dumpformat.NativeAllocatorMember)
		manifest.NativeAllocatorProfile = ""
		manifest.NativeAllocatorDigest = ""
	case "unknown-profile":
		manifest.NativeAllocatorProfile = "plug-native-allocator/2"
	case "source-store", "source-head":
		snapshot, err := driver.DecodeAllocatorSnapshot(raw)
		if err != nil {
			t.Fatal(err)
		}
		if scenario == "source-store" {
			snapshot.StoreID = "other-source"
		} else {
			n, err := snapshot.At.Uint64()
			if err != nil {
				t.Fatal(err)
			}
			snapshot.At = model.Natural64(n + 1)
		}
		raw, err = driver.EncodeAllocatorSnapshot(snapshot)
		if err != nil {
			t.Fatal(err)
		}
	case "coverage":
		snapshot, err := driver.DecodeAllocatorSnapshot(raw)
		if err != nil {
			t.Fatal(err)
		}
		n, _ := snapshot.At.Uint64()
		raw = bytes.Replace(raw, []byte(`"coverage_floor":`+snapshot.CoverageFloor.String()), []byte(fmt.Sprintf(`"coverage_floor":%d`, n+1)), 1)
	case "duplicate":
		raw = bytes.Replace(raw, []byte(`"prefix":"demo"`), []byte(`"prefix":"demo","prefix":"shadow"`), 1)
	case "overflow":
		raw = bytes.Replace(raw, []byte(`18446744073709551615`), []byte(`18446744073709551616`), 1)
	case "noncanonical":
		raw = append(append([]byte{}, raw...), ' ')
	case "missing-array":
		raw = bytes.Replace(raw, []byte(`"parents":[`), []byte(`"unknown_parents":[`), 1)
	case "digest":
		manifest.NativeAllocatorDigest = strings.Repeat("0", 64)
	case "priority-five":
		data := members["records/native.ndjson"]
		lines := bytes.Split(data, []byte{'\n'})
		changed := false
		var err error
		for i, line := range lines {
			if len(bytes.TrimSpace(line)) == 0 {
				continue
			}
			var record struct {
				Ref      model.Ref       `json:"ref"`
				Kind     string          `json:"kind"`
				Revision model.Revision  `json:"revision"`
				Body     json.RawMessage `json:"body"`
			}
			if err := json.Unmarshal(line, &record); err != nil {
				t.Fatal(err)
			}
			if record.Kind != "bead" {
				continue
			}
			var body map[string]json.RawMessage
			if err := json.Unmarshal(record.Body, &body); err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(body["fields"], &fields); err != nil {
				t.Fatal(err)
			}
			fields["priority"] = json.RawMessage("5")
			body["fields"], err = json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			record.Body, err = json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			// Keep the native record identity intact. Re-encoding only kind/body
			// would make the transport parser reject the intended field mutation
			// before restore validation can name the offending bead.
			lines[i], err = json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			changed = true
			break
		}
		if !changed {
			t.Fatal("native archive has no bead to mutate")
		}
		members["records/native.ndjson"] = bytes.Join(lines, []byte{'\n'})
	case "memory-id-mismatch", "memory-blank-key", "memory-empty-content", "memory-blank-content", "memory-missing-content", "memory-null-content", "memory-number-content":
		lines := bytes.Split(members["records/native.ndjson"], []byte{'\n'})
		changed := false
		for i, line := range lines {
			if len(line) == 0 {
				continue
			}
			var record struct {
				Ref      model.Ref       `json:"ref"`
				Kind     string          `json:"kind"`
				Revision model.Revision  `json:"revision"`
				Body     json.RawMessage `json:"body"`
			}
			if err := json.Unmarshal(line, &record); err != nil {
				t.Fatal(err)
			}
			if record.Kind != "memory" {
				continue
			}
			var body map[string]json.RawMessage
			if err := json.Unmarshal(record.Body, &body); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "memory-id-mismatch":
				body["id"] = json.RawMessage(`"different-memory-id"`)
			case "memory-blank-key":
				body["key"] = json.RawMessage(`" "`)
			case "memory-empty-content":
				body["content"] = json.RawMessage(`""`)
			case "memory-blank-content":
				body["content"] = json.RawMessage(`"   "`)
			case "memory-missing-content":
				delete(body, "content")
			case "memory-null-content":
				body["content"] = json.RawMessage(`null`)
			case "memory-number-content":
				body["content"] = json.RawMessage(`7`)
			}
			var err error
			record.Body, err = json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			lines[i], err = json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			changed = true
			break
		}
		if !changed {
			t.Fatal("native archive has no memory to mutate")
		}
		members["records/native.ndjson"] = bytes.Join(lines, []byte{'\n'})
	default:
		t.Fatalf("unknown negative scenario %s", scenario)
	}
	if scenario != "legacy-missing" {
		members[dumpformat.NativeAllocatorMember] = raw
		if scenario != "digest" {
			manifest.NativeAllocatorDigest = nativeSHA(raw)
		}
	}
	paths := make([]string, 0, len(members)-1)
	for path := range members {
		if path != "manifest.json" {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	manifest.Files = map[string]dumpformat.File{}
	h := sha256.New()
	for _, path := range paths {
		b := members[path]
		manifest.Files[path] = dumpformat.File{SHA256: nativeSHA(b), Size: int64(len(b))}
		h.Write([]byte(path))
		h.Write(b)
	}
	manifest.NativeDigest = hex.EncodeToString(h.Sum(nil))
	rawManifest, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	members["manifest.json"] = append(rawManifest, '\n')
	paths = append([]string{"manifest.json"}, paths...)
	var out bytes.Buffer
	tw := tar.NewWriter(&out)
	for _, path := range paths {
		b := members[path]
		if err := tw.WriteHeader(&tar.Header{Name: path, Mode: 0o600, Size: int64(len(b)), ModTime: time.Unix(0, 0), Typeflag: tar.TypeReg, Format: tar.FormatUSTAR}); err != nil {
			t.Fatal(err)
		}
		if _, err = tw.Write(b); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestNativeRestoreRejectsInvalidPriorityBeforePublicationAcrossStores(t *testing.T) {
	source := newFixture(t, "memory")
	nativeAllocatorSource(t, source)
	data, _, err := restore.Dump(source.ctx, source.store, "head")
	if err != nil {
		t.Fatal(err)
	}
	mutated := nativeMutatedArchive(t, data, "priority-five")
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			target := newFixture(t, engine)
			before := childAllocatorDurableState(t, target)
			empty := observedAllocatorSnapshot(t, target, nil)
			wrapped := &nativeAllocatorFaultStore{Store: target.store}
			manager := nativeManager(t, target, wrapped)
			status, hash := nativeUpload(t, target, manager, mutated)
			status, err := manager.Finish(target.ctx, status.RestoreID, hash, batchPrincipal, nativeFinishMeta(), nativeTestPresence)
			requireCode(t, err, "restore_violation")
			var typed *model.Error
			if !errors.As(err, &typed) || !strings.Contains(typed.Data.Hint, "native record bead/") || !strings.Contains(typed.Data.Hint, "P0 through P4") {
				t.Fatalf("invalid priority refusal did not name the bead: %+v %v", typed, err)
			}
			if status.State != "failed" || wrapped.writes != 0 {
				t.Fatalf("invalid native field reached publication: %+v writes=%d", status, wrapped.writes)
			}
			requireSameState(t, before, childAllocatorDurableState(t, target))
			requireAllocatorSnapshotBytes(t, empty, observedAllocatorSnapshot(t, target, nil))
			if target.receipt(t, nativeFinishMeta()) != nil || len(nativeAllocatorDerivedRows(t, target)) != 0 {
				t.Fatal("invalid native field published receipt or derived state")
			}
			if engine != "memory" {
				target.reopen(t)
				requireSameState(t, before, childAllocatorDurableState(t, target))
				requireAllocatorSnapshotBytes(t, empty, observedAllocatorSnapshot(t, target, nil))
			}
		})
	}
}

func TestNativeRestoreRejectsInvalidMemoryBeforePublicationAcrossStores(t *testing.T) {
	source := newFixture(t, "memory")
	meta := source.meta("native-memory-source")
	meta.Method, meta.Project = "memory.save", ""
	commit, err := source.writer.Apply(source.ctx, batchPrincipal, model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{
		Type: "memory", Fields: map[string]any{"key": "restore-key", "content": "ordinary memory content", "metadata": map[string]any{"origin": "ordinary"}},
	}}}}, meta)
	if err != nil || len(commit.Results) != 1 {
		t.Fatalf("ordinary source memory write: %+v %v", commit, err)
	}
	memoryID := commit.Results[0].Ref.ID
	data, _, err := restore.Dump(source.ctx, source.store, "head")
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []struct{ name, reason string }{
		{"memory-id-mismatch", "memory document ID differs"},
		{"memory-blank-key", "memory key must be nonempty"},
		{"memory-missing-content", "memory content must be a string"},
		{"memory-null-content", "memory content must be a string"},
		{"memory-number-content", "memory content must be a string"},
	} {
		mutated := nativeMutatedArchive(t, data, scenario.name)
		for _, engine := range []string{"memory", "sqlite", "postgres"} {
			t.Run(scenario.name+"/"+engine, func(t *testing.T) {
				target := newFixture(t, engine)
				before := childAllocatorDurableState(t, target)
				empty := observedAllocatorSnapshot(t, target, nil)
				wrapped := &nativeAllocatorFaultStore{Store: target.store}
				manager := nativeManager(t, target, wrapped)
				status, hash := nativeUpload(t, target, manager, mutated)
				status, err := manager.Finish(target.ctx, status.RestoreID, hash, batchPrincipal, nativeFinishMeta(), nativeTestPresence)
				requireCode(t, err, "restore_violation")
				var typed *model.Error
				if !errors.As(err, &typed) || !strings.Contains(typed.Data.Hint, "native record memory/"+memoryID) || !strings.Contains(typed.Data.Hint, scenario.reason) {
					t.Fatalf("invalid memory refusal did not name the record and field: %+v %v", typed, err)
				}
				if status.State != "failed" || wrapped.writes != 0 {
					t.Fatalf("invalid native memory reached publication: %+v writes=%d", status, wrapped.writes)
				}
				requireSameState(t, before, childAllocatorDurableState(t, target))
				requireAllocatorSnapshotBytes(t, empty, observedAllocatorSnapshot(t, target, nil))
				if target.receipt(t, nativeFinishMeta()) != nil || len(nativeAllocatorDerivedRows(t, target)) != 0 {
					t.Fatal("invalid native memory published receipt or derived state")
				}
				if engine != "memory" {
					target.reopen(t)
					requireSameState(t, before, childAllocatorDurableState(t, target))
					requireAllocatorSnapshotBytes(t, empty, observedAllocatorSnapshot(t, target, nil))
				}
			})
		}
	}
}

func TestNativeRestorePreservesEmptyMemoryAndRecallAcrossStores(t *testing.T) {
	source := newFixture(t, "memory")
	meta := source.meta("native-memory-empty-source")
	meta.Method, meta.Project = "memory.save", ""
	commit, err := source.writer.Apply(source.ctx, batchPrincipal, model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{
		Type: "memory", Fields: map[string]any{"key": "restore-key", "content": "ordinary memory content", "metadata": map[string]any{"origin": "ordinary"}},
	}}}}, meta)
	if err != nil || len(commit.Results) != 1 {
		t.Fatalf("ordinary source memory write: %+v %v", commit, err)
	}
	memoryID := commit.Results[0].Ref.ID
	data, _, err := restore.Dump(source.ctx, source.store, "head")
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []struct{ name, content string }{
		{"memory-empty-content", ""},
		{"memory-blank-content", "   "},
	} {
		mutated := nativeMutatedArchive(t, data, scenario.name)
		for _, engine := range []string{"memory", "sqlite", "postgres"} {
			t.Run(scenario.name+"/"+engine, func(t *testing.T) {
				target := newFixture(t, engine)
				manager := nativeManager(t, target, target.store)
				status, hash := nativeUpload(t, target, manager, mutated)
				status, err := manager.Finish(target.ctx, status.RestoreID, hash, batchPrincipal, nativeFinishMeta(), nativeTestPresence)
				if err != nil || status.State != "complete" || target.receipt(t, nativeFinishMeta()) == nil {
					t.Fatalf("native memory image was not published: %+v %v", status, err)
				}
				recall := func() {
					t.Helper()
					svc := service.New(target.store, target.lease, target.writer, service.WithOntology(target.ontology), service.WithClock(target.now))
					got, err := svc.MemoryGet(target.ctx, batchPrincipal, service.MemoryGetRequest{ID: model.RefByID(memoryID, model.Natural64(0))})
					if err != nil || got.ID != memoryID || got.Key != "restore-key" || got.Content != scenario.content || got.Revision.String() != "1" || !reflect.DeepEqual(got.Metadata, map[string]any{"origin": "ordinary"}) {
						t.Fatalf("restored memory was absent or changed: %+v %v", got, err)
					}
					page, err := svc.MemoryList(target.ctx, batchPrincipal, service.MemoryListRequest{Limit: 10})
					if err != nil || len(page.Items) != 1 || page.Items[0].ID != memoryID || page.Items[0].Content != scenario.content {
						t.Fatalf("restored memory list lost the present image: %+v %v", page, err)
					}
					_, err = svc.MemoryGet(target.ctx, batchPrincipal, service.MemoryGetRequest{ID: model.RefByID("never-restored-memory", model.Natural64(0))})
					requireCode(t, err, "not_found")
				}
				recall()
				if engine != "memory" {
					target.reopen(t)
					recall()
				}
			})
		}
	}
}

func TestNativeRestoreRequiresUnambiguousOntologySourceAcrossStores(t *testing.T) {
	source := newFixture(t, "memory")
	created := hierarchyApply(t, source, hierarchyCreate("source-with-title", nil, ""))
	if len(created.Results) != 1 {
		t.Fatalf("ordinary source bead: %+v", created)
	}
	seedOntology := func(id string, document ontology.Document) {
		t.Helper()
		body, err := service.MarshalOntologySource(service.OntologySource{Base: document})
		if err != nil {
			t.Fatal(err)
		}
		_, err = conformance.Seed(source.ctx, source.store, source.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{
			Ref: model.RefByID(id, model.Natural64(0)), Kind: "ontology", Op: model.OpCreate, Body: body,
		}})
		if err != nil {
			t.Fatal(err)
		}
	}
	seedOntology("ontology/a", ontology.DefaultDocument())
	unambiguous, _, err := restore.Dump(source.ctx, source.store, "head")
	if err != nil {
		t.Fatal(err)
	}
	conflicting := ontology.DefaultDocument()
	issue := conflicting.Types["issue"]
	issue.Fields["title"] = ontology.Field{Type: "integer"}
	conflicting.Types["issue"] = issue
	if _, problems := ontology.Compile(conflicting, nil, nil); len(problems) != 0 {
		t.Fatalf("second ontology source must compile independently: %+v", problems)
	}
	seedOntology("ontology/z", conflicting)
	ambiguous, _, err := restore.Dump(source.ctx, source.store, "head")
	if err != nil {
		t.Fatal(err)
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine+"/single-source", func(t *testing.T) {
			target := newFixture(t, engine)
			manager := nativeManager(t, target, target.store)
			status, hash := nativeUpload(t, target, manager, unambiguous)
			status, err := manager.Finish(target.ctx, status.RestoreID, hash, batchPrincipal, nativeFinishMeta(), nativeTestPresence)
			if err != nil || status.State != "complete" {
				t.Fatalf("single-source native restore: %+v %v", status, err)
			}
			meta, err := target.store.Meta(target.ctx)
			if err != nil {
				t.Fatal(err)
			}
			r, err := target.store.Read(target.ctx, driver.View{Incarnation: meta.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			rows, readErr := r.Get(target.ctx, []model.Ref{created.Results[0].Ref})
			closeErr := r.Close()
			if readErr != nil || closeErr != nil || len(rows) != 1 || rows[0].Kind != "bead" {
				t.Fatalf("single-source bead image: %+v read=%v close=%v", rows, readErr, closeErr)
			}
		})
		t.Run(engine+"/ambiguous-source", func(t *testing.T) {
			target := newFixture(t, engine)
			before := childAllocatorDurableState(t, target)
			empty := observedAllocatorSnapshot(t, target, nil)
			wrapped := &nativeAllocatorFaultStore{Store: target.store}
			manager := nativeManager(t, target, wrapped)
			status, hash := nativeUpload(t, target, manager, ambiguous)
			status, err := manager.Finish(target.ctx, status.RestoreID, hash, batchPrincipal, nativeFinishMeta(), nativeTestPresence)
			requireCode(t, err, "restore_violation")
			var typed *model.Error
			if !errors.As(err, &typed) || !strings.Contains(typed.Data.Hint, "native record ontology/ontology/z") || !strings.Contains(typed.Data.Hint, "multiple protected ontology sources") {
				t.Fatalf("ambiguous ontology refusal did not name the second source: status=%+v data=%+v err=%v", status, typed.Data, err)
			}
			if status.State != "failed" || wrapped.writes != 0 {
				t.Fatalf("ambiguous ontology reached publication: %+v writes=%d", status, wrapped.writes)
			}
			requireSameState(t, before, childAllocatorDurableState(t, target))
			requireAllocatorSnapshotBytes(t, empty, observedAllocatorSnapshot(t, target, nil))
			if target.receipt(t, nativeFinishMeta()) != nil || len(nativeAllocatorDerivedRows(t, target)) != 0 {
				t.Fatal("ambiguous ontology published receipt or derived state")
			}
			if engine != "memory" {
				target.reopen(t)
				requireSameState(t, before, childAllocatorDurableState(t, target))
				requireAllocatorSnapshotBytes(t, empty, observedAllocatorSnapshot(t, target, nil))
			}
		})
	}
}

func TestNativeAllocatorArchiveRefusalsPublishNothingAcrossStores(t *testing.T) {
	source := newFixture(t, "memory")
	nativeAllocatorSource(t, source)
	data, _, err := restore.Dump(source.ctx, source.store, "head")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		code   model.ErrorCode
		reason driver.NativeAllocatorReason
	}{
		{"legacy-missing", "restore_violation", driver.NativeAllocatorSnapshotMissing},
		{"unknown-profile", "restore_violation", driver.NativeAllocatorProfileUnsupported},
		{"source-store", "restore_violation", driver.NativeAllocatorSourceMismatch},
		{"source-head", "restore_violation", driver.NativeAllocatorSourceMismatch},
		{"coverage", "restore_violation", driver.NativeAllocatorCoverageUnavailable},
		{"duplicate", "restore_checksum_failed", ""}, {"overflow", "restore_checksum_failed", ""}, {"noncanonical", "restore_checksum_failed", ""}, {"missing-array", "restore_checksum_failed", ""}, {"digest", "restore_checksum_failed", ""},
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, test := range cases {
			t.Run(engine+"/"+test.name, func(t *testing.T) {
				target := newFixture(t, engine)
				before := childAllocatorDurableState(t, target)
				empty := observedAllocatorSnapshot(t, target, nil)
				wrapped := &nativeAllocatorFaultStore{Store: target.store}
				manager := nativeManager(t, target, wrapped)
				mutated := nativeMutatedArchive(t, data, test.name)
				status, hash := nativeUpload(t, target, manager, mutated)
				status, err := manager.Finish(target.ctx, status.RestoreID, hash, batchPrincipal, nativeFinishMeta(), nativeTestPresence)
				requireCode(t, err, test.code)
				var typed *model.Error
				if !errors.As(err, &typed) {
					t.Fatal(err)
				}
				if test.reason != "" && typed.Data.Details["reason"] != string(test.reason) {
					t.Fatalf("closed refusal reason: %+v", typed.Data)
				}
				if status.State != "failed" || wrapped.writes != 0 {
					t.Fatalf("invalid archive reached publication: %+v writes=%d", status, wrapped.writes)
				}
				requireSameState(t, before, childAllocatorDurableState(t, target))
				requireAllocatorSnapshotBytes(t, empty, observedAllocatorSnapshot(t, target, nil))
				if target.receipt(t, nativeFinishMeta()) != nil || len(nativeAllocatorDerivedRows(t, target)) != 0 {
					t.Fatal("refusal published receipt/derived state")
				}
				if engine != "memory" {
					target.reopen(t)
					requireSameState(t, before, childAllocatorDurableState(t, target))
					requireAllocatorSnapshotBytes(t, empty, observedAllocatorSnapshot(t, target, nil))
				}
			})
		}
	}
}

var errNativeAllocatorInjected = errors.New("injected native allocator publication failure")

type nativeAllocatorFaultStore struct {
	driver.Store
	mode                                   string
	writes, staged, puts, derived, commits int
	prepared                               driver.Meta
	locks                                  driver.LockSet
}

func (s *nativeAllocatorFaultStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	s.writes++
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	if s.mode == "unsupported" {
		return &nativeAllocatorHiddenTx{WriteTx: tx}, nil
	}
	return &nativeAllocatorFaultTx{WriteTx: tx, owner: s, lease: lease}, nil
}

type nativeAllocatorHiddenTx struct{ driver.WriteTx }

func (tx *nativeAllocatorHiddenTx) PrepareNativeRestore(ctx context.Context, p driver.NativeRestoreProfile) (driver.Meta, error) {
	return tx.WriteTx.(driver.NativeRestoreTx).PrepareNativeRestore(ctx, p)
}

type nativeAllocatorFaultTx struct {
	driver.WriteTx
	owner *nativeAllocatorFaultStore
	lease driver.ActiveLease
	head  bool
}

func (tx *nativeAllocatorFaultTx) Lock(ctx context.Context, locks []driver.LockRequest) ([]driver.LockState, error) {
	if tx.head {
		return nil, fmt.Errorf("test observed forbidden post-Head native lock")
	}
	tx.owner.locks = append(driver.LockSet{}, locks...)
	return tx.WriteTx.Lock(ctx, locks)
}
func (tx *nativeAllocatorFaultTx) Head(ctx context.Context) (model.Revision, error) {
	tx.head = true
	return tx.WriteTx.Head(ctx)
}
func (tx *nativeAllocatorFaultTx) PrepareNativeRestore(ctx context.Context, p driver.NativeRestoreProfile) (driver.Meta, error) {
	meta, err := tx.WriteTx.(driver.NativeRestoreTx).PrepareNativeRestore(ctx, p)
	if err == nil {
		tx.owner.prepared = meta
	}
	return meta, err
}
func (tx *nativeAllocatorFaultTx) RestoreNativeAllocators(ctx context.Context, snapshot driver.AllocatorSnapshot) error {
	if err := tx.WriteTx.(driver.NativeAllocatorRestoreTx).RestoreNativeAllocators(ctx, snapshot); err != nil {
		return err
	}
	tx.owner.staged++
	if tx.owner.mode == "allocator" {
		return errNativeAllocatorInjected
	}
	return nil
}
func (tx *nativeAllocatorFaultTx) Put(ctx context.Context, record driver.Record) error {
	if err := tx.WriteTx.Put(ctx, record); err != nil {
		return err
	}
	tx.owner.puts++
	if tx.owner.mode == "record" {
		return errNativeAllocatorInjected
	}
	return nil
}
func (tx *nativeAllocatorFaultTx) PutDerived(ctx context.Context, table driver.DerivedTable, remove, add []driver.DerivedRow) error {
	if err := tx.WriteTx.PutDerived(ctx, table, remove, add); err != nil {
		return err
	}
	tx.owner.derived++
	if tx.owner.mode == "derived" {
		return errNativeAllocatorInjected
	}
	return nil
}
func (tx *nativeAllocatorFaultTx) Commit(ctx context.Context, h driver.ChangesetHeader) (model.Changeset, error) {
	tx.owner.commits++
	switch tx.owner.mode {
	case "commit":
		return model.Changeset{}, errNativeAllocatorInjected
	case "cancel":
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		return tx.WriteTx.Commit(canceled, h)
	case "lease":
		if err := tx.lease.Release(ctx); err != nil {
			return model.Changeset{}, err
		}
	}
	return tx.WriteTx.Commit(ctx, h)
}

func TestNativeAllocatorStagedFailuresRollbackAllStateAcrossStores(t *testing.T) {
	source := newFixture(t, "memory")
	nativeAllocatorSource(t, source)
	captured := observedAllocatorSnapshot(t, source, nil)
	data, _, err := restore.Dump(source.ctx, source.store, "head")
	if err != nil {
		t.Fatal(err)
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, mode := range []string{"unsupported", "allocator", "record", "derived", "commit", "cancel", "lease"} {
			t.Run(engine+"/"+mode, func(t *testing.T) {
				target := newFixture(t, engine)
				before := childAllocatorDurableState(t, target)
				empty := observedAllocatorSnapshot(t, target, nil)
				wrapped := &nativeAllocatorFaultStore{Store: target.store, mode: mode}
				manager := nativeManager(t, target, wrapped)
				status, hash := nativeUpload(t, target, manager, data)
				status, err := manager.Finish(target.ctx, status.RestoreID, hash, batchPrincipal, nativeFinishMeta(), nativeTestPresence)
				wantState := "failed"
				if mode == "cancel" {
					// An interrupted restore remains eligible for the same-session
					// retry. It must still leave every durable target unchanged.
					wantState = "receiving"
				}
				if err == nil || status.State != wantState || wrapped.writes != 1 {
					t.Fatalf("fault did not refuse or retain retry state: %+v %v %+v", status, err, wrapped)
				}
				switch mode {
				case "unsupported":
					requireCode(t, err, "restore_violation")
					if wrapped.staged != 0 {
						t.Fatal("missing capability staged allocators")
					}
				case "cancel":
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("cancellation masked: %v", err)
					}
				case "lease":
					var fenced driver.ErrFenced
					if !errors.As(err, &fenced) {
						t.Fatalf("lease refusal masked: %v", err)
					}
				default:
					if !errors.Is(err, errNativeAllocatorInjected) {
						t.Fatalf("injected stage was not reached: %v", err)
					}
				}
				if mode != "unsupported" {
					if wrapped.staged != 1 || wrapped.prepared.Incarnation == "" {
						t.Fatal("fault bypassed actual allocator stage")
					}
					required := map[string]bool{}
					fence, _ := driver.NativeAllocatorRestoreLock()
					key, _ := driver.EncodeLockKey(fence)
					required[key] = true
					for _, project := range captured.Inventory.Projects {
						lock, _ := driver.AllocatorLock(project.Prefix)
						key, _ := driver.EncodeLockKey(lock)
						required[key] = true
					}
					previous := ""
					for _, lock := range wrapped.locks {
						key, _ := driver.EncodeLockKey(lock.Key)
						if key <= previous {
							t.Fatal("native pre-Head locks not sorted/deduplicated")
						}
						previous = key
						if _, ok := required[key]; ok {
							if lock.Mode != driver.LockUpdate {
								t.Fatal("native fence/project lock not Update")
							}
							delete(required, key)
						}
					}
					if len(required) != 0 {
						t.Fatalf("native restore missing required pre-Head locks: %+v", required)
					}
				}
				requireSameState(t, before, childAllocatorDurableState(t, target))
				requireAllocatorSnapshotBytes(t, empty, observedAllocatorSnapshot(t, target, nil))
				if target.receipt(t, nativeFinishMeta()) != nil || len(nativeAllocatorDerivedRows(t, target)) != 0 {
					t.Fatal("failed restore published receipt/derived rows")
				}
				r, err := target.store.Read(target.ctx, driver.View{Incarnation: before.Meta.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				for _, parent := range captured.Inventory.Parents {
					lock, _ := driver.ParentCounterLock(wrapped.prepared.Incarnation, parent.Parent)
					if wrapped.prepared.Incarnation == "" {
						continue
					}
					states, err := r.Observe(target.ctx, []driver.LockKey{lock})
					if err != nil || len(states) != 1 || states[0].Present || !states[0].Revision.IsZero() {
						t.Fatalf("failed restore published new-incarnation facts: %+v %v", states, err)
					}
				}
				r.Close()
				if mode == "lease" {
					target.lease, err = target.store.Lease(target.ctx, "after-native-fence", driver.LeaseTTL)
					if err != nil {
						t.Fatal(err)
					}
				}
				if engine != "memory" {
					target.reopen(t)
					requireSameState(t, before, childAllocatorDurableState(t, target))
					requireAllocatorSnapshotBytes(t, empty, observedAllocatorSnapshot(t, target, nil))
				}
			})
		}
	}
}

func nativeAllocatorDerivedRows(t *testing.T, f *fixture) []driver.DerivedRow {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.DerivedRows(f.ctx, driver.DerivedAncestorClosure, "")
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestNativeAllocatorTerminalConsumptionAndReplayAcrossStores(t *testing.T) {
	source := newFixture(t, "memory")
	profile := driver.OfflineAllocator{Prefix: "demo", Mode: driver.AllocatorSequential, LastID: ^uint64(0) - 1, Children: map[string]uint64{"demo-root": ^uint64(0) - 1}}
	seedChildFixture(t, source, &profile, true, "demo-root")
	captured := observedAllocatorSnapshot(t, source, nil)
	data, _, err := restore.Dump(source.ctx, source.store, "head")
	if err != nil {
		t.Fatal(err)
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			target := newFixture(t, engine)
			manager := nativeManager(t, target, target.store)
			status, hash := nativeUpload(t, target, manager, data)
			status, err := manager.Finish(target.ctx, status.RestoreID, hash, batchPrincipal, nativeFinishMeta(), nativeTestPresence)
			if err != nil || status.State != "complete" {
				t.Fatalf("terminal native restore: %+v %v", status, err)
			}
			requireNativeAllocatorImage(t, target, captured)
			if engine != "memory" {
				target.reopen(t)
				requireNativeAllocatorImage(t, target, captured)
			}
			parent := model.Ref{ID: "demo-root"}
			cs := model.ChangeSet{Ops: []model.Op{hierarchyCreate("final-root", nil, "demo"), hierarchyCreate("final-child", &parent, "demo")}}
			meta := target.meta("native-terminal")
			commit, err := target.writer.Apply(target.ctx, batchPrincipal, cs, meta)
			if err != nil || commit.Results[0].Ref.ID != "demo-18446744073709551615" || commit.Results[1].Ref.ID != "demo-root.18446744073709551615" {
				t.Fatalf("native final legal consumption: %+v %v", commit, err)
			}
			final := observedAllocatorSnapshot(t, target, nil)
			before := childAllocatorDurableState(t, target)
			replay, err := target.writer.ApplyWithOutcome(target.ctx, batchPrincipal, cs, meta)
			if err != nil || !replay.Replayed || !reflect.DeepEqual(replay.Commit, commit) {
				t.Fatalf("native final allocation replay: %+v %v", replay, err)
			}
			requireSameState(t, before, childAllocatorDurableState(t, target))
			requireAllocatorSnapshotBytes(t, final, observedAllocatorSnapshot(t, target, nil))
			if engine != "memory" {
				target.reopen(t)
				requireAllocatorSnapshotBytes(t, final, observedAllocatorSnapshot(t, target, nil))
			}
			for _, op := range []model.Op{hierarchyCreate("exhausted-root", nil, "demo"), hierarchyCreate("exhausted-child", &parent, "demo")} {
				before := childAllocatorDurableState(t, target)
				meta := target.meta("native-exhaustion")
				_, err := target.writer.Apply(target.ctx, batchPrincipal, model.ChangeSet{Ops: []model.Op{op}}, meta)
				if !errors.Is(err, driver.ErrUnsupported) {
					t.Fatalf("native terminal exhaustion masked: %v", err)
				}
				requireSameState(t, before, childAllocatorDurableState(t, target))
				requireAllocatorSnapshotBytes(t, final, observedAllocatorSnapshot(t, target, nil))
				if target.receipt(t, meta) != nil {
					t.Fatal("native exhaustion wrote receipt")
				}
			}
		})
	}
}

func TestNativeAllocatorBulkCapabilityRequiresCompletePreparedFenceAcrossStores(t *testing.T) {
	source := newFixture(t, "memory")
	nativeAllocatorSource(t, source)
	snapshot := observedAllocatorSnapshot(t, source, nil)
	binding, err := snapshot.Binding()
	if err != nil {
		t.Fatal(err)
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, scenario := range []string{"before-lock", "before-prepare", "missing-fence", "missing-project", "share-fence", "share-project", "missing-binding", "changed-payload", "duplicate-stage", "missing-stage", "post-head-lock", "offline"} {
			t.Run(engine+"/"+scenario, func(t *testing.T) {
				target := newFixture(t, engine)
				before := childAllocatorDurableState(t, target)
				empty := observedAllocatorSnapshot(t, target, nil)
				tx, err := target.store.Write(target.ctx, target.lease)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Close()
				native := tx.(driver.NativeRestoreTx)
				bulk := tx.(driver.NativeAllocatorRestoreTx)
				locks := driver.LockSet{}
				fence, _ := driver.NativeAllocatorRestoreLock()
				if scenario != "missing-fence" {
					mode := driver.LockUpdate
					if scenario == "share-fence" {
						mode = driver.LockShare
					}
					locks = append(locks, driver.LockRequest{Key: fence, Mode: mode})
				}
				for i, prefix := range binding.Projects {
					if scenario == "missing-project" && i == 0 {
						continue
					}
					key, _ := driver.AllocatorLock(prefix)
					mode := driver.LockUpdate
					if scenario == "share-project" {
						mode = driver.LockShare
					}
					locks = append(locks, driver.LockRequest{Key: key, Mode: mode})
				}
				sort.Slice(locks, func(i, j int) bool {
					a, _ := driver.EncodeLockKey(locks[i].Key)
					b, _ := driver.EncodeLockKey(locks[j].Key)
					return a < b
				})
				profile := driver.NativeRestoreProfile{StoreID: snapshot.StoreID, AllocatorBinding: binding, Principal: batchPrincipal, Method: "admin.restore.finish", IdempotencyKey: "native-guard", RequestHash: strings.Repeat("a", 64)}
				if scenario == "before-lock" {
					err = bulk.RestoreNativeAllocators(target.ctx, snapshot)
				} else {
					if _, err = tx.Lock(target.ctx, locks); err != nil {
						t.Fatal(err)
					}
					if _, err = tx.Head(target.ctx); err != nil {
						t.Fatal(err)
					}
					switch scenario {
					case "before-prepare":
						err = bulk.RestoreNativeAllocators(target.ctx, snapshot)
					case "missing-fence", "missing-project", "share-fence", "share-project":
						_, err = native.PrepareNativeRestore(target.ctx, profile)
					case "missing-binding":
						profile.AllocatorBinding = driver.NativeAllocatorBinding{}
						_, err = native.PrepareNativeRestore(target.ctx, profile)
					case "offline":
						if _, err = tx.(driver.OfflineRestoreTx).PrepareOfflineRestore(target.ctx, snapshot.StoreID); err != nil {
							t.Fatal(err)
						}
						err = bulk.RestoreNativeAllocators(target.ctx, snapshot)
					default:
						prepared, e := native.PrepareNativeRestore(target.ctx, profile)
						if e != nil {
							t.Fatal(e)
						}
						switch scenario {
						case "changed-payload":
							changed, e := snapshot.Detached()
							if e != nil {
								t.Fatal(e)
							}
							changed.Inventory.Projects[0].LastID++
							err = bulk.RestoreNativeAllocators(target.ctx, changed)
						case "duplicate-stage":
							if err = bulk.RestoreNativeAllocators(target.ctx, snapshot); err != nil {
								t.Fatal(err)
							}
							err = bulk.RestoreNativeAllocators(target.ctx, snapshot)
						case "post-head-lock":
							_, err = tx.Lock(target.ctx, locks)
						case "missing-stage":
							keys := make([]driver.LockKey, len(locks))
							for i := range locks {
								keys[i] = locks[i].Key
							}
							_, err = tx.Commit(target.ctx, driver.ChangesetHeader{LockKeys: keys, Incarnation: prepared.Incarnation, Kind: model.ChangesetRestore, Principal: profile.Principal, Method: profile.Method, IdempotencyKey: profile.IdempotencyKey, RequestHash: profile.RequestHash, Response: model.Commit{Results: []model.OpResult{}, Warnings: []model.Warning{}}})
						}
					}
				}
				if err == nil {
					t.Fatalf("native bulk guard %s was bypassed", scenario)
				}
				if scenario != "post-head-lock" {
					reason := driver.NativeAllocatorBindingRequired
					if scenario == "changed-payload" {
						reason = driver.NativeAllocatorDigestMismatch
					}
					var refusal *driver.NativeAllocatorSnapshotError
					if !errors.As(err, &refusal) || refusal.Reason != reason {
						t.Fatalf("native bulk guard %s returned wrong refusal: %v", scenario, err)
					}
				}
				if err := tx.Close(); err != nil {
					t.Fatal(err)
				}
				requireSameState(t, before, childAllocatorDurableState(t, target))
				requireAllocatorSnapshotBytes(t, empty, observedAllocatorSnapshot(t, target, nil))
				if target.receipt(t, model.WriteMeta{Method: profile.Method, IdempotencyKey: profile.IdempotencyKey}) != nil {
					t.Fatal("refused bulk guard published receipt")
				}
			})
		}
	}
}
