package restore_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/cli"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type nativeOntologyImage struct {
	Kind string
	Body string
}

func nativeOntologyCensus(t *testing.T, store driver.Store) map[string]nativeOntologyImage {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	page, err := r.Query(ctx, driver.Query{Limit: 100})
	if err != nil || page.NextCursor != nil {
		t.Fatalf("complete fixture census: %+v %v", page, err)
	}
	images := map[string]nativeOntologyImage{}
	for _, row := range page.Items {
		key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &row.Ref})
		if err != nil {
			t.Fatal(err)
		}
		images[key] = nativeOntologyImage{Kind: row.Kind, Body: string(row.Body)}
	}
	return images
}

func seedNativeOntology(t *testing.T, store driver.Store, row conformance.FixtureChange) {
	t.Helper()
	ctx := context.Background()
	lease, err := store.Lease(ctx, "ontology-source-fixture", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	if _, err := conformance.Seed(ctx, store, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{row}); err != nil {
		t.Fatal(err)
	}
}

func nativeOntologySource(t *testing.T, plain bool) (model.Ref, []byte) {
	t.Helper()
	ref := model.RefByID("ontology/fixture", model.Natural64(0))
	raw, err := service.MarshalOntologySource(service.OntologySource{Base: ontology.DefaultDocument()})
	if plain {
		ref = model.RefByID("ontology", model.Natural64(0))
		raw, err = json.Marshal(ontology.DefaultDocument())
	}
	if err != nil {
		t.Fatal(err)
	}
	return ref, raw
}

// This fixture goes through the real Writer and paged admin.dump. The source
// contains six ordinary bead images and the protected source, without filtering
// records or installing ontology on the restore target.
func nativeOntologyArchive(t *testing.T, plain bool) ([]byte, *dumpformat.Archive, map[string]nativeOntologyImage, string, string) {
	t.Helper()
	ctx := context.Background()
	store := memory.New()
	ref, raw := nativeOntologySource(t, plain)
	seedNativeOntology(t, store, conformance.FixtureChange{Ref: ref, Kind: "ontology", Op: model.OpCreate, Body: raw})
	source, err := cli.OpenLocal(ctx, cli.LocalConfig{
		Project: "fixture", Agent: "source", Storage: cli.StorageConfig{Driver: "provided"}, PresenceVerifier: trustedPresenceVerifier(),
		Factory: func(context.Context, cli.StorageConfig) (driver.Store, func() error, error) {
			return store, store.Close, nil
		},
	})
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	defer source.Close()
	var id string
	for i := 0; i < 6; i++ {
		created, err := source.Client.Call(ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": fmt.Sprintf("native ontology fixture %d", i)}}, "")
		if err != nil {
			t.Fatal(err)
		}
		var commit model.Commit
		if err := json.Unmarshal(created.JSON, &commit); err != nil || len(commit.Results) != 1 {
			t.Fatalf("create result: %s %v", created.JSON, err)
		}
		id = commit.Results[0].Ref.ID
	}
	images := nativeOntologyCensus(t, store)
	if len(images) != 7 {
		t.Fatalf("source census=%d, want six beads and protected ontology", len(images))
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data, archive := captureDump(t, ctx, source.Client)
	return data, archive, images, id, meta.Incarnation
}

func TestNativeOntologyDumpRestoreAndPhysicalReopen(t *testing.T) {
	ctx := context.Background()
	for _, plain := range []bool{false, true} {
		t.Run(fmt.Sprintf("plain=%t", plain), func(t *testing.T) {
			data, archive, want, beadID, sourceIncarnation := nativeOntologyArchive(t, plain)
			manifest, hash := manifestForRestore(t, archive)
			for _, kind := range []string{"memory", "sqlite"} {
				t.Run(kind, func(t *testing.T) {
					path := filepath.Join(t.TempDir(), "ontology.db")
					store := openNativeOntologyTarget(t, kind, path)
					defer func() { _ = store.Close() }()
					before, err := store.Meta(ctx)
					if err != nil {
						t.Fatal(err)
					}
					lease, err := store.Lease(ctx, "ontology-restore", driver.LeaseTTL)
					if err != nil {
						t.Fatal(err)
					}
					manager := restore.NewManager(store, lease, trustedPresenceVerifier())
					status, err := manager.Begin(ctx, manifest, "head", restore.Presence(presence))
					if err != nil {
						t.Fatal(err)
					}
					uploadManagerArchive(t, ctx, manager, status.RestoreID, data, restore.Presence(presence))
					status, err = manager.Finish(ctx, status.RestoreID, hash, model.PrincipalRef{Kind: "user", ID: "ontology-owner"}, model.WriteMeta{Method: "admin.restore.finish", IdempotencyKey: "ontology-restore"}, restore.Presence(presence))
					if err != nil || status.State != "complete" {
						t.Fatalf("protected native finish=%+v %v", status, err)
					}
					if err := lease.Release(ctx); err != nil {
						t.Fatal(err)
					}
					if got := nativeOntologyCensus(t, store); !reflect.DeepEqual(got, want) {
						t.Fatalf("bare target lost native images: got=%+v want=%+v", got, want)
					}
					if kind == "sqlite" {
						if err := store.Close(); err != nil {
							t.Fatal(err)
						}
						store = openNativeOntologyTarget(t, kind, path)
						// Check the physical database before constructing a host. No
						// fixture or ontology seeding is permitted on reopen.
						if got := nativeOntologyCensus(t, store); !reflect.DeepEqual(got, want) {
							t.Fatalf("physical reopen lost native images: got=%+v want=%+v", got, want)
						}
					}
					after, err := store.Meta(ctx)
					if err != nil || after.Incarnation != status.Incarnation || after.Incarnation == before.Incarnation || after.Incarnation == sourceIncarnation {
						t.Fatalf("restored incarnation=%+v status=%+v %v", after, status, err)
					}
					for _, old := range []string{before.Incarnation, sourceIncarnation} {
						r, err := store.Read(ctx, driver.View{Incarnation: old})
						if r != nil {
							_ = r.Close()
						}
						var changed driver.ErrIncarnationChanged
						if !errors.As(err, &changed) {
							t.Fatalf("old incarnation read=%v", err)
						}
					}
					ref, _ := nativeOntologySource(t, plain)
					key, _ := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
					src, err := service.DecodeOntologySource([]byte(nativeOntologyCensus(t, store)[key].Body))
					if err != nil {
						t.Fatal(err)
					}
					snapshot, problems := ontology.Compile(src.Base, src.Overlays, src.Plugins)
					if len(problems) != 0 {
						t.Fatal(problems)
					}
					host, err := cli.OpenLocal(ctx, cli.LocalConfig{
						Project: "fixture", Agent: "after-restore", Ontology: service.NewOntology(snapshot.Effective), Storage: cli.StorageConfig{Driver: "provided"},
						Factory: func(context.Context, cli.StorageConfig) (driver.Store, func() error, error) {
							return store, store.Close, nil
						},
					})
					if err != nil {
						t.Fatal(err)
					}
					defer host.Close()
					protected := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: map[string]any{"base": map[string]any{}}}}}}
					_, err = host.Client.Call(ctx, "tx.apply", map[string]any{"changeSet": protected}, "")
					var remote *wire.Error
					if !errors.As(err, &remote) || remote.Data.Code != "protected_record" {
						t.Fatalf("ordinary protected source patch=%v", err)
					}
					unchanged, err := store.Meta(ctx)
					if err != nil || !reflect.DeepEqual(unchanged, after) || !reflect.DeepEqual(nativeOntologyCensus(t, store), want) {
						t.Fatalf("refused protected write mutated restored store: %+v %v", unchanged, err)
					}
					if _, err := host.Client.Call(ctx, "bead.claim", map[string]any{"id": map[string]any{"id": beadID}}, ""); err != nil {
						t.Fatalf("restored bead claim: %v", err)
					}
					patch := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID(beadID, model.Natural64(0)), Fields: map[string]any{"title": "ordinary write after physical reopen"}}}}}
					if _, err := host.Client.Call(ctx, "tx.apply", map[string]any{"changeSet": patch}, ""); err != nil {
						t.Fatalf("future ordinary write: %v", err)
					}
					read, err := host.Client.Call(ctx, "bead.get", map[string]any{"id": map[string]any{"id": beadID}}, "")
					if err != nil || !bytes.Contains(read.JSON, []byte("ordinary write after physical reopen")) {
						t.Fatalf("future write readback=%s %v", read.JSON, err)
					}
					images := nativeOntologyCensus(t, store)
					if len(images) != 7 || images[key] != want[key] {
						t.Fatalf("ordinary write changed protected source or census: %+v", images)
					}
				})
			}
		})
	}
}

func openNativeOntologyTarget(t *testing.T, kind, path string) driver.Store {
	t.Helper()
	if kind == "memory" {
		return memory.New()
	}
	store, err := (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

type nativeOntologyWriteCounter struct {
	driver.Store
	writes int
}

func (s *nativeOntologyWriteCounter) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	s.writes++
	return s.Store.Write(ctx, lease)
}

// Even malformed records get their checksums and manifest from the actual
// native producer. Refusals cannot be caused by a hand-built broken envelope.
func rawNativeOntologyArchive(t *testing.T, row conformance.FixtureChange) ([]byte, map[string]any, string) {
	t.Helper()
	store := memory.New()
	defer store.Close()
	seedNativeOntology(t, store, row)
	data, manifest, err := restore.Dump(context.Background(), store, "head")
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]any
	if err := json.Unmarshal(canonical, &object); err != nil {
		t.Fatal(err)
	}
	return data, object, sha(canonical)
}

var nativeOntologyOwner = model.PrincipalRef{Kind: "user", ID: "ontology-owner"}
var nativeOntologyFinishMeta = model.WriteMeta{Method: "admin.restore.finish", AgentLabel: "native-ontology", IdempotencyKey: "native-ontology-negative"}

func uploadNativeOntologyAs(t *testing.T, manager *restore.Manager, id string, data []byte) {
	t.Helper()
	for offset, page := 0, uint64(0); offset < len(data); offset, page = offset+512, page+1 {
		end := offset + 512
		if end > len(data) {
			end = len(data)
		}
		chunk := data[offset:end]
		if _, err := manager.ChunkAs(context.Background(), nativeOntologyOwner, id, page, chunk, sha(chunk), restore.Presence(presence)); err != nil {
			t.Fatal(err)
		}
	}
}

func assertNativeOntologyRefusal(t *testing.T, store *nativeOntologyWriteCounter, before driver.Meta, err error, code model.ErrorCode) {
	t.Helper()
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != code {
		t.Errorf("native refusal=%T %v, want %s", err, err, code)
	}
	after, metaErr := store.Meta(context.Background())
	if metaErr != nil || !reflect.DeepEqual(before, after) || store.writes != 0 {
		t.Fatalf("refusal opened %d writes or changed metadata: before=%+v after=%+v err=%v", store.writes, before, after, metaErr)
	}
	assertEmptyRestoreTarget(t, context.Background(), store)
	r, readErr := store.Read(context.Background(), driver.View{Incarnation: after.Incarnation})
	if readErr != nil {
		t.Fatal(readErr)
	}
	defer r.Close()
	receipt, readErr := r.IdempotencyByKey(context.Background(), "user:ontology-owner", nativeOntologyFinishMeta.AgentLabel, nativeOntologyFinishMeta.Method, nativeOntologyFinishMeta.IdempotencyKey)
	if readErr != nil || receipt != nil {
		t.Fatalf("refusal published receipt: %+v %v", receipt, readErr)
	}
}

func TestNativeOntologyRestoreRefusesMalformedSourcesBeforeWrite(t *testing.T) {
	ref, valid := nativeOntologySource(t, false)
	invalid := ontology.DefaultDocument()
	typ := invalid.Types["issue"]
	typ.Workflow = "missing-workflow"
	invalid.Types["issue"] = typ
	invalidBody, err := service.MarshalOntologySource(service.OntologySource{Base: invalid})
	if err != nil {
		t.Fatal(err)
	}
	overlayBody, err := service.MarshalOntologySource(service.OntologySource{Base: ontology.DefaultDocument(), Overlays: map[string]ontology.Document{"fixture": invalid}})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		ref  model.Ref
		kind string
		body []byte
		code model.ErrorCode
	}{
		{"ordinary-id", model.RefByID("ordinary", model.Natural64(0)), "ontology", valid, "restore_violation"},
		{"key-ref", model.RefByKey("ontology/fixture"), "ontology", valid, "restore_violation"},
		{"nonzero-branch", model.RefByID("ontology/fixture", model.Natural64(1)), "ontology", valid, "restore_violation"},
		{"spoofed-kind", ref, "config", valid, "restore_violation"},
		{"unknown-envelope-member", ref, "ontology", append(append([]byte(nil), valid[:len(valid)-1]...), []byte(`,"unknown":true}`)...), "restore_violation"},
		{"asserted-digest", ref, "ontology", append(append([]byte(nil), valid[:len(valid)-1]...), []byte(`,"digest":"asserted"}`)...), "restore_violation"},
		{"invalid-workflow", ref, "ontology", invalidBody, "restore_violation"},
		{"invalid-overlay", ref, "ontology", overlayBody, "restore_violation"},
		{"null-body", ref, "ontology", []byte(`null`), "restore_violation"},
		{"duplicate-json-key", ref, "ontology", []byte(`{"base":{},"base":{}}`), "restore_checksum_failed"},
		{"unknown-kind", ref, "future-native-kind", valid, "restore_checksum_failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, manifest, hash := rawNativeOntologyArchive(t, conformance.FixtureChange{Ref: tc.ref, Kind: tc.kind, Op: model.OpCreate, Body: tc.body})
			for _, kind := range []string{"memory", "sqlite"} {
				t.Run(kind, func(t *testing.T) {
					ctx := context.Background()
					store := &nativeOntologyWriteCounter{Store: openNativeOntologyTarget(t, kind, filepath.Join(t.TempDir(), "refusal.db"))}
					defer store.Close()
					before, err := store.Meta(ctx)
					if err != nil {
						t.Fatal(err)
					}
					lease, err := store.Lease(ctx, "ontology-refusal", driver.LeaseTTL)
					if err != nil {
						t.Fatal(err)
					}
					defer lease.Release(ctx)
					manager := restore.NewManager(store, lease, trustedPresenceVerifier())
					status, err := manager.BeginAs(ctx, nativeOntologyOwner, manifest, "head", restore.Presence(presence))
					if err != nil {
						t.Fatal(err)
					}
					uploadNativeOntologyAs(t, manager, status.RestoreID, data)
					_, err = manager.Finish(ctx, status.RestoreID, hash, nativeOntologyOwner, nativeOntologyFinishMeta, restore.Presence(presence))
					assertNativeOntologyRefusal(t, store, before, err, tc.code)
				})
			}
		})
	}
}

func TestNativeOntologyRestoreRetainsAuthorityAndManifestBinding(t *testing.T) {
	ref, valid := nativeOntologySource(t, false)
	data, manifest, hash := rawNativeOntologyArchive(t, conformance.FixtureChange{Ref: ref, Kind: "ontology", Op: model.OpCreate, Body: valid})
	for _, mode := range []string{"missing-verifier", "wrong-proof", "changed-principal", "manifest-hash"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			store := &nativeOntologyWriteCounter{Store: memory.New()}
			defer store.Close()
			before, err := store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			lease, err := store.Lease(ctx, "ontology-authority", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Release(ctx)
			verifier := trustedPresenceVerifier()
			if mode == "missing-verifier" {
				verifier = nil
			}
			manager := restore.NewManager(store, lease, verifier)
			status, err := manager.BeginAs(ctx, nativeOntologyOwner, manifest, "head", restore.Presence(presence))
			if mode == "missing-verifier" {
				assertNativeOntologyRefusal(t, store, before, err, "human_presence_required")
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			uploadNativeOntologyAs(t, manager, status.RestoreID, data)
			principal, finishHash := nativeOntologyOwner, hash
			proof := restore.Presence(presence)
			code := model.ErrorCode("restore_checksum_failed")
			switch mode {
			case "wrong-proof":
				proof = restore.Presence{"mode": "presence_check", "challenge": "restore-test", "proof": "untrusted"}
				code = "human_presence_required"
			case "changed-principal":
				principal.ID = "other-user"
				code = "forbidden"
			case "manifest-hash":
				finishHash = sha([]byte("different admitted manifest"))
			}
			_, err = manager.Finish(ctx, status.RestoreID, finishHash, principal, nativeOntologyFinishMeta, proof)
			assertNativeOntologyRefusal(t, store, before, err, code)
		})
	}
}
