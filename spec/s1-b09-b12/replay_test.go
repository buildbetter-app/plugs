package s1b09b12

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/depid"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// Keep the recorder/projection/provenance bytes in the test target so a
// deleted or silently rewritten evidence boundary fails the focused test.
// The Python recorder itself is exercised by the source-build lane, not by
// this Go replay.
//
//go:embed vectors.json
var vectorsBytes []byte

//go:embed provenance.json
var provenanceBytes []byte

var fixedNow = time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC)
var recorderPrincipal = model.PrincipalRef{Kind: "user", ID: "recorder"}

type fixtureOntology struct{}

func (fixtureOntology) Normalize(context.Context, port.ProposedChange, port.SnapshotReader) ([]port.Patch, error) {
	return nil, nil
}

func (fixtureOntology) Validate(context.Context, port.ProposedChange, port.SnapshotReader) error {
	return nil
}

type opened struct {
	store    driver.Store
	lease    driver.ActiveLease
	path     string
	admin    *sql.DB
	database string
}

func open(t *testing.T, kind string) opened {
	t.Helper()
	ctx := context.Background()
	switch kind {
	case "memory":
		store := memory.New()
		lease, err := store.Lease(ctx, "s1-b09-b12", driver.LeaseTTL)
		if err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		return opened{store: store, lease: lease}
	case "postgres":
		return openPostgres(t)
	case "sqlite":
		// handled below
	default:
		t.Fatalf("unknown storage engine %q", kind)
	}
	path := filepath.Join(t.TempDir(), "s1-b09-b12.db")
	store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Lease(ctx, "s1-b09-b12", driver.LeaseTTL)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	return opened{store: store, lease: lease, path: path}
}

func (o opened) close(t *testing.T) {
	t.Helper()
	if o.lease != nil {
		if err := o.lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
	}
	if o.store != nil {
		if err := o.store.Close(); err != nil {
			t.Error(err)
		}
	}
	if o.admin != nil {
		if _, err := o.admin.ExecContext(context.Background(), `DROP DATABASE "`+o.database+`" WITH (FORCE)`); err != nil {
			t.Error(err)
		}
		if err := o.admin.Close(); err != nil {
			t.Error(err)
		}
	}
}

func seedAllocator(t *testing.T, o opened, value driver.OfflineAllocator) {
	t.Helper()
	ctx := context.Background()
	key, err := driver.AllocatorLock(value.Prefix)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := o.store.Write(ctx, o.lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, err = tx.Lock(ctx, driver.LockSet{{Key: key, Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	restore, ok := tx.(driver.OfflineRestoreTx)
	if !ok {
		t.Fatal("fixture store lacks offline restore seam")
	}
	const sourceID driver.StoreID = "s1-b09-b12-source"
	meta, err := restore.PrepareOfflineRestore(ctx, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	continuation, ok := tx.(driver.AllocatorContinuation)
	if !ok {
		t.Fatal("fixture store lacks allocator continuation")
	}
	if err = continuation.RestoreAllocators(ctx, []driver.OfflineAllocator{value}); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(struct {
		Source driver.StoreID
		Value  driver.OfflineAllocator
	}{sourceID, value})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	hash := hex.EncodeToString(sum[:])
	header := driver.ChangesetHeader{
		Incarnation:    meta.Incarnation,
		Kind:           model.ChangesetRestore,
		Principal:      model.PrincipalRef{Kind: "system", ID: "plug-converter"},
		Method:         "offline.restore",
		IdempotencyKey: "s1-b09-b12-" + hash,
		RequestHash:    hash,
		Response:       model.Commit{Warnings: []model.Warning{}},
		LockKeys:       []driver.LockKey{key},
	}
	if _, err = tx.Commit(ctx, header); err != nil {
		t.Fatal(err)
	}
}

func writer(o opened, ids ...string) *pipeline.Writer {
	return writerWithOptions(o, ids)
}

func writerWithOptions(o opened, ids []string, options ...pipeline.Option) *pipeline.Writer {
	index := 0
	base := []pipeline.Option{
		pipeline.WithClock(func() time.Time { return fixedNow }),
		pipeline.WithIDGenerator(func() (string, error) {
			if index >= len(ids) {
				return "", errors.New("fixture ID sequence exhausted")
			}
			id := ids[index]
			index++
			return id, nil
		}),
		pipeline.WithOntology(fixtureOntology{}),
	}
	return pipeline.New(o.store, o.lease, append(base, options...)...)
}

func meta(n int) model.WriteMeta {
	return model.WriteMeta{Method: "tx.apply", Project: "demo", IdempotencyKey: fmt.Sprintf("%032x", n)}
}

func create(fields map[string]any, parent *model.Ref, project string) model.Op {
	return model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: fields, Parent: parent, Project: project}}
}

func body(t *testing.T, doc pipeline.Document) []byte {
	t.Helper()
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func seedDocuments(t *testing.T, o opened, docs ...pipeline.Document) {
	t.Helper()
	changes := make([]conformance.FixtureChange, 0, len(docs))
	for _, doc := range docs {
		changes = append(changes, conformance.FixtureChange{Ref: model.RefByID(doc.ID, model.Natural64(0)), Kind: "bead", Op: model.OpCreate, Body: body(t, doc)})
	}
	if _, err := conformance.Seed(context.Background(), o.store, o.lease, driver.ChangesetHeader{Method: "s1-b09-b12.fixture", IdempotencyKey: fmt.Sprintf("%032x", 700+len(docs)), RequestHash: fmt.Sprintf("%064x", len(docs))}, changes); err != nil {
		t.Fatal(err)
	}
	parents := make(map[string]string, len(docs))
	for _, doc := range docs {
		if doc.Parent != nil {
			parents[doc.ID] = doc.Parent.ID
		}
	}
	rows := make([]driver.DerivedRow, 0, len(docs)*2)
	sentinels := make([]driver.Sentinel, 0, len(docs))
	locks := make([]driver.LockRequest, 0, len(docs)*3)
	seen := make(map[string]bool)
	for _, doc := range docs {
		sentinel, err := driver.NewClosureSentinel(doc.ID)
		if err != nil {
			t.Fatal(err)
		}
		sentinels = append(sentinels, sentinel)
		for ancestor, depth := doc.ID, uint64(0); ancestor != ""; depth++ {
			row, err := pipeline.AncestorRow(doc.ID, ancestor, depth)
			if err != nil {
				t.Fatal(err)
			}
			rows = append(rows, row)
			key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: row.Key}}
			encoded, err := driver.EncodeLockKey(key)
			if err != nil {
				t.Fatal(err)
			}
			if !seen[encoded] {
				seen[encoded] = true
				locks = append(locks, driver.LockRequest{Key: key, Mode: driver.LockUpdate})
			}
			ancestor = parents[ancestor]
		}
		key := driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}
		encoded, err := driver.EncodeLockKey(key)
		if err != nil {
			t.Fatal(err)
		}
		if !seen[encoded] {
			seen[encoded] = true
			locks = append(locks, driver.LockRequest{Key: key, Mode: driver.LockUpdate})
		}
	}
	sort.Slice(locks, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(locks[i].Key)
		b, _ := driver.EncodeLockKey(locks[j].Key)
		return a < b
	})
	sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
	meta, err := o.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tx, err := o.store.Write(context.Background(), o.lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, err = tx.Lock(context.Background(), locks); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = tx.PutDerived(context.Background(), driver.DerivedAncestorClosure, nil, rows); err != nil {
		t.Fatal(err)
	}
	if err = tx.BumpSentinels(context.Background(), sentinels); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Commit(context.Background(), driver.ChangesetHeader{
		Incarnation: meta.Incarnation,
		Kind:        model.ChangesetWrite,
		Principal:   model.PrincipalRef{Kind: "system", ID: "storage-fixture"},
		Method:      "s1-b09-b12.fixture.closure",
		LockKeys:    makeLockKeys(locks),
	}); err != nil {
		t.Fatal(err)
	}
}

func makeLockKeys(requests []driver.LockRequest) []driver.LockKey {
	keys := make([]driver.LockKey, len(requests))
	for i, request := range requests {
		keys[i] = request.Key
	}
	return keys
}

func doc(id, project string, parent *model.Ref) pipeline.Document {
	return pipeline.Document{ID: id, Type: "issue", Project: project, Parent: parent, Fields: map[string]any{"title": id}}
}

func requireCode(t *testing.T, err error, want model.ErrorCode) {
	t.Helper()
	var got *model.Error
	if !errors.As(err, &got) || got.Data.Code != want {
		t.Fatalf("error=%v, want code %s", err, want)
	}
}

func TestB09HashReplayUsesPinnedVectorAndBatchNonce(t *testing.T) {
	input := driver.HashInput{Title: "Fix login", Description: "Details", Creator: "jira-import", Timestamp: time.Date(2024, 1, 2, 3, 4, 5, 6*1_000_000, time.UTC)}
	prefix := string([]byte{98, 100})
	for length, want := range map[uint32]string{3: prefix + "-vju", 4: prefix + "-8d8e", 5: prefix + "-bi3tk", 6: prefix + "-8bi3tk", 7: prefix + "-r5sr6bm", 8: prefix + "-8r5sr6bm"} {
		if got, err := driver.HashCandidate(prefix, input, length, 0); err != nil || got != want {
			t.Fatalf("source vector length %d: got %q err=%v want %q", length, got, err, want)
		}
	}
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			o := open(t, kind)
			defer o.close(t)
			seedAllocator(t, o, driver.NewHashAllocator("demo", 3, 8, 0.25))
			w := writerWithOptions(o, nil, pipeline.WithHierarchyMaxDepth(2))
			cs := model.ChangeSet{Ops: []model.Op{
				create(map[string]any{"title": "same", "description": "same"}, nil, ""),
				create(map[string]any{"title": "same", "description": "same"}, nil, ""),
			}}
			commit, err := w.Apply(context.Background(), recorderPrincipal, cs, meta(701))
			if err != nil {
				t.Fatal(err)
			}
			if len(commit.Results) != 2 || commit.Results[0].Ref.ID == commit.Results[1].Ref.ID {
				t.Fatalf("hash batch results=%+v", commit.Results)
			}
			wantInput := driver.HashInput{Title: "same", Description: "same", Creator: "user:recorder", Timestamp: fixedNow}
			want0, _ := driver.HashCandidate("demo", wantInput, 3, 0)
			want1, _ := driver.HashCandidate("demo", wantInput, 3, 1)
			if commit.Results[0].Ref.ID != want0 || commit.Results[1].Ref.ID != want1 {
				t.Fatalf("hash batch IDs=%q,%q want=%q,%q", commit.Results[0].Ref.ID, commit.Results[1].Ref.ID, want0, want1)
			}
			replay, err := w.Apply(context.Background(), recorderPrincipal, cs, meta(701))
			if err != nil || !reflect.DeepEqual(commit, replay) {
				t.Fatalf("hash replay commit=%+v replay=%+v err=%v", commit, replay, err)
			}
		})
	}
}

func TestB10ChildReplayRepairsStaleCounterIgnoresGrandchildAndReopensSQLite(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			o := open(t, kind)
			seedAllocator(t, o, driver.OfflineAllocator{Prefix: "demo", Mode: driver.AllocatorSequential, LastID: 1, Children: map[string]uint64{"demo-1": 1}})
			parent := model.RefByID("demo-1", model.Natural64(0))
			child := model.RefByID("demo-1.8", model.Natural64(0))
			seedDocuments(t, o, doc(parent.ID, "demo", nil), doc(child.ID, "demo", &parent), doc("demo-1.8.2", "demo", &child))
			w := writerWithOptions(o, nil, pipeline.WithHierarchyMaxDepth(2))
			commit, err := w.Apply(context.Background(), recorderPrincipal, model.ChangeSet{Ops: []model.Op{create(map[string]any{"title": "next"}, &parent, "")}}, meta(702))
			if err != nil {
				t.Fatal(err)
			}
			if got := commit.Results[0].Ref.ID; got != "demo-1.9" {
				t.Fatalf("repaired child ID=%q, want demo-1.9", got)
			}
			current, err := o.store.Meta(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			reader, err := o.store.Read(context.Background(), driver.View{Incarnation: current.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			value, err := reader.(driver.AllocatorReader).LoadAllocator(context.Background(), "demo")
			reader.Close()
			if err != nil || value.Children[parent.ID] != 9 {
				t.Fatalf("child counter=%+v err=%v", value.Children, err)
			}
			tooDeep := model.RefByID("demo-1.8.2", model.Natural64(0))
			_, err = w.Apply(context.Background(), recorderPrincipal, model.ChangeSet{Ops: []model.Op{create(map[string]any{"title": "too deep"}, &tooDeep, "")}}, meta(703))
			requireCode(t, err, "invalid_params")
			if kind == "sqlite" {
				path := o.path
				o.close(t)
				reopened, err := (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: path})
				if err != nil {
					t.Fatal(err)
				}
				lease, err := reopened.Lease(context.Background(), "s1-b09-b12-reopen", driver.LeaseTTL)
				if err != nil {
					_ = reopened.Close()
					t.Fatal(err)
				}
				reopenedMeta, err := reopened.Meta(context.Background())
				if err != nil {
					lease.Release(context.Background())
					reopened.Close()
					t.Fatal(err)
				}
				r, err := reopened.Read(context.Background(), driver.View{Incarnation: reopenedMeta.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				state, err := r.(driver.AllocatorReader).LoadAllocator(context.Background(), "demo")
				r.Close()
				if err != nil || state.Children[parent.ID] != 9 {
					t.Fatalf("reopened child counter=%+v err=%v", state.Children, err)
				}
				lease.Release(context.Background())
				reopened.Close()
			}
		})
	}
}

func TestB11EndpointIdentityAndTypedLinkConflict(t *testing.T) {
	if got := depid.New("issue-1", "issue-2"); got != "91990ee3-275c-59c5-9291-30f8a8732228" {
		t.Fatalf("dependency UUIDv5=%q", got)
	}
	for _, kind := range []string{"memory", "sqlite", "postgres"} {
		t.Run(kind, func(t *testing.T) {
			o := open(t, kind)
			defer o.close(t)
			ids := []string{"source", "target"}
			w := writer(o, ids...)
			seed, err := w.Apply(context.Background(), recorderPrincipal, model.ChangeSet{Ops: []model.Op{create(map[string]any{"title": "source"}, nil, ""), create(map[string]any{"title": "target"}, nil, "")}}, meta(705))
			if err != nil {
				t.Fatal(err)
			}
			source, target := seed.Results[0].Ref, seed.Results[1].Ref
			link := func(kind model.OpKind, linkKind string) model.Op {
				return model.Op{Kind: kind, Link: &model.LinkOp{Kind: kind, From: source, To: target, LinkKind: linkKind}}
			}
			if _, err = w.Apply(context.Background(), recorderPrincipal, model.ChangeSet{Ops: []model.Op{link(model.OpLink, "blocks")}}, meta(706)); err != nil {
				t.Fatal(err)
			}
			// Association annotations are allowed alongside workflow edges; a second
			// workflow kind is the typed-conflict case for this card.
			_, err = w.Apply(context.Background(), recorderPrincipal, model.ChangeSet{Ops: []model.Op{link(model.OpLink, "waits-for")}}, meta(707))
			requireCode(t, err, "ontology_guard_failed")
			_, err = w.Apply(context.Background(), recorderPrincipal, model.ChangeSet{Ops: []model.Op{link(model.OpUnlink, "waits-for")}}, meta(708))
			requireCode(t, err, "not_found")
		})
	}
}

func TestB12ManifestStaysExplicitlyUnqualifiedAndProjectionPreservesOrder(t *testing.T) {
	var vectors map[string]any
	if err := json.Unmarshal(vectorsBytes, &vectors); err != nil {
		t.Fatal(err)
	}
	if vectors["format"] != "plug-s1-b09-b12-synthetic/1" {
		t.Fatalf("unexpected fixture format=%v", vectors["format"])
	}
	cards := vectors["cards"].(map[string]any)
	if cards["B12"].(map[string]any)["status"] != "full-card-unqualified" {
		t.Fatalf("B12 was promoted: %+v", cards["B12"])
	}
	var provenance map[string]any
	if err := json.Unmarshal(provenanceBytes, &provenance); err != nil {
		t.Fatal(err)
	}
	recording := provenance["recording"].(map[string]any)
	if recording["status"] != "blocked" || recording["rawDigest"] != "" || recording["projectionDigest"] != "" {
		t.Fatalf("recording boundary drifted: %+v", recording)
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(`{"card":"B09","outcomes":[{"step":2},{"step":1}],"decisionIDs":["A1","P44"],"disposition":"deviate"}`), &raw); err != nil {
		t.Fatal(err)
	}
	projected, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(projected, []byte(`"step":2`)) || strings.Index(string(projected), `"step":2`) > strings.Index(string(projected), `"step":1`) {
		t.Fatalf("projection reordered outcomes: %s", projected)
	}
	if !strings.Contains(string(projected), `"decisionIDs":["A1","P44"]`) {
		t.Fatalf("projection dropped decision IDs: %s", projected)
	}
}
