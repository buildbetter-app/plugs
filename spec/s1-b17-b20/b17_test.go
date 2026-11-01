package s1b17b20

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

var b17Actor = model.PrincipalRef{Kind: "user", ID: "s1-b17-actor"}

// b17Harness uses the production Writer and a real driver. The fixed clock,
// actor, request keys and allocator ID make every semantic projection stable.
type b17Harness struct {
	ctx   context.Context
	now   time.Time
	path  string
	store driver.Store
	lease driver.ActiveLease
	ids   int
}

func openB17Harness(t *testing.T, engine string) *b17Harness {
	t.Helper()
	h := &b17Harness{ctx: context.Background(), now: time.Date(2026, 11, 1, 23, 0, 0, 0, time.UTC)}
	var err error
	switch engine {
	case "memory":
		h.store, err = memory.NewWithOptions(memory.Options{StoreID: "s1-b17-store", Incarnation: "s1-b17-incarnation", Clock: func() time.Time { return h.now }})
	case "sqlite":
		h.path = filepath.Join(t.TempDir(), "s1-b17.db")
		h.store, err = (sqlite.Driver{}).Open(h.ctx, driver.Config{DSN: h.path})
	default:
		t.Fatalf("unknown B17 engine %q", engine)
	}
	if err != nil {
		t.Fatal(err)
	}
	h.lease, err = h.store.Lease(h.ctx, "s1-b17-holder", driver.LeaseTTL)
	if err != nil {
		_ = h.store.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { h.close(t) })
	return h
}

func (h *b17Harness) close(t *testing.T) {
	t.Helper()
	if h.lease != nil {
		if err := h.lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
		h.lease = nil
	}
	if h.store != nil {
		if err := h.store.Close(); err != nil {
			t.Error(err)
		}
		h.store = nil
	}
}

func (h *b17Harness) writer() *pipeline.Writer {
	return pipeline.New(h.store, h.lease,
		pipeline.WithOntology(service.NewOntology(nil)),
		pipeline.WithClock(func() time.Time { return h.now }),
		pipeline.WithIDGenerator(func() (string, error) {
			h.ids++
			return "pl-s1-b17", nil
		}),
	)
}

func b17Meta(n int) model.WriteMeta {
	return model.WriteMeta{Method: "tx.apply", Project: "s1-b17-b20", AgentLabel: "s1-b17-agent", IdempotencyKey: fmt.Sprintf("s1-b17-%032d", n)}
}

func b17Create() model.Op {
	return model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": "metadata anchor"}}}
}

func b17Set(ref model.Ref, key, value string) model.Op {
	return model.Op{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Target: ref, Key: key, Value: json.RawMessage(value)}}
}

func b17Patch(ref model.Ref, title string) model.Op {
	return model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: map[string]any{"title": title}}}
}

func b17Hash(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func b17Commit(t *testing.T, h *b17Harness, cs model.ChangeSet, n int) model.Commit {
	t.Helper()
	commit, err := h.writer().Apply(h.ctx, b17Actor, cs, b17Meta(n))
	if err != nil {
		t.Fatal(err)
	}
	return commit
}

type b17State struct {
	Meta     driver.Meta
	Body     []byte
	Versions []driver.Version
	Log      []model.Changeset
}

func b17StateOf(t *testing.T, h *b17Harness, ref model.Ref) b17State {
	t.Helper()
	meta, err := h.store.Meta(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := h.store.Read(h.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Get(h.ctx, []driver.Ref{ref})
	if err != nil || len(rows) != 1 {
		t.Fatalf("metadata row read: rows=%d err=%v", len(rows), err)
	}
	versions, err := r.Versions(h.ctx, ref, driver.PageReq{Limit: 100})
	if err != nil || versions.NextCursor != nil {
		t.Fatalf("metadata versions: %+v err=%v", versions, err)
	}
	log, err := r.Changesets(h.ctx, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	return b17State{Meta: meta, Body: append([]byte(nil), rows[0].Body...), Versions: versions.Items, Log: log}
}

func b17Document(t *testing.T, state b17State) pipeline.Document {
	t.Helper()
	doc, err := pipeline.DecodeDocument(state.Body)
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func TestB17MetadataCASCanonicalSiblingAndAtomicRefusal(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			h := openB17Harness(t, engine)
			created := b17Commit(t, h, model.ChangeSet{Ops: []model.Op{b17Create()}}, 1)
			if created.Revision.String() != "1" || len(created.Results) != 1 {
				t.Fatalf("create=%+v", created)
			}
			ref := created.Results[0].Ref
			seed := model.ChangeSet{Ops: []model.Op{
				b17Set(ref, "plugin.literal.dot/~key", `{"b":2,"a":1}`),
				b17Set(ref, "sibling", `{"kept":true}`),
			}}
			b17Commit(t, h, seed, 2)
			before := b17StateOf(t, h, ref)
			beforeDoc := b17Document(t, before)
			if string(beforeDoc.Metadata["plugin.literal.dot/~key"]) != `{"b":2,"a":1}` || string(beforeDoc.Metadata["sibling"]) != `{"kept":true}` {
				t.Fatalf("seed metadata=%+v", beforeDoc.Metadata)
			}

			// Canonical object ordering is independent of the stored byte spelling.
			swap := model.ChangeSet{
				Ops:    []model.Op{b17Patch(ref, "metadata swapped"), b17Set(ref, "plugin.literal.dot/~key", `{"next":true}`)},
				Expect: []model.Precondition{model.PreconditionByHash(ref, "metadata.plugin.literal.dot/~key", b17Hash(`{"a":1,"b":2}`))},
			}
			swapped := b17Commit(t, h, swap, 3)
			if swapped.Revision.String() != "3" || len(swapped.Results) != 2 {
				t.Fatalf("swap=%+v", swapped)
			}
			after := b17StateOf(t, h, ref)
			doc := b17Document(t, after)
			if string(doc.Metadata["plugin.literal.dot/~key"]) != `{"next":true}` || string(doc.Metadata["sibling"]) != `{"kept":true}` || doc.Fields["title"] != "metadata swapped" {
				t.Fatalf("swap lost sibling or patch: %+v", doc)
			}
			if len(after.Log) != len(before.Log)+1 || len(after.Versions) != len(before.Versions)+1 || len(after.Log[len(after.Log)-1].Changes) != 1 {
				t.Fatalf("swap history: before=%d after=%d versions=%d", len(before.Log), len(after.Log), len(after.Versions))
			}

			// A stale canonical expectation refuses the whole batch before any write.
			refusal := model.ChangeSet{
				Ops:    []model.Op{b17Patch(ref, "must not land"), b17Set(ref, "plugin.literal.dot/~key", `"refused"`)},
				Expect: []model.Precondition{model.PreconditionByHash(ref, "metadata.plugin.literal.dot/~key", b17Hash(`{"a":1,"b":3}`))},
			}
			_, err := h.writer().Apply(h.ctx, b17Actor, refusal, b17Meta(4))
			var typed *model.Error
			if !errors.As(err, &typed) || typed.Data.Code != "precondition_failed" {
				t.Fatalf("stale CAS error=%v", err)
			}
			if got := b17StateOf(t, h, ref); !reflect.DeepEqual(got, after) {
				t.Fatalf("stale CAS published state: before=%+v after=%+v", after, got)
			}

			// Equal target is a durable no-op. Missing keys do not equal JSON null.
			noop := model.ChangeSet{
				Ops:    []model.Op{b17Set(ref, "plugin.literal.dot/~key", `{"next":true}`)},
				Expect: []model.Precondition{model.PreconditionByHash(ref, "metadata.plugin.literal.dot/~key", b17Hash(`{"next":true}`))},
			}
			noOpCommit, err := h.writer().Apply(h.ctx, b17Actor, noop, b17Meta(5))
			if err != nil || noOpCommit.Revision.String() != after.Meta.Head.String() {
				t.Fatalf("no-op=%+v err=%v", noOpCommit, err)
			}
			if got := b17StateOf(t, h, ref); !reflect.DeepEqual(got, after) {
				t.Fatalf("equal CAS changed state: before=%+v after=%+v", after, got)
			}
			missing := model.ChangeSet{Ops: []model.Op{b17Patch(ref, "missing must refuse")}, Expect: []model.Precondition{model.PreconditionByHash(ref, "metadata.missing", b17Hash(`null`))}}
			_, err = h.writer().Apply(h.ctx, b17Actor, missing, b17Meta(6))
			if !errors.As(err, &typed) || typed.Data.Code != "precondition_failed" {
				t.Fatalf("absent-vs-null error=%v", err)
			}
			if got := b17StateOf(t, h, ref); !reflect.DeepEqual(got, after) {
				t.Fatalf("absent-vs-null refusal changed state")
			}
			t.Logf("B17 %s: canonical metadata CAS, sibling preservation, stale atomic refusal and equal no-op; fixed actor=%s head=%s", engine, b17Actor.ID, after.Meta.Head)
		})
	}
}

func TestB17SQLiteCASReopenRetainsHistoryAndNoOpBoundary(t *testing.T) {
	h := openB17Harness(t, "sqlite")
	created := b17Commit(t, h, model.ChangeSet{Ops: []model.Op{b17Create()}}, 10)
	ref := created.Results[0].Ref
	b17Commit(t, h, model.ChangeSet{Ops: []model.Op{b17Set(ref, "nullable", `{"value":1}`), b17Set(ref, "sibling", `true`)}}, 11)
	before := b17StateOf(t, h, ref)
	h.close(t)
	store, err := (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: h.path})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Lease(context.Background(), "s1-b17-reopen", driver.LeaseTTL)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	h.store, h.lease = store, lease
	after := b17StateOf(t, h, ref)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("SQLite reopen changed metadata graph: before=%+v after=%+v", before, after)
	}
	noOp := model.ChangeSet{Ops: []model.Op{b17Set(ref, "nullable", `{"value":1}`)}, Expect: []model.Precondition{model.PreconditionByHash(ref, "metadata.nullable", b17Hash(`{"value":1}`))}}
	commit, err := h.writer().Apply(h.ctx, b17Actor, noOp, b17Meta(12))
	if err != nil || commit.Revision.String() != before.Meta.Head.String() {
		t.Fatalf("reopened no-op=%+v err=%v", commit, err)
	}
	if got := b17StateOf(t, h, ref); !reflect.DeepEqual(got, before) {
		t.Fatal("reopened equal CAS changed durable history")
	}
	t.Logf("B17 SQLite close/reopen retained fixed metadata, version count=%d, changeset count=%d", len(after.Versions), len(after.Log))
}

func TestB17RetainedBeadsTwinsBindCanonicalCASDecisions(t *testing.T) {
	root := os.Getenv("TEST_SRCDIR")
	if root == "" {
		root = "."
	}
	path := filepath.Join(root, "_main", "spec", "recorded", "goldens.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != "26a086334357d55ea847daa301f45c9435a71bba75fb6c324456aebf459fbf32" {
		t.Fatalf("retained B17 bytes drifted: %s", got)
	}
	var manifest struct {
		Provenance struct {
			SourceCommit string   `json:"sourceCommit"`
			Recorded     []string `json:"recordedCards"`
		} `json:"provenance"`
		Recorders map[string]struct {
			Digest      string `json:"digest"`
			RepeatSHA   string `json:"repeatArtifactSha256"`
			ArtifactSHA string `json:"artifactSha256"`
			Projection  struct {
				Digest string `json:"digest"`
			} `json:"projection"`
		} `json:"recorders"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	b17, ok := manifest.Recorders["B17"]
	if !ok || manifest.Provenance.SourceCommit != "9d6fff760b7a314b476539198504d77ce0959656" || !reflect.DeepEqual(manifest.Provenance.Recorded, []string{"B17", "B24"}) {
		t.Fatalf("B17 retained provenance=%+v", manifest.Provenance)
	}
	if b17.Digest != "e91e136dd7fd0455a8c7416d9fd37ac252574ec9b7752934233b098e406d653b" || b17.ArtifactSHA == "" || b17.RepeatSHA == "" || b17.ArtifactSHA != b17.RepeatSHA || b17.Projection.Digest == "" {
		t.Fatalf("B17 twin evidence=%+v", b17)
	}
	t.Log("B17 retained two-run Beads twin has stable provenance; Plug decisions keep canonical CAS, sibling/no-op/refusal semantics and deviate only from the unsupported ephemeral plane")
}
