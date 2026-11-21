package s1b25b28_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/cli"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

// These controls execute the Plug producer and receiver through Client.Call.
// Their public fixture is authored here; it is not a Beads source recording,
// native config-union measurement, or full/partial native-history proof.
type b27PublicHost struct {
	t      *testing.T
	local  *cli.Local
	store  driver.Store
	engine string
	label  string
	dsn    string
	ids    int
	trace  []b27PublicExchange
}

type b27PublicExchange struct {
	Method  string          `json:"method"`
	Params  any             `json:"params"`
	Reply   json.RawMessage `json:"reply,omitempty"`
	Failure string          `json:"failure,omitempty"`
}

func b27PublicDocument() ontology.Document {
	doc := ontology.DefaultDocument()
	for _, name := range []string{"design", "acceptance_criteria", "notes", "spec_id", "external_ref"} {
		doc.Types["issue"].Fields[name] = ontology.Field{Type: "string"}
	}
	doc.Types["issue"].Fields["estimated_minutes"] = ontology.Field{Type: "integer"}
	return doc
}

func b27PublicOntology(t *testing.T) *service.Ontology {
	t.Helper()
	snapshot, problems := ontology.Compile(b27PublicDocument(), nil, nil)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	return service.NewOntology(snapshot.Effective)
}

func openB27PublicHost(t *testing.T, engine, label string) *b27PublicHost {
	t.Helper()
	h := &b27PublicHost{t: t, engine: engine, label: label}
	switch engine {
	case "sqlite":
		h.dsn = filepath.Join(t.TempDir(), "b27.db")
	case "postgres":
		h.dsn = testclock.PostgresDSN(t)
	case "memory":
	default:
		t.Fatalf("unknown engine %q", engine)
	}
	h.open()
	t.Cleanup(func() {
		if err := h.local.Close(); err != nil {
			t.Error(err)
		}
		if output := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); output != "" {
			name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()) + "-" + h.label
			data, err := json.MarshalIndent(h.trace, "", "  ")
			if err == nil {
				err = os.WriteFile(filepath.Join(output, name+"-rpc.json"), append(data, '\n'), 0600)
			}
			if err != nil {
				t.Error(err)
			}
		}
	})
	return h
}

func (h *b27PublicHost) open() {
	h.t.Helper()
	var engine driver.Driver = memory.Driver{}
	if h.engine == "sqlite" {
		engine = sqlite.Driver{}
	} else if h.engine == "postgres" {
		engine = postgres.Driver{}
	}
	ctx := context.Background()
	store, err := engine.Open(ctx, driver.Config{DSN: h.dsn})
	if err != nil {
		h.t.Fatal(err)
	}
	h.store = store
	// Host configuration is a stored source as well as a runtime resolver.
	// The ordinary public writes below cannot manufacture protected ontology
	// records. Use the existing conformance fixture seam before starting the
	// source host; the receiving host must remain genuinely empty.
	if h.label == "source" {
		meta, err := store.Meta(ctx)
		if err != nil {
			h.t.Fatal(err)
		}
		if meta.Head.IsZero() {
			lease, err := store.Lease(ctx, "b27-source-configuration", driver.LeaseTTL)
			if err != nil {
				h.t.Fatal(err)
			}
			raw := b27PublicSource(h.t)
			_, seedErr := conformance.Seed(ctx, store, lease, driver.ChangesetHeader{
				Principal: model.PrincipalRef{Kind: "anonymous", ID: "anonymous"},
				Time:      time.Date(2026, 11, 11, 11, 0, 0, 0, time.UTC),
			}, []conformance.FixtureChange{{Ref: model.RefByID("ontology/b27-public", model.Natural64(0)), Kind: "ontology", Op: model.OpCreate, Body: raw}})
			releaseErr := lease.Release(ctx)
			if seedErr != nil || releaseErr != nil {
				_ = store.Close()
				h.t.Fatalf("store authored host configuration: %v %v", seedErr, releaseErr)
			}
		}
	}
	h.local, err = cli.OpenLocal(ctx, cli.LocalConfig{
		Project: "alpha", Agent: "b27-public", Storage: cli.StorageConfig{Driver: "provided"},
		Factory: func(context.Context, cli.StorageConfig) (driver.Store, func() error, error) {
			return store, store.Close, nil
		},
		Ontology: b27PublicOntology(h.t),
		PipelineOptions: []pipeline.Option{
			pipeline.WithClock(func() time.Time { return time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC) }),
			pipeline.WithIDGenerator(func() (string, error) {
				h.ids++
				return fmt.Sprintf("alpha-%d", 40+h.ids), nil
			}),
		},
		PresenceVerifier: restore.PresenceVerifierFunc(func(ctx context.Context, method, digest string, p restore.Presence) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if method == "" || digest == "" || p["proof"] != "b27-public-disposable" {
				return fmt.Errorf("missing operation-bound disposable presence")
			}
			return nil
		}),
	})
	if err != nil {
		_ = store.Close()
		h.t.Fatal(err)
	}
}

func b27PublicSource(t *testing.T) []byte {
	t.Helper()
	raw, err := service.MarshalOntologySource(service.OntologySource{Base: b27PublicDocument()})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func (h *b27PublicHost) assertConfiguration() {
	h.t.Helper()
	ctx := context.Background()
	meta, err := h.store.Meta(ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	r, err := h.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		h.t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Get(ctx, []model.Ref{model.RefByID("ontology/b27-public", model.Natural64(0))})
	if err != nil || len(rows) != 1 || rows[0].Kind != "ontology" || !bytes.Equal(rows[0].Body, b27PublicSource(h.t)) {
		h.t.Fatalf("stored authored host configuration changed: %v %v", rows, err)
	}
}

func (h *b27PublicHost) call(method string, params map[string]any) (map[string]any, error) {
	h.t.Helper()
	reply, err := h.local.Client.Call(context.Background(), method, params, "")
	exchange := b27PublicExchange{Method: method, Params: params, Reply: append(json.RawMessage(nil), reply.JSON...)}
	if err != nil {
		exchange.Failure = err.Error()
		h.trace = append(h.trace, exchange)
		return nil, err
	}
	h.trace = append(h.trace, exchange)
	value, err := wire.DecodeJSON(reply.JSON)
	if err != nil {
		return nil, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s returned %T", method, value)
	}
	return object, nil
}

func (h *b27PublicHost) must(method string, params map[string]any) map[string]any {
	h.t.Helper()
	value, err := h.call(method, params)
	if err != nil {
		h.t.Fatalf("%s: %v", method, err)
	}
	return value
}

func b27Presence() map[string]any {
	return map[string]any{"mode": "presence_check", "challenge": "b27-public-disposable", "proof": "b27-public-disposable"}
}

func (h *b27PublicHost) dump() ([]byte, *dumpformat.Archive) {
	h.t.Helper()
	var data bytes.Buffer
	cursor, dumpID := "", ""
	for {
		params := map[string]any{"history": "head", "pageSize": 1024, "humanPresence": b27Presence()}
		if cursor != "" {
			params["cursor"] = cursor
		}
		page := h.must("admin.dump", params)
		id := page["dumpId"].(string)
		if dumpID != "" && dumpID != id {
			h.t.Fatal("public export changed its pinned dump ID")
		}
		dumpID = id
		chunk, err := base64.StdEncoding.DecodeString(page["data"].(string))
		if err != nil {
			h.t.Fatal(err)
		}
		data.Write(chunk)
		if page["done"].(bool) {
			break
		}
		cursor = fmt.Sprintf("%s:%s", id, page["nextPage"].(json.Number))
	}
	archive, err := dumpformat.Read(bytes.NewReader(data.Bytes()))
	if err != nil {
		h.t.Fatal(err)
	}
	if archive.Manifest().Scope != "plug-native-store" {
		h.t.Fatalf("public archive scope=%q", archive.Manifest().Scope)
	}
	h.t.Logf("actual public plug-dump/1 bytes=%d sha256=%x", data.Len(), sha256.Sum256(data.Bytes()))
	return data.Bytes(), archive
}

func (h *b27PublicHost) restore(data []byte, archive *dumpformat.Archive) error {
	h.t.Helper()
	manifest, ok := archive.Member("manifest.json")
	if !ok {
		return fmt.Errorf("public archive omitted manifest")
	}
	value, err := wire.DecodeJSON(manifest)
	if err != nil {
		return err
	}
	begin, err := h.call("admin.restore.begin", map[string]any{"manifest": value, "history": "head", "humanPresence": b27Presence()})
	if err != nil {
		return err
	}
	id := begin["restoreId"].(string)
	for page, offset := uint64(0), 0; offset < len(data); page++ {
		end := min(offset+1024, len(data))
		chunk := data[offset:end]
		sum := sha256.Sum256(chunk)
		if _, err = h.call("admin.restore.chunk", map[string]any{"restoreId": id, "page": page, "data": base64.StdEncoding.EncodeToString(chunk), "sha256": hex.EncodeToString(sum[:]), "humanPresence": b27Presence()}); err != nil {
			return err
		}
		offset = end
	}
	canonical, err := json.Marshal(archive.Manifest())
	if err != nil {
		return err
	}
	sum := sha256.Sum256(canonical)
	_, err = h.call("admin.restore.finish", map[string]any{"restoreId": id, "manifestSha256": hex.EncodeToString(sum[:]), "humanPresence": b27Presence()})
	return err
}

func (h *b27PublicHost) image(id string) map[string]any {
	h.t.Helper()
	ref := model.RefByID(id, model.Natural64(0))
	bead := h.must("bead.get", map[string]any{"id": ref})["bead"].(map[string]any)
	return h.must("history.get", map[string]any{"ref": ref, "asOf": map[string]any{"revision": bead["revision"]}})["body"].(map[string]any)
}

func b27Equal(t *testing.T, name string, got, want any) {
	t.Helper()
	g, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	w, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := wire.DecodeJSON(g)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := wire.DecodeJSON(w)
	// Lossless decoding makes object key order irrelevant while retaining
	// number token spellings, array order, absent members and explicit null.
	if err != nil || !reflect.DeepEqual(actual, expected) {
		t.Fatalf("%s: got %s want %s (%v)", name, g, w, err)
	}
}

func (h *b27PublicHost) assertCensus(ids []string) {
	h.t.Helper()
	meta, err := h.store.Meta(context.Background())
	if err != nil {
		h.t.Fatal(err)
	}
	want := map[string]bool{}
	for _, id := range ids {
		want[id] = true
	}
	seen := map[string]bool{}
	cursor := ""
	for page := 0; ; page++ {
		if page > len(ids) {
			h.t.Fatal("public census failed to terminate")
		}
		params := map[string]any{"asOf": map[string]any{"revision": meta.Head}, "limit": 1}
		if cursor != "" {
			params["cursor"] = cursor
		}
		value := h.must("history.query", params)
		at := value["asOf"].(map[string]any)
		b27Equal(h.t, "census snapshot revision", at["revision"], meta.Head)
		b27Equal(h.t, "census snapshot incarnation", at["incarnation"], meta.Incarnation)
		items := value["items"].([]any)
		if len(items) > 1 {
			h.t.Fatal("public census exceeded its page size")
		}
		for _, item := range items {
			id := item.(map[string]any)["id"].(string)
			if seen[id] {
				h.t.Fatalf("public census repeated %s", id)
			}
			seen[id] = true
		}
		cursor, _ = value["nextCursor"].(string)
		if cursor == "" {
			break
		}
	}
	if !reflect.DeepEqual(seen, want) {
		h.t.Fatalf("complete public census got %v want %v", seen, want)
	}
}

func TestB27PublicPlugRoundtripFieldsRelationsAndReopen(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			source := openB27PublicHost(t, engine, "source")
			fields := map[string]any{
				"title": "public interchange 子", "description": "first\nsecond", "design": "design body",
				"acceptance_criteria": "exact committed/readback values", "notes": "literal notes",
				"spec_id": "SPEC-27/子", "external_ref": "https://example.invalid/B27", "estimated_minutes": 37,
				"priority": 2, "labels": []string{"Alpha", "beta", "子"},
			}
			created := source.must("tx.apply", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
				{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": "parent"}}},
				{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: fields}},
				{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": "dependency target"}}},
			}}})
			results := created["results"].([]any)
			ids := []string{"alpha-41", "alpha-42", "alpha-43"}
			for i, id := range ids {
				b27Equal(t, "explicit public ID", results[i].(map[string]any)["ref"].(map[string]any)["id"], id)
			}
			ref := func(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
			metadata := json.RawMessage(`{"large":9007199254740993,"integer":1,"decimal":1.0,"nil":null,"flag":false,"nested":{"text":"子"}}`)
			source.must("tx.apply", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
				{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: ref(ids[1]), Parent: ptrB27Ref(ref(ids[0]))}},
				{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Target: ref(ids[1]), Key: "b27", Value: metadata}},
				{Kind: model.OpComment, Comment: &model.CommentOp{Target: ref(ids[1]), Body: "first comment\n子"}},
				{Kind: model.OpComment, Comment: &model.CommentOp{Target: ref(ids[1]), Body: "second comment"}},
				{Kind: model.OpLink, Link: &model.LinkOp{From: ref(ids[1]), To: ref(ids[2]), LinkKind: "relates-to"}},
			}}})
			source.must("bead.close", map[string]any{"id": ref(ids[1]), "reason": "finished with exact reason 子"})
			before := map[string]map[string]any{}
			for _, id := range ids {
				before[id] = source.image(id)
			}
			child := before[ids[1]]
			for name, want := range fields {
				if name == "labels" {
					continue // Core labels have their own stored member below.
				}
				b27Equal(t, "independent field "+name, child["fields"].(map[string]any)[name], want)
			}
			b27Equal(t, "independent parent", child["parent"], ref(ids[0]))
			b27Equal(t, "independent dependency", child["links"], []pipeline.Link{{To: ref(ids[2]), Kind: "relates-to"}})
			b27Equal(t, "independent closed status", child["fields"].(map[string]any)["status"], "done")
			b27Equal(t, "independent labels", child["labels"], []string{"Alpha", "beta", "子"})
			wantMetadata, err := wire.DecodeJSON(metadata)
			if err != nil {
				t.Fatal(err)
			}
			b27Equal(t, "independent typed metadata", child["metadata"].(map[string]any)["b27"], wantMetadata)
			comments := source.must("bead.get", map[string]any{"id": ref(ids[1])})["comments"].([]any)
			if len(comments) != 3 {
				t.Fatalf("comments=%v", comments)
			}
			for i, body := range []string{"first comment\n子", "second comment", "finished with exact reason 子"} {
				b27Equal(t, "independent comment body", comments[i].(map[string]any)["body"], body)
			}
			source.assertCensus(ids)
			source.assertConfiguration()
			data, archive := source.dump()
			if archive.Manifest().RecordCount != len(ids)+1 { // Three beads plus the actual stored ontology source.
				t.Fatal("public archive contains missing or extra record identities")
			}
			target := openB27PublicHost(t, engine, "target")
			if err := target.restore(data, archive); err != nil {
				t.Fatal(err)
			}
			assert := func() {
				target.assertCensus(ids)
				target.assertConfiguration()
				for _, id := range ids {
					b27Equal(t, "complete restored image "+id, target.image(id), before[id])
				}
				b27Equal(t, "complete restored comments", target.must("bead.get", map[string]any{"id": ref(ids[1])})["comments"], comments)
			}
			assert()
			if engine != "memory" {
				if err := target.local.Close(); err != nil {
					t.Fatal(err)
				}
				target.open()
				assert()
				t.Log("physical SQL close/reopen: full body, relations, comments and typed metadata exact")
			}
			// P44 is an empty-store restore contract. Refusing an initialized
			// target must conserve its entire actual public head snapshot.
			retained, _ := target.dump()
			meta, err := target.store.Meta(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			err = target.restore(data, archive)
			var typed *wire.Error
			if !errors.As(err, &typed) || typed.Data.Code != "restore_requires_empty" {
				t.Fatalf("nonempty restore must refuse: %v", err)
			}
			afterMeta, err := target.store.Meta(context.Background())
			if err != nil || !reflect.DeepEqual(meta, afterMeta) {
				t.Fatalf("refused restore changed metadata: %+v %+v %v", meta, afterMeta, err)
			}
			after, _ := target.dump()
			if !bytes.Equal(retained, after) {
				t.Fatal("refused restore changed actual complete public dump")
			}
			assert()
			if engine != "memory" {
				if err := target.local.Close(); err != nil {
					t.Fatal(err)
				}
				target.open()
				assert()
				reopenedMeta, err := target.store.Meta(context.Background())
				if err != nil || !reflect.DeepEqual(meta, reopenedMeta) {
					t.Fatalf("refusal metadata changed after reopen: %+v %+v %v", meta, reopenedMeta, err)
				}
				reopened, _ := target.dump()
				if !bytes.Equal(retained, reopened) {
					t.Fatal("refused restore changed persisted complete public dump after reopen")
				}
				t.Log("physical SQL close/reopen after nonempty refusal: retained census, complete bodies, dump and metadata exact")
			}
		})
	}
}

func ptrB27Ref(ref model.Ref) *model.Ref { return &ref }
