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

func b27PublicOntology(t *testing.T) *service.Ontology {
	t.Helper()
	doc := ontology.DefaultDocument()
	for _, name := range []string{"design", "acceptance_criteria", "notes", "spec_id", "external_ref"} {
		doc.Types["issue"].Fields[name] = ontology.Field{Type: "string"}
	}
	doc.Types["issue"].Fields["estimated_minutes"] = ontology.Field{Type: "integer"}
	snapshot, problems := ontology.Compile(doc, nil, nil)
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
	if err != nil || !bytes.Equal(g, w) {
		t.Fatalf("%s: got %s want %s (%v)", name, g, w, err)
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
			b27Equal(t, "independent typed metadata", child["metadata"].(map[string]any)["b27"], metadata)
			comments := source.must("bead.get", map[string]any{"id": ref(ids[1])})["comments"].([]any)
			if len(comments) != 3 {
				t.Fatalf("comments=%v", comments)
			}
			for i, body := range []string{"first comment\n子", "second comment", "finished with exact reason 子"} {
				b27Equal(t, "independent comment body", comments[i].(map[string]any)["body"], body)
			}
			data, archive := source.dump()
			target := openB27PublicHost(t, engine, "target")
			if err := target.restore(data, archive); err != nil {
				t.Fatal(err)
			}
			assert := func() {
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
		})
	}
}

func ptrB27Ref(ref model.Ref) *model.Ref { return &ref }
