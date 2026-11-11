package s1b17b20

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
)

// Each write has a new identity. A semantic no-op cannot be satisfied by a
// replay of an earlier receipt. Original public bytes are retained before any
// response decoding, independently of the raw store/version readbacks.
type precisionClient struct {
	r        *runtime
	sequence int
	root     string
}

func newPrecisionClient(t *testing.T, r *runtime) *precisionClient {
	t.Helper()
	root := filepath.Join(os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), "b17-b18-precision", strings.ReplaceAll(t.Name(), "/", "_"))
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	return &precisionClient{r: r, root: root}
}

func (c *precisionClient) save(t *testing.T, name string, value []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(c.root, name), value, 0600); err != nil {
		t.Fatal(err)
	}
}

func (c *precisionClient) call(t *testing.T, method string, params map[string]any, write bool) response {
	t.Helper()
	c.sequence++
	input := cloneMap(params)
	meta := map[string]any{"project": "cardb20", "agent": "b17-b18-precision"}
	if write {
		meta["idempotencyKey"] = fmt.Sprintf("%064x", 10000+c.sequence)
	}
	input["_meta"] = meta
	request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": c.sequence, "method": method, "params": input})
	if err != nil {
		t.Fatal(err)
	}
	stem := fmt.Sprintf("%04d-%s", c.sequence, method)
	c.save(t, stem+".request.json", request)
	raw := c.r.server.Dispatch(rpc.WithPrincipal(c.r.ctx, c.r.principal), request)
	c.save(t, stem+".response.json", raw)
	var out response
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("%s: %v %s", method, err, raw)
	}
	return out
}

func (c *precisionClient) set(t *testing.T, ref model.Ref, key, value string, expect ...model.Precondition) model.Commit {
	t.Helper()
	return success[model.Commit](t, "tx.apply", c.call(t, "tx.apply", b17PublicSetMeta(ref, key, json.RawMessage(value), expect...), true))
}

func precisionState(t *testing.T, p *pairedStore, c *precisionClient, ref model.Ref, label string) b17State {
	t.Helper()
	s := b17StateOf(t, p.b17Harness, ref)
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	c.save(t, label+".store.json", raw)
	c.save(t, label+".body.json", s.Body)
	return s
}

func precisionUnchanged(t *testing.T, p *pairedStore, c *precisionClient, ref model.Ref, before b17State, label string, commit model.Commit) {
	t.Helper()
	if commit.Revision.String() != before.Meta.Head.String() || !reflect.DeepEqual(before, precisionState(t, p, c, ref, label)) {
		t.Fatalf("%s was not a semantic no-op at %s: %+v", label, before.Meta.Head, commit)
	}
}

func precisionOneVersion(t *testing.T, p *pairedStore, c *precisionClient, ref model.Ref, before b17State, label string, commit model.Commit) b17State {
	t.Helper()
	after := precisionState(t, p, c, ref, label)
	if len(after.Versions) != len(before.Versions)+1 || len(after.Log) != len(before.Log)+1 || after.Meta.Head.String() != commit.Revision.String() {
		t.Fatalf("%s: versions %d/%d log %d/%d commit=%s head=%s", label, len(before.Versions), len(after.Versions), len(before.Log), len(after.Log), commit.Revision, after.Meta.Head)
	}
	v := after.Versions[len(after.Versions)-1]
	log := after.Log[len(after.Log)-1]
	if v.Revision.String() != commit.Revision.String() || v.Ref.ID != ref.ID || log.Revision.String() != commit.Revision.String() || log.Incarnation != after.Meta.Incarnation || len(log.Changes) != 1 || log.Changes[0].Ref.ID != ref.ID {
		t.Fatalf("%s: committed version/address differs: %+v %+v", label, v, log)
	}
	return after
}

func precisionMetadata(t *testing.T, c *precisionClient, ref model.Ref, at model.Revision) map[string]json.RawMessage {
	t.Helper()
	v := success[struct {
		Revision model.Revision `json:"revision"`
		Body     struct {
			Metadata map[string]json.RawMessage `json:"metadata"`
		} `json:"body"`
	}](t, "history.get", c.call(t, "history.get", map[string]any{"ref": ref, "asOf": map[string]any{"revision": at}}, false))
	if v.Revision.String() != at.String() {
		t.Fatalf("history selected %s instead of exact %s", v.Revision, at)
	}
	return v.Body.Metadata
}

func TestB17B18PublicSequentialMetadataPrecisionAndNoOps(t *testing.T) {
	const key = "plugin.literal.dot/~key"
	const original = `{"b":[1.0,9007199254740993],"a":null}`
	const canonical = `{"a":null,"b":[1.0,9007199254740993]}`
	const sibling = `{"integer":9007199254740995,"null":null,"spelling":1.0}`
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			p := newPairedStore(t, engine)
			c := newPrecisionClient(t, p.rpc(t))
			created := success[model.Commit](t, "bead.create", c.call(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "sequential precision"}}, true))
			ref := created.Results[0].Ref
			before := precisionState(t, p, c, ref, "created")
			for i := 0; i < 2; i++ {
				precisionUnchanged(t, p, c, ref, before, fmt.Sprintf("absent-remove-%d", i), c.set(t, ref, "missing", `null`))
			}
			bad := c.call(t, "tx.apply", b17PublicSetMeta(ref, "missing", json.RawMessage(`true`), model.PreconditionByHash(ref, "metadata.missing", b17Hash(`null`))), true)
			if bad.Error == nil || bad.Error.Data.Code != "precondition_failed" || len(bad.Result) != 0 || !reflect.DeepEqual(before, precisionState(t, p, c, ref, "absent-is-not-null")) {
				t.Fatalf("absent matched null: %+v", bad)
			}
			seedSibling := c.set(t, ref, "sibling", sibling)
			before = precisionOneVersion(t, p, c, ref, before, "sibling", seedSibling)
			seed := c.set(t, ref, key, original)
			before = precisionOneVersion(t, p, c, ref, before, "literal-key", seed)
			for i := 0; i < 2; i++ {
				out := c.set(t, ref, key, canonical, model.PreconditionByHash(ref, "metadata."+key, b17Hash(canonical)))
				precisionUnchanged(t, p, c, ref, before, fmt.Sprintf("reordered-noop-%d", i), out)
			}
			var doc struct {
				Metadata map[string]json.RawMessage `json:"metadata"`
			}
			if err := json.Unmarshal(before.Body, &doc); err != nil || string(doc.Metadata[key]) != original || string(doc.Metadata["sibling"]) != sibling {
				t.Fatalf("no-op rewrote committed siblings: %s %v", before.Body, err)
			}
			integer := c.set(t, ref, "number", `1`)
			before = precisionOneVersion(t, p, c, ref, before, "integer", integer)
			decimal := c.set(t, ref, "number", `1.0`, model.PreconditionByHash(ref, "metadata.number", b17Hash(`1`)))
			before = precisionOneVersion(t, p, c, ref, before, "decimal", decimal)
			for i := 0; i < 2; i++ {
				precisionUnchanged(t, p, c, ref, before, fmt.Sprintf("decimal-noop-%d", i), c.set(t, ref, "number", `1.0`, model.PreconditionByHash(ref, "metadata.number", b17Hash(`1.0`))))
			}
			stale := c.call(t, "tx.apply", b17PublicSetMeta(ref, "number", json.RawMessage(`false`), model.PreconditionByHash(ref, "metadata.number", b17Hash(`1`))), true)
			if stale.Error == nil || stale.Error.Data.Code != "precondition_failed" || len(stale.Result) != 0 || !reflect.DeepEqual(before, precisionState(t, p, c, ref, "stale-literal")) {
				t.Fatalf("1 matched stored1.0: %+v", stale)
			}
			removed := c.set(t, ref, key, `null`, model.PreconditionByHash(ref, "metadata."+key, b17Hash(canonical)))
			before = precisionOneVersion(t, p, c, ref, before, "removed-literal-key", removed)
			for i := 0; i < 2; i++ {
				precisionUnchanged(t, p, c, ref, before, fmt.Sprintf("repeat-remove-%d", i), c.set(t, ref, key, `null`))
			}
			check := func(t *testing.T) {
				t.Helper()
				for _, vector := range []struct {
					revision model.Revision
					number   string
					hasKey   bool
				}{{seed.Revision, "", true}, {integer.Revision, "1", true}, {decimal.Revision, "1.0", true}, {removed.Revision, "1.0", false}} {
					metadata := precisionMetadata(t, c, ref, vector.revision)
					if string(metadata["sibling"]) != sibling || string(metadata["number"]) != vector.number || (metadata[key] != nil) != vector.hasKey || vector.hasKey && string(metadata[key]) != canonical {
						t.Fatalf("history at%s lost exact tokens: %v", vector.revision, metadata)
					}
				}
				get := success[struct {
					Bead struct {
						Revision model.Revision             `json:"revision"`
						Metadata map[string]json.RawMessage `json:"metadata"`
					} `json:"bead"`
				}](t, "bead.get", c.call(t, "bead.get", map[string]any{"id": ref}, false))
				if get.Bead.Revision.String() != removed.Revision.String() || string(get.Bead.Metadata["sibling"]) != sibling || string(get.Bead.Metadata["number"]) != `1.0` || get.Bead.Metadata[key] != nil {
					t.Fatalf("current read changed exact metadata: %+v", get)
				}
				if !reflect.DeepEqual(before, precisionState(t, p, c, ref, "read-invariance")) {
					t.Fatal("public reads changed committed source/history")
				}
			}
			t.Run("before_reopen", check)
			if engine != "memory" {
				p.reopen(t)
				c.r = p.rpc(t)
				t.Run("after_reopen", check)
				precisionUnchanged(t, p, c, ref, before, "reopened-fresh-noop", c.set(t, ref, "number", `1.0`))
			}
		})
	}
}
