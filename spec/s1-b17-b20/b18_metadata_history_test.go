package s1b17b20

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

type b18LiteralPatch struct {
	Op    string          `json:"op"`
	Path  string          `json:"path"`
	Value json.RawMessage `json:"value"`
}

// This pins Plug's retained B17 literal semantics through the public B18 read
// surface. It does not claim Beads JCS admission or native roundtrip fidelity.
func TestB18PublicHistoryPreservesMetadataLiteralsAndPointerPaths(t *testing.T) {
	const literalKey = "literal/~key"
	const beforeLiteral = `{"integer":9007199254740993,"nested":{"keep":null,"slash/~":1.25e-2},"spelling":1.0,"zero":-0.00}`
	const afterLiteral = `{"integer":9007199254740995,"nested":{"added":null,"keep":null,"slash/~":0.0125},"spelling":1,"zero":0}`
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			p := newPairedStore(t, engine)
			r := p.rpc(t)
			created := success[model.Commit](t, "bead.create", r.call(t, "", "bead.create", map[string]any{
				"type": "issue", "fields": map[string]any{"title": "literal history anchor"},
			}, true))
			if created.Revision.String() != "1" || len(created.Results) != 1 {
				t.Fatalf("create=%+v, want one result at revision 1", created)
			}
			ref := created.Results[0].Ref
			set := func(key, raw string) map[string]any {
				return map[string]any{"kind": "setMeta", "target": ref, "key": key, "value": json.RawMessage(raw)}
			}
			p.advance(t)
			seed := success[model.Commit](t, "tx.apply", r.call(t, "", "tx.apply", map[string]any{
				"changeSet": map[string]any{"ops": []any{
					set(literalKey, beforeLiteral), set("sibling", `{"kept":true}`), set("gone", `true`),
				}},
			}, true))
			p.advance(t)
			changed := success[model.Commit](t, "tx.apply", r.call(t, "", "tx.apply", map[string]any{
				"changeSet": map[string]any{"ops": []any{set(literalKey, afterLiteral), set("gone", `null`)}},
			}, true))
			if seed.Revision.String() != "2" || changed.Revision.String() != "3" {
				t.Fatalf("seed=%s changed=%s, want revisions 2 and 3", seed.Revision, changed.Revision)
			}
			durable := b17StateOf(t, p.b17Harness, ref)
			if len(durable.Versions) != 3 || len(durable.Log) != 3 {
				t.Fatalf("versions=%d changesets=%d, want one per durable transaction", len(durable.Versions), len(durable.Log))
			}

			check := func(t *testing.T) {
				t.Helper()
				for _, snapshot := range []struct {
					revision model.Natural
					metadata map[string]json.RawMessage
				}{
					{seed.Revision, map[string]json.RawMessage{literalKey: json.RawMessage(beforeLiteral), "sibling": json.RawMessage(`{"kept":true}`), "gone": json.RawMessage(`true`)}},
					{changed.Revision, map[string]json.RawMessage{literalKey: json.RawMessage(afterLiteral), "sibling": json.RawMessage(`{"kept":true}`)}},
				} {
					version := success[struct {
						Ref      model.Ref     `json:"ref"`
						Revision model.Natural `json:"revision"`
						Body     struct {
							Metadata map[string]json.RawMessage `json:"metadata"`
						} `json:"body"`
					}](t, "history.get", r.call(t, "", "history.get", map[string]any{
						"ref": ref, "asOf": map[string]any{"revision": snapshot.revision},
					}, false))
					// The typed address treats an omitted branch and branch:0 as
					// the same zero; compare public identity, not Go representation.
					if version.Ref.Validate() != nil || version.Ref.IsID() != ref.IsID() || version.Ref.ID != ref.ID || version.Ref.Key != ref.Key || version.Ref.Branch.String() != ref.Branch.String() || version.Revision.String() != snapshot.revision.String() || !reflect.DeepEqual(version.Body.Metadata, snapshot.metadata) {
						t.Fatalf("history.get revision %s: ref=%+v revision=%s metadata=%v, want %+v", snapshot.revision, version.Ref, version.Revision, version.Body.Metadata, snapshot.metadata)
					}
				}

				diff := success[struct {
					Patch        []b18LiteralPatch `json:"patch"`
					ChangedPaths []string          `json:"changedPaths"`
				}](t, "history.diff", r.call(t, "", "history.diff", map[string]any{
					"ref": ref, "fromRevision": seed.Revision, "toRevision": changed.Revision,
				}, false))
				// Expectations are literal public JSON Patch values, independent of
				// the production diff/escaping helpers. Missing remove.value differs
				// from the required JSON null on an add operation.
				wantPatch := []b18LiteralPatch{
					{Op: "remove", Path: "/metadata/gone"},
					{Op: "replace", Path: "/metadata/literal~1~0key/integer", Value: json.RawMessage(`9007199254740995`)},
					{Op: "add", Path: "/metadata/literal~1~0key/nested/added", Value: json.RawMessage(`null`)},
					{Op: "replace", Path: "/metadata/literal~1~0key/nested/slash~1~0", Value: json.RawMessage(`0.0125`)},
					{Op: "replace", Path: "/metadata/literal~1~0key/spelling", Value: json.RawMessage(`1`)},
					{Op: "replace", Path: "/metadata/literal~1~0key/zero", Value: json.RawMessage(`0`)},
				}
				wantPaths := []string{
					"/metadata/gone", "/metadata/literal~1~0key/integer", "/metadata/literal~1~0key/nested/added",
					"/metadata/literal~1~0key/nested/slash~1~0", "/metadata/literal~1~0key/spelling", "/metadata/literal~1~0key/zero",
				}
				if !reflect.DeepEqual(diff.Patch, wantPatch) || !reflect.DeepEqual(diff.ChangedPaths, wantPaths) {
					t.Fatalf("history.diff=%+v, want patch=%+v paths=%v", diff, wantPatch, wantPaths)
				}
				equal := success[struct {
					Patch        []b18LiteralPatch `json:"patch"`
					ChangedPaths []string          `json:"changedPaths"`
				}](t, "history.diff", r.call(t, "", "history.diff", map[string]any{
					"ref": ref, "fromRevision": changed.Revision, "toRevision": changed.Revision,
				}, false))
				if equal.Patch == nil || len(equal.Patch) != 0 || equal.ChangedPaths == nil || len(equal.ChangedPaths) != 0 {
					t.Fatalf("same-revision diff=%+v, want empty arrays", equal)
				}
				if got := b17StateOf(t, p.b17Harness, ref); !reflect.DeepEqual(got, durable) {
					t.Fatal("public history reads changed the durable row, head, versions or journal")
				}
			}
			t.Run("before_reopen", check)
			if engine != "memory" {
				p.reopen(t)
				r = p.rpc(t)
				t.Run("after_reopen", check)
			}
		})
	}
}
