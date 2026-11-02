package labelcontract_test

import (
	"reflect"
	"strings"
	"testing"
)

func TestB16PublicLabelSetProjectionAndPreservation(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		id, first := f.create(map[string]any{"title": "labelled", "labels": []string{"z", " Alpha ", "a", "A", "", "z", "日本語"}}, "")
		f.labels(id, " Alpha ", "A", "a", "z", "日本語")
		f.call("bead.update", map[string]any{"id": id, "fields": map[string]any{"description": "other field"}}, true, "")
		f.call("bead.close", map[string]any{"id": id}, true, "")
		f.call("bead.reopen", map[string]any{"id": id}, true, "")
		f.labels(id, " Alpha ", "A", "a", "z", "日本語")
		original := f.call("history.get", map[string]any{"ref": map[string]any{"id": id}, "asOf": map[string]any{"revision": first}}, false, "")
		if got := stringsValue(t, original["body"].(map[string]any)["labels"]); !reflect.DeepEqual(got, []string{" Alpha ", "A", "a", "z", "日本語"}) {
			t.Fatalf("historical labels: %v", original)
		}
		if f.engine == "sqlite" {
			before := f.meta()
			f.close()
			f.open()
			if after := f.meta(); !reflect.DeepEqual(before, after) {
				t.Fatalf("reopen changed metadata: %+v -> %+v", before, after)
			}
			f.labels(id, " Alpha ", "A", "a", "z", "日本語")
		}
	})
}

func TestB16PublicLabelNoopsReplacementAndReplay(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		id, _ := f.create(map[string]any{"title": "noops", "labels": []string{"b", "a"}}, "")
		before := f.meta()
		bead := f.bead(id)
		params := map[string]any{"id": id, "fields": map[string]any{"labels": []string{"b", "", "a", "b"}}}
		commit := f.call("bead.update", params, true, "")
		key := f.lastKey
		if after := f.meta(); !reflect.DeepEqual(before, after) || commit["revision"] != bead["revision"] {
			t.Fatalf("equal label set minted a version: %v %+v -> %+v", commit, before, after)
		}
		if replay := f.request("bead.update", params, key, ""); !reflect.DeepEqual(replay, commit) || !reflect.DeepEqual(f.meta(), before) {
			t.Fatalf("no-op replay changed response or state: %v vs %v", replay, commit)
		}
		f.call("bead.update", map[string]any{"id": id, "fields": map[string]any{"labels": []string{"new"}}}, true, "")
		f.labels(id, "new")
		f.call("bead.update", map[string]any{"id": id, "fields": map[string]any{"labels": []string{}}}, true, "")
		f.labels(id)
		before = f.meta()
		f.call("bead.update", map[string]any{"id": id, "fields": map[string]any{"labels": []string{""}}}, true, "")
		if !reflect.DeepEqual(before, f.meta()) {
			t.Fatal("empty-only repeated replacement minted a version")
		}
		plain, _ := f.create(map[string]any{"title": "never labelled"}, "")
		before = f.meta()
		f.call("bead.update", map[string]any{"id": plain, "fields": map[string]any{"labels": []string{}}}, true, "")
		if !reflect.DeepEqual(before, f.meta()) {
			t.Fatal("setting an absent label set to empty minted a version")
		}
	})
}

func TestB16PublicLabelBoundsAndAtomicRefusals(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		max := strings.Repeat("界", 255)
		id, _ := f.create(map[string]any{"title": "bounds", "labels": []string{max}}, "")
		f.labels(id, max)
		for _, bad := range []any{strings.Repeat("a", 256), []string{strings.Repeat("界", 256)}, []any{"ok", 3}, map[string]any{"add": []string{"invented"}}, true} {
			f.refuse("bead.update", map[string]any{"id": id, "fields": map[string]any{"labels": bad}}, "invalid_params")
			f.labels(id, max)
		}
		f.refuse("tx.apply", map[string]any{"changeSet": map[string]any{"ops": []any{
			map[string]any{"kind": "patch", "target": map[string]any{"id": id}, "fields": map[string]any{"title": "must roll back"}},
			patch(id, []string{strings.Repeat("a", 256)}),
		}}}, "invalid_params")
		if got := f.bead(id)["fields"].(map[string]any)["title"]; got != "bounds" {
			t.Fatalf("failed batch changed title: %v", got)
		}
		before := f.meta()
		f.call("bead.update", map[string]any{"id": id, "fields": map[string]any{"labels": []string{"missing key"}}}, false, "invalid_params")
		if !reflect.DeepEqual(before, f.meta()) {
			t.Fatal("missing idempotency key wrote labels")
		}
	})
}

func TestB16PublicLabelCreateInheritsTrackedParentSet(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		parent, _ := f.create(map[string]any{"title": "parent", "labels": []string{"shared", "parent"}}, "")
		parentRevision := f.bead(parent)["revision"]
		child, _ := f.create(map[string]any{"title": "child", "labels": []string{"own", "shared"}}, parent)
		f.labels(child, "own", "parent", "shared")
		if f.bead(parent)["revision"] != parentRevision {
			t.Fatal("inheriting labels wrote the parent")
		}
		f.call("bead.update", map[string]any{"id": parent, "fields": map[string]any{"labels": []string{"later"}}}, true, "")
		f.labels(child, "own", "parent", "shared")
		commit := f.call("tx.apply", map[string]any{"changeSet": map[string]any{"ops": []any{
			map[string]any{"kind": "create", "key": "new-parent", "type": "issue", "fields": map[string]any{"title": "batch parent", "labels": []string{"earlier"}}},
			map[string]any{"kind": "patch", "target": map[string]any{"key": "new-parent"}, "fields": map[string]any{"labels": []string{"batch"}}},
			map[string]any{"kind": "create", "type": "issue", "parent": map[string]any{"key": "new-parent"}, "fields": map[string]any{"title": "batch child", "labels": []string{"own"}}},
		}}}, true, "")
		f.labels(resultID(commit, 2), "batch", "own")
		if f.engine == "sqlite" {
			f.close()
			f.open()
			f.labels(child, "own", "parent", "shared")
			f.labels(resultID(commit, 2), "batch", "own")
		}
	})
}

// The frozen patch wire carries whole field values. Add/remove and ordered
// replacement compose revision-guarded sets; no label-only wire object is used.
func TestB16PublicLabelOrderedPatchCASAndRemoveWins(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		id, first := f.create(map[string]any{"title": "ordered", "labels": []string{"old"}}, "")
		commit := f.call("tx.apply", map[string]any{"changeSet": map[string]any{"expect": expect(id, first), "ops": []any{
			patch(id, []string{"replace", "both"}),
			patch(id, []string{"replace", "both", "added"}),
			patch(id, []string{"replace", "added"}),
		}}}, true, "")
		f.labels(id, "added", "replace")
		if commit["revision"] == first {
			t.Fatal("changed label batch did not mint a revision")
		}
		f.refuse("bead.update", map[string]any{"id": id, "fields": map[string]any{"labels": []string{"stale"}}, "expect": expect(id, first)}, "precondition_failed")
		f.labels(id, "added", "replace")
		f.call("bead.update", map[string]any{"id": id, "fields": map[string]any{"labels": []string{"replace", "added", "retry"}}, "expect": expect(id, f.bead(id)["revision"])}, true, "")
		f.labels(id, "added", "replace", "retry")
		old := f.call("history.get", map[string]any{"ref": map[string]any{"id": id}, "asOf": map[string]any{"revision": first}}, false, "")
		if got := stringsValue(t, old["body"].(map[string]any)["labels"]); !reflect.DeepEqual(got, []string{"old"}) {
			t.Fatalf("batch rewrote history: %v", old)
		}
	})
}

func TestB16PublicLabelRenameFoldUsesGuardedAtomicSets(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		a, ar := f.create(map[string]any{"title": "rename", "labels": []string{"old", "other"}}, "")
		b, br := f.create(map[string]any{"title": "merge", "labels": []string{"old", "new"}}, "")
		// A concurrent addition changes the target snapshot. The whole migration
		// must refuse rather than lose the addition or partially rename a.
		f.call("bead.update", map[string]any{"id": b, "fields": map[string]any{"labels": []string{"old", "new", "concurrent"}}}, true, "")
		ops := []any{patch(a, []string{"new", "other"}), patch(b, []string{"new"})}
		expects := append(expect(a, ar), expect(b, br)...)
		f.refuse("tx.apply", map[string]any{"changeSet": map[string]any{"expect": expects, "ops": ops}}, "precondition_failed")
		f.labels(a, "old", "other")
		f.labels(b, "concurrent", "new", "old")
		ops[1] = patch(b, []string{"new", "concurrent"})
		expects = append(expect(a, ar), expect(b, f.bead(b)["revision"])...)
		commit := f.call("tx.apply", map[string]any{"changeSet": map[string]any{"expect": expects, "ops": ops}}, true, "")
		if len(commit["results"].([]any)) != 2 {
			t.Fatalf("rename fold result count: %v", commit)
		}
		f.labels(a, "new", "other")
		f.labels(b, "concurrent", "new")
	})
}
