package postgresacceptance

import (
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func labels(t *testing.T, h *host, id string) []string {
	t.Helper()
	got := get(t, h, id, nil)
	raw, ok := got.Fields["labels"]
	if !ok || raw == nil {
		return nil
	}
	values, ok := raw.([]any)
	if !ok {
		t.Fatalf("labels for %s are not a JSON array: %T", id, raw)
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		label, ok := value.(string)
		if !ok {
			t.Fatalf("label for %s is not a string: %T", id, value)
		}
		out = append(out, label)
	}
	return out
}

func TestB16PublicLabelsPostgresReplacementInheritanceRefusalAndReopen(t *testing.T) {
	dsn, trace, sequence := freshDatabase(t), newTrace(t), newIDs("pl-b16-parent", "pl-b16-child")
	h := startHost(t, openStore(t, dsn), sequence, trace)

	parentParams := map[string]any{
		"type": "issue",
		"fields": map[string]any{
			"title":  "label parent",
			"labels": []string{"shared", "parent", "", "shared"},
		},
	}
	parent := committed(t, h.call(t, "bead.create", parentParams, key("b16-parent")), 1)
	parentID := parent.Results[0].Ref.ID
	if got := labels(t, h, parentID); !reflect.DeepEqual(got, []string{"parent", "shared"}) {
		t.Fatalf("create did not sort/deduplicate/drop empty labels: %v", got)
	}

	childParams := map[string]any{
		"type":   "issue",
		"parent": ref(parentID),
		"fields": map[string]any{"title": "label child", "labels": []string{"own", "shared"}},
	}
	child := committed(t, h.call(t, "bead.create", childParams, key("b16-child")), 2)
	childID := child.Results[0].Ref.ID
	if got := labels(t, h, childID); !reflect.DeepEqual(got, []string{"own", "parent", "shared"}) {
		t.Fatalf("create did not inherit the parent's sorted labels: %v", got)
	}
	if got := get(t, h, parentID, nil); got.Revision.String() != "1" {
		t.Fatalf("label inheritance changed parent revision: %+v", got)
	}

	committed(t, h.call(t, "bead.update", map[string]any{
		"id": ref(parentID), "fields": map[string]any{"labels": []string{"later"}},
	}, key("b16-parent-later")), 3)
	if got := labels(t, h, childID); !reflect.DeepEqual(got, []string{"own", "parent", "shared"}) {
		t.Fatalf("later parent update propagated into child: %v", got)
	}

	replacement := map[string]any{
		"id":     ref(childID),
		"fields": map[string]any{"labels": []string{"replace", "", "added", "replace"}},
	}
	changed := committed(t, h.call(t, "bead.update", replacement, key("b16-replace")), 4)
	if got := labels(t, h, childID); !reflect.DeepEqual(got, []string{"added", "replace"}) {
		t.Fatalf("whole-set replacement was not canonical: %v", got)
	}

	beforeNoop := graph(t, h.store)
	noop := committed(t, h.call(t, "bead.update", map[string]any{
		"id": ref(childID), "fields": map[string]any{"labels": []string{"replace", "added", "added", ""}},
	}, key("b16-noop")), 4)
	if len(noop.Results) != 1 || noop.Results[0].Revision.String() != "4" {
		t.Fatalf("equal label replacement changed the child revision: %+v", noop)
	}
	unchanged(t, beforeNoop, graph(t, h.store))

	beforeRefusal := graph(t, h.store)
	for name, bad := range map[string]any{
		"overlong":   strings.Repeat("界", 256),
		"non_string": []any{"ok", 3},
	} {
		requestKey := key("b16-refuse-" + name)
		refused(t, h.call(t, "bead.update", map[string]any{
			"id": ref(childID),
			"fields": map[string]any{
				"title":  "must remain",
				"labels": bad,
			},
		}, requestKey), "invalid_params")
		unchanged(t, beforeRefusal, graph(t, h.store))
	}
	if got := get(t, h, childID, nil); got.Fields["title"] != "label child" {
		t.Fatalf("invalid label update partially changed the title: %+v", got)
	}

	if err := h.close(); err != nil {
		t.Fatal(err)
	}
	h = startHost(t, openStore(t, dsn), sequence, trace)
	if got := labels(t, h, childID); !reflect.DeepEqual(got, []string{"added", "replace"}) {
		t.Fatalf("physical PostgreSQL reopen changed labels: %v", got)
	}
	replay(t, h.call(t, "bead.update", replacement, key("b16-replace")), changed.Result)
	unchanged(t, beforeRefusal, graph(t, h.store))
	if got := labels(t, h, parentID); !reflect.DeepEqual(got, []string{"later"}) {
		t.Fatalf("reopen changed parent labels: %v", got)
	}
	t.Log("B16 public PostgreSQL: sorted labels, create-time parent inheritance, whole-set idempotent replacement, bounded atomic refusals, physical reopen and replay")
}
