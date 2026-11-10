package cardb20

import (
	"encoding/json"
	"fmt"
)

// These are real public calls. The high-ranked unrelated tree, blocked P0
// descendant and hidden P0 descendant must not consume the selected page.
func (f *publicFront) parentSelectionControls(label string) map[string][]string {
	out := map[string][]string{}
	parent := f.ids["parent"]
	out["baseline"] = f.pagesWithFilter(label+"-all-descendants", map[string]any{"parent": parent}, []string{"child", "grandchild", "greatgrandchild", "priority-only"})
	out["priority1"] = f.pagesWithFilter(label+"-priority1", map[string]any{"parent": parent, "priority": 1}, []string{"child", "grandchild"})
	out["priority2"] = f.pagesWithFilter(label+"-priority2", map[string]any{"parent": parent, "priority": 2}, []string{"greatgrandchild", "priority-only"})
	f.pagesWithFilter(label+"-blocked-or-hidden-priority0", map[string]any{"parent": parent, "priority": 0}, []string{})
	f.pagesWithFilter(label+"-other-tree", map[string]any{"parent": f.ids["other-root"]}, []string{"other-child"})
	f.pagesWithFilter(label+"-unknown", map[string]any{"parent": "pl-b01-missing"}, []string{})
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	asOf := map[string]any{"revision": meta.Head, "incarnation": meta.Incarnation}
	// history.query is this receiver's list-like public method. Its stable ID
	// order is compared with native list --sort id, separately from ready order.
	out["list"] = f.pageMethod(label+"-list", "history.query", map[string]any{"parent": parent, "types": []string{"issue"}}, asOf, []string{"blocked-child", "child", "grandchild", "greatgrandchild", "priority-only"})
	out["list-priority2"] = f.pageMethod(label+"-list-priority2", "history.query", map[string]any{"parent": parent, "priority": 2}, asOf, []string{"greatgrandchild", "priority-only"})
	f.pageMethod(label+"-list-unknown", "history.query", map[string]any{"parent": "pl-b01-missing"}, asOf, []string{})
	for _, method := range []string{"bead.ready", "history.query"} {
		params := map[string]any{"filter": map[string]any{"parent": parent}, "limit": 1}
		if method == "history.query" {
			params["asOf"] = asOf
		}
		var first frontPage
		if json.Unmarshal(f.call(label+"-cursor-origin", method, params), &first) != nil || first.Next == nil || len(first.Items) != 1 {
			f.t.Fatal("missing genuine parent-bound cursor")
		}
		for _, changed := range []map[string]any{{"parent": f.ids["other-root"]}, {"parent": parent, "priority": 2}} {
			params["filter"] = changed
			params["cursor"] = *first.Next
			f.refuse(label+"-cursor-other-membership", method, params, "cursor_invalid")
		}
	}
	f.refuse(label+"-invalid-priority", "bead.ready", map[string]any{"filter": map[string]any{"parent": parent, "priority": 5}}, "invalid_params")
	frontSave(f.t, f.root, label+"-count-contract.json", frontJSON(f.t, map[string]any{
		"ready_descendant_count": 4, "priority1_count": 2, "priority2_count": 2,
		"list_task_descendant_count": 5, "query_limits": []int{1, 2, 1000},
		"counts":                     "exact concatenated public pages checked against complete literal membership",
		"standalone_total_count_api": "NOT_AVAILABLE_IN_SAVED_RECEIVER",
		"source":                     fmt.Sprint(meta.Head),
	}))
	return out
}
