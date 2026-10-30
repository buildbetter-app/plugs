package cardb01

import (
	"reflect"
	"testing"
)

func TestB01SQLiteReadyMembershipUsesPlugCategories(t *testing.T) {
	f := newFixture(t, "pl-ready", "pl-backlog", "pl-completed")
	f.create(t, "ready", 1)
	f.create(t, "backlog", 0)
	f.create(t, "completed", 0)
	f.call(t, "bead.defer", map[string]any{"id": ref("pl-backlog"), "reason": "bounded category case"}, true)
	f.call(t, "bead.close", map[string]any{"id": ref("pl-completed"), "reason": "bounded category case"}, true)

	page := f.ready(t, 100, "")
	if got, want := ids(page.Items), []string{"pl-ready"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ready membership = %v, want %v", got, want)
	}
	if page.Items[0].Status != "todo" || f.get(t, "pl-backlog").Status != "backlog" || f.get(t, "pl-completed").Status != "done" {
		t.Fatalf("Plug category projection changed: ready=%+v backlog=%+v completed=%+v", page.Items[0], f.get(t, "pl-backlog"), f.get(t, "pl-completed"))
	}
	f.assertSQLite(t)
	t.Log("B01 bounded Plug projection: unstarted todo is ready; backlog and completed categories are excluded through actual lifecycle RPCs")
}

func TestB01SQLitePriorityIDTieAndTailInsertContinuation(t *testing.T) {
	f := newFixture(t, "pl-z-p0", "pl-b-tie", "pl-a-tie", "pl-tail")
	f.create(t, "priority zero", 0)
	f.create(t, "tie b", 1)
	f.create(t, "tie a", 1)

	first := f.ready(t, 1, "")
	if got := ids(first.Items); !reflect.DeepEqual(got, []string{"pl-z-p0"}) || first.NextCursor == "" {
		t.Fatalf("first priority page = %v, cursor present=%t", got, first.NextCursor != "")
	}
	issuedAt := first.AsOf.Revision.String()

	// This intervening write appends after the delivered prefix. The contract
	// permits arbitrary live reorders to skip or repeat rows; this case only
	// pins the useful monotonic-tail continuation and makes no broader claim.
	f.create(t, "tail insert", 4)
	want := []string{"pl-z-p0", "pl-a-tie", "pl-b-tie", "pl-tail"}
	seen := append([]string{}, ids(first.Items)...)
	cursor := first.NextCursor
	lastRevision := issuedAt
	for cursor != "" {
		page := f.ready(t, 1, cursor)
		if len(page.Items) != 1 {
			t.Fatalf("continuation page has %d items: %+v", len(page.Items), page)
		}
		seen = append(seen, page.Items[0].ID)
		lastRevision = page.AsOf.Revision.String()
		cursor = page.NextCursor
		if len(seen) > len(want) {
			t.Fatalf("continuation failed to terminate: %v", seen)
		}
	}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("priority/ID order or duplicate-free tail continuation = %v, want %v", seen, want)
	}
	unique := map[string]bool{}
	for _, id := range seen {
		if unique[id] {
			t.Fatalf("duplicate ID after intervening tail write: %s in %v", id, seen)
		}
		unique[id] = true
	}
	if issuedAt == lastRevision {
		t.Fatalf("continuation did not report the live post-write revision: %s", lastRevision)
	}
	f.assertSQLite(t)
	t.Log("B01 bounded Plug projection: priority then deterministic ID tie order; signed live cursor traversed one monotonic tail insert without duplicates")
}
