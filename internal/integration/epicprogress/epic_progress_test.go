package epicprogress_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type graph struct{ parent, open, done, secret, grandchild, unrelated model.Ref }

func (f *fixture) graph(t *testing.T) graph {
	t.Helper()
	g := graph{parent: f.create(t, epics, "epic", nil)}
	g.open = f.create(t, visible, "visible open", &g.parent)
	g.done = f.create(t, visible, "visible complete", &g.parent)
	g.secret = f.create(t, hidden, "hidden child title must never escape", &g.parent)
	g.grandchild = f.create(t, visible, "grandchild is not direct", &g.open)
	g.unrelated = f.create(t, visible, "same type is not enough", nil)
	f.status(t, g.done, "done", visible)
	return g
}

func (f *fixture) status(t *testing.T, id model.Ref, status, project string) {
	t.Helper()
	committed(t, f.write(t, f.owner, "bead.update", project, map[string]any{"id": id, "fields": map[string]any{"status": status}}))
}

func (f *fixture) viewer(t *testing.T, allowed ...string) string {
	t.Helper()
	refs := make([]model.Ref, 0, len(allowed))
	for _, p := range allowed {
		refs = append(refs, projectRef(p))
	}
	return f.issue(t, model.PrincipalRef{Kind: "user", ID: "user:ep6-reader", Caveats: map[string]any{"projects": refs}}).Token
}

type progress struct {
	counts  map[string]int
	hidden  int
	refresh map[string]any
	asOf    map[string]any
}

// Progress must be a complete count-only answer. A green test cannot hide
// partial counts, hidden child identities/statuses, or an unauthorized cursor.
func inspectProgress(t *testing.T, r reply, want map[string]int, hiddenCount int) progress {
	t.Helper()
	value := result(t, r)
	items, ok := value["items"].([]any)
	if !ok || len(items) != 0 {
		t.Fatalf("progress contains row bodies: %s", r.Result)
	}
	if _, ok := value["nextCursor"]; ok {
		t.Fatal("atomic progress exposed a page cursor")
	}
	out := progress{counts: map[string]int{}, asOf: object(t, value["asOf"])}
	if out.asOf["revision"] == nil || out.asOf["incarnation"] == "" {
		t.Fatal("progress has no exact content snapshot")
	}
	groups, ok := value["groups"].([]any)
	if !ok {
		t.Fatalf("progress has no groups: %s", r.Result)
	}
	for _, raw := range groups {
		g := object(t, raw)
		key, ok := g["key"].(string)
		if !ok {
			t.Fatal("group has no canonical key")
		}
		if _, exists := out.counts[key]; exists {
			t.Fatalf("duplicate group %s", key)
		}
		n, ok := g["count"].(json.Number)
		if !ok {
			t.Fatal("group count is not an integer")
		}
		count, e := n.Int64()
		if e != nil || count < 0 {
			t.Fatal("invalid count")
		}
		out.counts[key] = int(count)
		rows, ok := g["items"].([]any)
		if !ok || len(rows) != 0 {
			t.Fatal("count group contains child bodies")
		}
		if raw, exists := g["elided"]; exists {
			if key != "total" {
				t.Fatal("hidden count appears in a status bucket")
			}
			elision := object(t, raw)
			n, ok := elision["count"].(json.Number)
			if !ok {
				t.Fatal("hidden count is not an integer")
			}
			number, e := n.Int64()
			if e != nil || number <= 0 {
				t.Fatal("invalid hidden remainder")
			}
			out.hidden = int(number)
			if elision["reason"] != "authorization" {
				t.Fatal("hidden remainder is not authorization elision")
			}
			continuation := object(t, elision["continueWith"])
			if continuation["method"] != "view.run" {
				t.Fatal("hidden remainder introduced another method")
			}
			out.refresh = object(t, continuation["params"])
			for _, key := range []string{"cursor", "asOf"} {
				if _, exists := out.refresh[key]; exists {
					t.Fatalf("hidden refresh retained %s", key)
				}
			}
		}
	}
	if !reflect.DeepEqual(out.counts, want) || out.hidden != hiddenCount {
		t.Fatalf("progress counts=%v hidden=%d, want %v hidden=%d: %s", out.counts, out.hidden, want, hiddenCount, r.Result)
	}
	return out
}

func object(t *testing.T, value any) map[string]any {
	t.Helper()
	out, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("expected object, got %T", value)
	}
	return out
}

func TestEpicProgressRealHTTPVisibilityAndFreshAuthority(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			f := newFixture(t, backend, t.TempDir()+"/store.db", true)
			g := f.graph(t)
			viewer := f.viewer(t, epics, visible)
			request := progressParams(g.parent, epics)
			first := inspectProgress(t, f.api.call(t, viewer, "view.run", request), map[string]int{"total": 2, "status:todo": 1, "status:done": 1}, 1)
			if !bytes.Equal(firstJSON(t, first.refresh), firstJSON(t, request)) {
				t.Fatalf("hidden refresh changes the authorized request: %v", first.refresh)
			}
			for _, canary := range []string{g.secret.ID, "hidden child title must never escape", hidden} {
				if bytes.Contains(f.api.call(t, viewer, "view.run", request).Result, []byte(canary)) {
					t.Fatalf("hidden child data escaped: %s", canary)
				}
			}
			// Changing a hidden status cannot alter a visible numerator or leak a
			// hidden status bucket. Actual mutation crosses the guarded HTTP Writer.
			f.status(t, g.secret, "failed", hidden)
			inspectProgress(t, f.api.call(t, viewer, "view.run", first.refresh), first.counts, 1)
			inspectProgress(t, f.api.call(t, f.viewer(t, projects...), "view.run", request), map[string]int{"total": 3, "status:todo": 1, "status:done": 1, "status:failed": 1}, 0)
			// Historical CONTENT remains pinned while a refresh reads current
			// content. Both still apply current authority.
			pinned := progressParams(g.parent, epics)
			pinned["asOf"] = first.asOf
			f.status(t, g.done, "todo", visible)
			inspectProgress(t, f.api.call(t, viewer, "view.run", pinned), first.counts, 1)
			inspectProgress(t, f.api.call(t, viewer, "view.run", first.refresh), map[string]int{"total": 2, "status:todo": 2}, 1)
			// A real credential revocation must reject even an old valid content
			// snapshot. CP7 User reads are not invented project-grant permissions.
			key := f.issue(t, model.PrincipalRef{Kind: "user", ID: "user:revoked-reader", Caveats: map[string]any{"projects": []model.Ref{projectRef(epics), projectRef(visible)}}})
			inspectProgress(t, f.api.call(t, key.Token, "view.run", pinned), first.counts, 1)
			if !f.keys.Revoke(key.ID) {
				t.Fatal("credential revocation did not happen")
			}
			requireCode(t, f.api.call(t, key.Token, "view.run", pinned), "unauthenticated")
			bad := progressParams(g.parent, epics)
			bad["cursor"] = "not-a-progress-cursor"
			requireCode(t, f.api.call(t, viewer, "view.run", bad), "cursor_invalid")
			wrongIncarnation := progressParams(g.parent, epics)
			wrongIncarnation["asOf"] = map[string]any{"revision": first.asOf["revision"], "incarnation": "different-incarnation"}
			requireCode(t, f.api.call(t, viewer, "view.run", wrongIncarnation), "incarnation_changed")
			requireCode(t, f.api.call(t, f.viewer(t, visible), "view.run", request), "not_found")
		})
	}
}

func TestEpicProgressHierarchyPermissionsAndAtomicRefusals(t *testing.T) {
	f := newFixture(t, "sqlite", t.TempDir()+"/permissions.db", true)
	parent := f.create(t, epics, "attach target", nil)
	child := f.create(t, visible, "child keeps its project", nil)
	old := f.create(t, oldEpics, "old parent private", nil)
	next := f.create(t, newEpics, "new parent", nil)
	move := func(token string, target model.Ref, parent *model.Ref, project string) reply {
		op := model.Op{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: target, Parent: parent, Project: project}}
		return f.write(t, token, "tx.apply", visible, map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{op}}})
	}
	for _, who := range []string{"child-only", "parent-only"} {
		before := f.head(t)
		key := f.issue(t, agent(who)).Token
		requireCode(t, move(key, child, &parent, ""), "forbidden")
		if f.head(t) != before {
			t.Fatalf("%s refusal changed head", who)
		}
		denied := f.write(t, key, "bead.create", visible, map[string]any{"type": "issue", "fields": map[string]any{"title": "must not exist"}, "parent": parent})
		requireCode(t, denied, "forbidden")
		if f.head(t) != before {
			t.Fatal("denied create partially committed")
		}
	}
	committed(t, move(f.owner, child, &old, ""))
	// Mover lacks every grant/caveat for the OLD project. Only the child and
	// NEW parent matter; the child's own project remains unchanged.
	mover := f.issue(t, agent("mover")).Token
	committed(t, move(mover, child, &next, ""))
	read := result(t, f.api.call(t, f.viewer(t, visible), "bead.get", map[string]any{"id": child, "_meta": map[string]any{"project": visible}}))
	bead := object(t, read["bead"])
	if bead["project"] != visible {
		t.Fatalf("reparent moved child project: %v", bead)
	}
	parentObject := object(t, bead["parent"])
	if len(parentObject) != 1 || parentObject["exists"] != true {
		t.Fatalf("hidden parent is not strict existence-only object: %v", parentObject)
	}
	if bytes.Contains(firstJSON(t, read), []byte(next.ID)) {
		t.Fatal("bead.get disclosed a hidden parent's identity")
	}
	meta, e := f.store.Meta(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	snapshot, e := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if e != nil {
		t.Fatal(e)
	}
	defer snapshot.Close()
	disclosure, e := f.app.Checker.ParentDisclosure(context.Background(), model.PrincipalRef{Kind: "user", ID: "user:ep6-reader", Caveats: map[string]any{"projects": []model.Ref{projectRef(visible)}}}, child, snapshot)
	if e != nil || !disclosure.Exists || disclosure.Ref != nil {
		t.Fatalf("parent disclosure is not existence-only: %+v %v", disclosure, e)
	}
	before := f.head(t)
	requireCode(t, move(f.owner, child, &child, ""), "ontology_guard_failed")
	requireCode(t, move(f.owner, child, &parent, hidden), "invalid_params")
	if f.head(t) != before {
		t.Fatal("cycle/project refusal partially wrote")
	}
	// The complete batch must roll back when a later operation is denied.
	batch := model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpCreate, Create: &model.CreateOp{Key: "earlier", Type: "issue", Project: visible, Fields: map[string]any{"title": "rolled back"}, Parent: &next}},
		{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: child, Parent: &parent}},
	}}
	requireCode(t, f.write(t, mover, "tx.apply", visible, map[string]any{"changeSet": batch}), "forbidden")
	if f.head(t) != before {
		t.Fatal("mixed-permission batch partially committed")
	}
	// A stored receipt is not a grant. The same replay must recheck CURRENT
	// child/new-parent authorization after the owner's grants are removed.
	request := map[string]any{"type": "issue", "fields": map[string]any{"title": "receipt current authority"}, "parent": parent}
	accepted := f.write(t, f.owner, "bead.create", visible, request)
	committed(t, accepted)
	replay := f.api.call(t, f.owner, "bead.create", request)
	committed(t, replay)
	if !replay.Meta.Replayed || !bytes.Equal(accepted.Result, replay.Result) {
		t.Fatal("identical hierarchy request did not replay")
	}
	revoked := f.base
	revoked.Grants = nil
	if err := f.facts.Replace(revoked); err != nil {
		t.Fatal(err)
	}
	before = f.head(t)
	requireCode(t, f.api.call(t, f.owner, "bead.create", request), "forbidden")
	if f.head(t) != before {
		t.Fatal("revoked replay changed the store")
	}
}

func TestEpicProgressCapabilityIsExplicit(t *testing.T) {
	f := newFixture(t, "memory", "", false)
	parent := f.create(t, epics, "capability disabled", nil)
	requireCode(t, f.api.call(t, f.viewer(t, epics), "view.run", progressParams(parent, epics)), "invalid_params")
}

func firstJSON(t *testing.T, value any) []byte {
	t.Helper()
	b, e := json.Marshal(value)
	if e != nil {
		t.Fatal(e)
	}
	return b
}

// The fifth task keeps the existing E3 load: 20,000 actual Writer-created
// beads, with 50 distinct sponsored agents. This is not the previous four
// browser tasks, a human usability study, or authenticated browser support.
func TestEpicProgressFifthTaskAtTwentyThousandBeads(t *testing.T) {
	f := newFixture(t, "sqlite", t.TempDir()+"/e3-epic.db", true)
	parent := f.create(t, epics, "fifth task epic", nil)
	var direct []model.Ref
	for i := 0; i < 50; i++ {
		p := agent("owner")
		p.ID = fmt.Sprintf("agent:ep6-%02d", i)
		key := f.issue(t, p).Token
		op := model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Project: visible, Fields: map[string]any{"title": fmt.Sprintf("agent child %02d", i)}, Parent: &parent}}
		direct = append(direct, committed(t, f.write(t, key, "tx.apply", visible, map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{op}}})).Results[0].Ref)
	}
	for start := 51; start < 20000; start += 100 {
		end := start + 100
		if end > 20000 {
			end = 20000
		}
		ops := make([]model.Op, 0, end-start)
		for i := start; i < end; i++ {
			ops = append(ops, model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Project: visible, Fields: map[string]any{"title": fmt.Sprintf("unrelated %05d", i)}}})
		}
		committed(t, f.write(t, f.owner, "tx.apply", visible, map[string]any{"changeSet": model.ChangeSet{Ops: ops}}))
	}
	// Enumerate actual durable records instead of trusting a seed counter.
	meta, e := f.store.Meta(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	r, e := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	seen := map[string]bool{}
	var cursor *driver.Cursor
	for {
		page, e := r.Query(context.Background(), driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000, Cursor: cursor})
		if e != nil {
			t.Fatal(e)
		}
		for _, row := range page.Items {
			if seen[row.Ref.ID] {
				t.Fatal("duplicate durable identity")
			}
			seen[row.Ref.ID] = true
		}
		if page.NextCursor == nil {
			break
		}
		cursor = page.NextCursor
	}
	if len(seen) != 20000 {
		t.Fatalf("stored population=%d, want 20000", len(seen))
	}
	for _, id := range direct {
		if !seen[id.ID] {
			t.Fatal("agent child was not durably stored")
		}
	}
	viewer := f.viewer(t, epics, visible)
	request := progressParams(parent, epics)
	started := time.Now()
	response := f.api.call(t, viewer, "view.run", request)
	elapsed := time.Since(started)
	inspectProgress(t, response, map[string]int{"total": 50, "status:todo": 50}, 0)
	t.Logf("EP6 actual fifth-task HTTP observation: 20000 stored beads, 50 agents, 50 direct children; view.run query_elapsed_ms=%.3f; total=50 status:todo=50 hidden=0; head=%s incarnation=%s; historical E3 stays 4/5 pending independently admitted integrated browser qualification", float64(elapsed)/float64(time.Millisecond), meta.Head.String(), meta.Incarnation)
}
