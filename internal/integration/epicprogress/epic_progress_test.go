package epicprogress_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
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
	method := map[string]string{"done": "bead.close", "failed": "bead.fail", "todo": "bead.reopen"}[status]
	if method == "" {
		t.Fatalf("unsupported fixture status %q", status)
	}
	before := f.head(t)
	requireCode(t, f.write(t, f.owner, method, project, map[string]any{"id": id}), "forbidden")
	if f.head(t) != before {
		t.Fatal("fixed-policy lifecycle refusal changed source state")
	}
	if f.lifecycle == nil {
		f.lifecycle = lifecycleFixture(t, f.store, f.lease)
	}
	f.sequence++
	params := map[string]any{"id": id, "_meta": map[string]any{"project": project, "idempotencyKey": fmt.Sprintf("%032x", f.sequence), "return": "full"}}
	committed(t, f.lifecycle.call(t, "", method, params))
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
			// hidden status bucket. The separate fixture host uses actual slot verbs;
			// counts continue through the API-key/fixed-policy HTTP boundary.
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
	viewer := f.viewer(t, visible)
	read := result(t, f.api.call(t, viewer, "bead.get", map[string]any{"id": child, "_meta": map[string]any{"project": visible}}))
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
	// Use the actual provider-detached principal from this HTTP credential.
	// Rebuilding a typed caveat literal would bypass its canonical admission form.
	principal, e := f.keys.Authenticate(context.Background(), viewer)
	if e != nil {
		t.Fatal(e)
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
	disclosure, binding, e := f.app.Checker.AuthorizeParentProjection(context.Background(), principal, snapshot, child)
	if e != nil || !disclosure.Exists || disclosure.Ref != nil || binding == "" {
		t.Fatalf("parent disclosure is not existence-only: %+v %v", disclosure, e)
	}
	if e := f.app.Checker.ValidateReadAuthorityBinding(context.Background(), principal, binding); e != nil {
		t.Fatalf("parent projection lost its current authority binding: %v", e)
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

func epicDefinition(parent model.Ref) *viewops.Definition {
	return &viewops.Definition{
		Filter:  &viewops.Filter{Query: "parent=" + parent.ID},
		Fields:  []string{},
		GroupBy: []string{viewops.EpicProgressGrouping},
	}
}

func buildLargeEpic(t *testing.T, f *fixture) (model.Ref, model.Ref) {
	t.Helper()
	// Populate this read-only regression through the real Writer and ordinary
	// ontology, without the independent fixed-policy bulk-write deadline.
	// The original authenticated 20k workload below is unchanged. No startup
	// builder or test-installed coverage substitutes for typed publication.
	writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(service.NewOntology(nil)))
	apply := func(ops []model.Op) model.Commit {
		t.Helper()
		f.sequence++
		out, err := writer.Apply(context.Background(), model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, model.ChangeSet{Ops: ops}, model.WriteMeta{
			Method: "tx.apply", Project: visible, Agent: "ep6-index-regression", AgentLabel: "ep6-index-regression", IdempotencyKey: fmt.Sprintf("%032x", f.sequence),
		})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	parent := apply([]model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Project: epics, Fields: map[string]any{"title": "indexed epic"}}}}).Results[0].Ref
	children := make([]model.Op, 0, 50)
	for i := 0; i < 50; i++ {
		children = append(children, model.Op{Kind: model.OpCreate, Create: &model.CreateOp{
			Type: "issue", Project: visible, Parent: &parent,
			Fields: map[string]any{"title": fmt.Sprintf("direct child %02d", i)},
		}})
	}
	created := apply(children)
	for start := 0; start < 19949; start += 100 {
		end := start + 100
		if end > 19949 {
			end = 19949
		}
		ops := make([]model.Op, 0, end-start)
		for i := start; i < end; i++ {
			ops = append(ops, model.Op{Kind: model.OpCreate, Create: &model.CreateOp{
				Type: "issue", Project: visible,
				Fields: map[string]any{"title": fmt.Sprintf("unrelated bead %05d", i)},
			}})
		}
		apply(ops)
	}
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var cursor *driver.Cursor
	for {
		page, err := r.Query(context.Background(), driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		count += len(page.Items)
		if page.NextCursor == nil {
			break
		}
		cursor = page.NextCursor
	}
	if count != 20000 {
		t.Fatalf("large epic population=%d, want 20000", count)
	}
	return parent, created.Results[0].Ref
}

func runProgress(t *testing.T, service *viewops.Service, by model.PrincipalRef, parent model.Ref, asOf *viewops.AsOf) (viewops.Result, error) {
	t.Helper()
	// The native HTTP host's ordinary read budget is 30 seconds. Do not
	// lengthen it to make the historical all-bead scan finish.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return service.Run(ctx, by, viewops.RunRequest{Definition: epicDefinition(parent), Project: epics, AsOf: asOf})
}

func errorData(t *testing.T, err error) model.ErrorData {
	t.Helper()
	var typed *model.Error
	if !errors.As(err, &typed) {
		t.Fatalf("error is not a model error: %v", err)
	}
	return typed.Data
}

func TestEpicProgressIndexedReadDoesNotScanTwentyThousandBeads(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newFixture(t, backend, t.TempDir()+"/indexed.db", true)
			parent, child := buildLargeEpic(t, f)
			// Make head-1 historical without removing any of the 20,000 beads.
			// The pinned and current answers must now differ in status only.
			f.status(t, child, "done", visible)
			viewer := f.viewer(t, epics, visible)
			by, err := f.keys.Authenticate(context.Background(), viewer)
			if err != nil {
				t.Fatal(err)
			}
			meta, err := f.store.Meta(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			head, err := meta.Head.Uint64()
			if err != nil || head == 0 {
				t.Fatalf("invalid head %s: %v", meta.Head, err)
			}
			for _, tc := range []struct {
				name string
				asOf *viewops.AsOf
				want map[string]int
			}{
				{name: "current", want: map[string]int{"total": 50, "status:todo": 49, "status:done": 1}},
				{name: "historical", asOf: &viewops.AsOf{Revision: model.Natural64(head - 1), Incarnation: meta.Incarnation}, want: map[string]int{"total": 50, "status:todo": 50}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					f.reads.reset()
					started := time.Now()
					result, err := runProgress(t, f.views, by, parent, tc.asOf)
					elapsed := time.Since(started)
					if f.reads.queryCount() != 0 {
						t.Errorf("epic progress performed whole-bead Query work: %d pages", f.reads.queryCount())
					}
					if f.reads.hierarchyCount() == 0 {
						t.Error("epic progress did not use the direct-child hierarchy index")
					}
					if err != nil {
						t.Fatal(err)
					}
					counts := map[string]int{}
					for _, group := range result.Groups {
						if group.Elided != nil || len(group.Items) != 0 {
							t.Fatalf("unexpected indexed progress group: %+v", group)
						}
						counts[group.Key] = group.Count
					}
					if !reflect.DeepEqual(counts, tc.want) || len(result.Items) != 0 || result.NextCursor != nil {
						t.Fatalf("indexed progress=%+v, want counts=%v", result, tc.want)
					}
					wantRevision := meta.Head
					if tc.asOf != nil {
						wantRevision = tc.asOf.Revision
					}
					if result.AsOf.Revision != wantRevision || result.AsOf.Incarnation != meta.Incarnation {
						t.Fatalf("indexed progress lost pinned snapshot: %+v", result.AsOf)
					}
					t.Logf("20000-bead %s epic progress: elapsed=%s whole-bead queries=%d hierarchy lookups=%d asOf=%s", tc.name, elapsed, f.reads.queryCount(), f.reads.hierarchyCount(), result.AsOf.Revision)
				})
			}
		})
	}
}

func TestEpicProgressIndexedResultMatchesScanPath(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newFixture(t, backend, t.TempDir()+"/equivalence.db", true)
			g := f.graph(t)
			viewer := f.viewer(t, epics, visible)
			by, err := f.keys.Authenticate(context.Background(), viewer)
			if err != nil {
				t.Fatal(err)
			}
			meta, err := f.store.Meta(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			historical := &viewops.AsOf{Revision: meta.Head, Incarnation: meta.Incarnation}
			f.status(t, g.done, "todo", visible)
			scanService, err := viewops.New(&unindexedReadStore{store: f.store}, f.lease, f.viewConfig)
			if err != nil {
				t.Fatal(err)
			}
			for _, asOf := range []*viewops.AsOf{nil, historical} {
				indexed, err := runProgress(t, f.views, by, g.parent, asOf)
				if err != nil {
					t.Fatal(err)
				}
				if len(indexed.Groups) == 0 || indexed.Groups[0].Elided == nil || indexed.Groups[0].Elided.Count != 1 {
					t.Fatalf("indexed result lost hidden-child remainder: %+v", indexed.Groups)
				}
				scanned, err := runProgress(t, scanService, by, g.parent, asOf)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(indexed, scanned) || !bytes.Equal(firstJSON(t, indexed), firstJSON(t, scanned)) {
					t.Fatalf("indexed result differs from scan path:\nindexed=%+v\nscanned=%+v", indexed, scanned)
				}
			}
			// A scope adapter's visible set must not bypass an additional host
			// read boundary. The denied visible child joins only the remainder.
			narrow := f.viewConfig
			baseRead := narrow.AuthorizeRead
			narrow.AuthorizeRead = func(ctx context.Context, by model.PrincipalRef, resource model.ResourceRef, project string) error {
				if resource.Ref.ID == g.done.ID {
					return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
				}
				return baseRead(ctx, by, resource, project)
			}
			narrowIndexed, err := viewops.New(&observedReadStore{store: f.store, observer: f.reads}, f.lease, narrow)
			if err != nil {
				t.Fatal(err)
			}
			narrowScan, err := viewops.New(&unindexedReadStore{store: f.store}, f.lease, narrow)
			if err != nil {
				t.Fatal(err)
			}
			indexed, err := runProgress(t, narrowIndexed, by, g.parent, nil)
			if err != nil {
				t.Fatal(err)
			}
			scanned, err := runProgress(t, narrowScan, by, g.parent, nil)
			if err != nil {
				t.Fatal(err)
			}
			wantGroups := []viewops.Group{
				{Key: "total", Count: 1, Items: []viewops.Bead{}, Elided: &viewops.Elision{Count: 2, Reason: "authorization"}},
				{Key: "status:todo", Count: 1, Items: []viewops.Bead{}},
			}
			if len(indexed.Items) != 0 || !reflect.DeepEqual(indexed.Groups, wantGroups) || !reflect.DeepEqual(indexed, scanned) {
				t.Fatalf("host read boundary changed: indexed=%s scanned=%s want groups=%s and no items",
					firstJSON(t, indexed), firstJSON(t, scanned), firstJSON(t, wantGroups))
			}
			// Real readers expose the interface even below coverage start. That
			// revision must still use the scan, never fabricate child absence.
			f.reads.reset()
			beforeCoverage := &viewops.AsOf{Revision: model.Natural64(0), Incarnation: meta.Incarnation}
			_, indexedErr := runProgress(t, f.views, by, g.parent, beforeCoverage)
			_, scannedErr := runProgress(t, scanService, by, g.parent, beforeCoverage)
			if !reflect.DeepEqual(errorData(t, indexedErr), errorData(t, scannedErr)) || errorData(t, indexedErr).Code != "not_found" || f.reads.queryCount() == 0 || f.reads.hierarchyCount() == 0 {
				t.Fatalf("pre-coverage fallback changed: indexed=%v scanned=%v queries=%d hierarchy=%d", indexedErr, scannedErr, f.reads.queryCount(), f.reads.hierarchyCount())
			}
			withoutParent, err := f.keys.Authenticate(context.Background(), f.viewer(t, visible))
			if err != nil {
				t.Fatal(err)
			}
			_, indexedErr = runProgress(t, f.views, withoutParent, g.parent, nil)
			_, scannedErr = runProgress(t, scanService, withoutParent, g.parent, nil)
			if !reflect.DeepEqual(errorData(t, indexedErr), errorData(t, scannedErr)) {
				t.Fatalf("hidden-parent refusal differs: indexed=%+v scanned=%+v", errorData(t, indexedErr), errorData(t, scannedErr))
			}
			if errorData(t, indexedErr).Code != "not_found" {
				t.Fatalf("hidden parent error=%+v, want not_found", errorData(t, indexedErr))
			}
		})
	}
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
