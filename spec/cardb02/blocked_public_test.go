package cardb02_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	proof "github.com/buildbetter-app/plugs/internal/integration/b07positive"
	public "github.com/buildbetter-app/plugs/internal/integration/b07positive/public"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflow "github.com/buildbetter-app/plugs/plugins/workflow"
)

type blockedFixture struct {
	h    *public.Host
	refs map[string]model.Ref
	seq  int
}
type blockedItem struct {
	Bead struct {
		ID, Status string
		Blocked    bool
		Parent     any
		Links      json.RawMessage
		Fields     map[string]any
	}
	BlockedBy                      []model.Ref
	BlockedByCount, HiddenBlockers int
}
type blockedPage struct {
	Items      []blockedItem
	NextCursor string
	AsOf       struct {
		Revision    model.Revision
		Incarnation string
	}
}

func (f *blockedFixture) call(method string, params map[string]any) public.Reply {
	f.seq++
	return f.h.Call(method, fmt.Sprintf("b02-%04d-%s", f.seq, method), params, "")
}
func (f *blockedFixture) write(method string, params map[string]any) model.Commit {
	f.h.T.Helper()
	reply := f.call(method, params)
	var commit model.Commit
	if err := json.Unmarshal(reply.Result, &commit); err != nil || len(commit.Results) == 0 {
		f.h.T.Fatal("actual public commit", err, string(reply.Result))
	}
	state := f.h.Capture()
	if !commit.Revision.IsZero() && commit.Revision != state.Meta.Head {
		f.h.T.Fatal("commit revision is not actual committed head")
	}
	for _, result := range commit.Results {
		found := false
		for _, row := range state.Rows {
			if row.Ref == result.Ref {
				found = true
				if row.Revision != commit.Revision || !bytes.Equal(row.Body, result.Record) {
					f.h.T.Fatal("public result differs from actual committed body/revision")
				}
			}
		}
		if !found {
			var receipts map[string]struct{ Result model.Commit }
			if json.Unmarshal(state.Workflow(f.h.T)["receipts"], &receipts) != nil {
				f.h.T.Fatal("missing actual workflow receipts")
			}
			for _, receipt := range receipts {
				if reflect.DeepEqual(receipt.Result, commit) {
					found = true
				}
			}
			if !found {
				f.h.T.Fatal("public result is absent from committed rows and workflow receipts")
			}
		}
	}
	return commit
}
func (f *blockedFixture) create(role string) {
	f.refs[role] = f.write("bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "B02 " + role}}).Results[0].Ref
}
func (f *blockedFixture) invoke(roles ...string) {
	checklist := []any{}
	for _, role := range roles {
		checklist = append(checklist, map[string]any{"label": role, "title": "B02 " + role})
	}
	reply := f.call("x.workflow.invoke", map[string]any{"template": "b02-blocked", "project": "b07positive", "mode": "ephemeral", "checklist": checklist})
	var result workflow.InvokeResult
	if err := json.Unmarshal(reply.Result, &result); err != nil || len(result.Items) != len(roles) || result.Run == nil {
		f.h.T.Fatal("actual ephemeral invocation", err)
	}
	for _, item := range result.Items {
		f.refs[item.Label] = model.RefByID(item.ID, model.Natural64(0))
		if item.Plane != "ephemeral" || item.Kind != workflow.ItemKind || item.RunID != result.Run.ID {
			f.h.T.Fatal("actual item identity missing")
		}
	}
	var items map[string]struct {
		ID, RunID, Label, Title, Status, Project string
		Generation                               uint64
	}
	if json.Unmarshal(f.h.Capture().Workflow(f.h.T)["items"], &items) != nil || len(items) != len(roles) {
		f.h.T.Fatal("actual invocation checkpoint item inventory")
	}
	for _, item := range result.Items {
		stored := items[item.ID]
		if stored.ID != item.ID || stored.RunID != result.Run.ID || stored.Label != item.Label || stored.Title != "B02 "+item.Label || stored.Status != "open" || stored.Project != "b07positive" || model.Natural64(stored.Generation) != item.Revision {
			f.h.T.Fatal("returned invocation differs from exact committed checkpoint", stored)
		}
	}
	capability, err := driver.RequireWorkflowCheckpointStore(f.h.Runtime.Store, f.h.Runtime.Lease)
	if err != nil {
		f.h.T.Fatal(err)
	}
	lifetime := driver.CheckpointLifetimeRestart
	if f.h.Config.Driver == "memory" {
		lifetime = driver.CheckpointLifetimeProcess
		state, err := pluginhost.NewWorkflowRuntimeState(capability, f.h.Runtime.Lease)
		if err != nil {
			f.h.T.Fatal(err)
		}
		marker, ok := state.(interface{ Persistent() bool })
		if !ok || marker.Persistent() {
			f.h.T.Fatal("memory must truthfully report Persistent false")
		}
	}
	if capability.WorkflowCheckpointLifetime() != lifetime {
		f.h.T.Fatal("wrong actual checkpoint lifetime")
	}
	proof.Save(f.h.T, f.h.Root, "b02-lifetime.json", map[string]any{"engine": f.h.Config.Driver, "lifetime": capability.WorkflowCheckpointLifetime(), "restartProof": f.h.Config.Driver != "memory"})
}
func (f *blockedFixture) link(from, to, kind string, metadata *model.LinkMetadata) {
	p := map[string]any{"from": f.refs[from], "to": f.refs[to], "kind": kind}
	if metadata != nil {
		p["metadata"] = metadata
	}
	f.write("bead.link", p)
}
func (f *blockedFixture) observe(phase string, want map[string][]string) {
	t := f.h.T
	t.Helper()
	before := f.h.Capture()
	roles := map[string]string{}
	for role, ref := range f.refs {
		roles[ref.ID] = role
	}
	check := func() {
		t.Helper()
		var page blockedPage
		reply := f.call("bead.blocked", map[string]any{"limit": 1000})
		if err := json.Unmarshal(reply.Result, &page); err != nil || page.NextCursor != "" {
			t.Fatal("complete blocked public page", err)
		}
		got := map[string][]string{}
		ids := []string{}
		for _, item := range page.Items {
			role := roles[item.Bead.ID]
			if role == "" || !item.Bead.Blocked || item.HiddenBlockers != 0 || item.BlockedByCount != len(item.BlockedBy) {
				t.Fatalf("unexpected subject/reasons %+v", item)
			}
			ids = append(ids, item.Bead.ID)
			got[role] = []string{}
			reasonIDs := []string{}
			for _, ref := range item.BlockedBy {
				if roles[ref.ID] == "" {
					t.Fatal("unbound returned reason")
				}
				got[role] = append(got[role], roles[ref.ID])
				reasonIDs = append(reasonIDs, ref.ID)
			}
			if !sort.StringsAreSorted(reasonIDs) {
				t.Fatal("reason order is not ID ascending")
			}
			sort.Strings(got[role])
		}
		expected := map[string][]string{}
		for role, reasons := range want {
			expected[role] = append([]string{}, reasons...)
			sort.Strings(expected[role])
		}
		if !sort.StringsAreSorted(ids) || !reflect.DeepEqual(got, expected) {
			t.Fatalf("%s actual blocked=%v expected=%v IDs=%v", phase, got, expected, ids)
		}
		if page.AsOf.Revision != before.Meta.Head || page.AsOf.Incarnation != before.Meta.Incarnation {
			t.Fatal("public asOf lost pinned head")
		}
	}
	check()
	check()
	proof.Same(t, before, f.h.Capture())
	proof.Save(t, f.h.Root, "b02-expected-"+phase+".json", want)
	if f.h.Config.Driver != "memory" {
		f.h.Reopen()
		proof.Same(t, before, f.h.Capture())
		check()
		proof.Same(t, before, f.h.Capture())
	}
}
func newBlockedFixture(t *testing.T, engine string) *blockedFixture {
	return &blockedFixture{h: public.NewHost(t, engine), refs: map[string]model.Ref{}}
}

func TestB02PublicBlockedMixedDiamondAndLiveReopen(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newBlockedFixture(t, engine)
			for _, role := range []string{"root", "right", "tail", "child", "grandchild"} {
				f.create(role)
			}
			f.invoke("left", "join")
			for _, edge := range [][2]string{{"left", "root"}, {"right", "root"}, {"join", "left"}, {"join", "right"}, {"tail", "join"}} {
				f.link(edge[0], edge[1], "blocks", nil)
			}
			f.link("child", "right", "parent-child", nil)
			f.link("grandchild", "child", "parent-child", nil)
			initial := map[string][]string{"left": {"root"}, "right": {"root"}, "join": {"left", "right"}, "tail": {"join"}, "child": {"right"}, "grandchild": {"child"}}
			f.observe("mixed-initial", initial)
			f.write("bead.close", map[string]any{"id": f.refs["root"]})
			f.observe("mixed-root-closed", map[string][]string{"join": {"left", "right"}, "tail": {"join"}})
			f.write("bead.reopen", map[string]any{"id": f.refs["root"]})
			f.observe("mixed-root-reopened", initial)
			f.write("bead.close", map[string]any{"id": f.refs["root"]})
			f.write("bead.close", map[string]any{"id": f.refs["left"]})
			f.observe("mixed-left-completed", map[string][]string{"join": {"right"}, "tail": {"join"}})
			f.write("bead.close", map[string]any{"id": f.refs["grandchild"]})
			f.write("bead.close", map[string]any{"id": f.refs["child"]})
			f.write("bead.close", map[string]any{"id": f.refs["right"]})
			f.observe("mixed-right-completed", map[string][]string{"tail": {"join"}})
			f.write("bead.close", map[string]any{"id": f.refs["join"]})
			f.observe("mixed-finished", map[string][]string{})
		})
	}
}

func TestB02PublicBlockedNativeGateReasonsAndStaleFlags(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newBlockedFixture(t, engine)
			for _, role := range []string{"root", "spawner", "open-child", "done-child", "all", "any-plus-root", "childless", "childless-any", "also", "stale-true", "closed-subject", "parent", "fallback", "open-parent", "open-fallback"} {
				f.create(role)
			}
			f.link("open-child", "spawner", "parent-child", nil)
			f.link("done-child", "spawner", "parent-child", nil)
			f.write("bead.close", map[string]any{"id": f.refs["done-child"]})
			f.link("all", "spawner", "waits-for", &model.LinkMetadata{Gate: "all-children"})
			f.link("any-plus-root", "spawner", "waits-for", &model.LinkMetadata{Gate: "any-children"})
			f.link("any-plus-root", "root", "blocks", nil)
			f.link("childless-any", "childless", "waits-for", &model.LinkMetadata{Gate: "any-children"})
			f.link("also", "childless", "waits-for", &model.LinkMetadata{Gate: "all-children", AlsoBlocks: true})
			f.link("parent", "root", "blocks", nil)
			f.link("fallback", "parent", "parent-child", nil)
			f.link("open-fallback", "open-parent", "parent-child", nil)
			f.link("open-fallback", "spawner", "waits-for", &model.LinkMetadata{Gate: "all-children"})
			f.write("bead.close", map[string]any{"id": f.refs["closed-subject"]})
			f.link("closed-subject", "root", "blocks", nil)
			state := f.h.Capture()
			changes := []conformance.FixtureChange{}
			for _, role := range []string{"all", "stale-true"} {
				ref := f.refs[role]
				row := state.Row(t, ref.ID)
				doc, err := pipeline.DecodeDocument(row.Body)
				if err != nil {
					t.Fatal(err)
				}
				doc.Fields["blocked"] = role == "stale-true"
				doc.Fields["updated_at"] = "2026-11-11T16:42:00Z"
				body, err := json.Marshal(doc)
				if err != nil {
					t.Fatal(err)
				}
				changes = append(changes, conformance.FixtureChange{Ref: ref, Kind: "bead", Op: model.OpPatch, Body: body})
			}
			if _, err := conformance.Seed(context.Background(), f.h.Runtime.Store, f.h.Runtime.Lease, driver.ChangesetHeader{Time: time.Date(2026, 11, 11, 17, 0, 0, 0, time.UTC)}, changes); err != nil {
				t.Fatal(err)
			}
			initial := map[string][]string{"all": {"spawner"}, "any-plus-root": {"root", "spawner"}, "also": {"childless"}, "parent": {"root"}, "fallback": {"parent"}, "open-fallback": {"spawner"}}
			f.observe("gates-initial-stale", initial)
			// Membership retains the live all-children condition. Native display omits
			// a closed spawner, even when it is the reason the stored set is blocked.
			for _, role := range []string{"open-child", "done-child"} {
				f.write("bead.unlink", map[string]any{"from": f.refs[role], "to": f.refs["spawner"], "kind": "parent-child"})
			}
			f.write("bead.close", map[string]any{"id": f.refs["spawner"]})
			for _, role := range []string{"open-child", "done-child"} {
				f.link(role, "spawner", "parent-child", nil)
			}
			f.observe("closed-spawner-open-child", map[string][]string{"any-plus-root": {"root"}, "also": {"childless"}, "parent": {"root"}, "fallback": {"parent"}, "open-fallback": {"open-parent"}})
			f.write("bead.reopen", map[string]any{"id": f.refs["spawner"]})
			f.observe("spawner-reopened", initial)
			f.write("bead.close", map[string]any{"id": f.refs["open-child"]})
			f.observe("all-children-completed", map[string][]string{"any-plus-root": {"root", "spawner"}, "also": {"childless"}, "parent": {"root"}, "fallback": {"parent"}})
			f.write("bead.close", map[string]any{"id": f.refs["childless"]})
			f.write("bead.unlink", map[string]any{"from": f.refs["parent"], "to": f.refs["root"], "kind": "blocks"})
			f.observe("also-spawner-completed", map[string][]string{"any-plus-root": {"root", "spawner"}})
			// Native controls for these literals are retained by the independent
			// original recorder. No native execution is inferred from this target.
		})
	}
}

func TestB02PublicBlockedPagingFiltersAndWorkflowVisibility(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newBlockedFixture(t, engine)
			for _, role := range []string{"root", "spawner", "waiter", "one", "two", "three"} {
				f.create(role)
			}
			f.invoke("owned", "hidden-child")
			f.link("owned", "root", "blocks", nil)
			f.link("hidden-child", "spawner", "parent-child", nil)
			f.link("waiter", "spawner", "waits-for", &model.LinkMetadata{Gate: "all-children"})
			for _, role := range []string{"one", "two", "three"} {
				f.link(role, "root", "blocks", nil)
			}
			f.write("bead.update", map[string]any{"id": f.refs["owned"], "fields": map[string]any{"priority": 1}})
			before := f.h.Capture()
			reply := f.call("bead.blocked", map[string]any{"limit": 1, "filter": map[string]any{"priority": 1}})
			var filtered blockedPage
			if json.Unmarshal(reply.Result, &filtered) != nil || len(filtered.Items) != 1 || filtered.Items[0].Bead.ID != f.refs["owned"].ID {
				t.Fatal("workflow priority filter lost real fields", string(reply.Result))
			}
			for _, selection := range []struct {
				name   string
				filter map[string]any
				want   []string
			}{
				{"durable-only", map[string]any{"includeEphemeral": false}, []string{f.refs["one"].ID, f.refs["two"].ID, f.refs["three"].ID, f.refs["waiter"].ID}},
				{"workflow-type", map[string]any{"types": []string{"task"}}, []string{f.refs["owned"].ID}},
				{"resource", map[string]any{"resources": []string{f.refs["owned"].ID}}, []string{f.refs["owned"].ID}},
				{"wrong-type", map[string]any{"types": []string{"absent-type"}}, []string{}},
			} {
				var selected blockedPage
				reply := f.call("bead.blocked", map[string]any{"limit": 1000, "filter": selection.filter})
				if json.Unmarshal(reply.Result, &selected) != nil || selected.NextCursor != "" {
					t.Fatal("complete filtered listing", selection.name)
				}
				actual := []string{}
				for _, item := range selected.Items {
					actual = append(actual, item.Bead.ID)
				}
				sort.Strings(selection.want)
				if !reflect.DeepEqual(actual, selection.want) {
					t.Fatalf("%s filter actual=%v want=%v", selection.name, actual, selection.want)
				}
			}
			cursor := ""
			ids := []string{}
			for {
				params := map[string]any{"limit": 1}
				if cursor != "" {
					params["cursor"] = cursor
				}
				reply := f.call("bead.blocked", params)
				var page blockedPage
				if json.Unmarshal(reply.Result, &page) != nil || len(page.Items) != 1 {
					t.Fatal("one-item public page")
				}
				ids = append(ids, page.Items[0].Bead.ID)
				cursor = page.NextCursor
				if cursor == "" {
					break
				}
				parts := strings.Split(cursor, ".")
				decoded, err := base64.RawURLEncoding.DecodeString(parts[0])
				if err != nil || bytes.Contains(decoded, []byte(f.refs["root"].ID)) || bytes.Contains(decoded, []byte(f.refs["hidden-child"].ID)) {
					t.Fatal("cursor exposes a graph identity")
				}
			}
			if len(ids) != 5 || !sort.StringsAreSorted(ids) {
				t.Fatal("full ID-ordered page sequence", ids)
			}
			proof.Same(t, before, f.h.Capture())
			// A different anonymous agent cannot read either workflow item. The hidden
			// child's topology still blocks the shared ordinary waiter.
			raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "bead.blocked", "params": map[string]any{"limit": 1000, "_meta": map[string]any{"project": "b07positive", "agent": "different-b02-owner"}}})
			response := f.h.Runtime.RPC.Dispatch(context.Background(), raw)
			proof.Write(t, f.h.Root, "b02-other-owner-request.json", raw)
			proof.Write(t, f.h.Root, "b02-other-owner-response.json", response)
			var other struct {
				Result blockedPage
				Error  any
			}
			if json.Unmarshal(response, &other) != nil || other.Error != nil {
				t.Fatal("other owner public blocked read", string(response))
			}
			found := false
			for _, item := range other.Result.Items {
				if item.Bead.ID == f.refs["waiter"].ID {
					found = true
					if item.BlockedByCount != 1 || item.HiddenBlockers != 0 || len(item.BlockedBy) != 1 || item.BlockedBy[0].ID != f.refs["spawner"].ID {
						t.Fatal("hidden child lost gate fact")
					}
				}
			}
			if !found || bytes.Contains(response, []byte(f.refs["hidden-child"].ID)) || bytes.Contains(response, []byte(f.refs["owned"].ID)) {
				t.Fatal("hidden workflow identity escaped or gate opened")
			}
			proof.Same(t, before, f.h.Capture())
			first := f.call("bead.blocked", map[string]any{"limit": 1})
			var page blockedPage
			json.Unmarshal(first.Result, &page)
			f.write("bead.update", map[string]any{"id": f.refs["one"], "fields": map[string]any{"title": "new head"}})
			f.seq++
			f.h.Call("bead.blocked", fmt.Sprintf("b02-stale-cursor-%d", f.seq), map[string]any{"limit": 1, "cursor": page.NextCursor}, "cursor_invalid")
		})
	}
}

func TestB02PublicBlockedUnresolvedReasonsAndSubjectCategories(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newBlockedFixture(t, engine)
			for _, role := range []string{"root", "pinned-subject", "backlog-subject", "dangling", "external", "orphan"} {
				f.create(role)
			}
			f.write("bead.update", map[string]any{"id": f.refs["pinned-subject"], "fields": map[string]any{"status": "pinned"}})
			f.write("bead.update", map[string]any{"id": f.refs["backlog-subject"], "fields": map[string]any{"status": "backlog"}})
			f.link("pinned-subject", "root", "blocks", nil)
			f.link("backlog-subject", "root", "blocks", nil)
			external := model.RefByID("external-b02-target", model.Natural64(0))
			f.write("bead.link", map[string]any{"from": f.refs["external"], "to": external, "kind": "blocks", "targetKind": "external"})
			beforeMissing := f.h.Capture()
			f.seq++
			f.h.Call("bead.link", fmt.Sprintf("b02-missing-%d", f.seq), map[string]any{"from": f.refs["dangling"], "to": model.RefByID("missing-b02-target", model.Natural64(0)), "kind": "blocks"}, "not_found")
			proof.Same(t, beforeMissing, f.h.Capture())
			// Public mutation refuses a missing local endpoint. Seed only the
			// deliberately corrupt read controls, then exercise the real method.
			changes := []conformance.FixtureChange{}
			for _, role := range []string{"dangling", "orphan"} {
				ref := f.refs[role]
				doc, err := pipeline.DecodeDocument(beforeMissing.Row(t, ref.ID).Body)
				if err != nil {
					t.Fatal(err)
				}
				missing := model.RefByID("missing-b02-"+role, model.Natural64(0))
				if role == "dangling" {
					doc.Links = []pipeline.Link{{To: missing, Kind: "blocks"}}
				} else {
					doc.Parent = &missing
				}
				body, err := json.Marshal(doc)
				if err != nil {
					t.Fatal(err)
				}
				changes = append(changes, conformance.FixtureChange{Ref: ref, Kind: "bead", Op: model.OpPatch, Body: body})
			}
			if _, err := conformance.Seed(context.Background(), f.h.Runtime.Store, f.h.Runtime.Lease, driver.ChangesetHeader{Time: time.Date(2026, 11, 11, 17, 10, 0, 0, time.UTC)}, changes); err != nil {
				t.Fatal(err)
			}
			before := f.h.Capture()
			check := func() {
				t.Helper()
				reply := f.call("bead.blocked", map[string]any{"limit": 1000})
				var page blockedPage
				if json.Unmarshal(reply.Result, &page) != nil || page.NextCursor != "" || len(page.Items) != 4 {
					t.Fatal("unresolved subject set", string(reply.Result))
				}
				got := map[string]int{}
				for _, item := range page.Items {
					got[item.Bead.ID] = item.HiddenBlockers
					if !item.Bead.Blocked || item.BlockedByCount != 1 {
						t.Fatal("unresolved reason must fail closed with exact count")
					}
					if item.Bead.ID == f.refs["backlog-subject"].ID {
						if item.HiddenBlockers != 0 || !reflect.DeepEqual(item.BlockedBy, []model.Ref{f.refs["root"]}) || item.Bead.Status != "backlog" {
							t.Fatal("backlog subject was treated as terminal")
						}
					} else if len(item.BlockedBy) != 0 || len(item.Bead.Links) != 0 {
						t.Fatal("unresolved ref disclosed")
					}
				}
				want := map[string]int{f.refs["backlog-subject"].ID: 0, f.refs["dangling"].ID: 1, f.refs["external"].ID: 1, f.refs["orphan"].ID: 1}
				if !reflect.DeepEqual(got, want) || bytes.Contains(reply.Result, []byte("missing-b02-")) || bytes.Contains(reply.Result, []byte(external.ID)) {
					t.Fatal("unresolved identities escaped or literal pinned subject included", string(reply.Result))
				}
			}
			check()
			check()
			proof.Same(t, before, f.h.Capture())
			if engine != "memory" {
				f.h.Reopen()
				proof.Same(t, before, f.h.Capture())
				check()
				proof.Same(t, before, f.h.Capture())
			}
		})
	}
}

func TestB02PublicBlockedAuthorizationBeforePageAndHiddenReasons(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newBlockedFixture(t, engine)
			for _, role := range []string{"secret-root", "secret-parent", "dependent-a", "dependent-b", "dependent-c"} {
				f.create(role)
			}
			f.link("secret-parent", "secret-root", "blocks", nil)
			dependents := []model.Ref{f.refs["dependent-a"], f.refs["dependent-b"], f.refs["dependent-c"]}
			sort.Slice(dependents, func(i, j int) bool { return dependents[i].ID < dependents[j].ID })
			for _, ref := range dependents {
				f.write("bead.link", map[string]any{"from": ref, "to": f.refs["secret-root"], "kind": "blocks"})
			}
			f.write("bead.unlink", map[string]any{"from": dependents[2], "to": f.refs["secret-root"], "kind": "blocks"})
			f.write("bead.link", map[string]any{"from": dependents[2], "to": f.refs["secret-parent"], "kind": "parent-child"})
			ctx := context.Background()
			meta, err := f.h.Runtime.Store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "b02-read-fixture", Authority: model.RefByID("b02-read-authority", model.Natural64(0))}
			keys := authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
			app, err := authzrpc.New(ctx, authzrpc.Config{Store: f.h.Runtime.Store, Lease: f.h.Runtime.Lease, Provider: keys, Namespace: ns, DefaultProject: "b07positive", Facts: authzrpc.StaticFacts{Facts: authzfixed.Facts{Namespace: ns, Projects: []authzfixed.Project{{Ref: model.RefByID("b02-project", model.Natural64(0)), Name: "b07positive", InstallationID: ns.InstallationID, Active: true}}}}})
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(app.Handler)
			defer server.Close()
			by := model.PrincipalRef{Kind: "user", ID: "b02-scoped-reader", Caveats: map[string]any{"refs": dependents[1:]}}
			issued, err := keys.Issue(by, time.Now().Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			call := func(label string, params map[string]any, token string) public.Reply {
				t.Helper()
				params["_meta"] = map[string]any{"project": "b07positive", "agent": "b02-auth-reader"}
				raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "bead.blocked", "params": params})
				if err != nil {
					t.Fatal(err)
				}
				request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/rpc/v1", bytes.NewReader(raw))
				if err != nil {
					t.Fatal(err)
				}
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("Plug-RPC", "1")
				request.Header.Set("Authorization", "Bearer "+token)
				response, err := server.Client().Do(request)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				body, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
				if err != nil {
					t.Fatal(err)
				}
				proof.Write(t, f.h.Root, "b02-auth-"+label+"-request.json", raw)
				proof.Write(t, f.h.Root, "b02-auth-"+label+"-response.json", body)
				for _, secret := range []string{dependents[0].ID, f.refs["secret-root"].ID, f.refs["secret-parent"].ID} {
					if bytes.Contains(body, []byte(secret)) {
						t.Fatal("hidden subject/reason escaped HTTP response", string(body))
					}
				}
				var reply public.Reply
				if json.Unmarshal(body, &reply) != nil || response.StatusCode != http.StatusOK {
					t.Fatal("actual guarded HTTP response", string(body))
				}
				return reply
			}
			before := f.h.Capture()
			cursor := ""
			for index := 0; index < 2; index++ {
				params := map[string]any{"limit": 1}
				if cursor != "" {
					params["cursor"] = cursor
				}
				reply := call(fmt.Sprintf("page-%d", index), params, issued.Token)
				var page blockedPage
				if reply.Error != nil || json.Unmarshal(reply.Result, &page) != nil || len(page.Items) != 1 {
					t.Fatal("denied front subject consumed page", string(reply.Result), reply.Error)
				}
				item := page.Items[0]
				if item.Bead.ID != dependents[index+1].ID || item.HiddenBlockers != 1 || item.BlockedByCount != 1 || len(item.BlockedBy) != 0 || len(item.Bead.Links) != 0 {
					t.Fatalf("hidden reason projection %+v", item)
				}
				if index == 1 {
					parent, _ := json.Marshal(item.Bead.Parent)
					if string(parent) != `{"exists":true}` {
						t.Fatal("hidden immediate parent not redacted", string(parent))
					}
				}
				cursor = page.NextCursor
				if index == 0 {
					parts := strings.Split(cursor, ".")
					payload, err := base64.RawURLEncoding.DecodeString(parts[0])
					if err != nil {
						t.Fatal(err)
					}
					for _, secret := range []string{f.refs["secret-root"].ID, f.refs["secret-parent"].ID} {
						if bytes.Contains(payload, []byte(secret)) {
							t.Fatal("hidden reason identity in cursor")
						}
					}
				} else if cursor != "" {
					t.Fatal("extra continuation")
				}
			}
			first := call("bound-first", map[string]any{"limit": 1}, issued.Token)
			var page blockedPage
			json.Unmarshal(first.Result, &page)
			mismatch := call("filter-mismatch", map[string]any{"limit": 1, "filter": map[string]any{"types": []string{"issue"}}, "cursor": page.NextCursor}, issued.Token)
			if mismatch.Error == nil || mismatch.Error.Data.Code != "cursor_invalid" {
				t.Fatal("filter mismatch accepted")
			}
			another, err := keys.Issue(model.PrincipalRef{Kind: "user", ID: "b02-another-reader", Caveats: by.Caveats}, time.Now().Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			mismatch = call("principal-mismatch", map[string]any{"limit": 1, "cursor": page.NextCursor}, another.Token)
			if mismatch.Error == nil || mismatch.Error.Data.Code != "cursor_invalid" {
				t.Fatal("principal mismatch accepted")
			}
			proof.Same(t, before, f.h.Capture())
		})
	}
}
