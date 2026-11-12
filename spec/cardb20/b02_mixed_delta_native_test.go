package cardb20

import (
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

type b02DeltaMixedEdge struct{ From, To, Kind, Gate string }

func b02DeltaSameRef(a, b model.Ref) bool {
	aBranch, aErr := a.Branch.Canonical()
	bBranch, bErr := b.Branch.Canonical()
	return aErr == nil && bErr == nil && a.IsID() == b.IsID() && a.ID == b.ID && aBranch == bBranch && a.Key == b.Key
}

func b02RecordNewMixedConditions(t *testing.T, cli, root string) {
	t.Helper()
	t.Run("new-mixed-all-any", func(t *testing.T) {
		w := newB02DeltaCLI(t, cli, filepath.Join(root, "new-mixed-all-any"))
		for _, role := range []string{"spawner", "durable-child"} {
			w.create(role)
		}
		w.ids["ephemeral-child"], w.ephemeral["ephemeral-child"] = "b02-ephemeral-child", true
		w.run("create-ephemeral-child", "create", "--id", w.ids["ephemeral-child"], "--title", "B02 ephemeral-child", "--type", "task", "--force", "--status", "open", "--ephemeral", "--json")
		for _, role := range []string{"durable-child", "ephemeral-child"} {
			w.run("parent-"+role, "update", w.ids[role], "--parent", w.ids["spawner"], "--json")
		}
		for _, waiter := range []struct {
			role, gate string
			ephemeral  bool
		}{
			{"durable-all", "all-children", false}, {"ephemeral-all", "all-children", true},
			{"durable-any", "any-children", false}, {"ephemeral-any", "any-children", true},
		} {
			w.ids[waiter.role] = "b02-" + waiter.role
			args := []string{"create", "--id", w.ids[waiter.role], "--title", "B02 " + waiter.role, "--type", "task", "--force", "--status", "open", "--waits-for", w.ids["spawner"], "--waits-for-gate", waiter.gate, "--json"}
			if waiter.ephemeral {
				w.ephemeral[waiter.role] = true
				args = append(args, "--ephemeral")
			}
			w.run("create-"+waiter.role, args...)
		}
		w.edges = []b02DeltaMixedEdge{
			{"durable-child", "spawner", "parent-child", ""}, {"ephemeral-child", "spawner", "parent-child", ""},
			{"durable-all", "spawner", "waits-for", "all-children"}, {"ephemeral-all", "spawner", "waits-for", "all-children"},
			{"durable-any", "spawner", "waits-for", "any-children"}, {"ephemeral-any", "spawner", "waits-for", "any-children"},
		}
		statuses := map[string]string{"spawner": "open", "durable-child": "open", "ephemeral-child": "open", "durable-all": "open", "ephemeral-all": "open", "durable-any": "open", "ephemeral-any": "open"}
		bothOpen := map[string][]string{"durable-all": {"spawner"}, "ephemeral-all": {"spawner"}, "durable-any": {"spawner"}, "ephemeral-any": {"spawner"}}
		oneClosed := map[string][]string{"durable-all": {"spawner"}, "ephemeral-all": {"spawner"}}
		w.observe("mixed-gates-both-open", bothOpen, statuses)
		w.run("close-durable-child", "close", w.ids["durable-child"], "--reason", "B02 mixed any child", "--json")
		statuses["durable-child"] = "closed"
		w.observe("mixed-gates-one-closed", oneClosed, statuses)
		w.run("reopen-durable-child", "reopen", w.ids["durable-child"], "--reason", "B02 mixed any live again", "--json")
		statuses["durable-child"] = "open"
		w.observe("mixed-gates-child-reopened", bothOpen, statuses)
		w.run("close-durable-child-again", "close", w.ids["durable-child"], "--reason", "B02 mixed all", "--json")
		statuses["durable-child"] = "closed"
		w.run("close-ephemeral-child", "close", w.ids["ephemeral-child"], "--reason", "B02 mixed all complete", "--json")
		statuses["ephemeral-child"] = "closed"
		w.observe("mixed-gates-children-closed", map[string][]string{}, statuses)
		w.run("close-spawner", "close", w.ids["spawner"], "--reason", "B02 mixed spawner", "--json")
		statuses["spawner"] = "closed"
		w.observe("mixed-gates-spawner-closed", map[string][]string{}, statuses)
		w.run("reopen-spawner", "reopen", w.ids["spawner"], "--reason", "B02 mixed live spawner", "--json")
		statuses["spawner"] = "open"
		w.observe("mixed-gates-spawner-reopened", map[string][]string{}, statuses)
		w.run("reopen-durable-child-final", "reopen", w.ids["durable-child"], "--reason", "B02 mixed live all", "--json")
		statuses["durable-child"] = "open"
		w.observe("mixed-gates-durable-child-live-again", oneClosed, statuses)
	})
	t.Run("new-mixed-conditional", func(t *testing.T) {
		w := newB02DeltaCLI(t, cli, filepath.Join(root, "new-mixed-conditional"))
		for _, role := range []string{"blocker", "durable-ordinary", "durable-conditional"} {
			w.create(role)
		}
		for _, role := range []string{"ephemeral-ordinary", "ephemeral-conditional", "ephemeral-blocker"} {
			w.ids[role], w.ephemeral[role] = "b02-"+role, true
			w.run("create-"+role, "create", "--id", w.ids[role], "--title", "B02 "+role, "--type", "task", "--force", "--status", "open", "--ephemeral", "--json")
		}
		w.edges = []b02DeltaMixedEdge{
			{"ephemeral-ordinary", "blocker", "blocks", ""}, {"ephemeral-conditional", "blocker", "conditional-blocks", ""},
			{"durable-ordinary", "ephemeral-blocker", "blocks", ""}, {"durable-conditional", "ephemeral-blocker", "conditional-blocks", ""},
		}
		for _, edge := range w.edges {
			w.run("edge-"+edge.From, "dep", "add", w.ids[edge.From], w.ids[edge.To], "--type", edge.Kind, "--json")
		}
		statuses := map[string]string{"blocker": "open", "durable-ordinary": "open", "durable-conditional": "open", "ephemeral-ordinary": "open", "ephemeral-conditional": "open", "ephemeral-blocker": "open"}
		live := map[string][]string{"ephemeral-ordinary": {"blocker"}, "ephemeral-conditional": {"blocker"}, "durable-ordinary": {"ephemeral-blocker"}, "durable-conditional": {"ephemeral-blocker"}}
		w.observe("mixed-conditional-live", live, statuses)
		w.run("close-durable-blocker", "close", w.ids["blocker"], "--reason", "B02 native has closed without Plug failure outcome", "--json")
		statuses["blocker"] = "closed"
		w.observe("mixed-conditional-native-closed", map[string][]string{"durable-ordinary": {"ephemeral-blocker"}, "durable-conditional": {"ephemeral-blocker"}}, statuses)
		w.run("reopen-durable-blocker", "reopen", w.ids["blocker"], "--reason", "B02 mixed condition live again", "--json")
		statuses["blocker"] = "open"
		w.observe("mixed-conditional-reopened", live, statuses)
		w.run("close-ephemeral-blocker", "close", w.ids["ephemeral-blocker"], "--reason", "B02 native retained ephemeral closed row", "--json")
		statuses["ephemeral-blocker"] = "closed"
		w.observe("mixed-conditional-native-ephemeral-closed", map[string][]string{"ephemeral-ordinary": {"blocker"}, "ephemeral-conditional": {"blocker"}}, statuses)
	})
}
