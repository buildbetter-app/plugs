package cardb20

import (
	"encoding/json"
	"os"
	"sort"
	"testing"
)

// B02-specific independent expectations. Native originals and adopted Plug
// results are separate; neither is generated from the other's returned rows.
type b02GraphNode struct {
	Role, Parent, Spawner, Gate string
	Ephemeral                   bool
}
type b02GraphEdge struct{ Dependent, Blocker, Kind string }
type b02GraphStep struct {
	Name, Action, Role, Target, Value, PlugMethod string
	NativeReady, PlugReady                        []string
	NativeBlocked                                 map[string][]string
	AdoptedDifference                             string
}
type b02GraphPlan struct {
	Name   string
	Nodes  []b02GraphNode
	Edges  []b02GraphEdge
	Steps  []b02GraphStep
	Custom bool
}

func b02GraphPlans() []b02GraphPlan {
	diamond := b02GraphPlan{
		Name:  "mixed-diamond",
		Nodes: []b02GraphNode{{Role: "root"}, {Role: "left", Ephemeral: true}, {Role: "right"}, {Role: "join", Ephemeral: true}, {Role: "tail"}},
		Edges: []b02GraphEdge{{"left", "root", "blocks"}, {"right", "root", "blocks"}, {"join", "left", "blocks"}, {"join", "right", "blocks"}, {"tail", "join", "blocks"}},
		Steps: []b02GraphStep{
			{Name: "initial", NativeReady: []string{"root"}, PlugReady: []string{"root"}, NativeBlocked: map[string][]string{"left": {"root"}, "right": {"root"}, "join": {"left", "right"}, "tail": {"join"}}},
			{Name: "root-closed", Action: "close", Role: "root", PlugMethod: "bead.close", NativeReady: []string{"left", "right"}, PlugReady: []string{"left", "right"}, NativeBlocked: map[string][]string{"join": {"left", "right"}, "tail": {"join"}}},
			{Name: "root-reopened", Action: "reopen", Role: "root", PlugMethod: "bead.reopen", NativeReady: []string{"root"}, PlugReady: []string{"root"}, NativeBlocked: map[string][]string{"left": {"root"}, "right": {"root"}, "join": {"left", "right"}, "tail": {"join"}}},
			{Name: "left-unlinked", Action: "unlink", Role: "left", Target: "root", NativeReady: []string{"root", "left"}, PlugReady: []string{"root", "left"}, NativeBlocked: map[string][]string{"right": {"root"}, "join": {"left", "right"}, "tail": {"join"}}},
			{Name: "root-closed-again", Action: "close", Role: "root", PlugMethod: "bead.close", NativeReady: []string{"left", "right"}, PlugReady: []string{"left", "right"}, NativeBlocked: map[string][]string{"join": {"left", "right"}, "tail": {"join"}}},
			{Name: "left-closed", Action: "close", Role: "left", PlugMethod: "bead.close", NativeReady: []string{"right"}, PlugReady: []string{"right"}, NativeBlocked: map[string][]string{"join": {"right"}, "tail": {"join"}}, AdoptedDifference: "Option C removes completed workflow item and incident edges; native retains closed native ephemeral row"},
			{Name: "right-closed", Action: "close", Role: "right", PlugMethod: "bead.close", NativeReady: []string{"join"}, PlugReady: []string{"join"}, NativeBlocked: map[string][]string{"tail": {"join"}}},
			{Name: "join-closed", Action: "close", Role: "join", PlugMethod: "bead.close", NativeReady: []string{"tail"}, PlugReady: []string{"tail"}, NativeBlocked: map[string][]string{}, AdoptedDifference: "Option C item removal differs from native retained closed row"},
		},
	}
	chain := b02GraphPlan{
		Name:  "mixed-transitive",
		Nodes: []b02GraphNode{{Role: "root"}, {Role: "middle", Ephemeral: true}, {Role: "tail"}, {Role: "last", Ephemeral: true}},
		Edges: []b02GraphEdge{{"middle", "root", "blocks"}, {"tail", "middle", "blocks"}, {"last", "tail", "blocks"}},
		Steps: []b02GraphStep{
			{Name: "initial", NativeReady: []string{"root"}, PlugReady: []string{"root"}, NativeBlocked: map[string][]string{"middle": {"root"}, "tail": {"middle"}, "last": {"tail"}}},
			{Name: "root-closed", Action: "close", Role: "root", PlugMethod: "bead.close", NativeReady: []string{"middle"}, PlugReady: []string{"middle"}, NativeBlocked: map[string][]string{"tail": {"middle"}, "last": {"tail"}}},
			{Name: "middle-closed", Action: "close", Role: "middle", PlugMethod: "bead.close", NativeReady: []string{"tail"}, PlugReady: []string{"tail"}, NativeBlocked: map[string][]string{"last": {"tail"}}, AdoptedDifference: "Completed workflow item is removed; closed native ephemeral row remains"},
			{Name: "tail-closed", Action: "close", Role: "tail", PlugMethod: "bead.close", NativeReady: []string{"last"}, PlugReady: []string{"last"}, NativeBlocked: map[string][]string{}},
		},
	}
	parent := b02GraphPlan{
		Name:  "parent-fallback",
		Nodes: []b02GraphNode{{Role: "blocker"}, {Role: "parent"}, {Role: "child", Parent: "parent"}, {Role: "grandchild", Parent: "child"}, {Role: "direct", Parent: "parent"}},
		Edges: []b02GraphEdge{{"parent", "blocker", "blocks"}, {"direct", "blocker", "blocks"}},
		Steps: []b02GraphStep{
			{Name: "initial", NativeReady: []string{"blocker"}, PlugReady: []string{"blocker"}, NativeBlocked: map[string][]string{"parent": {"blocker"}, "child": {"parent"}, "grandchild": {"child"}, "direct": {"blocker"}}},
			{Name: "blocker-closed", Action: "close", Role: "blocker", PlugMethod: "bead.close", NativeReady: []string{"parent", "child", "grandchild", "direct"}, PlugReady: []string{"parent", "child", "grandchild", "direct"}, NativeBlocked: map[string][]string{}},
			{Name: "blocker-reopened", Action: "reopen", Role: "blocker", PlugMethod: "bead.reopen", NativeReady: []string{"blocker"}, PlugReady: []string{"blocker"}, NativeBlocked: map[string][]string{"parent": {"blocker"}, "child": {"parent"}, "grandchild": {"child"}, "direct": {"blocker"}}},
			{Name: "direct-unlinked-inherits", Action: "unlink", Role: "direct", Target: "blocker", NativeReady: []string{"blocker"}, PlugReady: []string{"blocker"}, NativeBlocked: map[string][]string{"parent": {"blocker"}, "child": {"parent"}, "grandchild": {"child"}, "direct": {"parent"}}},
			{Name: "parent-unlinked", Action: "unlink", Role: "parent", Target: "blocker", NativeReady: []string{"blocker", "parent", "child", "grandchild", "direct"}, PlugReady: []string{"blocker", "parent", "child", "grandchild", "direct"}, NativeBlocked: map[string][]string{}},
		},
	}
	// Independent durable controls are explicitly named as such. They provide
	// ordinary three-engine graph outcomes, never a replacement for memory's
	// refused ephemeral invocation or positive mixed-plane SQL observations.
	return append([]b02GraphPlan{b02DurablePlan(chain, "durable-transitive"), b02DurablePlan(diamond, "durable-diamond"), chain, diamond, parent}, b02CategoryAndWaitPlans()...)
}

func b02DurablePlan(source b02GraphPlan, name string) b02GraphPlan {
	plan := source
	plan.Name = name
	plan.Nodes = append([]b02GraphNode{}, source.Nodes...)
	for i := range plan.Nodes {
		plan.Nodes[i].Ephemeral = false
	}
	plan.Steps = append([]b02GraphStep{}, source.Steps...)
	for i := range plan.Steps {
		plan.Steps[i].AdoptedDifference = "Separate ordinary durable graph; no memory mixed-plane capability or evidence transfer"
	}
	return plan
}

// These expectations describe the actual public get projection, not the
// complement of ready and not native's returned reason list. Ordinary get's
// blocked field is the explicit stored flag plus active workflow dependency
// facts; it does not enumerate/evaluate ordinary core link reasons. Every
// all-durable control therefore expects false while separately requiring its
// exact adjacency, statuses and independently authored ready membership.
func b02ExpectedPublicBlocked(plan b02GraphPlan, phase string) map[string]bool {
	want := map[string]bool{}
	for _, node := range plan.Nodes {
		want[node.Role] = false
	}
	var blocked []string
	switch plan.Name {
	case "mixed-transitive":
		blocked = map[string][]string{
			"initial":       {"middle", "tail", "last"},
			"root-closed":   {"tail", "last"},
			"middle-closed": {"last"},
			"tail-closed":   {},
		}[phase]
	case "mixed-diamond":
		blocked = map[string][]string{
			"initial":           {"left", "join", "tail"},
			"root-closed":       {"join", "tail"},
			"root-reopened":     {"left", "join", "tail"},
			"left-unlinked":     {"join", "tail"},
			"root-closed-again": {"join", "tail"},
			"left-closed":       {"join", "tail"},
			"right-closed":      {"tail"},
			"join-closed":       {},
		}[phase]
	}
	for _, role := range blocked {
		want[role] = true
	}
	return want
}

func b02CategoryAndWaitPlans() []b02GraphPlan {
	waits := b02GraphPlan{
		Name:  "waits-gates",
		Nodes: []b02GraphNode{{Role: "spawner"}, {Role: "one", Parent: "spawner"}, {Role: "two", Parent: "spawner"}, {Role: "all", Spawner: "spawner", Gate: "all-children"}, {Role: "plain", Spawner: "spawner", Gate: "all-children"}, {Role: "any", Spawner: "spawner", Gate: "any-children"}, {Role: "empty"}, {Role: "emptygate", Spawner: "empty", Gate: "any-children"}},
		Steps: []b02GraphStep{
			{Name: "initial", NativeReady: []string{"spawner", "one", "two", "empty", "emptygate"}, PlugReady: []string{"spawner", "one", "two", "empty", "emptygate"}, NativeBlocked: map[string][]string{"all": {"spawner"}, "plain": {"spawner"}, "any": {"spawner"}}},
			// Raw SQL does not maintain native's materialized is_blocked flag.
			// Set metadata while both children are still open, then normal child
			// closes recompute both waiters. The separate plain waiter must open
			// after the last child; the also-blocking waiter must keep its reason.
			{Name: "also-blocks-stored-metadata", Action: "also", Role: "all", Target: "spawner", NativeReady: []string{"spawner", "one", "two", "empty", "emptygate"}, PlugReady: []string{"spawner", "one", "two", "empty", "emptygate"}, NativeBlocked: map[string][]string{"all": {"spawner"}, "plain": {"spawner"}, "any": {"spawner"}}, AdoptedDifference: "Native SQL changes existing waits-for metadata before child completion: stored evaluator only, no native public writer or automatic raw-SQL flag refresh; normal child-close writes recompute it. Plug uses typed public unlink/link"},
			{Name: "one-closed", Action: "close", Role: "one", PlugMethod: "bead.close", NativeReady: []string{"spawner", "two", "any", "empty", "emptygate"}, PlugReady: []string{"spawner", "two", "any", "empty", "emptygate"}, NativeBlocked: map[string][]string{"all": {"spawner"}, "plain": {"spawner"}}},
			{Name: "two-closed", Action: "close", Role: "two", PlugMethod: "bead.close", NativeReady: []string{"spawner", "plain", "any", "empty", "emptygate"}, PlugReady: []string{"spawner", "plain", "any", "empty", "emptygate"}, NativeBlocked: map[string][]string{"all": {"spawner"}}},
			{Name: "spawner-closed", Action: "close", Role: "spawner", PlugMethod: "bead.close", NativeReady: []string{"all", "plain", "any", "empty", "emptygate"}, PlugReady: []string{"all", "plain", "any", "empty", "emptygate"}, NativeBlocked: map[string][]string{}},
		},
	}
	subjects := b02GraphPlan{
		Name: "subject-membership", Custom: true,
		Nodes: []b02GraphNode{{Role: "blocker"}, {Role: "pinned"}, {Role: "frozen"}, {Role: "closed"}},
		Edges: []b02GraphEdge{{"pinned", "blocker", "blocks"}, {"frozen", "blocker", "blocks"}, {"closed", "blocker", "blocks"}},
		Steps: []b02GraphStep{
			{Name: "initial", NativeReady: []string{"blocker"}, PlugReady: []string{"blocker"}, NativeBlocked: map[string][]string{"pinned": {"blocker"}, "frozen": {"blocker"}, "closed": {"blocker"}}},
			{Name: "pinned-subject", Action: "status", Role: "pinned", Value: "pinned", PlugMethod: "bead.defer", NativeReady: []string{"blocker"}, PlugReady: []string{"blocker"}, NativeBlocked: map[string][]string{"frozen": {"blocker"}, "closed": {"blocker"}}, AdoptedDifference: "Native pinned excluded from blocked; Plug backlog is not ready and satisfies no blocker"},
			{Name: "frozen-subject", Action: "status", Role: "frozen", Value: "b02frozen", PlugMethod: "bead.defer", NativeReady: []string{"blocker"}, PlugReady: []string{"blocker"}, NativeBlocked: map[string][]string{"frozen": {"blocker"}, "closed": {"blocker"}}, AdoptedDifference: "Native configured b02frozen remains a literal blocked subject; Plug uses backlog"},
			{Name: "closed-subject", Action: "force-close", Role: "closed", PlugMethod: "bead.cancel", NativeReady: []string{"blocker"}, PlugReady: []string{"blocker"}, NativeBlocked: map[string][]string{"frozen": {"blocker"}}, AdoptedDifference: "Native forced close and Plug unguarded cancel establish terminal subjects by distinct public operations"},
		},
	}
	outcomes := b02GraphPlan{
		Name: "category-outcomes", Custom: true,
		Nodes: []b02GraphNode{{Role: "blocker"}, {Role: "ordinary"}, {Role: "conditional"}},
		Edges: []b02GraphEdge{{"ordinary", "blocker", "blocks"}, {"conditional", "blocker", "conditional-blocks"}},
		Steps: []b02GraphStep{
			{Name: "initial", NativeReady: []string{"blocker"}, PlugReady: []string{"blocker"}, NativeBlocked: map[string][]string{"ordinary": {"blocker"}, "conditional": {"blocker"}}},
			{Name: "pinned-backlog", Action: "status", Role: "blocker", Value: "pinned", PlugMethod: "bead.defer", NativeReady: []string{"ordinary", "conditional"}, PlugReady: []string{}, NativeBlocked: map[string][]string{}, AdoptedDifference: "A3/CP19/P22: native pinned satisfies both; Plug backlog satisfies neither"},
			{Name: "open-again", Action: "status", Role: "blocker", Value: "open", PlugMethod: "bead.reopen", NativeReady: []string{"blocker"}, PlugReady: []string{"blocker"}, NativeBlocked: map[string][]string{"ordinary": {"blocker"}, "conditional": {"blocker"}}, AdoptedDifference: "Native reopen leaves non-done pinned unchanged; native update --status open and Plug bead.reopen explicitly resume their distinct states"},
			{Name: "custom-active", Action: "status", Role: "blocker", Value: "b02active", NativeReady: []string{"blocker"}, PlugReady: []string{"blocker"}, NativeBlocked: map[string][]string{"ordinary": {"blocker"}, "conditional": {"blocker"}}, AdoptedDifference: "Native active name retained; Plug keeps installed queued category key without mutation"},
			{Name: "custom-done", Action: "status", Role: "blocker", Value: "b02done", PlugMethod: "bead.close", NativeReady: []string{}, PlugReady: []string{"ordinary"}, NativeBlocked: map[string][]string{"ordinary": {"blocker"}, "conditional": {"blocker"}}, AdoptedDifference: "Native literal recompute differs from configured ready view; Plug completed success satisfies ordinary only, with conditionUnmet"},
			{Name: "custom-frozen", Action: "status", Role: "blocker", Value: "b02frozen", PlugMethod: "bead.defer", NativeReady: []string{}, PlugReady: []string{}, NativeBlocked: map[string][]string{"ordinary": {"blocker"}, "conditional": {"blocker"}}, AdoptedDifference: "Native b02frozen stays literal; Plug parked backlog satisfies neither"},
			{Name: "closed-success", Action: "close", Role: "blocker", PlugMethod: "bead.close", NativeReady: []string{"ordinary", "conditional"}, PlugReady: []string{"ordinary"}, NativeBlocked: map[string][]string{}, AdoptedDifference: "A3 success satisfies ordinary only and emits dependency.conditionUnmet"},
			{Name: "reopen-for-failure", Action: "reopen", Role: "blocker", PlugMethod: "bead.reopen", NativeReady: []string{"blocker"}, PlugReady: []string{"blocker"}, NativeBlocked: map[string][]string{"ordinary": {"blocker"}, "conditional": {"blocker"}}},
			{Name: "closed-failure", Action: "close", Role: "blocker", PlugMethod: "bead.fail", NativeReady: []string{"ordinary", "conditional"}, PlugReady: []string{"ordinary", "conditional"}, NativeBlocked: map[string][]string{}, AdoptedDifference: "A3 completed failure satisfies conditional; native has no Plug outcome field"},
			{Name: "reopen-for-cancel", Action: "reopen", Role: "blocker", PlugMethod: "bead.reopen", NativeReady: []string{"blocker"}, PlugReady: []string{"blocker"}, NativeBlocked: map[string][]string{"ordinary": {"blocker"}, "conditional": {"blocker"}}},
			{Name: "closed-canceled", Action: "close", Role: "blocker", PlugMethod: "bead.cancel", NativeReady: []string{"ordinary", "conditional"}, PlugReady: []string{"ordinary"}, NativeBlocked: map[string][]string{}, AdoptedDifference: "A3 canceled satisfies ordinary only and emits conditionUnmet; native retains closed"},
		},
	}
	return []b02GraphPlan{waits, subjects, outcomes}
}

func b02Sorted(values []string) []string {
	result := append([]string{}, values...)
	sort.Strings(result)
	return result
}
func b02Bytes(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func b02Save(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
