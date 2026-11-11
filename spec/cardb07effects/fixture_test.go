// These cases extend the retained B07 vocabulary and three mixed positives.
// Expectations are declared here, independently of native and product code.
package cardb20

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	proof "github.com/buildbetter-app/plugs/internal/integration/b07positive"
	publicproof "github.com/buildbetter-app/plugs/internal/integration/b07positive/public"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

type effectCase struct {
	Name, Kind, Route, Settlement string
}

func effectCases() []effectCase {
	// Conditional outcome and waits-for predicates retain their already named
	// B02/B14 owners. Every fixed inert spelling is explicit, not read from a
	// production classifier. The earlier B07 vocabulary tests remain intact.
	var cases []effectCase
	for _, route := range []string{"durable", "durable-active", "active-durable", "active-active"} {
		for _, kind := range []string{"blocks", "related", "custom kind"} {
			for _, settlement := range []string{"target-close", "unlink"} {
				cases = append(cases, effectCase{route + "/" + kind + "/" + settlement, kind, route, settlement})
			}
		}
	}
	return cases
}

type effectStep struct {
	Case, Phase string
	Ready       []string
	GuardRefused bool
}

type effectView struct {
	Bead struct {
		ID, Status, Parent string
		Blocked            bool
		Links              []struct {
			From string
			To model.Ref
			Kind string
			Metadata *model.LinkMetadata
		}
	}
}

func effectRead(t *testing.T, h *publicproof.Host, ref model.Ref) effectView {
	t.Helper()
	r := h.Call("bead.get", "get-"+ref.ID, map[string]any{"id": ref}, "")
	var got effectView
	if err := json.Unmarshal(r.Result, &got); err != nil || got.Bead.ID != ref.ID {
		t.Fatalf("exact public endpoint readback: %s (%v)", r.Result, err)
	}
	return got
}

func effectReady(t *testing.T, h *publicproof.Host, roles map[string]string) []string {
	t.Helper()
	r := h.Call("bead.ready", "ready", map[string]any{"filter":map[string]any{"includeEphemeral":true}, "limit": 128}, "")
	var page struct {
		Items []struct{ ID string }
		NextCursor string
	}
	if err := json.Unmarshal(r.Result, &page); err != nil || page.NextCursor != "" {
		t.Fatal("complete public ready page unavailable", err)
	}
	ids := []string{}
	seen := map[string]bool{}
	for _, row := range page.Items {
		role, known := roles[row.ID]
		if !known || seen[row.ID] {
			t.Fatal("unknown or duplicate ready identity", row.ID)
		}
		seen[row.ID] = true
		ids = append(ids, role)
	}
	sort.Strings(ids)
	return ids
}

func effectWithout(ids []string, roles ...string) []string {
	next := []string{}
	for _, id := range ids {
		remove := false
		for _, role := range roles { remove = remove || id == role }
		if !remove { next = append(next, id) }
	}
	return next
}

func effectAssertStep(t *testing.T, h *publicproof.Host, roles map[string]string, c effectCase, phase string, source model.Ref, want []string, guarded bool) effectStep {
	t.Helper()
	got := effectReady(t, h, roles)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s %s: ready=%v want=%v", c.Name, phase, got, want)
	}
	if guarded {
		before := h.Capture()
		h.Call("bead.close", c.Name+"-"+phase+"-guard", map[string]any{"id": source}, "ontology_guard_failed")
		proof.Same(t, before, h.Capture())
	}
	return effectStep{c.Name, phase, got, guarded}
}

func effectLink(t *testing.T, h *publicproof.Host, source, target model.Ref, kind string, metadata *model.LinkMetadata) {
	t.Helper()
	view := effectRead(t, h, source)
	if len(view.Bead.Links) != 1 || view.Bead.Links[0].From != source.ID || view.Bead.Links[0].To.ID != target.ID || view.Bead.Links[0].To.Branch.String() != "0" || view.Bead.Links[0].Kind != kind || !reflect.DeepEqual(view.Bead.Links[0].Metadata, metadata) {
		t.Fatalf("exact public directed edge changed: %+v", view.Bead.Links)
	}
}

func effectPublic(t *testing.T, engine string) []effectStep {
	h := publicproof.NewHost(t, engine)
	effectLifetime(t,h,engine)
	roles := map[string]string{}
	expected := []string{}
	var steps []effectStep
	for index, c := range effectCases() {
		completed := false
		h.Run(c.Name, func(t *testing.T) {
			label := fmt.Sprintf("case-%02d", index)
			var source, target model.Ref
			if c.Route == "durable" || c.Route == "durable-active" { source = h.Create(label+"-source") }
			if c.Route == "durable" || c.Route == "active-durable" { target = h.Create(label+"-target") }
			if c.Route != "durable" {
				// A third open item keeps the actual run alive after either edge
				// endpoint closes, so completion cannot erase the tested edge.
				checklist := []any{map[string]any{"label": "sentinel", "title": label+"-sentinel"}}
				if source.ID == "" { checklist = append(checklist, map[string]any{"label": "source", "title": label+"-source"}) }
				if target.ID == "" { checklist = append(checklist, map[string]any{"label": "target", "title": label+"-target"}) }
				r := h.Call("x.workflow.invoke", label+"-invoke", map[string]any{"template": "b07-effects", "project": "b07positive", "mode": "ephemeral", "checklist": checklist}, "")
				var invocation workflowplugin.InvokeResult
				if json.Unmarshal(r.Result, &invocation) != nil || len(invocation.Items) != len(checklist) { t.Fatal("actual workflow endpoints missing") }
				for _, item := range invocation.Items {
					if item.Plane != "ephemeral" || item.Project != "b07positive" || item.RunID == "" { t.Fatal("workflow plane identity lost") }
					ref := model.RefByID(item.ID, model.Natural64(0))
					switch item.Label {
					case "source": source = ref
					case "target": target = ref
					case "sentinel": roles[item.ID] = label+"-sentinel"
						expected = append(expected, label+"-sentinel")
					default: t.Fatal("unexpected workflow role")
					}
				}
			}
			roles[source.ID], roles[target.ID] = label+"-source", label+"-target"
			expected = append(expected, label+"-source", label+"-target")
			sort.Strings(expected)
			baseline := append([]string{}, expected...)
			steps = append(steps, effectAssertStep(t, h, roles, c, "created", source, baseline, false))
			metadata := &model.LinkMetadata{NativeExtensions: map[string]string{"beads.metadata": `"{\"label\":\"opaque original\"}"`, "beads.thread_id": `"original thread"`}}
			params := map[string]any{"from": source, "to": target, "kind": c.Kind, "metadata": metadata}
			effectCall(t, h, "bead.link", label+"-link", params, []model.Ref{source}, effectActive(source, target))
			effectLink(t, h, source, target, c.Kind, metadata)
			blocked := c.Kind == "blocks"
			want := baseline
			if blocked { want = effectWithout(want, label+"-source") }
			steps = append(steps, effectAssertStep(t, h, roles, c, "linked", source, want, blocked))
			// The first unlink/relink is while both original endpoints are open.
			// This is a new effect sequence, not another initial-add vocabulary gate.
			effectCall(t, h, "bead.unlink", label+"-unlink-open", params, []model.Ref{source}, effectActive(source, target))
			steps = append(steps, effectAssertStep(t, h, roles, c, "unlinked-open", source, baseline, false))
			if len(effectRead(t, h, source).Bead.Links) != 0 { t.Fatal("unlink retained endpoint relation") }
			effectCall(t, h, "bead.link", label+"-relink-open", params, []model.Ref{source}, effectActive(source, target))
			effectLink(t, h, source, target, c.Kind, metadata)
			steps = append(steps, effectAssertStep(t, h, roles, c, "relinked-open", source, want, blocked))
			if c.Settlement == "unlink" {
				effectCall(t, h, "bead.unlink", label+"-unlink-final", params, []model.Ref{source}, effectActive(source, target))
				steps = append(steps, effectAssertStep(t, h, roles, c, "unlinked-final", source, baseline, false))
				effectCall(t, h, "bead.close", label+"-close-source-unlinked", map[string]any{"id": source}, []model.Ref{source}, effectActive(source))
				expected = effectWithout(baseline, label+"-source")
				steps = append(steps, effectAssertStep(t, h, roles, c, "source-closed-unlinked", source, expected, false))
				effectReopen(t, h)
				completed = true
				return
			}
			effectCall(t, h, "bead.close", label+"-close-target", map[string]any{"id": target}, []model.Ref{target}, effectActive(target))
			want = effectWithout(baseline, label+"-target")
			steps = append(steps, effectAssertStep(t, h, roles, c, "target-closed", source, want, false))
			effectLink(t, h, source, target, c.Kind, metadata)
			closed := effectRead(t, h, target)
			status := "done"
			if strings.HasPrefix(target.ID, workflowplugin.ItemPrefix) { status = "closed" }
			if closed.Bead.Status != status { t.Fatalf("exact close status %q want %q", closed.Bead.Status, status) }
			effectReopen(t, h)
			steps = append(steps, effectAssertStep(t, h, roles, c, "store-readback", source, want, false))
			if !strings.HasPrefix(target.ID, workflowplugin.ItemPrefix) {
				effectCall(t, h, "bead.reopen", label+"-reopen-target", map[string]any{"id": target}, []model.Ref{target}, nil)
				want = baseline
				if blocked { want = effectWithout(want, label+"-source") }
				steps = append(steps, effectAssertStep(t, h, roles, c, "target-reopened", source, want, blocked))
				if effectRead(t, h, target).Bead.Status != "todo" { t.Fatal("reopen did not select the exact installed todo slot") }
				effectLink(t, h, source, target, c.Kind, metadata)
				effectCall(t, h, "bead.close", label+"-reclose-target", map[string]any{"id": target}, []model.Ref{target}, nil)
				want = effectWithout(baseline, label+"-target")
				steps = append(steps, effectAssertStep(t, h, roles, c, "target-reclosed", source, want, false))
			} else {
				// Reopen is not among the frozen ordinary workflow operations.
				// Keep this real refusal separate; it earns no positive reopen credit.
				before := h.Capture()
				h.Call("bead.reopen", label+"-unsupported-workflow-reopen", map[string]any{"id": target}, "invalid_params")
				proof.Same(t, before, h.Capture())
			}
			// This close still has the original relation and now succeeds because
			// that exact target has closed. The unlink branch separately proves
			// successful source close while its original target remains open.
			effectCall(t, h, "bead.close", label+"-close-source", map[string]any{"id": source}, []model.Ref{source}, effectActive(source))
			want = effectWithout(want, label+"-source")
			steps = append(steps, effectAssertStep(t, h, roles, c, "source-closed", source, want, false))
			effectLink(t, h, source, target, c.Kind, metadata)
			effectReopen(t, h)
			if !reflect.DeepEqual(effectReady(t, h, roles), want) { t.Fatal("final SQL reopen changed ready membership") }
			expected = want
			completed = true
		})
		if !completed { t.Fatal("effect sequence incomplete", c.Name) }
	}
	proof.Save(t, h.Root, "effect-sequences.json", steps)
	return steps
}
