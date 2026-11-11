package cardb20

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	proof "github.com/buildbetter-app/plugs/internal/integration/b07positive"
	publicproof "github.com/buildbetter-app/plugs/internal/integration/b07positive/public"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// These roles come from actual invocation results, never invented workflow IDs.
func effectParentEndpoints(t *testing.T, h *publicproof.Host, route string, roles map[string]string) (model.Ref, model.Ref, []string) {
	t.Helper()
	var child, parent model.Ref
	want := []string{"blocker", "child", "parent"}
	if route == "durable" || route == "durable-active" { child = h.Create("child") }
	if route == "durable" || route == "active-durable" { parent = h.Create("parent") }
	if route != "durable" {
		checklist := []any{map[string]any{"label":"sentinel", "title":"parent scheduling sentinel"}}
		if child.ID == "" { checklist = append(checklist, map[string]any{"label":"child", "title":"actual child"}) }
		if parent.ID == "" { checklist = append(checklist, map[string]any{"label":"parent", "title":"actual parent"}) }
		r := h.Call("x.workflow.invoke", "invoke", map[string]any{"template":"b07-parent-effects", "project":"b07positive", "mode":"ephemeral", "checklist":checklist}, "")
		var invocation workflowplugin.InvokeResult
		if json.Unmarshal(r.Result,&invocation) != nil || len(invocation.Items) != len(checklist) { t.Fatal("real parent/child endpoints missing") }
		for _, item := range invocation.Items {
			if item.Plane != "ephemeral" || item.RunID == "" || item.Project != "b07positive" { t.Fatal("provider identity changed") }
			ref := model.RefByID(item.ID,model.Natural64(0))
			switch item.Label {
			case "child": child = ref
			case "parent": parent = ref
			case "sentinel": roles[item.ID] = "sentinel"; want = append(want,"sentinel")
			default: t.Fatal("unexpected invocation role")
			}
		}
	}
	roles[child.ID], roles[parent.ID] = "child", "parent"
	sort.Strings(want)
	return child,parent,want
}

func effectParentPublic(t *testing.T, engine, route string) []effectStep {
	h := publicproof.NewHost(t,engine)
	effectLifetime(t,h,engine)
	roles := map[string]string{}
	child,parent,baseline := effectParentEndpoints(t,h,route,roles)
	blocker := h.Create("blocker")
	roles[blocker.ID] = "blocker"
	c := effectCase{Name:"parent-child/"+route,Kind:"parent-child",Route:route}
	metadata := &model.LinkMetadata{NativeExtensions:map[string]string{"beads.thread_id":`"parent original thread"`}}
	parentEdge := map[string]any{"from":child,"to":parent,"kind":"parent-child","metadata":metadata}
	blockingEdge := map[string]any{"from":parent,"to":blocker,"kind":"blocks","metadata":metadata}
	steps := []effectStep{effectAssertStep(t,h,roles,c,"created",child,baseline,false)}
	effectCall(t,h,"bead.link","child-parent",parentEdge,[]model.Ref{child},effectActive(child,parent))
	effectLink(t,h,child,parent,"parent-child",metadata)
	steps = append(steps,effectAssertStep(t,h,roles,c,"open-parent",child,baseline,false))
	effectCall(t,h,"bead.link","parent-blocker",blockingEdge,[]model.Ref{parent},effectActive(parent,blocker))
	want := effectWithout(baseline,"child","parent")
	steps = append(steps,effectAssertStep(t,h,roles,c,"blocked-parent",child,want,false))
	effectCall(t,h,"bead.close","close-blocker",map[string]any{"id":blocker},[]model.Ref{blocker},nil)
	want = effectWithout(baseline,"blocker")
	steps = append(steps,effectAssertStep(t,h,roles,c,"parent-unblocked-by-close",child,want,false))
	effectCall(t,h,"bead.reopen","reopen-blocker",map[string]any{"id":blocker},[]model.Ref{blocker},nil)
	want = effectWithout(baseline,"child","parent")
	steps = append(steps,effectAssertStep(t,h,roles,c,"parent-reblocked",child,want,false))
	effectCall(t,h,"bead.unlink","unlink-parent-blocker",blockingEdge,[]model.Ref{parent},effectActive(parent,blocker))
	steps = append(steps,effectAssertStep(t,h,roles,c,"parent-unblocked-by-unlink",child,baseline,false))
	if len(effectActive(parent)) == 0 {
		effectCall(t,h,"bead.defer","defer-parent",map[string]any{"id":parent},[]model.Ref{parent},nil)
		want = effectWithout(baseline,"child","parent")
		steps = append(steps,effectAssertStep(t,h,roles,c,"deferred-parent",child,want,false))
	} else {
		effectCall(t,h,"bead.link","relink-parent-blocker",blockingEdge,[]model.Ref{parent},effectActive(parent,blocker))
		want = effectWithout(baseline,"child","parent")
		steps = append(steps,effectAssertStep(t,h,roles,c,"parent-reblocked-by-relink",child,want,false))
	}
	// Parent scheduling suppresses ready but never guards closing this child.
	effectCall(t,h,"bead.close","close-child",map[string]any{"id":child},[]model.Ref{child},effectActive(child))
	steps = append(steps,effectAssertStep(t,h,roles,c,"child-closed-with-ineligible-parent",child,want,false))
	effectLink(t,h,child,parent,"parent-child",metadata)
	effectReopen(t,h)
	if !reflect.DeepEqual(effectReady(t,h,roles),want) { t.Fatal("parent effects changed on SQL reopen") }
	proof.Save(t,h.Root,"parent-effect-sequences.json",steps)
	return steps
}

func effectExternalPublic(t *testing.T, engine string) {
	for _, targetCase := range []string{"explicit-capability","cross-prefix","terminal-local-alias"} {
		for _, kind := range []string{"blocks","parent-child","custom kind"} {
			t.Run(targetCase+"/"+kind,func(t *testing.T) {
				h := publicproof.NewHost(t,engine)
				source, unrelated := h.Create("source"),h.Create("unrelated")
				roles := map[string]string{source.ID:"source",unrelated.ID:"unrelated"}
				target := model.RefByID("external:unconfigured:ticket",model.Natural64(0))
				if targetCase == "cross-prefix" { target = model.RefByID("remote-1",model.Natural64(0)) }
				if targetCase == "terminal-local-alias" {
					target = h.Create("local-alias")
					h.Call("bead.close","close-alias",map[string]any{"id":target},"")
					roles[target.ID] = "closed-alias"
				}
				baseline := []string{"source","unrelated"}
				c := effectCase{Name:targetCase+"/"+kind,Kind:kind}
				effectAssertStep(t,h,roles,c,"created",source,baseline,false)
				before := h.Capture()
				// Missing local remains local, including a cross-prefix spelling.
				h.Call("bead.link","missing-local",map[string]any{"from":source,"to":model.RefByID("remote-unresolved",model.Natural64(0)),"kind":kind},"not_found")
				proof.Same(t,before,h.Capture())
				metadata := &model.LinkMetadata{NativeExtensions:map[string]string{"beads.metadata":`" {\"external\":true} "`,"beads.thread_id":`"external thread"`}}
				params := map[string]any{"from":source,"to":target,"targetKind":"external","kind":kind,"metadata":metadata}
				effectCall(t,h,"bead.link","external-link",params,[]model.Ref{source},nil)
				effectLink(t,h,source,target,kind,metadata)
				image := h.Capture()
				doc,err := pipeline.DecodeDocument(image.Row(t,source.ID).Body)
				if err != nil || doc.Parent != nil || len(doc.Links) != 1 || doc.Links[0].TargetKind != model.LinkTargetExternal || doc.Links[0].To.ID != target.ID { t.Fatal("external arm was resolved locally or entered hierarchy",err) }
				want := baseline
				if kind == "blocks" || kind == "parent-child" { want = []string{"unrelated"} }
				effectAssertStep(t,h,roles,c,"external-linked",source,want,kind=="blocks")
				effectReopen(t,h)
				effectLink(t,h,source,target,kind,metadata)
				effectAssertStep(t,h,roles,c,"external-reopened",source,want,kind=="blocks")
				effectCall(t,h,"bead.unlink","external-unlink",params,[]model.Ref{source},nil)
				effectAssertStep(t,h,roles,c,"external-unlinked",source,baseline,false)
				effectCall(t,h,"bead.link","external-relink",params,[]model.Ref{source},nil)
				effectAssertStep(t,h,roles,c,"external-relinked",source,want,kind=="blocks")
				if kind == "blocks" { effectCall(t,h,"bead.unlink","external-remove-for-close",params,[]model.Ref{source},nil) }
				effectCall(t,h,"bead.close","external-source-close",map[string]any{"id":source},[]model.Ref{source},nil)
				effectAssertStep(t,h,roles,c,"source-closed",source,[]string{"unrelated"},false)
				effectReopen(t,h)
				proof.Save(t,h.Root,"external-contract.json",map[string]any{"targetCase":targetCase,"target":target,"targetKind":"external","kind":kind,"prefixInference":false,"readiness":"explicit external blockers and parents stay unknown; custom kind stays inert","nativeParity":"explicit capability policy only; bare-prefix native policy recorded separately"})
			})
		}
	}
}
