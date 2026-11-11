package cardb20

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

const b06WorkflowNamespace = "plugin/workflow/default/durable"

func b06InstallMixed(f *b06PlugFixture) *workflowplugin.Provider {
	f.t.Helper()
	store, err := driver.RequireWorkflowCheckpointStore(f.store, f.lease)
	if err != nil {
		f.t.Fatal(err)
	}
	state, err := pluginhost.NewWorkflowRuntimeState(store, f.lease)
	if err != nil {
		f.t.Fatal(err)
	}
	provider, err := workflowplugin.New(state)
	if err != nil {
		f.t.Fatal(err)
	}
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		f.t.Fatal(err)
	}
	provider.WithIdentity(workflowplugin.Alias, "default", meta.Incarnation)
	service.WithWorkflow(provider)(f.domain)
	return provider
}

func b06MixedImage(f *b06PlugFixture, label string) []byte {
	f.t.Helper()
	cp, err := f.store.(driver.CheckpointStore).LoadCheckpoint(context.Background(), b06WorkflowNamespace)
	if err != nil {
		f.t.Fatal(err)
	}
	image := map[string]any{"core": json.RawMessage(f.image(label)), "checkpoint": cp}
	b06Retain(f.t, label+"-joint-image.json", image)
	raw, err := json.Marshal(image)
	if err != nil {
		f.t.Fatal(err)
	}
	return raw
}

func b06MixedRefuse(f *b06PlugFixture, edge b06Edge, code string) {
	f.t.Helper()
	before := b06MixedImage(f, fmt.Sprintf("before-mixed-refusal-%d", f.requests+1))
	got := f.link(edge)
	if got.Error == nil || string(got.Error.Data.Code) != code || len(got.Result) != 0 {
		f.t.Fatalf("mixed refusal %+v want %s", got, code)
	}
	if !bytes.Equal(before, b06MixedImage(f, fmt.Sprintf("after-mixed-refusal-%d", f.requests))) {
		f.t.Fatal("refused mixed link changed core/checkpoint/history/receipts")
	}
}

func b06MixedReport(f *b06PlugFixture, tracks bool, limit int) service.CyclesResult {
	f.t.Helper()
	before := b06MixedImage(f, fmt.Sprintf("before-mixed-read-%d", f.requests+1))
	got := b06RequireSuccess[service.CyclesResult](f.t, f.call("bead.cycles", map[string]any{"includeEphemeral": true, "includeTracks": tracks, "limit": limit}, false))
	if !bytes.Equal(before, b06MixedImage(f, fmt.Sprintf("after-mixed-read-%d", f.requests))) {
		f.t.Fatal("mixed cycle read changed core/checkpoint/history/receipts")
	}
	return got
}

func b06InvokeMixed(f *b06PlugFixture, labels ...string) []string {
	f.t.Helper()
	checklist := []map[string]any{}
	for _, label := range labels {
		checklist = append(checklist, map[string]any{"label": label, "title": label})
	}
	got := b06RequireSuccess[workflowplugin.InvokeResult](f.t, f.call("x.workflow.invoke", map[string]any{"template": "b06", "project": "b06", "mode": "ephemeral", "checklist": checklist}, true))
	if len(got.Items) != len(labels) || got.Plane != "ephemeral" {
		f.t.Fatalf("public workflow invocation %+v", got)
	}
	ids := []string{}
	for i, item := range got.Items {
		if item.Label != labels[i] || item.Title != labels[i] || item.Project != "b06" || item.ID == "" {
			f.t.Fatal("public workflow label/order/identity changed")
		}
		ids = append(ids, item.ID)
	}
	return ids
}

// Explicit disposable checkpoint damage supplies parallel adjacency. Ordinary
// same-endpoint links are idempotent; this never claims they accepted duplicates.
func b06MixedParallel(f *b06PlugFixture, target, from string) {
	f.t.Helper()
	store := f.store.(driver.CheckpointStore)
	cp, err := store.LoadCheckpoint(context.Background(), b06WorkflowNamespace)
	if err != nil {
		f.t.Fatal(err)
	}
	var namespace struct {
		Values map[string]struct {
			Revision uint64         `json:"revision"`
			Value    map[string]any `json:"value"`
		} `json:"values"`
	}
	if err := json.Unmarshal(cp.Payload, &namespace); err != nil || len(namespace.Values) != 1 {
		f.t.Fatal("actual workflow checkpoint not readable", err)
	}
	for key, value := range namespace.Values {
		items := value.Value["items"].(map[string]any)
		item := items[target].(map[string]any)
		inbound := item["inbound"].([]any)
		var original map[string]any
		for _, entry := range inbound {
			relation := entry.(map[string]any)
			if relation["from"] == from && relation["kind"] == "blocks" {
				original = relation
			}
		}
		if original == nil {
			f.t.Fatal("publicly accepted raw inbound edge absent")
		}
		inbound = append(inbound, original, map[string]any{"from": from, "to": model.RefByID(target, model.Natural64(0)), "kind": "tracks"})
		item["inbound"] = inbound
		value.Revision++
		namespace.Values[key] = value
	}
	payload, err := json.Marshal(namespace)
	if err != nil {
		f.t.Fatal(err)
	}
	after, err := store.SaveCheckpoint(context.Background(), f.lease, b06WorkflowNamespace, cp.Generation, payload)
	if err != nil || !bytes.Equal(payload, after.Payload) || after.Generation != cp.Generation+1 {
		f.t.Fatal("diagnostic checkpoint readback changed", err)
	}
	b06Retain(f.t, "mixed-parallel-diagnostic.json", map[string]any{"before": cp, "after": after, "route": "explicit damaged public disposable checkpoint; original edge plus two duplicate endpoint facts", "ordinary_admission": false})
}

func b06MixedPolicy(f *b06PlugFixture, provider *workflowplugin.Provider, expected [][]string) {
	f.t.Helper()
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		f.t.Fatal(err)
	}
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: model.RefByID("b06-authority", model.Natural64(0))}
	keys := authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
	app, err := authzrpc.New(context.Background(), authzrpc.Config{Store: f.store, Lease: f.lease, Provider: keys, Namespace: ns, DefaultProject: "b06", Workflow: provider, Facts: authzrpc.StaticFacts{Facts: authzfixed.Facts{Namespace: ns, Projects: []authzfixed.Project{{Ref: model.RefByID("b06-project", model.Natural64(0)), Name: "b06", InstallationID: "fixed", Active: true}}}}})
	if err != nil {
		f.t.Fatal(err)
	}
	by := model.PrincipalRef{Kind: "user", ID: f.by.ID, Caveats: map[string]any{"refs": []model.Ref{model.RefByID("b06-x-a", model.Natural64(0)), model.RefByID("b06-x-b", model.Natural64(0)), model.RefByID("b06-x-c", model.Natural64(0))}}}
	key, err := keys.Issue(by, time.Now().Add(time.Hour))
	if err != nil {
		f.t.Fatal(err)
	}
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"bead.cycles","params":{"includeEphemeral":true,"includeTracks":true,"_meta":{"project":"b06"}}}`)
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1"+httpjson.Path, bytes.NewReader(body))
	request.RemoteAddr = "127.0.0.1:33333"
	request.Header.Set("Plug-RPC", "1")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+key.Token)
	before := b06MixedImage(f, "before-mixed-policy")
	reply := httptest.NewRecorder()
	app.Handler.ServeHTTP(reply, request)
	var envelope b06Envelope
	if err := json.Unmarshal(reply.Body.Bytes(), &envelope); err != nil {
		f.t.Fatal(err)
	}
	got := b06RequireSuccess[service.CyclesResult](f.t, envelope)
	if reply.Code != 200 || !reflect.DeepEqual(b06PlugPaths(got), expected) || got.CycleCount.String() != "2" || got.Truncated {
		f.t.Fatalf("guarded mixed report %+v HTTP%d", got, reply.Code)
	}
	for _, cycle := range got.Items {
		if !cycle.Partial || cycle.UnreadableCount.String() != "1" || !cycle.Path[1].Unreadable || cycle.Path[1].Ref != nil {
			f.t.Fatalf("unreadable workflow member vanished or disclosed its ID: %+v", cycle)
		}
	}
	if !bytes.Equal(before, b06MixedImage(f, "after-mixed-policy")) {
		f.t.Fatal("guarded mixed read changed state")
	}
	b06Retain(f.t, "mixed-policy-original.json", map[string]any{"request": json.RawMessage(body), "response": json.RawMessage(reply.Body.Bytes()), "principal": by, "meaning": "extant workflow members hidden by installed project policy; topology retained, identities withheld"})
}

func b06RunMixedPlug(t *testing.T, engine string) [][]string {
	f := b06OpenPlug(t, engine)
	f.by = model.PrincipalRef{Kind: "user", ID: "user:b06-mixed"}
	provider := b06InstallMixed(f)
	for _, id := range []string{"b06-x-b", "b06-x-a", "b06-x-c", "b06-wait", "b06-schedule", "b06-child", "b06-grandchild"} {
		f.create(id)
	}
	ids := b06InvokeMixed(f, "cycle-z", "pure-u", "pure-v", "wait-z", "cycle-y", "schedule-z", "parent-z")
	edges := []b06Edge{
		{"b06-x-a", ids[0], "blocks"}, {ids[0], "b06-x-b", "tracks"}, {"b06-x-b", "b06-x-a", "tracks"},
		{ids[1], ids[2], "tracks"}, {ids[2], ids[1], "tracks"},
		{"b06-wait", ids[3], "waits-for"}, {ids[3], "b06-wait", "blocks"},
		{"b06-x-c", ids[4], "blocks"}, {ids[4], "b06-x-c", "tracks"},
		{"b06-schedule", ids[5], "blocks"},
		{"b06-child", ids[6], "parent-child"}, {"b06-grandchild", "b06-child", "parent-child"},
	}
	for _, edge := range edges {
		b06RequireSuccess[model.Commit](t, f.link(edge))
	}
	b06MixedRefuse(f, b06Edge{ids[0], ids[0], "tracks"}, "invalid_params")
	b06MixedRefuse(f, b06Edge{ids[5], "b06-schedule", "blocks"}, "ontology_guard_failed")
	b06MixedRefuse(f, b06Edge{ids[6], "b06-grandchild", "parent-child"}, "ontology_guard_failed")
	b06MixedRefuse(f, b06Edge{"b06-grandchild", ids[6], "blocks"}, "ontology_guard_failed")
	b06MixedRefuse(f, b06Edge{ids[6], "b06-grandchild", "blocks"}, "ontology_guard_failed")
	want := [][]string{{"b06-x-a", ids[0], "b06-x-b"}, {"b06-x-c", ids[4]}}
	check := func() {
		b06AssertReport(t, b06MixedReport(f, false, 100), [][]string{})
		b06AssertReport(t, b06MixedReport(f, true, 100), want)
		limited := b06MixedReport(f, true, 1)
		if !reflect.DeepEqual(b06PlugPaths(limited), want[:1]) || limited.CycleCount.String() != "2" || !limited.Truncated || limited.Items[0].MemberCount.String() != "3" {
			t.Fatalf("mixed raw order/count/truncation %+v", limited)
		}
		// A durable-only selector must retain its original graph scope.
		b06AssertReport(t, f.report(true), [][]string{})
	}
	check()
	b06MixedParallel(f, ids[0], "b06-x-a")
	check()
	before := b06MixedImage(f, "mixed-before-reopen")
	f.reopen()
	provider = b06InstallMixed(f)
	if !bytes.Equal(before, b06MixedImage(f, "mixed-after-reopen")) {
		t.Fatal("mixed SQL reopen changed committed values")
	}
	check()
	b06MixedPolicy(f, provider, [][]string{{"b06-x-a", "", "b06-x-b"}, {"b06-x-c", ""}})
	b06Retain(t, "mixed-public-summary.json", map[string]any{"actual_ids_in_declaration_order": ids, "accepted_edges": edges, "raw_ordered_paths": want, "engines": engine, "sql_reopen": engine != "memory", "memory_lifetime": "same_store", "private": "HARD_DISABLED", "official": "UNSPENT"})
	t.Logf("B06_MIXED engine=%s both_endpoint_directions=true blocked_members=retained lowest_id=true parallel=collapsed unreadable=honest pure_tracks=excluded waits_for=excluded self_cycle_ancestor=refused no_loss=true", engine)
	return want
}
