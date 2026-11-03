package s1b05b08

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type operationEvidence struct {
	Method string          `json:"method"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

type edgeProjection struct {
	From string `json:"from"`
	To   string `json:"to"`
	Kind string `json:"kind"`
}

type leaseProjection struct {
	ID        string    `json:"id"`
	Holder    string    `json:"holder"`
	Agent     string    `json:"agent"`
	ExpiresAt time.Time `json:"expiresAt"`
	TTLMS     string    `json:"ttlMs"`
}

// semanticProjection contains only declared stable fields. It deliberately
// keeps an explicit archive disposition instead of treating the absence of an
// archive table as an empty successful archive.
type semanticProjection struct {
	Schema       string            `json:"schema"`
	IDs          []string          `json:"ids"`
	Prefixes     []string          `json:"prefixes"`
	Counters     map[string]string `json:"counters"`
	Dependencies []edgeProjection  `json:"dependencies"`
	Leases       []leaseProjection `json:"leases"`
	Blocked      []string          `json:"blocked"`
	Categories   map[string]string `json:"categories"`
	Outcomes     map[string]string `json:"outcomes"`
	Ready        []string          `json:"ready"`
	Versions     map[string]int    `json:"versions"`
	Changesets   int               `json:"changesets"`
	Events       []string          `json:"events"`
	Archives     string            `json:"archives"`
}

type runtimeRecording struct {
	Engine           string              `json:"engine"`
	Operations       []operationEvidence `json:"operations"`
	RawDigest        string              `json:"rawDigest"`
	Projection       semanticProjection  `json:"projection"`
	ProjectionDigest string              `json:"projectionDigest"`
}

func captureCall(t *testing.T, f *fixture, trace *[]operationEvidence, method string, params map[string]any, idempotency string) envelope {
	t.Helper()
	got := f.call(t, f.ctx, method, params, idempotency, actor)
	evidence := operationEvidence{Method: method, Result: append(json.RawMessage(nil), got.Result...)}
	if got.Error != nil {
		evidence.Error = got.Error.Data.Code
	}
	*trace = append(*trace, evidence)
	return got
}

func runStableRecording(t *testing.T, engine string) runtimeRecording {
	t.Helper()
	f := newFixture(t, engine)
	trace := []operationEvidence{}
	hb := f.create(t, "heartbeat")
	claim := captureCall(t, f, &trace, "bead.claim", map[string]any{"id": hb}, key("projection-claim"))
	if claim.Error != nil {
		t.Fatalf("claim refused: %s", claim.Error.Data.Code)
	}
	f.now = f.now.Add(10 * time.Second)
	heartbeat := captureCall(t, f, &trace, "bead.heartbeat", map[string]any{"id": hb}, key("projection-heartbeat"))
	if heartbeat.Error != nil {
		t.Fatalf("heartbeat refused: %s", heartbeat.Error.Data.Code)
	}
	var heartbeatResult struct {
		LeaseExpiresAt time.Time `json:"leaseExpiresAt"`
	}
	if err := json.Unmarshal(heartbeat.Result, &heartbeatResult); err != nil {
		t.Fatal(err)
	}

	depA := f.create(t, "dependency-source")
	depB := f.create(t, "dependency-target")
	if got := captureCall(t, f, &trace, "bead.link", map[string]any{"from": depA, "to": depB, "kind": "custom-edge"}, key("projection-custom")); got.Error != nil {
		t.Fatalf("custom edge refused: %s", got.Error.Data.Code)
	}
	if got := captureCall(t, f, &trace, "bead.link", map[string]any{"from": depA, "to": depB, "kind": "blocks"}, key("projection-retype")); got.Error == nil || got.Error.Data.Code != "ontology_guard_failed" {
		t.Fatalf("valid-kind invariant refusal: %+v", got)
	}

	cycleA := f.create(t, "cycle-a")
	cycleB := f.create(t, "cycle-b")
	if got := captureCall(t, f, &trace, "bead.link", map[string]any{"from": cycleA, "to": cycleB, "kind": "blocks"}, key("projection-cycle-seed")); got.Error != nil {
		t.Fatalf("cycle seed refused: %s", got.Error.Data.Code)
	}
	if got := captureCall(t, f, &trace, "bead.link", map[string]any{"from": cycleB, "to": cycleA, "kind": "blocks"}, key("projection-cycle")); got.Error == nil {
		t.Fatal("cycle was accepted")
	}

	parent := f.create(t, "parent")
	child := f.create(t, "child")
	if got := captureCall(t, f, &trace, "bead.link", map[string]any{"from": child, "to": parent, "kind": "parent-child"}, key("projection-parent-link")); got.Error != nil {
		t.Fatalf("parent-child link refused: %s", got.Error.Data.Code)
	}
	if got := captureCall(t, f, &trace, "bead.close", map[string]any{"id": parent}, key("projection-parent-close")); got.Error == nil {
		t.Fatal("open parent close was accepted")
	}
	if got := captureCall(t, f, &trace, "bead.close", map[string]any{"id": child}, key("projection-child-close")); got.Error != nil {
		t.Fatalf("child close refused: %s", got.Error.Data.Code)
	}
	if got := captureCall(t, f, &trace, "bead.close", map[string]any{"id": parent}, key("projection-parent-close-after")); got.Error != nil {
		t.Fatalf("settled parent close refused: %s", got.Error.Data.Code)
	}
	if got := captureCall(t, f, &trace, "bead.reopen", map[string]any{"id": parent}, key("projection-parent-reopen")); got.Error != nil {
		t.Fatalf("parent reopen refused: %s", got.Error.Data.Code)
	}
	ready := captureCall(t, f, &trace, "bead.ready", map[string]any{}, "")
	if ready.Error != nil {
		t.Fatalf("ready refused: %s", ready.Error.Data.Code)
	}

	ids := []string{hb.ID, depA.ID, depB.ID, cycleA.ID, cycleB.ID, parent.ID, child.ID}
	projection := projectStore(t, f, ids, heartbeatResult.LeaseExpiresAt, trace)
	raw, err := json.Marshal(trace)
	if err != nil {
		t.Fatal(err)
	}
	projectionBytes, err := json.Marshal(projection)
	if err != nil {
		t.Fatal(err)
	}
	rawDigest := sha256.Sum256(raw)
	projectionDigest := sha256.Sum256(projectionBytes)
	return runtimeRecording{
		Engine: engine, Operations: trace, RawDigest: hex.EncodeToString(rawDigest[:]),
		Projection: projection, ProjectionDigest: hex.EncodeToString(projectionDigest[:]),
	}
}

func projectStore(t *testing.T, f *fixture, ids []string, heartbeatExpiry time.Time, trace []operationEvidence) semanticProjection {
	t.Helper()
	refs := make([]model.Ref, len(ids))
	for i, id := range ids {
		refs[i] = model.RefByID(id, model.Natural64(0))
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Get(f.ctx, refs)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != len(ids) {
		t.Fatalf("stored rows=%d want=%d", len(rows), len(ids))
	}
	status := map[string]string{}
	categories := map[string]string{}
	outcomes := map[string]string{}
	versions := map[string]int{}
	dependencies := []edgeProjection{}
	for _, row := range rows {
		doc, err := pipeline.DecodeDocument(row.Body)
		if err != nil {
			t.Fatal(err)
		}
		value, _ := doc.Fields["status"].(string)
		status[doc.ID] = value
		categories[doc.ID], outcomes[doc.ID] = statusCategory(value)
		page, err := r.Versions(f.ctx, row.Ref, driver.PageReq{Limit: 1000})
		if err != nil {
			t.Fatal(err)
		}
		versions[doc.ID] = len(page.Items)
		for _, link := range doc.Links {
			dependencies = append(dependencies, edgeProjection{From: doc.ID, To: link.To.ID, Kind: link.Kind})
		}
	}
	sort.Slice(dependencies, func(i, j int) bool {
		if dependencies[i].From != dependencies[j].From {
			return dependencies[i].From < dependencies[j].From
		}
		if dependencies[i].To != dependencies[j].To {
			return dependencies[i].To < dependencies[j].To
		}
		return dependencies[i].Kind < dependencies[j].Kind
	})
	changesets, err := r.Changesets(f.ctx, nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	events := []string{}
	for _, changeset := range changesets {
		for _, event := range changeset.Events {
			events = append(events, string(event.Type))
		}
	}
	readyPage, err := f.service.Ready(f.ctx, anonymous, service.ReadyRequest{Project: project})
	if err != nil {
		t.Fatal(err)
	}
	readyIDs := make([]string, len(readyPage.Items))
	for i, item := range readyPage.Items {
		readyIDs[i] = item.ID
	}
	blocked := []string{}
	for _, operation := range trace {
		if operation.Error != "" {
			blocked = append(blocked, operation.Method+":"+operation.Error)
		}
	}
	leases := []leaseProjection{{ID: ids[0], Holder: "anonymous:anonymous", Agent: actor, ExpiresAt: heartbeatExpiry, TTLMS: "300000"}}
	return semanticProjection{
		Schema: "plug.s1-b05-b08.semantic.v1", IDs: append([]string(nil), ids...), Prefixes: []string{"s1"},
		Counters:     map[string]string{"topLevel": "explicit", "child": "not-allocated", "ephemeral": "not-allocated"},
		Dependencies: dependencies, Leases: leases, Blocked: blocked, Categories: categories,
		Outcomes: outcomes, Ready: readyIDs, Versions: versions, Changesets: len(changesets), Events: events,
		Archives: "not-produced:disposable-plug-store",
	}
}

func statusCategory(status string) (string, string) {
	switch status {
	case "todo":
		return "unstarted", "none"
	case "in_progress":
		return "started", "none"
	case "done":
		return "completed", "success"
	case "failed":
		return "completed", "failure"
	case "canceled":
		return "canceled", "none"
	case "backlog":
		return "backlog", "none"
	default:
		return "unknown", "unknown"
	}
}

func TestStablePlugProjectionAcrossFreshStores(t *testing.T) {
	first := runStableRecording(t, "memory")
	second := runStableRecording(t, "memory")
	third := runStableRecording(t, "sqlite")
	fourth := runStableRecording(t, "postgres")
	firstBytes, err := json.Marshal(first.Projection)
	if err != nil {
		t.Fatal(err)
	}
	for name, other := range map[string]runtimeRecording{"memory-repeat": second, "sqlite-reopenable": third, "postgres-reopenable": fourth} {
		otherBytes, marshalErr := json.Marshal(other.Projection)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if string(firstBytes) != string(otherBytes) {
			t.Fatalf("semantic projection differs for %s: %s != %s", name, firstBytes, otherBytes)
		}
		if other.ProjectionDigest != first.ProjectionDigest {
			t.Fatalf("projection digest differs for %s: %s != %s", name, other.ProjectionDigest, first.ProjectionDigest)
		}
	}
	if first.RawDigest == "" || second.RawDigest == "" || third.RawDigest == "" || fourth.RawDigest == "" {
		t.Fatal("raw operation digest is missing")
	}
	if first.Projection.Changesets == 0 || len(first.Projection.Versions) != len(first.Projection.IDs) {
		t.Fatalf("history projection is incomplete: %+v", first.Projection)
	}
	t.Logf("stable Plug semantic projection digest=%s raw(memory)=%s raw(sqlite)=%s", first.ProjectionDigest, first.RawDigest, third.RawDigest)
}
