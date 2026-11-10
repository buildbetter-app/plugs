package cardb20

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

var b02CatalogRef = model.RefByID("ontology/b02-canonical-public", model.Natural64(0))

func b02Catalog() ontology.Document {
	return ontology.Document{
		Statuses: map[string]ontology.Status{
			"queued":    {Name: "Queued", Category: ontology.Unstarted},
			"working":   {Name: "Working", Category: ontology.Started},
			"finished":  {Name: "Finished", Category: ontology.Completed, Outcome: ontology.Success},
			"broken":    {Name: "Broken", Category: ontology.Completed, Outcome: ontology.Failure},
			"abandoned": {Name: "Abandoned", Category: ontology.Canceled},
			"parked":    {Name: "Parked", Category: ontology.Backlog},
		},
		Workflows: map[string]ontology.Workflow{"issue": {
			Statuses: []string{"queued", "working", "finished", "broken", "abandoned", "parked"}, Initial: "queued",
			Slots:       map[ontology.Slot]string{ontology.Claim: "working", ontology.Close: "finished", ontology.Fail: "broken", ontology.Cancel: "abandoned", ontology.Reopen: "queued", ontology.Defer: "parked"},
			Transitions: []ontology.Rule{{To: "working"}, {To: "finished", Guards: []ontology.Guard{{Kind: ontology.NoOpenBlockers}}}, {To: "broken"}, {To: "abandoned"}, {To: "queued"}, {To: "parked"}},
		}},
		Types: map[string]ontology.Type{"issue": {Name: "Issue", Workflow: "issue", Fields: map[string]ontology.Field{"title": {Type: "string"}}}},
	}
}

type b02Call struct{ Method, Key string }
type b02PublicRuntime struct {
	t                   *testing.T
	engine, output      string
	store               driver.Store
	lease               driver.ActiveLease
	open                func() (driver.Store, error)
	server              *rpc.Server
	requests, allocated int
	calls               []b02Call
	refs                map[string]model.Ref
	statuses            map[string]string
	removed             map[string]bool
	edges               []b02GraphEdge
	also                bool
	catalogRevision     model.Revision
	catalogDigest       string
}

func newB02Public(t *testing.T, engine, output string) *b02PublicRuntime {
	f := openStore(t, engine)
	r := &b02PublicRuntime{t: t, engine: engine, output: output, store: f.store, open: f.open, refs: map[string]model.Ref{}, statuses: map[string]string{}, removed: map[string]bool{}}
	r.bind(true)
	t.Cleanup(func() { r.close() })
	return r
}
func (r *b02PublicRuntime) bind(seed bool) {
	t := r.t
	ctx := context.Background()
	var err error
	r.lease, err = r.store.Lease(ctx, "b02-public", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	if seed {
		body, err := service.MarshalOntologySource(service.OntologySource{Base: b02Catalog()})
		if err != nil {
			t.Fatal(err)
		}
		// Only the declared ontology is fixture-installed. Graph rows, workflow
		// items, links and lifecycle changes all use normal public byte RPC.
		if _, err := conformance.Seed(ctx, r.store, r.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: b02CatalogRef, Kind: "ontology", Op: model.OpCreate, Body: body}}); err != nil {
			t.Fatal(err)
		}
	}
	meta, err := r.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := r.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	rows, readErr := reader.Get(ctx, []model.Ref{b02CatalogRef})
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || len(rows) != 1 {
		t.Fatalf("retained B02 catalog: %v %v", readErr, closeErr)
	}
	source, err := service.DecodeOntologySource(rows[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	compiled, problems := ontology.Compile(source.Base, source.Overlays, source.Plugins)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	if r.catalogDigest != "" && (r.catalogDigest != compiled.Digest() || r.catalogRevision != rows[0].Revision) {
		t.Fatal("SQL reopen changed installed ontology")
	}
	r.catalogDigest, r.catalogRevision = compiled.Digest(), rows[0].Revision
	o := service.NewOntology(compiled.Effective)
	writer := pipeline.New(r.store, r.lease, pipeline.WithOntology(o), pipeline.WithOntologyAudit(service.NewTrackedOntologyAudit(service.StaticOntologySource(b02CatalogRef))), pipeline.WithIDGenerator(func() (string, error) { r.allocated++; return fmt.Sprintf("b02-public-%d", r.allocated), nil }))
	options := []service.Option{service.WithOntology(o)}
	if checkpoints, ok := r.store.(driver.PersistentCheckpointStore); ok && checkpoints.Persistent() {
		provider, err := workflowplugin.New(pluginhost.NewDurableState(checkpoints, r.lease, workflowplugin.Alias, "b02"))
		if err != nil {
			t.Fatal(err)
		}
		provider.WithIdentity(workflowplugin.Alias, "b02", meta.Incarnation)
		options = append(options, service.WithWorkflow(provider))
	}
	domain := service.New(r.store, r.lease, writer, options...)
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		t.Fatal(err)
	}
	r.server, err = rpc.New(registry, rpc.Config{DefaultProject: "b02", Incarnation: meta.Incarnation, OntologyRevision: r.catalogRevision, CursorKey: []byte("b02-canonical-public-cursor-key-01")})
	if err != nil {
		t.Fatal(err)
	}
}
func (r *b02PublicRuntime) close() {
	if r.lease != nil {
		if err := r.lease.Release(context.Background()); err != nil {
			r.t.Error(err)
		}
		r.lease = nil
	}
	if r.store != nil {
		if err := r.store.Close(); err != nil {
			r.t.Error(err)
		}
		r.store = nil
	}
}
func (r *b02PublicRuntime) call(label, method string, params map[string]any, write bool, wantCode string) json.RawMessage {
	t := r.t
	t.Helper()
	if err := r.lease.Renew(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.requests++
	copy := map[string]any{}
	for k, v := range params {
		copy[k] = v
	}
	meta := map[string]any{"project": "b02", "agent": "b02-recorder"}
	if write {
		key := fmt.Sprintf("%064x", r.requests)
		meta["idempotencyKey"] = key
		r.calls = append(r.calls, b02Call{method, key})
	}
	copy["_meta"] = meta
	request := b02Bytes(t, map[string]any{"jsonrpc": "2.0", "id": r.requests, "method": method, "params": copy})
	raw := r.server.Dispatch(rpc.WithPrincipal(context.Background(), model.PrincipalRef{Kind: "user", ID: "b02-public"}), request)
	prefix := filepath.Join(r.output, fmt.Sprintf("%04d-%s", r.requests, label))
	if err := os.WriteFile(prefix+".request.json", request, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(prefix+".response.json", raw, 0600); err != nil {
		t.Fatal(err)
	}
	var envelope response
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	code := ""
	if envelope.Error != nil {
		code = envelope.Error.Data.Code
	}
	if code != wantCode {
		t.Fatalf("%s/%s code=%q want=%q; original response=%s", label, method, code, wantCode, raw)
	}
	return envelope.Result
}

type b02PublicReceipt struct {
	RequestHash     string
	Commit          model.Commit
	ResponsePending bool
	Changeset       *model.Changeset
}
type b02PublicState struct {
	Meta       driver.Meta
	Records    []driver.Record
	Versions   map[string][]driver.Version
	History    []model.Changeset
	Derived    map[driver.DerivedTable][]driver.DerivedRow
	Checkpoint driver.Checkpoint
	Receipts   map[string]b02PublicReceipt
}

func (r *b02PublicRuntime) snapshot() b02PublicState {
	t := r.t
	ctx := context.Background()
	meta, err := r.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := r.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
	}()
	s := b02PublicState{Meta: meta, Versions: map[string][]driver.Version{}, Derived: map[driver.DerivedTable][]driver.DerivedRow{}, Receipts: map[string]b02PublicReceipt{}}
	page, err := reader.Query(ctx, driver.Query{Limit: 1000})
	if err != nil || page.NextCursor != nil {
		t.Fatalf("finite complete public census: %v", err)
	}
	s.Records = page.Items
	sort.Slice(s.Records, func(i, j int) bool { return s.Records[i].Ref.ID < s.Records[j].Ref.ID })
	for _, record := range s.Records {
		versions, err := reader.Versions(ctx, record.Ref, driver.PageReq{Limit: 1000})
		if err != nil || versions.NextCursor != nil {
			t.Fatalf("complete versions: %v", err)
		}
		s.Versions[record.Ref.ID] = versions.Items
	}
	s.History, err = reader.Changesets(ctx, nil, 1000)
	if err != nil || len(s.History) >= 1000 {
		t.Fatalf("finite complete history: %v", err)
	}
	for _, table := range []driver.DerivedTable{driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins} {
		s.Derived[table], err = reader.DerivedRows(ctx, table, "")
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, call := range r.calls {
		receipt, err := reader.IdempotencyByKey(ctx, "user:b02-public", "b02-recorder", call.Method, call.Key)
		if err != nil {
			t.Fatal(err)
		}
		if receipt != nil {
			e := b02PublicReceipt{RequestHash: receipt.RequestHash, Commit: receipt.Commit, ResponsePending: receipt.ResponsePending}
			if !reflect.DeepEqual(receipt.Changeset, model.Changeset{}) {
				v := receipt.Changeset
				e.Changeset = &v
			}
			s.Receipts[call.Method+"/"+call.Key] = e
		}
	}
	if cp, ok := r.store.(driver.CheckpointStore); ok {
		s.Checkpoint, err = cp.LoadCheckpoint(ctx, "plugin/workflow/b02/durable")
		if err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func TestB02PublicBlockingAcrossStores(t *testing.T) {
	// All child processes, including the unchanged owned PostgreSQL fixture,
	// inherit the guarded worker PATH. No process executes on the author host.
	t.Setenv("PATH", "/tmp/plug-swarm/bin/worker-safe:"+os.Getenv("PATH"))
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if root == "" {
		t.Fatal("B02 declared evidence directory required")
	}
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			if engine == "memory" {
				t.Run("ephemeral-capability-refusal", func(t *testing.T) {
					dir := filepath.Join(root, "memory-ephemeral-refusal")
					if err := os.MkdirAll(dir, 0700); err != nil {
						t.Fatal(err)
					}
					r := newB02Public(t, engine, dir)
					before := r.snapshot()
					r.call("unsupported-ephemeral", "x.workflow.invoke", map[string]any{"template": "b02", "project": "b02", "mode": "ephemeral", "checklist": []map[string]any{{"label": "ephemeral", "title": "B02 public ephemeral", "type": "issue"}}}, true, "plugin_durable_state_unavailable")
					after := r.snapshot()
					b02Save(t, filepath.Join(dir, "conservation.json"), map[string]any{"before": before, "after": after, "positive_memory_mixed_plane": "CAPABILITY_ABSENT"})
					if !bytes.Equal(b02Bytes(t, before), b02Bytes(t, after)) {
						t.Fatal("refused ephemeral invocation published state")
					}
					t.Log("B02_MEMORY_EPHEMERAL typed_refusal=plugin_durable_state_unavailable publication=none positive_mixed=CAPABILITY_ABSENT")
				})
			}
			for _, plan := range b02GraphPlans() {
				if engine == "memory" && strings.HasPrefix(plan.Name, "mixed-") {
					continue
				} // Explicit capability control above, never a durable substitute.
				t.Run(plan.Name, func(t *testing.T) {
					b02ReplayPublic(t, engine, plan, filepath.Join(root, "public-"+engine+"-"+plan.Name))
				})
			}
		})
	}
}

func b02AllEdges(plan b02GraphPlan) []b02GraphEdge {
	edges := append([]b02GraphEdge{}, plan.Edges...)
	for _, node := range plan.Nodes {
		if node.Spawner != "" {
			edges = append(edges, b02GraphEdge{node.Role, node.Spawner, "waits-for"})
		}
	}
	return edges
}
func (r *b02PublicRuntime) edgeParams(plan b02GraphPlan, edge b02GraphEdge, also bool) map[string]any {
	params := map[string]any{"from": r.refs[edge.Dependent], "to": r.refs[edge.Blocker], "kind": edge.Kind}
	if edge.Kind == "waits-for" {
		for _, node := range plan.Nodes {
			if node.Role == edge.Dependent {
				params["metadata"] = model.LinkMetadata{Gate: node.Gate, AlsoBlocks: also}
			}
		}
	}
	return params
}
func b02ReplayPublic(t *testing.T, engine string, plan b02GraphPlan, dir string) []map[string]any {
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	r := newB02Public(t, engine, dir)
	b02Save(t, filepath.Join(dir, "independent-plan.json"), plan)
	for _, node := range plan.Nodes {
		if node.Ephemeral {
			before := r.snapshot()
			var invoked workflowplugin.InvokeResult
			if err := json.Unmarshal(r.call("invoke-"+node.Role, "x.workflow.invoke", map[string]any{"template": "b02-" + plan.Name + "-" + node.Role, "project": "b02", "mode": "ephemeral", "checklist": []map[string]any{{"label": node.Role, "title": "B02 " + node.Role, "type": "issue"}}}, true, ""), &invoked); err != nil {
				t.Fatal(err)
			}
			if invoked.Plane != "ephemeral" || len(invoked.Items) != 1 || invoked.Items[0].Plane != "ephemeral" || invoked.Items[0].Kind != "workflow.item" || !strings.HasPrefix(invoked.Items[0].ID, workflowplugin.ItemPrefix) {
				t.Fatal("public invocation did not return a real typed workflow item")
			}
			r.refs[node.Role] = model.RefByID(invoked.Items[0].ID, model.Natural64(0))
			r.statuses[node.Role] = "open"
			after := r.snapshot()
			if !reflect.DeepEqual(before.Records, after.Records) || !reflect.DeepEqual(before.History, after.History) || after.Checkpoint.Generation <= before.Checkpoint.Generation {
				t.Fatal("workflow invocation used durable core rows/history or omitted its checkpoint")
			}
		} else {
			params := map[string]any{"type": "issue", "fields": map[string]any{"title": "B02 " + node.Role, "priority": 1}}
			if node.Parent != "" {
				params["parent"] = r.refs[node.Parent]
			}
			var commit model.Commit
			if err := json.Unmarshal(r.call("create-"+node.Role, "bead.create", params, true, ""), &commit); err != nil || len(commit.Results) != 1 {
				t.Fatalf("durable public create: %v", err)
			}
			r.refs[node.Role] = commit.Results[0].Ref
			r.statuses[node.Role] = "queued"
		}
	}
	r.edges = b02AllEdges(plan)
	for i, edge := range r.edges {
		r.call(fmt.Sprintf("link-%d", i), "bead.link", r.edgeParams(plan, edge, false), true, "")
	}
	var observations []map[string]any
	for _, step := range plan.Steps {
		if !t.Run(step.Name, func(t *testing.T) {
			parent := r.t
			r.t = t
			defer func() { r.t = parent }()
			if step.Action == "unlink" {
				edge := b02GraphEdge{step.Role, step.Target, "blocks"}
				r.call(step.Name, "bead.unlink", r.edgeParams(plan, edge, false), true, "")
				next := []b02GraphEdge{}
				for _, prior := range r.edges {
					if prior != edge {
						next = append(next, prior)
					}
				}
				r.edges = next
			} else if step.Action == "also" {
				edge := b02GraphEdge{step.Role, step.Target, "waits-for"}
				r.call(step.Name+"-unlink", "bead.unlink", r.edgeParams(plan, edge, false), true, "")
				r.call(step.Name+"-link", "bead.link", r.edgeParams(plan, edge, true), true, "")
				r.also = true
			} else if step.PlugMethod != "" {
				var commit model.Commit
				if err := json.Unmarshal(r.call(step.Name, step.PlugMethod, map[string]any{"id": r.refs[step.Role], "reason": "B02 " + step.Name}, true, ""), &commit); err != nil {
					t.Fatal(err)
				}
				workflow := strings.HasPrefix(r.refs[step.Role].ID, workflowplugin.ItemPrefix)
				if workflow && step.PlugMethod == "bead.close" {
					r.removed[step.Role] = true
					next := []b02GraphEdge{}
					for _, edge := range r.edges {
						if edge.Dependent != step.Role && edge.Blocker != step.Role {
							next = append(next, edge)
						}
					}
					r.edges = next
				} else {
					r.statuses[step.Role] = map[string]string{"bead.close": "finished", "bead.fail": "broken", "bead.cancel": "abandoned", "bead.defer": "parked", "bead.reopen": "queued"}[step.PlugMethod]
				}
				if plan.Name == "category-outcomes" && (step.PlugMethod == "bead.close" || step.PlugMethod == "bead.fail" || step.PlugMethod == "bead.cancel") {
					r.assertOutcome(step.Name, step.PlugMethod, commit)
				}
			}
			observations = append(observations, r.observe(plan, step))
		}) {
			t.Fatalf("B02 public %s failed; subsequent phases NOT_RUN", step.Name)
		}
	}
	b02Save(t, filepath.Join(dir, "public-observations.json"), observations)
	return observations
}

func (r *b02PublicRuntime) observe(plan b02GraphPlan, step b02GraphStep) map[string]any {
	t := r.t
	before := r.snapshot()
	read := func(label string) map[string]any {
		gets := map[string]any{}
		blockedExpectation := b02ExpectedPublicBlocked(plan, step.Name)
		for _, node := range plan.Nodes {
			role := node.Role
			if r.removed[role] {
				r.call(label+"-absent-"+role, "bead.get", map[string]any{"id": r.refs[role]}, false, "not_found")
				gets[role] = map[string]any{"code": "not_found", "completed_ephemeral": true}
				continue
			}
			var got struct {
				Bead struct {
					ID, Status, Plane, Kind, Parent, Type string
					Links                                 []workflowplugin.RelationView
					Fields                                map[string]any
					Blocked                               bool
				}
			}
			raw := r.call(label+"-get-"+role, "bead.get", map[string]any{"id": r.refs[role]}, false, "")
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			if got.Bead.ID != r.refs[role].ID || got.Bead.Status != r.statuses[role] || got.Bead.Fields["title"] != "B02 "+role || got.Bead.Type != "issue" {
				t.Fatalf("public %s identity/status/body mismatch: %s", role, raw)
			}
			if node.Ephemeral && (got.Bead.Plane != "ephemeral" || got.Bead.Kind != "workflow.item") {
				t.Fatal("mixed counterpart lost its actual public plane")
			}
			wantBlocked, known := blockedExpectation[role]
			if !known || got.Bead.Blocked != wantBlocked {
				t.Fatalf("literal public blocked field %s/%s/%s=%t want=%t; not a ready complement or returned reason enumeration", plan.Name, step.Name, role, got.Bead.Blocked, wantBlocked)
			}
			wantParent := ""
			if node.Parent != "" {
				wantParent = r.refs[node.Parent].ID
			}
			if got.Bead.Parent != wantParent {
				t.Fatalf("public %s parent=%q want=%q", role, got.Bead.Parent, wantParent)
			}
			actual := []string{}
			for _, edge := range got.Bead.Links {
				if edge.From == r.refs[role].ID && edge.Kind != "parent-child" {
					actual = append(actual, edge.Kind+"/"+edge.To.ID+"/"+string(b02Bytes(t, edge.Metadata)))
				}
			}
			want := []string{}
			for _, edge := range r.edges {
				if edge.Dependent == role {
					params := r.edgeParams(plan, edge, r.also && role == "all")
					var metadata any = params["metadata"]
					if metadata == nil {
						metadata = (*model.LinkMetadata)(nil)
					}
					want = append(want, edge.Kind+"/"+r.refs[edge.Blocker].ID+"/"+string(b02Bytes(t, metadata)))
				}
			}
			if !reflect.DeepEqual(b02Sorted(actual), b02Sorted(want)) {
				t.Fatalf("public %s outgoing adjacency=%v want=%v", role, actual, want)
			}
			gets[role] = json.RawMessage(raw)
		}
		var ready struct {
			Items      []struct{ ID string }
			NextCursor string
		}
		raw := r.call(label+"-ready", "bead.ready", map[string]any{"limit": 1000, "filter": map[string]any{"includeEphemeral": true}}, false, "")
		if err := json.Unmarshal(raw, &ready); err != nil || ready.NextCursor != "" {
			t.Fatalf("finite complete public ready: %v", err)
		}
		roles := []string{}
		seen := map[string]bool{}
		for _, item := range ready.Items {
			role := ""
			for name, ref := range r.refs {
				if ref.ID == item.ID {
					role = name
				}
			}
			if role == "" || seen[role] || r.removed[role] {
				t.Fatal("public ready contains unknown/duplicate/completed identity")
			}
			seen[role] = true
			roles = append(roles, role)
		}
		if !reflect.DeepEqual(b02Sorted(roles), b02Sorted(step.PlugReady)) {
			t.Fatalf("public %s/%s ready=%v want=%v", plan.Name, step.Name, roles, step.PlugReady)
		}
		return map[string]any{"gets": gets, "ready_roles": b02Sorted(roles), "ready_original": json.RawMessage(raw)}
	}
	first := read(step.Name + "-before-reopen")
	if !bytes.Equal(b02Bytes(t, before), b02Bytes(t, r.snapshot())) {
		t.Fatal("public reads changed records/versions/history/catalog/checkpoint/receipts/derived")
	}
	reopened := false
	if r.engine != "memory" {
		r.close()
		var err error
		r.store, err = r.open()
		if err != nil {
			t.Fatal(err)
		}
		r.bind(false)
		reopened = true
		if !bytes.Equal(b02Bytes(t, before), b02Bytes(t, r.snapshot())) {
			t.Fatal("physical SQL reopen changed complete declared durable state")
		}
		second := read(step.Name + "-after-reopen")
		// Signed ready cursors/asOf serialization is a transport observation;
		// compare exact named facts and original graph results on both handles.
		if !bytes.Equal(b02Bytes(t, first["gets"]), b02Bytes(t, second["gets"])) || !reflect.DeepEqual(first["ready_roles"], second["ready_roles"]) {
			t.Fatal("SQL reconstructed provider lost graph/ready facts")
		}
		if !bytes.Equal(b02Bytes(t, before), b02Bytes(t, r.snapshot())) {
			t.Fatal("post-reopen reads published state")
		}
	}
	b02Save(t, filepath.Join(r.output, step.Name+"-state.json"), before)
	result := map[string]any{"plan": plan.Name, "phase": step.Name, "engine": r.engine, "public": first, "sql_physically_reopened": reopened, "adopted_difference": step.AdoptedDifference, "native_returned_reasons": "separate native observation; not synthesized from public ready", "public_blocked_field_expectation": b02ExpectedPublicBlocked(plan, step.Name), "public_blocked_field_limit": "ordinary get projects stored flag plus workflow facts; ordinary core reason enumeration is NOT_EXPOSED"}
	b02Save(t, filepath.Join(r.output, step.Name+"-observed.json"), result)
	t.Logf("B02_PUBLIC_GRAPH engine=%s plan=%s phase=%s ready=%v sql_reopen=%t conserved=true", r.engine, plan.Name, step.Name, first["ready_roles"], reopened)
	return result
}

func (r *b02PublicRuntime) assertOutcome(label, method string, commit model.Commit) {
	t := r.t
	s := r.snapshot()
	var changeset *model.Changeset
	for i := range s.History {
		if s.History[i].Revision == commit.Revision {
			if changeset != nil {
				t.Fatal("duplicate terminal revision")
			}
			changeset = &s.History[i]
		}
	}
	wantType := map[string]model.DurableEventType{"bead.close": model.EventBeadClosed, "bead.fail": model.EventBeadFailed, "bead.cancel": model.EventBeadCanceled}[method]
	wantSlot := map[string]string{"bead.close": "close", "bead.fail": "fail", "bead.cancel": "cancel"}[method]
	wantNotices := 1
	if method == "bead.fail" {
		wantNotices = 0
	}
	if changeset == nil || changeset.Events == nil || len(changeset.Changes) != 1 || len(changeset.Events) != 1+wantNotices || changeset.Changes[0].Ref.ID != r.refs["blocker"].ID || s.Meta.Head != commit.Revision {
		t.Fatal("terminal outcome lacks its explicit exact blocker event plan")
	}
	if err := changeset.ValidateEvents(); err != nil {
		t.Fatal(err)
	}
	ordinary := changeset.Events[0]
	if ordinary.Type != wantType || ordinary.Resource.Ref.ID != r.refs["blocker"].ID || ordinary.Provenance.Method != method || ordinary.Provenance.Slot != wantSlot {
		t.Fatal("terminal lifecycle event changed method/slot/identity")
	}
	var dependentRevision model.Revision
	for _, record := range s.Records {
		if record.Ref.ID == r.refs["conditional"].ID {
			dependentRevision = record.Revision
		}
	}
	notices := 0
	for _, event := range changeset.Events {
		if event.Type != model.EventDependencyConditionUnmet {
			continue
		}
		notices++
		if event.Resource.Ref.ID != r.refs["conditional"].ID || event.Details == nil || event.Details.Dependent.Ref.ID != r.refs["conditional"].ID || event.Details.Dependent.Revision != dependentRevision || event.Details.BlockerBefore.Ref.ID != r.refs["blocker"].ID || event.Details.BlockerBefore.Revision != changeset.Changes[0].FromRevision || event.Details.BlockerAfter.Ref.ID != r.refs["blocker"].ID || event.Details.BlockerAfter.Revision != commit.Revision {
			t.Fatal("conditionUnmet lacks exact dependent and changed blocker revisions")
		}
		pin := event.Details.Ontology
		if pin.Ref.ID != b02CatalogRef.ID || pin.Revision != r.catalogRevision || pin.Digest != r.catalogDigest || pin.Project == nil || pin.Project.ID != "b02" {
			t.Fatal("conditionUnmet is not anchored to installed catalog/project")
		}
	}
	if notices != wantNotices {
		t.Fatalf("terminal %s notices=%d want=%d", label, notices, wantNotices)
	}
	b02Save(t, filepath.Join(r.output, label+"-terminal-event-plan.json"), map[string]any{"commit": commit, "changeset": changeset, "catalog_digest": r.catalogDigest, "notices": notices})
	t.Logf("B02_OUTCOME engine=%s phase=%s method=%s revision=%s conditional_notices=%d", r.engine, label, method, commit.Revision.String(), notices)
}
