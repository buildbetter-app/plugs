package s1b01b04

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func (r *b02Replay) observe(capture b02Capture, label string, want []string, deviation string) {
	t := r.f.t
	observed, exists := capture.Phases[label]
	if !exists || len(observed.Nodes) != len(capture.Plan.Nodes) {
		t.Fatal("public replay lacks its actual native phase")
	}
	r.renew()
	before, epoch := r.snapshot(), r.f.lease.Epoch()
	nativeJSON(t, filepath.Join(r.output, label+"-reads-before.json"), before)
	for index, node := range capture.Plan.Nodes {
		value := r.read(label+"-get-"+node.Role, "bead.get", map[string]any{"id": r.refs[node.Role]})
		bead, ok := value["bead"].(map[string]any)
		fields, fieldsOK := bead["fields"].(map[string]any)
		priority, priorityOK := fields["priority"].(json.Number)
		parent := ""
		if node.Parent != "" {
			parent = r.refs[node.Parent].ID
		}
		actualParent, _ := bead["parent"].(string)
		if !ok || !fieldsOK || !priorityOK || bead["id"] != r.refs[node.Role].ID || fields["title"] != observed.Nodes[node.Role].Title || priority.String() != strconv.Itoa(index%5) || bead["status"] != r.statuses[node.Role] || actualParent != parent {
			t.Fatalf("public identity/title/priority/status/parent differs for %s", node.Role)
		}
	}
	full := r.read(label+"-ready-full", "bead.ready", map[string]any{"limit": 1000})
	first := r.read(label+"-ready-first", "bead.ready", map[string]any{"limit": 1})
	if !bytes.Equal(b02Bytes(t, full["asOf"]), b02Bytes(t, first["asOf"])) {
		t.Fatal("public full and first pages disagree on snapshot")
	}
	var binding struct {
		Revision    model.Revision
		Incarnation string
	}
	nativeDecode(t, b02Bytes(t, full["asOf"]), &binding)
	if binding.Revision != before.Meta.Head || binding.Incarnation != before.Meta.Incarnation {
		t.Fatal("public ready snapshot is not the actual head/incarnation")
	}
	if next, _ := full["nextCursor"].(string); next != "" {
		t.Fatal("small full page unexpectedly truncated")
	}
	all, drained := r.readyRoles(full), r.readyRoles(first)
	if len(all) > 1 {
		if cursor, _ := first["nextCursor"].(string); cursor == "" {
			t.Fatal("multirow case lacks a real signed first-page cursor")
		}
	}
	if r.f.backend != "memory" {
		r.reopen(label)
		epoch = r.f.lease.Epoch()
	}
	page := first
	cursors := map[string]bool{}
	for index := 0; ; index++ {
		cursor, _ := page["nextCursor"].(string)
		if cursor == "" {
			break
		}
		if cursors[cursor] || index > len(capture.Plan.Nodes) {
			t.Fatal("public continuation repeated or exceeded the finite source graph")
		}
		cursors[cursor] = true
		// Omit the unfiltered selector entirely; filter:null is invalid wire.
		page = r.read(fmt.Sprintf("%s-ready-next-%d", label, index), "bead.ready", map[string]any{"limit": 1, "cursor": cursor})
		if !bytes.Equal(b02Bytes(t, first["asOf"]), b02Bytes(t, page["asOf"])) {
			t.Fatal("signed continuation changed its pinned snapshot across reopen")
		}
		drained = append(drained, r.readyRoles(page)...)
	}
	if !reflect.DeepEqual(all, drained) || !reflect.DeepEqual(b02Sorted(all), b02Sorted(want)) {
		t.Fatalf("public full/one-row/expected membership differs: %v / %v / %v", all, drained, want)
	}
	after := r.snapshot()
	nativeJSON(t, filepath.Join(r.output, label+"-reads-after.json"), after)
	if !bytes.Equal(b02Bytes(t, before), b02Bytes(t, after)) || epoch != r.f.lease.Epoch() {
		t.Fatal("public reads changed source/history/versions/receipts/derived/facts/authority")
	}
	select {
	case <-r.f.lease.Lost():
		t.Fatal("public reads lost active authority")
	default:
	}
	parity := any(true)
	if deviation != "" {
		parity = nil
	} else if !reflect.DeepEqual(b02Sorted(all), observed.Ready) {
		t.Fatal("public membership differs from actual native observation without a declared deviation")
	}
	nativeJSON(t, filepath.Join(r.output, label+"-comparison.json"), map[string]any{
		"actualNative": observed, "actualPlugReadyRoles": all, "membershipParity": parity, "deviation": deviation,
		"categoryMapping": map[string]string{"open": "queued", "closed-success": "finished", "closed-failure": "broken", "closed-canceled": "abandoned", "pinned": "parked", "b02active": "queued", "b02done": "finished", "b02frozen": "parked"},
		"sourceIdentity":  capture.Plan.Name + "/" + capture.Run, "scope": "core durable public operations only; no native clock, raw status equality, ephemeral substitute or full-card qualification",
	})
}

func b02ReplayCapture(t *testing.T, capture b02Capture, engine, dsn string) {
	output := filepath.Join(capture.Output, "replay-"+engine)
	if err := os.Mkdir(output, 0o700); err != nil {
		t.Fatal(err)
	}
	ids := []string{}
	for _, node := range capture.Plan.Nodes {
		if node.Ephemeral {
			t.Fatal("durable replay must not silently replace a native ephemeral row")
		}
		ids = append(ids, "plug-"+b02ID(capture.Plan, node.Role))
	}
	f := newFixture(t, engine, dsn, compileOntology(t, customDocument(), nil), ids)
	r := &b02Replay{f: f, output: output, refs: map[string]model.Ref{}, statuses: map[string]string{}}
	nativeJSON(t, filepath.Join(output, "authored-plug-ontology.json"), customDocument())
	bindings := map[string]any{}
	for index, node := range capture.Plan.Nodes {
		observed := capture.Phases["initial"].Nodes[node.Role]
		commit := r.write("create-"+node.Role, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": observed.Title, "priority": index % 5}}, ids[index])
		if len(commit.Results) != 1 || commit.Results[0].Ref.ID != ids[index] || commit.Results[0].Ref.ID == observed.ID || !commit.Results[0].Ref.Branch.IsZero() {
			t.Fatal("public create did not return its separately bound actual Plug ref")
		}
		r.refs[node.Role], r.statuses[node.Role] = commit.Results[0].Ref, "queued"
		bindings[node.Role] = map[string]any{"actualNative": observed, "actualPlugRef": commit.Results[0].Ref}
	}
	nativeJSON(t, filepath.Join(output, "source-to-plug-bindings.json"), bindings)
	for _, node := range capture.Plan.Nodes {
		if node.Parent != "" {
			r.write("parent-"+node.Role, "bead.link", map[string]any{"from": r.refs[node.Role], "to": r.refs[node.Parent], "kind": "parent-child"}, r.refs[node.Role].ID)
		}
		if node.Spawner != "" {
			r.write("waits-"+node.Role, "bead.link", map[string]any{"from": r.refs[node.Role], "to": r.refs[node.Spawner], "kind": "waits-for", "metadata": map[string]any{"gate": node.Gate, "also_blocks": false}}, r.refs[node.Role].ID)
		}
	}
	for index, edge := range capture.Plan.Edges {
		r.write(fmt.Sprintf("edge-%d", index), "bead.link", map[string]any{"from": r.refs[edge.From], "to": r.refs[edge.To], "kind": edge.Kind}, r.refs[edge.From].ID)
	}
	// A genuine public transaction requests a title change followed by a missing
	// endpoint. Its failure must conserve the entire image and add no receipt;
	// this does not assert an unmeasured internal staging point.
	first := capture.Plan.Nodes[0].Role
	r.refuse("atomic-missing-endpoint", "tx.apply", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
		patchOp(r.refs[first].ID, map[string]any{"title": "must roll back"}),
		linkOp(model.OpLink, r.refs[first].ID, "plug-b02-missing-endpoint", "blocks"),
	}}}, "not_found")
	r.observe(capture, "initial", capture.Plan.InitialReady, "")
	for _, step := range capture.Plan.Steps {
		parent := t
		if !t.Run(step.Name, func(t *testing.T) {
			r.f.t = t
			defer func() { r.f.t = parent }()
			params := map[string]any{"id": r.refs[step.Role], "reason": "B02 " + step.Name}
			method := step.PlugMethod
			if step.Action == "self" {
				method, params = "bead.link", map[string]any{"from": r.refs[step.Role], "to": r.refs[step.Role], "kind": "blocks"}
			} else if step.Action == "unlink" {
				method, params = "bead.unlink", map[string]any{"from": r.refs[step.Role], "to": r.refs[step.Target], "kind": step.Kind}
			}
			if step.PlugRefusal != "" {
				if method == "" {
					method = "bead.close"
				}
				r.refuse(step.Name, method, params, step.PlugRefusal)
			} else if step.Action == "also" {
				r.write(step.Name+"-unlink", "bead.unlink", map[string]any{"from": r.refs[step.Role], "to": r.refs[step.Target], "kind": "waits-for"}, r.refs[step.Role].ID)
				r.write(step.Name+"-link", "bead.link", map[string]any{"from": r.refs[step.Role], "to": r.refs[step.Target], "kind": "waits-for", "metadata": map[string]any{"gate": "all-children", "also_blocks": true}}, r.refs[step.Role].ID)
			} else {
				if method == "" {
					t.Fatalf("missing authored public method for %s", step.Name)
				}
				commit := r.write(step.Name, method, params, r.refs[step.Role].ID)
				if status, exists := map[string]string{"bead.close": "finished", "bead.fail": "broken", "bead.cancel": "abandoned", "bead.defer": "parked", "bead.reopen": "queued"}[method]; exists {
					r.statuses[step.Role] = status
				}
				if capture.Plan.Name == "category-outcomes" && (method == "bead.close" || method == "bead.fail" || method == "bead.cancel") {
					r.assertTerminalEvents(step.Name, method, commit)
				}
			}
			r.observe(capture, step.Name, step.PlugReady, step.Deviation)
		}) {
			t.Fatalf("B02 replay %s incomplete; subsequent leaves are NOT_RUN", step.Name)
		}
	}
	t.Logf("B02_PUBLIC_REPLAY case=%s run=%s engine=%s phases=%d rpc=%d", capture.Plan.Name, capture.Run, engine, len(capture.Phases), len(r.calls))
}
