package s1b01b04

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type b02RPCScope struct {
	Method, Key string
	Committed   bool
}

// The complete typed zero Changeset is a driver no-op receipt, not a valid
// wire Changeset. Record that exact distinction without projecting any field
// of a nonzero changeset or changing product serialization or validation.
type b02ReceiptEvidence struct {
	RequestHash     string
	Commit          model.Commit
	Changeset       *model.Changeset
	ResponsePending bool
	ZeroChangeset   bool
}

type b02PlugState struct {
	Meta     driver.Meta
	Records  []driver.Record
	History  []model.Changeset
	Versions map[string][]driver.Version
	Derived  map[driver.DerivedTable][]driver.DerivedRow
	Facts    []driver.LockState
	Receipts map[string]b02ReceiptEvidence
}

type b02Replay struct {
	f        *fixture
	output   string
	refs     map[string]model.Ref
	statuses map[string]string
	calls    []b02RPCScope
}

func (r *b02Replay) rpc(label, method string, params map[string]any, write bool, code string) []byte {
	t := r.f.t
	key := r.f.key(label, method, owner)
	payload := map[string]any{}
	for name, value := range params {
		payload[name] = value
	}
	payload["_meta"] = map[string]any{"project": project, "agent": owner, "idempotencyKey": key}
	request := b02Bytes(t, map[string]any{"jsonrpc": "2.0", "id": len(r.calls) + 1, "method": method, "params": payload})
	name := fmt.Sprintf("%04d-%s", len(r.calls)+1, label)
	nativeWrite(t, filepath.Join(r.output, name+".request.json"), request)
	started := time.Now().UTC()
	response, err := r.f.transport.RoundTrip(r.f.ctx, request)
	nativeWrite(t, filepath.Join(r.output, name+".response.json"), response)
	errorText := ""
	if err != nil {
		errorText = err.Error()
	}
	nativeJSON(t, filepath.Join(r.output, name+".receipt.json"), map[string]any{
		"method": method, "key": key, "startedUTC": started, "endedUTC": time.Now().UTC(), "transportError": errorText,
		"requestSHA256": sha256Hex(request), "requestBytes": len(request), "responseSHA256": sha256Hex(response), "responseBytes": len(response), "expectedErrorCode": code,
	})
	if err != nil {
		t.Fatalf("B02 actual RPC %s: %v", method, err)
	}
	var envelope map[string]json.RawMessage
	nativeDecode(t, response, &envelope)
	if code == "" {
		if envelope["error"] != nil || envelope["result"] == nil {
			t.Fatalf("B02 unexpected RPC result: %s", response)
		}
	} else {
		if _, exists := envelope["result"]; exists || errorCode(t, response) != code {
			t.Fatalf("B02 refusal requires %s with no result: %s", code, response)
		}
		// Inspect actual fields without changing their typed public spelling.
		var failure map[string]any
		nativeDecode(t, envelope["error"], &failure)
		data, ok := failure["data"].(map[string]any)
		if !ok || data["code"] != code || data["traceId"] == nil || data["traceId"] == "" {
			t.Fatal("public refusal lacks its typed code and transport trace")
		}
	}
	r.calls = append(r.calls, b02RPCScope{Method: method, Key: key, Committed: write && code == ""})
	return response
}

func (r *b02Replay) snapshot() b02PlugState {
	t := r.f.t
	state := b02PlugState{
		Meta: r.f.metaSnapshot(), Records: []driver.Record{}, History: []model.Changeset{},
		Versions: map[string][]driver.Version{}, Derived: map[driver.DerivedTable][]driver.DerivedRow{}, Receipts: map[string]b02ReceiptEvidence{},
	}
	reader, err := r.f.store.Read(r.f.ctx, driver.View{Incarnation: state.Meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var cursor *driver.Cursor
	seen := map[string]bool{}
	for {
		page, err := reader.Query(r.f.ctx, driver.Query{Limit: 1000, Cursor: cursor})
		if err != nil || (page.NextCursor != nil && len(page.Items) == 0) {
			t.Fatalf("complete B02 source query: %v", err)
		}
		for _, record := range page.Items {
			key := string(b02Bytes(t, record.Ref))
			if seen[key] {
				t.Fatal("complete source query repeated a record")
			}
			seen[key] = true
			state.Records = append(state.Records, record)
			versions, err := reader.Versions(r.f.ctx, record.Ref, driver.PageReq{Limit: 1000})
			if err != nil || versions.NextCursor != nil {
				t.Fatalf("bounded complete versions for %s: %v", record.Ref.ID, err)
			}
			state.Versions[key] = versions.Items
		}
		if page.NextCursor == nil {
			break
		}
		cursor = page.NextCursor
	}
	sort.Slice(state.Records, func(i, j int) bool {
		return bytes.Compare(b02Bytes(t, state.Records[i].Ref), b02Bytes(t, state.Records[j].Ref)) < 0
	})
	state.History, err = reader.Changesets(r.f.ctx, nil, 1000)
	// This finite fixture has fewer than 1000 writes. Refuse the boundary rather
	// than quietly calling a potentially truncated history complete.
	if err != nil || len(state.History) >= 1000 {
		t.Fatalf("complete bounded changeset history: %v", err)
	}
	for _, table := range []driver.DerivedTable{
		driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure,
		driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex,
		driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins,
	} {
		rows, err := reader.DerivedRows(r.f.ctx, table, "")
		if err != nil {
			t.Fatalf("complete B02 derived table %s: %v", table, err)
		}
		state.Derived[table] = rows
	}
	state.Facts, err = reader.Observe(r.f.ctx, []driver.LockKey{pipeline.GraphValidityLockKey()})
	if err != nil || len(state.Facts) != 1 {
		t.Fatalf("B02 graph validity fact: %v", err)
	}
	for _, call := range r.calls {
		receipt, err := reader.IdempotencyByKey(r.f.ctx, "anonymous:anonymous", owner, call.Method, call.Key)
		if err != nil || (call.Committed && (receipt == nil || receipt.ResponsePending)) || (!call.Committed && receipt != nil) {
			t.Fatalf("B02 receipt scope %s/committed=%t: %v %+v", call.Method, call.Committed, err, receipt)
		}
		if receipt != nil {
			evidence := b02ReceiptEvidence{RequestHash: receipt.RequestHash, Commit: receipt.Commit, ResponsePending: receipt.ResponsePending}
			if reflect.DeepEqual(receipt.Changeset, model.Changeset{}) {
				evidence.ZeroChangeset = true
			} else {
				copy := receipt.Changeset
				evidence.Changeset = &copy
			}
			state.Receipts[call.Method+"/"+call.Key] = evidence
		}
	}
	return state
}

func (r *b02Replay) renew() {
	if err := r.f.lease.Renew(r.f.ctx); err != nil {
		r.f.t.Fatalf("B02 renew original active fixture authority: %v", err)
	}
	select {
	case <-r.f.lease.Lost():
		r.f.t.Fatal("B02 fixture authority is lost")
	default:
	}
}

func (r *b02Replay) write(label, method string, params map[string]any, target string) model.Commit {
	t := r.f.t
	r.renew()
	before := r.snapshot()
	nativeJSON(t, filepath.Join(r.output, label+"-before.json"), before)
	raw := r.rpc(label, method, params, true, "")
	var envelope struct{ Result model.Commit }
	nativeDecode(t, raw, &envelope)
	commit := envelope.Result
	after := r.snapshot()
	nativeJSON(t, filepath.Join(r.output, label+"-after.json"), after)
	key := method + "/" + r.f.key(label, method, owner)
	receipt, exists := after.Receipts[key]
	if !exists || receipt.ResponsePending || !bytes.Equal(b02Bytes(t, commit), b02Bytes(t, receipt.Commit)) || commit.Revision != after.Meta.Head {
		t.Fatal("completed receipt and whole returned Commit do not bind the current head")
	}
	for scope, prior := range before.Receipts {
		if !bytes.Equal(b02Bytes(t, prior), b02Bytes(t, after.Receipts[scope])) {
			t.Fatal("write changed a prior receipt")
		}
	}
	if _, existed := before.Receipts[key]; existed || len(after.Receipts) != len(before.Receipts)+1 {
		t.Fatal("fresh B02 write did not add exactly its own method/key receipt")
	}
	priorMeta := before.Meta
	priorMeta.Head = after.Meta.Head
	if !reflect.DeepEqual(priorMeta, after.Meta) || len(after.History) < len(before.History) {
		t.Fatal("B02 write changed store identity or the prior complete history")
	}
	for index, changeset := range before.History {
		if !bytes.Equal(b02Bytes(t, changeset), b02Bytes(t, after.History[index])) {
			t.Fatal("B02 write changed a prior complete changeset")
		}
	}
	afterRecords := map[string]driver.Record{}
	for _, record := range after.Records {
		afterRecords[record.Ref.ID] = record
	}
	for _, record := range before.Records {
		if record.Ref.ID != target && !bytes.Equal(b02Bytes(t, record), b02Bytes(t, afterRecords[record.Ref.ID])) {
			t.Fatalf("B02 %s changed unrelated source record %s", label, record.Ref.ID)
		}
		versionKey := string(b02Bytes(t, record.Ref))
		if record.Ref.ID != target && !bytes.Equal(b02Bytes(t, before.Versions[versionKey]), b02Bytes(t, after.Versions[versionKey])) {
			t.Fatalf("B02 %s changed unrelated version history %s", label, record.Ref.ID)
		}
	}
	wantRecords := len(before.Records)
	if method == "bead.create" {
		wantRecords++
	}
	if len(after.Records) != wantRecords {
		t.Fatal("B02 write added or removed an unexpected source record")
	}
	if after.Meta.Head == before.Meta.Head {
		if !receipt.ZeroChangeset || len(after.History) != len(before.History) || !bytes.Equal(b02Bytes(t, before.Records), b02Bytes(t, after.Records)) || !bytes.Equal(b02Bytes(t, before.Versions), b02Bytes(t, after.Versions)) || !bytes.Equal(b02Bytes(t, before.Derived), b02Bytes(t, after.Derived)) || !bytes.Equal(b02Bytes(t, before.Facts), b02Bytes(t, after.Facts)) {
			t.Fatal("quiet B02 receipt changed source/history/versions/derived/facts")
		}
	} else if receipt.ZeroChangeset || len(after.History) != len(before.History)+1 || after.History[len(after.History)-1].Revision != commit.Revision {
		t.Fatal("B02 write did not publish exactly one complete changeset")
	}
	// Re-send the same semantic request and key, preserving the second original.
	// A replay must conserve every durable byte, including the first receipt.
	replayed := r.rpc(label, method, params, true, "")
	var replayEnvelope struct {
		Result model.Commit
		Meta   struct{ Replayed bool } `json:"_meta"`
	}
	nativeDecode(t, replayed, &replayEnvelope)
	final := r.snapshot()
	nativeJSON(t, filepath.Join(r.output, label+"-after-replay.json"), final)
	if !replayEnvelope.Meta.Replayed || !bytes.Equal(b02Bytes(t, commit), b02Bytes(t, replayEnvelope.Result)) || !bytes.Equal(b02Bytes(t, after), b02Bytes(t, final)) {
		t.Fatal("same-key public replay changed result or complete state")
	}
	return commit
}

func (r *b02Replay) refuse(label, method string, params map[string]any, code string) {
	r.renew()
	before := r.snapshot()
	nativeJSON(r.f.t, filepath.Join(r.output, label+"-before.json"), before)
	r.rpc(label, method, params, true, code)
	after := r.snapshot()
	nativeJSON(r.f.t, filepath.Join(r.output, label+"-after.json"), after)
	if !bytes.Equal(b02Bytes(r.f.t, before), b02Bytes(r.f.t, after)) {
		r.f.t.Fatal("public refusal changed records/history/versions/receipts/derived/facts")
	}
}

func (r *b02Replay) read(label, method string, params map[string]any) map[string]any {
	return result(r.f.t, r.rpc(label, method, params, false, ""))
}

func (r *b02Replay) readyRoles(page map[string]any) []string {
	items, ok := page["items"].([]any)
	if !ok {
		r.f.t.Fatal("public ready page lacks its actual items array")
	}
	known := map[string]string{}
	for role, ref := range r.refs {
		known[ref.ID] = role
	}
	roles := []string{}
	seen := map[string]bool{}
	for _, item := range items {
		row, ok := item.(map[string]any)
		id, _ := row["id"].(string)
		role := known[id]
		if !ok || role == "" || seen[role] {
			r.f.t.Fatal("public ready contains an unknown or duplicate actual ref")
		}
		seen[role] = true
		roles = append(roles, role)
	}
	return roles
}

func (r *b02Replay) reopen(label string) {
	t := r.f.t
	before := r.snapshot()
	oldStore, oldLease := r.f.store, r.f.lease
	if err := oldLease.Release(r.f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := oldStore.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-oldLease.Lost():
	default:
		t.Fatal("old SQL authority did not close")
	}
	r.f.store = openStore(t, r.f.backend, r.f.path, r.f.dsn, r.f.clock)
	lease, err := r.f.store.Lease(context.Background(), "s1-b02-native-reopened", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	r.f.lease = lease
	r.f.install()
	after := r.snapshot()
	nativeJSON(t, filepath.Join(r.output, label+"-sql-reopen.json"), map[string]any{
		"engine": r.f.backend, "oldHandle": fmt.Sprintf("%p", oldStore), "newHandle": fmt.Sprintf("%p", r.f.store),
		"oldEpoch": oldLease.Epoch(), "newEpoch": lease.Epoch(), "before": before, "after": after,
	})
	if r.f.store == oldStore || lease.Epoch() <= oldLease.Epoch() || !bytes.Equal(b02Bytes(t, before), b02Bytes(t, after)) {
		t.Fatal("physical SQL reopen changed complete state or failed handle/epoch boundary")
	}
}

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

func (r *b02Replay) assertConditionNotice(label string, commit model.Commit, want bool) {
	t := r.f.t
	state := r.snapshot()
	count := 0
	for _, changeset := range state.History {
		if changeset.Revision != commit.Revision {
			continue
		}
		if err := changeset.ValidateEvents(); err != nil {
			t.Fatalf("complete durable event plan: %v", err)
		}
		for _, event := range changeset.Events {
			if event.Type != model.EventDependencyConditionUnmet {
				continue
			}
			count++
			if event.Resource.Ref != r.refs["conditional"] || event.Details == nil || event.Details.Dependent.Ref != r.refs["conditional"] || event.Details.BlockerAfter.Ref != r.refs["blocker"] || event.Details.BlockerAfter.Revision != commit.Revision {
				t.Fatalf("%s notice is not anchored to the actual dependent/blocker revision", label)
			}
		}
	}
	if (want && count != 1) || (!want && count != 0) {
		t.Fatalf("%s condition notice count=%d wantNotice=%t", label, count, want)
	}
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
					r.assertConditionNotice(step.Name, commit, method != "bead.fail")
				}
			}
			r.observe(capture, step.Name, step.PlugReady, step.Deviation)
		}) {
			t.Fatalf("B02 replay %s incomplete; subsequent leaves are NOT_RUN", step.Name)
		}
	}
	t.Logf("B02_PUBLIC_REPLAY case=%s run=%s engine=%s phases=%d rpc=%d", capture.Plan.Name, capture.Run, engine, len(capture.Phases), len(r.calls))
}
