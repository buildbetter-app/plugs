package s1b01b04

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Shared by public acceptance and the physical recorder's replay. These
// helpers perform real public requests and read actual retained store state;
// they never construct a native observation or start a native process.
func b02Bytes(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

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

func (r *b02Replay) assertTerminalEvents(label, method string, commit model.Commit) {
	t := r.f.t
	state := r.snapshot()
	want, exists := map[string]struct {
		Event   model.DurableEventType
		Slot    string
		Notices int
	}{
		"bead.close":  {model.EventBeadClosed, "close", 1},
		"bead.fail":   {model.EventBeadFailed, "fail", 0},
		"bead.cancel": {model.EventBeadCanceled, "cancel", 1},
	}[method]
	if !exists {
		t.Fatalf("%s lacks an explicit terminal event expectation for %s", label, method)
	}
	blocker, dependent := r.refs["blocker"], r.refs["conditional"]
	current := map[model.Ref]model.Revision{}
	for _, record := range state.Records {
		current[record.Ref] = record.Revision
	}
	if current[blocker] != commit.Revision || state.Meta.Head != commit.Revision {
		t.Fatalf("%s terminal commit is not the actual changed blocker revision", label)
	}
	var changeset *model.Changeset
	for index := range state.History {
		if state.History[index].Revision == commit.Revision {
			if changeset != nil {
				t.Fatalf("%s has duplicate history entries for the terminal revision", label)
			}
			changeset = &state.History[index]
		}
	}
	if changeset == nil || changeset.Incarnation != state.Meta.Incarnation || changeset.Kind != model.ChangesetWrite || len(changeset.Changes) != 1 || changeset.Changes[0].Ref != blocker || changeset.Changes[0].Op != model.OpPatch || changeset.Changes[0].FromRevision == commit.Revision {
		t.Fatalf("%s lacks the single actual blocker change at its terminal revision", label)
	}
	// ValidateEvents accepts a nil legacy plan, so require the complete explicit
	// plan before validation. Failure MUST retain bead.failed, even with no notice.
	if changeset.Events == nil || len(changeset.Events) != 1+want.Notices {
		t.Fatalf("%s event plan must contain one %s and %d condition notices", label, want.Event, want.Notices)
	}
	if err := changeset.ValidateEvents(); err != nil {
		t.Fatalf("complete durable event plan: %v", err)
	}
	ordinary := changeset.Events[0]
	if ordinary.Type != want.Event || ordinary.Resource.Kind != "bead" || ordinary.Resource.Ref != blocker || ordinary.ChangeIndex.String() != "0" || ordinary.Provenance.Method != method || ordinary.Provenance.Slot != want.Slot {
		t.Fatalf("%s lacks the exact ordinary lifecycle event on the actual blocker change", label)
	}
	count := 0
	for _, event := range changeset.Events {
		if event.Type != model.EventDependencyConditionUnmet {
			continue
		}
		count++
		if event.Resource.Ref != dependent || event.Details == nil || event.Details.Dependent.Ref != dependent || event.Details.Dependent.Revision != current[dependent] || event.Details.BlockerBefore.Ref != blocker || event.Details.BlockerBefore.Revision != changeset.Changes[0].FromRevision || event.Details.BlockerAfter.Ref != blocker || event.Details.BlockerAfter.Revision != commit.Revision {
			t.Fatalf("%s notice is not anchored to the actual dependent/blocker revisions", label)
		}
	}
	if count != want.Notices {
		t.Fatalf("%s condition notice count=%d want=%d", label, count, want.Notices)
	}
}
