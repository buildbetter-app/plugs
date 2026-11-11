package cardb20

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/client"
	"github.com/buildbetter-app/plugs/internal/core/model"
)

// These are additional observations on the retained three-mutation corpus.
// Each CLI invocation constructs a fresh real Client; transport supplies no data.
type b20CallerObservation struct {
	Checkpoint string            `json:"checkpoint"`
	Since      int               `json:"since"`
	Form       int               `json:"form"`
	Projection []stableEvent     `json:"projection"`
	Events     []json.RawMessage `json:"exact_wire_events"`
	Requests   []json.RawMessage `json:"requests"`
	Responses  []json.RawMessage `json:"responses"`
	Stdout     string            `json:"stdout"`
}

type b20ClosureResult struct {
	Engine       string                 `json:"engine"`
	Process      int                    `json:"process"`
	Reopened     bool                   `json:"sql_reopened"`
	Observations []b20CallerObservation `json:"observations"`
}

func b20CallerWire(t *testing.T, transport *callerTransport, output string, export bool, after string, limit int) []json.RawMessage {
	t.Helper()
	if len(transport.Requests) == 0 || len(transport.Requests) != len(transport.Responses) {
		t.Fatal("caller wire inventory incomplete")
	}
	var wire []json.RawMessage
	for i, raw := range transport.Requests {
		var request struct {
			Method string
			Params struct {
				After string
				Limit int
			}
		}
		var response struct {
			Result client.EventPage
			Error  json.RawMessage
		}
		if err := json.Unmarshal(raw, &request); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(transport.Responses[i], &response); err != nil {
			t.Fatal(err)
		}
		if request.Method != "event.list" || request.Params.After != after || request.Params.Limit != limit || len(response.Error) != 0 {
			t.Fatalf("caller request chain or positive bounded limit differs: %s => %s", raw, transport.Responses[i])
		}
		page := response.Result
		if page.Cursor == "" || len(page.Events) > limit {
			t.Fatal("wire page omitted cursor or exceeded row cap")
		}
		if i == len(transport.Requests)-1 {
			if len(page.Events) != 0 || page.Cursor != after {
				t.Fatal("caller stopped without an actual empty stable-cursor page")
			}
		} else if page.Cursor == after {
			t.Fatal("caller continued after a stable cursor")
		}
		wire = append(wire, page.Events...)
		after = page.Cursor
	}
	var stdout []json.RawMessage
	if export {
		decoder := json.NewDecoder(bytes.NewBufferString(output))
		for decoder.More() {
			var event json.RawMessage
			if err := decoder.Decode(&event); err != nil {
				t.Fatal(err)
			}
			stdout = append(stdout, event)
		}
	} else {
		var page client.EventPage
		if err := json.Unmarshal([]byte(output), &page); err != nil {
			t.Fatal(err)
		}
		if page.Cursor != after {
			t.Fatal("CLI completion cursor differs from final original response")
		}
		stdout = page.Events
	}
	if len(stdout) != len(wire) {
		t.Fatal("CLI stdout omitted or duplicated original wire events")
	}
	for i := range wire {
		actual, err := decodeNativeSQLDocument(stdout[i])
		if err != nil {
			t.Fatal(err)
		}
		expected, err := decodeNativeSQLDocument(wire[i])
		if err != nil || !reflect.DeepEqual(actual, expected) {
			t.Fatalf("CLI changed original event values/types/numeric tokens at%d: %v", i, err)
		}
	}
	return wire
}

func b20ReplayCallerClosure(t *testing.T, engine string) b20ClosureResult {
	t.Helper()
	fixture := openStore(t, engine)
	r := newRuntime(t, fixture.store, false, "b20-1")
	result := b20ClosureResult{Engine: engine, Process: os.Getpid()}
	writeNumber := 1000
	var writes []map[string]any
	callExpected := func(method string, params map[string]any, write bool, wantCode string) json.RawMessage {
		transport := &callerTransport{r: r}
		c, err := client.New(transport, "cardb20", "b20-closure-client")
		if err != nil {
			t.Fatal(err)
		}
		key := ""
		if write {
			writeNumber++
			key = fmt.Sprintf("%032x", writeNumber)
		}
		called, err := c.Call(r.ctx, method, params, key)
		writes = append(writes, map[string]any{"method": method, "requests": transport.Requests, "responses": transport.Responses, "error": fmt.Sprint(err)})
		if wantCode != "" {
			var envelope response
			if len(transport.Responses) != 1 || json.Unmarshal(transport.Responses[0], &envelope) != nil || envelope.Error == nil || envelope.Error.Data.Code != wantCode || err == nil {
				t.Fatalf("actual Client.Call %s required typed refusal%s: %v", method, wantCode, err)
			}
			return nil
		}
		if err != nil {
			t.Fatalf("actual Client.Call %s: %v", method, err)
		}
		return called.JSON
	}
	call := func(method string, params map[string]any, write bool) json.RawMessage {
		return callExpected(method, params, write, "")
	}
	refuse := func() {
		callExpected("tx.apply", map[string]any{"changeSet": map[string]any{"ops": []any{
			map[string]any{"kind": "patch", "target": map[string]any{"id": "b20-1"}, "fields": map[string]any{"title": "must roll back"}},
			map[string]any{"kind": "patch", "target": map[string]any{"id": "pl-b20-absent"}, "fields": map[string]any{"title": "missing"}},
		}}}, true, "not_found")
	}
	page := func(after string) eventPage {
		params := map[string]any{"filter": map[string]any{}, "limit": 1}
		if after != "" {
			params["after"] = after
		}
		var p eventPage
		if err := json.Unmarshal(call("event.list", params, false), &p); err != nil {
			t.Fatal(err)
		}
		return p
	}
	initial := page("")
	if len(initial.Events) != 0 || initial.Cursor == "" {
		t.Fatal("fresh store did not supply the actual zero-head cursor")
	}
	cursors := []string{initial.Cursor}
	want := []stableEvent{{"1", "create", "b20-1", "cardb20-reader", "journal anchor", "todo"}, {"2", "update", "b20-1", "cardb20-reader", "journal edited", "todo"}, {"3", "close", "b20-1", "cardb20-reader", "journal edited", "done"}}
	var committed []model.Event
	capture := func(checkpoint string, head int) {
		meta, sets := journal(t, r.store)
		if meta.Head.String() != fmt.Sprint(head) || len(sets) != head {
			t.Fatalf("%s committed head/rows=%s/%d", checkpoint, meta.Head, len(sets))
		}
		for i, set := range sets {
			if set.Revision.String() != fmt.Sprint(i+1) || len(set.Changes) != 1 {
				t.Fatalf("%s commit order differs at%d", checkpoint, i)
			}
		}
		full := list(t, r, "", 100).Events
		if !reflect.DeepEqual(projectEvents(t, full), want[:head]) || (len(committed) > 0 && !reflect.DeepEqual(full[:len(committed)], committed)) {
			t.Fatalf("%s committed public prefix changed or contains a gap", checkpoint)
		}
		for len(cursors) <= head {
			p := page(cursors[len(cursors)-1])
			if len(p.Events) != 1 || p.Cursor == cursors[len(cursors)-1] || !reflect.DeepEqual(p.Events[0], full[len(cursors)-1]) {
				t.Fatalf("%s exclusive one-row continuation changed committed event", checkpoint)
			}
			cursors = append(cursors, p.Cursor)
		}
		committed = append([]model.Event(nil), full...)
		for since := 0; since <= head; since++ {
			for form, args := range [][]string{
				{"events", "--limit", "0", "--page-size", "1"},
				{"events", "--limit", "all", "--page-size", "1"},
				{"events", "all", "--page-size", "1"},
				{"events", "export", "--page-size", "1"},
			} {
				args = append(args, "--after", cursors[since])
				got, transport, output := runCaller(t, r, args)
				if !reflect.DeepEqual(projectEvents(t, got), projectEvents(t, full[since:])) {
					t.Fatalf("%s since%d form%d lost/repeated/reordered events", checkpoint, since, form)
				}
				wire := b20CallerWire(t, transport, output, form == 3, cursors[since], 1)
				if len(wire) != len(full)-since {
					t.Fatal("wire suffix cardinality differs from committed public read")
				}
				for i, raw := range wire {
					var event model.Event
					if err := json.Unmarshal(raw, &event); err != nil || !reflect.DeepEqual(event, full[since+i]) {
						t.Fatalf("%s since%d changed complete committed event%d: %v", checkpoint, since, i, err)
					}
				}
				result.Observations = append(result.Observations, b20CallerObservation{checkpoint, since, form, projectEvents(t, got), wire, transport.Requests, transport.Responses, output})
			}
		}
		retain(t, "b20-closure-"+engine+"-"+checkpoint+"-store.json", map[string]any{"meta": meta, "changesets": sets, "exact_public_events": full, "cursors": cursors, "process": os.Getpid()})
	}
	mutate := func(method string, params map[string]any, revision string) {
		var commit model.Commit
		if err := json.Unmarshal(call(method, params, true), &commit); err != nil || commit.Revision.String() != revision {
			t.Fatalf("%s expected revision%s: %+v (%v)", method, revision, commit, err)
		}
	}
	patch := map[string]any{"id": map[string]any{"id": "b20-1"}, "fields": map[string]any{"title": "journal edited"}}
	mutate("bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "journal anchor"}}, "1")
	capture("create", 1)
	mutate("bead.update", patch, "2")
	capture("update", 2)
	before, beforeSets := journal(t, r.store)
	mutate("bead.update", patch, "2")
	capture("no-op", 2)
	refuse()
	capture("abort", 2)
	after, afterSets := journal(t, r.store)
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(beforeSets, afterSets) {
		t.Fatal("no-op/admission refusal changed complete committed rows")
	}
	if engine != "memory" {
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := fixture.open()
		if err != nil {
			t.Fatal(err)
		}
		r = newRuntime(t, reopened, false, "unused")
		result.Reopened = true
		if os.Getpid() != result.Process {
			t.Fatal("reopen left the retained signing-key/process lifetime")
		}
	}
	// Every cursor here was minted before reconnect/reopen. The fixture uses
	// the SAME process and explicit unchanged b20CursorKey; no process-restart claim.
	capture("reconnected", 2)
	mutate("bead.update", patch, "2")
	refuse()
	post, postSets := journal(t, r.store)
	if !reflect.DeepEqual(before, post) || !reflect.DeepEqual(beforeSets, postSets) {
		t.Fatal("post-reconnect no-op/refusal changed committed counter or payload")
	}
	mutate("bead.close", map[string]any{"id": map[string]any{"id": "b20-1"}, "reason": "complete"}, "3")
	capture("final", 3)
	retain(t, "b20-closure-"+engine+".json", map[string]any{"result": result, "client_call_receipts": writes, "signer_lifetime": "SAME_PROCESS_EXPLICIT_KEY", "old_signed_cursor_reused": true, "memory_persistence": "NOT_CLAIMED", "physical_storage_rollback": "NOT_CLAIMED", "complete": !t.Failed()})
	return result
}

func TestB20CallerExactCommitPrefixesReconnectAndSQLReopen(t *testing.T) {
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) { b20ReplayCallerClosure(t, engine) })
	}
}
