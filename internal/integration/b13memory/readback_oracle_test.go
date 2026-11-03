package b13memory_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (f *fixture) readbackState() durableState {
	f.t.Helper()
	meta, err := f.oracle.Meta(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	if meta.HistoryFloor.IsZero() {
		return f.state()
	}
	r := f.reader()
	_, err = r.Changesets(f.ctx, nil, 1000)
	if closeErr := r.Close(); closeErr != nil {
		f.t.Fatal(closeErr)
	}
	var truncated driver.ErrTruncated
	if !errors.As(err, &truncated) || truncated.Floor.String() != meta.HistoryFloor.String() {
		f.t.Fatalf("restore invented an available history prefix: floor=%s err=%v", meta.HistoryFloor, err)
	}
	return f.stateAfter(&driver.Cursor{Incarnation: meta.Incarnation, Revision: meta.HistoryFloor, Method: "changesets"})
}

func (f *fixture) reopenReadback(phase string) {
	f.t.Helper()
	before := f.readbackState()
	if f.engine == "memory" {
		f.bind()
	} else {
		f.close()
		f.open() // Every prior store/oracle/SQL handle closes; the DSN is unchanged.
	}
	requireSame(f.t, before, f.readbackState())
	f.t.Logf("B13 readback phase=%s engine=%s same-store reopen complete head=%s floor=%s", phase, f.engine, before.Meta.Head, before.Meta.HistoryFloor)
}

func (f *fixture) readbackRefuse(method string, params map[string]any, receiptKey, code string) {
	f.t.Helper()
	if receiptKey != "" {
		f.scopes[scope{method, receiptKey}] = true
	}
	before := f.readbackState()
	r := f.call(method, params, receiptKey)
	f.readbackRefusal(r, code, before)
}

func (f *fixture) readbackRefusal(r reply, code string, before durableState) {
	f.t.Helper()
	if r.Error == nil || r.Error.Data.Code != code || r.Result != nil || r.Meta != nil {
		f.t.Fatalf("expected complete %s refusal, got %+v", code, r)
	}
	for _, content := range []string{originalBody, replacedBody, readbackBody} {
		quoted := encode(f.t, content)
		if bytes.Contains(encode(f.t, r.Error), quoted[1:len(quoted)-1]) {
			f.t.Fatal("readback refusal disclosed memory content")
		}
	}
	requireSame(f.t, before, f.readbackState())
}

func (f *fixture) readbackAccept(method string, params map[string]any, receiptKey string, op model.OpKind) (model.Commit, reply) {
	f.t.Helper()
	before := f.readbackState()
	r := f.call(method, params, receiptKey)
	commit := decode[model.Commit](f.t, result(f.t, r))
	after := f.readbackState()
	head, err := before.Meta.Head.Uint64()
	if err != nil || len(commit.Results) != 1 || commit.Revision.String() != fmt.Sprint(head+1) || after.Meta.Head.String() != commit.Revision.String() || len(after.Changesets) != len(before.Changesets)+1 {
		f.t.Fatalf("readback write did not publish exactly one revision/result/log: %+v err=%v", commit, err)
	}
	item := commit.Results[0]
	versions := after.Versions[item.Ref.ID]
	set := after.Changesets[len(after.Changesets)-1]
	receipt := after.Receipts[scope{method, receiptKey}]
	if item.Revision.String() != commit.Revision.String() || len(versions) != len(before.Versions[item.Ref.ID])+1 || set.Revision.String() != commit.Revision.String() || set.Kind != model.ChangesetWrite || set.Incarnation != after.Meta.Incarnation || set.IdempotencyKey != receiptKey || !reflect.DeepEqual(set.Principal, anonymous) || len(set.Changes) != 1 || set.Changes[0].Ref.ID != item.Ref.ID || set.Changes[0].Op != op || receipt == nil || receipt.ResponsePending || receipt.RequestHash == "" || !reflect.DeepEqual(receipt.Commit, commit) || !reflect.DeepEqual(receipt.Changeset, set) {
		f.t.Fatalf("readback commit/version/log/receipt binding failed: commit=%+v set=%+v receipt=%+v", commit, set, receipt)
	}
	var previous model.Revision
	for _, row := range before.Records {
		if row.Ref.ID == item.Ref.ID {
			previous = row.Revision
		}
	}
	v := versions[len(versions)-1]
	if set.Changes[0].FromRevision.String() != previous.String() || v.Ref.ID != item.Ref.ID || v.Revision.String() != commit.Revision.String() || v.Deleted != (op == model.OpDelete) || !reflect.DeepEqual(v.Principal, anonymous) {
		f.t.Fatalf("readback write lost its prior revision or version identity: %+v", v)
	}
	if !v.Deleted {
		body := v.Body
		if !v.Base {
			patch := decode[[]struct {
				Op    string          `json:"op"`
				Path  string          `json:"path"`
				Value json.RawMessage `json:"value"`
			}](f.t, v.Patch)
			if len(patch) != 1 || patch[0].Op != "replace" || patch[0].Path != "" {
				f.t.Fatalf("readback version has an unexpected forward patch: %s", v.Patch)
			}
			body = patch[0].Value
		}
		if !bytes.Equal(body, stored(f.t, after, item.Ref.ID).Body) {
			f.t.Fatal("readback version body differs from the current raw record")
		}
	}
	return commit, r
}

func (f *fixture) replayReadback(method string, params map[string]any, receiptKey string, original reply) {
	f.t.Helper()
	before := f.readbackState()
	r := f.call(method, params, receiptKey)
	if r.Error != nil || r.Meta == nil || !r.Meta.Replayed || !bytes.Equal(r.Result, original.Result) {
		f.t.Fatalf("readback replay changed exact public result bytes: first=%s replay=%+v", original.Result, r)
	}
	requireSame(f.t, before, f.readbackState())
}

func (f *fixture) readbackValues(want map[string]memoryValue, beadID string) {
	f.t.Helper()
	before := f.readbackState()
	expected := make([]memoryValue, 0, len(want))
	for id, value := range want {
		got := memoryRow(f.t, result(f.t, f.call("memory.get", map[string]any{"id": id}, "")))
		row := stored(f.t, before, id)
		persisted := memoryRow(f.t, row.Body)
		persisted.Revision = row.Revision
		if row.Kind != "memory" || !reflect.DeepEqual(got, value) || !reflect.DeepEqual(persisted, value) {
			f.t.Fatalf("restored public/raw memory differs from literal expectation: get=%+v row=%+v want=%+v", got, row, value)
		}
		expected = append(expected, value)
	}
	sort.Slice(expected, func(i, j int) bool {
		if expected[i].Key != expected[j].Key {
			return expected[i].Key < expected[j].Key
		}
		return expected[i].ID < expected[j].ID
	})
	seen, cursor := make([]memoryValue, 0), ""
	for page := 0; ; page++ {
		if page > len(expected)+1 {
			f.t.Fatal("restored memory pages did not terminate")
		}
		params := map[string]any{"limit": 2}
		if cursor != "" {
			params["cursor"] = cursor
		}
		value := decode[struct {
			Items      []json.RawMessage `json:"items"`
			NextCursor string            `json:"nextCursor"`
		}](f.t, result(f.t, f.call("memory.list", params, "")))
		seen = append(seen, memoryRows(f.t, value.Items)...)
		if value.NextCursor == "" {
			break
		}
		if len(value.Items) == 0 || value.NextCursor == cursor {
			f.t.Fatal("restored memory cursor did not advance")
		}
		cursor = value.NextCursor
	}
	brief := decode[struct {
		Memories []json.RawMessage `json:"memories"`
		ReadyTop []struct {
			ID string `json:"id"`
		} `json:"readyTop"`
	}](f.t, result(f.t, f.call("agent.brief", map[string]any{"budgetTokens": 1000}, "")))
	if !reflect.DeepEqual(seen, expected) || !reflect.DeepEqual(memoryRows(f.t, brief.Memories), expected) || len(brief.ReadyTop) != 1 || brief.ReadyTop[0].ID != beadID {
		f.t.Fatalf("restored list/brief changed memory values or plane membership: list=%+v brief=%+v want=%+v", seen, brief, expected)
	}
	requireSame(f.t, before, f.readbackState())
}

func (f *fixture) readbackOtherPlanes(before durableState, beadID string) {
	f.t.Helper()
	after := f.readbackState()
	for _, id := range []string{"b13-setting", "b13-generic-setting", beadID} {
		if !reflect.DeepEqual(stored(f.t, before, id), stored(f.t, after, id)) || !reflect.DeepEqual(before.Versions[id], after.Versions[id]) {
			f.t.Fatalf("readback memory write changed config/bead %s", id)
		}
	}
}

// Retain separate source and target public transcripts. The legacy fixture's
// single per-engine filename cannot represent both sides of a round trip.
func (f *fixture) retainReadbackTrace(side string) {
	f.t.Cleanup(func() {
		dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
		if dir == "" {
			f.t.Error("B13 public transcript sink unavailable: TEST_UNDECLARED_OUTPUTS_DIR is unset")
			return
		}
		if len(f.frames) == 0 {
			f.t.Error("B13 public transcript contains no actual request/response frames")
			return
		}
		var data bytes.Buffer
		for _, frame := range f.frames {
			data.Write(encode(f.t, frame))
			data.WriteByte('\n')
		}
		name := fmt.Sprintf("b13-readback-%s-%s-public.jsonl", f.engine, side)
		if err := os.WriteFile(filepath.Join(dir, name), data.Bytes(), 0o600); err != nil {
			f.t.Error(err)
		}
	})
}
