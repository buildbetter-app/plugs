package cardb20

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	proof "github.com/buildbetter-app/plugs/internal/integration/b07positive"
	publicproof "github.com/buildbetter-app/plugs/internal/integration/b07positive/public"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

func effectLifetime(t *testing.T,h *publicproof.Host,engine string) {
	t.Helper()
	marker,ok := h.Runtime.Store.(interface{WorkflowCheckpointLifetime() driver.CheckpointLifetime})
	if !ok || marker.WorkflowCheckpointLifetime().Validate() != nil { t.Fatal("actual checkpoint lifetime missing") }
	persistent := false
	if value,ok := h.Runtime.Store.(driver.PersistentCheckpointStore); ok { persistent = value.Persistent() }
	if engine == "memory" && (marker.WorkflowCheckpointLifetime() != driver.CheckpointLifetimeProcess || persistent) { t.Fatal("memory lifetime or persistence claim is false") }
	if engine != "memory" && (marker.WorkflowCheckpointLifetime() != driver.CheckpointLifetimeRestart || !persistent) { t.Fatal("SQL restart lifetime missing") }
	proof.Save(t,h.Root,"actual-lifetime.json",map[string]any{"engine":engine,"lifetime":marker.WorkflowCheckpointLifetime(),"persistent":persistent,"memoryRestartCredit":false})
}

func effectActive(refs ...model.Ref) []model.Ref {
	var active []model.Ref
	for _, ref := range refs {
		if strings.HasPrefix(ref.ID, workflowplugin.ItemPrefix) { active = append(active, ref) }
	}
	return active
}

func effectReopen(t *testing.T, h *publicproof.Host) {
	t.Helper()
	before := h.Capture()
	h.Reopen()
	proof.Same(t, before, h.Capture())
}

// Every mutation has a complete before/after image. The expected body change
// is constructed from the requested operation, never copied from its output.
// Generated application and storage times retain their separate clock values
// and must fall within the actual request interval before exact readback.
func effectCall(t *testing.T, h *publicproof.Host, method, label string, params map[string]any, touched, active []model.Ref) {
	t.Helper()
	if method == "bead.unlink" {
		copy := map[string]any{}
		for key, value := range params { if key != "metadata" { copy[key] = value } }
		params = copy
	}
	before := h.Capture()
	started := time.Now().UTC()
	reply := h.Call(method, label, params, "")
	finished := time.Now().UTC()
	var commit model.Commit
	if json.Unmarshal(reply.Result, &commit) != nil || len(commit.Results) != 1 { t.Fatal("exact one-operation commit missing") }
	if len(commit.Warnings)!=0 || commit.WouldCommit || len(commit.Violations)!=0 || commit.LeaseExpiresAt!=nil || commit.LeaseTtlMs!=nil {t.Fatal("edge effect returned an unrelated warning/dry-run/lease result")}
	after := h.Capture()
	changed := map[string]bool{}
	for _, ref := range touched {
		if len(effectActive(ref)) == 0 { changed[ref.ID] = true }
	}
	if len(active) != 0 && (method == "bead.link" || method == "bead.unlink") { changed = map[string]bool{} }
	if len(before.Rows) != len(after.Rows) { t.Fatal("edge effect changed record inventory") }
	for _, row := range before.Rows {
		next := after.Row(t, row.Ref.ID)
		oldVersions, newVersions := before.Versions[row.Ref.ID], after.Versions[row.Ref.ID]
		if !changed[row.Ref.ID] {
			if !reflect.DeepEqual(row, next) || !reflect.DeepEqual(oldVersions, newVersions) { t.Fatal("changed unrelated record/version", row.Ref.ID) }
			continue
		}
		oldDoc, err := pipeline.DecodeDocument(row.Body)
		if err != nil { t.Fatal(err) }
		nextDoc, err := pipeline.DecodeDocument(next.Body)
		if err != nil { t.Fatal(err) }
		switch method {
		case "bead.link":
			targetKind := model.LinkTargetKind("")
			if params["targetKind"] == "external" { targetKind = model.LinkTargetExternal }
			oldDoc.Links = append(oldDoc.Links, pipeline.Link{To: params["to"].(model.Ref), Kind: params["kind"].(string), TargetKind: targetKind, Metadata: params["metadata"].(*model.LinkMetadata)})
			if params["kind"] == "parent-child" && targetKind != model.LinkTargetExternal { parent := params["to"].(model.Ref); oldDoc.Parent = &parent }
		case "bead.unlink":
			if len(oldDoc.Links) != 1 { t.Fatal("expected one exact original edge") }
			oldDoc.Links = nil
			if params["kind"] == "parent-child" && params["targetKind"] != "external" { oldDoc.Parent = nil }
		case "bead.close":
			oldDoc.Fields["status"] = "done"
			if nextDoc.ClosedAt == nil || nextDoc.ClosedAt.Before(started) || nextDoc.ClosedAt.After(finished) { t.Fatal("close instant is outside the exact request interval") }
			stamp := *nextDoc.ClosedAt
			oldDoc.ClosedAt = &stamp
		case "bead.reopen":
			// The ordinary host keeps the installed issue workflow slots:
			// reopen selects todo, independently of the create request's status.
			oldDoc.Fields["status"] = "todo"
			oldDoc.ClosedAt = nil
		case "bead.defer":
			oldDoc.Fields["status"] = "backlog"
			oldDoc.ClosedAt = nil
		default:
			t.Fatal("undeclared body effect", method)
		}
		if !reflect.DeepEqual(oldDoc, nextDoc) { t.Fatalf("%s changed fields outside exact expected body: want %+v got %+v", method, oldDoc, nextDoc) }
		body, err := json.Marshal(oldDoc)
		if err != nil || !bytes.Equal(body, next.Body) { t.Fatal("current record differs from independently requested exact body", err) }
		oldRef, _ := json.Marshal(row.Ref)
		newRef, _ := json.Marshal(next.Ref)
		if !bytes.Equal(oldRef, newRef) || row.Kind != next.Kind || next.Revision != after.Meta.Head || len(newVersions) != len(oldVersions)+1 || !reflect.DeepEqual(oldVersions, newVersions[:len(oldVersions)]) { t.Fatal("wrong exact record/version publication", row.Ref.ID) }
		effectVersion(t, before, after, row, next, oldVersions, newVersions, method, params, commit, started, finished)
	}
	if len(changed) == 0 {
		if before.Meta != after.Meta || !reflect.DeepEqual(before.Changes, after.Changes) || !reflect.DeepEqual(before.Derived, after.Derived) || !reflect.DeepEqual(before.Receipts, after.Receipts) { t.Fatal("workflow-only mutation published core effects") }
	} else {
		wantMeta := before.Meta
		head, err := before.Meta.Head.Uint64()
		if err != nil { t.Fatal(err) }
		wantMeta.Head = model.Natural64(head+1)
		if after.Meta != wantMeta || commit.Revision != after.Meta.Head || len(after.Changes) != len(before.Changes)+1 || !reflect.DeepEqual(before.Changes, after.Changes[:len(before.Changes)]) { t.Fatal("wrong exact history/meta effect") }
		last := after.Changes[len(before.Changes)]
		if last.Revision != after.Meta.Head || last.Branch != model.Natural64(0) || last.Incarnation != before.Meta.Incarnation || last.Kind != model.ChangesetWrite || !reflect.DeepEqual(last.Principal, publicproof.Anonymous) || last.IdempotencyKey != proof.Hash([]byte(label))[:32] { t.Fatal("wrong exact changeset/request identity") }
		if len(last.Changes) != len(changed) { t.Fatal("history has extra or missing record effects") }
		for _, change := range last.Changes {
			if !changed[change.Ref.ID] { t.Fatal("history changed unrelated endpoint") }
		}
		if len(after.Receipts) != len(before.Receipts)+1 { t.Fatal("wrong core receipt count") }
		for key, receipt := range before.Receipts {
			if !reflect.DeepEqual(receipt, after.Receipts[key]) { t.Fatal("retained receipt changed") }
		}
		receipt, ok := after.Receipts[label]
		if !ok || receipt.Pending || !reflect.DeepEqual(receipt.Commit, commit) || !reflect.DeepEqual(receipt.Changeset, last) { t.Fatal("commit/history/receipt binding differs") }
	}
	effectDerived(t, before, after, changed, method, params, started, finished)
	if len(before.Checkpoints) != len(after.Checkpoints) { t.Fatal("checkpoint inventory changed") }
	for key, checkpoint := range before.Checkpoints {
		if len(active) == 0 || key != proof.Namespace {
			if !reflect.DeepEqual(checkpoint, after.Checkpoints[key]) { t.Fatal("unrelated namespace changed", key) }
		}
	}
	if len(active) != 0 { effectProvider(t, before, after, method, params, active); effectProviderReceipt(t, before, after, method, label, params, active[0], commit, started, finished) }
	// Replay must retain the original bytes after closing and reopening the
	// actual SQL handles. The memory row is explicitly process-local evidence.
	h.Reopen()
	proof.Same(t, after, h.Capture())
	replay := h.Call(method, label, params, "")
	if !bytes.Equal(reply.Result, replay.Result) { t.Fatal("reopen/replay changed exact committed result") }
	if len(changed) != 0 && !replay.Meta.Replayed { t.Fatal("durable replay flag absent") }
	proof.Same(t, after, h.Capture())
}

func effectObject(t *testing.T, raw json.RawMessage) map[string]json.RawMessage {
	t.Helper()
	var value map[string]json.RawMessage
	if json.Unmarshal(raw, &value) != nil || value == nil { t.Fatal("invalid retained provider object") }
	return value
}

func effectToken(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil { t.Fatal(err) }
	return raw
}

func effectJSONSame(t *testing.T, want, got any) {
	t.Helper()
	var a, b any
	left, right := effectToken(t, want), effectToken(t, got)
	// Raw metadata tokens remain strings throughout these maps. Unmarshal
	// compares only the surrounding object encoding, never their token bytes.
	if json.Unmarshal(left, &a) != nil || json.Unmarshal(right, &b) != nil || !reflect.DeepEqual(a,b) { t.Fatalf("provider value differs: want %s got %s", left, right) }
}

func effectProvider(t *testing.T, before, after proof.State, method string, params map[string]any, active []model.Ref) {
	t.Helper()
	proof.WorkflowNamespaceEffect(t, before, after)
	oldState, newState := before.Workflow(t), after.Workflow(t)
	oldItems, newItems := effectObject(t, oldState["items"]), effectObject(t, newState["items"])
	if len(oldItems) != len(newItems) { t.Fatal("provider inventory changed before run completion") }
	primary := active[0].ID
	for id, raw := range oldItems {
		want, got := effectObject(t, raw), effectObject(t, newItems[id])
		if id == primary {
			var generation uint64
			if json.Unmarshal(want["generation"], &generation) != nil { t.Fatal("invalid generation") }
			want["generation"] = effectToken(t, generation+1)
		}
		if method == "bead.close" && id == primary {
			want["status"] = effectToken(t, "closed")
			delete(want, "claim")
		}
		if method == "bead.link" || method == "bead.unlink" {
			from, to := params["from"].(model.Ref), params["to"].(model.Ref)
			for key, applies := range map[string]bool{"links": id == from.ID, "inbound": id == to.ID} {
				if !applies { continue }
				if method == "bead.unlink" {
					delete(want, key)
				} else {
					want[key] = effectToken(t, []workflowplugin.RelationView{{From: from.ID, To: to, Kind: params["kind"].(string), Metadata: params["metadata"].(*model.LinkMetadata)}})
				}
			}
		}
		effectJSONSame(t, want, got)
	}
	oldReceipts, newReceipts := effectObject(t, oldState["receipts"]), effectObject(t, newState["receipts"])
	if len(newReceipts) != len(oldReceipts)+1 { t.Fatal("wrong exact provider receipt effect") }
	for key, raw := range oldReceipts {
		if !bytes.Equal(raw, newReceipts[key]) { t.Fatal("retained provider receipt changed") }
	}
	var oldAudit, newAudit []json.RawMessage
	if len(oldState["audit"]) != 0 && json.Unmarshal(oldState["audit"], &oldAudit) != nil { t.Fatal("invalid audit") }
	if json.Unmarshal(newState["audit"], &newAudit) != nil || len(newAudit) != len(oldAudit)+1 { t.Fatal("wrong provider audit delta") }
	for i, raw := range oldAudit {
		if !bytes.Equal(raw, newAudit[i]) { t.Fatal("retained provider audit changed") }
	}
	var audit struct{ Method, ItemID, Key, Digest, RunID string }
	if json.Unmarshal(newAudit[len(oldAudit)], &audit) != nil || audit.Method != method || audit.ItemID != primary || audit.Key == "" || audit.Digest == "" || audit.RunID == "" { t.Fatal("provider audit identity missing") }
	for _, key := range []string{"items", "receipts", "audit"} {
		delete(oldState,key)
		delete(newState,key)
	}
	if !reflect.DeepEqual(oldState,newState) { t.Fatal("unrelated provider state changed") }
}
