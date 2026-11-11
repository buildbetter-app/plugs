package b07positive

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// WorkflowEffect accounts for every field outside the explicitly checked link
// image. Callers independently compare the complete expected edge tuple/token
// set, while this oracle requires the exact item, receipt and audit deltas.
func WorkflowEffect(t *testing.T, before, after State, itemID, title string, methods ...string) {
	t.Helper()
	object := func(raw json.RawMessage) map[string]json.RawMessage {
		t.Helper()
		var value map[string]json.RawMessage
		if err := json.Unmarshal(raw, &value); err != nil || value == nil {
			t.Fatal("invalid provider object", err)
		}
		return value
	}
	oldLogical, newLogical := before.Workflow(t), after.Workflow(t)
	oldItems, newItems := object(oldLogical["items"]), object(newLogical["items"])
	if len(oldItems) != len(newItems) || len(oldItems[itemID]) == 0 || len(newItems[itemID]) == 0 {
		t.Fatal("mixed operation changed the item inventory")
	}
	for id, raw := range oldItems {
		if id != itemID && !bytes.Equal(raw, newItems[id]) {
			t.Fatal("mixed operation changed another item", id)
		}
	}
	oldItem, newItem := object(oldItems[itemID]), object(newItems[itemID])
	var oldGeneration, newGeneration uint64
	if json.Unmarshal(oldItem["generation"], &oldGeneration) != nil || json.Unmarshal(newItem["generation"], &newGeneration) != nil || newGeneration != oldGeneration+uint64(len(methods)) {
		t.Fatal("mixed operations changed the item generation by the wrong amount")
	}
	if title != "" {
		var actual string
		if json.Unmarshal(newItem["title"], &actual) != nil || actual != title {
			t.Fatal("mixed operation lost the exact requested title")
		}
		oldItem["title"] = newItem["title"]
	}
	for _, name := range []string{"generation", "links", "inbound"} {
		delete(oldItem, name)
		delete(newItem, name)
	}
	if !reflect.DeepEqual(oldItem, newItem) {
		t.Fatal("mixed operations changed item fields outside title/generation/relations")
	}
	oldReceipts, newReceipts := object(oldLogical["receipts"]), object(newLogical["receipts"])
	if len(newReceipts) != len(oldReceipts)+len(methods) {
		t.Fatal("mixed operations published the wrong number of provider receipts")
	}
	for key, receipt := range oldReceipts {
		if !bytes.Equal(receipt, newReceipts[key]) {
			t.Fatal("mixed operations changed a retained provider receipt", key)
		}
	}
	var oldAudit, newAudit []json.RawMessage
	// An admitted import has no lifecycle audit yet, and persisted.audit is
	// omitted when empty. Its first public mutation must create the exact
	// ordered entries below; omission is not malformed JSON input.
	if raw, present := oldLogical["audit"]; present && json.Unmarshal(raw, &oldAudit) != nil {
		t.Fatal("invalid retained provider audit")
	}
	if json.Unmarshal(newLogical["audit"], &newAudit) != nil || len(newAudit) != len(oldAudit)+len(methods) {
		t.Fatal("mixed operations lost retained audit or added the wrong audit effects")
	}
	for i, raw := range oldAudit {
		if !bytes.Equal(raw, newAudit[i]) {
			t.Fatal("mixed operations changed a retained audit entry", i)
		}
	}
	for i, raw := range newAudit[len(oldAudit):] {
		var audit struct{ Method, Key, Digest, ItemID, RunID, Project, Plugin, Instance string }
		if json.Unmarshal(raw, &audit) != nil || audit.Method != methods[i] || audit.ItemID != itemID || audit.Plugin != "workflow" || audit.Instance != "default" || audit.Key == "" || audit.Digest == "" {
			t.Fatal("mixed operation audit lost ordered method/provider identity")
		}
		matches := 0
		for key, rawReceipt := range newReceipts {
			if _, retained := oldReceipts[key]; retained {
				continue
			}
			var receipt struct {
				Method, Key, Digest, ItemID, RunID, Project, Plugin, Instance string
				Result                                                        model.Commit
			}
			if json.Unmarshal(rawReceipt, &receipt) != nil {
				t.Fatal("invalid new provider receipt")
			}
			if receipt.Key == audit.Key && receipt.Method == audit.Method && receipt.Digest == audit.Digest && receipt.ItemID == audit.ItemID && receipt.RunID == audit.RunID && receipt.Project == audit.Project && receipt.Plugin == audit.Plugin && receipt.Instance == audit.Instance {
				matches++
				if len(receipt.Result.Results) != 1 || receipt.Result.Results[0].Ref != model.RefByID(itemID, model.Natural64(0)) {
					t.Fatal("new provider receipt lost its actual endpoint")
				}
			}
		}
		if matches != 1 {
			t.Fatal("ordered audit does not identify exactly one new receipt")
		}
	}
	for _, key := range []string{"items", "receipts", "audit"} {
		delete(oldLogical, key)
		delete(newLogical, key)
	}
	if !reflect.DeepEqual(oldLogical, newLogical) {
		t.Fatal("mixed operations changed unrelated logical provider state")
	}
	WorkflowNamespaceEffect(t, before, after)
}

// WorkflowNamespaceEffect accounts for the complete envelope and every
// sibling value. Callers compare workflow/state's exact logical change.
func WorkflowNamespaceEffect(t *testing.T, before, after State) {
	t.Helper()
	object := func(raw json.RawMessage) map[string]json.RawMessage {
		t.Helper()
		if len(raw) == 0 {
			return map[string]json.RawMessage{}
		}
		var value map[string]json.RawMessage
		if json.Unmarshal(raw, &value) != nil || value == nil {
			t.Fatal("invalid namespace object")
		}
		return value
	}
	if after.Checkpoints[Namespace].Generation != before.Checkpoints[Namespace].Generation+1 {
		t.Fatal("expected one complete namespace publication")
	}
	oldNamespace, newNamespace := object(before.Checkpoints[Namespace].Payload), object(after.Checkpoints[Namespace].Payload)
	oldValues, newValues := object(oldNamespace["values"]), object(newNamespace["values"])
	oldValue, newValue := object(oldValues["workflow/state"]), object(newValues["workflow/state"])
	var oldRevision, newRevision uint64
	if len(oldValue) != 0 && json.Unmarshal(oldValue["revision"], &oldRevision) != nil {
		t.Fatal("invalid prior workflow value revision")
	}
	if json.Unmarshal(newValue["revision"], &newRevision) != nil || newRevision != oldRevision+1 || len(newValue["value"]) == 0 {
		t.Fatal("joined batch did not publish one logical namespace revision")
	}
	delete(oldValues, "workflow/state")
	delete(newValues, "workflow/state")
	delete(oldNamespace, "values")
	delete(newNamespace, "values")
	delete(oldValue, "value")
	delete(newValue, "value")
	delete(oldValue, "revision")
	delete(newValue, "revision")
	if !reflect.DeepEqual(oldValues, newValues) || !reflect.DeepEqual(oldNamespace, newNamespace) || !reflect.DeepEqual(oldValue, newValue) {
		t.Fatal("mixed operations changed unrelated namespace values or source origin")
	}
}

// DerivedTitleEffect requires all derived content to remain exact except the
// changed bead's established ready-sort projection, whose revision advances
// with the title patch even when the ordering tuple stays the same.
func DerivedTitleEffect(t *testing.T, before, after State, changed string) {
	t.Helper()
	for table, rows := range before.Derived {
		if table != driver.DerivedSortKey || changed == "" {
			if !reflect.DeepEqual(rows, after.Derived[table]) {
				t.Fatal("mixed title/link operation changed unrelated derived content", table)
			}
			continue
		}
		index := func(rows []driver.DerivedRow) map[string]driver.DerivedRow {
			t.Helper()
			result := map[string]driver.DerivedRow{}
			for _, row := range rows {
				if _, duplicate := result[row.Key]; duplicate {
					t.Fatal("duplicate derived row", row.Key)
				}
				result[row.Key] = row
			}
			return result
		}
		oldRows, newRows := index(rows), index(after.Derived[table])
		key, err := pipeline.ReadySortKeyRowKey(changed)
		if err != nil {
			t.Fatal(err)
		}
		old, oldExists := oldRows[key]
		row, newExists := newRows[key]
		if oldExists && !newExists {
			t.Fatal("mixed title operation lost its ready-sort projection")
		}
		if newExists {
			id, value, err := pipeline.DecodeReadySortKey(row)
			if err != nil || id != changed || value.Revision != after.Meta.Head || row.Index != nil {
				t.Fatal("mixed title operation published an invalid ready-sort projection", err)
			}
			if oldExists {
				_, previous, err := pipeline.DecodeReadySortKey(old)
				if err != nil || previous.Bucket != value.Bucket || previous.Rank != value.Rank || !previous.CreatedAt.Equal(value.CreatedAt) || value.RankedAt.Before(previous.RankedAt) {
					t.Fatal("title-only patch changed the ready-sort ordering tuple", err)
				}
			}
		}
		delete(oldRows, key)
		delete(newRows, key)
		if !reflect.DeepEqual(oldRows, newRows) {
			t.Fatal("mixed title operation changed unrelated sort projections")
		}
	}
}
