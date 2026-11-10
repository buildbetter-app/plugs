package converterroundtrip

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This oracle walks raw JSON leaves with a work queue, independently of the
// product's classifier and recursive counter. It never reads Projection.
func rollbackPhysicalFieldCount(t *testing.T, raw []byte) int {
	t.Helper()
	pending, fields := [][]byte{raw}, 0
	for len(pending) != 0 {
		data := bytes.TrimSpace(pending[0])
		pending = pending[1:]
		switch {
		case len(data) == 0:
			t.Fatal("missing physical JSON field")
		case data[0] == '{':
			var children map[string]json.RawMessage
			if err := json.Unmarshal(data, &children); err != nil {
				t.Fatal(err)
			}
			if len(children) == 0 {
				fields++
			}
			for _, child := range children {
				pending = append(pending, child)
			}
		case data[0] == '[':
			var children []json.RawMessage
			if err := json.Unmarshal(data, &children); err != nil {
				t.Fatal(err)
			}
			if len(children) == 0 {
				fields++
			}
			for _, child := range children {
				pending = append(pending, child)
			}
		default:
			if !json.Valid(data) {
				t.Fatal("invalid physical scalar")
			}
			fields++
		}
	}
	return fields
}

func rollbackAssertLossAccounting(t *testing.T, report reverseconverter.Report, physical []rollbackNamespaceRow, core []driver.Record, ephemeral bool) {
	t.Helper()
	expectedCore := map[string][2]int{}
	for _, row := range core {
		if row.Kind != "instance_config" && row.Kind != "restore-archive" && row.Kind != "beads-archive" {
			continue
		}
		count := expectedCore[row.Kind]
		count[0]++
		count[1] += rollbackPhysicalFieldCount(t, row.Body)
		expectedCore[row.Kind] = count
	}
	if len(report.CoreKindLosses) != len(expectedCore) {
		t.Fatal("incomplete product core-kind inventory", report.CoreKindLosses, expectedCore)
	}
	for _, loss := range report.CoreKindLosses {
		disposition := "retained_control_state"
		if loss.Kind == "beads-archive" {
			disposition = "retained_source_provenance"
		}
		if expectedCore[loss.Kind] != [2]int{loss.RecordCount, loss.FieldInstances} || loss.Disposition != disposition || loss.Family != "" {
			t.Fatal("incorrect product core-kind accounting", loss, expectedCore)
		}
		delete(expectedCore, loss.Kind)
	}
	if len(expectedCore) != 0 || len(report.NamespaceLosses) != len(physical) {
		t.Fatal("incomplete physical loss census", report)
	}
	byKey := map[string]reverseconverter.NamespaceLoss{}
	for _, ns := range report.NamespaceLosses {
		if _, duplicate := byKey[ns.Namespace]; duplicate {
			t.Fatal("duplicate namespace report")
		}
		byKey[ns.Namespace] = ns
	}
	for _, ns := range physical {
		actual, ok := byKey[ns.Key]
		if !ok {
			t.Fatal("unreported physical namespace", ns.Key)
		}
		generation, err := strconv.ParseUint(ns.Generation, 10, 64)
		if err != nil || actual.Generation != generation || actual.PayloadBytes != len(ns.Payload) {
			t.Fatal("loss report not bound to physical checkpoint", actual, err)
		}
		var envelope struct {
			Values map[string]struct {
				Revision json.RawMessage            `json:"revision"`
				Value    map[string]json.RawMessage `json:"value"`
			} `json:"values"`
			Origin json.RawMessage `json:"sourceOrigin"`
		}
		if err := json.Unmarshal(ns.Payload, &envelope); err != nil {
			t.Fatal(err)
		}
		if len(actual.Values) != len(envelope.Values) {
			t.Fatal("unreported physical value key", ns.Key)
		}
		wrapperFields := 0
		if len(envelope.Origin) != 0 {
			wrapperFields += rollbackPhysicalFieldCount(t, envelope.Origin)
		}
		for key, value := range envelope.Values {
			wrapperFields += rollbackPhysicalFieldCount(t, value.Revision)
			var state []reverseconverter.StateLoss
			for _, entry := range actual.Values {
				if entry.Key == key {
					state = entry.State
				}
			}
			if len(state) == 0 {
				t.Fatal("missing value state", key)
			}
			expected := map[string][2]int{}
			for family, raw := range value.Value {
				families := map[string]json.RawMessage{family: raw}
				if family == "source" {
					var source map[string]json.RawMessage
					if err := json.Unmarshal(raw, &source); err != nil {
						t.Fatal(err)
					}
					families = map[string]json.RawMessage{}
					for name, body := range source {
						families["source."+name] = body
					}
				}
				for name, raw := range families {
					var records []json.RawMessage
					if raw[0] == '{' {
						var rows map[string]json.RawMessage
						if err := json.Unmarshal(raw, &rows); err != nil {
							t.Fatal(err)
						}
						for _, body := range rows {
							records = append(records, body)
						}
					} else if raw[0] == '[' {
						if err := json.Unmarshal(raw, &records); err != nil {
							t.Fatal(err)
						}
					} else {
						records = []json.RawMessage{raw}
					}
					count := [2]int{len(records), 0}
					for _, body := range records {
						count[1] += rollbackPhysicalFieldCount(t, body)
					}
					if count[0] != 0 {
						expected[name] = count
					}
				}
			}
			if len(state) != len(expected) {
				t.Fatal("incomplete product persisted-family inventory", ns.Key, state, expected)
			}
			for _, loss := range state {
				if expected[loss.Family] != [2]int{loss.RecordCount, loss.FieldInstances} {
					t.Fatal("incorrect product family counts", ns.Key, loss, expected)
				}
				disposition := "unbound_native_state"
				if ns.Key == "plugin/workflow/default/durable" {
					disposition = "retained_control_state"
				}
				if strings.HasPrefix(loss.Family, "source.") {
					disposition = "retained_source_provenance"
				}
				if loss.Family == "items" {
					if loss.Kind != "workflow.item" {
						t.Fatal("native public kind changed", loss)
					}
					if ns.Key == "plugin/workflow/default/durable" {
						disposition = "selected_source_content"
						if ephemeral {
							disposition = "default_excluded_source_content"
						}
						if loss.SourceTargetKind != "workflow:item" {
							t.Fatal("source TargetKind changed", loss)
						}
					} else if loss.SourceTargetKind != "" {
						t.Fatal("invented native source kind", loss)
					}
				} else if loss.Kind != "" || loss.SourceTargetKind != "" {
					t.Fatal("family was mislabeled as a content kind", loss)
				}
				if loss.Disposition != disposition {
					t.Fatal("incorrect source/native/control disposition", ns.Key, loss, disposition)
				}
				delete(expected, loss.Family)
			}
			if len(expected) != 0 {
				t.Fatal("unreported family", expected)
			}
		}
		if actual.WrapperFieldInstances != wrapperFields {
			t.Fatal("wrapper and logical row units were conflated", actual, wrapperFields)
		}
	}
	wantKinds := []string{"beads-archive", "instance_config", "restore-archive"}
	if len(physical) > 1 {
		wantKinds = append(wantKinds, "workflow.item")
	}
	if !reflect.DeepEqual(report.PlugOnlyKinds, wantKinds) {
		t.Fatal("actual Plug-only kind inventory differs", report.PlugOnlyKinds, wantKinds)
	}
}

func rollbackAssertImportedAllocatorImages(t *testing.T, core []driver.Record) {
	t.Helper()
	// Main's admitted adaptive allocator replaces four six-field random images:
	// two eight-field demo images and two seven-field empty images add six leaves.
	// Pin each complete physical image independently of the allocator constructor
	// and the loss report before asserting the new fixture total.
	demo := json.RawMessage(`{"prefix":"demo","id_mode":"hash","hash_length":8,"min_hash_length":3,"max_hash_length":8,"max_collision_prob":0.25,"last_id":8,"child_counters":{"demo-7":4}}`)
	empty := json.RawMessage(`{"prefix":"empty","id_mode":"hash","hash_length":8,"min_hash_length":3,"max_hash_length":8,"max_collision_prob":0.25,"last_id":1}`)
	want := map[string]json.RawMessage{
		"binding/demo": demo, "binding/empty": empty,
		importProjectRecordID("demo"): demo, importProjectRecordID("empty"): empty,
	}
	got := map[string]json.RawMessage{}
	for _, row := range core {
		switch row.Ref.ID {
		case importBindingRecordID:
			var binding struct {
				Projects []struct {
					Source struct {
						Prefix string `json:"prefix"`
					} `json:"sourceAllocator"`
					Target json.RawMessage `json:"targetAllocator"`
				} `json:"projects"`
			}
			decode(t, row.Body, &binding)
			for _, project := range binding.Projects {
				key := "binding/" + project.Source.Prefix
				if _, duplicate := got[key]; duplicate {
					t.Fatal("duplicate imported allocator image", key)
				}
				got[key] = project.Target
			}
		case importProjectRecordID("demo"), importProjectRecordID("empty"):
			var metadata struct {
				Target json.RawMessage `json:"targetAllocator"`
			}
			decode(t, row.Body, &metadata)
			if _, duplicate := got[row.Ref.ID]; duplicate {
				t.Fatal("duplicate project allocator image", row.Ref.ID)
			}
			got[row.Ref.ID] = metadata.Target
		}
	}
	if len(got) != len(want) {
		t.Fatal("fixture imported allocator census differs", got, want)
	}
	for key, expected := range want {
		actual, present := got[key]
		if !present {
			t.Fatal("missing imported allocator image", key)
		}
		var actualFields, expectedFields map[string]any
		decode(t, actual, &actualFields)
		decode(t, expected, &expectedFields)
		if !reflect.DeepEqual(actualFields, expectedFields) {
			t.Fatalf("fixture imported allocator image %s differs: got %s want %s", key, actual, expected)
		}
	}
}

func TestJointSourceReverseReportsNamespaceLossesAcrossThreeStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, ephemeral := range []bool{false, true} {
			t.Run(engine+"/ephemeral="+strconv.FormatBool(ephemeral), func(t *testing.T) {
				ctx := context.Background()
				var dsn string
				store, lease, _ := openIdentityTarget(t, engine, func(value string) { dsn = value })
				data := sourceReverseFixture(t, ephemeral)
				profile := sourceReverseProfile(t, data, store, lease, ephemeral)
				restored, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
				if err != nil {
					t.Fatal(err)
				}
				_, server := sourcePublicService(t, store, lease, profile, restored, &sourcePublicPolicy{})
				edit := model.ChangeSet{Ops: []model.Op{
					{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID("demo-9", model.Natural64(0)), Fields: map[string]any{"title": "rollback current active", "labels": []string{"rollback-active"}}}},
					{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID("demo-8", model.Natural64(0)), Fields: map[string]any{"title": "rollback current durable"}}},
				}}
				sourcePublicCall(t, server, profile.Admission.Actor, "tx.apply", 9201, map[string]any{"changeSet": edit}, "")
				if engine != "memory" {
					rollbackPopulateNativeNamespace(t, ctx, store, lease, profile.Admission.Actor, restored.Restored.Incarnation, func(any) {})
				} // Memory truthfully cannot invoke new ephemeral workflows.
				var physical []rollbackNamespaceRow
				if engine == "memory" {
					key := restored.SourcePlanes.Providers[0].Provider.Namespace
					cp, err := store.(driver.CheckpointStore).LoadCheckpoint(ctx, key)
					if err != nil {
						t.Fatal(err)
					}
					physical = []rollbackNamespaceRow{{Key: key, Generation: strconv.FormatUint(cp.Generation, 10), Payload: cp.Payload}}
				} else if engine == "sqlite" {
					physical = rollbackReadNamespaces(t, ctx, dsn)
				} else {
					db, err := sql.Open("pgx", dsn)
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()
					rows, err := db.QueryContext(ctx, "SELECT key, generation::text, payload FROM plug_postgres.checkpoints ORDER BY key")
					if err != nil {
						t.Fatal(err)
					}
					for rows.Next() {
						var row rollbackNamespaceRow
						if err := rows.Scan(&row.Key, &row.Generation, &row.Payload); err != nil {
							t.Fatal(err)
						}
						physical = append(physical, row)
					}
					if err := rows.Err(); err != nil {
						t.Fatal(err)
					}
					if err := rows.Close(); err != nil {
						t.Fatal(err)
					}
				}
				reader, err := store.Read(ctx, driver.View{Incarnation: restored.Restored.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				page, err := reader.Query(ctx, driver.Query{Limit: 1000})
				if err != nil || page.NextCursor != nil {
					t.Fatal("incomplete independent core census", err)
				}
				if err := reader.Close(); err != nil {
					t.Fatal(err)
				}
				converted, err := ReverseRestored(ctx, store)
				if err != nil {
					t.Fatal(err)
				}
				rollbackAssertLossAccounting(t, converted.Result.Report, physical, page.Items, ephemeral)
				rollbackAssertImportedAllocatorImages(t, page.Items)
				wantCore := []reverseconverter.StateLoss{
					{Kind: "beads-archive", RecordCount: 108, FieldInstances: 1067, Disposition: "retained_source_provenance"},
					{Kind: "instance_config", RecordCount: 4, FieldInstances: 125, Disposition: "retained_control_state"},
					{Kind: "restore-archive", RecordCount: 1, FieldInstances: 20, Disposition: "retained_control_state"},
				}
				if !reflect.DeepEqual(converted.Result.Report.CoreKindLosses, wantCore) {
					t.Fatal("fixture core-control field conservation differs", converted.Result.Report.CoreKindLosses, wantCore)
				}
				all, err := ReverseRestoredWithOptions(ctx, store, reverseconverter.Options{All: true})
				if err != nil {
					t.Fatal(err)
				}
				rollbackAssertLossAccounting(t, all.Result.Report, physical, page.Items, false)
				for _, ns := range converted.Result.Report.NamespaceLosses {
					fields := 0
					for _, value := range ns.Values {
						for _, row := range value.State {
							fields += row.FieldInstances
						}
					}
					// Preserve the predecessor census, adding exactly the two
					// opaque token strings on each of the two imported relations.
					want := 5321 + 2*2
					if ns.Namespace == "plugin/workflow/rollback-native/durable" {
						want = 359
					}
					if fields != want {
						t.Fatal("fixture field conservation differs", ns.Namespace, fields, want)
					}
				}
			})
		}
	}
}

func TestCheckpointInventoryPinsCompleteSetAcrossThreeStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			store, lease, _ := openIdentityTarget(t, engine)
			writer := store.(driver.CheckpointStore)
			for _, key := range []string{"inventory/z", "inventory/a"} {
				if _, err := writer.SaveCheckpoint(ctx, lease, key, 0, []byte(`{"version":1}`)); err != nil {
					t.Fatal(err)
				}
			}
			meta, err := store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			pinned, err := store.(driver.CheckpointInventoryStore).ReadCheckpointInventory(ctx, driver.View{Incarnation: meta.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer pinned.Close()
			keys, err := pinned.CheckpointKeys()
			if err != nil || !reflect.DeepEqual(keys, []string{"inventory/a", "inventory/z"}) {
				t.Fatal("incomplete key inventory", keys, err)
			}
			keys[0] = "changed caller copy"
			for _, key := range []string{"inventory/a", "inventory/later"} {
				expected := uint64(0)
				if key == "inventory/a" {
					expected = 1
				}
				if _, err := writer.SaveCheckpoint(ctx, lease, key, expected, []byte(`{"version":2}`)); err != nil {
					t.Fatal(err)
				}
			}
			keys, err = pinned.CheckpointKeys()
			if err != nil || !reflect.DeepEqual(keys, []string{"inventory/a", "inventory/z"}) {
				t.Fatal("key inventory mixed store generations", keys, err)
			}
			cp, err := pinned.SourcePlaneCheckpoint("inventory/a")
			if err != nil || cp.Generation != 1 || string(cp.Payload) != `{"version":1}` {
				t.Fatal("checkpoint mixed store generations", cp, err)
			}
			cp.Payload[0] = '!'
			cp, err = pinned.SourcePlaneCheckpoint("inventory/a")
			if err != nil || string(cp.Payload) != `{"version":1}` {
				t.Fatal("caller mutated pinned checkpoint", cp, err)
			}
			if _, err := pinned.SourcePlaneCheckpoint("inventory/later"); !errors.Is(err, driver.ErrUnsupported) {
				t.Fatal("later namespace leaked into pinned snapshot", err)
			}
			if err := pinned.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := pinned.CheckpointKeys(); !errors.Is(err, driver.ErrTxClosed) {
				t.Fatal("closed inventory remained readable", err)
			}
			latest, err := store.(driver.CheckpointInventoryStore).ReadCheckpointInventory(ctx, driver.View{Incarnation: meta.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer latest.Close()
			keys, err = latest.CheckpointKeys()
			if err != nil || !sort.StringsAreSorted(keys) || len(keys) != 3 {
				t.Fatal("fresh inventory omitted new namespace", keys, err)
			}
		})
	}
}

func TestReverseRefusesUnavailableCheckpointInventory(t *testing.T) {
	store, _, _ := openIdentityTarget(t, "memory")
	// Hiding optional capabilities must not be mistaken for an empty namespace set.
	converted, err := ReverseRestored(context.Background(), struct{ driver.Store }{store})
	var refused ReverseRefusal
	if !errors.As(err, &refused) || refused.Reason != "checkpoint_inventory_unavailable" || len(converted.Result.JSONL) != 0 {
		t.Fatal("missing inventory silently accepted", converted, err)
	}
}
