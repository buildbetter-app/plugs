package b13memory_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

func (f *fixture) fullcardProvider() *workflowplugin.Provider {
	f.t.Helper()
	store, err := driver.RequireWorkflowCheckpointStore(f.store, f.lease)
	if err != nil {
		f.t.Fatal(err)
	}
	state, err := pluginhost.NewWorkflowRuntimeState(store, f.lease)
	if err != nil {
		f.t.Fatal(err)
	}
	provider, err := workflowplugin.New(state)
	if err != nil {
		f.t.Fatal(err)
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	return provider.WithIdentity(workflowplugin.Alias, "default", meta.Incarnation)
}

func (f *fixture) retainFullcard(label string) {
	f.t.Helper()
	if root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); root != "" {
		checkpoint, err := f.store.(driver.CheckpointStore).LoadCheckpoint(f.ctx, "plugin/workflow/default/durable")
		if err != nil {
			f.t.Fatal(err)
		}
		state := f.state()
		receipts := map[string]any{}
		for scope, receipt := range state.Receipts {
			name := scope.Method + ":" + scope.Key
			if receipt == nil {
				receipts[name] = nil
				continue
			}
			entry := receipt.Changeset
			// A quiet receipt has a zero Changeset with an empty kind. Preserve
			// all fields without passing that empty enum through the wire codec.
			receipts[name] = map[string]any{"RequestHash": receipt.RequestHash, "Commit": receipt.Commit, "ResponsePending": receipt.ResponsePending, "Changeset": map[string]any{"Revision": entry.Revision, "Branch": entry.Branch, "Incarnation": entry.Incarnation, "Kind": string(entry.Kind), "Time": entry.Time, "Principal": map[string]any{"kind": entry.Principal.Kind, "id": entry.Principal.ID, "sponsor": entry.Principal.Sponsor, "caveats": entry.Principal.Caveats}, "Chain": entry.Chain, "IdempotencyKey": entry.IdempotencyKey, "Mutators": entry.Mutators, "Changes": entry.Changes, "Events": entry.Events}}
		}
		data := encode(f.t, map[string]any{"state": map[string]any{"Meta": state.Meta, "Records": state.Records, "Versions": state.Versions, "Changesets": state.Changesets, "Receipts": receipts, "Observations": state.Observations, "Derived": state.Derived, "SQL": state.SQL}, "workflow_checkpoint": checkpoint, "restart_durable": f.engine != "memory", "checkpoint_lifetime": func() driver.CheckpointLifetime {
			if f.engine == "memory" {
				return driver.CheckpointLifetimeProcess
			}
			return driver.CheckpointLifetimeRestart
		}()})
		if err := os.WriteFile(filepath.Join(root, "b13-fullcard-"+f.engine+"-"+label+".json"), data, 0600); err != nil {
			f.t.Fatal(err)
		}
	}
}

func TestB13FullcardLiteralBoundariesAndPublicSearch(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			// Adopted PR467 and the 17:14Z reconciliation admit all three
			// engines, with truthful process versus restart checkpoint lifetime.
			f.fullcardWorkflow = true
			f.bind()
			bead, _ := f.accept("bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "  Search KEY/Ω é  "}}, key("fullcard-durable-decoy"), model.OpCreate)
			beadID := bead.Results[0].Ref.ID
			invokeParams := map[string]any{"template": "b13-plane-decoy", "project": project, "mode": "ephemeral", "checklist": []map[string]any{{"label": "decoy", "title": "  Search KEY/Ω é  ", "type": "task"}}}
			ephemeralID := ""
			invoked := decode[struct {
				Items []struct {
					ID string `json:"id"`
				} `json:"items"`
			}](t, result(t, f.call("x.workflow.invoke", invokeParams, key("fullcard-ephemeral-decoy"))))
			if len(invoked.Items) != 1 || invoked.Items[0].ID == "" {
				t.Fatal("real ephemeral decoy was not created")
			}
			ephemeralID = invoked.Items[0].ID
			capability, err := driver.RequireWorkflowCheckpointStore(f.store, f.lease)
			if err != nil {
				t.Fatal(err)
			}
			lifetime := driver.CheckpointLifetimeRestart
			if engine == "memory" {
				lifetime = driver.CheckpointLifetimeProcess
			}
			if capability.WorkflowCheckpointLifetime() != lifetime {
				t.Fatal("wrong actual checkpoint lifetime")
			}
			persistent := false
			if marker, ok := f.store.(driver.PersistentCheckpointStore); ok {
				persistent = marker.Persistent()
			}
			if persistent != (engine != "memory") {
				t.Fatal("untruthful restart persistence marker")
			}
			checkpoints := f.store.(driver.CheckpointStore)
			checkpoint, err := checkpoints.LoadCheckpoint(f.ctx, "plugin/workflow/default/durable")
			if err != nil || checkpoint.Generation == 0 || len(checkpoint.Payload) == 0 {
				t.Fatal("actual workflow checkpoint missing", err)
			}
			f.seedExistingRows()
			planes := f.state()
			filled, _ := f.accept("memory.save", map[string]any{"key": "empty legacy ✓", "content": "Previously empty, now replaced"}, key("fullcard-fill-empty"), model.OpPatch)
			if filled.Results[0].Ref.ID != "b13-empty" {
				t.Fatal("existing empty replacement changed identity")
			}
			want := map[string]memoryValue{"b13-empty": {ID: "b13-empty", Key: "empty legacy ✓", Content: "Previously empty, now replaced", Revision: filled.Revision}}
			liveProof := func(phase string) {
				t.Helper()
				before := f.state()
				read := decode[struct {
					Bead struct {
						ID, Project, Status, Plane, Kind, Plugin string
						Fields                                   map[string]any `json:"fields"`
					} `json:"bead"`
				}](t, result(t, f.call("bead.get", map[string]any{"id": ref(ephemeralID)}, "")))
				active := read.Bead
				if active.ID != ephemeralID || active.Project != project || active.Plane != "ephemeral" || active.Plugin != workflowplugin.Alias || active.Kind != workflowplugin.ItemKind || active.Fields["title"] != "  Search KEY/Ω é  " || active.Status != "open" {
					t.Fatalf("actual populated live workflow decoy lost: %+v", active)
				}
				point := memoryRow(t, result(t, f.call("memory.get", map[string]any{"id": "b13-empty"}, "")))
				if !reflect.DeepEqual(point, want["b13-empty"]) {
					t.Fatal("live-plane memory point differs from literal")
				}
				for _, query := range []string{"", "pReViOuSlY eMpTy"} {
					f.search(query, "b13-empty")
				}
				f.search("sEaRcH kEy/ω É") // Both ordinary and active titles match; neither is memory.
				brief := decode[struct {
					Memories []json.RawMessage `json:"memories"`
				}](t, result(t, f.call("agent.brief", map[string]any{"budgetTokens": 2000}, "")))
				if got := memoryRows(t, brief.Memories); !reflect.DeepEqual(got, []memoryValue{want["b13-empty"]}) {
					t.Fatalf("live decoy leaked into normal memory brief: %+v", got)
				}
				f.refuse("memory.get", map[string]any{"id": ephemeralID}, "", "not_found")
				f.preserveOtherPlanes(planes, beadID)
				gotCheckpoint, e := f.store.(driver.CheckpointStore).LoadCheckpoint(f.ctx, "plugin/workflow/default/durable")
				if e != nil || !reflect.DeepEqual(gotCheckpoint, checkpoint) {
					t.Fatal("memory work changed live workflow state", e)
				}
				requireSame(t, before, f.state())
				f.retainFullcard(phase)
			}
			liveProof("live-decoy-with-nonempty-memory")
			f.reopen() // SQL closes all handles and reopens the identical DSN; memory rebinds.
			liveProof("live-decoy-with-nonempty-memory-reopened")
			result(t, f.call("bead.close", map[string]any{"id": ref(ephemeralID), "reason": "B13 closed public decoy"}, key("fullcard-close-decoy")))
			checkpoint, err = f.store.(driver.CheckpointStore).LoadCheckpoint(f.ctx, "plugin/workflow/default/durable")
			if err != nil {
				t.Fatal(err)
			}
			planes = f.state()
			verify := func() {
				f.verifyConnected(want, beadID)
				f.preserveOtherPlanes(planes, beadID)
				got, err := f.store.(driver.CheckpointStore).LoadCheckpoint(f.ctx, "plugin/workflow/default/durable")
				if err != nil || !reflect.DeepEqual(got, checkpoint) {
					t.Fatal("memory operation changed real ephemeral checkpoint", err)
				}
				for _, id := range []string{beadID, ephemeralID, "b13-setting", "b13-generic-setting"} {
					if id == "" {
						continue
					}
					f.refuse("memory.get", map[string]any{"id": id}, "", "not_found")
				}
			}
			verify()
			for i, vector := range []struct{ name, content, key string }{
				{"exact-eight", "Alpha bravo charlie delta echo foxtrot golf hotel", "alpha-bravo-charlie-delta-echo-foxtrot-golf-hotel"},
				{"exact-nine", "India juliet kilo lima mike november oscar papa QUEBEC", "india-juliet-kilo-lima-mike-november-oscar-papa"},
				{"over-ten", "One two three four five six seven eight NINE ten ELEVEN", "one-two-three-four-five-six-seven-eight"},
				{"midword-cut", "123456789012345678901234567890123456789012345678901234567-ABCDE", "123456789012345678901234567890123456789012345678901234567-ab"},
				{"separator-at-cut", "12345678901234567890123456789012345678901234567890123456789 tail", "12345678901234567890123456789012345678901234567890123456789"},
			} {
				t.Run(vector.name, func(t *testing.T) {
					commit, _ := f.accept("memory.save", map[string]any{"content": vector.content}, key(fmt.Sprintf("fullcard-vector-%d", i)), model.OpCreate)
					id := commit.Results[0].Ref.ID
					want[id] = memoryValue{ID: id, Key: vector.key, Content: vector.content, Revision: commit.Revision}
					verify()
				})
			}
			for i, vector := range []struct{ key, content string }{
				{"  Search KEY/Ω é  ", " \tVerBaTiM value-only\n東京  "},
				{"Search KEY/Ω é", "trimmed neighbor"},
				{"Unicode é", "composed"},
				{"Unicode e\u0301", "decomposed"},
				{"explicit punctuation", "!!!"},
				{"explicit nonASCII", "東京"},
			} {
				commit, _ := f.accept("memory.save", map[string]any{"key": vector.key, "content": vector.content}, key(fmt.Sprintf("fullcard-explicit-%d", i)), model.OpCreate)
				id := commit.Results[0].Ref.ID
				want[id] = memoryValue{ID: id, Key: vector.key, Content: vector.content, Revision: commit.Revision}
			}
			verify()
			var spaced, trimmed string
			for id, value := range want {
				if value.Key == "  Search KEY/Ω é  " {
					spaced = id
				}
				if value.Key == "Search KEY/Ω é" {
					trimmed = id
				}
			}
			search := func(oldValue, newValue bool) {
				f.search("sEaRcH kEy/ω É", spaced, trimmed)
				if oldValue {
					f.search("vErBaTiM VaLuE-oNlY", spaced)
				} else {
					f.search("vErBaTiM VaLuE-oNlY")
				}
				if newValue {
					f.search("rEpLaCeMeNt VaLuE-oNlY", spaced)
				} else {
					f.search("rEpLaCeMeNt VaLuE-oNlY")
				}
				f.search("CONFIG-ONLY-DECOY")
				f.search("nO sUcH mEmOrY")
				verify()
			}
			search(true, false)
			f.retainFullcard("created")
			f.reopen()
			search(true, false)
			f.retainFullcard("created-reopened")
			replaceParams := map[string]any{"key": "  Search KEY/Ω é  ", "content": "Replacement VALUE-only"}
			replaced, replaceReply := f.accept("memory.save", replaceParams, key("fullcard-replace"), model.OpPatch)
			want[spaced] = memoryValue{ID: spaced, Key: "  Search KEY/Ω é  ", Content: "Replacement VALUE-only", Revision: replaced.Revision}
			search(false, true)
			f.retainFullcard("replaced")
			f.reopen()
			search(false, true)
			f.retainFullcard("replaced-reopened")
			// A same-value save reports existence, stores a quiet receipt, and
			// preserves every graph row/version/log. Replay preserves all SQL.
			beforeNoop := f.state()
			noopReply := f.call("memory.save", replaceParams, key("fullcard-same-value"))
			noopCommit := decode[model.Commit](t, result(t, noopReply))
			if len(noopCommit.Results) != 1 || noopCommit.Results[0].Replaced == nil || !*noopCommit.Results[0].Replaced {
				t.Fatal("same-value save must report replaced:true")
			}
			afterNoop := f.state()
			if !reflect.DeepEqual(beforeNoop.Records, afterNoop.Records) || !reflect.DeepEqual(beforeNoop.Versions, afterNoop.Versions) || !reflect.DeepEqual(beforeNoop.Changesets, afterNoop.Changesets) || beforeNoop.Meta != afterNoop.Meta {
				t.Fatal("same-value save published graph history")
			}
			f.replay("memory.save", replaceParams, key("fullcard-same-value"), noopReply)
			for i, params := range []map[string]any{
				{"content": ""}, {"content": " \t\n"}, {"content": "!!! ... ---"}, {"content": "東京"},
				{"key": "\u2003\t", "content": "valid"}, {"key": "  Search KEY/Ω é  ", "content": " \t"},
			} {
				f.refuse("memory.save", params, key(fmt.Sprintf("fullcard-refuse-%d", i)), "invalid_params")
			}
			f.accept("memory.delete", map[string]any{"id": spaced}, key("fullcard-forget"), model.OpDelete)
			delete(want, spaced)
			f.search("sEaRcH kEy/ω É", trimmed)
			f.search("rEpLaCeMeNt VaLuE-oNlY")
			verify()
			f.retainFullcard("forgotten")
			f.reopen()
			f.search("sEaRcH kEy/ω É", trimmed)
			f.search("rEpLaCeMeNt VaLuE-oNlY")
			verify()
			f.retainFullcard("forgotten-reopened")
			// The stored true receipt survives effective absence. Re-remember is
			// a fresh absent-create false, whose replay stays false after delete.
			f.replay("memory.save", replaceParams, key("fullcard-replace"), replaceReply)
			verify()
			recreateParams := map[string]any{"key": "  Search KEY/Ω é  ", "content": "Recreated exact value"}
			recreated, recreateReply := f.accept("memory.save", recreateParams, key("fullcard-recreate"), model.OpCreate)
			newID := recreated.Results[0].Ref.ID
			want[newID] = memoryValue{ID: newID, Key: "  Search KEY/Ω é  ", Content: "Recreated exact value", Revision: recreated.Revision}
			verify()
			f.replay("memory.save", recreateParams, key("fullcard-recreate"), recreateReply)
			verify() // Effective presence is now true; the original result stays false.
			f.retainFullcard("recreate-false-replay-while-present")
			f.reopen()
			verify()
			f.accept("memory.delete", map[string]any{"id": newID}, key("fullcard-recreate-delete"), model.OpDelete)
			delete(want, newID)
			f.replay("memory.save", recreateParams, key("fullcard-recreate"), recreateReply)
			verify()
			f.retainFullcard("recreate-false-replay-after-delete")
		})
	}
}
