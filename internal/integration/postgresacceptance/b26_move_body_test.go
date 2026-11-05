package postgresacceptance

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// Snapshot every application table, preserving the complete column census,
// SQL NULL, actual SQL value types and full bytes. No lock/receipt/checkpoint
// table is removed to make a refusal pass.
func b26CompleteSQL(t *testing.T, db *sql.DB, engine string) map[string][]string {
	t.Helper()
	query := "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
	if engine == "postgres" {
		query = "SELECT table_name FROM information_schema.tables WHERE table_schema='plug_postgres' AND table_type='BASE TABLE' ORDER BY table_name"
	}
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		if name == "" || strings.ContainsAny(name, "\"\x00") {
			t.Fatal("unsafe application table")
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(tables) < 10 {
		t.Fatalf("incomplete application-table census: %v", tables)
	}
	result := map[string][]string{}
	for _, table := range tables {
		qualified := `"` + table + `"`
		if engine == "postgres" {
			qualified = `plug_postgres.` + qualified
		}
		rows, err := db.QueryContext(context.Background(), "SELECT * FROM "+qualified)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		header, err := json.Marshal(columns)
		if err != nil {
			t.Fatal(err)
		}
		images := []string{}
		for rows.Next() {
			values, ptrs := make([]any, len(columns)), make([]any, len(columns))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			cells := make([]map[string]any, len(columns))
			for i, value := range values {
				cells[i] = map[string]any{"column": columns[i], "type": fmt.Sprintf("%T", value), "value": value}
			}
			body, err := json.Marshal(cells)
			if err != nil {
				t.Fatal(err)
			}
			images = append(images, string(body))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		sort.Strings(images)
		result[table] = append([]string{string(header)}, images...)
	}
	return result
}

func b26SQLRefusal(t *testing.T, h *b26HTTP, db *sql.DB, engine, method string, params map[string]any, label, code string) {
	t.Helper()
	before := b26CompleteSQL(t, db, engine)
	b26AssertRefusalUnchanged(t, h, method, params, label, code)
	after := b26CompleteSQL(t, db, engine)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("%s changed complete SQL state: before=%v after=%v", method, before, after)
	}
}

func b26MixedParentService(t *testing.T, h *b26HTTP, options ...pipeline.Option) *service.Service {
	t.Helper()
	metadata, err := h.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	provider, err := workflowplugin.New(pluginhost.NewDurableState(h.store.(driver.PersistentCheckpointStore), h.lease, "workflow", "default"))
	if err != nil {
		t.Fatal(err)
	}
	ontology := service.NewOntology(nil)
	writer := pipeline.New(h.store, h.lease, append([]pipeline.Option{pipeline.WithOntology(ontology)}, options...)...)
	return service.New(h.store, h.lease, writer, service.WithOntology(ontology), service.WithWorkflow(provider.WithIdentity("workflow", "default", metadata.Incarnation)))
}

func b26MixedParentCode(t *testing.T, err error, want model.ErrorCode) {
	t.Helper()
	var actual *model.Error
	if !errors.As(err, &actual) || actual.Data.Code != want {
		t.Fatalf("expected %s, got %v", want, err)
	}
}

func TestB26PublicMoveMetadataPrecisionSQLite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "move-metadata-precision.db")
	open := func() driver.Store {
		store, err := (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: path})
		if err != nil {
			t.Fatal(err)
		}
		return store
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	b26PublicMoveMetadataPrecision(t, open, db, "sqlite")
}

func TestB26PublicMoveMetadataPrecisionPostgreSQL(t *testing.T) {
	dsn := freshDatabase(t)
	open := func() driver.Store { return openStore(t, dsn) }
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	b26PublicMoveMetadataPrecision(t, open, db, "postgres")
}

// Exercise the public move and actual persisted checkpoint/receipt, keeping an
// independent JSON-number oracle rather than decoding through float64 again.
func b26PublicMoveMetadataPrecision(t *testing.T, open func() driver.Store, db *sql.DB, engine string) {
	h := b26Host(t, open())
	canonical := func(raw json.RawMessage) []byte {
		t.Helper()
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var value any
		if err := decoder.Decode(&value); err != nil {
			t.Fatal(err)
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			t.Fatalf("unexpected trailing JSON: %v", err)
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	const metadataKey = "user.precision"
	value := json.RawMessage(`{"large":9007199254740993,"negative":-9007199254740993,"nested":[null,{"largest":9223372036854775807,"text":"9007199254740993"}],"zero":0}`)
	want := canonical(json.RawMessage(`{"` + metadataKey + `":` + string(value) + `}`))
	assertMetadata := func(raw json.RawMessage) {
		t.Helper()
		if got := canonical(raw); !bytes.Equal(got, want) {
			t.Fatalf("public move changed complete metadata: want=%s got=%s", want, got)
		}
	}
	assertGet := func(id string) {
		t.Helper()
		var result struct {
			Bead struct {
				Metadata json.RawMessage `json:"metadata"`
			} `json:"bead"`
		}
		if err := json.Unmarshal(b26OK(t, h.call(t, "bead.get", map[string]any{"id": ref(id)}, "")), &result); err != nil {
			t.Fatal(err)
		}
		assertMetadata(result.Bead.Metadata)
	}
	source := h.create(t, "precision-source")
	set := model.Op{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Target: ref(source), Key: metadataKey, Value: value}}
	b26OK(t, h.call(t, "tx.apply", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{set}}}, "precision-source-metadata"))
	assertGet(source)
	params := map[string]any{"source": ref(source), "template": "precision", "label": "precision-item"}
	first := b26OK(t, h.call(t, "x.workflow.run.demote", params, "precision-demote"))
	var demoted struct {
		Item struct {
			ID       string          `json:"id"`
			Metadata json.RawMessage `json:"metadata"`
		} `json:"item"`
	}
	if err := json.Unmarshal(first, &demoted); err != nil || demoted.Item.ID == "" {
		t.Fatalf("demotion omitted its item: %s %v", first, err)
	}
	assertMetadata(demoted.Item.Metadata)
	assertGet(demoted.Item.ID)
	b26NoVersion(t, h.store, demoted.Item.ID)
	checkpoint := workflowCheckpoint(t, h.store.(driver.CheckpointStore))
	var stored struct {
		Metadata json.RawMessage `json:"metadata"`
	}
	if err := json.Unmarshal(checkpoint.Items[demoted.Item.ID], &stored); err != nil {
		t.Fatal(err)
	}
	assertMetadata(stored.Metadata)
	var actualReceipt json.RawMessage
	for _, receipt := range checkpoint.Receipts {
		if receipt.Method == "x.workflow.run.demote" && receipt.Key == key("precision-demote") {
			if actualReceipt != nil {
				t.Fatal("duplicate demotion receipt")
			}
			actualReceipt = receipt.Result
		}
	}
	if len(actualReceipt) == 0 || !bytes.Equal(canonical(first), canonical(actualReceipt)) {
		t.Fatalf("first result differs from actual stored receipt: first=%s receipt=%s", first, actualReceipt)
	}
	h.server.Close()
	if err := h.lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := h.store.Close(); err != nil {
		t.Fatal(err)
	}
	h = b26Host(t, open())
	before := b26CompleteSQL(t, db, engine)
	assertGet(demoted.Item.ID)
	replay := b26OK(t, h.call(t, "x.workflow.run.demote", params, "precision-demote"))
	if !bytes.Equal(canonical(replay), canonical(actualReceipt)) || !reflect.DeepEqual(before, b26CompleteSQL(t, db, engine)) {
		t.Fatal("reopen/read/replay changed the complete stored result or SQL state")
	}
	changed := map[string]any{"source": ref(source), "template": "different-template", "label": "precision-item"}
	b26SQLRefusal(t, h, db, engine, "x.workflow.run.demote", changed, "precision-demote", "idempotency_key_reused")
	target := h.create(t, "precision-target")
	promote := map[string]any{"item": ref(demoted.Item.ID), "target": ref(target)}
	promoted := b26OK(t, h.call(t, "x.workflow.run.promote", promote, "precision-promote"))
	assertGet(target)
	b26NoVersion(t, h.store, demoted.Item.ID)
	actualReceipt = nil
	for _, receipt := range workflowCheckpoint(t, h.store.(driver.CheckpointStore)).Receipts {
		if receipt.Method == "x.workflow.run.promote" && receipt.Key == key("precision-promote") {
			if actualReceipt != nil {
				t.Fatal("duplicate promotion receipt")
			}
			actualReceipt = receipt.Result
		}
	}
	if len(actualReceipt) == 0 || !bytes.Equal(canonical(promoted), canonical(actualReceipt)) {
		t.Fatalf("first promotion differs from actual stored receipt: first=%s receipt=%s", promoted, actualReceipt)
	}
	before = b26CompleteSQL(t, db, engine)
	replay = b26OK(t, h.call(t, "x.workflow.run.promote", promote, "precision-promote"))
	if !bytes.Equal(canonical(replay), canonical(actualReceipt)) || !reflect.DeepEqual(before, b26CompleteSQL(t, db, engine)) {
		t.Fatal("promotion replay changed the complete stored result or SQL state")
	}
}

func TestB26PublicMixedParentReadinessSQLite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mixed-parent-readiness.db")
	store, err := (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	defer db.Close()
	b26PublicMixedParentReadiness(t, b26Host(t, store), db, "sqlite")
}

func TestB26PublicMixedParentReadinessPostgreSQL(t *testing.T) {
	dsn := freshDatabase(t)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	b26PublicMixedParentReadiness(t, b26Host(t, openStore(t, dsn)), db, "postgres")
}

// This uses the existing authenticated mixed-parent link, not the refused
// public hierarchy promotion path. Parent readiness applies to ready/claim;
// it does not add a parent predicate to the separate close contract.
func b26PublicMixedParentReadiness(t *testing.T, h *b26HTTP, db *sql.DB, engine string) {
	unchanged := func(t *testing.T, before map[string][]string) {
		t.Helper()
		if after := b26CompleteSQL(t, db, engine); !reflect.DeepEqual(before, after) {
			t.Fatalf("parent refusal/read/replay changed full SQL: before=%v after=%v", before, after)
		}
	}
	isReady := func(t *testing.T, id string) bool {
		t.Helper()
		page := decode[struct{ Items []struct{ ID string } }](t, b26OK(t, h.call(t, "bead.ready", map[string]any{"limit": 100, "filter": map[string]any{"includeEphemeral": true, "resources": []string{id}}}, "")))
		return len(page.Items) == 1 && page.Items[0].ID == id
	}
	workflowChild := func(t *testing.T, label, parent string) string {
		t.Helper()
		created := decode[workflowplugin.InvokeResult](t, b26OK(t, h.call(t, "x.workflow.invoke", map[string]any{"template": label, "project": project, "mode": "ephemeral", "checklist": []map[string]any{{"label": "child", "title": label, "type": "issue", "parent": ref(parent)}}}, label)))
		if len(created.Items) != 1 {
			t.Fatal("workflow descendant omitted its committed identity")
		}
		return created.Items[0].ID
	}
	for _, shape := range []string{"ordinary", "ordinary-descendant", "workflow-descendant", "alternating-descendant"} {
		for _, action := range []string{"ready", "claim", "claimNext", "close", "tx-close"} {
			if action == "tx-close" && shape == "workflow-descendant" {
				continue // Workflow items are not ordinary ChangeSet operations.
			}
			t.Run(shape+"/"+action, func(t *testing.T) {
				prefix := "mixed-parent-" + shape + "-" + action
				parent := b26Invoke(t, h, prefix, 1)[0].ID
				child := h.create(t, prefix+"-child")
				b26OK(t, h.call(t, "bead.link", map[string]any{"from": ref(child), "to": ref(parent), "kind": "parent-child"}, prefix+"-link"))
				if h.get(t, child)["bead"].(map[string]any)["parent"] != parent {
					t.Fatal("public mixed link did not persist its actual typed parent")
				}
				candidate := child
				var extraWorkflow string
				switch shape {
				case "ordinary-descendant":
					created := decode[model.Commit](t, b26OK(t, h.call(t, "bead.create", map[string]any{"type": "issue", "parent": ref(child), "fields": map[string]any{"title": prefix + "-grandchild"}}, prefix+"-grandchild")))
					if len(created.Results) != 1 {
						t.Fatal("ordinary descendant omitted its committed identity")
					}
					candidate = created.Results[0].Ref.ID
				case "workflow-descendant", "alternating-descendant":
					extraWorkflow = workflowChild(t, prefix+"-grandchild", child)
					candidate = extraWorkflow
					if shape == "alternating-descendant" {
						candidate = h.create(t, prefix+"-great-grandchild")
						b26OK(t, h.call(t, "bead.link", map[string]any{"from": ref(candidate), "to": ref(extraWorkflow), "kind": "parent-child"}, prefix+"-great-grandchild-link"))
					}
				}
				if !isReady(t, candidate) {
					t.Fatal("an open unblocked mixed parent suppressed its descendant")
				}
				b26OK(t, h.call(t, "bead.update", map[string]any{"id": ref(parent), "fields": map[string]any{"blocked": true}}, prefix+"-block"))
				before := b26CompleteSQL(t, db, engine)
				if isReady(t, candidate) {
					t.Fatal("blocked mixed-parent descendant remained ready")
				}
				unchanged(t, before)
				method := "bead." + action
				params := map[string]any{"id": ref(candidate)}
				if action == "claim" {
					code := "ontology_guard_failed"
					if strings.HasPrefix(candidate, workflowplugin.ItemPrefix) {
						code = "busy" // Preserve the existing workflow claim refusal.
					}
					b26SQLRefusal(t, h, db, engine, method, params, prefix+"-write", code)
				}
				if action == "claimNext" {
					params = map[string]any{"filter": map[string]any{"includeEphemeral": true, "resources": []string{candidate}}}
					if raw := b26OK(t, h.call(t, method, params, prefix+"-empty")); string(raw) != "null" {
						t.Fatalf("blocked descendant claimed by claimNext: %s", raw)
					}
					before = b26CompleteSQL(t, db, engine)
					if raw := b26OK(t, h.call(t, method, params, prefix+"-empty")); string(raw) != "null" {
						t.Fatalf("empty receipt replay changed response: %s", raw)
					}
					unchanged(t, before)
				}
				if action == "close" || action == "tx-close" {
					if action == "tx-close" {
						method = "tx.apply"
						params = map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{{Kind: model.OpClose, Close: &model.CloseOp{Target: ref(candidate)}}}}}
					}
					b26OK(t, h.call(t, method, params, prefix+"-write"))
					before = b26CompleteSQL(t, db, engine)
					b26OK(t, h.call(t, method, params, prefix+"-write"))
					unchanged(t, before)
					b26NoVersion(t, h.store, parent)
					if extraWorkflow != "" {
						b26NoVersion(t, h.store, extraWorkflow)
					}
					return // Parent blocking never becomes a close predicate.
				}
				b26OK(t, h.call(t, "bead.update", map[string]any{"id": ref(parent), "fields": map[string]any{"blocked": false}}, prefix+"-unblock"))
				if !isReady(t, candidate) {
					t.Fatal("clearing the actual parent did not restore the same descendant")
				}
				if action != "ready" {
					if action == "claimNext" {
						before = b26CompleteSQL(t, db, engine)
						if raw := b26OK(t, h.call(t, method, params, prefix+"-empty")); string(raw) != "null" {
							t.Fatalf("empty receipt consumed later ready work: %s", raw)
						}
						unchanged(t, before)
					}
					b26OK(t, h.call(t, method, params, prefix+"-write"))
					b26OK(t, h.call(t, "bead.update", map[string]any{"id": ref(parent), "fields": map[string]any{"blocked": true}}, prefix+"-block-after-receipt"))
					before = b26CompleteSQL(t, db, engine)
					b26OK(t, h.call(t, method, params, prefix+"-write"))
					unchanged(t, before)
				}
				b26NoVersion(t, h.store, parent)
				if extraWorkflow != "" {
					b26NoVersion(t, h.store, extraWorkflow)
				}
			})
		}
	}
	for _, condition := range []string{"blocked", "deferred"} {
		t.Run("ordinary-upstream-"+condition, func(t *testing.T) {
			prefix := "mixed-parent-upstream-" + condition
			upstream := h.create(t, prefix+"-ordinary")
			parent := workflowChild(t, prefix+"-workflow", upstream)
			child := h.create(t, prefix+"-child")
			b26OK(t, h.call(t, "bead.link", map[string]any{"from": ref(child), "to": ref(parent), "kind": "parent-child"}, prefix+"-link"))
			grandchild := workflowChild(t, prefix+"-grandchild", child)
			if !isReady(t, child) || !isReady(t, grandchild) {
				t.Fatal("open upstream ordinary parent suppressed mixed descendants")
			}
			if condition == "blocked" {
				b26OK(t, h.call(t, "bead.update", map[string]any{"id": ref(upstream), "fields": map[string]any{"blocked": true}}, prefix+"-block"))
			} else {
				b26OK(t, h.call(t, "bead.defer", map[string]any{"id": ref(upstream)}, prefix+"-block"))
			}
			before := b26CompleteSQL(t, db, engine)
			if isReady(t, child) || isReady(t, grandchild) {
				t.Fatal("blocked or deferred upstream ancestor did not suppress mixed descendants")
			}
			unchanged(t, before)
			b26SQLRefusal(t, h, db, engine, "bead.claim", map[string]any{"id": ref(child)}, prefix+"-claim", "ontology_guard_failed")
			b26SQLRefusal(t, h, db, engine, "bead.claim", map[string]any{"id": ref(grandchild)}, prefix+"-workflow-claim", "busy")
			if condition == "blocked" {
				b26OK(t, h.call(t, "bead.update", map[string]any{"id": ref(upstream), "fields": map[string]any{"blocked": false}}, prefix+"-unblock"))
			} else {
				b26OK(t, h.call(t, "bead.reopen", map[string]any{"id": ref(upstream)}, prefix+"-unblock"))
			}
			if !isReady(t, child) || !isReady(t, grandchild) {
				t.Fatal("clearing upstream state failed to restore mixed descendants")
			}
			b26OK(t, h.call(t, "bead.claim", map[string]any{"id": ref(child)}, prefix+"-claim"))
			b26OK(t, h.call(t, "bead.claim", map[string]any{"id": ref(grandchild)}, prefix+"-workflow-claim"))
			b26NoVersion(t, h.store, parent)
			b26NoVersion(t, h.store, grandchild)
		})
	}
	for _, race := range []string{"checkpoint-parent", "ordinary-blocker"} {
		t.Run("claim-fence/"+race, func(t *testing.T) {
			prefix := "mixed-parent-race-" + race
			parent := b26Invoke(t, h, prefix, 1)[0].ID
			child := h.create(t, prefix+"-child")
			b26OK(t, h.call(t, "bead.link", map[string]any{"from": ref(child), "to": ref(parent), "kind": "parent-child"}, prefix+"-parent"))
			blocker := ""
			if race == "ordinary-blocker" {
				blocker = h.create(t, prefix+"-blocker")
				b26OK(t, h.call(t, "bead.close", map[string]any{"id": ref(blocker)}, prefix+"-initial-close"))
				b26OK(t, h.call(t, "bead.link", map[string]any{"from": ref(parent), "to": ref(blocker), "kind": "blocks"}, prefix+"-dependency"))
			}
			if !isReady(t, child) {
				t.Fatal("race control did not begin with a ready mixed child")
			}
			var fired atomic.Bool
			var expected map[string][]string
			mutator := b26GuardMutator(func(_ context.Context, proposal port.ProposedChange, reader port.SnapshotReader) ([]port.Patch, error) {
				if proposal.Meta.Method != "bead.claim" || proposal.Meta.IdempotencyKey != key(prefix+"-claim") || !fired.CompareAndSwap(false, true) {
					return nil, nil
				}
				if reader.Snapshot().At == nil || *reader.Snapshot().At != proposal.BaseRevision {
					return nil, fmt.Errorf("claim guard and mutator lost the original snapshot")
				}
				if race == "checkpoint-parent" {
					b26OK(t, h.call(t, "bead.update", map[string]any{"id": ref(parent), "fields": map[string]any{"blocked": true}}, prefix+"-race"))
				} else {
					b26OK(t, h.call(t, "bead.reopen", map[string]any{"id": ref(blocker)}, prefix+"-race"))
				}
				expected = b26CompleteSQL(t, db, engine)
				return nil, nil
			})
			domain := b26MixedParentService(t, h, pipeline.WithMutators(pipeline.MutatorBinding{ID: "b26-parent-race", Mutator: mutator}))
			by := model.PrincipalRef{Kind: "user", ID: "user:b26-public"}
			meta := model.WriteMeta{Project: project, AgentLabel: "b26-public", IdempotencyKey: key(prefix + "-claim")}
			_, err := domain.ClaimWithOutcome(context.Background(), by, service.ClaimRequest{ID: ref(child)}, meta)
			b26MixedParentCode(t, err, "ontology_guard_failed")
			if !fired.Load() || expected == nil {
				t.Fatal("race did not occur after the successful initial guard read")
			}
			unchanged(t, expected)
			if isReady(t, child) || h.get(t, child)["lease"] != nil {
				t.Fatal("stale mixed-parent claim became a live lease")
			}
			if race == "checkpoint-parent" {
				b26OK(t, h.call(t, "bead.update", map[string]any{"id": ref(parent), "fields": map[string]any{"blocked": false}}, prefix+"-correct"))
			} else {
				b26OK(t, h.call(t, "bead.close", map[string]any{"id": ref(blocker)}, prefix+"-correct"))
			}
			first, err := domain.ClaimWithOutcome(context.Background(), by, service.ClaimRequest{ID: ref(child)}, meta)
			if err != nil || first.Replayed {
				t.Fatalf("corrected claim did not use its unreserved key: %+v %v", first, err)
			}
			before := b26CompleteSQL(t, db, engine)
			replay, err := domain.ClaimWithOutcome(context.Background(), by, service.ClaimRequest{ID: ref(child)}, meta)
			if err != nil || !replay.Replayed || !reflect.DeepEqual(first.Commit, replay.Commit) {
				t.Fatalf("corrected claim replay: %+v %v", replay, err)
			}
			unchanged(t, before)
			b26NoVersion(t, h.store, parent)
		})
	}
}
