package postgresacceptance

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

func b25CallMeta(t *testing.T, h *b26HTTP, params map[string]any, label string, overrides map[string]any) reply {
	t.Helper()
	values := map[string]any{}
	for name, value := range params {
		values[name] = value
	}
	meta := map[string]any{"project": project, "agent": "b26-public", "idempotencyKey": key(label)}
	for name, value := range overrides {
		meta[name] = value
	}
	values["_meta"] = meta
	data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": h.serial.Add(1), "method": "tx.apply", "params": values})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, h.server.URL+httpjson.Path, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Plug-RPC", "1")
	request.Header.Set("Authorization", "Bearer "+h.token)
	response, err := h.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result reply
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result
}

// Compare the HTTP result with the existing independent public provider read,
// including fields, comments, ownership scope, predicate metadata and links.
func b25AssertFullWorkflowResult(t *testing.T, h *b26HTTP, result model.OpResult, id string) {
	t.Helper()
	metadata, err := h.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	provider, err := workflowplugin.New(pluginhost.NewDurableState(h.store.(driver.PersistentCheckpointStore), h.lease, "workflow", "default"))
	if err != nil {
		t.Fatal(err)
	}
	expected, err := provider.WithIdentity("workflow", "default", metadata.Incarnation).Get(context.Background(), model.PrincipalRef{Kind: "user", ID: "user:b26-public"}, ref(id), model.WriteMeta{Project: project})
	if err != nil {
		t.Fatal(err)
	}
	// The transaction record is the public JSON image.  Normalize the
	// independently read image through that same wire projection before
	// comparing it: current-main carries internal ordering provenance on
	// ItemView, but CreatedAt is deliberately omitted from this response.
	expectedWire, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}
	actual := decode[workflowplugin.ItemView](t, result.Record)
	wireExpected := decode[workflowplugin.ItemView](t, expectedWire)
	if result.Ref != ref(id) || result.Revision != expected.Revision || !reflect.DeepEqual(wireExpected, actual) {
		t.Fatalf("full surviving endpoint post-image differs: result=%s expected=%+v", result.Record, expected)
	}
}

func b25CoreDeleteReceipt(t *testing.T, h *b26HTTP, label string, workflowPosition int) *driver.IdempotencyRecord {
	t.Helper()
	ctx := context.Background()
	metadata, err := h.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := h.store.Read(ctx, driver.View{Incarnation: metadata.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	principal, err := driver.PrincipalKey(model.PrincipalRef{Kind: "user", ID: "user:b26-public"})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := reader.IdempotencyByKey(ctx, principal, "", "tx.apply", key(label))
	if err != nil || receipt == nil || workflowPosition >= len(receipt.Commit.Results) {
		t.Fatalf("missing original core receipt: %+v %v", receipt, err)
	}
	if len(receipt.Commit.Results[workflowPosition].Record) != 0 {
		t.Fatal("permanent core receipt retained an ordinary ephemeral payload")
	}
	return receipt
}

func b25DeleteResultControls(t *testing.T, h *b26HTTP, db *sql.DB, engine string) {
	t.Helper()
	for _, format := range []string{"default", "full", "minimal"} {
		t.Run("results/"+format, func(t *testing.T) {
			label, secret := "b25-result-"+format, "b25-private-payload-"+format
			item := b26Invoke(t, h, secret, 1)[0].ID
			b26OK(t, h.call(t, "bead.comment", map[string]any{"id": ref(item), "body": secret + "-comment"}, label+"-comment"))
			victim := h.create(t, label+"-victim")
			b25Link(t, h, item, victim, "relates-to", label+"-link")
			b25Link(t, h, victim, item, "blocks", label+"-second-link")
			generation := b25Items(t, h)[item].Generation
			finalRevision := model.Natural64(generation + 2)
			params := map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{b25Unlink(item, victim, "relates-to"), b25Unlink(victim, item, "blocks"), b25Delete(victim)}}}
			meta := map[string]any{}
			if format != "default" {
				meta["return"] = format
			}
			before := b26CompleteSQL(t, db, engine)
			dryMeta := map[string]any{"dryRun": true}
			for name, value := range meta {
				dryMeta[name] = value
			}
			preview := decode[model.Commit](t, b26OK(t, b25CallMeta(t, h, params, label+"-dry", dryMeta)))
			if !preview.WouldCommit || len(preview.Results) != 3 || len(preview.Results[2].Record) != 0 || !reflect.DeepEqual(before, b26CompleteSQL(t, db, engine)) {
				t.Fatal("mixed delete dry-run changed storage or lost original result positions")
			}
			if !reflect.DeepEqual(preview.Results[0], preview.Results[1]) || preview.Results[0].Ref != ref(item) || preview.Results[0].Revision != finalRevision {
				t.Fatal("same-endpoint dry-run results did not share the final image and revision")
			}
			if format == "minimal" {
				if len(preview.Results[0].Record) != 0 {
					t.Fatal("minimal dry-run disclosed an active body")
				}
			} else if image := decode[workflowplugin.ItemView](t, preview.Results[0].Record); image.ID != item || image.Revision != finalRevision || image.Title != secret || len(image.Links) != 0 || len(image.Comments) != 1 || image.Comments[0].Body != secret+"-comment" {
				t.Fatal("full dry-run omitted the unpublished active post-image")
			}
			first := b25CallMeta(t, h, params, label, meta)
			commit := decode[model.Commit](t, b26OK(t, first))
			if first.replayed() || len(commit.Results) != 3 || commit.Results[0].Ref != ref(item) || commit.Results[1].Ref != ref(item) || commit.Results[2].Ref != ref(victim) || len(commit.Results[2].Record) != 0 {
				t.Fatal("mixed delete result changed original positions or deleted post-image")
			}
			if !reflect.DeepEqual(commit.Results[:2], preview.Results[:2]) || model.Natural64(b25Items(t, h)[item].Generation) != finalRevision {
				t.Fatal("committed same-endpoint results differ from the final prepared generation")
			}
			if format == "minimal" {
				if len(commit.Results[0].Record) != 0 {
					t.Fatal("minimal result disclosed an active body")
				}
			} else {
				b25AssertFullWorkflowResult(t, h, commit.Results[0], item)
				b25AssertFullWorkflowResult(t, h, commit.Results[1], item)
			}
			coreReceipt := b25CoreDeleteReceipt(t, h, label, 0)
			if !reflect.DeepEqual(coreReceipt, b25CoreDeleteReceipt(t, h, label, 1)) || !reflect.DeepEqual(coreReceipt.Commit.Results[0], coreReceipt.Commit.Results[1]) || coreReceipt.Commit.Results[0].Revision != finalRevision {
				t.Fatal("core receipt did not retain both final payload-free result identities")
			}
			// The receipt must retain its original post-image after a later
			// public update; rebuilding a replay from the current item is wrong.
			b26OK(t, h.call(t, "bead.update", map[string]any{"id": ref(item), "fields": map[string]any{"title": label + "-later"}}, label+"-later"))
			before = b26CompleteSQL(t, db, engine)
			replay(t, b25CallMeta(t, h, params, label, meta), first.Result)
			if !reflect.DeepEqual(before, b26CompleteSQL(t, db, engine)) || !reflect.DeepEqual(coreReceipt, b25CoreDeleteReceipt(t, h, label, 0)) {
				t.Fatal("post-image replay changed graph, namespace or original core receipt")
			}
			otherFormat := "minimal"
			if format == "minimal" {
				otherFormat = "full"
			}
			refused(t, b25CallMeta(t, h, params, label, map[string]any{"return": otherFormat}), "idempotency_key_reused")
			if !reflect.DeepEqual(before, b26CompleteSQL(t, db, engine)) {
				t.Fatal("changed return mode mutated a retained receipt")
			}
			if format == "full" {
				target := h.create(t, label+"-promoted")
				b26OK(t, h.call(t, "x.workflow.run.promote", map[string]any{"item": ref(item), "target": ref(target)}, label+"-promote"))
			} else {
				b26OK(t, h.call(t, "bead.close", map[string]any{"id": ref(item)}, label+"-close"))
			}
			if strings.Contains(string(workflowCheckpoint(t, h.store.(driver.CheckpointStore)).Checkpoint.Payload), secret) {
				t.Fatal("completion or promotion retained a private result projection")
			}
			before = b26CompleteSQL(t, db, engine)
			terminalReply := b25CallMeta(t, h, params, label, meta)
			terminal := decode[model.Commit](t, b26OK(t, terminalReply))
			commit.Results[0].Record = nil
			commit.Results[1].Record = nil
			if !terminalReply.replayed() || !reflect.DeepEqual(commit, terminal) || !reflect.DeepEqual(before, b26CompleteSQL(t, db, engine)) || !reflect.DeepEqual(coreReceipt, b25CoreDeleteReceipt(t, h, label, 0)) {
				t.Fatal("terminal replay did not retain identity-only results without writes")
			}
		})
	}
}
