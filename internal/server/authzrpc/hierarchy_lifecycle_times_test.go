package authzrpc

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
)

func TestHierarchyCreateClosePreservesCoreTimesAndCurrentPermits(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			for _, reason := range []string{"with-reason", "without-reason"} {
				t.Run(reason, func(t *testing.T) {
					f := newHierarchyLifecycleFixture(t, engine)
					rules := &hierarchyLifecycleRules{denyClose: true}
					f.app.Checker.chain.Rules = []authz.RuleSet{rules}
					params := hierarchyLifecycleMixedParams(2120, nil)
					if reason == "without-reason" {
						ops := params["changeSet"].(map[string]any)["ops"].([]any)
						delete(ops[2].(map[string]any), "reason")
					}
					head, rows := hierarchyLifecycleState(t, f, hRef("child"))
					count := hierarchyLifecycleCount(t, f)
					hierarchyLifecycleForbidden(t, f.callRPC(t, hAgent(), "tx.apply", params))
					hierarchyLifecycleSameState(t, f, head, rows, hRef("child"))
					if hierarchyLifecycleCount(t, f) != count {
						t.Fatal("close timestamp granted publication without close permission")
					}
					hierarchyLifecycleNoReceipt(t, f, params)

					rules.denyClose = false
					first := f.callRPC(t, hAgent(), "tx.apply", params)
					var commit model.Commit
					if first.Error != nil || json.Unmarshal(first.Result, &commit) != nil || len(commit.Results) != 3 {
						t.Fatalf("authorized create/close failed: %+v result=%s", first, first.Result)
					}
					fresh := commit.Results[0].Ref
					row := hierarchyLifecycleRecord(t, f, fresh)
					doc, err := pipeline.DecodeDocument(row.Body)
					if err != nil || doc.CreatedAt == nil || doc.ClosedAt == nil || doc.StartedAt != nil || doc.Fields["status"] != "done" {
						t.Fatalf("full persisted lifecycle image lost core times: body=%s err=%v", row.Body, err)
					}
					var body map[string]any
					if err := json.Unmarshal(row.Body, &body); err != nil {
						t.Fatal(err)
					}
					for _, key := range []string{"createdAt", "closedAt"} {
						text, ok := body[key].(string)
						instant, err := time.Parse(time.RFC3339Nano, text)
						if !ok || err != nil || instant.IsZero() || instant.UTC().Format(time.RFC3339Nano) != text {
							t.Fatalf("noncanonical persisted %s=%#v", key, body[key])
						}
					}
					wantComments := 1
					if reason == "without-reason" {
						wantComments = 0
					}
					if len(doc.Comments) != wantComments || rules.count("tx.apply") == 0 || rules.count("bead.close") == 0 {
						t.Fatal("mixed lifecycle lost exact reason image or independent method checks")
					}
					committedHead, committedRows := hierarchyLifecycleState(t, f, fresh, hRef("child"))
					replay := f.callRPC(t, hAgent(), "tx.apply", params)
					if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(first.Result, replay.Result) {
						t.Fatalf("authorized timestamp replay changed response: %+v", replay)
					}
					hierarchyLifecycleSameState(t, f, committedHead, committedRows, fresh, hRef("child"))
					rules.denyClose = true
					hierarchyLifecycleForbidden(t, f.callRPC(t, hAgent(), "tx.apply", params))
					hierarchyLifecycleSameState(t, f, committedHead, committedRows, fresh, hRef("child"))
				})
			}
		})
	}
}

func TestHierarchyCloseCreateImageRequiresBackedCanonicalCloseTime(t *testing.T) {
	const stamp = "2026-10-29T12:00:00.123456789Z"
	for _, invalid := range []any{nil, true, 1, "", "not-an-instant", "0001-01-01T00:00:00Z", "2026-10-29T12:00:00+00:00", "2026-10-29T13:00:00+01:00"} {
		image := model.Record{Body: map[string]any{"createdAt": stamp, "closedAt": invalid}}
		if err := hierarchyCloseCreateImage(&image, true, nil, hAgent()); err == nil {
			t.Fatalf("malformed core close time accepted: %#v", invalid)
		}
	}
	image := model.Record{Body: map[string]any{"createdAt": stamp, "closedAt": stamp}}
	if err := hierarchyCloseCreateImage(&image, false, nil, hAgent()); err == nil {
		t.Fatal("unbacked close timestamp was erased from creation validation")
	}
	// Normalization is limited to the same-batch close output. Unsupported
	// creation/permission members remain visible to the strict create guard.
	image = model.Record{Body: map[string]any{"createdAt": stamp, "closedAt": stamp, "startedAt": stamp, "owner": "operator", "unknown": true}}
	if err := hierarchyCloseCreateImage(&image, true, nil, hAgent()); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"createdAt": stamp, "startedAt": stamp, "owner": "operator", "unknown": true}
	if !reflect.DeepEqual(image.Body, want) {
		t.Fatalf("normalization altered unrelated members: got=%#v want=%#v", image.Body, want)
	}
}
