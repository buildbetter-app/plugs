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
					ops := params["changeSet"].(map[string]any)["ops"].([]any)
					firstReason, _ := ops[2].(map[string]any)["reason"].(string)
					if doc.CloseReason == nil || *doc.CloseReason != firstReason {
						t.Fatalf("full persisted lifecycle image lost first reason: body=%s", row.Body)
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
		if err := hierarchyCloseCreateImage(&image, true, "", nil, hAgent()); err == nil {
			t.Fatalf("malformed core close time accepted: %#v", invalid)
		}
	}
	image := model.Record{Body: map[string]any{"createdAt": stamp, "closedAt": stamp}}
	if err := hierarchyCloseCreateImage(&image, false, "", nil, hAgent()); err == nil {
		t.Fatal("unbacked close timestamp was erased from creation validation")
	}
	// Normalization is limited to the same-batch close output. Unsupported
	// creation/permission members remain visible to the strict create guard.
	image = model.Record{Body: map[string]any{"createdAt": stamp, "closedAt": stamp, "startedAt": stamp, "owner": "operator", "unknown": true}}
	if err := hierarchyCloseCreateImage(&image, true, "", nil, hAgent()); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"createdAt": stamp, "startedAt": stamp, "owner": "operator", "unknown": true}
	if !reflect.DeepEqual(image.Body, want) {
		t.Fatalf("normalization altered unrelated members: got=%#v want=%#v", image.Body, want)
	}
}

func TestLifecycleCloseReasonPreservesFirstIdentityAndCompleteImages(t *testing.T) {
	principal := model.PrincipalRef{Kind: "user", ID: "user:operator"}
	caller := authz.Caller{Principal: principal}
	images := func() (model.Record, model.Record) {
		before := model.Record{Ref: hRef("child"), Kind: "bead", Revision: model.Natural64(1), Body: map[string]any{
			"project": "project-a", "type": "issue", "fields": map[string]any{"status": "open", "title": "unchanged"},
		}}
		after := before
		after.Body = map[string]any{"project": "project-a", "type": "issue", "fields": map[string]any{"status": "done", "title": "unchanged"}}
		return before, after
	}
	const imported = " \timported e\u0301 \"first\"\r\n "
	for _, tc := range []struct {
		name                string
		before, after       any
		beforeSet, afterSet bool
		setter              string
		deny                bool
	}{
		{"first-reason", nil, "first", false, true, "request", false},
		{"first-known-empty", nil, "", false, true, "request", false},
		{"legacy-absent", nil, nil, false, false, "", false},
		{"stored-known-empty", "", "", true, true, "", false},
		{"imported-exact-bytes", imported, imported, true, true, "", false},
		{"imported-rewrite", imported, "imported é \"first\"", true, true, "request", true},
		{"imported-removal", imported, nil, true, false, "core", true},
		{"known-empty-replacement", "", "replacement", true, true, "request", true},
		{"null-before", nil, "first", true, true, "request", true},
		{"null-after", nil, nil, false, true, "request", true},
		{"numeric-after", nil, 1, false, true, "request", true},
		{"object-after", nil, map[string]any{}, false, true, "request", true},
		{"missing-request-provenance", nil, "first", false, true, "", true},
		{"core-first-provenance", nil, "first", false, true, "core", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before, after := images()
			if tc.beforeSet {
				before.Body["closeReason"] = tc.before
			}
			if tc.afterSet {
				after.Body["closeReason"] = tc.after
			}
			setters := map[string]string{}
			if tc.setter != "" {
				setters["closeReason"] = tc.setter
			}
			beforeRaw, _ := json.Marshal(before.Body)
			afterRaw, _ := json.Marshal(after.Body)
			err := validateLifecycleImage(before, after, principal, caller, setters)
			if (err != nil) != tc.deny {
				t.Fatalf("first-reason contract: err=%v wantRefusal=%v", err, tc.deny)
			}
			gotBefore, _ := json.Marshal(before.Body)
			gotAfter, _ := json.Marshal(after.Body)
			if !bytes.Equal(beforeRaw, gotBefore) || !bytes.Equal(afterRaw, gotAfter) {
				t.Fatal("validation changed the full source/proposed image")
			}
		})
	}
	for _, key := range []string{"project", "parent", "owner", "assignee", "type", "labels", "restricted", "unknown", "closeReasonOther"} {
		t.Run("reassignment-"+key, func(t *testing.T) {
			before, after := images()
			after.Body["closeReason"] = ""
			after.Body[key] = "changed"
			if err := validateLifecycleImage(before, after, principal, caller, map[string]string{"closeReason": "request"}); err == nil {
				t.Fatalf("close identity hid unrelated mutation %q", key)
			}
		})
	}
	t.Run("ordinary-field", func(t *testing.T) {
		before, after := images()
		after.Body["fields"].(map[string]any)["title"] = "changed"
		if err := validateLifecycleImage(before, after, principal, caller, nil); err == nil {
			t.Fatal("close identity widened ordinary field mutation")
		}
	})
	t.Run("deputy", func(t *testing.T) {
		before, after := images()
		if err := validateLifecycleImage(before, after, principal, caller, map[string]string{"closeReason": "plugin"}); err == nil {
			t.Fatal("close identity widened deputy authority")
		}
	})
	t.Run("principal", func(t *testing.T) {
		before, after := images()
		if err := validateLifecycleImage(before, after, hAgent(), authz.Caller{Principal: hAgent()}, nil); err == nil {
			t.Fatal("close identity granted Agent lifecycle authority")
		}
	})
}

func TestHierarchyCloseCreateImageBindsExactFirstReasonOnlyInDetachedView(t *testing.T) {
	const stamp = "2026-10-29T12:00:00.123456789Z"
	const exact = " \tfirst e\u0301 \"reason\"\r\n "
	for _, tc := range []struct {
		name     string
		closed   bool
		first    string
		identity any
		present  bool
		reasons  []string
		deny     bool
	}{
		{"legacy-absence", true, "", nil, false, nil, false},
		{"known-empty", true, "", "", true, nil, false},
		{"exact-first", true, exact, exact, true, []string{exact}, false},
		{"empty-first-later-comment", true, "", "", true, []string{"later"}, false},
		{"first-survives-later-comment", true, exact, exact, true, []string{exact, "later"}, false},
		{"wrong-first", true, exact, "later", true, []string{exact, "later"}, true},
		{"empty-is-not-absent", true, "", "inferred", true, nil, true},
		{"nonempty-is-not-empty", true, "first", "", true, []string{"first"}, true},
		{"null", true, "", nil, true, nil, true},
		{"number", true, "", 1, true, nil, true},
		{"object", true, "", map[string]any{}, true, nil, true},
		{"unbacked", false, "", "", true, nil, true},
		{"plain-create", false, "", nil, false, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := map[string]any{"createdAt": stamp, "project": "project-a", "unknown": true, "owner": "operator"}
			if tc.closed {
				body["closedAt"] = stamp
			}
			if tc.present {
				body["closeReason"] = tc.identity
			}
			if len(tc.reasons) != 0 {
				comments := make([]any, 0, len(tc.reasons))
				for _, reason := range tc.reasons {
					comments = append(comments, map[string]any{"body": reason, "principal": hAgent(), "time": stamp})
				}
				body["comments"] = comments
			}
			raw, _ := json.Marshal(body)
			var detached map[string]any
			if err := json.Unmarshal(raw, &detached); err != nil {
				t.Fatal(err)
			}
			image := model.Record{Body: detached}
			err := hierarchyCloseCreateImage(&image, tc.closed, tc.first, tc.reasons, hAgent())
			if (err != nil) != tc.deny {
				t.Fatalf("typed first close reason: err=%v wantRefusal=%v", err, tc.deny)
			}
			got, _ := json.Marshal(body)
			if !bytes.Equal(raw, got) {
				t.Fatal("detached view changed the complete persisted image")
			}
			if !tc.deny {
				want := map[string]any{"createdAt": stamp, "project": "project-a", "unknown": true, "owner": "operator"}
				if !reflect.DeepEqual(image.Body, want) {
					t.Fatalf("creation view lost unrelated strict members: got=%#v want=%#v", image.Body, want)
				}
			}
		})
	}
}
