package authzrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

func workflowWriteParams(t *testing.T, f *workflowClaimNextFixture, method string) map[string]any {
	t.Helper()
	if method == "x.workflow.invoke" {
		return map[string]any{"template": "template", "project": "project", "mode": "ephemeral", "checklist": []workflowplugin.ChecklistItem{{Label: "first", Title: "private first item"}, {Label: "second", Title: "private second item"}}}
	}
	items := f.invoke(t, 200, 2)
	params := map[string]any{"id": model.RefByID(items[0].ID, model.Natural64(0))}
	if method == "bead.comment" {
		params["body"] = "private comment"
	} else {
		params["reason"] = "completed item"
	}
	return params
}

func workflowWriteCall(t *testing.T, f *workflowClaimNextFixture, method string, params map[string]any, key int, project string) workflowClaimNextReply {
	t.Helper()
	issued, err := f.keys.Issue(f.by, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	params["_meta"] = map[string]any{"project": project, "idempotencyKey": fmt.Sprintf("%032x", key), "agent": "client-label"}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1"+httpjson.Path, bytes.NewReader(body))
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Plug-RPC", "1")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+issued.Token)
	response := httptest.NewRecorder()
	f.app.Handler.ServeHTTP(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("HTTP status=%d", response.Code)
	}
	var got workflowClaimNextReply
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	return got
}

func workflowWriteCheckpoint(t *testing.T, f *workflowClaimNextFixture) driver.Checkpoint {
	t.Helper()
	checkpoint, err := f.store.(driver.CheckpointStore).LoadCheckpoint(context.Background(), "plugin/"+workflowplugin.Alias+"/default/durable")
	if err != nil {
		t.Fatal(err)
	}
	return checkpoint
}

func workflowWriteSuccess(t *testing.T, got workflowClaimNextReply) {
	t.Helper()
	if got.Error != nil || len(got.Result) == 0 || bytes.Equal(got.Result, []byte("null")) {
		t.Fatalf("authorized workflow write failed: %+v result=%s", got.Error, got.Result)
	}
}

func TestWorkflowWritesCurrentProjectPolicy(t *testing.T) {
	for _, method := range []string{"x.workflow.invoke", "bead.comment", "bead.close"} {
		for _, phase := range []string{"live", "receipt-replay"} {
			for _, denial := range []string{"revoked-grant", "inactive-project", "facts-unavailable"} {
				t.Run(method+"/"+phase+"/"+denial, func(t *testing.T) {
					f := newWorkflowClaimNextFixture(t)
					params := workflowWriteParams(t, f, method)
					if phase == "receipt-replay" {
						workflowWriteSuccess(t, workflowWriteCall(t, f, method, params, 201, "project"))
					}
					before := workflowWriteCheckpoint(t, f)
					want := model.ErrorCode("forbidden")
					switch denial {
					case "revoked-grant":
						f.revoke(t)
					case "inactive-project":
						f.facts.Projects[0].Active = false
						if err := f.source.Replace(f.facts); err != nil {
							t.Fatal(err)
						}
					case "facts-unavailable":
						f.app.Checker.source = &workflowClaimNextFacts{FactsSource: f.source, failure: errors.New("facts unavailable")}
						want = "authz_unavailable"
					}
					workflowClaimNextDenied(t, workflowWriteCall(t, f, method, params, 201, "project"), want)
					after := workflowWriteCheckpoint(t, f)
					if before.Generation != after.Generation || !bytes.Equal(before.Payload, after.Payload) {
						t.Fatal("current denial changed the provider checkpoint, receipt or audit")
					}
				})
			}
		}
	}
}

func TestWorkflowWritesCheckImmutableResourceOnEveryRetry(t *testing.T) {
	for _, method := range []string{"x.workflow.invoke", "bead.comment", "bead.close"} {
		t.Run(method, func(t *testing.T) {
			f := newWorkflowClaimNextFixture(t)
			params := workflowWriteParams(t, f, method)
			rules := &workflowClaimNextCheckedRules{RuleSet: f.app.Checker.chain.Rules[0]}
			f.app.Checker.chain.Rules = []authz.RuleSet{rules}
			first := workflowWriteCall(t, f, method, params, 202, "project")
			workflowWriteSuccess(t, first)
			before := workflowWriteCheckpoint(t, f)
			retry := workflowWriteCall(t, f, method, params, 202, "project")
			workflowWriteSuccess(t, retry)
			after := workflowWriteCheckpoint(t, f)
			if !bytes.Equal(first.Result, retry.Result) || before.Generation != after.Generation || !bytes.Equal(before.Payload, after.Payload) {
				t.Fatal("authorized receipt retry changed its result or provider state")
			}
			if len(rules.checks) != 2 {
				t.Fatalf("first write and receipt retry did not each check current policy: %+v", rules.checks)
			}
			for _, req := range rules.checks {
				if req.Action != authz.Modify || req.Resource.Kind != authz.Project || req.Resource.Ref.ID != "project:immutable-claimnext" || req.Method != "" {
					t.Fatalf("wrong current project authority mapping: %+v", req)
				}
			}
		})
	}
}

func TestWorkflowWritesPreflightAuthorityFence(t *testing.T) {
	for _, method := range []string{"x.workflow.invoke", "bead.comment", "bead.close"} {
		t.Run(method, func(t *testing.T) {
			f := newWorkflowClaimNextFixture(t)
			params := workflowWriteParams(t, f, method)
			before := workflowWriteCheckpoint(t, f)
			revoked := false
			f.app.Checker.source = &workflowClaimNextFacts{FactsSource: f.source, onLoad: func() error {
				if !revoked {
					revoked = true
					f.facts.Grants = nil
					return f.source.Replace(f.facts)
				}
				return nil
			}}
			workflowClaimNextDenied(t, workflowWriteCall(t, f, method, params, 203, "project"), "authz_unavailable")
			after := workflowWriteCheckpoint(t, f)
			if !revoked || before.Generation != after.Generation || !bytes.Equal(before.Payload, after.Payload) {
				t.Fatal("changed preflight authority reached the provider checkpoint")
			}
		})
	}
}

// This wrapper changes current authority after a real provider mutation or
// receipt reply. Refusing its response does not establish atomic rollback.
type workflowWriteRevokingDelegate struct {
	*service.Service
	after func()
}

func (d *workflowWriteRevokingDelegate) WorkflowInvoke(ctx context.Context, by model.PrincipalRef, req workflowplugin.InvokeRequest, meta model.WriteMeta) (workflowplugin.InvokeResult, error) {
	out, err := d.Service.WorkflowInvoke(ctx, by, req, meta)
	if err == nil && d.after != nil {
		d.after()
	}
	return out, err
}

func (d *workflowWriteRevokingDelegate) CommentWithOutcome(ctx context.Context, by model.PrincipalRef, req service.CommentRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	out, err := d.Service.CommentWithOutcome(ctx, by, req, meta)
	if err == nil && d.after != nil {
		d.after()
	}
	return out, err
}

func (d *workflowWriteRevokingDelegate) CloseWithOutcome(ctx context.Context, by model.PrincipalRef, req service.CloseRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	out, err := d.Service.CloseWithOutcome(ctx, by, req, meta)
	if err == nil && d.after != nil {
		d.after()
	}
	return out, err
}

func TestWorkflowWritesPublicationAuthorityFence(t *testing.T) {
	for _, method := range []string{"x.workflow.invoke", "bead.comment", "bead.close"} {
		for _, phase := range []string{"live", "receipt-replay"} {
			t.Run(method+"/"+phase, func(t *testing.T) {
				f := newWorkflowClaimNextFixture(t)
				params := workflowWriteParams(t, f, method)
				delegate := &workflowWriteRevokingDelegate{Service: f.domain}
				f.app.Service.delegate = delegate
				if phase == "receipt-replay" {
					workflowWriteSuccess(t, workflowWriteCall(t, f, method, params, 204, "project"))
				}
				before := workflowWriteCheckpoint(t, f)
				revoked := false
				delegate.after = func() { revoked = true; f.revoke(t) }
				workflowClaimNextDenied(t, workflowWriteCall(t, f, method, params, 204, "project"), "authz_unavailable")
				after := workflowWriteCheckpoint(t, f)
				if !revoked {
					t.Fatal("authority did not change at the provider result boundary")
				}
				if phase == "live" && after.Generation <= before.Generation {
					t.Fatal("test did not exercise a committed mutation before response refusal")
				}
				if phase == "receipt-replay" && (after.Generation != before.Generation || !bytes.Equal(before.Payload, after.Payload)) {
					t.Fatal("receipt retry mutated provider state")
				}
			})
		}
	}
}

func TestB26LifecycleDeniedProjectDoesNotPublish(t *testing.T) {
	for _, method := range []string{"x.workflow.run.promote", "x.workflow.run.demote"} {
		t.Run(method, func(t *testing.T) {
			f := newWorkflowClaimNextFixture(t)
			item := model.RefByID(f.invoke(t, 260, 1)[0].ID, model.Natural64(0))
			before := workflowWriteCheckpoint(t, f)
			f.revoke(t)
			var err error
			if method == "x.workflow.run.promote" {
				_, err = f.app.Service.WorkflowPromote(context.Background(), f.by, workflowplugin.PromoteRequest{Item: item, Target: model.RefByID("ordinary-target", model.Natural64(0))}, workflowClaimNextMeta(method, 261))
			} else {
				_, err = f.app.Service.WorkflowDemote(context.Background(), f.by, workflowplugin.DemoteRequest{Source: model.RefByID("ordinary-source", model.Natural64(0)), Template: "demote", Label: "item"}, workflowClaimNextMeta(method, 261))
			}
			var denied *model.Error
			if !errors.As(err, &denied) || denied.Data.Code != "forbidden" {
				t.Fatalf("project denial=%v", err)
			}
			after := workflowWriteCheckpoint(t, f)
			if before.Generation != after.Generation || !bytes.Equal(before.Payload, after.Payload) {
				t.Fatal("denied lifecycle mutated namespace")
			}
		})
	}
}
