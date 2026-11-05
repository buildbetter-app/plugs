package authzrpc

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

func workflowBoundaryCall(f *workflowClaimNextFixture, method string, ref model.Ref, expect []model.Precondition, meta model.WriteMeta) error {
	ctx := context.Background()
	switch method {
	case "bead.get":
		_, err := f.domain.Get(ctx, f.by, service.GetRequest{ID: ref, Agent: meta.Agent})
		return err
	case "bead.claim":
		_, err := f.domain.ClaimWithOutcome(ctx, f.by, service.ClaimRequest{ID: ref, Expect: expect}, meta)
		return err
	case "bead.comment":
		_, err := f.domain.CommentWithOutcome(ctx, f.by, service.CommentRequest{ID: ref, Body: "private comment", Expect: expect}, meta)
		return err
	case "bead.close":
		_, err := f.domain.CloseWithOutcome(ctx, f.by, service.CloseRequest{ID: ref, Reason: "completed", Expect: expect}, meta)
		return err
	case "bead.claimNext":
		_, err := f.domain.ClaimNextWithOutcome(ctx, f.by, service.ClaimNextRequest{Project: "project"}, meta)
		return err
	default:
		_, err := f.domain.WorkflowInvoke(ctx, f.by, workflowplugin.InvokeRequest{Template: "template", Project: "project", Mode: "ephemeral", Checklist: []workflowplugin.ChecklistItem{{Label: "first", Title: "private invocation"}}}, meta)
		return err
	}
}

func workflowBoundaryError(t *testing.T, err error, code model.ErrorCode) {
	t.Helper()
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}

func TestWorkflowItemReferenceBoundary(t *testing.T) {
	f := newWorkflowClaimNextFixture(t)
	ref := model.RefByID(f.invoke(t, 300, 2)[0].ID, model.Natural64(0))
	before := workflowWriteCheckpoint(t, f)
	refs := map[string]model.Ref{
		"branch":        model.RefByID(ref.ID, model.Natural64(1)),
		"local-alias":   model.RefByKey(ref.ID),
		"missing-id":    model.RefByID(workflowplugin.ItemPrefix, model.Natural64(0)),
		"short":         model.RefByID(workflowplugin.ItemPrefix+"abc", model.Natural64(0)),
		"uppercase":     model.RefByID(workflowplugin.ItemPrefix+strings.Repeat("A", 32), model.Natural64(0)),
		"extra-segment": model.RefByID(ref.ID+"/extra", model.Natural64(0)),
		"run":           model.RefByID("workflow:run/"+strings.Repeat("a", 32), model.Natural64(0)),
	}
	for name, invalid := range refs {
		for _, method := range []string{"bead.get", "bead.claim", "bead.comment", "bead.close"} {
			t.Run(name+"/"+method, func(t *testing.T) {
				workflowBoundaryError(t, workflowBoundaryCall(f, method, invalid, nil, workflowClaimNextMeta(method, 301)), "invalid_params")
			})
		}
	}
	current, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.domain.Get(context.Background(), f.by, service.GetRequest{ID: ref, AsOf: &service.AsOf{Revision: current.Head, Incarnation: current.Incarnation}})
	workflowBoundaryError(t, err, "invalid_params")
	version := model.Natural64(1)
	_, err = f.domain.UpdateWithOutcome(context.Background(), f.by, service.UpdateRequest{ID: ref, Fields: map[string]any{"title": "versioned overwrite"}, Expect: []model.Precondition{{Ref: ref, Revision: &version}}}, workflowClaimNextMeta("bead.update", 302))
	workflowBoundaryError(t, err, "invalid_params")
	unknown := model.RefByID(workflowplugin.ItemPrefix+strings.Repeat("0", 32), model.Natural64(0))
	workflowBoundaryError(t, workflowBoundaryCall(f, "bead.get", unknown, nil, workflowClaimNextMeta("bead.get", 303)), "not_found")
	after := workflowWriteCheckpoint(t, f)
	if before.Generation != after.Generation || !bytes.Equal(before.Payload, after.Payload) {
		t.Fatal("invalid or historical reference reached provider mutation")
	}
}

func TestWorkflowUnsupportedOptionsDoNotWriteOrReplay(t *testing.T) {
	for _, method := range []string{"x.workflow.invoke", "bead.claimNext", "bead.claim", "bead.comment", "bead.close"} {
		for _, phase := range []string{"live", "receipt-replay"} {
			for _, option := range []string{"dryRun", "expect"} {
				if option == "expect" && (method == "x.workflow.invoke" || method == "bead.claimNext") {
					continue
				}
				t.Run(method+"/"+phase+"/"+option, func(t *testing.T) {
					f := newWorkflowClaimNextFixture(t)
					ref := model.RefByID(f.invoke(t, 310, 2)[0].ID, model.Natural64(0))
					meta := workflowClaimNextMeta(method, 311)
					if phase == "receipt-replay" {
						if err := workflowBoundaryCall(f, method, ref, nil, meta); err != nil {
							t.Fatal(err)
						}
					}
					before := workflowWriteCheckpoint(t, f)
					var expect []model.Precondition
					if option == "dryRun" {
						meta.DryRun = true
					} else {
						revision := model.Natural64(1)
						expect = []model.Precondition{{Ref: ref, Revision: &revision}}
					}
					workflowBoundaryError(t, workflowBoundaryCall(f, method, ref, expect, meta), "invalid_params")
					after := workflowWriteCheckpoint(t, f)
					if before.Generation != after.Generation || !bytes.Equal(before.Payload, after.Payload) {
						t.Fatal("unsupported option changed workflow state, receipt or audit")
					}
				})
			}
		}
	}
}

func TestWorkflowItemWritesRequireProjectScope(t *testing.T) {
	for _, method := range []string{"bead.claim", "bead.comment", "bead.close"} {
		t.Run(method, func(t *testing.T) {
			f := newWorkflowClaimNextFixture(t)
			ref := model.RefByID(f.invoke(t, 320, 2)[0].ID, model.Natural64(0))
			before := workflowWriteCheckpoint(t, f)
			meta := workflowClaimNextMeta(method, 321)
			meta.Project = ""
			workflowBoundaryError(t, workflowBoundaryCall(f, method, ref, nil, meta), "project_required")
			after := workflowWriteCheckpoint(t, f)
			if before.Generation != after.Generation || !bytes.Equal(before.Payload, after.Payload) {
				t.Fatal("unscoped workflow write reached provider mutation")
			}
		})
	}
}

// A durable external edge remains subject to the Writer's authorization. This
// boundary probe makes any attempted workflow-authority/lookup path fail and
// retains the exact delegate refusal instead of granting a write in the test.
type externalWorkflowBoundaryDelegate struct {
	rpc.Service
	request service.LinkRequest
	called  bool
	refusal error
}

func (d *externalWorkflowBoundaryDelegate) LinkWithOutcome(_ context.Context, _ model.PrincipalRef, req service.LinkRequest, _ model.WriteMeta) (port.WriteOutcome, error) {
	d.called, d.request = true, req
	return port.WriteOutcome{}, d.refusal
}
func (d *externalWorkflowBoundaryDelegate) UnlinkWithOutcome(ctx context.Context, by model.PrincipalRef, req service.LinkRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	return d.LinkWithOutcome(ctx, by, req, meta)
}
func TestB14GuardedExternalWorkflowSpellingPreservesWriterRefusal(t *testing.T) {
	for _, target := range []string{"workflow:item/0123456789abcdef0123456789abcdef", "workflow:item/ opaque 外部 "} {
		for _, unlink := range []bool{false, true} {
			t.Run(target+map[bool]string{false: "/link", true: "/unlink"}[unlink], func(t *testing.T) {
				denied := &model.Error{Data: model.ErrorData{Code: "forbidden", Hint: "actual delegate refusal"}}
				d := &externalWorkflowBoundaryDelegate{refusal: denied}
				guard := &GuardedService{delegate: d}
				req := service.LinkRequest{From: model.RefByID("ordinary-source", model.Natural64(0)), To: model.RefByID(target, model.Natural64(0)), Kind: "blocks", TargetKind: model.LinkTargetExternal}
				var err error
				if unlink {
					_, err = guard.UnlinkWithOutcome(context.Background(), model.PrincipalRef{}, req, model.WriteMeta{})
				} else {
					_, err = guard.LinkWithOutcome(context.Background(), model.PrincipalRef{}, req, model.WriteMeta{})
				}
				if err != denied || !d.called || d.request.To.ID != target || d.request.TargetKind != model.LinkTargetExternal {
					t.Fatalf("opaque external lost Writer refusal/identity: called=%v request=%+v error=%v", d.called, d.request, err)
				}
			})
		}
	}
}
