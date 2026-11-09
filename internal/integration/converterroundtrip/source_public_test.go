package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

type sourcePublicPolicy struct {
	deny bool
	seen int
}

func (p *sourcePublicPolicy) Authorize(_ context.Context, _ port.AuthorizationStage, proposal port.ProposedChange, _ port.SnapshotReader) error {
	for _, change := range proposal.Changes {
		if change.Resource.Kind != "bead" && change.Resource.Kind != "workflow_item" {
			return errors.New("unexpected proposal kind")
		}
	}
	p.seen++
	if p.deny {
		return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
	}
	return nil
}

func sourcePublicService(t *testing.T, store driver.Store, lease driver.ActiveLease, p driver.RestoreImportProfile, report Report, policy port.Authorizer) (*service.Service, *rpc.Server) {
	t.Helper()
	ctx := context.Background()
	ontology := service.NewOntology(nil)
	writer := pipeline.New(store, lease, pipeline.WithOntology(ontology), pipeline.WithAuthorizer(policy))
	domain := service.New(store, lease, writer, service.WithOntology(ontology))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{DefaultProject: "demo", Incarnation: report.Restored.Incarnation, AuthMode: "authenticated"})
	if err != nil {
		t.Fatal(err)
	}
	// Installation deliberately follows transport binding on the same object.
	capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
	if err != nil {
		t.Fatal(err)
	}
	state, err := pluginhost.NewSourceRuntimeState(ctx, capability, lease, report.SourcePlanes.Providers[0])
	if err != nil {
		t.Fatal(err)
	}
	provider, err := workflowplugin.NewSourceRuntime(state, report.SourcePlanes.Providers[0])
	if err != nil {
		t.Fatal(err)
	}
	if report.Receipt == nil {
		t.Fatal("source import has no original receipt")
	}
	if err := domain.InstallSourceWorkflow(ctx, provider, p, *report.SourcePlanes, *report.Receipt); err != nil {
		t.Fatal(err)
	}
	return domain, server
}

func assertSourceCommentAttribution(t *testing.T, result []byte, p driver.RestoreImportProfile, report Report) {
	t.Helper()
	var value struct {
		Comments []struct {
			Body         string             `json:"body"`
			Principal    model.PrincipalRef `json:"principal"`
			Time         time.Time          `json:"time"`
			Origin       string             `json:"origin"`
			SourceAuthor string             `json:"sourceAuthor"`
			SourceTime   time.Time          `json:"sourceTime"`
		} `json:"comments"`
	}
	decode(t, result, &value)
	if len(value.Comments) != 1 || report.Receipt == nil {
		t.Fatalf("missing imported source comment or original receipt: %s", result)
	}
	comment := value.Comments[0]
	if comment.Body != "raw active comment" || comment.Origin != "import" || !reflect.DeepEqual(comment.Principal, p.Admission.Actor) || !comment.Time.Equal(report.Receipt.Changeset.Time) || comment.SourceAuthor != "unverified source author" || !comment.SourceTime.Equal(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)) || comment.Time.Equal(comment.SourceTime) || comment.Principal.ID == comment.SourceAuthor {
		t.Fatalf("verified importer/restore time were conflated with raw source attribution: %s", result)
	}
}

type sourcePublicReply struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data struct {
			Code string `json:"code"`
		} `json:"data"`
	} `json:"error"`
	Meta struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
}

func sourcePublicCall(t *testing.T, server *rpc.Server, actor model.PrincipalRef, method string, key int, params map[string]any, want string) sourcePublicReply {
	t.Helper()
	meta := map[string]any{"project": "demo"}
	if key != 0 {
		meta["idempotencyKey"] = fmt.Sprintf("%032d", key)
	}
	params["_meta"] = meta
	request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	raw := server.Dispatch(rpc.WithPrincipal(context.Background(), actor), request)
	var reply sourcePublicReply
	if err := json.Unmarshal(raw, &reply); err != nil {
		t.Fatal(err)
	}
	if want != "" {
		if reply.Error == nil || reply.Error.Data.Code != want || len(reply.Result) != 0 {
			t.Fatalf("%s want %s: %s", method, want, raw)
		}
	} else if reply.Error != nil {
		t.Fatalf("%s: %s", method, raw)
	}
	return reply
}

func TestJointSourcePublicRoutingBatchReplayAndReopen(t *testing.T) {
	data := jointSourceFixture(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			store, lease, reopen := openIdentityTarget(t, engine)
			p := jointProfile(t, data, store, lease)
			report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, p)
			if err != nil {
				t.Fatal(err)
			}
			policy := &sourcePublicPolicy{}
			domain, server := sourcePublicService(t, store, lease, p, report, policy)
			actor := p.Admission.Actor
			// This service has no native provider. Combined Ready must select the
			// installed source provider, including the active/durable alias collision.
			for _, filtered := range []bool{false, true} {
				request := service.ReadyRequest{Project: "demo", Limit: 1000}
				var ready service.BeadPage
				if filtered {
					ready, err = domain.ReadyWithFilter(ctx, actor, request, nil)
				} else {
					ready, err = domain.Ready(ctx, actor, request)
				}
				if err != nil {
					t.Fatalf("source-only combined Ready: %v", err)
				}
				assertSourceReady(t, ready)
			}
			get := sourcePublicCall(t, server, actor, "bead.get", 0, map[string]any{"id": model.RefByID("demo-7", model.Natural64(0))}, "")
			if !bytes.Contains(get.Result, []byte("workflow:item/restored")) || !bytes.Contains(get.Result, []byte("Exact active collision")) {
				t.Fatalf("source route did not reach active item: %s", get.Result)
			}
			assertSourceCommentAttribution(t, get.Result, p, report)
			capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
			if err != nil {
				t.Fatal(err)
			}
			namespace := report.SourcePlanes.Providers[0].Provider.Namespace
			initial, err := capability.LoadCheckpoint(ctx, namespace)
			if err != nil {
				t.Fatal(err)
			}
			core := savedLabelImport(t, store, report.Archive, p, "")
			state, err := pluginhost.NewSourceRuntimeState(ctx, capability, lease, report.SourcePlanes.Providers[0])
			if err != nil {
				t.Fatal(err)
			}
			provider, err := workflowplugin.NewSourceRuntime(state, report.SourcePlanes.Providers[0])
			if err != nil {
				t.Fatal(err)
			}
			for _, bad := range []string{"hash", "pending", "time", "actor", "key", "incarnation", "evidence"} {
				t.Run("install-refuses-"+bad, func(t *testing.T) {
					var receipt driver.IdempotencyRecord
					decode(t, encode(t, report.Receipt), &receipt)
					switch bad {
					case "hash":
						receipt.RequestHash = strings.Repeat("f", 64)
					case "pending":
						receipt.ResponsePending = true
					case "time":
						receipt.Changeset.Time = time.Time{}
					case "actor":
						receipt.Changeset.Principal = model.PrincipalRef{Kind: "user", ID: "user:other-importer"}
					case "key":
						receipt.Changeset.IdempotencyKey = strings.Repeat("f", 32)
					case "incarnation":
						receipt.Changeset.Incarnation = "other-incarnation"
					case "evidence":
						receipt.Commit.Results = nil
					}
					if err := domain.InstallSourceWorkflow(ctx, provider, p, *report.SourcePlanes, receipt); !errors.Is(err, driver.ErrConflict) {
						t.Fatalf("invalid import attribution was installed: %v", err)
					}
				})
			}
			still, err := capability.LoadCheckpoint(ctx, namespace)
			if err != nil || !reflect.DeepEqual(initial, still) || !reflect.DeepEqual(core, savedLabelImport(t, store, report.Archive, p, "")) {
				t.Fatal("refused attribution binding changed the source namespace or core", err)
			}
			updated := sourcePublicCall(t, server, actor, "bead.update", 1, map[string]any{"id": model.RefByID("demo-7", model.Natural64(0)), "fields": map[string]any{"title": "public active title"}}, "")
			if !reflect.DeepEqual(core, savedLabelImport(t, store, report.Archive, p, "")) {
				t.Fatal("active-only public write touched core/history/derived data")
			}
			later, err := capability.LoadCheckpoint(ctx, namespace)
			if err != nil || later.Generation != initial.Generation+1 {
				t.Fatal("active-only generation", err)
			}
			replayed := sourcePublicCall(t, server, actor, "bead.update", 1, map[string]any{"id": model.RefByID("demo-7", model.Natural64(0)), "fields": map[string]any{"title": "public active title"}}, "")
			if !replayed.Meta.Replayed || !bytes.Equal(replayed.Result, updated.Result) {
				t.Fatal("canonical source replay changed result")
			}
			unchanged, _ := capability.LoadCheckpoint(ctx, namespace)
			if !reflect.DeepEqual(unchanged, later) {
				t.Fatal("source replay republished namespace")
			}
			policy.deny = true
			sourcePublicCall(t, server, actor, "bead.update", 1, map[string]any{"id": model.RefByID("demo-7", model.Natural64(0)), "fields": map[string]any{"title": "public active title"}}, "forbidden")
			policy.deny = false
			beforeFailure := savedLabelImport(t, store, report.Archive, p, "")
			bad := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID("demo-7", model.Natural64(0)), Fields: map[string]any{"title": "discard active draft"}}}, {Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID("missing", model.Natural64(0)), Fields: map[string]any{"title": "absent"}}}}}
			sourcePublicCall(t, server, actor, "tx.apply", 2, map[string]any{"changeSet": bad}, "not_found")
			afterFailure, _ := capability.LoadCheckpoint(ctx, namespace)
			if !reflect.DeepEqual(beforeFailure, savedLabelImport(t, store, report.Archive, p, "")) || !reflect.DeepEqual(later, afterFailure) {
				t.Fatal("late ordinary refusal published part of the mixed proposal")
			}
			mixed := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID("demo-7", model.Natural64(0)), Fields: map[string]any{"title": "committed mixed active"}}}, {Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID("demo-8", model.Natural64(0)), Fields: map[string]any{"title": "committed mixed ordinary"}}}}}
			mixedReply := sourcePublicCall(t, server, actor, "tx.apply", 3, map[string]any{"changeSet": mixed}, "")
			var commit model.Commit
			decode(t, mixedReply.Result, &commit)
			if len(commit.Results) != 2 || commit.Results[0].Ref.ID != "workflow:item/restored" || commit.Results[1].Ref.ID != "demo-8" {
				t.Fatal("lost original mixed result order", commit)
			}
			both, _ := capability.LoadCheckpoint(ctx, namespace)
			meta, _ := store.Meta(ctx)
			if both.Generation != later.Generation+1 || meta.Head.String() != "2" {
				t.Fatal("mixed proposal did not publish once in both planes", both.Generation, meta.Head)
			}
			currentGet := sourcePublicCall(t, server, actor, "bead.get", 0, map[string]any{"id": model.RefByID("demo-7", model.Natural64(0))}, "")
			assertSourceCommentAttribution(t, currentGet.Result, p, report)
			closed := sourcePublicCall(t, server, actor, "bead.close", 4, map[string]any{"id": model.RefByID("demo-7", model.Natural64(0)), "reason": "public source complete"}, "")
			sourcePublicCall(t, server, actor, "bead.get", 0, map[string]any{"id": model.RefByID("workflow:item/restored", model.Natural64(0))}, "not_found")
			fallback := sourcePublicCall(t, server, actor, "bead.get", 0, map[string]any{"id": model.RefByID("demo-7", model.Natural64(0))}, "")
			if bytes.Contains(fallback.Result, []byte("workflow:item/restored")) || bytes.Contains(fallback.Result, []byte("committed mixed active")) {
				t.Fatal("completed source did not fall back to its durable collision")
			}
			closedReplay := sourcePublicCall(t, server, actor, "bead.close", 4, map[string]any{"id": model.RefByID("demo-7", model.Natural64(0)), "reason": "public source complete"}, "")
			if !closedReplay.Meta.Replayed || !bytes.Equal(closed.Result, closedReplay.Result) {
				t.Fatal("completed active replay was routed to durable collision")
			}
			if policy.seen < 6 {
				t.Fatal("public writes bypassed writer authorization")
			}
			store, lease = reopen()
			bound, recovered, found, err := RecoverBoundSourceImport(ctx, store)
			if err != nil || !found || !reflect.DeepEqual(recovered.Receipt, report.Receipt) {
				t.Fatal("protected source recovery", err)
			}
			_, server = sourcePublicService(t, store, lease, bound, recovered, policy)
			retry := sourcePublicCall(t, server, actor, "tx.apply", 3, map[string]any{"changeSet": mixed}, "")
			if !retry.Meta.Replayed || !bytes.Equal(retry.Result, mixedReply.Result) {
				t.Fatal("reopened mixed receipt was not canonical")
			}
			if strings.Contains(string(retry.Result), "discard active draft") {
				t.Fatal("failed draft escaped into recovered receipt")
			}
		})
	}
}

func assertSourceReady(t *testing.T, page service.BeadPage) {
	t.Helper()
	if page.Next != nil || page.WorkflowSnapshot == nil || page.ReadyMembership == "" {
		t.Fatal("source Ready lost its complete combined snapshot")
	}
	matched := 0
	for _, item := range page.Items {
		if item.ID == "workflow:item/restored" {
			matched++
			if item.Fields["title"] != "Exact active collision" || item.Kind != "workflow.item" || item.Project != "demo" {
				t.Fatalf("source Ready substituted a native or durable collision: %+v", item)
			}
		}
	}
	if matched != 1 {
		t.Fatalf("source Ready must include the exact active target once: %+v", page.Items)
	}
}

func TestJointSourcePublicOrdinaryRelationsAndActiveLifecycle(t *testing.T) {
	data := jointSourceFixture(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			store, lease, _ := openIdentityTarget(t, engine)
			p := jointProfile(t, data, store, lease)
			report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, p)
			if err != nil {
				t.Fatal(err)
			}
			_, server := sourcePublicService(t, store, lease, p, report, &sourcePublicPolicy{})
			actor := p.Admission.Actor
			capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
			if err != nil {
				t.Fatal(err)
			}
			namespace := report.SourcePlanes.Providers[0].Provider.Namespace
			initial, err := capability.LoadCheckpoint(ctx, namespace)
			if err != nil {
				t.Fatal(err)
			}
			var refs []model.Ref
			for i, title := range []string{"ordinary relation source", "ordinary relation target"} {
				created := sourcePublicCall(t, server, actor, "bead.create", 10+i, map[string]any{"type": "issue", "fields": map[string]any{"title": title}}, "")
				var commit model.Commit
				decode(t, created.Result, &commit)
				if len(commit.Results) != 1 {
					t.Fatal("ordinary create result", commit)
				}
				refs = append(refs, commit.Results[0].Ref)
			}
			link := sourcePublicCall(t, server, actor, "bead.link", 12, map[string]any{"from": refs[0], "to": refs[1], "kind": "related"}, "")
			replay := sourcePublicCall(t, server, actor, "bead.link", 12, map[string]any{"from": refs[0], "to": refs[1], "kind": "related"}, "")
			if !replay.Meta.Replayed || !bytes.Equal(link.Result, replay.Result) {
				t.Fatal("ordinary named link lost canonical replay after source installation")
			}
			sourcePublicCall(t, server, actor, "bead.unlink", 13, map[string]any{"from": refs[0], "to": refs[1], "kind": "related"}, "")
			// An external ID remains opaque even when it equals a live source alias.
			sourcePublicCall(t, server, actor, "bead.link", 14, map[string]any{"from": refs[0], "to": model.RefByID("demo-7", model.Natural64(0)), "targetKind": "external", "kind": "related"}, "")
			reader, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			rows, readErr := reader.Get(ctx, refs[:1])
			_ = reader.Close()
			if readErr != nil || len(rows) != 1 {
				t.Fatal("ordinary relation readback", readErr)
			}
			doc, err := pipeline.DecodeDocument(rows[0].Body)
			if err != nil || len(doc.Links) != 1 || doc.Links[0].To.ID != "demo-7" || doc.Links[0].TargetKind != model.LinkTargetExternal {
				t.Fatal("ordinary/external relation was routed into active state", doc.Links, err)
			}
			sourcePublicCall(t, server, actor, "bead.close", 15, map[string]any{"id": refs[1], "reason": "ordinary named close"}, "")
			sourcePublicCall(t, server, actor, "bead.close", 16, map[string]any{"id": refs[1]}, "")
			unchanged, err := capability.LoadCheckpoint(ctx, namespace)
			if err != nil || !reflect.DeepEqual(initial, unchanged) {
				t.Fatal("ordinary named operations changed the source namespace", err)
			}
			core := savedLabelImport(t, store, report.Archive, p, "")
			comment := sourcePublicCall(t, server, actor, "bead.comment", 17, map[string]any{"id": model.RefByID("demo-7", model.Natural64(0)), "body": "public active comment"}, "")
			commentReplay := sourcePublicCall(t, server, actor, "bead.comment", 17, map[string]any{"id": model.RefByID("demo-7", model.Natural64(0)), "body": "public active comment"}, "")
			if !commentReplay.Meta.Replayed || !bytes.Equal(comment.Result, commentReplay.Result) {
				t.Fatal("active comment receipt was not canonical")
			}
			claimed := sourcePublicCall(t, server, actor, "bead.claim", 18, map[string]any{"id": model.RefByID("demo-7", model.Natural64(0))}, "")
			claimedReplay := sourcePublicCall(t, server, actor, "bead.claim", 18, map[string]any{"id": model.RefByID("demo-7", model.Natural64(0))}, "")
			if !claimedReplay.Meta.Replayed || !bytes.Equal(claimed.Result, claimedReplay.Result) {
				t.Fatal("active claim receipt was not canonical")
			}
			if !reflect.DeepEqual(core, savedLabelImport(t, store, report.Archive, p, "")) {
				t.Fatal("active comment/claim changed core records/history/derived data")
			}
			active, err := capability.LoadCheckpoint(ctx, namespace)
			if err != nil || active.Generation != initial.Generation+2 {
				t.Fatal("active comment/claim did not each publish once", err)
			}
		})
	}
}
