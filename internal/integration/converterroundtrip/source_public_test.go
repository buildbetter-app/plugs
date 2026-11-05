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
	if err := domain.InstallSourceWorkflow(ctx, provider, *p.SourcePlanes, *report.SourcePlanes); err != nil {
		t.Fatal(err)
	}
	return domain, server
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
			_, server := sourcePublicService(t, store, lease, p, report, policy)
			actor := p.Admission.Actor
			get := sourcePublicCall(t, server, actor, "bead.get", 0, map[string]any{"id": "demo-7"}, "")
			if !bytes.Contains(get.Result, []byte("workflow:item/restored")) || !bytes.Contains(get.Result, []byte("Exact active collision")) {
				t.Fatalf("source route did not reach active item: %s", get.Result)
			}
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
			updated := sourcePublicCall(t, server, actor, "bead.update", 1, map[string]any{"id": "demo-7", "fields": map[string]any{"title": "public active title"}}, "")
			if !reflect.DeepEqual(core, savedLabelImport(t, store, report.Archive, p, "")) {
				t.Fatal("active-only public write touched core/history/derived data")
			}
			later, err := capability.LoadCheckpoint(ctx, namespace)
			if err != nil || later.Generation != initial.Generation+1 {
				t.Fatal("active-only generation", err)
			}
			replayed := sourcePublicCall(t, server, actor, "bead.update", 1, map[string]any{"id": "demo-7", "fields": map[string]any{"title": "public active title"}}, "")
			if !replayed.Meta.Replayed || !bytes.Equal(replayed.Result, updated.Result) {
				t.Fatal("canonical source replay changed result")
			}
			unchanged, _ := capability.LoadCheckpoint(ctx, namespace)
			if !reflect.DeepEqual(unchanged, later) {
				t.Fatal("source replay republished namespace")
			}
			policy.deny = true
			sourcePublicCall(t, server, actor, "bead.update", 1, map[string]any{"id": "demo-7", "fields": map[string]any{"title": "public active title"}}, "forbidden")
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
			closed := sourcePublicCall(t, server, actor, "bead.close", 4, map[string]any{"id": "demo-7", "reason": "public source complete"}, "")
			fallback := sourcePublicCall(t, server, actor, "bead.get", 0, map[string]any{"id": "demo-7"}, "")
			if bytes.Contains(fallback.Result, []byte("workflow:item/restored")) || bytes.Contains(fallback.Result, []byte("committed mixed active")) {
				t.Fatal("completed source did not fall back to its durable collision")
			}
			closedReplay := sourcePublicCall(t, server, actor, "bead.close", 4, map[string]any{"id": "demo-7", "reason": "public source complete"}, "")
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
