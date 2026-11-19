package batchsql_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type hierarchyNamedReply struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data    model.ErrorData `json:"data"`
		Message string          `json:"message"`
	} `json:"error"`
	Meta struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
}

func hierarchyNamedSummary(reply hierarchyNamedReply) string {
	if reply.Error == nil {
		return fmt.Sprintf("result=%s replay=%t error=<nil>", reply.Result, reply.Meta.Replayed)
	}
	return fmt.Sprintf("result=%s replay=%t error={code=%q message=%q retryable=%t}", reply.Result, reply.Meta.Replayed, reply.Error.Data.Code, reply.Error.Message, reply.Error.Data.Retryable)
}

func TestRPCResponseDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want string
	}{
		{"success", `{"result":{"id":"bead"},"_meta":{"replayed":true}}`, `result={"id":"bead"} replay=true error=<nil>`},
		{"retryable", `{"error":{"message":"authorization unavailable","data":{"code":"authz_unavailable","retryable":true}},"_meta":{"replayed":false}}`, `result= replay=false error={code="authz_unavailable" message="authorization unavailable" retryable=true}`},
		{"refusal", `{"error":{"message":"denied","data":{"code":"forbidden","retryable":false}}}`, `result= replay=false error={code="forbidden" message="denied" retryable=false}`},
	} {
		var reply hierarchyNamedReply
		if err := json.Unmarshal([]byte(tc.raw), &reply); err != nil {
			t.Fatalf("%s decode: %v", tc.name, err)
		}
		if got := hierarchyNamedSummary(reply); got != tc.want {
			t.Errorf("%s diagnostics=%q want=%q", tc.name, got, tc.want)
		}
	}
}

// Exercise the real EP2 checker on EP3's complete ordered Writer proposal and
// persisted graph, rather than constructing proposed images in a policy test.
// Authentication and the public RPC adapter are separate acceptance boundaries.
func TestHierarchyFixedAuthorizationAtWriterAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			seed := hierarchyApply(t, f,
				hierarchyCreate("old", nil, "old-project"),
				hierarchyCreate("new", nil, "parent-project"),
				hierarchyCreate("other", nil, "parent-project"),
				hierarchyCreate("secret", nil, "secret-project"),
				hierarchyCreate("child", hierarchyRef("old"), "child-project"),
			)
			child, parent, other, secret := seed.Results[4].Ref, seed.Results[1].Ref, seed.Results[2].Ref, seed.Results[3].Ref
			meta, err := f.store.Meta(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))}
			base := authzfixed.Facts{Namespace: ns}
			for _, project := range []string{"child-project", "parent-project", "old-project", "secret-project"} {
				base.Projects = append(base.Projects, authzfixed.Project{Ref: model.RefByID(project, model.Natural64(0)), Name: project, InstallationID: "fixed", Active: true})
			}
			for _, project := range []string{"child-project", "parent-project"} {
				base.Grants = append(base.Grants, authz.Grant{Subject: authz.SubjectRef{Kind: "user", ID: "user:operator"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: model.RefByID(project, model.Natural64(0))}, Role: "involved"})
			}
			facts, err := authzrpc.NewMutableFacts(base)
			if err != nil {
				t.Fatal(err)
			}
			provider := authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
			composition := func(enabled bool) *authzrpc.Composition {
				app, err := authzrpc.New(f.ctx, authzrpc.Config{Store: f.store, Lease: f.lease, Provider: provider, Facts: facts, Namespace: ns, DefaultProject: "child-project", Hierarchy: enabled})
				if err != nil {
					t.Fatal(err)
				}
				return app
			}
			writer := func(enabled bool) *pipeline.Writer {
				app := composition(enabled)
				return pipeline.New(f.store, f.lease, pipeline.WithOntology(f.ontology), pipeline.WithAuthorizer(app.Checker), pipeline.WithEventContext(app.Checker), pipeline.WithClock(f.now), pipeline.WithIDGenerator(f.nextID))
			}
			f.writer = writer(true)
			errorData := func(err error) any {
				var typed *model.Error
				if errors.As(err, &typed) {
					return typed.Data
				}
				return err
			}
			by := model.PrincipalRef{Kind: "agent", ID: "agent:hierarchy-worker", Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:operator"}}
			requestMeta := func(label string) model.WriteMeta {
				m := f.meta(label)
				m.Project = "child-project"
				return m
			}
			receipt := func(m model.WriteMeta) *driver.IdempotencyRecord {
				principal, err := driver.PrincipalKey(by)
				if err != nil {
					t.Fatal(err)
				}
				entry, err := hierarchyReader(t, f).IdempotencyByKey(f.ctx, principal, "", m.Method, m.IdempotencyKey)
				if err != nil {
					t.Fatal(err)
				}
				return entry
			}
			// Exercise the actual authenticated named HTTP route, including its
			// service lowering, rather than substituting the direct tx.apply seam.
			issued, err := provider.Issue(by, time.Now().Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			namedCreate := func(app *authzrpc.Composition, destination model.Ref, key string) hierarchyNamedReply {
				t.Helper()
				body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "bead.create", "params": map[string]any{
					"type": "issue", "fields": map[string]any{"title": "named child"}, "parent": destination,
					"_meta": map[string]any{"project": "child-project", "idempotencyKey": key},
				}})
				if err != nil {
					t.Fatal(err)
				}
				req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/rpc/v1", bytes.NewReader(body))
				req.RemoteAddr = "127.0.0.1:12345"
				req.Header.Set("Plug-RPC", "1")
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", "Bearer "+issued.Token)
				recorder := httptest.NewRecorder()
				app.Handler.ServeHTTP(recorder, req)
				var reply hierarchyNamedReply
				if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &reply) != nil {
					t.Fatalf("named create HTTP response: %d %s", recorder.Code, recorder.Body.Bytes())
				}
				return reply
			}
			namedApp := composition(true)
			namedMeta := requestMeta("named-parent-create")
			namedMeta.Method = "bead.create"
			refuseNamed := func(app *authzrpc.Composition, destination model.Ref, key string, code model.ErrorCode) {
				t.Helper()
				before, rows, predicates := f.state(t), hierarchyRows(t, f), hierarchyFacts(t, f)
				reply := namedCreate(app, destination, key)
				if reply.Error == nil || reply.Error.Data.Code != code {
					t.Fatalf("named create refusal: %s, want %s", hierarchyNamedSummary(reply), code)
				}
				requireSameState(t, before, f.state(t))
				if !reflect.DeepEqual(rows, hierarchyRows(t, f)) || !reflect.DeepEqual(predicates, hierarchyFacts(t, f)) {
					t.Fatal("refused named create published closure or predicates")
				}
			}
			refuseNamed(composition(false), parent, namedMeta.IdempotencyKey, "forbidden")
			refuseNamed(namedApp, secret, requestMeta("named-hidden-parent").IdempotencyKey, "forbidden")
			refuseNamed(namedApp, model.RefByKey("unresolved"), requestMeta("named-alias-parent").IdempotencyKey, "invalid_params")
			if receipt(namedMeta) != nil {
				t.Fatal("refused named create left a receipt")
			}
			named := namedCreate(namedApp, parent, namedMeta.IdempotencyKey)
			var namedCommit model.Commit
			if named.Error != nil || json.Unmarshal(named.Result, &namedCommit) != nil || len(namedCommit.Results) != 1 {
				t.Fatalf("authorized named parent create: %s", hierarchyNamedSummary(named))
			}
			namedDoc := decodeDocument(t, f.state(t).Records[namedCommit.Results[0].Ref.ID])
			if namedDoc.Project != "child-project" || namedDoc.Parent == nil || namedDoc.Parent.ID != parent.ID {
				t.Fatalf("named create lost parent or child ownership: %+v", namedDoc)
			}
			if replay := namedCreate(namedApp, parent, namedMeta.IdempotencyKey); replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(replay.Result, named.Result) {
				t.Fatalf("named parent create replay differs: %s", hierarchyNamedSummary(replay))
			}
			verifyHierarchy(t, f)
			batch := model.ChangeSet{Ops: []model.Op{
				hierarchyCreate("epic", nil, "parent-project"),
				hierarchyCreate("task", hierarchyRef("epic"), "child-project"),
			}}
			created, err := f.writer.Apply(f.ctx, by, batch, requestMeta("authorized-ordered-create"))
			if err != nil {
				t.Fatalf("authorized same-batch hierarchy create: %v (%+v)", err, errorData(err))
			}
			doc := decodeDocument(t, f.state(t).Records[created.Results[1].Ref.ID])
			if doc.Project != "child-project" || doc.Parent == nil || doc.Parent.ID != created.Results[0].Ref.ID {
				t.Fatalf("authorized create changed project or lost resolved parent: %+v", doc)
			}
			move := model.ChangeSet{Ops: []model.Op{hierarchyMove(child, &parent)}}
			moveMeta := requestMeta("authorized-move")
			moved, err := f.writer.Apply(f.ctx, by, move, moveMeta)
			if err != nil {
				t.Fatalf("move required permission on the old parent: %v (%+v)", err, errorData(err))
			}
			verifyHierarchy(t, f)
			replayed, err := f.writer.ApplyWithOutcome(f.ctx, by, move, moveMeta)
			if err != nil || !replayed.Replayed || !reflect.DeepEqual(replayed.Commit, moved) {
				t.Fatalf("authorized hierarchy replay: %+v %v", replayed, err)
			}
			refuse := func(label string, cs model.ChangeSet, m model.WriteMeta) {
				t.Helper()
				before, rows, predicates := f.state(t), hierarchyRows(t, f), hierarchyFacts(t, f)
				_, err := f.writer.Apply(f.ctx, by, cs, m)
				requireCode(t, err, "forbidden")
				requireSameState(t, before, f.state(t))
				if !reflect.DeepEqual(rows, hierarchyRows(t, f)) || !reflect.DeepEqual(predicates, hierarchyFacts(t, f)) {
					t.Fatalf("%s published closure or predicates", label)
				}
			}
			refuse("hidden destination", model.ChangeSet{Ops: []model.Op{hierarchyMove(child, &secret)}}, requestMeta("hidden-destination"))
			for _, missing := range []string{"child-project", "parent-project"} {
				revoked := base
				revoked.Grants = nil
				for _, grant := range base.Grants {
					if grant.Scope.Ref.ID != missing {
						revoked.Grants = append(revoked.Grants, grant)
					}
				}
				if err := facts.Replace(revoked); err != nil {
					t.Fatal(err)
				}
				refuse("missing "+missing, model.ChangeSet{Ops: []model.Op{hierarchyMove(child, &other)}}, requestMeta("revoked-move"))
				refuse("revoked replay "+missing, move, moveMeta)
				refuseNamed(namedApp, parent, requestMeta("named-missing-grant").IdempotencyKey, "forbidden")
				refuseNamed(namedApp, parent, namedMeta.IdempotencyKey, "forbidden")
				if entry := receipt(namedMeta); entry == nil || !reflect.DeepEqual(entry.Commit, namedCommit) {
					t.Fatal("revoked named create replay modified the original receipt")
				}
				if entry := receipt(moveMeta); entry == nil || !reflect.DeepEqual(entry.Commit, moved) {
					t.Fatal("revoked replay modified the original receipt")
				}
			}
			if err := facts.Replace(base); err != nil {
				t.Fatal(err)
			}
			f.writer = writer(false)
			disabledMeta := requestMeta("disabled-hierarchy")
			disabledMeta.Method = "bead.reparent"
			refuse("default fixed permissions", model.ChangeSet{Ops: []model.Op{hierarchyMove(child, &other)}}, disabledMeta)
			verifyHierarchy(t, f)
			t.Logf("hierarchy %s: fixed checker authorized ordered alias create and move without old-parent permission; child ownership, both edit grants, revocation/replay, and default-off refusal preserved", engine)
		})
	}
}
