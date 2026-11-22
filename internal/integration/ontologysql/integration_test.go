package ontologysql_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/ontologyops"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/ontologyrpc"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const postgresImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"

var postgresDSN string

var fixturePrincipal = model.PrincipalRef{Kind: "system", ID: "ontology-sql-fixture"}
var caller = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}

// The SQL lane owns its engine and has no absent-engine or memory fallback.
// SQLite is opened per subtest; PostgreSQL is one disposable database for the
// package and the matrix has one isolated fixture run against it.
func TestMain(m *testing.M) {
	code := func() int {
		id, err := randomID()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		name := "plug-swarm-v3-ontology-sql-" + id
		out, err := docker("run", "-d", "--rm", "--label", "plug.test.run="+os.Getenv("PLUG_TEST_CONTAINER_LABEL"), "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_test", "-p", "127.0.0.1::5432", postgresImage)
		if err != nil {
			fmt.Fprintf(os.Stderr, "real PostgreSQL start failed: %v %s\n", err, out)
			return 1
		}
		defer func() {
			if out, err := docker("rm", "-f", "-v", name); err != nil {
				fmt.Fprintf(os.Stderr, "PostgreSQL cleanup failed: %v %s\n", err, out)
			}
		}()
		out, err = docker("port", name, "5432/tcp")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		addr := strings.TrimSpace(string(out))
		host, _, err := net.SplitHostPort(addr)
		if err != nil || host != "127.0.0.1" {
			fmt.Fprintf(os.Stderr, "PostgreSQL must bind loopback, got %q\n", addr)
			return 1
		}
		postgresDSN = "postgres://postgres@" + addr + "/plug_test?sslmode=disable"
		db, err := sql.Open("pgx", postgresDSN)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for {
			if err = db.PingContext(ctx); err == nil {
				break
			}
			if ctx.Err() != nil {
				fmt.Fprintln(os.Stderr, "real PostgreSQL never became ready")
				return 1
			}
			time.Sleep(30 * time.Millisecond)
		}
		var version string
		if err = db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		fmt.Printf("Real engine: %s\nImage: %s\n", version, postgresImage)
		return m.Run()
	}()
	os.Exit(code)
}

func randomID() (string, error) {
	// The storage package's IDs are intentionally opaque; a timestamp plus the
	// process ID is sufficient for a disposable container name and avoids a
	// second credential or fixture mechanism.
	return fmt.Sprintf("%x-%d", time.Now().UnixNano(), os.Getpid()), nil
}

func docker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "DOCKER_HOST=") && !strings.HasPrefix(value, "DOCKER_CONTEXT=") && !strings.HasPrefix(value, "DOCKER_TLS=") && !strings.HasPrefix(value, "DOCKER_TLS_VERIFY=") && !strings.HasPrefix(value, "DOCKER_CERT_PATH=") {
			cmd.Env = append(cmd.Env, value)
		}
	}
	return cmd.CombinedOutput()
}

type sqlFixture struct {
	t         *testing.T
	ctx       context.Context
	engine    string
	store     driver.Store
	lease     driver.ActiveLease
	writer    *pipeline.Writer
	domain    *service.Service
	core      *ontologyops.Service
	registry  *rpc.Registry
	transport *inprocess.Transport
	source    model.Ref
	base      ontology.Document
	overlay   ontology.Document
	current   service.OntologySource
}

func newFixture(t *testing.T, engine string) *sqlFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	f := &sqlFixture{t: t, ctx: ctx, engine: engine, source: model.Ref{ID: "ontology/" + engine}, base: ontology.DefaultDocument()}
	f.overlay = f.baseOverlay()
	f.current = service.OntologySource{Base: f.base, Overlays: map[string]ontology.Document{"overlay-project": f.overlay}}
	var err error
	switch engine {
	case "sqlite":
		f.store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "ontology.db")})
	case "postgres":
		f.store, err = postgres.Open(ctx, driver.Config{DSN: postgresDSN})
	default:
		t.Fatalf("unknown SQL engine %q", engine)
	}
	if err != nil {
		t.Fatalf("open %s: %v", engine, err)
	}
	t.Cleanup(func() {
		if err := f.store.Close(); err != nil {
			t.Error(err)
		}
	})
	f.lease, err = f.store.Lease(ctx, "ontology-sql-"+engine, driver.LeaseTTL)
	if err != nil {
		t.Fatalf("lease %s: %v", engine, err)
	}
	f.seedSource(f.current, model.OpCreate)
	compiled, problems := ontology.Compile(f.current.Base, f.current.Overlays, f.current.Plugins)
	if len(problems) != 0 {
		t.Fatalf("fixture ontology does not compile: %+v", problems)
	}
	workflow := service.NewOntology(compiled.Effective)
	f.writer = pipeline.New(f.store, f.lease, pipeline.WithOntology(workflow))
	f.domain = service.New(f.store, f.lease, f.writer, service.WithOntology(workflow))
	f.core, err = ontologyops.New(f.store, f.lease, ontologyops.Config{
		Source:        service.StaticOntologySource(f.source),
		ValidatingPre: ontologyops.NoValidatingPre(),
	})
	if err != nil {
		t.Fatal(err)
	}
	f.registry, err = rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := ontologyrpc.Bind(f.registry, f.core); err != nil {
		t.Fatalf("ontology RPC bind: %v", err)
	}
	if err := rpc.BindService(f.registry, f.domain); err != nil {
		t.Fatalf("service RPC bind: %v", err)
	}
	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(f.registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "own-project", OntologyRevision: model.Natural64(1)})
	if err != nil {
		t.Fatal(err)
	}
	f.transport = inprocess.New(server)
	t.Logf("%s fixture uses store=%s incarnation=%s source=%s", engine, meta.StoreID, meta.Incarnation, f.source.ID)
	return f
}

func (f *sqlFixture) baseOverlay() ontology.Document {
	typ := f.base.Types["issue"]
	typ.Name = "Overlay task"
	if typ.Fields == nil {
		typ.Fields = map[string]ontology.Field{}
	}
	typ.Fields["tenant_tag"] = ontology.Field{Type: "string"}
	return ontology.Document{Types: map[string]ontology.Type{"issue": typ}}
}

func (f *sqlFixture) seedSource(source service.OntologySource, op model.OpKind) {
	f.t.Helper()
	body, err := service.MarshalOntologySource(source)
	if err != nil {
		f.t.Fatal(err)
	}
	f.seedRawSource(body, op)
	f.current = source
}

func (f *sqlFixture) seedRawSource(body []byte, op model.OpKind) {
	f.t.Helper()
	_, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{
		Kind:       model.ChangesetWrite,
		Principal:  fixturePrincipal,
		Method:     "fixture.ontology.source",
		AgentLabel: "ontology-sql",
		Time:       time.Date(2026, 10, 29, 15, 0, 0, 0, time.UTC),
	}, []conformance.FixtureChange{{Ref: f.source, Kind: "ontology", Op: op, Body: body, ChangedPaths: []string{"base", "overlays"}}})
	if err != nil {
		f.t.Fatalf("seed protected source: %v", err)
	}
}

func (f *sqlFixture) seedRecord(ref model.Ref, kind string, op model.OpKind, body []byte) {
	f.t.Helper()
	change := conformance.FixtureChange{Ref: ref, Kind: kind, Op: op, Body: body}
	if kind == "bead" {
		// Keep the deliberately malformed field byte-exact while declaring the
		// fixture's known root/no-links source metadata. No decoding or repair.
		change.Graph = &conformance.FixtureGraphSource{}
	}
	_, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{
		Kind:       model.ChangesetWrite,
		Principal:  fixturePrincipal,
		Method:     "fixture.ontology.row",
		AgentLabel: "ontology-sql",
		Time:       time.Date(2026, 10, 29, 15, 1, 0, 0, time.UTC),
	}, []conformance.FixtureChange{change})
	if err != nil {
		f.t.Fatalf("seed %s/%s: %v", kind, ref.ID, err)
	}
}

func (f *sqlFixture) request(t *testing.T, method string, params string) (json.RawMessage, *wire.Error) {
	t.Helper()
	request := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":"ontology-sql","method":%q,"params":%s}`, method, params))
	response, err := f.transport.RoundTrip(f.ctx, request)
	if err != nil {
		t.Fatalf("%s transport: %v", method, err)
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  *wire.Error     `json:"error"`
	}
	if err := json.Unmarshal(response, &envelope); err != nil {
		t.Fatalf("%s response %s: %v", method, response, err)
	}
	if envelope.Error != nil {
		return nil, envelope.Error
	}
	desc, ok := f.registry.Catalog().Descriptor(method)
	if !ok {
		t.Fatalf("missing descriptor for bound method %s", method)
	}
	if err := f.registry.Catalog().ValidateResult(desc, envelope.Result); err != nil {
		t.Fatalf("%s returned invalid canonical result %s: %v", method, envelope.Result, err)
	}
	return envelope.Result, nil
}

func (f *sqlFixture) result(t *testing.T, method, params string, into any) {
	t.Helper()
	raw, errValue := f.request(t, method, params)
	if errValue != nil {
		t.Fatalf("%s returned %s: %+v", method, errValue.Data.Code, errValue)
	}
	if err := json.Unmarshal(raw, into); err != nil {
		t.Fatalf("decode %s result %s: %v", method, raw, err)
	}
}

func (f *sqlFixture) refusal(t *testing.T, method, params, want string) {
	t.Helper()
	if _, got := f.request(t, method, params); got == nil || string(got.Data.Code) != want {
		t.Fatalf("%s refusal = %+v, want %s", method, got, want)
	}
}

func createParams(project, key string) string {
	return fmt.Sprintf(`{"type":"issue","fields":{"title":"%s"},"_meta":{"project":%q,"idempotencyKey":%q,"agent":"sql-owner"}}`, project, project, key)
}

func updateParams(project, id, key string) string {
	return fmt.Sprintf(`{"id":{"id":%q},"fields":{"title":"changed"},"_meta":{"project":%q,"idempotencyKey":%q,"agent":"sql-owner"}}`, id, project, key)
}

func errorCode(err error) string {
	var typed *model.Error
	if errors.As(err, &typed) {
		return string(typed.Data.Code)
	}
	return ""
}

func TestStoredOntologyRPCReadsAndPlansOnSQL(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)

			var ownCommit, overlayCommit model.Commit
			f.result(t, "bead.create", createParams("own-project", "00000000000000000000000000000001"), &ownCommit)
			f.result(t, "bead.create", createParams("overlay-project", "00000000000000000000000000000002"), &overlayCommit)
			if len(ownCommit.Results) != 1 || len(overlayCommit.Results) != 1 || ownCommit.Revision.String() != "2" || overlayCommit.Revision.String() != "3" {
				t.Fatalf("Writer-backed SQL rows = own=%+v overlay=%+v", ownCommit, overlayCommit)
			}

			var baseRead, overlayRead ontologyops.Result
			f.result(t, "ontology.get", `{"project":""}`, &baseRead)
			f.result(t, "ontology.get", `{"project":"overlay-project"}`, &overlayRead)
			if baseRead.Document.Types["issue"].Name != f.base.Types["issue"].Name || overlayRead.Document.Types["issue"].Name != "Overlay task" || baseRead.Revision.String() != "1" || overlayRead.Revision.String() != "1" {
				t.Fatalf("stored base/overlay reads = base=%+v overlay=%+v", baseRead, overlayRead)
			}

			var described ontologyops.Result
			f.result(t, "ontology.describe", `{"project":"overlay-project","type":"issue"}`, &described)
			if len(described.Document.Types) != 1 || described.Document.Types["issue"].Fields["tenant_tag"].Type != "string" {
				t.Fatalf("overlay describe = %+v", described)
			}
			var schema map[string]any
			f.result(t, "ontology.schema", `{"project":"overlay-project","type":"issue","status":"todo"}`, &schema)
			properties, ok := schema["properties"].(map[string]any)
			fields, fieldsOK := properties["fields"].(map[string]any)
			fieldProperties, fieldsPropertiesOK := fields["properties"].(map[string]any)
			if !ok || !fieldsOK || !fieldsPropertiesOK || fieldProperties["tenant_tag"] == nil {
				t.Fatalf("overlay schema omitted stored field: %s", mustJSON(schema))
			}
			var check ontologyops.CheckResult
			f.result(t, "ontology.check", `{"project":"overlay-project"}`, &check)
			if !check.OK || check.Drifted || len(check.Problems) != 0 {
				t.Fatalf("stored SQL rows drifted: %+v", check)
			}

			candidateBase := f.base
			candidateBase.Types = cloneTypes(candidateBase.Types)
			baseType := candidateBase.Types["issue"]
			baseType.Fields = cloneFields(baseType.Fields)
			baseType.Fields["reviewed"] = ontology.Field{Type: "boolean", Default: false}
			candidateBase.Types["issue"] = baseType
			basePlanRaw := mustJSON(candidateBase)
			var basePlan ontologyops.PlanResult
			f.result(t, "ontology.plan", fmt.Sprintf(`{"project":"","document":%s,"expect":[{"ref":{"id":%q},"revision":1}]}`, basePlanRaw, f.source.ID), &basePlan)
			if !basePlan.RequiresHuman || !basePlan.Changes.Ready || basePlan.BaseRevision.String() != "3" || len(basePlan.Changes.Migration.Rows) != 2 {
				t.Fatalf("base candidate plan = %+v", basePlan)
			}
			if basePlan.Changes.Source.Ref.ID != f.source.ID || basePlan.Changes.Source.Revision.String() != "1" || basePlan.Changes.Source.BodyHash == "" || basePlan.Changes.Source.Digest == "" {
				t.Fatalf("base candidate lost its exact protected source pin: %+v", basePlan.Changes.Source)
			}
			foundBase := false
			for _, row := range basePlan.Changes.Migration.Rows {
				if row.Before.State.Project == "own-project" {
					foundBase = true
					if row.After.State.Fields["reviewed"] != false {
						t.Fatalf("base candidate did not default actual row: %+v", row)
					}
				}
			}
			if !foundBase {
				t.Fatal("base plan did not retain the actual own-project row")
			}

			// A normal production service write advances the SQL head and must
			// invalidate a retained plan from its complete-row snapshot.
			f.result(t, "bead.update", updateParams("own-project", ownCommit.Results[0].Ref.ID, "00000000000000000000000000000003"), &model.Commit{})
			if _, err := f.core.Planned(f.ctx, caller, basePlan.PlanDigest); errorCode(err) != "ontology_plan_stale" {
				t.Fatalf("row change retained base plan: %v", err)
			}

			candidateOverlay := f.overlay
			candidateOverlay.Types = cloneTypes(candidateOverlay.Types)
			overlayType := candidateOverlay.Types["issue"]
			overlayType.Fields = cloneFields(overlayType.Fields)
			overlayType.Fields["reviewed"] = ontology.Field{Type: "boolean", Default: false}
			candidateOverlay.Types["issue"] = overlayType
			var overlayPlan ontologyops.PlanResult
			f.result(t, "ontology.plan", fmt.Sprintf(`{"project":"overlay-project","document":%s}`, mustJSON(candidateOverlay)), &overlayPlan)
			if !overlayPlan.Changes.Ready || overlayPlan.BaseRevision.String() != "4" || len(overlayPlan.Changes.Migration.Rows) != 2 {
				t.Fatalf("overlay candidate plan = %+v", overlayPlan)
			}
			if overlayPlan.Changes.Source.Ref.ID != f.source.ID || overlayPlan.Changes.Source.Revision.String() != "1" || overlayPlan.Changes.Source.BodyHash == "" || overlayPlan.Changes.Source.Digest == "" {
				t.Fatalf("overlay candidate lost its exact protected source pin: %+v", overlayPlan.Changes.Source)
			}
			foundOverlay := false
			for _, row := range overlayPlan.Changes.Migration.Rows {
				if row.Before.State.Project == "overlay-project" {
					foundOverlay = true
					if row.After.State.Fields["reviewed"] != false {
						t.Fatalf("overlay candidate lost default: %+v", row)
					}
				}
			}
			if !foundOverlay {
				t.Fatal("overlay plan did not retain the actual overlay-project row")
			}

			// RPC response data is detached. Mutating it cannot change the
			// retained immutable report returned by the real service.
			overlayPlan.Changes.Document.Overlays["overlay-project"].Types["issue"] = ontology.Type{Name: "caller mutation"}
			retained, err := f.core.Planned(f.ctx, caller, overlayPlan.PlanDigest)
			if err != nil || retained.Document.Overlays["overlay-project"].Types["issue"].Name != "Overlay task" {
				t.Fatalf("retained candidate was not detached: %+v, %v", retained, err)
			}

			updatedOverlay := f.overlay
			updatedOverlay.Types = cloneTypes(updatedOverlay.Types)
			updatedType := updatedOverlay.Types["issue"]
			updatedType.Name = "Overlay task v2"
			updatedOverlay.Types["issue"] = updatedType
			updatedSource := service.OntologySource{Base: f.base, Overlays: map[string]ontology.Document{"overlay-project": updatedOverlay}}
			f.seedSource(updatedSource, model.OpPatch)
			if _, err := f.core.Planned(f.ctx, caller, overlayPlan.PlanDigest); errorCode(err) != "ontology_plan_stale" {
				t.Fatalf("source change retained overlay plan: %v", err)
			}

			var current ontologyops.Result
			f.result(t, "ontology.get", `{"project":"overlay-project"}`, &current)
			if current.Revision.String() != "5" || current.Document.Types["issue"].Name != "Overlay task v2" {
				t.Fatalf("current source did not advance: %+v", current)
			}
			var history ontologyops.HistoryResult
			f.result(t, "ontology.history", `{"project":"overlay-project","after":0,"limit":100}`, &history)
			if len(history.Revisions) != 2 || history.Revisions[0].Revision.String() != "1" || history.Revisions[1].Revision.String() != "5" {
				t.Fatalf("source-only history = %+v", history)
			}
			for _, revision := range history.Revisions {
				if len(revision.Changes) != 1 || revision.Changes[0].Ref.ID != f.source.ID || revision.Principal.Kind != fixturePrincipal.Kind || revision.Principal.ID != fixturePrincipal.ID || len(revision.Changes[0].ChangedPaths) == 0 {
					t.Fatalf("source attribution/pin lost: %+v", revision)
				}
			}
			var continuation ontologyops.HistoryResult
			f.result(t, "ontology.history", `{"project":"overlay-project","after":1,"limit":100}`, &continuation)
			if len(continuation.Revisions) != 1 || continuation.Revisions[0].Revision.String() != "5" {
				t.Fatalf("history continuation = %+v", continuation)
			}
			var diff ontologyops.DiffResult
			f.result(t, "ontology.diff", `{"project":"overlay-project","fromRevision":1,"toRevision":5}`, &diff)
			if len(diff.Patch) != 1 || diff.Patch[0].Op != "replace" || diff.Patch[0].Value.Types["issue"].Name != "Overlay task v2" {
				t.Fatalf("historical source diff = %+v", diff)
			}

			discovery, errValue := f.request(t, "rpc.discover", `{}`)
			if errValue != nil || strings.Contains(string(discovery), `"name":"ontology.apply"`) {
				t.Fatalf("ontology.apply was advertised: %s / %+v", discovery, errValue)
			}
			f.refusal(t, "ontology.apply", `{"planDigest":"unbound","expect":[],"humanPresence":{"kind":"presence"}}`, "method_not_found")
		})
	}
}

func TestOntologySQLRefusalsAndSourceProtection(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			f.result(t, "bead.create", createParams("own-project", "00000000000000000000000000000011"), &model.Commit{})

			missing, err := ontologyops.New(f.store, f.lease, ontologyops.Config{Source: func(string) (model.Ref, error) { return model.Ref{ID: "ontology/missing"}, nil }, ValidatingPre: ontologyops.NoValidatingPre()})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := missing.Get(f.ctx, caller, "own-project"); errorCode(err) != "not_found" {
				t.Fatalf("missing protected source = %v", err)
			}
			wrongKindRef := model.Ref{ID: "ontology/wrong-kind"}
			f.seedRecord(wrongKindRef, "ordinary", model.OpCreate, []byte(`{"id":"ontology/wrong-kind","project":"own-project","type":"issue","fields":{"status":"todo"}}`))
			wrongKind, err := ontologyops.New(f.store, f.lease, ontologyops.Config{Source: service.StaticOntologySource(wrongKindRef), ValidatingPre: ontologyops.NoValidatingPre()})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := wrongKind.Get(f.ctx, caller, "own-project"); errorCode(err) != "invalid_params" {
				t.Fatalf("wrong source kind = %v", err)
			}

			_, err = f.writer.Apply(f.ctx, caller, model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: f.source, Fields: map[string]any{"base": "forbidden"}}}}}, model.WriteMeta{Method: "tx.apply", Project: "own-project", IdempotencyKey: "00000000000000000000000000000012"})
			if errorCode(err) != "protected_record" {
				t.Fatalf("generic Writer source mutation = %v", err)
			}

			f.seedRecord(model.Ref{ID: "bad-row"}, "bead", model.OpCreate, []byte(`{"id":"bad-row","project":"own-project","type":"issue","fields":{"status":"todo"},"unsupported":true}`))
			f.refusal(t, "ontology.check", `{"project":"own-project"}`, "invalid_params")

			f.seedRawSource([]byte(`{"base":{},"unknown":true}`), model.OpPatch)
			f.refusal(t, "ontology.get", `{"project":"own-project"}`, "invalid_params")
		})
	}
}

func cloneFields(input map[string]ontology.Field) map[string]ontology.Field {
	output := make(map[string]ontology.Field, len(input))
	for key, value := range input {
		value.Values = append([]string(nil), value.Values...)
		output[key] = value
	}
	return output
}

func cloneTypes(input map[string]ontology.Type) map[string]ontology.Type {
	output := make(map[string]ontology.Type, len(input))
	for key, value := range input {
		value.Fields = cloneFields(value.Fields)
		output[key] = value
	}
	return output
}

func mustJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return string(data)
}
