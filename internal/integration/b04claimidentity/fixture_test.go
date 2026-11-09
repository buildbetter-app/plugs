package b04claimidentity_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const (
	poolA = "pool-alpha"
	poolB = "pool__alpha"
)

const ontologySourceID = "ontology/b04-claim-identity"

type credential struct {
	principal model.PrincipalRef
	key       authnapikey.IssuedKey
}

type receiptScope struct {
	principal, agent, method, key string
}

type fixture struct {
	t          *testing.T
	ctx        context.Context
	engine     engine
	store      driver.Store
	lease      driver.ActiveLease
	keys       *authnapikey.Provider
	app        *authzrpc.Composition
	catalog    *wire.Catalog
	facts      authzfixed.Facts
	source     *authzrpc.MutableFacts
	bootstrap  http.Handler
	work       map[string]model.Ref
	queries    map[receiptScope]bool
	writeKeys  map[string]string
	ontology   []byte
	httpConfig httpjson.Config
	claimPools []string
}

func newFixture(t *testing.T, e engine) *fixture {
	return newFixtureWithSetup(t, e, ontology.DefaultDocument(), nil)
}

// newFixtureWithSetup keeps the ordinary default fixture unchanged. Native
// outcome replay may explicitly configure a test workflow and starting state;
// this remains a fixture, never an imported native protected authority.
func newFixtureWithSetup(t *testing.T, e engine, document ontology.Document, statuses map[string]string) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	catalog, err := wire.DefaultCatalog()
	if err != nil {
		t.Fatalf("parse public error catalog: %v", err)
	}
	f := &fixture{t: t, ctx: ctx, engine: e, catalog: catalog, work: map[string]model.Ref{}, queries: map[receiptScope]bool{}, writeKeys: map[string]string{}}
	f.open()
	t.Cleanup(f.close)
	f.seedOntologyDocument(document)
	meta := f.meta()
	f.keys = authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: ref("authority")}
	f.facts = authzfixed.Facts{
		Namespace: ns,
		Teams: []authzfixed.Team{
			{Ref: ref("team-pool-a"), InstallationID: "fixed", Active: true},
			{Ref: ref("team-pool-b"), InstallationID: "fixed", Active: true},
		},
		Projects: []authzfixed.Project{
			{Ref: ref(poolA), Name: poolA, InstallationID: "fixed", Active: true},
			{Ref: ref(poolB), Name: poolB, InstallationID: "fixed", Active: true},
		},
		Grants: []authz.Grant{
			{Subject: authz.SubjectRef{Kind: "user", ID: "user:pool-a"}, Scope: authz.Scope{Kind: authz.TeamScope, Ref: ref("team-pool-a")}, Role: "member"},
			{Subject: authz.SubjectRef{Kind: "user", ID: "user:pool-b"}, Scope: authz.Scope{Kind: authz.TeamScope, Ref: ref("team-pool-b")}, Role: "member"},
			{Subject: authz.SubjectRef{Kind: "team", ID: "team-pool-a"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: ref(poolA)}, Role: "involved"},
			{Subject: authz.SubjectRef{Kind: "team", ID: "team-pool-b"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: ref(poolB)}, Role: "involved"},
		},
	}
	// Only setup uses an anonymous public host to create existing beads. CP7
	// deliberately permits only existing-bead Agent writes. Subject claims use
	// the installed authenticated host; no test calls WithPrincipal.
	f.bootstrap = f.anonymousHandler()
	for _, seed := range []struct{ title, project string }{{"a1", poolA}, {"a2", poolA}, {"b1", poolB}} {
		fields := map[string]any{"title": seed.title, "description": "hydrated " + seed.title}
		if status := statuses[seed.title]; status != "" {
			fields["status"] = status
		}
		r := f.request(f.bootstrap, credential{}, "bead.create", seed.project, f.key("seed-"+seed.title), "setup", map[string]any{"type": "issue", "fields": fields})
		commit := r.commit(t)
		if len(commit.Results) != 1 {
			t.Fatalf("setup create has no singular result: %s", r.raw)
		}
		f.work[seed.title] = commit.Results[0].Ref
	}
	f.install()
	return f
}

func (f *fixture) seedOntology() {
	f.seedOntologyDocument(ontology.DefaultDocument())
}

func (f *fixture) seedOntologyDocument(document ontology.Document) {
	f.t.Helper()
	body, err := service.MarshalOntologySource(service.OntologySource{Base: document})
	if err != nil {
		f.t.Fatal(err)
	}
	// Trusted setup persists only the explicitly configured ontology fixture.
	// The real driver records this fixture attribution; no claim event,
	// authorization chain, receipt or lease history is supplied here.
	if _, err := conformance.Seed(f.ctx, f.store, f.lease,
		driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: "b04-ontology-fixture"}},
		[]conformance.FixtureChange{{Ref: ref(ontologySourceID), Kind: "ontology", Op: model.OpCreate, Body: body, ChangedPaths: []string{"base"}}}); err != nil {
		f.t.Fatal(err)
	}
	f.ontology = append([]byte(nil), body...)
}

func ref(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }

func (f *fixture) open() {
	f.t.Helper()
	store, err := f.engine.storage.Open(f.ctx, driver.Config{DSN: f.engine.dsn})
	if err != nil {
		f.t.Fatal(err)
	}
	lease, err := store.Lease(f.ctx, "b04-claim-identity", driver.LeaseTTL)
	if err != nil {
		_ = store.Close()
		f.t.Fatal(err)
	}
	f.store, f.lease = store, lease
	if f.engine.name == "sqlite_wal" {
		db := f.engine.db(f.t)
		defer db.Close()
		var mode string
		if err := db.QueryRowContext(f.ctx, "PRAGMA journal_mode").Scan(&mode); err != nil || mode != "wal" {
			f.t.Fatalf("SQLite must be a real WAL file: mode=%q err=%v", mode, err)
		}
	}
}

func (f *fixture) close() {
	f.t.Helper()
	if f.lease != nil {
		if err := f.lease.Release(context.Background()); err != nil {
			f.t.Error(err)
		}
		f.lease = nil
	}
	if f.store != nil {
		if err := f.store.Close(); err != nil {
			f.t.Error(err)
		}
		f.store = nil
	}
}

func (f *fixture) install() {
	f.installMutators()
}

func (f *fixture) installMutators(mutators ...pipeline.MutatorBinding) {
	f.t.Helper()
	source, err := authzrpc.NewMutableFacts(f.facts)
	if err != nil {
		f.t.Fatal(err)
	}
	app, err := authzrpc.New(f.ctx, authzrpc.Config{
		Store: f.store, Lease: f.lease, Provider: f.keys, Facts: source, Namespace: f.facts.Namespace, DefaultProject: poolA,
		Audit:      service.NewTrackedOntologyAudit(service.StaticOntologySource(ref(ontologySourceID))),
		Mutators:   mutators,
		HTTP:       f.httpConfig,
		ClaimPools: f.claimPools,
	})
	if err != nil {
		f.t.Fatal(err)
	}
	f.source, f.app = source, app
}

func (f *fixture) reopen() {
	f.t.Helper()
	if f.engine.name == "memory" {
		f.t.Fatal("memory reopen is not durable SQL evidence")
	}
	before, epoch := f.state(), f.lease.Epoch()
	f.close()
	f.open()
	// The key ring is intentionally retained: this proves SQL durability, not
	// durable key provisioning. The chain is recomposed against the new lease.
	f.install()
	f.bootstrap = f.anonymousHandler()
	after := f.state()
	if !reflect.DeepEqual(before, after) || f.lease.Epoch() <= epoch {
		f.t.Fatalf("SQL reopen changed content/receipt/identity or failed to advance host epoch: epoch=%d->%d", epoch, f.lease.Epoch())
	}
	f.t.Logf("%s physical close/reopen preserved store=%s incarnation=%s head=%s; host epoch %d -> %d", f.engine.name, after.Meta.StoreID, after.Meta.Incarnation, after.Meta.Head, epoch, f.lease.Epoch())
}

func (f *fixture) anonymousHandler() http.Handler {
	f.t.Helper()
	ontology := service.NewOntology(nil)
	writer := pipeline.New(f.store, f.lease,
		pipeline.WithOntology(ontology),
		pipeline.WithClaimPools(f.claimPools...),
		pipeline.WithOntologyAudit(service.NewTrackedOntologyAudit(service.StaticOntologySource(ref(ontologySourceID)))),
	)
	domain := service.New(f.store, f.lease, writer, service.WithOntology(ontology), service.WithClaimPools(f.claimPools...))
	registry, err := rpc.NewRegistry()
	if err != nil {
		f.t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		f.t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: f.meta().Incarnation, OntologyRevision: model.Natural64(1), DefaultProject: poolA})
	if err != nil {
		f.t.Fatal(err)
	}
	admission, err := authn.New(authn.Config{})
	if err != nil {
		f.t.Fatal(err)
	}
	handler, err := httpjson.New(server, admission, httpjson.Config{})
	if err != nil {
		f.t.Fatal(err)
	}
	return handler
}

func (f *fixture) issue(id, sponsor string, caveats map[string]any) credential {
	f.t.Helper()
	principal := model.PrincipalRef{Kind: "agent", ID: id, Sponsor: &model.PrincipalRef{Kind: "user", ID: sponsor}, Caveats: caveats}
	// Use the provider's strict detached representation for state comparison.
	encoded, err := json.Marshal(principal)
	if err != nil {
		f.t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &principal); err != nil {
		f.t.Fatal(err)
	}
	key, err := f.keys.Issue(principal, time.Now().Add(time.Hour))
	if err != nil {
		f.t.Fatal(err)
	}
	return credential{principal: principal, key: key}
}

func (f *fixture) key(label string) string {
	if f.writeKeys[label] == "" {
		f.writeKeys[label] = randomID()
	}
	return f.writeKeys[label]
}

func (f *fixture) remember(by credential, method, key, agent string) receiptScope {
	f.t.Helper()
	principal := by.principal
	if principal.Kind == "" {
		principal = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	}
	p, err := driver.PrincipalKey(principal)
	if err != nil {
		f.t.Fatal(err)
	}
	scope := receiptScope{principal: p, method: method, key: key, agent: agent}
	if principal.Kind != "anonymous" {
		scope.agent = ""
	}
	f.queries[scope] = true
	// Observe authenticated label lookups too; state requires them to return
	// the same complete canonical receipt.
	if principal.Kind != "anonymous" && agent != "" {
		f.queries[receiptScope{principal: p, method: method, key: key, agent: agent}] = true
	}
	return scope
}

type reply struct {
	raw        []byte
	status     int
	envelope   map[string]json.RawMessage
	code       string
	error      *wire.Error
	started    time.Time
	finished   time.Time
	retryAfter string
}

func (f *fixture) request(handler http.Handler, by credential, method, project, key, agent string, payload map[string]any) reply {
	f.t.Helper()
	if err := f.lease.Renew(f.ctx); err != nil {
		f.t.Fatal(err)
	}
	if key != "" {
		f.remember(by, method, key, agent)
	}
	params := map[string]any{}
	for k, v := range payload {
		params[k] = v
	}
	params["_meta"] = map[string]any{"project": project, "idempotencyKey": key, "agent": agent}
	raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		f.t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/rpc/v1", bytes.NewReader(raw)).WithContext(f.ctx)
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Plug-RPC", "1")
	req.Header.Set("Content-Type", "application/json")
	if by.key.Token != "" {
		req.Header.Set("Authorization", "Bearer "+by.key.Token)
	}
	recorder := httptest.NewRecorder()
	started := time.Now().UTC()
	handler.ServeHTTP(recorder, req)
	r := reply{raw: append([]byte(nil), recorder.Body.Bytes()...), status: recorder.Code, started: started, finished: time.Now().UTC()}
	return f.checkedReply(r)
}

func (f *fixture) checkedReply(r reply) reply {
	f.t.Helper()
	if err := json.Unmarshal(r.raw, &r.envelope); err != nil {
		f.t.Fatalf("invalid public response: %v", err)
	}
	if errorRaw := r.envelope["error"]; len(errorRaw) != 0 {
		var wireError wire.Error
		decoder := json.NewDecoder(bytes.NewReader(errorRaw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&wireError); err != nil {
			f.t.Fatalf("public error is not a closed wire schema: %v: %s", err, r.raw)
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			f.t.Fatalf("public error has trailing JSON: %v: %s", err, r.raw)
		}
		if err := f.catalog.ValidateError(&wireError); err != nil {
			f.t.Fatalf("public error violates catalog schema: %v: %s", err, r.raw)
		}
		canonical := f.catalog.NewError(wireError.Data.Code, "", nil)
		if wireError.Code != canonical.Code || wireError.Message != canonical.Message || wireError.Data.Retryable != canonical.Data.Retryable {
			f.t.Fatalf("public error is not canonical for %q: got=%+v want=%+v", wireError.Data.Code, wireError, canonical)
		}
		if strings.TrimSpace(wireError.Data.TraceID) == "" {
			f.t.Fatalf("public error omitted transport-generated traceId: %s", r.raw)
		}
		r.code = wireError.Data.Code
		r.error = &wireError
	}
	return r
}

func (f *fixture) call(by credential, method, project, key, agent string, payload map[string]any) reply {
	return f.request(f.app.Handler, by, method, project, key, agent, payload)
}

func (r reply) commit(t *testing.T) model.Commit {
	t.Helper()
	if r.status != http.StatusOK || r.code != "" || len(r.envelope["result"]) == 0 || bytes.Equal(r.envelope["result"], []byte("null")) {
		t.Fatalf("expected public hydrated commit, status=%d: %s", r.status, r.raw)
	}
	var commit model.Commit
	if err := json.Unmarshal(r.envelope["result"], &commit); err != nil {
		t.Fatal(err)
	}
	return commit
}

func (r reply) replay(t *testing.T, first reply) {
	t.Helper()
	r.commit(t)
	var meta struct{ Replayed bool }
	if err := json.Unmarshal(r.envelope["_meta"], &meta); err != nil || !meta.Replayed || !bytes.Equal(r.envelope["result"], first.envelope["result"]) {
		t.Fatalf("replay changed durable result bytes or omitted replay marker: %s", r.raw)
	}
}

func (f *fixture) meta() driver.Meta {
	f.t.Helper()
	m, err := f.store.Meta(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	return m
}

type durableState struct {
	Meta          driver.Meta
	Records       []driver.Record
	Ontology      []driver.Record
	Versions      map[string][]driver.Version
	Changesets    []model.Changeset
	Derived       map[driver.DerivedTable][]driver.DerivedRow
	Observations  []driver.LockState
	LeaseOverlays map[string]driver.Checkpoint
	Receipts      map[receiptScope]*driver.IdempotencyRecord
}

func (f *fixture) state() durableState {
	f.t.Helper()
	if err := f.lease.Renew(f.ctx); err != nil {
		f.t.Fatal(err)
	}
	s := durableState{Meta: f.meta(), Versions: map[string][]driver.Version{}, Derived: map[driver.DerivedTable][]driver.DerivedRow{}, LeaseOverlays: map[string]driver.Checkpoint{}, Receipts: map[receiptScope]*driver.IdempotencyRecord{}}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: s.Meta.Incarnation, At: &s.Meta.Head})
	if err != nil {
		f.t.Fatal(err)
	}
	defer r.Close()
	var cursor *driver.Cursor
	seenRows := map[string]bool{}
	for {
		page, err := r.Query(f.ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000, Cursor: cursor})
		if err != nil {
			f.t.Fatalf("complete fixture rows: %v", err)
		}
		for _, row := range page.Items {
			if seenRows[row.Ref.ID] {
				f.t.Fatalf("fixture row repeated across pages: %s", row.Ref.ID)
			}
			seenRows[row.Ref.ID] = true
			s.Records = append(s.Records, row)
		}
		if page.NextCursor == nil {
			break
		}
		if len(page.Items) == 0 || reflect.DeepEqual(cursor, page.NextCursor) {
			f.t.Fatal("fixture row cursor failed to advance")
		}
		cursor = page.NextCursor
	}
	sort.Slice(s.Records, func(i, j int) bool { return s.Records[i].Ref.ID < s.Records[j].Ref.ID })
	checkpoints, ok := f.store.(driver.CheckpointStore)
	if !ok {
		f.t.Fatal("fixture driver must expose durable lease-overlay checkpoints")
	}
	lockKeys := map[string]driver.LockKey{}
	addLock := func(key driver.LockKey) {
		encoded, err := driver.EncodeLockKey(key)
		if err != nil {
			f.t.Fatal(err)
		}
		lockKeys[encoded] = key
	}
	for _, key := range []driver.LockKey{driver.GraphBlockingSentinelLock, driver.OntologySentinelLock, driver.PluginsSentinelLock} {
		addLock(key)
	}
	for _, dependency := range []authz.Dependency{authz.ACLDependency(f.facts.Namespace), authz.ContextDependency(f.facts.Namespace)} {
		fact, err := driver.NewReadFact(dependency.Key)
		if err != nil {
			f.t.Fatal(err)
		}
		addLock(driver.LockKey{Kind: driver.LockReadFact, Fact: fact})
	}
	s.Ontology, err = r.Get(f.ctx, []model.Ref{ref(ontologySourceID)})
	if err != nil || len(s.Ontology) != 1 || !sameResolvedRef(s.Ontology[0].Ref, ref(ontologySourceID)) || s.Ontology[0].Kind != "ontology" || !bytes.Equal(s.Ontology[0].Body, f.ontology) {
		f.t.Fatalf("protected default ontology source is missing or changed: %v", err)
	}
	sourceRef := s.Ontology[0].Ref
	addLock(driver.LockKey{Kind: driver.LockRecord, Ref: &sourceRef})
	sourceVersions, err := r.Versions(f.ctx, sourceRef, driver.PageReq{Limit: 1000})
	if err != nil || sourceVersions.NextCursor != nil || len(sourceVersions.Items) == 0 {
		f.t.Fatalf("complete protected ontology versions: %v", err)
	}
	s.Versions[sourceRef.ID] = sourceVersions.Items
	for _, row := range s.Records {
		rowRef := row.Ref
		addLock(driver.LockKey{Kind: driver.LockRecord, Ref: &rowRef})
		beadSentinel, err := driver.NewBeadSentinel(row.Ref.ID)
		if err != nil {
			f.t.Fatal(err)
		}
		addLock(driver.LockKey{Kind: driver.LockSentinel, Sentinel: beadSentinel})
		closureSentinel, err := driver.NewClosureSentinel(row.Ref.ID)
		if err != nil {
			f.t.Fatal(err)
		}
		addLock(driver.LockKey{Kind: driver.LockSentinel, Sentinel: closureSentinel})
		doc, err := pipeline.DecodeDocument(row.Body)
		if err != nil {
			f.t.Fatal(err)
		}
		readySentinel, err := driver.NewReadySentinel(doc.Project, doc.Type)
		if err != nil {
			f.t.Fatal(err)
		}
		addLock(driver.LockKey{Kind: driver.LockSentinel, Sentinel: readySentinel})
		versions, err := r.Versions(f.ctx, row.Ref, driver.PageReq{Limit: 1000})
		if err != nil || versions.NextCursor != nil {
			f.t.Fatalf("complete versions for %s: %v", row.Ref.ID, err)
		}
		s.Versions[row.Ref.ID] = versions.Items
		// Observe the production overlay namespace without replacing its loader
		// or mutating a checkpoint. No heartbeat is invoked by this fixture.
		digest := sha256.Sum256([]byte("plug/lease/" + row.Ref.ID))
		checkpoint, err := checkpoints.LoadCheckpoint(f.ctx, "plug/lease/"+hex.EncodeToString(digest[:]))
		if err != nil {
			f.t.Fatal(err)
		}
		s.LeaseOverlays[row.Ref.ID] = checkpoint
	}
	s.Changesets, err = r.Changesets(f.ctx, nil, 1000)
	if err != nil || len(s.Changesets) >= 1000 {
		f.t.Fatalf("complete fixture log: %v rows=%d", err, len(s.Changesets))
	}
	for _, table := range []driver.DerivedTable{driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins} {
		rows, err := r.DerivedRows(f.ctx, table, "")
		if err != nil {
			f.t.Fatal(err)
		}
		s.Derived[table] = rows
		for _, row := range rows {
			key := driver.DerivedRowKey{Table: table, Key: row.Key}
			addLock(driver.LockKey{Kind: driver.LockDerived, Derived: &key})
		}
	}
	encodedKeys := make([]string, 0, len(lockKeys))
	for key := range lockKeys {
		encodedKeys = append(encodedKeys, key)
	}
	sort.Strings(encodedKeys)
	keys := make([]driver.LockKey, 0, len(encodedKeys))
	for _, key := range encodedKeys {
		keys = append(keys, lockKeys[key])
	}
	s.Observations, err = r.Observe(f.ctx, keys)
	if err != nil || len(s.Observations) != len(keys) {
		f.t.Fatalf("complete fixture predicate/record/derived observations: %v", err)
	}
	for scope := range f.queries {
		entry, err := r.IdempotencyByKey(f.ctx, scope.principal, scope.agent, scope.method, scope.key)
		if err != nil {
			f.t.Fatal(err)
		}
		// IdempotencyByKey ignores labels for authenticated principals. Keep
		// every tracked lookup view checked, but snapshot one canonical receipt.
		canonical := scope
		if !strings.HasPrefix(canonical.principal, "anonymous:") {
			canonical.agent = ""
		}
		if previous, seen := s.Receipts[canonical]; seen && !reflect.DeepEqual(previous, entry) {
			f.t.Fatalf("receipt lookup views disagree for principal=%q method=%q key=%q", scope.principal, scope.method, scope.key)
		}
		s.Receipts[canonical] = entry
	}
	return s
}

// Compare resolved bead IDs with the model's branch-zero semantics. Public
// create replies may omit branch zero; driver rows store an explicit zero.
func sameResolvedRef(a, b model.Ref) bool {
	return a.Validate() == nil && b.Validate() == nil &&
		a.IsID() && b.IsID() && a.ID != "" && a.ID == b.ID &&
		a.Branch.IsZero() && b.Branch.IsZero()
}

func (s durableState) record(t *testing.T, id model.Ref) driver.Record {
	t.Helper()
	for _, row := range s.Records {
		if sameResolvedRef(row.Ref, id) {
			return row
		}
	}
	t.Fatalf("stored row is missing: %s", id.ID)
	return driver.Record{}
}

func sameState(t *testing.T, before, after durableState) {
	t.Helper()
	if !reflect.DeepEqual(before, after) {
		t.Fatal("refusal/replay changed rows, history, head, derived state, predicate generations, lease overlays or tracked receipt scopes")
	}
}

func onlyNoopReceipt(t *testing.T, before, after durableState, scope receiptScope, result model.Commit) {
	t.Helper()
	entry := after.Receipts[scope]
	if before.Receipts[scope] != nil || entry == nil || entry.ResponsePending || entry.Changeset.Kind != "" || !reflect.DeepEqual(entry.Commit, result) {
		t.Fatalf("same-owner response needs one final no-op receipt: %+v", entry)
	}
	copyReceipts := make(map[receiptScope]*driver.IdempotencyRecord, len(after.Receipts))
	for k, v := range after.Receipts {
		copyReceipts[k] = v
	}
	copyReceipts[scope] = nil
	after.Receipts = copyReceipts
	sameState(t, before, after)
}

func sameClaimState(t *testing.T, first, noop model.Commit) {
	t.Helper()
	// SetBy describes fields touched by this request, so an empty no-op patch
	// differs from the original winning transition. The complete winning row,
	// revision, original lease and other response fields must stay identical.
	first.Results = append([]model.OpResult(nil), first.Results...)
	noop.Results = append([]model.OpResult(nil), noop.Results...)
	for i := range first.Results {
		first.Results[i].SetBy = nil
	}
	for i := range noop.Results {
		noop.Results[i].SetBy = nil
	}
	if !reflect.DeepEqual(first, noop) {
		t.Fatal("same owner changed hydrated claim state, revision or original lease")
	}
}

func (f *fixture) refuse(by credential, method, project, key, agent string, payload map[string]any, code string) {
	f.t.Helper()
	scope := f.remember(by, method, key, agent)
	before := f.state()
	r := f.call(by, method, project, key, agent, payload)
	canonical := f.catalog.NewError(code, "", nil)
	if r.error == nil || r.code != code || r.error.Data.Retryable != canonical.Data.Retryable || len(r.envelope["result"]) != 0 {
		f.t.Fatalf("want typed %s with no result, status=%d: %s", code, r.status, r.raw)
	}
	after := f.state()
	sameState(f.t, before, after)
	if before.Receipts[scope] == nil && after.Receipts[scope] != nil {
		f.t.Fatal("refusal persisted a successful receipt")
	}
}

func (f *fixture) winner(before durableState, by credential, method, key, agent string, id model.Ref, r reply, authenticated bool) model.Commit {
	f.t.Helper()
	commit := r.commit(f.t)
	after := f.state()
	head, err := before.Meta.Head.Uint64()
	if err != nil {
		f.t.Fatal(err)
	}
	row := after.record(f.t, id)
	if commit.Revision != model.Natural64(head+1) || after.Meta.Head != commit.Revision || row.Revision != commit.Revision || len(commit.Results) != 1 || !sameResolvedRef(commit.Results[0].Ref, id) || commit.Results[0].Revision != row.Revision {
		f.t.Fatalf("claim did not publish one H+1 winning row: %+v", commit)
	}
	var returned, stored bytes.Buffer
	if json.Compact(&returned, commit.Results[0].Record) != nil || json.Compact(&stored, row.Body) != nil || !bytes.Equal(returned.Bytes(), stored.Bytes()) {
		f.t.Fatal("public claim omitted or changed the complete hydrated stored postimage")
	}
	doc, err := pipeline.DecodeDocument(row.Body)
	if err != nil {
		f.t.Fatal(err)
	}
	expectedPrincipal := by.principal
	expectedChain := []string{"authz/fixed"}
	if !authenticated {
		expectedPrincipal = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
		expectedChain = []string{}
	}
	expectedAgent := agent
	if authenticated {
		expectedAgent = ""
	}
	if doc.Lease == nil || !reflect.DeepEqual(doc.Lease.Principal, expectedPrincipal) || doc.Lease.Agent != expectedAgent || doc.Fields["status"] != "in_progress" || doc.Lease.Epoch != model.Natural64(f.lease.Epoch()) || doc.Lease.TtlMs != model.Natural64(300000) || commit.LeaseTtlMs == nil || *commit.LeaseTtlMs != doc.Lease.TtlMs || commit.LeaseExpiresAt == nil || !commit.LeaseExpiresAt.Equal(doc.Lease.ExpiresAt) {
		f.t.Fatalf("winner lost verified owner/status/default lease: %+v", doc)
	}
	if len(after.Versions[id.ID]) != len(before.Versions[id.ID])+1 || len(after.Changesets) != len(before.Changesets)+1 {
		f.t.Fatal("winning claim did not add exactly one version and one changeset")
	}
	versions := after.Versions[id.ID]
	if !reflect.DeepEqual(versions[:len(versions)-1], before.Versions[id.ID]) || !reflect.DeepEqual(after.Changesets[:len(after.Changesets)-1], before.Changesets) {
		f.t.Fatal("winning claim rewrote prior history")
	}
	change := after.Changesets[len(after.Changesets)-1]
	if change.Revision != commit.Revision || !reflect.DeepEqual(change.Principal, expectedPrincipal) || !reflect.DeepEqual(change.Chain, expectedChain) || change.IdempotencyKey != key || len(change.Changes) != 1 || !sameResolvedRef(change.Changes[0].Ref, id) || len(change.Events) != 1 || change.Events[0].Type != model.EventBeadClaimed || !sameResolvedRef(change.Events[0].Resource.Ref, id) || change.Events[0].Provenance.Method != method || change.Events[0].Provenance.Cause.Root != key || !reflect.DeepEqual(change.Events[0].Provenance.Chain, expectedChain) {
		f.t.Fatalf("claim event/history lost installed authorization or first lease boundary: %+v", change)
	}
	// Production uses separate real clock reads for grant and publication.
	// Bound them by the actual HTTP call; do not force equality or alter clocks.
	leaseStart := doc.Lease.ExpiresAt.Add(-5 * time.Minute)
	version := versions[len(versions)-1]
	if leaseStart.Before(r.started) || leaseStart.After(r.finished) || change.Time.Before(r.started) || change.Time.After(r.finished) || version.Revision != commit.Revision || !version.CreatedAt.Equal(change.Time) || !reflect.DeepEqual(version.Principal, expectedPrincipal) {
		f.t.Fatal("claim grant/history time or actor is outside the actual public request")
	}
	scope := f.remember(by, method, key, agent)
	entry := after.Receipts[scope]
	if entry == nil || entry.ResponsePending || !reflect.DeepEqual(entry.Commit, commit) || !reflect.DeepEqual(entry.Changeset, change) {
		f.t.Fatal("winning public response differs from final durable receipt")
	}
	for _, old := range before.Records {
		if !sameResolvedRef(old.Ref, id) && (!reflect.DeepEqual(old, after.record(f.t, old.Ref)) || !reflect.DeepEqual(before.Versions[old.Ref.ID], after.Versions[old.Ref.ID])) {
			f.t.Fatalf("claim changed unrelated pool work %s", old.Ref.ID)
		}
	}
	if !reflect.DeepEqual(before.Ontology, after.Ontology) || !reflect.DeepEqual(before.Versions[ontologySourceID], after.Versions[ontologySourceID]) {
		f.t.Fatal("claim changed the protected ontology source or its history")
	}
	return commit
}

func (f *fixture) revokePoolA() {
	f.t.Helper()
	// Removing the trusted team-to-project grant changes current policy. It
	// does not revoke tokens or mutate claim rows, and must block old receipts.
	next := f.facts
	next.Grants = append([]authz.Grant(nil), f.facts.Grants[:2]...)
	next.Grants = append(next.Grants, f.facts.Grants[3:]...)
	if err := f.source.Replace(next); err != nil {
		f.t.Fatal(err)
	}
	f.facts = next
}

func target(id model.Ref) map[string]any { return map[string]any{"id": id} }
