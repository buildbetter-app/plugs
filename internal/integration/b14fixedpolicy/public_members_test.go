package b14fixedpolicy_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const project = "b14-fixed-project"

var sourceRef = ref("ontology/b14-fixed-policy")

func ref(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }

func worker() model.PrincipalRef {
	return model.PrincipalRef{Kind: "agent", ID: "agent:b14-worker", Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:b14-owner"}}
}

type response struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data    model.ErrorData `json:"data"`
		Message string          `json:"message"`
	} `json:"error"`
	Meta struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
}

func responseSummary(reply response) string {
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
		var reply response
		if err := json.Unmarshal([]byte(tc.raw), &reply); err != nil {
			t.Fatalf("%s decode: %v", tc.name, err)
		}
		if got := responseSummary(reply); got != tc.want {
			t.Errorf("%s diagnostics=%q want=%q", tc.name, got, tc.want)
		}
	}
}

type host struct {
	t      *testing.T
	ctx    context.Context
	engine string
	path   string
	store  driver.Store
	lease  driver.ActiveLease
	api    *httptest.Server
	keys   *authnapikey.Provider
	facts  *authzrpc.MutableFacts
	base   authzfixed.Facts
	seq    int
}

func newHost(t *testing.T, engine string) *host {
	t.Helper()
	h := &host{t: t, ctx: context.Background(), engine: engine, path: filepath.Join(t.TempDir(), "b14.db")}
	h.open()
	t.Cleanup(h.close)
	// Only protected host configuration uses the fixture seam. No bead,
	// graph edge, gate observation, status, or receipt is seeded.
	body, err := service.MarshalOntologySource(service.OntologySource{Base: ontology.DefaultDocument()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conformance.Seed(h.ctx, h.store, h.lease, driver.ChangesetHeader{
		Principal: model.PrincipalRef{Kind: "system", ID: "b14-ontology-fixture"},
	}, []conformance.FixtureChange{{Ref: sourceRef, Kind: "ontology", Op: model.OpCreate, Body: body}}); err != nil {
		t.Fatal(err)
	}
	m := h.meta()
	ns := authz.Namespace{StoreID: string(m.StoreID), Incarnation: m.Incarnation, InstallationID: "fixed", Authority: ref("authority")}
	h.base = authzfixed.Facts{Namespace: ns,
		Projects: []authzfixed.Project{{Ref: ref(project), Name: project, InstallationID: "fixed", Active: true}},
		Grants:   []authz.Grant{{Subject: authz.SubjectRef{Kind: "user", ID: "user:b14-owner"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: ref(project)}, Role: "involved"}},
	}
	h.bind()
	return h
}

func (h *host) open() {
	h.t.Helper()
	var err error
	switch h.engine {
	case "memory":
		h.store = memory.New()
	case "sqlite":
		h.store, err = (sqlite.Driver{}).Open(h.ctx, driver.Config{DSN: h.path})
	default:
		h.t.Fatalf("unknown engine %q", h.engine)
	}
	if err != nil {
		h.t.Fatal(err)
	}
	h.lease, err = h.store.Lease(h.ctx, "b14-fixed-policy", driver.LeaseTTL)
	if err != nil {
		h.t.Fatal(err)
	}
}

func (h *host) bind() {
	h.t.Helper()
	var err error
	h.facts, err = authzrpc.NewMutableFacts(h.base)
	if err != nil {
		h.t.Fatal(err)
	}
	h.keys = authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
	app, err := authzrpc.New(h.ctx, authzrpc.Config{
		Store: h.store, Lease: h.lease, Provider: h.keys, Facts: h.facts,
		Namespace: h.base.Namespace, DefaultProject: project, Hierarchy: true,
		Audit: service.NewTrackedOntologyAudit(service.StaticOntologySource(sourceRef)),
	})
	if err != nil {
		h.t.Fatal(err)
	}
	// This is the actual composition's handler and single Writer. No test
	// Authorizer, RuleSet, caller source, or alternate lifecycle host exists.
	h.api = httptest.NewServer(app.Handler)
}

func (h *host) close() {
	if h.api != nil {
		h.api.Close()
		h.api = nil
	}
	if h.lease != nil {
		if err := h.lease.Release(h.ctx); err != nil {
			h.t.Error(err)
		}
		h.lease = nil
	}
	if h.store != nil {
		if err := h.store.Close(); err != nil {
			h.t.Error(err)
		}
		h.store = nil
	}
}

func (h *host) reopen() {
	h.t.Helper()
	if h.engine != "sqlite" {
		h.t.Fatal("physical reopen requires SQLite")
	}
	before := h.meta()
	h.close()
	h.open()
	h.bind() // Preserve current facts; never reseed the protected source/content.
	if after := h.meta(); !reflect.DeepEqual(before, after) {
		h.t.Fatalf("physical reopen changed identity/head: before=%+v after=%+v", before, after)
	}
}

func (h *host) meta() driver.Meta {
	h.t.Helper()
	m, err := h.store.Meta(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	return m
}

func (h *host) reader() driver.Reader {
	h.t.Helper()
	r, err := h.store.Read(h.ctx, driver.View{Incarnation: h.meta().Incarnation})
	if err != nil {
		h.t.Fatal(err)
	}
	return r
}

func (h *host) call(by model.PrincipalRef, method string, params map[string]any, key string) response {
	h.t.Helper()
	issued, err := h.keys.Issue(by, time.Now().Add(time.Hour))
	if err != nil {
		h.t.Fatal(err)
	}
	return h.callToken(issued.Token, method, params, key)
}

func (h *host) callToken(token, method string, params map[string]any, key string) response {
	h.t.Helper()
	p := make(map[string]any, len(params)+1)
	for k, v := range params {
		p[k] = v
	}
	if key != "" {
		p["_meta"] = map[string]any{"project": project, "idempotencyKey": key, "return": "full"}
	}
	h.seq++
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": h.seq, "method": method, "params": p})
	if err != nil {
		h.t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(h.ctx, http.MethodPost, h.api.URL+httpjson.Path, bytes.NewReader(body))
	if err != nil {
		h.t.Fatal(err)
	}
	req.Header.Set("Plug-RPC", "1")
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := h.api.Client().Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		h.t.Fatal(err)
	}
	var reply response
	if err = json.Unmarshal(raw, &reply); err != nil {
		h.t.Fatalf("HTTP %d %s: %s (%v)", res.StatusCode, method, raw, err)
	}
	if res.StatusCode != http.StatusOK && reply.Error == nil {
		h.t.Fatalf("HTTP %d %s without typed refusal: %s", res.StatusCode, method, raw)
	}
	return reply
}

func accepted(t *testing.T, reply response, replay bool) model.Commit {
	t.Helper()
	if reply.Error != nil || reply.Meta.Replayed != replay || len(reply.Result) == 0 {
		t.Fatalf("want accepted replay=%v: %s", replay, responseSummary(reply))
	}
	var commit model.Commit
	if err := json.Unmarshal(reply.Result, &commit); err != nil || len(commit.Results) != 1 {
		t.Fatalf("commit=%s err=%v", reply.Result, err)
	}
	return commit
}

func refused(t *testing.T, reply response, code string) {
	t.Helper()
	if reply.Error == nil || reply.Error.Data.Code != model.ErrorCode(code) || len(reply.Result) != 0 || reply.Meta.Replayed {
		t.Fatalf("want %s without result/replay: %s", code, responseSummary(reply))
	}
}

func key(n int) string { return fmt.Sprintf("b14%029x", n) }

// The oracle is the declared public contract, not the ontology evaluator or
// the pipeline document codec: ordinary updates cannot manufacture any of
// these core-owned members, even if an otherwise permitted edit shares them.
var ownedMembers = []struct {
	name  string
	value any
}{
	{"gate_kind", "human_approval"},
	{"gate_state", "resolved"},
	{"gate_approvals", []any{map[string]any{"principal": map[string]any{"kind": "user", "id": "user:b14-owner"}, "token": "forged"}}},
	{"gate_approval_count", 1},
	{"gate_details", map[string]any{"resolver": "forged"}},
}

type state struct {
	Meta     driver.Meta
	Rows     []driver.Record
	Versions map[string][]driver.Version
	History  []model.Changeset
}

func (h *host) state() state {
	h.t.Helper()
	r := h.reader()
	defer r.Close()
	page, err := r.Query(h.ctx, driver.Query{Limit: 1000})
	if err != nil || page.NextCursor != nil {
		h.t.Fatalf("complete content query: %+v %v", page, err)
	}
	sort.Slice(page.Items, func(i, j int) bool { return page.Items[i].Ref.ID < page.Items[j].Ref.ID })
	s := state{Meta: h.meta(), Rows: page.Items, Versions: map[string][]driver.Version{}}
	for _, row := range page.Items {
		versions, err := r.Versions(h.ctx, row.Ref, driver.PageReq{Limit: 1000})
		if err != nil || versions.NextCursor != nil {
			h.t.Fatalf("complete versions %s: %+v %v", row.Ref.ID, versions, err)
		}
		s.Versions[row.Ref.ID] = versions.Items
	}
	s.History, err = r.Changesets(h.ctx, nil, 1000)
	if err != nil || len(s.History) >= 1000 {
		h.t.Fatalf("complete history: %d %v", len(s.History), err)
	}
	return s
}

func (h *host) unchanged(before state) {
	h.t.Helper()
	if after := h.state(); !reflect.DeepEqual(before, after) {
		h.t.Fatalf("refusal/replay changed records, versions, events or metadata:\nbefore=%+v\nafter=%+v", before, after)
	}
}

func (h *host) receipt(method, k string) *driver.IdempotencyRecord {
	h.t.Helper()
	r := h.reader()
	defer r.Close()
	p, err := driver.PrincipalKey(worker())
	if err != nil {
		h.t.Fatal(err)
	}
	receipt, err := r.IdempotencyByKey(h.ctx, p, "", method, k)
	if err != nil {
		h.t.Fatal(err)
	}
	return receipt
}

func (h *host) noReceipt(method, k string) {
	h.t.Helper()
	if receipt := h.receipt(method, k); receipt != nil {
		h.t.Fatalf("refused %s left receipt: %+v", method, receipt)
	}
}

func (h *host) oneWrite(before state, commit model.Commit, k string, event model.DurableEventType) {
	h.t.Helper()
	method := "bead.update"
	if event == model.EventBeadCreated {
		method = "bead.create"
	}
	after := h.state()
	if len(after.History) != len(before.History)+1 || after.Meta.Head.String() != commit.Revision.String() {
		h.t.Fatalf("accepted write did not publish one head/history entry: %+v", commit)
	}
	entry := after.History[len(after.History)-1]
	if entry.Revision.String() != commit.Revision.String() || !reflect.DeepEqual(entry.Principal, worker()) ||
		!reflect.DeepEqual(entry.Chain, []string{"authz/fixed"}) || entry.IdempotencyKey != k ||
		len(entry.Changes) != 1 || entry.Changes[0].Ref.ID != commit.Results[0].Ref.ID ||
		len(entry.Events) != 1 || entry.Events[0].Type != event || entry.Events[0].Provenance.Method != method ||
		entry.Events[0].Provenance.Cause.Root != k || !reflect.DeepEqual(entry.Events[0].Provenance.Chain, []string{"authz/fixed"}) {
		h.t.Fatalf("write lost fixed-policy principal/event provenance: %+v", entry)
	}
	target := commit.Results[0].Ref.ID
	if len(after.Versions[target]) != len(before.Versions[target])+1 {
		h.t.Fatal("accepted write did not append exactly one target version")
	}
	for id, versions := range before.Versions {
		if id != target && !reflect.DeepEqual(versions, after.Versions[id]) {
			h.t.Fatalf("accepted write changed unrelated %s history", id)
		}
	}
	receipt := h.receipt(method, k)
	if receipt == nil || receipt.ResponsePending || receipt.RequestHash == "" || !reflect.DeepEqual(receipt.Commit, commit) {
		h.t.Fatalf("accepted write missing final exact receipt: %+v", receipt)
	}
}

func (h *host) fields(id, title string, template bool) {
	h.t.Helper()
	by := model.PrincipalRef{Kind: "user", ID: "user:b14-reader"}
	reply := h.call(by, "bead.get", map[string]any{"id": ref(id)}, "")
	if reply.Error != nil {
		h.t.Fatalf("public get refused: %s", responseSummary(reply))
	}
	var result struct {
		Bead struct {
			Fields map[string]any `json:"fields"`
		} `json:"bead"`
	}
	if err := json.Unmarshal(reply.Result, &result); err != nil {
		h.t.Fatal(err)
	}
	want := map[string]any{"title": title, "status": "todo", "priority": float64(2), "template": template}
	if !reflect.DeepEqual(result.Bead.Fields, want) {
		h.t.Fatalf("public subject fields=%v want independently declared %v", result.Bead.Fields, want)
	}
}

func (h *host) ready(want ...string) {
	h.t.Helper()
	reply := h.call(model.PrincipalRef{Kind: "user", ID: "user:b14-reader"}, "bead.ready", map[string]any{"limit": 1000}, "")
	if reply.Error != nil {
		h.t.Fatalf("public readiness refused: %s", responseSummary(reply))
	}
	var page struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
		Next any `json:"nextCursor"`
	}
	if err := json.Unmarshal(reply.Result, &page); err != nil || page.Next != nil {
		h.t.Fatalf("complete public ready=%s err=%v", reply.Result, err)
	}
	got := make([]string, 0, len(page.Items))
	for _, item := range page.Items {
		got = append(got, item.ID)
	}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		h.t.Fatalf("public ready membership=%v want declared %v", got, want)
	}
}

func (h *host) grant(allow bool) {
	h.t.Helper()
	before := h.state()
	base := h.base
	if allow {
		base.Grants = []authz.Grant{{Subject: authz.SubjectRef{Kind: "user", ID: "user:b14-owner"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: ref(project)}, Role: "involved"}}
	} else {
		base.Grants = nil
	}
	if err := h.facts.Replace(base); err != nil {
		h.t.Fatal(err)
	}
	h.base = base
	after := h.state()
	if len(after.History) != len(before.History)+1 || after.Meta.Head.String() == before.Meta.Head.String() ||
		!reflect.DeepEqual(before.Rows, after.Rows) || !reflect.DeepEqual(before.Versions, after.Versions) {
		h.t.Fatal("current grant replacement must advance only its real ACL revision")
	}
}

func TestB14FixedPolicyGateMemberRefusalPreservesPublicMembershipAndCurrentReplay(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			h := newHost(t, engine)
			create := func(title string, n int) string {
				before := h.state()
				commit := accepted(t, h.call(worker(), "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": title}}, key(n)), false)
				h.oneWrite(before, commit, key(n), model.EventBeadCreated)
				return commit.Results[0].Ref.ID
			}
			subject, control := create("ordinary subject", 1), create("independent ready control", 2)
			h.fields(subject, "ordinary subject", false)
			h.ready(subject, control)

			for i, member := range ownedMembers {
				t.Run(member.name, func(t *testing.T) {
					parent := h.t
					h.t = t
					defer func() { h.t = parent }()
					before := h.state()
					k := key(10 + i)
					params := map[string]any{"id": ref(subject), "fields": map[string]any{"title": "must roll back", "template": true, member.name: member.value}}
					refused(t, h.call(worker(), "bead.update", params, k), "ontology_use_slot_verb")
					h.unchanged(before)
					h.noReceipt("bead.update", k)
					h.fields(subject, "ordinary subject", false)
					h.ready(subject, control)
				})
			}

			beforeAuth := h.state()
			for i, token := range []string{"", "b14-invalid-credential"} {
				k := key(40 + i)
				refused(t, h.callToken(token, "bead.update", map[string]any{"id": ref(subject), "fields": map[string]any{"template": true}}, k), "unauthenticated")
				h.noReceipt("bead.update", k)
			}
			h.unchanged(beforeAuth)

			// Retrying a refused key with an ordinary, admitted whole proposal
			// proves that no invisible failed receipt can poison that key.
			params := map[string]any{"id": ref(subject), "fields": map[string]any{"template": true}}
			before := h.state()
			first := h.call(worker(), "bead.update", params, key(10))
			commit := accepted(t, first, false)
			h.oneWrite(before, commit, key(10), model.EventBeadUpdated)
			h.fields(subject, "ordinary subject", true)
			h.ready(control)
			before = h.state()
			second := accepted(t, h.call(worker(), "bead.update", map[string]any{"id": ref(subject), "fields": map[string]any{"template": false}}, key(30)), false)
			h.oneWrite(before, second, key(30), model.EventBeadUpdated)
			h.ready(subject, control)

			// An older accepted response describes template=true. Returning it
			// cannot reapply that stale image or remove today's ready member.
			before = h.state()
			replay := accepted(t, h.call(worker(), "bead.update", params, key(10)), true)
			if !reflect.DeepEqual(replay, commit) {
				t.Fatal("accepted receipt replay changed its original result")
			}
			h.unchanged(before)
			h.fields(subject, "ordinary subject", false)
			h.ready(subject, control)

			h.grant(false)
			before = h.state() // ACL replacement is not attributed to a refused call.
			savedReceipt := h.receipt("bead.update", key(10))
			refused(t, h.call(worker(), "bead.update", params, key(10)), "forbidden")
			refused(t, h.call(worker(), "bead.update", params, key(31)), "forbidden")
			h.noReceipt("bead.update", key(31))
			h.unchanged(before)
			if !reflect.DeepEqual(savedReceipt, h.receipt("bead.update", key(10))) {
				t.Fatal("revoked replay changed the immutable original receipt")
			}
			h.ready(subject, control)

			if engine == "sqlite" {
				h.reopen()
				h.unchanged(before)
				refused(t, h.call(worker(), "bead.update", params, key(10)), "forbidden")
				h.unchanged(before)
				h.fields(subject, "ordinary subject", false)
				h.ready(subject, control)
				if !reflect.DeepEqual(savedReceipt, h.receipt("bead.update", key(10))) {
					t.Fatal("physical reopen changed the retained receipt")
				}
			}
			// Restoring the actual current grant permits the immutable receipt
			// again, while its old template=true image still cannot be applied.
			h.grant(true)
			before = h.state()
			if replay := accepted(t, h.call(worker(), "bead.update", params, key(10)), true); !reflect.DeepEqual(replay, commit) {
				t.Fatal("current grant restoration did not recover the exact receipt")
			}
			h.unchanged(before)
			h.fields(subject, "ordinary subject", false)
			h.ready(subject, control)
		})
	}
}
