package authzrpc_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

const beadID = "00000000-0000-4000-8000-000000000001"

type leaseLossSnapshotStore struct {
	driver.Store
	lost      atomic.Bool
	metaReads atomic.Int64
}

func (s *leaseLossSnapshotStore) Meta(ctx context.Context) (driver.Meta, error) {
	if s.lost.Load() {
		s.metaReads.Add(1)
		return driver.Meta{}, errors.New("snapshot access after lease loss")
	}
	return s.Store.Meta(ctx)
}

func TestLostLeaseFencesAuthorizationSnapshotsBeforeStorage(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(ctx, "lost-lease-snapshots", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(ctx) })
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))}
	observed := &leaseLossSnapshotStore{Store: store}
	f := &fixture{store: store, keys: authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})}
	f.server, err = authzrpc.New(ctx, authzrpc.Config{
		Store: observed, Lease: lease, Provider: f.keys,
		Facts:     authzrpc.StaticFacts{Facts: authzfixed.Facts{Namespace: ns}},
		Namespace: ns, DefaultProject: "project",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	observed.lost.Store(true)
	for i, method := range []string{"bead.get", "bead.claimNext"} {
		t.Run(method, func(t *testing.T) {
			params := map[string]any{}
			if method == "bead.get" {
				params["id"] = model.RefByID(beadID, model.Natural64(0))
			}
			got := f.call(t, worker("user:operator"), method, params, 9000+i)
			if got.Error == nil || got.Error.Data.Code != "busy" || !got.Error.Data.Retryable || got.Error.Data.Hint != "active storage lease was lost" {
				t.Error("lost lease was not returned as a retryable busy response before snapshot access")
			}
		})
	}
	if reads := observed.metaReads.Load(); reads != 0 {
		t.Fatalf("authorization accessed storage %d times after lease loss", reads)
	}
}

type fixture struct {
	store  driver.Store
	lease  driver.ActiveLease
	keys   *authnapikey.Provider
	facts  *authzrpc.MutableFacts
	base   authzfixed.Facts
	server *authzrpc.Composition
}

func newFixture(t *testing.T, mutators []pipeline.MutatorBinding) *fixture {
	t.Helper()
	return newFixtureOn(t, memory.New(), mutators)
}

func newFixtureOn(t *testing.T, store driver.Store, mutators []pipeline.MutatorBinding) *fixture {
	t.Helper()
	return newFixtureWith(t, store, mutators, authzrpc.New)
}

func newFixtureWith(t *testing.T, store driver.Store, mutators []pipeline.MutatorBinding, construct func(context.Context, authzrpc.Config) (*authzrpc.Composition, error)) *fixture {
	t.Helper()
	ctx := context.Background()
	f := &fixture{store: store}
	t.Cleanup(func() { _ = f.store.Close() })
	lease, err := f.store.Lease(ctx, "authzrpc-test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	f.lease = lease
	t.Cleanup(func() { _ = lease.Release(ctx) })
	// Seed a disposable record through the adopted storage fixture. No second
	// Writer or alternate service exists: all product writes use New's Writer.
	_, err = conformance.Seed(ctx, f.store, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{
		Ref: model.RefByID(beadID, model.Natural64(0)), Kind: "bead", Op: model.OpCreate,
		Body: []byte(`{"id":"` + beadID + `","type":"issue","project":"project","createdAt":"2026-10-01T00:00:00Z","fields":{"title":"seed","status":"todo","priority":0}}`),
	}})
	if err != nil {
		t.Fatal(err)
	}
	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))}
	team, project := model.RefByID("team", model.Natural64(0)), model.RefByID("project", model.Natural64(0))
	f.base = authzfixed.Facts{
		Namespace: ns,
		Teams:     []authzfixed.Team{{Ref: team, InstallationID: "fixed", Active: true}},
		Projects:  []authzfixed.Project{{Ref: project, Name: "project", InstallationID: "fixed", Active: true}},
		Grants: []authz.Grant{
			{Subject: authz.SubjectRef{Kind: "user", ID: "user:operator"}, Scope: authz.Scope{Kind: authz.TeamScope, Ref: team}, Role: "member"},
			{Subject: authz.SubjectRef{Kind: "team", ID: "team"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: project}, Role: "involved"},
		},
	}
	f.facts, err = authzrpc.NewMutableFacts(f.base)
	if err != nil {
		t.Fatal(err)
	}
	f.keys = authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
	f.server, err = construct(ctx, authzrpc.Config{
		Store: f.store, Lease: lease, Provider: f.keys, Facts: f.facts,
		Namespace: ns, DefaultProject: "project", Mutators: mutators,
	})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func worker(sponsor string) model.PrincipalRef {
	return model.PrincipalRef{Kind: "agent", ID: "agent:worker", Sponsor: &model.PrincipalRef{Kind: "user", ID: sponsor}, Caveats: map[string]any{
		"actions": []string{"modify"}, "projects": []model.Ref{model.RefByID("project", model.Natural64(0))},
	}}
}

type reply struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data model.ErrorData `json:"data"`
	} `json:"error"`
	Meta struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
}

func (f *fixture) call(t *testing.T, by model.PrincipalRef, method string, params map[string]any, key int) reply {
	t.Helper()
	issued, err := f.keys.Issue(by, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	meta, _ := params["_meta"].(map[string]any)
	if meta == nil {
		meta = map[string]any{}
	}
	meta["idempotencyKey"] = fmt.Sprintf("%032x", key)
	params["_meta"] = meta
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
	f.server.Handler.ServeHTTP(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("HTTP status = %d", response.Code)
	}
	var got reply
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	return got
}

func success(t *testing.T, got reply) {
	t.Helper()
	if got.Error != nil {
		t.Fatalf("RPC error = %+v", got.Error.Data)
	}
	if len(got.Result) == 0 {
		t.Fatal("RPC omitted its result")
	}
}

func samePrincipal(t *testing.T, a, b model.PrincipalRef) bool {
	t.Helper()
	left, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	right, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	// Authentication and durable storage detach JSON values, including caveats.
	var leftValue, rightValue any
	for _, item := range []struct {
		data  []byte
		value *any
	}{{left, &leftValue}, {right, &rightValue}} {
		decoder := json.NewDecoder(bytes.NewReader(item.data))
		decoder.UseNumber()
		if err := decoder.Decode(item.value); err != nil {
			t.Fatal(err)
		}
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

func (f *fixture) reader(t *testing.T) driver.Reader {
	t.Helper()
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func (f *fixture) receipt(t *testing.T, by model.PrincipalRef, method string, key int) *driver.IdempotencyRecord {
	t.Helper()
	principal, err := driver.PrincipalKey(by)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := f.reader(t).IdempotencyByKey(context.Background(), principal, "", method, fmt.Sprintf("%032x", key))
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func hostMutators(t *testing.T, calls *atomic.Int32, fail bool) []pipeline.MutatorBinding {
	t.Helper()
	native := sdk.Native{
		Instance: "default",
		Manifest: pluginapi.PluginManifest{ID: "app.example.authzrpc", Alias: "authzrpc", Version: "1.0.0", PluginAPI: pluginapi.APIVersion, Runtime: "native", Provides: []pluginapi.Provision{{
			Kind: "subscriber", Name: "normalize", Phases: []pluginapi.Phase{pluginapi.PhaseMutate}, Reads: []string{"fields.title"}, Owns: []string{"fields.title"}, MaxPointReads: 1,
		}}},
		New: func(pluginapi.Host) (sdk.Registration, error) {
			return sdk.Registration{Mutators: map[string]pluginapi.Mutator{"normalize": sdk.MutateFunc(func(ctx context.Context, change pluginapi.ProposedChange, snapshot pluginapi.SnapshotReader) ([]pluginapi.Patch, error) {
				calls.Add(1)
				if change.Principal.Kind != "agent" || change.Principal.ID != "agent:worker" || change.Principal.Sponsor == nil || change.Principal.Sponsor.ID != "user:operator" {
					return nil, errors.New("authenticated principal was not preserved")
				}
				if fail {
					return nil, errors.New("mutator unavailable")
				}
				rows, err := snapshot.Get(ctx, []pluginapi.Ref{pluginapi.RefByID(beadID, "")})
				if err != nil || len(rows) != 1 || rows[0].Revision != change.BaseRevision {
					return nil, errors.New("mutator did not receive the pinned snapshot")
				}
				var doc pipeline.Document
				if err := json.Unmarshal(change.Changes[0].After, &doc); err != nil {
					return nil, err
				}
				title, _ := doc.Fields["title"].(string)
				value, err := json.Marshal(strings.ToUpper(title))
				return []pluginapi.Patch{{Resource: change.Changes[0].Resource, Paths: map[string]json.RawMessage{"fields.title": value}}}, err
			})}}, nil
		},
	}
	host, err := pluginhost.New(pluginhost.Config{Natives: []sdk.Native{native}, Bindings: []pluginhost.Binding{{
		ID: "normalize", Alias: "authzrpc", Instance: "default", Subscriber: "normalize", Phase: pluginapi.PhaseMutate, Reads: []string{"fields.title"}, Owns: []string{"fields.title"}, MaxPointReads: 1,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return host.Mutators()
}

func TestConfiguredHostMutatorsUseGuardedWriter(t *testing.T) {
	var calls atomic.Int32
	f := newFixture(t, hostMutators(t, &calls, false))
	by := worker("user:operator")
	params := map[string]any{"id": model.RefByID(beadID, model.Natural64(0)), "fields": map[string]any{"title": "changed"}}
	first := f.call(t, by, "bead.update", params, 1)
	success(t, first)
	rows, err := f.reader(t).Get(context.Background(), []model.Ref{model.RefByID(beadID, model.Natural64(0))})
	if err != nil || len(rows) != 1 {
		t.Fatal("committed bead is missing")
	}
	doc, err := pipeline.DecodeDocument(rows[0].Body)
	if err != nil || doc.Fields["title"] != "CHANGED" || calls.Load() != 1 {
		t.Fatalf("guarded Writer did not persist the configured host mutation: calls=%d", calls.Load())
	}
	receipt := f.receipt(t, by, "bead.update", 1)
	if receipt == nil || !samePrincipal(t, receipt.Changeset.Principal, by) || !reflect.DeepEqual(receipt.Changeset.Mutators, []string{"normalize"}) {
		t.Fatal("mutator and authenticated principal must share the durable receipt")
	}
	replay := f.call(t, by, "bead.update", params, 1)
	success(t, replay)
	if !bytes.Equal(first.Result, replay.Result) || !replay.Meta.Replayed || calls.Load() != 1 {
		t.Fatal("replay reran the mutator or changed the durable result")
	}
	before := *f.reader(t).Snapshot().At
	denied := f.call(t, worker("user:outsider"), "bead.update", params, 2)
	if denied.Error == nil || denied.Error.Data.Code != "forbidden" || calls.Load() != 1 || f.receipt(t, worker("user:outsider"), "bead.update", 2) != nil || f.reader(t).Snapshot().At.String() != before.String() {
		t.Fatal("configured mutator bypassed authorization before execution")
	}
	revoked := f.base
	revoked.Grants = nil
	if err := f.facts.Replace(revoked); err != nil {
		t.Fatal(err)
	}
	before = *f.reader(t).Snapshot().At
	denied = f.call(t, by, "bead.update", params, 1)
	if denied.Error == nil || denied.Error.Data.Code != "forbidden" || calls.Load() != 1 || f.reader(t).Snapshot().At.String() != before.String() {
		t.Fatal("configured mutator bypassed current authorization on replay")
	}
}

func TestClaimNextDefaultsWithOptionalMutators(t *testing.T) {
	for _, mode := range []string{"omitted", "empty", "configured"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			var bindings []pipeline.MutatorBinding
			if mode == "empty" {
				bindings = []pipeline.MutatorBinding{}
			} else if mode == "configured" {
				bindings = hostMutators(t, &calls, false)
			}
			f := newFixtureWith(t, memory.New(), bindings, func(ctx context.Context, config authzrpc.Config) (*authzrpc.Composition, error) {
				// This positive claim test installs its distinct transition
				// policy; the default fixture continues to grant none.
				config.ClaimPolicy = &authzrpc.ClaimPolicyConfig{UseImmutableDefault: true}
				return authzrpc.New(ctx, config)
			})
			by := worker("user:operator")
			by.Caveats["actions"] = []string{"modify", "transition"}
			claimed := f.call(t, by, "bead.claimNext", map[string]any{}, 10)
			success(t, claimed)
			var result map[string]json.RawMessage
			if err := json.Unmarshal(claimed.Result, &result); err != nil || result["leaseTtlMs"] == nil || result["revision"] == nil || result["leaseExpiresAt"] == nil {
				t.Fatal("claimNext binding did not return the cataloged claim outcome")
			}
			rows, err := f.reader(t).Get(context.Background(), []model.Ref{model.RefByID(beadID, model.Natural64(0))})
			if err != nil || len(rows) != 1 {
				t.Fatal("claimed bead is missing")
			}
			doc, err := pipeline.DecodeDocument(rows[0].Body)
			wantTitle, wantCalls := "seed", int32(0)
			if mode == "configured" {
				wantTitle, wantCalls = "SEED", 1
			}
			if err != nil || doc.Lease == nil || !samePrincipal(t, doc.Lease.Principal, by) || doc.Fields["title"] != wantTitle || calls.Load() != wantCalls {
				t.Fatal("claimNext changed principal, defaults or configured mutation")
			}
			before, err := f.reader(t).Snapshot().At.Uint64()
			if err != nil {
				t.Fatal(err)
			}
			empty := f.call(t, by, "bead.claimNext", map[string]any{}, 11)
			success(t, empty)
			receipt := f.receipt(t, by, "bead.claimNext", 11)
			after, err := f.reader(t).Snapshot().At.Uint64()
			if err != nil || string(empty.Result) != "null" || empty.Meta.Replayed || after != before+1 || receipt == nil || receipt.Commit.Revision.String() != fmt.Sprint(after) || len(receipt.Changeset.Changes) != 0 || len(receipt.Changeset.Events) != 0 {
				t.Fatal("keyed terminal empty must retain its durable H+1 zero-change receipt")
			}
			replay := f.call(t, by, "bead.claimNext", map[string]any{}, 11)
			success(t, replay)
			if string(replay.Result) != "null" || !replay.Meta.Replayed || calls.Load() != wantCalls || f.reader(t).Snapshot().At.String() != fmt.Sprint(after) {
				t.Fatal("empty replay changed the head or reran a configured mutator")
			}
		})
	}
}

func TestConfiguredMutatorFailurePublishesNothing(t *testing.T) {
	var calls atomic.Int32
	f := newFixture(t, hostMutators(t, &calls, true))
	by := worker("user:operator")
	before := *f.reader(t).Snapshot().At
	got := f.call(t, by, "bead.update", map[string]any{"id": model.RefByID(beadID, model.Natural64(0)), "fields": map[string]any{"title": "refused"}}, 20)
	if got.Error == nil || got.Error.Data.Code != "subscriber_unavailable" || calls.Load() != 1 || f.reader(t).Snapshot().At.String() != before.String() || f.receipt(t, by, "bead.update", 20) != nil {
		t.Fatal("mutator failure must refuse the write without a receipt or head advance")
	}
	rows, err := f.reader(t).Get(context.Background(), []model.Ref{model.RefByID(beadID, model.Natural64(0))})
	if err != nil || len(rows) != 1 {
		t.Fatal("stored bead is missing after refusal")
	}
	doc, err := pipeline.DecodeDocument(rows[0].Body)
	if err != nil || doc.Fields["title"] != "seed" {
		t.Fatal("mutator failure published a partial update")
	}
}

// The finite ordinary adapter deliberately has no close permit. A new union
// member must reach that fail-closed authorization boundary before exposing a
// stale guard, invoking a configured mutator, or changing any durable image.
func TestAtomicCloseCannotBorrowTxApplyOrUpdateAuthority(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			var open storeFactory
			switch engine {
			case "memory":
				open = func(*testing.T) driver.Store { return memory.New() }
			case "sqlite":
				open = func(t *testing.T) driver.Store {
					s, err := (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: filepath.Join(t.TempDir(), "close-authz.db")})
					if err != nil {
						t.Fatal(err)
					}
					return s
				}
			case "postgres":
				open = postgresFactory(t)
			}
			var calls atomic.Int32
			f := newFixtureOn(t, open(t), hostMutators(t, &calls, false))
			before := *f.reader(t).Snapshot().At
			for i, by := range []model.PrincipalRef{worker("user:operator"), worker("user:outsider")} {
				params := map[string]any{"changeSet": map[string]any{"ops": []any{map[string]any{
					"kind": "close", "target": model.RefByID(beadID, model.Natural64(0)), "reason": "must not append",
					"expect": []model.Precondition{model.PreconditionByRevision(model.RefByID(beadID, model.Natural64(0)), model.Natural64(0))},
				}}}}
				got := f.call(t, by, "tx.apply", params, 9900+i)
				if got.Error == nil || got.Error.Data.Code != "forbidden" || len(got.Error.Data.Details) != 0 || len(got.Result) != 0 || got.Meta.Replayed {
					t.Fatalf("close borrowed batch/update authority or disclosed CAS: %+v", got)
				}
				if f.receipt(t, by, "tx.apply", 9900+i) != nil || f.reader(t).Snapshot().At.String() != before.String() || calls.Load() != 0 {
					t.Fatal("denied close changed receipt/head or invoked a mutator")
				}
			}
		})
	}
}
