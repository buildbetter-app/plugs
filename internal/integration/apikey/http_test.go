package apikey_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/integration/apikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

func TestRealHTTPAPIKeysStampCoreWritesAndIsolateReplay(t *testing.T) {
	f := newFixture(t, nil)
	user := model.PrincipalRef{Kind: "user", ID: "user:operator"}
	service := model.PrincipalRef{Kind: "service", ID: "service:worker"}
	userKey, serviceKey := f.issue(t, user), f.issue(t, service)
	params := writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "Authenticated core call"}})
	first := f.call(t, userKey.Token, "bead.create", params, nil)
	created := success(t, first)
	ref := created["results"].([]any)[0].(map[string]any)["ref"].(map[string]any)
	other := f.call(t, serviceKey.Token, "bead.create", params, nil)
	otherRef := success(t, other)["results"].([]any)[0].(map[string]any)["ref"]
	if reflect.DeepEqual(ref, otherRef) || other.value["_meta"] != nil {
		t.Fatal("different authenticated principals shared an idempotency receipt")
	}
	beforeReplay := f.metadata(t).Head
	replay := f.call(t, userKey.Token, "bead.create", params, nil)
	if !reflect.DeepEqual(success(t, replay), created) || replay.value["_meta"].(map[string]any)["replayed"] != true || f.metadata(t).Head != beforeReplay {
		t.Fatal("authenticated retry did not replay its own durable receipt")
	}
	success(t, f.call(t, userKey.Token, "bead.claim", writeParams(t, map[string]any{"id": ref}), nil))
	got := success(t, f.call(t, serviceKey.Token, "bead.get", map[string]any{"id": ref}, nil))
	lease := got["lease"].(map[string]any)
	if lease["holder"] != user.ID || lease["agent"] != nil {
		t.Fatal("core lease did not retain the key's authoritative principal")
	}
	meta := f.metadata(t)
	reader, err := f.store.Store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	changes, err := reader.Changesets(context.Background(), nil, 10)
	if err != nil || len(changes) != 3 {
		t.Fatal("actual create/create/claim changesets were not persisted")
	}
	for i, want := range []model.PrincipalRef{user, service, user} {
		if !reflect.DeepEqual(changes[i].Principal, want) {
			t.Fatal("persisted changeset lost its authenticated principal")
		}
	}
	data, err := json.Marshal(changes)
	if err != nil {
		t.Fatal("unable to inspect persisted changesets")
	}
	credentialFree(t, data, userKey, serviceKey)
	credentialFree(t, first.body, userKey, serviceKey)
	credentialFree(t, other.body, userKey, serviceKey)
	spoof := writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "Client identity"}})
	spoof["_meta"].(map[string]any)["principal"] = map[string]any{"kind": "user", "id": "user:someone-else"}
	reads := f.store.reads.Load()
	code(t, f.call(t, serviceKey.Token, "bead.create", spoof, nil), http.StatusOK, "invalid_params")
	if f.metadata(t).Head != meta.Head || f.store.reads.Load() != reads {
		t.Fatal("caller-asserted identity reached core storage")
	}
}

func TestRealHTTPRefusesUnknownRevokedExpiredAndMalformedKeys(t *testing.T) {
	f := newFixture(t, nil)
	principal := model.PrincipalRef{Kind: "service", ID: "service:refusal"}
	valid := f.issue(t, principal)
	revoked := f.issue(t, principal)
	if !f.keys.Revoke(revoked.ID) {
		t.Fatal("revocation was not acknowledged")
	}
	unknown, err := authnapikey.New(authnapikey.Config{}).Issue(principal, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	params := writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "Must not be written"}})
	for _, test := range []struct {
		name, token string
		change      func(*http.Request)
		status      int
		code        string
	}{
		{name: "missing", status: 401, code: "unauthenticated"},
		{name: "malformed", token: "malformed", status: 401, code: "unauthenticated"},
		{name: "unknown", token: unknown.Token, status: 401, code: "unauthenticated"},
		{name: "wrong secret", token: "plg_k_" + valid.ID + "_" + strings.Repeat("A", 43), status: 401, code: "unauthenticated"},
		{name: "revoked", token: revoked.Token, status: 401, code: "unauthenticated"},
		{name: "duplicate credential", token: valid.Token, change: func(r *http.Request) { r.Header.Add("Authorization", "Bearer "+unknown.Token) }, status: 401, code: "unauthenticated"},
		{name: "missing admission header", token: valid.Token, change: func(r *http.Request) { r.Header.Del("Plug-RPC") }, status: 403, code: "forbidden"},
		{name: "other origin", token: valid.Token, change: func(r *http.Request) { r.Header.Set("Origin", "http://example.invalid") }, status: 403, code: "forbidden"},
	} {
		t.Run(test.name, func(t *testing.T) {
			reply := f.call(t, test.token, "bead.create", params, test.change)
			code(t, reply, test.status, test.code)
			credentialFree(t, reply.body, valid, revoked, unknown)
			if reply.value["id"] != nil || (test.status == 401 && reply.header.Get("WWW-Authenticate") != "Bearer") {
				t.Fatal("authentication failure did not preserve the transport envelope")
			}
			if f.store.reads.Load() != 0 || !f.metadata(t).Head.IsZero() {
				t.Fatal("refused credential reached actual core storage")
			}
		})
	}
	// Prove this same listener can execute an admitted call, then cross the
	// exact expiry boundary and try a fresh write with that same credential.
	success(t, f.call(t, valid.Token, "bead.create", params, nil))
	meta, reads := f.metadata(t), f.store.reads.Load()
	f.clock.Store(valid.ExpiresAt.UnixNano())
	expired := f.call(t, valid.Token, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "Expired"}}), nil)
	code(t, expired, http.StatusUnauthorized, "token_expired")
	credentialFree(t, expired.body, valid)
	if f.metadata(t).Head != meta.Head || f.store.reads.Load() != reads {
		t.Fatal("expired key reached actual core storage")
	}
}

func TestRealHTTPCanceledAdmissionNeverReachesCore(t *testing.T) {
	f := newFixture(t, nil)
	issued := f.issue(t, model.PrincipalRef{Kind: "service", ID: "service:canceled"})
	f.cancel()
	reply := f.call(t, issued.Token, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "Canceled"}}), nil)
	code(t, reply, http.StatusServiceUnavailable, "authn_unavailable")
	credentialFree(t, reply.body, issued)
	data := reply.value["error"].(map[string]any)["data"].(map[string]any)
	if data["retryable"] != true || reply.value["id"] != nil || f.store.reads.Load() != 0 || !f.metadata(t).Head.IsZero() {
		t.Fatal("canceled admission returned a principal or reached actual storage")
	}
}

func TestRealHTTPScopedKeyCannotSupplyItsOwnInstalledAuthorizer(t *testing.T) {
	// A ring may have been provisioned for a different host with authorization.
	// Its predicate cannot install enforcement in this separate composition.
	f := newFixture(t, func() bool { return true })
	for _, principal := range []model.PrincipalRef{
		{Kind: "agent", ID: "agent:worker", Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:sponsor"}},
		{Kind: "user", ID: "user:scoped", Caveats: map[string]any{"projects": []any{"apikey-test"}}},
	} {
		issued := f.issue(t, principal)
		reply := f.call(t, issued.Token, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "Scoped"}}), nil)
		code(t, reply, http.StatusForbidden, "authz_not_installed")
		credentialFree(t, reply.body, issued)
		if f.store.reads.Load() != 0 || !f.metadata(t).Head.IsZero() {
			t.Fatal("scoped key bypassed missing installed authorization")
		}
	}
}

func TestAPIKeysStayNoninteractiveAndUnsupportedMethodsRefuse(t *testing.T) {
	f := newFixture(t, nil)
	admission, err := authn.New(authn.Config{Provider: f.keys})
	if err != nil {
		t.Fatal(err)
	}
	for _, principal := range []model.PrincipalRef{{Kind: "user", ID: "user:operator"}, {Kind: "service", ID: "service:worker"}} {
		issued := f.issue(t, principal)
		identity, err := admission.Admit(context.Background(), issued.Token)
		if err != nil || !reflect.DeepEqual(identity.Principal(), principal) || !errors.Is(identity.RequireInteractiveUser(), authn.ErrHumanPresenceRequired) {
			t.Fatal("API key was treated as an interactive human session")
		}
		for _, method := range []string{"plugin.apply", "ontology.apply", "auth.whoami"} {
			code(t, f.call(t, issued.Token, method, map[string]any{}, nil), http.StatusOK, "method_not_found")
		}
	}
	if f.store.reads.Load() != 0 || !f.metadata(t).Head.IsZero() {
		t.Fatal("unsupported authenticated methods reached storage")
	}
}

func TestCompositionRequiresExplicitProviderAndResources(t *testing.T) {
	var store *memory.Store
	for _, config := range []apikey.Config{
		{},
		{Store: store, Keys: authnapikey.New(authnapikey.Config{})},
		{Store: memory.New(), Keys: authnapikey.New(authnapikey.Config{})},
	} {
		handler, err := apikey.New(context.Background(), config)
		if handler != nil || err == nil {
			t.Fatal("incomplete composition was accepted or fell back to anonymous")
		}
		if config.Store != nil && config.Store != store {
			_ = config.Store.Close()
		}
	}
}

func credentialFree(t *testing.T, data []byte, keys ...authnapikey.IssuedKey) {
	t.Helper()
	for _, issued := range keys {
		_, secret, _ := strings.Cut(strings.TrimPrefix(issued.Token, "plg_k_"), "_")
		if bytes.Contains(data, []byte(issued.Token)) || bytes.Contains(data, []byte(issued.ID)) || bytes.Contains(data, []byte(secret)) {
			t.Fatal("credential material escaped into a response or durable changeset")
		}
	}
}
