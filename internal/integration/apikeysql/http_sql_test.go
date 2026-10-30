package apikeysql_test

import (
	"bytes"
	"context"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestAPIKeySQLDurableAttributionAndReplayAcrossReopen(t *testing.T) {
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) {
			f := newFixture(t, b)
			alice := f.issue(t, model.PrincipalRef{Kind: "user", ID: "user:alice"}, f.now().Add(time.Hour))
			bob := f.issue(t, model.PrincipalRef{Kind: "user", ID: "user:bob"}, f.now().Add(time.Hour))

			params := fixedParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "durable API-key write"}})
			firstAlice := f.call(t, alice.Token, "bead.create", params, nil)
			aliceResult := success(t, firstAlice)
			aliceRef := createdRef(t, aliceResult)
			if firstAlice.meta != nil {
				t.Fatal("first API-key write was incorrectly marked replayed")
			}

			// The same wire request key is independent for another authenticated
			// User. This is the durable G4 principal scope, not a memory map.
			firstBob := f.call(t, bob.Token, "bead.create", params, nil)
			bobResult := success(t, firstBob)
			bobRef := createdRef(t, bobResult)
			if reflect.DeepEqual(aliceRef, bobRef) || firstBob.meta != nil || f.meta(t).Head.String() != "2" {
				t.Fatalf("independent User receipt scopes collapsed: alice=%+v bob=%+v head=%s", aliceRef, bobRef, f.meta(t).Head)
			}

			headAfterCreates := f.meta(t).Head
			replayAlice := f.call(t, alice.Token, "bead.create", params, nil)
			success(t, replayAlice)
			if !bytes.Equal(firstAlice.result, replayAlice.result) || replayAlice.meta == nil || replayAlice.meta["replayed"] != true || f.meta(t).Head != headAfterCreates {
				t.Fatal("same-key retry changed the exact result or advanced SQL head")
			}

			claimParams := writeParams(t, map[string]any{"id": aliceRef})
			claim := f.call(t, alice.Token, "bead.claim", claimParams, nil)
			success(t, claim)
			got := success(t, f.call(t, bob.Token, "bead.get", map[string]any{"id": aliceRef}, nil))
			lease, ok := got["lease"].(map[string]any)
			if !ok || lease["holder"] != "user:alice" {
				t.Fatalf("core lease lost transport principal attribution: %+v", got)
			}

			beforeReopen := f.meta(t)
			f.reopen(t)
			replayAfterReopen := f.call(t, alice.Token, "bead.create", params, nil)
			success(t, replayAfterReopen)
			if !bytes.Equal(firstAlice.result, replayAfterReopen.result) || replayAfterReopen.meta == nil || replayAfterReopen.meta["replayed"] != true || f.meta(t).Head != beforeReopen.Head {
				t.Fatal("SQL reopen did not preserve the exact principal-scoped receipt")
			}

			// The same process-local key ring is intentionally retained across
			// reopen. Only the listener, store and lease were replaced; key
			// material is never claimed to be durable SQL state.
			historyParams := map[string]any{
				"asOf":  map[string]any{"revision": beforeReopen.Head, "incarnation": beforeReopen.Incarnation},
				"limit": 100,
			}
			history := f.call(t, alice.Token, "history.query", historyParams, nil)
			success(t, history)
			if len(history.result) == 0 {
				t.Fatal("history response was empty")
			}
			durable := durableBytes(t, f, []model.Ref{aliceRef, bobRef})
			credentialFree(t, firstAlice.raw, alice, bob)
			credentialFree(t, firstBob.raw, alice, bob)
			credentialFree(t, replayAfterReopen.raw, alice, bob)
			credentialFree(t, history.raw, alice, bob)
			credentialFree(t, durable, alice, bob)

			meta := f.meta(t)
			reader, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			changes, err := reader.Changesets(context.Background(), nil, 100)
			if err != nil || len(changes) != 3 {
				t.Fatalf("durable SQL changesets=%d err=%v", len(changes), err)
			}
			for i, want := range []model.PrincipalRef{{Kind: "user", ID: "user:alice"}, {Kind: "user", ID: "user:bob"}, {Kind: "user", ID: "user:alice"}} {
				if !reflect.DeepEqual(changes[i].Principal, want) {
					t.Fatalf("changeset %d principal=%+v want=%+v", i, changes[i].Principal, want)
				}
			}
			for _, tc := range []struct {
				ref   model.Ref
				count int
			}{{aliceRef, 2}, {bobRef, 1}} {
				versions, err := reader.Versions(context.Background(), tc.ref, driver.PageReq{Limit: 100})
				if err != nil || len(versions.Items) != tc.count {
					t.Fatalf("SQL versions for %s=%+v err=%v", tc.ref.ID, versions, err)
				}
				if versions.NextCursor != nil {
					t.Fatal("version page unexpectedly truncated")
				}
			}
			t.Logf("%s real HTTP API-key admission stamped independent Users, persisted three SQL changesets, and replayed the exact first result after close/reopen", b.name)
		})
	}
}

func TestAPIKeySQLAdmissionRefusalsDoNotMutate(t *testing.T) {
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) {
			f := newFixture(t, b)
			principal := model.PrincipalRef{Kind: "user", ID: "user:admission"}
			valid := f.issue(t, principal, f.now().Add(time.Hour))
			revoked := f.issue(t, principal, f.now().Add(time.Hour))
			if !f.keys.Revoke(revoked.ID) {
				t.Fatal("provider did not revoke test key")
			}
			unknownProvider := authnapikey.New(authnapikey.Config{Now: f.now})
			unknown, err := unknownProvider.Issue(principal, f.now().Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			expired := f.issue(t, principal, f.now().Add(time.Minute))
			f.clock.Store(expired.ExpiresAt.UnixNano())

			cases := []struct {
				name   string
				token  string
				status int
				code   string
				change func(*http.Request)
			}{
				{name: "missing", status: http.StatusUnauthorized, code: "unauthenticated"},
				{name: "malformed", token: "malformed", status: http.StatusUnauthorized, code: "unauthenticated"},
				{name: "unknown", token: unknown.Token, status: http.StatusUnauthorized, code: "unauthenticated"},
				{name: "wrong-secret", token: "plg_k_" + valid.ID + "_" + strings.Repeat("A", 43), status: http.StatusUnauthorized, code: "unauthenticated"},
				{name: "revoked", token: revoked.Token, status: http.StatusUnauthorized, code: "unauthenticated"},
				{name: "expired", token: expired.Token, status: http.StatusUnauthorized, code: "token_expired"},
				{name: "duplicate-credential", token: valid.Token, status: http.StatusUnauthorized, code: "unauthenticated", change: func(r *http.Request) { r.Header.Add("Authorization", "Bearer "+unknown.Token) }},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					before := f.meta(t)
					params := writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "must not land"}})
					got := f.call(t, tc.token, "bead.create", params, tc.change)
					requireCode(t, got, tc.status, tc.code)
					credentialFree(t, got.raw, valid, revoked, expired, unknown)
					after := f.meta(t)
					if before != after || !after.Head.IsZero() {
						t.Fatalf("refused credential mutated SQL state: before=%+v after=%+v", before, after)
					}
				})
			}

			// A valid key cannot smuggle an actor through the request. The wire
			// decoder refuses the field before the core service or Writer runs.
			spoof := writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "spoof"}})
			spoof["_meta"].(map[string]any)["principal"] = map[string]any{"kind": "user", "id": "user:spoofed"}
			before := f.meta(t)
			got := f.call(t, valid.Token, "bead.create", spoof, nil)
			requireCode(t, got, http.StatusOK, "invalid_params")
			if after := f.meta(t); after != before || !after.Head.IsZero() {
				t.Fatalf("supplied actor reached SQL core: before=%+v after=%+v", before, after)
			}
			credentialFree(t, got.raw, valid)
			t.Logf("%s real SQL HTTP admission rejected malformed, unknown, revoked, expired and spoofed credentials before core mutation", b.name)
		})
	}
}

func TestAPIKeySQLReopenKeepsReceiptButNotCredentialState(t *testing.T) {
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) {
			f := newFixture(t, b)
			principal := model.PrincipalRef{Kind: "user", ID: "user:process-local"}
			key := f.issue(t, principal, f.now().Add(time.Hour))
			params := fixedParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "key ring boundary"}})
			first := f.call(t, key.Token, "bead.create", params, nil)
			success(t, first)
			before := f.meta(t)
			f.reopen(t)
			if got := f.call(t, key.Token, "bead.create", params, nil); got.meta == nil || got.meta["replayed"] != true || !bytes.Equal(got.result, first.result) || f.meta(t).Head != before.Head {
				t.Fatal("retained process-local key could not resolve durable SQL receipt after reopen")
			}
			if fresh := authnapikey.New(authnapikey.Config{Now: f.now}); func() error { _, err := fresh.Authenticate(context.Background(), key.Token); return err }() == nil {
				t.Fatal("a fresh provider unexpectedly treated non-durable key material as persisted")
			}
			credentialFree(t, first.raw, key)
			credentialFree(t, durableBytes(t, f, []model.Ref{createdRef(t, success(t, first))}), key)
			t.Logf("%s kept the provider in process while SQL receipt/state survived resource reopen; a fresh provider had no key", b.name)
		})
	}
}

func TestAPIKeySQLResponseAndHistoryContainNoCredentialMaterial(t *testing.T) {
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) {
			f := newFixture(t, b)
			key := f.issue(t, model.PrincipalRef{Kind: "user", ID: "user:history"}, f.now().Add(time.Hour))
			params := writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "history credential boundary"}})
			created := f.call(t, key.Token, "bead.create", params, nil)
			ref := createdRef(t, success(t, created))
			at := f.meta(t)
			history := f.call(t, key.Token, "history.query", map[string]any{"asOf": map[string]any{"revision": at.Head, "incarnation": at.Incarnation}, "limit": 100}, nil)
			success(t, history)
			credentialFree(t, created.raw, key)
			credentialFree(t, history.raw, key)
			credentialFree(t, durableBytes(t, f, []model.Ref{ref}), key)
			t.Logf("%s changesets, versions and history remained credential-free", b.name)
		})
	}
}
