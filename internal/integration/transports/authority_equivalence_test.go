package transports_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/composition"
	"github.com/buildbetter-app/plugs/internal/server/transport/mcp"
	"github.com/buildbetter-app/plugs/internal/server/transport/websocket"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type authorityImage struct {
	Meta       driver.Meta
	Records    []driver.Record
	Versions   driver.Page[driver.Version]
	Changesets []model.Changeset
	Created    *driver.IdempotencyRecord
	Updated    *driver.IdempotencyRecord
	Native     driver.Checkpoint
}

func authoritySnapshot(t *testing.T, runtime *composition.Runtime, principal model.PrincipalRef, ref model.Ref, createKey, updateKey string) authorityImage {
	t.Helper()
	ctx := context.Background()
	var image authorityImage
	var err error
	image.Meta, err = runtime.Store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := runtime.Store.Read(ctx, driver.View{Incarnation: image.Meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	image.Records, err = r.Get(ctx, []model.Ref{ref})
	if err != nil || len(image.Records) != 1 {
		t.Fatalf("authority record unavailable: records=%d err=%v", len(image.Records), err)
	}
	image.Versions, err = r.Versions(ctx, ref, driver.PageReq{Limit: 10})
	if err != nil || image.Versions.NextCursor != nil {
		t.Fatalf("authority version census incomplete: %v", err)
	}
	image.Changesets, err = r.Changesets(ctx, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	principalKey, err := driver.PrincipalKey(principal)
	if err != nil {
		t.Fatal(err)
	}
	image.Created, err = r.IdempotencyByKey(ctx, principalKey, "", "bead.create", createKey)
	if err != nil {
		t.Fatal(err)
	}
	image.Updated, err = r.IdempotencyByKey(ctx, principalKey, "", "bead.update", updateKey)
	if err != nil {
		t.Fatal(err)
	}
	image.Native, err = runtime.Store.(driver.CheckpointStore).LoadCheckpoint(ctx, "plugin/transportprobe/default/durable")
	if err != nil {
		t.Fatal(err)
	}
	return image
}

func authorityRead(t *testing.T, f *fixture, adapter string, request []byte, title string, revision model.Revision) {
	t.Helper()
	var result struct {
		Bead struct {
			Fields   map[string]any `json:"fields"`
			Revision model.Revision `json:"revision"`
		} `json:"bead"`
	}
	if err := json.Unmarshal(success(t, f.call(t, adapter, request)), &result); err != nil || result.Bead.Fields["title"] != title || result.Bead.Revision != revision {
		t.Fatalf("%s did not read its admitted durable write: %v", adapter, err)
	}
}

func requireExpiredAuthority(t *testing.T, f *fixture, adapter string, canonical []byte) {
	t.Helper()
	if adapter == "stdio" {
		// Stdio receives a trusted host Identity before Serve. A fresh failed
		// admission never starts framing; retained streams have no invented
		// per-message credential recheck in this contract.
		identity, err := f.auth.Admit(context.Background(), f.token)
		var failure *model.Error
		if !errors.Is(err, authn.ErrTokenExpired) || !errors.As(err, &failure) || failure.Data.Code != "token_expired" || failure.Data.Retryable || !reflect.DeepEqual(identity, authn.Identity{}) {
			t.Fatal("expired stdio host admission did not return typed token_expired and a zero Identity")
		}
		return
	}

	var status int
	var header http.Header
	var body []byte
	if adapter == "websocket" {
		// Read the real failed upgrade body: the shared connect helper only
		// returns its status. No JSON-RPC frame is sent before admission.
		req, err := http.NewRequest(http.MethodGet, f.url+websocket.Path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Upgrade", "websocket")
		req.Header.Set("Connection", "Upgrade")
		req.Header.Set("Sec-WebSocket-Version", "13")
		req.Header.Set("Sec-WebSocket-Key", "MDEyMzQ1Njc4OWFiY2RlZg==")
		req.Header.Set("Origin", f.url)
		req.Header.Set("Authorization", "Bearer "+f.token)
		response, err := f.client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		status, header = response.StatusCode, response.Header
		if status != http.StatusUnauthorized {
			t.Fatalf("expired WebSocket handshake status=%d want=401", status)
		}
		body, err = io.ReadAll(io.LimitReader(response.Body, 1<<16))
		if err != nil {
			t.Fatal(err)
		}
	} else {
		path, payload := "/rpc/v1", canonical
		if adapter == "mcp" {
			path, payload = mcp.Path, mcpRequest(t, canonical)
		}
		// f.post retains the already admitted MCP session. Session possession
		// must not substitute for the now-expired bearer credential.
		status, header, body = f.post(t, path, payload, nil)
	}
	var reply envelope
	if json.Unmarshal(body, &reply) != nil || errorCode(t, reply) != "token_expired" || status != http.StatusUnauthorized || header.Get("WWW-Authenticate") != "Bearer" || string(reply.ID) != "null" || reply.Meta.Replayed {
		t.Fatalf("%s expiry did not return a pre-dispatch typed 401 refusal", adapter)
	}
	var failure struct {
		Code int `json:"code"`
		Data struct {
			Retryable bool `json:"retryable"`
		} `json:"data"`
	}
	if json.Unmarshal(reply.Error, &failure) != nil || failure.Code != -32002 || failure.Data.Retryable {
		t.Fatalf("%s expiry changed the closed token_expired error contract", adapter)
	}
	if bytes.Contains(body, []byte(f.token)) || bytes.Contains(body, []byte(f.keyID)) {
		t.Fatal("expiry response disclosed API-key material")
	}
}

// A5 specifies each credential channel; authnapikey's contract makes expiry
// half-open, and contracts/errors.md assigns token_expired -32002/nonretryable.
// This covers configured admission, not revalidation of a retained stdio stream.
func TestConfiguredAPIKeyExpiryAuthorityAcrossAdapters(t *testing.T) {
	for _, adapter := range transports {
		t.Run(adapter, func(t *testing.T) {
			start := time.Now()
			var clock atomic.Int64
			clock.Store(start.UnixNano())
			f, runtime, _ := configuredFixture(t, sqliteServerConfig(t), true, func() time.Time { return time.Unix(0, clock.Load()) })
			issued, err := f.keys.Issue(f.principal, start.Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			f.token, f.keyID = issued.Token, issued.ID
			// Only the existing API-key provider clock advances. SQLite, RPC,
			// leases, callback deadlines and production clocks are unchanged.
			clock.Store(issued.ExpiresAt.Add(-time.Nanosecond).UnixNano())
			const createKey = "550e8400-e29b-41d4-a716-446655440071"
			const updateKey = "550e8400-e29b-41d4-a716-446655440072"
			create := request(t, "live", "bead.create", map[string]any{
				"type": "issue", "fields": map[string]any{"title": "accepted before expiry"},
				"_meta": map[string]any{"idempotencyKey": createKey},
			})
			var created model.Commit
			first := f.call(t, adapter, create)
			if err := json.Unmarshal(success(t, first), &created); err != nil || len(created.Results) != 1 || created.Revision.String() != "1" || first.Meta.Replayed {
				t.Fatal("live API key did not commit exactly one configured write")
			}
			ref := created.Results[0].Ref
			get := request(t, "read", "bead.get", map[string]any{"id": ref})
			authorityRead(t, f, adapter, get, "accepted before expiry", created.Revision)
			acceptedCheckpoint(t, runtime, 1)
			before := authoritySnapshot(t, runtime, f.principal, ref, createKey, updateKey)
			if before.Created == nil || before.Created.ResponsePending || before.Updated != nil || len(before.Changesets) != 1 || len(before.Versions.Items) != 1 || !reflect.DeepEqual(before.Changesets[0].Principal, f.principal) {
				t.Fatal("positive control lacks its actual finalized receipt, version or admitted principal")
			}
			update := request(t, "expired", "bead.update", map[string]any{
				"id": ref, "fields": map[string]any{"title": "accepted by replacement key"},
				"_meta": map[string]any{"idempotencyKey": updateKey},
			})
			clock.Store(issued.ExpiresAt.UnixNano())
			requireExpiredAuthority(t, f, adapter, update)
			if adapter == "http" || adapter == "mcp" {
				// An existing receipt is also behind live credential admission.
				requireExpiredAuthority(t, f, adapter, create)
			}
			if after := authoritySnapshot(t, runtime, f.principal, ref, createKey, updateKey); !reflect.DeepEqual(before, after) || f.validators.Load() != 1 || f.posts.Load() != 1 || f.targetCalls.Load() != 0 {
				t.Fatal("expired credential changed SQLite records, head, history, receipts, native state or callback effects")
			}
			// The same principal and exact refused body/key succeed with a
			// distinct live credential; the request/session/runtime were valid.
			replacement, err := f.keys.Issue(f.principal, issued.ExpiresAt.Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			f.token, f.keyID = replacement.Token, replacement.ID
			var updated model.Commit
			accepted := f.call(t, adapter, update)
			if err := json.Unmarshal(success(t, accepted), &updated); err != nil || updated.Revision.String() != "2" || accepted.Meta.Replayed {
				t.Fatal("replacement key did not commit the exact refused request once")
			}
			authorityRead(t, f, adapter, get, "accepted by replacement key", updated.Revision)
			after := authoritySnapshot(t, runtime, f.principal, ref, createKey, updateKey)
			if len(after.Changesets) != 2 || len(after.Versions.Items) != 2 || after.Updated == nil || after.Updated.ResponsePending || after.Updated.Commit.Revision != updated.Revision || !reflect.DeepEqual(before.Created, after.Created) || f.validators.Load() != 2 || f.posts.Load() != 2 || f.targetCalls.Load() != 0 {
				t.Fatal("replacement-key control did not publish exactly one new finalized write and native callback pair")
			}
			acceptedCheckpoint(t, runtime, 2)
			t.Logf("adapter=%s configured SQLite: live at expiry-1ns; typed token_expired at expiry; durable refusal unchanged; exact request accepted once with replacement key", adapter)
		})
	}
}
