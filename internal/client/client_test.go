package client

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/buildbetter-app/plugs/internal/api/wire"
)

type roundTripFunc func(context.Context, []byte) ([]byte, error)

func (f roundTripFunc) RoundTrip(ctx context.Context, b []byte) ([]byte, error) { return f(ctx, b) }

func TestLostResponseKeepsRandomKeyForExplicitRetry(t *testing.T) {
	var keys []string
	lost := errors.New("lost response")
	c, err := New(roundTripFunc(func(_ context.Context, b []byte) ([]byte, error) {
		var req struct {
			Params struct {
				Meta wire.Meta `json:"_meta"`
			} `json:"params"`
		}
		if err := json.Unmarshal(b, &req); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, req.Params.Meta.IdempotencyKey)
		return nil, lost
	}), "local", "test")
	if err != nil {
		t.Fatal(err)
	}
	params := map[string]any{"type": "issue", "fields": map[string]any{"title": "work"}}
	a, err := c.Call(context.Background(), "bead.create", params, "")
	if !errors.Is(err, lost) {
		t.Fatal(err)
	}
	random, err := hex.DecodeString(a.IdempotencyKey)
	if err != nil || len(random) < 16 {
		t.Fatalf("key %q: %v", a.IdempotencyKey, err)
	}
	_, _ = c.Call(context.Background(), "bead.create", params, a.IdempotencyKey)
	b, _ := c.Call(context.Background(), "bead.create", params, "")
	if len(keys) != 3 || keys[0] != keys[1] || keys[0] == keys[2] || b.IdempotencyKey == a.IdempotencyKey {
		t.Fatalf("keys %+v", keys)
	}
	if _, ok := params["_meta"]; ok {
		t.Fatal("caller parameters mutated")
	}
}

func TestRemoteTypedErrorAndResponseCorrelation(t *testing.T) {
	for _, mode := range []string{"error", "wrong-id", "result-and-error", "duplicate", "warning"} {
		t.Run(mode, func(t *testing.T) {
			c, err := New(roundTripFunc(func(_ context.Context, b []byte) ([]byte, error) {
				var req struct {
					ID string `json:"id"`
				}
				_ = json.Unmarshal(b, &req)
				cat, _ := wire.DefaultCatalog()
				e := cat.NewError("not_found", "missing bead", nil)
				switch mode {
				case "error":
					return json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": e})
				case "wrong-id":
					return []byte(`{"jsonrpc":"2.0","id":"other","result":{}}`), nil
				case "result-and-error":
					return json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": nil, "error": e})
				case "duplicate":
					return []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%q,"result":{},"result":{}}`, req.ID)), nil
				default:
					return json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": map[string]any{"revision": 1, "results": []any{}, "warnings": []any{map[string]any{"code": "post_subscriber_failed", "message": "visible"}}}})
				}
			}), "local", "test")
			if err != nil {
				t.Fatal(err)
			}
			if mode == "warning" {
				r, err := c.Call(context.Background(), "bead.comment", map[string]any{"id": map[string]any{"id": "pl-1"}, "body": "text"}, "")
				if err != nil {
					t.Fatal(err)
				}
				var out struct {
					Warnings []any `json:"warnings"`
				}
				_ = json.Unmarshal(r.JSON, &out)
				if len(out.Warnings) != 1 {
					t.Fatal("warning lost")
				}
				return
			}
			_, err = c.Call(context.Background(), "bead.get", map[string]any{"id": map[string]any{"id": "missing"}}, "")
			if err == nil {
				t.Fatal("bad response accepted")
			}
			if mode == "error" {
				var remote *wire.Error
				if !errors.As(err, &remote) || remote.Data.Code != "not_found" {
					t.Fatalf("typed error lost: %T %v", err, err)
				}
			}
		})
	}
}
