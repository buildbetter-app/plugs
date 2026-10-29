package authnapikey

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/authn"
)

// Requests go through net/http's handler boundary in process. All credentials
// are actually issued and verified; no network dial or external probe occurs.
func TestAuthenticAPIKeyHTTPRequests(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	p := New(Config{Now: func() time.Time { return now }})
	issued, err := p.Issue(model.PrincipalRef{Kind: "user", ID: "user:admin"}, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	a, err := authn.New(authn.Config{Provider: p})
	if err != nil {
		t.Fatal(err)
	}
	var admitted int
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, err := a.AdmitHTTP(r)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path == "/protected" && identity.RequireInteractiveUser() != nil {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		admitted++
		_, _ = io.WriteString(w, identity.Principal().ID)
	})
	request := func(path, token string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(http.MethodPost, "http://localhost:8888"+path, nil)
		r.RemoteAddr = "127.0.0.1:41000"
		r.Header.Set("Plug-RPC", "1")
		r.Header.Set("Origin", "http://localhost:8888")
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	response := request("/rpc", issued.Token)
	if response.Code != http.StatusOK || response.Body.String() != "user:admin" || admitted != 1 {
		t.Fatal("authentic API-key request did not reach handler with its stored identity")
	}
	response = request("/protected", issued.Token)
	if response.Code != http.StatusForbidden || admitted != 1 {
		t.Fatal("User API key provided interactive human proof for protected operation")
	}
	for _, token := range []string{"", "unsupported"} {
		if request("/rpc", token).Code != http.StatusUnauthorized || admitted != 1 {
			t.Fatal("failed authentication reached handler or became anonymous")
		}
	}
	p.Revoke(issued.ID)
	if request("/rpc", issued.Token).Code != http.StatusUnauthorized || admitted != 1 {
		t.Fatal("revoked API key reached handler")
	}
	issued, err = p.Issue(model.PrincipalRef{Kind: "service", ID: "service:ci"}, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	now = issued.ExpiresAt
	if request("/rpc", issued.Token).Code != http.StatusUnauthorized || admitted != 1 {
		t.Fatal("expired API key reached handler")
	}
}

func TestAPIKeyDoesNotWaiveLoopbackBrowserDefenses(t *testing.T) {
	p := New(Config{})
	issued, err := p.Issue(model.PrincipalRef{Kind: "service", ID: "service:ci"}, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	a, err := authn.New(authn.Config{Provider: p})
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*http.Request){
		func(r *http.Request) { r.Host = "example.invalid:8888" },
		func(r *http.Request) { r.Header.Set("Origin", "http://example.invalid") },
		func(r *http.Request) { r.Header.Del("Plug-RPC") },
		func(r *http.Request) { r.RemoteAddr = "192.0.2.1:41000" },
	} {
		r := httptest.NewRequest(http.MethodPost, "http://localhost:8888/rpc", nil)
		r.RemoteAddr = "127.0.0.1:41000"
		r.Header.Set("Plug-RPC", "1")
		r.Header.Set("Authorization", "Bearer "+issued.Token)
		change(r)
		identity, err := a.AdmitHTTP(r)
		if !errors.Is(err, authn.ErrForbidden) || identity.Principal().ID != "" {
			t.Fatal("valid API key bypassed loopback browser/peer admission")
		}
	}
	listener, err := a.Listen(context.Background(), "0.0.0.0:0", nil)
	if listener != nil {
		listener.Close()
		t.Fatal("API-key network listener was created without TLS")
	}
	if !errors.Is(err, authn.ErrForbidden) {
		t.Fatal("API-key network listener did not require TLS before binding")
	}
}
