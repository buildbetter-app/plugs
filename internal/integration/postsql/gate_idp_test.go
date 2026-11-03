package postsql_test

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/plugins/authnoidc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// The IdP is only the disposable authentication peer. The production OIDC
// verifier, core-session source, HTTP admission, gate evaluator and Writer all
// execute. Nothing supplies a callback-only gate decision or session proof.
type approvalIDP struct {
	server *httptest.Server
	now    atomic.Int64
}

func newApprovalIDP(t *testing.T) *approvalIDP {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal("generate disposable IdP key")
	}
	idp := &approvalIDP{}
	idp.now.Store(time.Now().Unix())
	var mu sync.Mutex
	codes := map[string]url.Values{}
	idp.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/authorize":
			q := r.URL.Query()
			if r.Method != http.MethodGet || q.Get("client_id") != "plug-approval" || q.Get("response_type") != "code" ||
				q.Get("redirect_uri") != "https://plug.invalid/approval/callback" || q.Get("scope") != "openid" ||
				q.Get("code_challenge_method") != "S256" || len(q.Get("code_challenge")) != 43 || len(q.Get("nonce")) != 43 ||
				len(q.Get("state")) != 43 || q.Get("prompt") != "login" || q.Get("max_age") != "0" {
				http.Error(w, "invalid authorization request", http.StatusBadRequest)
				return
			}
			code := nonce()
			codes[code] = q
			callback, _ := url.Parse(q.Get("redirect_uri"))
			callback.RawQuery = url.Values{"state": {q.Get("state")}, "code": {code}}.Encode()
			w.Header().Set("Location", callback.String())
			w.WriteHeader(http.StatusFound)
		case "/token":
			if r.Method != http.MethodPost || r.ParseForm() != nil {
				http.Error(w, "invalid token request", http.StatusBadRequest)
				return
			}
			q, exists := codes[r.PostForm.Get("code")]
			delete(codes, r.PostForm.Get("code"))
			challenge := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
			if !exists || base64.RawURLEncoding.EncodeToString(challenge[:]) != q.Get("code_challenge") ||
				r.PostForm.Get("client_id") != q.Get("client_id") || r.PostForm.Get("redirect_uri") != q.Get("redirect_uri") ||
				r.PostForm.Get("grant_type") != "authorization_code" || r.PostForm.Get("client_secret") != "" || r.Header.Get("Authorization") != "" {
				http.Error(w, "invalid code or PKCE", http.StatusBadRequest)
				return
			}
			now := idp.clock().Unix()
			header := encoded(t, map[string]any{"alg": "RS256", "kid": "approval", "typ": "JWT"})
			claims := encoded(t, map[string]any{"iss": idp.server.URL, "sub": "bob", "aud": "plug-approval",
				"iat": now, "exp": now + 3600, "auth_time": now, "nonce": q.Get("nonce")})
			payload := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(claims)
			digest := sha256.Sum256([]byte(payload))
			signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
			if err != nil {
				http.Error(w, "signing failed", http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"id_token": payload + "." + base64.RawURLEncoding.EncodeToString(signature)})
		case "/jwks":
			_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]any{
				"kty": "RSA", "kid": "approval", "alg": "RS256", "use": "sig",
				"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
			}}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(idp.server.Close)
	return idp
}

func (idp *approvalIDP) clock() time.Time { return time.Unix(idp.now.Load(), 0) }

func (idp *approvalIDP) provider(t *testing.T, store driver.Store) *authnoidc.Provider {
	t.Helper()
	config := authnoidc.Config{Issuer: idp.server.URL, ClientID: "plug-approval", RedirectURI: "https://plug.invalid/approval/callback",
		AuthorizationEndpoint: idp.server.URL + "/authorize", TokenEndpoint: idp.server.URL + "/token", JWKSURL: idp.server.URL + "/jwks",
		Users: map[string]string{"bob": "user:bob"}, HTTPClient: idp.server.Client(), Now: idp.clock, SessionTTL: time.Minute}
	if sessionStore, ok := store.(driver.CoreSessionStore); ok {
		config.SessionStore = sessionStore
	}
	provider, err := authnoidc.New(config)
	if err != nil {
		t.Fatal(err)
	}
	return provider
}

func (idp *approvalIDP) login(t *testing.T, provider *authnoidc.Provider) authnoidc.IssuedSession {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	login, err := provider.Begin(ctx)
	if err != nil {
		t.Fatal("begin interactive login")
	}
	client := *idp.server.Client()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Get(login.AuthorizationURL)
	if err != nil {
		t.Fatal("interactive authorization request")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusFound {
		t.Fatal("IdP refused interactive authorization")
	}
	callback, err := url.Parse(response.Header.Get("Location"))
	if err != nil {
		t.Fatal("invalid IdP callback")
	}
	session, err := provider.Complete(ctx, callback.Query().Get("state"), login.Binding, callback.Query().Get("code"))
	if err != nil {
		t.Fatal("complete verified interactive login")
	}
	return session
}
