package s1b05b08

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/plugins/authnoidc"
)

// Source reuse from B03 author6ce spec/b03-native/oidc_test.go.
// Retains PR368-era OIDC integration contributor code; the disposable peer
// supplies signed tokens, never Plug authority or protected rows.
// b068IDP is a disposable HTTPS identity provider. It validates the actual
// authorization-code and PKCE requests, serves the JWKS used by the Plug
// provider, and signs the returned ID token with an in-memory RSA key.
type b068IDP struct {
	t      *testing.T
	server *httptest.Server
	key    *rsa.PrivateKey
	now    atomic.Int64

	mu    sync.Mutex
	codes map[string]url.Values
	token int
	keys  int
}

func newB068IDP(t *testing.T) *b068IDP {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal("could not create the disposable IdP signing key")
	}
	idp := &b068IDP{t: t, key: key, codes: make(map[string]url.Values)}
	idp.now.Store(time.Now().Unix())
	idp.server = httptest.NewTLSServer(http.HandlerFunc(idp.serve))
	t.Cleanup(idp.server.Close)
	return idp
}

func (idp *b068IDP) clock() time.Time { return time.Unix(idp.now.Load(), 0) }

func (idp *b068IDP) config() authnoidc.Config {
	return authnoidc.Config{
		Issuer: idp.server.URL, ClientID: "plug-client",
		RedirectURI:           "https://plug.invalid/oidc/callback",
		AuthorizationEndpoint: idp.server.URL + "/authorize",
		TokenEndpoint:         idp.server.URL + "/token",
		JWKSURL:               idp.server.URL + "/jwks",
		Users:                 map[string]string{"oidc-subject": "user:operator"},
		HTTPClient:            idp.server.Client(),
		Now:                   idp.clock,
	}
}

func (idp *b068IDP) serve(w http.ResponseWriter, r *http.Request) {
	idp.mu.Lock()
	defer idp.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/authorize":
		idp.authorize(w, r)
	case "/token":
		idp.tokenExchange(w, r)
	case "/jwks":
		idp.jwks(w, r)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (idp *b068IDP) authorize(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if r.Method != http.MethodGet || q.Get("response_type") != "code" ||
		q.Get("client_id") != "plug-client" || q.Get("redirect_uri") != "https://plug.invalid/oidc/callback" ||
		q.Get("scope") != "openid" || q.Get("code_challenge_method") != "S256" ||
		len(q.Get("code_challenge")) != 43 || len(q.Get("state")) != 43 || len(q.Get("nonce")) != 43 ||
		q.Get("prompt") != "login" || q.Get("max_age") != "0" {
		http.Error(w, "invalid authorization request", http.StatusBadRequest)
		return
	}
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		http.Error(w, "identity provider unavailable", http.StatusServiceUnavailable)
		return
	}
	code := base64.RawURLEncoding.EncodeToString(raw[:])
	idp.codes[code] = q
	callback, _ := url.Parse(q.Get("redirect_uri"))
	callback.RawQuery = url.Values{"code": {code}, "state": {q.Get("state")}}.Encode()
	w.Header().Set("Location", callback.String())
	w.WriteHeader(http.StatusFound)
}

func (idp *b068IDP) tokenExchange(w http.ResponseWriter, r *http.Request) {
	idp.token++
	if r.Method != http.MethodPost || r.ParseForm() != nil {
		http.Error(w, "invalid token request", http.StatusBadRequest)
		return
	}
	code := r.PostForm.Get("code")
	q, exists := idp.codes[code]
	delete(idp.codes, code)
	challenge := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
	if !exists || base64.RawURLEncoding.EncodeToString(challenge[:]) != q.Get("code_challenge") ||
		r.PostForm.Get("grant_type") != "authorization_code" || r.PostForm.Get("client_id") != q.Get("client_id") ||
		r.PostForm.Get("redirect_uri") != q.Get("redirect_uri") || r.PostForm.Get("client_secret") != "" ||
		r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
		http.Error(w, "invalid code or PKCE verifier", http.StatusBadRequest)
		return
	}
	now := idp.clock().Unix()
	claims := map[string]any{
		"iss": idp.server.URL, "sub": "oidc-subject", "aud": "plug-client",
		"iat": now, "exp": now + 3600, "auth_time": now, "nonce": q.Get("nonce"),
	}
	header := map[string]any{"alg": "RS256", "kid": "local-signing-key", "typ": "JWT"}
	token := b068SignedJWT(idp.t, idp.key, header, claims)
	_ = json.NewEncoder(w).Encode(map[string]any{"id_token": token, "access_token": "unused", "token_type": "Bearer"})
}

func (idp *b068IDP) jwks(w http.ResponseWriter, _ *http.Request) {
	idp.keys++
	key := map[string]any{
		"kty": "RSA", "kid": "local-signing-key", "alg": "RS256", "use": "sig",
		"n": base64.RawURLEncoding.EncodeToString(idp.key.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(idp.key.E)).Bytes()),
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{key}})
}

func b068SignedJWT(t *testing.T, key *rsa.PrivateKey, header, claims map[string]any) string {
	t.Helper()
	h, err := json.Marshal(header)
	if err != nil {
		t.Fatal("could not encode disposable IdP header")
	}
	c, err := json.Marshal(claims)
	if err != nil {
		t.Fatal("could not encode disposable IdP claims")
	}
	payload := base64.RawURLEncoding.EncodeToString(h) + "." + base64.RawURLEncoding.EncodeToString(c)
	digest := sha256.Sum256([]byte(payload))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal("could not sign disposable IdP token")
	}
	return payload + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func (idp *b068IDP) authorizeLogin(login authnoidc.Login) (state, code string) {
	idp.t.Helper()
	client := *idp.server.Client()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Get(login.AuthorizationURL)
	if err != nil {
		idp.t.Fatal("authorization request failed")
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	if response.StatusCode != http.StatusFound {
		idp.t.Fatal("disposable IdP refused the authorization request")
	}
	callback, err := url.Parse(response.Header.Get("Location"))
	if err != nil {
		idp.t.Fatal("disposable IdP returned an invalid callback")
	}
	return callback.Query().Get("state"), callback.Query().Get("code")
}

func b068Login(t *testing.T, idp *b068IDP, provider *authnoidc.Provider) authnoidc.IssuedSession {
	t.Helper()
	flow, err := provider.Begin(context.Background())
	if err != nil {
		t.Fatal("OIDC flow did not begin")
	}
	state, code := idp.authorizeLogin(flow)
	session, err := provider.Complete(context.Background(), state, flow.Binding, code)
	if err != nil {
		t.Fatalf("verified OIDC flow failed: %v", err)
	}
	return session
}
