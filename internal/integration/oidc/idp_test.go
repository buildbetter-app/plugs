package oidc_test

import (
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

// This disposable TLS IdP performs actual authorization and token requests,
// checks PKCE and signs real RS256 ID tokens. Keys never leave process memory.
type mockIDP struct {
	t         *testing.T
	server    *httptest.Server
	key       *rsa.PrivateKey
	now       atomic.Int64
	mu        sync.Mutex
	codes     map[string]url.Values
	mutate    func(map[string]any)
	jwtHeader func(map[string]any)
	signKey   *rsa.PrivateKey
	keySet    func(map[string]any)
	tokenRaw  string
	redirect  string
	tokens    int
	keys      int
}

func newIDP(t *testing.T) *mockIDP {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal("could not generate disposable signing key")
	}
	idp := &mockIDP{t: t, key: key, codes: map[string]url.Values{}}
	idp.now.Store(time.Now().Unix())
	idp.server = httptest.NewTLSServer(http.HandlerFunc(idp.serve))
	t.Cleanup(idp.server.Close)
	return idp
}

func (idp *mockIDP) clock() time.Time { return time.Unix(idp.now.Load(), 0) }

func (idp *mockIDP) config() authnoidc.Config {
	return authnoidc.Config{
		Issuer: idp.server.URL, ClientID: "plug-client", RedirectURI: "https://plug.invalid/oidc/callback",
		AuthorizationEndpoint: idp.server.URL + "/authorize", TokenEndpoint: idp.server.URL + "/token",
		JWKSURL: idp.server.URL + "/jwks", Users: map[string]string{"oidc-subject": "user:nikhil"},
		HTTPClient: idp.server.Client(), Now: idp.clock,
	}
}

func (idp *mockIDP) serve(w http.ResponseWriter, r *http.Request) {
	idp.mu.Lock()
	defer idp.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/authorize":
		q := r.URL.Query()
		if r.Method != http.MethodGet || q.Get("response_type") != "code" || q.Get("client_id") != "plug-client" ||
			q.Get("redirect_uri") != "https://plug.invalid/oidc/callback" || q.Get("scope") != "openid" ||
			q.Get("code_challenge_method") != "S256" || len(q.Get("code_challenge")) != 43 ||
			len(q.Get("state")) != 43 || len(q.Get("nonce")) != 43 || q.Get("prompt") != "login" || q.Get("max_age") != "0" {
			http.Error(w, "invalid authorization request", http.StatusBadRequest)
			return
		}
		var secret [32]byte
		_, _ = rand.Read(secret[:])
		code := base64.RawURLEncoding.EncodeToString(secret[:])
		idp.codes[code] = q
		callback, _ := url.Parse(q.Get("redirect_uri"))
		callback.RawQuery = url.Values{"code": {code}, "state": {q.Get("state")}}.Encode()
		w.Header().Set("Location", callback.String())
		w.WriteHeader(http.StatusFound)
	case "/token":
		idp.tokens++
		if idp.redirect != "" {
			w.Header().Set("Location", idp.redirect)
			w.WriteHeader(http.StatusTemporaryRedirect)
			return
		}
		if r.Method != http.MethodPost || r.ParseForm() != nil {
			http.Error(w, "invalid token request", http.StatusBadRequest)
			return
		}
		q, exists := idp.codes[r.PostForm.Get("code")]
		delete(idp.codes, r.PostForm.Get("code"))
		challenge := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
		if !exists || base64.RawURLEncoding.EncodeToString(challenge[:]) != q.Get("code_challenge") ||
			r.PostForm.Get("grant_type") != "authorization_code" || r.PostForm.Get("client_id") != q.Get("client_id") ||
			r.PostForm.Get("redirect_uri") != q.Get("redirect_uri") || r.PostForm.Get("client_secret") != "" ||
			r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			http.Error(w, "invalid code or PKCE verifier", http.StatusBadRequest)
			return
		}
		if idp.tokenRaw != "" {
			_, _ = io.WriteString(w, idp.tokenRaw)
			return
		}
		now := idp.clock().Unix()
		claims := map[string]any{"iss": idp.server.URL, "sub": "oidc-subject", "aud": "plug-client",
			"iat": now, "exp": now + 3600, "auth_time": now, "nonce": q.Get("nonce")}
		if idp.mutate != nil {
			idp.mutate(claims)
		}
		header := map[string]any{"alg": "RS256", "kid": "local-signing-key", "typ": "JWT"}
		if idp.jwtHeader != nil {
			idp.jwtHeader(header)
		}
		key := idp.signKey
		if key == nil {
			key = idp.key
		}
		token := signedJWT(idp.t, key, header, claims)
		_ = json.NewEncoder(w).Encode(map[string]any{"id_token": token, "access_token": "unused", "token_type": "Bearer"})
	case "/jwks":
		idp.keys++
		key := map[string]any{"kty": "RSA", "kid": "local-signing-key", "alg": "RS256", "use": "sig",
			"n": base64.RawURLEncoding.EncodeToString(idp.key.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(idp.key.E)).Bytes())}
		if idp.keySet != nil {
			idp.keySet(key)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{key}})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func signedJWT(t *testing.T, key *rsa.PrivateKey, header, claims map[string]any) string {
	t.Helper()
	h, err := json.Marshal(header)
	if err != nil {
		t.Fatal("could not encode disposable token header")
	}
	c, err := json.Marshal(claims)
	if err != nil {
		t.Fatal("could not encode disposable token claims")
	}
	payload := base64.RawURLEncoding.EncodeToString(h) + "." + base64.RawURLEncoding.EncodeToString(c)
	digest := sha256.Sum256([]byte(payload))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal("could not sign disposable token")
	}
	return payload + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func (idp *mockIDP) authorize(login authnoidc.Login) (state, code string) {
	idp.t.Helper()
	client := *idp.server.Client()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Get(login.AuthorizationURL)
	if err != nil {
		idp.t.Fatal("authorization request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusFound {
		idp.t.Fatal("mock IdP refused actual authorization request")
	}
	callback, err := url.Parse(response.Header.Get("Location"))
	if err != nil {
		idp.t.Fatal("invalid mock callback")
	}
	return callback.Query().Get("state"), callback.Query().Get("code")
}
