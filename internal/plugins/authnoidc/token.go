package authnoidc

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/big"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/server/authn"
)

const maxDocument = 64 << 10

func (p *Provider) exchange(ctx context.Context, code, verifier string) (string, error) {
	form := url.Values{"grant_type": {"authorization_code"}, "client_id": {p.config.ClientID},
		"redirect_uri": {p.config.RedirectURI}, "code": {code}, "code_verifier": {verifier}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.config.TokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", authn.ErrUnavailable
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	data, err := p.request(req)
	if err != nil {
		return "", err
	}
	var response struct {
		IDToken string `json:"id_token"`
	}
	if decodeDocument(data, &response) != nil || response.IDToken == "" || len(response.IDToken) > 16<<10 {
		return "", authn.ErrUnauthenticated
	}
	return response.IDToken, nil
}

func (p *Provider) request(req *http.Request) ([]byte, error) {
	req.Header.Set("Accept", "application/json")
	response, err := p.client.Do(req)
	if err != nil {
		return nil, authn.ErrUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode >= 500 || response.StatusCode == http.StatusTooManyRequests {
		return nil, authn.ErrUnavailable
	}
	if response.StatusCode != http.StatusOK {
		return nil, authn.ErrUnauthenticated
	}
	typ, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || typ != "application/json" {
		return nil, authn.ErrUnauthenticated
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxDocument+1))
	if err != nil {
		return nil, authn.ErrUnavailable
	}
	if len(data) > maxDocument {
		return nil, authn.ErrUnauthenticated
	}
	return data, nil
}

func decodeDocument(data []byte, target any) error {
	// The shared decoder rejects duplicate members and trailing values before
	// struct decoding. Unknown OIDC extension claims have no authority here.
	if _, err := wire.DecodeJSON(data); err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

type tokenHeader struct {
	Algorithm string          `json:"alg"`
	KeyID     string          `json:"kid"`
	Type      string          `json:"typ"`
	Critical  json.RawMessage `json:"crit"`
	JWK       json.RawMessage `json:"jwk"`
	JWKURL    json.RawMessage `json:"jku"`
	X509URL   json.RawMessage `json:"x5u"`
}

type claims struct {
	Issuer    string          `json:"iss"`
	Subject   string          `json:"sub"`
	Audience  json.RawMessage `json:"aud"`
	Party     string          `json:"azp"`
	Nonce     string          `json:"nonce"`
	Issued    json.Number     `json:"iat"`
	Expires   json.Number     `json:"exp"`
	NotBefore json.Number     `json:"nbf"`
	AuthTime  json.Number     `json:"auth_time"`
}

func (p *Provider) verifyIDToken(ctx context.Context, raw string, f flow) (string, time.Time, error) {
	parts := strings.Split(raw, ".")
	if len(parts) != 3 || len(raw) > 16<<10 {
		return "", time.Time{}, authn.ErrUnauthenticated
	}
	headerBytes, err := base64Part(parts[0])
	if err != nil {
		return "", time.Time{}, authn.ErrUnauthenticated
	}
	var header tokenHeader
	if decodeDocument(headerBytes, &header) != nil || header.Algorithm != "RS256" ||
		!boundedText(header.KeyID, 128) || (header.Type != "" && header.Type != "JWT") ||
		len(header.Critical) != 0 || len(header.JWK) != 0 || len(header.JWKURL) != 0 || len(header.X509URL) != 0 {
		return "", time.Time{}, authn.ErrUnauthenticated
	}
	signature, err := base64Part(parts[2])
	if err != nil {
		return "", time.Time{}, authn.ErrUnauthenticated
	}
	key, err := p.publicKey(ctx, header.KeyID)
	if err != nil {
		return "", time.Time{}, err
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature) != nil {
		return "", time.Time{}, authn.ErrUnauthenticated
	}
	payload, err := base64Part(parts[1])
	if err != nil {
		return "", time.Time{}, authn.ErrUnauthenticated
	}
	var c claims
	if decodeDocument(payload, &c) != nil || c.Issuer != p.config.Issuer || !boundedText(c.Subject, 255) ||
		!validAudience(c.Audience, c.Party, p.config.ClientID) ||
		subtle.ConstantTimeCompare([]byte(c.Nonce), []byte(f.nonce)) != 1 {
		return "", time.Time{}, authn.ErrUnauthenticated
	}
	decoded, _ := wire.DecodeJSON(payload)
	object, _ := decoded.(map[string]any)
	for _, name := range []string{"exp", "iat", "auth_time", "nbf"} {
		value, present := object[name]
		if _, numeric := value.(json.Number); (present && !numeric) || (!present && name != "nbf") {
			return "", time.Time{}, authn.ErrUnauthenticated
		}
	}
	now := p.now()
	exp, expErr := c.Expires.Int64()
	iat, iatErr := c.Issued.Int64()
	authTime, authErr := c.AuthTime.Int64()
	if expErr != nil || iatErr != nil || authErr != nil || iat <= 0 || exp <= iat ||
		iat > now.Unix() || iat < now.Add(-5*time.Minute).Unix() ||
		authTime <= 0 || authTime > iat || authTime < f.startedAt.Add(-time.Minute).Unix() {
		return "", time.Time{}, authn.ErrUnauthenticated
	}
	if c.NotBefore != "" {
		nbf, err := c.NotBefore.Int64()
		if err != nil || nbf > now.Unix() || nbf > exp {
			return "", time.Time{}, authn.ErrUnauthenticated
		}
	}
	expires := time.Unix(exp, 0)
	if !now.Before(expires) {
		return "", time.Time{}, authn.ErrTokenExpired
	}
	user, known := p.config.Users[c.Subject]
	if !known {
		return "", time.Time{}, authn.ErrUnauthenticated
	}
	return user, expires, nil
}

func validAudience(raw json.RawMessage, party, client string) bool {
	var audiences []string
	var single string
	if json.Unmarshal(raw, &single) == nil && single != "" {
		audiences = []string{single}
	} else if json.Unmarshal(raw, &audiences) != nil {
		return false
	}
	if len(audiences) == 0 || len(audiences) > 16 || (party != "" && party != client) || (len(audiences) > 1 && party != client) {
		return false
	}
	seen, matched := map[string]bool{}, false
	for _, aud := range audiences {
		if !boundedText(aud, 256) || seen[aud] {
			return false
		}
		seen[aud] = true
		matched = matched || aud == client
	}
	return matched
}

func base64Part(raw string) ([]byte, error) {
	data, err := base64.RawURLEncoding.Strict().DecodeString(raw)
	if err != nil || base64.RawURLEncoding.EncodeToString(data) != raw {
		return nil, authn.ErrUnauthenticated
	}
	return data, nil
}

type jwk struct {
	Type       string   `json:"kty"`
	KeyID      string   `json:"kid"`
	Algorithm  string   `json:"alg"`
	Use        string   `json:"use"`
	Operations []string `json:"key_ops"`
	Modulus    string   `json:"n"`
	Exponent   string   `json:"e"`
}

func (p *Provider) publicKey(ctx context.Context, id string) (*rsa.PublicKey, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.config.JWKSURL, nil)
	if err != nil {
		return nil, authn.ErrUnavailable
	}
	data, err := p.request(req)
	if err != nil {
		return nil, err
	}
	var set struct {
		Keys []jwk `json:"keys"`
	}
	if decodeDocument(data, &set) != nil || len(set.Keys) == 0 || len(set.Keys) > 32 {
		return nil, authn.ErrUnauthenticated
	}
	var selected *jwk
	for i := range set.Keys {
		if set.Keys[i].KeyID == id {
			if selected != nil {
				return nil, authn.ErrUnauthenticated
			}
			selected = &set.Keys[i]
		}
	}
	if selected == nil || selected.Type != "RSA" ||
		(selected.Algorithm != "" && selected.Algorithm != "RS256") || (selected.Use != "" && selected.Use != "sig") ||
		(len(selected.Operations) != 0 && (len(selected.Operations) != 1 || selected.Operations[0] != "verify")) {
		return nil, authn.ErrUnauthenticated
	}
	n, nErr := base64Part(selected.Modulus)
	e, eErr := base64Part(selected.Exponent)
	if nErr != nil || eErr != nil || len(n) < 256 || len(n) > 512 || n[0] == 0 ||
		len(e) == 0 || len(e) > 4 || e[0] == 0 {
		return nil, authn.ErrUnauthenticated
	}
	modulus, exponent := new(big.Int).SetBytes(n), new(big.Int).SetBytes(e).Int64()
	if modulus.BitLen() < 2048 || modulus.BitLen() > 4096 || modulus.Bit(0) == 0 || exponent < 3 ||
		exponent > 1<<31-1 || exponent%2 == 0 {
		return nil, authn.ErrUnauthenticated
	}
	return &rsa.PublicKey{N: modulus, E: int(exponent)}, nil
}
