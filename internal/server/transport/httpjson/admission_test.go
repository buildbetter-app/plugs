package httpjson_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
)

func TestNetworkAdmissionAndFramingNeverReachDomainOrStore(t *testing.T) {
	for _, authenticated := range []bool{false, true} {
		name := "anonymous"
		if authenticated {
			name = "API-key"
		}
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, authenticated, false, httpjson.Config{})
			data := message(t, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "must not be written"}}), json.RawMessage(`1`))
			tests := []struct {
				name   string
				change func(*http.Request)
				status int
				code   string
			}{
				{"missing Plug-RPC", func(r *http.Request) { r.Header.Del("Plug-RPC") }, 403, "forbidden"},
				{"duplicate Plug-RPC", func(r *http.Request) { r.Header.Add("Plug-RPC", "1") }, 403, "forbidden"},
				{"host", func(r *http.Request) { r.Host = "example.invalid" }, 403, "forbidden"},
				{"cross Origin", func(r *http.Request) { r.Header.Set("Origin", "http://example.invalid") }, 403, "forbidden"},
				{"null Origin", func(r *http.Request) { r.Header.Set("Origin", "null") }, 403, "forbidden"},
				{"wrong Origin scheme", func(r *http.Request) { r.Header.Set("Origin", "https://"+r.Host) }, 403, "forbidden"},
				{"duplicate Origin", func(r *http.Request) { r.Header.Add("Origin", f.url); r.Header.Add("Origin", f.url) }, 403, "forbidden"},
				{"forwarded host cannot override", func(r *http.Request) {
					r.Host = "example.invalid"
					r.Header.Set("X-Forwarded-Host", "127.0.0.1")
					r.Header.Set("X-Forwarded-Proto", "https")
				}, 403, "forbidden"},
				{"wrong credential", func(r *http.Request) { r.Header.Set("Authorization", "Bearer invalid") }, 401, "unauthenticated"},
				{"duplicate Authorization", func(r *http.Request) {
					r.Header.Add("Authorization", "Bearer invalid")
					r.Header.Add("Authorization", "Bearer invalid")
				}, 401, "unauthenticated"},
				{"cookie", func(r *http.Request) { r.Header.Set("Cookie", "session=unsupported") }, 403, "forbidden"},
				{"compression", func(r *http.Request) { r.Header.Set("Content-Encoding", "gzip") }, 415, "invalid_request"},
				{"identity encoding", func(r *http.Request) { r.Header.Set("Content-Encoding", "identity") }, 415, "invalid_request"},
				{"missing content type", func(r *http.Request) { r.Header.Del("Content-Type") }, 415, "invalid_request"},
				{"plain text", func(r *http.Request) { r.Header.Set("Content-Type", "text/plain") }, 415, "invalid_request"},
				{"non UTF8", func(r *http.Request) { r.Header.Set("Content-Type", "application/json; charset=utf-16") }, 415, "invalid_request"},
				{"unknown media parameter", func(r *http.Request) { r.Header.Set("Content-Type", "application/json; foo=bar") }, 415, "invalid_request"},
				{"duplicate content type", func(r *http.Request) { r.Header.Add("Content-Type", "application/json") }, 415, "invalid_request"},
				{"trailing slash", func(r *http.Request) { r.URL.Path += "/" }, 404, "not_found"},
				{"WebSocket path", func(r *http.Request) { r.URL.Path += "/ws" }, 404, "not_found"},
				{"MCP path", func(r *http.Request) { r.URL.Path = "/mcp" }, 404, "not_found"},
				{"query", func(r *http.Request) { r.URL.RawQuery = "token=unsupported" }, 404, "not_found"},
				{"empty query", func(r *http.Request) { r.URL.ForceQuery = true }, 404, "not_found"},
				{"encoded route", func(r *http.Request) { r.URL.RawPath = "/rpc/%761" }, 404, "not_found"},
				{"GET", func(r *http.Request) { r.Method = http.MethodGet }, 405, "invalid_request"},
				{"OPTIONS", func(r *http.Request) {
					r.Method = http.MethodOptions
					r.Header.Set("Origin", "http://example.invalid")
					r.Header.Set("Access-Control-Request-Method", "POST")
				}, 405, "invalid_request"},
				{"Upgrade without Plug-RPC", func(r *http.Request) {
					r.Header.Del("Plug-RPC")
					r.Header.Set("Upgrade", "websocket")
					r.Header.Set("Connection", "Upgrade")
				}, 403, "forbidden"},
			}
			if authenticated {
				tests = append(tests, struct {
					name   string
					change func(*http.Request)
					status int
					code   string
				}{"missing credential", func(r *http.Request) { r.Header.Del("Authorization") }, 401, "unauthenticated"})
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					r := f.request(t, data)
					test.change(r)
					response := f.send(t, r)
					requireCode(t, response, test.status, test.code)
					var envelope map[string]json.RawMessage
					if err := json.Unmarshal(response.body, &envelope); err != nil {
						t.Fatal(err)
					}
					if string(envelope["id"]) != "null" {
						t.Fatal("transport failure consumed a caller ID")
					}
					if response.header.Get("Access-Control-Allow-Origin") != "" || response.header.Get("Access-Control-Allow-Credentials") != "" || response.header.Get("Set-Cookie") != "" {
						t.Fatal("unsupported browser credentials/CORS enabled")
					}
					if test.status == 405 && response.header.Get("Allow") != "POST" {
						t.Fatal("missing method guidance")
					}
					if f.domain.creates.Load() != 0 || f.store.reads.Load() != 0 || f.store.writes.Load() != 0 {
						t.Fatal("rejected request reached domain/store")
					}
				})
			}
			// Prove the same real listener can actually dispatch an admitted write.
			r := f.request(t, data)
			r.Header.Set("Origin", f.url)
			r.Header.Set("Content-Type", "application/json; charset=UTF-8")
			result(t, f.send(t, r))
			if f.domain.creates.Load() != 1 || f.store.writes.Load() != 1 {
				t.Fatal("admitted control did not reach actual Writer")
			}
			if authenticated {
				reads, writes := f.store.reads.Load(), f.store.writes.Load()
				f.provider.Revoke(f.keyID)
				requireCode(t, f.send(t, f.request(t, data)), 401, "unauthenticated")
				if f.store.reads.Load() != reads || f.store.writes.Load() != writes || f.domain.creates.Load() != 1 {
					t.Fatal("revoked key reached domain/store")
				}
				_, err := f.provider.Issue(model.PrincipalRef{Kind: "agent", ID: "agent:scoped", Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:sponsor"}}, time.Now().Add(time.Hour))
				if !errors.Is(err, authn.ErrAuthzNotInstalled) {
					t.Fatal("scoped key was issued without an actual authorizer")
				}
				issued, err := f.provider.Issue(f.principal, time.Unix(0, f.keyClock.Load()).Add(time.Minute))
				if err != nil {
					t.Fatal(err)
				}
				f.token = issued.Token
				f.keyClock.Add(int64(2 * time.Minute))
				requireCode(t, f.send(t, f.request(t, data)), 401, "token_expired")
				if f.store.reads.Load() != reads || f.store.writes.Load() != writes || f.domain.creates.Load() != 1 {
					t.Fatal("expired key reached domain/store")
				}

			}
		})
	}
}

type delayedProvider struct {
	real    authn.Provider
	entered chan struct{}
}

func (p delayedProvider) Authenticate(ctx context.Context, token string) (model.PrincipalRef, error) {
	if _, err := p.real.Authenticate(ctx, token); err != nil {
		return model.PrincipalRef{}, err
	}
	if p.entered != nil {
		close(p.entered)
	}
	<-ctx.Done()
	return model.PrincipalRef{}, errors.New("private provider diagnostic must never reach HTTP")
}

func TestUnavailableAndCanceledAuthenticationFailClosed(t *testing.T) {
	f := newFixture(t, true, false, httpjson.Config{})
	a, err := authn.New(authn.Config{Provider: delayedProvider{real: f.provider}})
	if err != nil {
		t.Fatal(err)
	}
	h, err := httpjson.New(f.rpc, a, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	defer server.Close()
	data := message(t, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "unavailable"}}), json.RawMessage(`1`))
	r := f.request(t, data)
	r.URL.Host = server.Listener.Addr().String()
	r.Host = r.URL.Host
	requireCode(t, f.send(t, r), 503, "authn_unavailable")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r = f.request(t, data).WithContext(ctx)
	r.RemoteAddr = "127.0.0.1:30001"
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	requireCode(t, httpResult{w.Code, w.Header(), w.Body.Bytes()}, 503, "authn_unavailable")
	if f.domain.creates.Load() != 0 || f.store.reads.Load() != 0 || f.store.writes.Load() != 0 {
		t.Fatal("unavailable/canceled authentication reached domain/store")
	}
}

func TestListenerPostureAndConfiguration(t *testing.T) {
	f := newFixture(t, false, false, httpjson.Config{})
	for _, cfg := range []httpjson.Config{{MaxBodyBytes: -1}, {MaxConcurrentRequests: -1}, {MaxHeaderBytes: -1}, {RequestTimeout: -1}, {ReadHeaderTimeout: -1}, {ReadTimeout: -1}, {WriteTimeout: -1}, {IdleTimeout: -1}} {
		if _, err := httpjson.New(f.rpc, f.auth, cfg); err == nil {
			t.Fatal("negative configuration accepted")
		}
	}
	if _, err := httpjson.New(nil, f.auth, httpjson.Config{}); err == nil {
		t.Fatal("missing RPC accepted")
	}
	if _, err := httpjson.New(f.rpc, nil, httpjson.Config{}); err == nil {
		t.Fatal("missing authenticator accepted")
	}
	h, err := httpjson.New(f.rpc, f.auth, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, address := range []string{"", ":0", "0.0.0.0:0", "192.0.2.1:0", "example.invalid:0"} {
		s, err := h.Listen(context.Background(), address, nil)
		if s != nil {
			s.Close()
			t.Fatal("anonymous listener bound forbidden address")
		}
		if !errors.Is(err, authn.ErrForbidden) {
			t.Fatal("anonymous startup did not fail closed")
		}
	}
	authenticated := newFixture(t, true, false, httpjson.Config{})
	h, err = httpjson.New(authenticated.rpc, authenticated.auth, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	s, err := h.Listen(context.Background(), "0.0.0.0:0", nil)
	if s != nil {
		s.Close()
		t.Fatal("network listener omitted TLS")
	}
	if !errors.Is(err, authn.ErrForbidden) {
		t.Fatal("network TLS posture was bypassed")
	}
	// The lifecycle also owns a listener closed before Serve begins.
	s, err = h.Listen(context.Background(), "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTLSListenerUsesActualAdmissionAndDispatcher(t *testing.T) {
	f := newFixture(t, true, false, httpjson.Config{})
	h, err := httpjson.New(f.rpc, f.auth, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	certificate := httptest.NewTLSServer(h)
	client := certificate.Client()
	config := &tls.Config{Certificates: certificate.TLS.Certificates}
	certificate.Close()
	defer client.CloseIdleConnections()
	s, err := h.Listen(context.Background(), "127.0.0.1:0", config)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve() }()
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
		select {
		case err := <-done:
			if !errors.Is(err, http.ErrServerClosed) {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("TLS server did not stop")
		}
	}()
	r := f.request(t, message(t, "bead.ready", map[string]any{}, []byte(`9007199254740993`)))
	r.URL.Scheme = "https"
	r.URL.Host = s.Addr().String()
	r.Host = r.URL.Host
	r.Header.Set("Origin", "https://"+r.Host)
	response, err := roundTrip(client, r)
	if err != nil {
		t.Fatal(err)
	}
	result(t, response)
	if f.store.reads.Load() != 1 {
		t.Fatal("TLS request did not reach actual service")
	}
}
